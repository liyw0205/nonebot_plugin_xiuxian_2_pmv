"""本插件内的流媒体链接解析（不依赖上游 core / GitHub 下载）。

策略：
1. 本地正则提链并识别平台
2. 跟随短链跳转（本机 HTTP）
3. 按平台请求公开接口 / 页面 HTML，提取标题、封面、视频直链

不保证覆盖所有风控场景；失败时返回可读错误，不拖垮娱乐模块。
"""
from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from nonebot.log import logger

from ...xiuxian_utils.http_proxy import get_custom_proxy_url, http_client

_URL_RE = re.compile(
    r"https?://[^\s\u200b\u00a0<>\"'，。！？、；：（）【】《》]+",
    re.I,
)
_TRAIL_TRIM = re.compile(r"[\s\u200b\u00a0<>\"'，。！？、；：（）【】《》]+$")

# host keyword -> platform
_PLATFORM_HOSTS: list[tuple[str, str]] = [
    ("douyin.com", "douyin"),
    ("iesdouyin.com", "douyin"),
    ("b23.tv", "bilibili"),
    ("bili2233.cn", "bilibili"),
    ("bilibili.com", "bilibili"),
    ("biliapi.net", "bilibili"),
    ("xiaohongshu.com", "xiaohongshu"),
    ("xhslink.com", "xiaohongshu"),
    ("kuaishou.com", "kuaishou"),
    ("chenzhongtech.com", "kuaishou"),
    ("weibo.com", "weibo"),
    ("weibo.cn", "weibo"),
    ("t.cn", "weibo"),
    ("toutiao.com", "toutiao"),
    ("xiaoheihe.cn", "xiaoheihe"),
    ("twitter.com", "twitter"),
    ("x.com", "twitter"),
    ("tiktok.com", "tiktok"),
    ("goofish.com", "xianyu"),
    ("instagram.com", "instagram"),
]

# 这些平台在配置了 custom_proxy 时走代理（国内平台保持直连）
_PROXY_PLATFORMS = frozenset({"twitter", "tiktok", "instagram"})

_MOBILE_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 "
    "Mobile/15E148 Safari/604.1"
)
_DESKTOP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _trim_url(url: str) -> str:
    return _TRAIL_TRIM.sub("", (url or "").strip())


def detect_platform(url: str) -> str | None:
    host = (urlparse(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    for key, name in _PLATFORM_HOSTS:
        if host == key or host.endswith("." + key) or key in host:
            return name
    return None


def _use_proxy_for(platform: str | None = None, url: str | None = None) -> bool:
    """X/Twitter 等：仅当配置了 custom_proxy 时走代理。"""
    plat = platform or (detect_platform(url or "") if url else None)
    if plat not in _PROXY_PLATFORMS:
        return False
    return bool(get_custom_proxy_url())


def extract_urls(text: str) -> list[str]:
    if not text:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for m in _URL_RE.finditer(text):
        u = _trim_url(m.group(0))
        if not u.startswith("http"):
            continue
        key = u.rstrip("/")
        if key in seen:
            continue
        seen.add(key)
        out.append(u)
    return out


def extract_supported_links(text: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for u in extract_urls(text):
        plat = detect_platform(u)
        if plat:
            pairs.append((u, plat))
    return pairs


def _http_get(
    url: str,
    *,
    headers: dict | None = None,
    timeout: int = 20,
    use_proxy: bool = False,
):
    hdrs = {
        "User-Agent": _MOBILE_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    if headers:
        hdrs.update(headers)
    return http_client.request(
        "GET",
        url,
        headers=hdrs,
        timeout=timeout,
        allow_redirects=True,
        check_status=False,
        # 默认不用全局代理；X/Twitter 等按需打开
        use_config_proxy=bool(use_proxy),
    )


def _get_text(
    url: str,
    *,
    headers: dict | None = None,
    timeout: int = 20,
    use_proxy: bool = False,
) -> tuple[str, str]:
    """返回 (final_url, text)。"""
    resp = _http_get(url, headers=headers, timeout=timeout, use_proxy=use_proxy)
    text = getattr(resp, "text", "") or ""
    final = str(getattr(resp, "url", url) or url)
    return final, text


def expand_url(
    url: str,
    timeout: int = 15,
    *,
    use_proxy: bool | None = None,
    platform: str | None = None,
) -> str:
    """跟随短链，失败则返回原 URL。"""
    if use_proxy is None:
        use_proxy = _use_proxy_for(platform, url)
    try:
        resp = _http_get(
            url,
            headers={"User-Agent": _MOBILE_UA, "Accept": "*/*"},
            timeout=timeout,
            use_proxy=bool(use_proxy),
        )
        final = str(getattr(resp, "url", "") or "")
        return final or url
    except Exception as e:
        logger.debug(f"短链展开失败 {url[:80]}: {e}")
        return url


def _meta_content(html: str, prop: str) -> str:
    # property / name
    patterns = [
        rf'<meta[^>]+(?:property|name)=["\']{re.escape(prop)}["\'][^>]+content=["\']([^"\']+)["\']',
        rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\']{re.escape(prop)}["\']',
    ]
    for p in patterns:
        m = re.search(p, html, re.I)
        if m:
            return unquote(m.group(1).strip())
    return ""


def _first_json_script(html: str, marker: str) -> dict | list | None:
    idx = html.find(marker)
    if idx < 0:
        return None
    # find first { or [ after marker
    start = -1
    for i in range(idx, min(idx + 200, len(html))):
        if html[i] in "{[":
            start = i
            break
    if start < 0:
        return None
    # crude brace match
    open_ch = html[start]
    close_ch = "}" if open_ch == "{" else "]"
    depth = 0
    in_str = False
    esc = False
    for j in range(start, min(start + 500_000, len(html))):
        ch = html[j]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch == open_ch:
            depth += 1
        elif ch == close_ch:
            depth -= 1
            if depth == 0:
                blob = html[start : j + 1]
                try:
                    return json.loads(blob)
                except Exception:
                    return None
    return None


def _collect_http_urls(obj: Any, out: list[str], depth: int = 0) -> None:
    if depth > 8 or obj is None:
        return
    if isinstance(obj, str):
        if obj.startswith("http") and len(obj) < 2000:
            out.append(obj)
        return
    if isinstance(obj, dict):
        for v in obj.values():
            _collect_http_urls(v, out, depth + 1)
    elif isinstance(obj, list):
        for v in obj:
            _collect_http_urls(v, out, depth + 1)


def _pick_media(urls: list[str]) -> tuple[list[str], list[str]]:
    images: list[str] = []
    videos: list[str] = []
    seen: set[str] = set()
    for u in urls:
        key = u.split("?")[0].rstrip("/")
        if key in seen:
            continue
        low = u.lower()
        if any(x in low for x in (".mp4", ".m3u8", "mime_type=video", "video/mp4", "/play/")):
            seen.add(key)
            videos.append(u)
        elif any(x in low for x in (".jpg", ".jpeg", ".png", ".webp", "image", "cover", "thumb")):
            seen.add(key)
            images.append(u)
    return images, videos


def _base_meta(platform: str, url: str) -> dict[str, Any]:
    return {
        "platform": platform,
        "parser_name": platform,
        "url": url,
        "source_url": url,
        "title": "",
        "author": "",
        "desc": "",
        "image_urls": [],
        "video_urls": [],
        "error": None,
    }


# ---------- platform parsers ----------


def parse_bilibili(url: str) -> dict[str, Any]:
    meta = _base_meta("bilibili", url)
    final = expand_url(url)
    meta["url"] = final
    bvid = None
    m = re.search(r"\b(BV[\w]+)\b", final)
    if m:
        bvid = m.group(1)
    aid = None
    if not bvid:
        m = re.search(r"[?&]aid=(\d+)", final) or re.search(r"/av(\d+)", final)
        if m:
            aid = m.group(1)
    if not bvid and not aid:
        # try page
        try:
            _, html = _get_text(final, headers={"User-Agent": _DESKTOP_UA})
            m = re.search(r"\b(BV[\w]+)\b", html)
            if m:
                bvid = m.group(1)
        except Exception as e:
            meta["error"] = f"B站页面读取失败：{e}"
            return meta
    if not bvid and not aid:
        meta["error"] = "未能识别 B 站 BV/av 号"
        return meta
    try:
        api = "https://api.bilibili.com/x/web-interface/view"
        params = {"bvid": bvid} if bvid else {"aid": aid}
        data = http_client.get_json(
            api,
            params=params,
            timeout=15,
            headers={"User-Agent": _DESKTOP_UA, "Referer": "https://www.bilibili.com"},
            use_config_proxy=False,
        )
        if int(data.get("code", -1)) != 0:
            meta["error"] = f"B站接口：{data.get('message') or data.get('code')}"
            return meta
        info = data.get("data") or {}
        meta["title"] = str(info.get("title") or "")
        owner = info.get("owner") or {}
        meta["author"] = str(owner.get("name") or "")
        meta["desc"] = str(info.get("desc") or "")[:400]
        pic = info.get("pic")
        if pic:
            meta["image_urls"] = [str(pic)]
        cid = None
        pages = info.get("pages") or []
        if pages:
            cid = pages[0].get("cid")
        if not cid:
            cid = info.get("cid")
        aid_n = info.get("aid") or aid
        if cid and aid_n:
            play = http_client.get_json(
                "https://api.bilibili.com/x/player/playurl",
                params={
                    "avid": aid_n,
                    "cid": cid,
                    "qn": 64,
                    "fnval": 1,
                    "fourk": 1,
                },
                timeout=15,
                headers={
                    "User-Agent": _DESKTOP_UA,
                    "Referer": final if "bilibili.com" in final else "https://www.bilibili.com",
                },
                use_config_proxy=False,
            )
            if int(play.get("code", -1)) == 0:
                durl = ((play.get("data") or {}).get("durl") or [])
                videos = [str(x.get("url")) for x in durl if x.get("url")]
                # dash backup
                dash = ((play.get("data") or {}).get("dash") or {})
                for v in (dash.get("video") or [])[:2]:
                    if v.get("baseUrl"):
                        videos.append(str(v["baseUrl"]))
                    elif v.get("base_url"):
                        videos.append(str(v["base_url"]))
                meta["video_urls"] = videos[:3]
        if not meta["video_urls"] and not meta["image_urls"]:
            meta["error"] = "B站未解析到可发送媒体（可能需 cookie/大会员清晰度）"
    except Exception as e:
        meta["error"] = f"B站解析异常：{e}"
    return meta


def _parse_html_og(
    platform: str,
    url: str,
    *,
    use_proxy: bool | None = None,
) -> dict[str, Any]:
    meta = _base_meta(platform, url)
    if use_proxy is None:
        use_proxy = _use_proxy_for(platform, url)
    final = expand_url(url, use_proxy=use_proxy, platform=platform)
    meta["url"] = final
    try:
        _, html = _get_text(
            final,
            headers={
                "User-Agent": _MOBILE_UA if platform != "twitter" else _DESKTOP_UA,
                "Referer": final,
            },
            timeout=25 if use_proxy else 20,
            use_proxy=bool(use_proxy),
        )
    except Exception as e:
        meta["error"] = f"页面获取失败：{e}"
        return meta

    title = _meta_content(html, "og:title") or _meta_content(html, "twitter:title")
    if not title:
        m = re.search(r"<title[^>]*>([^<]+)</title>", html, re.I)
        if m:
            title = m.group(1).strip()
    desc = _meta_content(html, "og:description") or _meta_content(html, "description")
    image = _meta_content(html, "og:image") or _meta_content(html, "twitter:image")
    video = (
        _meta_content(html, "og:video:url")
        or _meta_content(html, "og:video")
        or _meta_content(html, "twitter:player:stream")
        or _meta_content(html, "twitter:player")
    )
    meta["title"] = title
    meta["desc"] = (desc or "")[:400]
    if image:
        meta["image_urls"] = [image]
    if video and video.startswith("http"):
        meta["video_urls"] = [video]

    # 尝试页面内嵌 JSON 里捞 mp4
    found: list[str] = []
    for marker in (
        "window._ROUTER_DATA",
        "window.__INITIAL_STATE__",
        "window.INIT_STATE",
        "RENDER_DATA",
        "playAddr",
        "play_addr",
        '"mp4"',
        "video_url",
        "playbackUrl",
    ):
        obj = _first_json_script(html, marker) or _extract_json_assignment(html, marker)
        if obj is not None:
            _collect_http_urls(obj, found)
    # regex mp4：匹配到引号/空白/尖括号为止，避免 pkey 等 query 被截断
    for m in re.finditer(r"https?://[^\s\"'<>]+?\.mp4(?:\?[^\s\"'<>]*)?", html):
        found.append(m.group(0).encode("utf-8").decode("unicode_escape", errors="ignore"))
    # m3u8 also useful for some platforms
    for m in re.finditer(r"https?://[^\s\"'<>]+?\.m3u8(?:\?[^\s\"'<>]*)?", html):
        found.append(m.group(0))
    # unescape \/
    found = [u.replace("\\/", "/").replace("\\u002F", "/") for u in found]
    imgs, vids = _pick_media(found)
    if vids:
        meta["video_urls"] = list(dict.fromkeys(meta["video_urls"] + vids))[:3]
    if imgs and not meta["image_urls"]:
        meta["image_urls"] = imgs[:6]

    if not meta["video_urls"] and not meta["image_urls"] and not meta["title"]:
        meta["error"] = "未能从页面提取标题或媒体直链（平台可能风控/需登录）"
    return meta


def _extract_json_assignment(html: str, marker: str) -> dict | list | None:
    """从 window.XXX = {...} 提取完整 JSON（比标记后首个对象更稳）。"""
    idx = html.find(marker)
    if idx < 0:
        return None
    eq = html.find("=", idx)
    if eq < 0:
        return None
    start = -1
    for i in range(eq + 1, min(eq + 80, len(html))):
        if html[i] in "{[":
            start = i
            break
    if start < 0:
        return None
    open_ch = html[start]
    close_ch = "}" if open_ch == "{" else "]"
    depth = 0
    in_str = False
    esc = False
    for j in range(start, min(start + 800_000, len(html))):
        ch = html[j]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch == open_ch:
            depth += 1
        elif ch == close_ch:
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(html[start : j + 1])
                except Exception:
                    return None
    return None


def _walk_pick_str(obj: Any, keys: set[str]) -> str:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys and isinstance(v, str) and v.strip():
                return v.strip()
        for v in obj.values():
            got = _walk_pick_str(v, keys)
            if got:
                return got
    elif isinstance(obj, list):
        for v in obj:
            got = _walk_pick_str(v, keys)
            if got:
                return got
    return ""


def _walk_cdn_urls(obj: Any, field_names: set[str], out: list[str]) -> None:
    """快手等平台常见 {cdn,url} 列表字段。"""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in field_names and isinstance(v, list):
                for item in v:
                    if isinstance(item, dict) and isinstance(item.get("url"), str):
                        u = item["url"].strip()
                        if u.startswith("http"):
                            out.append(u)
                    elif isinstance(item, str) and item.startswith("http"):
                        out.append(item.strip())
            else:
                _walk_cdn_urls(v, field_names, out)
    elif isinstance(obj, list):
        for v in obj:
            _walk_cdn_urls(v, field_names, out)


def parse_douyin(url: str) -> dict[str, Any]:
    """抖音：参考 astrbot_plugin_parser — ttwid + share 页 window._ROUTER_DATA。"""
    meta = _base_meta("douyin", url)
    jar: dict[str, str] = {}

    def _cookie_header() -> str:
        return "; ".join(f"{k}={v}" for k, v in jar.items() if v)

    def _merge_set_cookie(resp) -> None:
        # requests: resp.cookies
        try:
            for c in resp.cookies:
                jar[c.name] = c.value
        except Exception:
            pass
        # raw header
        raw = ""
        try:
            raw = resp.headers.get("Set-Cookie") or ""
        except Exception:
            raw = ""
        if raw:
            # may be multiple joined; take name=value pairs at start of each
            for part in re.split(r",(?=[^ ;]+=)", raw):
                m = re.match(r"\s*([^=;\s]+)=([^;]+)", part)
                if m:
                    jar[m.group(1)] = m.group(2)

    def _headers_for(referer: str = "https://www.iesdouyin.com/") -> dict:
        h = {
            "User-Agent": _MOBILE_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Referer": referer,
        }
        if jar:
            h["Cookie"] = _cookie_header()
        return h

    # 0) 短链展开（保留 cookie）
    cur = url
    try:
        resp = http_client.request(
            "GET",
            cur,
            headers=_headers_for("https://www.douyin.com/"),
            timeout=15,
            allow_redirects=True,
            check_status=False,
            use_config_proxy=False,
        )
        _merge_set_cookie(resp)
        cur = str(getattr(resp, "url", cur) or cur)
    except Exception as e:
        logger.debug(f"抖音短链展开失败: {e}")
    meta["url"] = cur

    aweme_id = None
    m = re.search(r"/(?:video|note|slides)/(\d+)", cur) or re.search(
        r"modal_id=(\d+)", cur
    )
    if m:
        aweme_id = m.group(1)
    if not aweme_id:
        m = re.search(r"/(?:video|note)/(\d+)", url)
        if m:
            aweme_id = m.group(1)

    # 1) 注册匿名 ttwid（上游同款）
    if "ttwid" not in jar:
        try:
            reg = http_client.request(
                "POST",
                "https://ttwid.bytedance.com/ttwid/union/register/",
                headers={
                    "User-Agent": _MOBILE_UA,
                    "Content-Type": "application/json",
                    "Referer": "https://www.iesdouyin.com/",
                },
                json={
                    "region": "cn",
                    "aid": 1768,
                    "needFid": False,
                    "service": "www.iesdouyin.com",
                    "union": True,
                    "fid": "",
                },
                timeout=15,
                check_status=False,
                use_config_proxy=False,
            )
            _merge_set_cookie(reg)
            try:
                body = reg.json()
            except Exception:
                body = {}
            cb = body.get("redirect_url") if isinstance(body, dict) else None
            if cb:
                cbr = http_client.request(
                    "GET",
                    cb,
                    headers=_headers_for(),
                    timeout=15,
                    allow_redirects=False,
                    check_status=False,
                    use_config_proxy=False,
                )
                _merge_set_cookie(cbr)
        except Exception as e:
            logger.debug(f"抖音 ttwid 注册失败: {e}")

    # 2) share 页抽 _ROUTER_DATA
    share_urls = []
    if aweme_id:
        share_urls.append(f"https://www.iesdouyin.com/share/video/{aweme_id}/")
        share_urls.append(f"https://www.iesdouyin.com/share/note/{aweme_id}/")
        share_urls.append(f"https://m.douyin.com/share/video/{aweme_id}/")
    if cur not in share_urls:
        share_urls.insert(0, cur)

    for share in share_urls:
        try:
            resp = http_client.request(
                "GET",
                share,
                headers=_headers_for(share),
                timeout=20,
                allow_redirects=True,
                check_status=False,
                use_config_proxy=False,
            )
            _merge_set_cookie(resp)
            html = getattr(resp, "text", "") or ""
            meta["url"] = str(getattr(resp, "url", share) or share)
            m = re.search(
                r"window\._ROUTER_DATA\s*=\s*(\{.*?\})\s*</script>",
                html,
                re.S,
            )
            data = None
            if m:
                try:
                    data = json.loads(m.group(1).strip())
                except Exception:
                    data = _extract_json_assignment(html, "window._ROUTER_DATA")
            if data is None:
                data = _extract_json_assignment(html, "window._ROUTER_DATA")
            if not isinstance(data, dict):
                continue

            # loaderData.video_(id)/page or note_(id)/page -> videoInfoRes.item_list
            loader = data.get("loaderData") or {}
            page = None
            if isinstance(loader, dict):
                for k, v in loader.items():
                    if isinstance(k, str) and (
                        "video_" in k or "note_" in k or k.endswith("/page")
                    ):
                        if isinstance(v, dict):
                            page = v
                            break
            item = None
            if isinstance(page, dict):
                vinfo = page.get("videoInfoRes") or page.get("video_info_res") or page
                items = []
                if isinstance(vinfo, dict):
                    items = vinfo.get("item_list") or vinfo.get("itemList") or []
                if items and isinstance(items[0], dict):
                    item = items[0]
            if item is None:
                # 兜底全树找 play_addr
                found: list[str] = []
                _collect_http_urls(data, found)
                vids = [
                    u.replace("playwm", "play")
                    for u in found
                    if isinstance(u, str) and (".mp4" in u.lower() or "play" in u.lower())
                ]
                imgs = [
                    u
                    for u in found
                    if isinstance(u, str)
                    and any(x in u.lower() for x in (".jpg", ".jpeg", ".png", ".webp"))
                ]
                if vids or imgs:
                    meta["video_urls"] = list(dict.fromkeys(vids))[:3]
                    meta["image_urls"] = list(dict.fromkeys(imgs))[:6]
                    meta["title"] = meta.get("title") or "抖音视频"
                    meta["error"] = None
                    return meta
                continue

            meta["title"] = str(item.get("desc") or "")[:200]
            author = item.get("author") or {}
            if isinstance(author, dict):
                meta["author"] = str(author.get("nickname") or "")
                # avatar for card
                for ak in ("avatar_thumb", "avatar_medium", "avatar_larger"):
                    av = author.get(ak) or {}
                    if isinstance(av, dict):
                        ul = av.get("url_list") or []
                        if ul:
                            meta["avatar_url"] = ul[0]
                            break
            video = item.get("video") or {}
            play = video.get("play_addr") or video.get("play_addr_h264") or {}
            urls = list(play.get("url_list") or []) if isinstance(play, dict) else []
            # play token -> snssdk play endpoint
            uri = play.get("uri") if isinstance(play, dict) else None
            if not uri and urls:
                for u in urls:
                    qs = parse_qs(urlparse(u).query)
                    if qs.get("video_id"):
                        uri = qs["video_id"][0]
                        break
            if uri:
                for ratio in ("1080p", "720p", "540p", "360p"):
                    play_u = (
                        f"https://aweme.snssdk.com/aweme/v1/play/"
                        f"?video_id={uri}&ratio={ratio}"
                    )
                    try:
                        pr = http_client.request(
                            "GET",
                            play_u,
                            headers={
                                "User-Agent": _MOBILE_UA,
                                "Referer": share,
                                "Range": "bytes=0-1",
                            },
                            timeout=12,
                            allow_redirects=True,
                            check_status=False,
                            use_config_proxy=False,
                        )
                        if int(getattr(pr, "status_code", 0) or 0) < 400:
                            final_u = str(getattr(pr, "url", "") or play_u)
                            if final_u.startswith("http"):
                                urls.insert(0, final_u)
                                break
                    except Exception:
                        continue
            clean = []
            for u in urls:
                if isinstance(u, str) and u.startswith("http"):
                    clean.append(u.replace("playwm", "play"))
            cover = video.get("cover") or video.get("origin_cover") or {}
            images = list(cover.get("url_list") or []) if isinstance(cover, dict) else []
            for im in item.get("images") or []:
                if isinstance(im, dict):
                    images.extend(
                        [x for x in (im.get("url_list") or []) if isinstance(x, str)]
                    )
            meta["video_urls"] = list(dict.fromkeys(clean))[:3]
            meta["image_urls"] = list(dict.fromkeys(images))[:6]
            if meta["video_urls"] or meta["image_urls"]:
                meta["error"] = None
                return meta
        except Exception as e:
            logger.debug(f"抖音 share 解析失败 {share}: {e}")

    # 3) 旧 iteminfo 兜底
    if aweme_id:
        try:
            resp = http_client.request(
                "GET",
                f"https://www.iesdouyin.com/web/api/v2/aweme/iteminfo/?item_ids={aweme_id}",
                headers=_headers_for(),
                timeout=15,
                check_status=False,
                use_config_proxy=False,
            )
            raw = getattr(resp, "text", "") or ""
            if raw.strip().startswith("{"):
                data = json.loads(raw)
                items = data.get("item_list") or []
                if items:
                    item = items[0]
                    meta["title"] = str(item.get("desc") or "")[:200]
                    author = item.get("author") or {}
                    if isinstance(author, dict):
                        meta["author"] = str(author.get("nickname") or "")
                    video = item.get("video") or {}
                    play = video.get("play_addr") or {}
                    urls = [
                        u.replace("playwm", "play")
                        for u in (play.get("url_list") or [])
                        if isinstance(u, str)
                    ]
                    cover = video.get("cover") or {}
                    images = list(cover.get("url_list") or [])
                    meta["video_urls"] = list(dict.fromkeys(urls))[:3]
                    meta["image_urls"] = list(dict.fromkeys(images))[:6]
                    if meta["video_urls"] or meta["image_urls"]:
                        meta["error"] = None
                        return meta
        except Exception as e:
            logger.debug(f"抖音 iteminfo 兜底失败: {e}")

    meta["error"] = meta.get("error") or "抖音未解析到可发送媒体（需 ttwid/页面数据）"
    return meta


def parse_kuaishou(url: str) -> dict[str, Any]:
    """快手：优先 window.INIT_STATE 的 mainMvUrls/coverUrls/caption。"""
    meta = _base_meta("kuaishou", url)
    final = expand_url(url)
    meta["url"] = final
    try:
        _, html = _get_text(
            final,
            headers={"User-Agent": _MOBILE_UA, "Referer": "https://v.kuaishou.com/"},
            timeout=20,
        )
    except Exception as e:
        meta["error"] = f"页面获取失败：{e}"
        return meta

    state = _extract_json_assignment(html, "window.INIT_STATE")
    if state is None:
        # 回退通用解析
        return _parse_html_og("kuaishou", url)

    videos: list[str] = []
    images: list[str] = []
    _walk_cdn_urls(state, {"mainMvUrls", "mp4Url", "photoUrl"}, videos)
    _walk_cdn_urls(state, {"coverUrls", "webpCoverUrls", "coverUrl"}, images)
    # 再捞 manifest representation 里的 mp4
    found: list[str] = []
    _collect_http_urls(state, found)
    for u in found:
        low = u.lower()
        if ".mp4" in low and u.startswith("http"):
            videos.append(u)
        elif any(x in low for x in (".jpg", ".jpeg", ".png", ".webp")) and "uhead" not in low:
            images.append(u)

    # 去重保序；优先 kwimgs/yximgs 等相对完整的直链
    def _rank(u: str) -> tuple[int, int]:
        low = u.lower()
        score = 0
        if "mainmv" in low or "clientcachekey" in low:
            score += 2
        if "pkey=" in low:
            score += 1
        if "kwimgs.com" in low or "yximgs.com" in low or "kwaicdn.com" in low:
            score += 1
        return (-score, -len(u))

    videos = sorted(dict.fromkeys(videos), key=_rank)
    images = list(dict.fromkeys(images))
    # 去掉头像类
    images = [
        u
        for u in images
        if "uhead" not in u.lower() and "/bg" not in urlparse(u).path.lower()
    ]

    caption = _walk_pick_str(state, {"caption"})
    if not caption:
        raw_title = _walk_pick_str(state, {"title"})
        if raw_title and raw_title not in {
            "去快手享超清画质",
            "参与免费领道具！",
            "快手",
            "快手视频",
        }:
            caption = raw_title
    author = _walk_pick_str(state, {"userName"})
    if not author:
        author = _walk_pick_str(state, {"authorName", "name"})
    meta["title"] = caption or "快手视频"
    meta["author"] = author or ""
    meta["desc"] = (caption or "")[:400]
    meta["video_urls"] = videos[:3]
    meta["image_urls"] = images[:6]
    if not meta["video_urls"] and not meta["image_urls"]:
        # 最后回退
        fallback = _parse_html_og("kuaishou", final)
        if fallback.get("video_urls") or fallback.get("image_urls"):
            return fallback
        meta["error"] = "快手未解析到可发送媒体（可能风控/登录）"
    return meta


def parse_xiaohongshu(url: str) -> dict[str, Any]:
    return _parse_html_og("xiaohongshu", url)


def parse_weibo(url: str) -> dict[str, Any]:
    return _parse_html_og("weibo", url)


def parse_twitter(url: str) -> dict[str, Any]:
    """X/Twitter：有 custom_proxy 时走代理。

    参考 astrbot_plugin_parser：优先 xdown.app 网页接口抽 MP4/图片；
    再回退 fxtwitter / 页面 OG。
    """
    meta = _base_meta("twitter", url)
    use_proxy = _use_proxy_for("twitter", url)
    final = expand_url(url, use_proxy=use_proxy, platform="twitter")
    meta["url"] = final
    m = re.search(r"/status(?:es)?/(\d+)", final) or re.search(r"/status(?:es)?/(\d+)", url)
    status_id = m.group(1) if m else None
    query_url = final if "status/" in final else url

    # 1) xdown.app（上游 TwitterParser 同款）
    try:
        resp = http_client.request(
            "POST",
            "https://xdown.app/api/ajaxSearch",
            headers={
                "User-Agent": _DESKTOP_UA,
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://xdown.app",
                "Referer": "https://xdown.app/",
            },
            data={"q": query_url, "lang": "zh-cn"},
            timeout=30,
            check_status=False,
            use_config_proxy=bool(use_proxy),
        )
        raw = getattr(resp, "text", "") or ""
        data = json.loads(raw) if raw.strip().startswith("{") else {}
        if str(data.get("status") or "").lower() == "ok" and data.get("data"):
            html = str(data["data"])
            # 粗解析：img src / 下载 MP4 / 下载图片 / h3 标题
            title_m = re.search(r"<h3[^>]*>(.*?)</h3>", html, re.S | re.I)
            if title_m:
                meta["title"] = re.sub(r"<[^>]+>", "", title_m.group(1)).strip()[:200]
            imgs = re.findall(r'<img[^>]+src=["\'](https?://[^"\']+)["\']', html, re.I)
            videos: list[str] = []
            images: list[str] = []
            for href, text in re.findall(
                r'<a[^>]+href=["\'](https?://[^"\']+)["\'][^>]*>(.*?)</a>',
                html,
                re.S | re.I,
            ):
                t = re.sub(r"<[^>]+>", "", text)
                if "下载 MP4" in t or "MP4" in t.upper():
                    videos.append(href)
                elif "下载图片" in t or "图片" in t:
                    images.append(href)
            if imgs:
                images = list(dict.fromkeys(imgs + images))
            meta["video_urls"] = list(dict.fromkeys(videos))[:3]
            meta["image_urls"] = list(dict.fromkeys(images))[:6]
            if not meta.get("author"):
                meta["author"] = ""
            if meta["video_urls"] or meta["image_urls"] or meta.get("title"):
                meta["error"] = None
                return meta
    except Exception as e:
        logger.debug(f"xdown.app 解析失败: {e}")

    # 2) fxtwitter / vxtwitter
    if status_id:
        for api in (
            f"https://api.fxtwitter.com/status/{status_id}",
            f"https://api.vxtwitter.com/Twitter/status/{status_id}",
        ):
            try:
                resp = _http_get(
                    api,
                    headers={"User-Agent": _DESKTOP_UA, "Accept": "application/json"},
                    timeout=25,
                    use_proxy=bool(use_proxy),
                )
                raw = getattr(resp, "text", "") or ""
                data = json.loads(raw) if raw.strip().startswith("{") else {}
                tweet = data.get("tweet") if isinstance(data.get("tweet"), dict) else data
                if not isinstance(tweet, dict):
                    continue
                if not (tweet.get("text") or tweet.get("media") or tweet.get("mediaURLs")):
                    continue
                meta["title"] = str(tweet.get("text") or tweet.get("title") or "")[:200]
                author = tweet.get("author") or {}
                if isinstance(author, dict):
                    meta["author"] = str(author.get("name") or author.get("screen_name") or "")
                videos, images = [], []
                media = tweet.get("media") or {}
                if isinstance(media, dict):
                    for v in media.get("videos") or []:
                        if isinstance(v, dict) and (v.get("url") or v.get("src")):
                            videos.append(str(v.get("url") or v.get("src")))
                        elif isinstance(v, str):
                            videos.append(v)
                    for im in media.get("photos") or media.get("images") or []:
                        if isinstance(im, dict) and (im.get("url") or im.get("src")):
                            images.append(str(im.get("url") or im.get("src")))
                        elif isinstance(im, str):
                            images.append(im)
                for key in ("mediaURLs", "media_urls"):
                    for u in tweet.get(key) or data.get(key) or []:
                        if isinstance(u, str) and u.startswith("http"):
                            (videos if ".mp4" in u.lower() else images).append(u)
                if not videos:
                    found: list[str] = []
                    _collect_http_urls(data, found)
                    videos = [u for u in found if ".mp4" in u.lower()]
                    if not images:
                        images = [
                            u
                            for u in found
                            if any(x in u.lower() for x in (".jpg", ".jpeg", ".png", ".webp"))
                        ]
                meta["video_urls"] = list(dict.fromkeys(videos))[:3]
                meta["image_urls"] = list(dict.fromkeys(images))[:6]
                if meta["video_urls"] or meta["image_urls"] or meta["title"]:
                    meta["error"] = None
                    return meta
            except Exception as e:
                logger.debug(f"fxtwitter/vxtwitter 失败 {api}: {e}")

    page = _parse_html_og("twitter", final if final else url, use_proxy=use_proxy)
    if page.get("video_urls") or page.get("image_urls") or page.get("title"):
        return page
    meta["error"] = meta.get("error") or page.get("error") or "X/Twitter 未解析到媒体"
    if not use_proxy:
        meta["error"] = "X/Twitter 建议配置 custom_proxy；" + str(meta["error"])
    return meta


def parse_xiaoheihe(url: str) -> dict[str, Any]:
    """小黑盒：分享页多为 SPA，尝试公开 web share / 链接接口。"""
    meta = _base_meta("xiaoheihe", url)
    final = expand_url(url, use_proxy=False, platform="xiaoheihe")
    meta["url"] = final
    link_id = None
    m = re.search(r"link_id=(\d+)", final) or re.search(r"/link/(\d+)", final) or re.search(
        r"/link/(\d+)", url
    )
    if m:
        link_id = m.group(1)
    if link_id:
        candidates = [
            f"https://api.xiaoheihe.cn/bbs/app/link/web/share?link_id={link_id}",
            f"https://api.xiaoheihe.cn/bbs/app/api/web/share?link_id={link_id}",
            f"https://www.xiaoheihe.cn/bbs/app/api/web/share/link?link_id={link_id}",
            f"https://api.xiaoheihe.cn/bbs/app/link/tree?link_id={link_id}&h_src=web",
        ]
        for api in candidates:
            try:
                resp = _http_get(
                    api,
                    headers={
                        "User-Agent": _DESKTOP_UA,
                        "Referer": "https://www.xiaoheihe.cn/",
                        "Accept": "application/json",
                    },
                    timeout=15,
                    use_proxy=False,
                )
                raw = getattr(resp, "text", "") or ""
                if not raw.strip().startswith("{"):
                    continue
                data = json.loads(raw)
                # 常见 result/link
                node = data.get("result") or data.get("data") or data
                if not isinstance(node, dict):
                    continue
                link = node.get("link") or node.get("share") or node
                if not isinstance(link, dict):
                    continue
                title = link.get("title") or link.get("text") or link.get("description")
                if title:
                    meta["title"] = str(title)[:200]
                # 视频/图
                found: list[str] = []
                _collect_http_urls(link, found)
                vids = [u for u in found if ".mp4" in u.lower() or "video" in u.lower()]
                imgs = [
                    u
                    for u in found
                    if any(x in u.lower() for x in (".jpg", ".jpeg", ".png", ".webp"))
                ]
                # thumbs
                for k in ("img", "imgs", "thumb", "image", "cover"):
                    v = link.get(k)
                    if isinstance(v, str) and v.startswith("http"):
                        imgs.append(v)
                    elif isinstance(v, list):
                        imgs.extend([x for x in v if isinstance(x, str) and x.startswith("http")])
                meta["video_urls"] = list(dict.fromkeys(vids))[:3]
                meta["image_urls"] = list(dict.fromkeys(imgs))[:6]
                if meta["video_urls"] or meta["image_urls"] or meta["title"]:
                    meta["error"] = None
                    if meta["video_urls"] or meta["image_urls"]:
                        return meta
            except Exception as e:
                logger.debug(f"小黑盒接口失败 {api}: {e}")
    page = _parse_html_og("xiaoheihe", final, use_proxy=False)
    if page.get("video_urls") or page.get("image_urls"):
        return page
    if page.get("title") and page.get("title") not in ("小黑盒 - 玩家高能聚集地", "小黑盒"):
        meta["title"] = page["title"]
    if not meta.get("video_urls") and not meta.get("image_urls"):
        meta["error"] = meta.get("error") or "小黑盒未解析到媒体（分享页多为前端渲染）"
    return meta


def parse_generic(platform: str, url: str) -> dict[str, Any]:
    return _parse_html_og(platform, url)


_PARSERS = {
    "bilibili": parse_bilibili,
    "douyin": parse_douyin,
    "kuaishou": parse_kuaishou,
    "xiaohongshu": parse_xiaohongshu,
    "weibo": parse_weibo,
    "twitter": parse_twitter,
    "xiaoheihe": parse_xiaoheihe,
    "tiktok": lambda u: _parse_html_og("tiktok", u),
    "instagram": lambda u: _parse_html_og("instagram", u),
}


def parse_url(url: str, platform: str | None = None) -> dict[str, Any]:
    plat = platform or detect_platform(url) or "unknown"
    fn = _PARSERS.get(plat)
    if fn:
        return fn(url)
    if plat != "unknown":
        return parse_generic(plat, url)
    meta = _base_meta("unknown", url)
    meta["error"] = "不支持的平台"
    return meta


def parse_text_native(text: str) -> list[dict[str, Any]]:
    links = extract_supported_links(text)
    if not links:
        # 若用户强制「链接解析」塞了任意 URL，尝试通用
        any_urls = extract_urls(text)
        if not any_urls:
            return []
        return [parse_url(any_urls[0], detect_platform(any_urls[0]) or "unknown")]
    return [parse_url(u, p) for u, p in links]
