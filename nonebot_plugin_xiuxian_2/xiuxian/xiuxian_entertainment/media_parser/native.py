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

from ...xiuxian_utils.http_proxy import http_client

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


def _get_text(url: str, *, headers: dict | None = None, timeout: int = 20) -> tuple[str, str]:
    """返回 (final_url, text)。"""
    hdrs = {
        "User-Agent": _MOBILE_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    if headers:
        hdrs.update(headers)
    resp = http_client.request(
        "GET",
        url,
        headers=hdrs,
        timeout=timeout,
        allow_redirects=True,
        check_status=False,
    )
    text = getattr(resp, "text", "") or ""
    final = str(getattr(resp, "url", url) or url)
    return final, text


def expand_url(url: str, timeout: int = 15) -> str:
    """跟随短链，失败则返回原 URL。"""
    try:
        resp = http_client.request(
            "GET",
            url,
            headers={"User-Agent": _MOBILE_UA, "Accept": "*/*"},
            timeout=timeout,
            allow_redirects=True,
            check_status=False,
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


def _parse_html_og(platform: str, url: str) -> dict[str, Any]:
    meta = _base_meta(platform, url)
    final = expand_url(url)
    meta["url"] = final
    try:
        _, html = _get_text(
            final,
            headers={
                "User-Agent": _MOBILE_UA,
                "Referer": final,
            },
            timeout=20,
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
        "RENDER_DATA",
        "playAddr",
        "play_addr",
        '"mp4"',
    ):
        obj = _first_json_script(html, marker)
        if obj is not None:
            _collect_http_urls(obj, found)
    # regex mp4：匹配到引号/空白/尖括号为止，避免 pkey 等 query 被截断
    for m in re.finditer(r"https?://[^\s\"'<>]+?\.mp4(?:\?[^\s\"'<>]*)?", html):
        found.append(m.group(0).encode("utf-8").decode("unicode_escape", errors="ignore"))
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
    # 优先移动端页 + 通用 og / 内嵌 JSON
    meta = _parse_html_og("douyin", url)
    if meta.get("video_urls") or meta.get("image_urls"):
        return meta
    # 再试 iesdouyin share 页
    final = meta.get("url") or url
    m = re.search(r"/video/(\d+)", final)
    if m:
        share = f"https://www.iesdouyin.com/share/video/{m.group(1)}/"
        meta2 = _parse_html_og("douyin", share)
        if meta2.get("video_urls") or meta2.get("image_urls") or meta2.get("title"):
            return meta2
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

    caption = _walk_pick_str(state, {"caption", "title"})
    author = _walk_pick_str(state, {"userName", "name", "authorName"})
    # caption 优先；避免广告 config 里的 title 抢答
    if caption and caption not in ("去快手享超清画质", "参与免费领道具！"):
        meta["title"] = caption
    else:
        meta["title"] = caption or "快手视频"
    meta["author"] = author if author and "快手" not in author else author
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


def parse_generic(platform: str, url: str) -> dict[str, Any]:
    return _parse_html_og(platform, url)


_PARSERS = {
    "bilibili": parse_bilibili,
    "douyin": parse_douyin,
    "kuaishou": parse_kuaishou,
    "xiaohongshu": parse_xiaohongshu,
    "weibo": parse_weibo,
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
