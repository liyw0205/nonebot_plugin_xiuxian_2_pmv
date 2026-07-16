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
    ("youtu.be", "youtube"),
    ("youtube.com", "youtube"),
    ("goofish.com", "xianyu"),
    ("instagram.com", "instagram"),
]

# 这些平台在配置了 custom_proxy 时走代理（国内平台保持直连）
_PROXY_PLATFORMS = frozenset({"twitter", "tiktok", "instagram", "youtube"})

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
        "audio_urls": [],
        "error": None,
    }


# 发送侧上限（字节）：优先最高清，超过则降档
MEDIA_MAX_BYTES = 20 * 1024 * 1024


def media_quality_score(url: str, *, kind: str = "video") -> int:
    """URL 质量分：越高越好。用于排序候选链。"""
    low = (url or "").lower()
    score = 0
    if kind == "video":
        # 分辨率/档位
        if any(x in low for x in ("ultrav", "v6ultra", "4k", "2160", "uhd")):
            score += 100
        elif any(x in low for x in ("1080", "fhd", "highv", "v6high", "hd15", "mp4_720p")):
            score += 80
        if "mp4_720p" in low or "720p" in low:
            score += 70
        elif "high" in low and "highlight" not in low:
            score += 55
        if any(x in low for x in ("720", "mid", "standard", "mp4_hd")):
            score += 50
        if any(x in low for x in ("480", "540", "mp4_ld", "low")):
            score += 25
        if any(x in low for x in ("360p", "240p", "preview", "thumb", "gif")):
            score -= 40
        # B站 qn 痕迹 / 码率提示
        for mark, pts in (
            ("-120.", 95),
            ("-116.", 90),
            ("-112.", 85),
            ("-80.", 70),
            ("-64.", 55),
            ("-32.", 30),
            ("-16.", 15),
        ):
            if mark in low:
                score += pts
                break
        # 主链/完整文件略加分
        if "pkey=" in low or "clientcachekey" in low or "mainmv" in low:
            score += 8
        # 非 mp4 流略降（QQ 更爱 progressive mp4）
        if ".m3u8" in low:
            score -= 15
        # 更长 URL 常带更完整鉴权/更高档，弱加权
        score += min(len(url) // 80, 5)
    else:
        # 图片：大图/原图优先
        if any(x in low for x in ("original", "origin", "large", "orj1080", "orj960", "1080", "raw", "urldefault")):
            score += 60
        if any(x in low for x in ("orj480", "mw690", "thumbnail", "thumb", "small", "avatar", "face")):
            score -= 30
        if any(x in low for x in (".png", ".jpg", ".jpeg", ".webp")):
            score += 5
        score += min(len(url) // 100, 5)
    return score


def sort_media_urls_by_quality(urls: list[str], *, kind: str = "video") -> list[str]:
    """去重后按质量分从高到低排序。"""
    uniq = list(dict.fromkeys(u for u in urls if isinstance(u, str) and u.startswith("http")))
    return sorted(uniq, key=lambda u: (-media_quality_score(u, kind=kind), -len(u)))


# ---------- platform parsers ----------


def _extract_json_object_after(html: str, marker: str) -> Any | None:
    """从 html 中 marker 后的首个 { ... } 解析 JSON。"""
    idx = html.find(marker)
    if idx < 0:
        return None
    start = html.find("{", idx + len(marker))
    if start < 0:
        return None
    depth = 0
    in_str = False
    esc = False
    for j in range(start, min(start + 2_500_000, len(html))):
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
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(html[start : j + 1])
                except Exception:
                    return None
    return None


def _parse_bilibili_opus(url: str, final: str) -> dict[str, Any]:
    """B站图文动态/opus 图集：从页面 __INITIAL_STATE__ 抽 album/content 图片。"""
    meta = _base_meta("bilibili", url)
    meta["url"] = final
    m = re.search(r"/opus/(\d+)", final) or re.search(r"/opus/(\d+)", url)
    opus_id = m.group(1) if m else None
    page_urls = []
    if opus_id:
        page_urls.append(f"https://www.bilibili.com/opus/{opus_id}")
        page_urls.append(f"https://m.bilibili.com/opus/{opus_id}")
    page_urls.append(final)

    state = None
    for page in page_urls:
        try:
            _, html = _get_text(
                page,
                headers={
                    "User-Agent": _DESKTOP_UA,
                    "Referer": "https://www.bilibili.com/",
                },
                timeout=20,
            )
        except Exception as e:
            logger.debug(f"B站 opus 页面失败 {page}: {e}")
            continue
        state = _extract_json_object_after(html, "window.__INITIAL_STATE__")
        if isinstance(state, dict):
            meta["url"] = page
            break
    if not isinstance(state, dict):
        meta["error"] = "B站图文页未拿到 INITIAL_STATE"
        return meta

    detail = state.get("detail") if isinstance(state.get("detail"), dict) else state
    modules = detail.get("modules") if isinstance(detail, dict) else None
    if not isinstance(modules, list):
        modules = []

    images: list[str] = []
    title = ""
    author = ""
    avatar = ""
    ts = None
    texts: list[str] = []

    for mod in modules:
        if not isinstance(mod, dict):
            continue
        # album top
        top = ((mod.get("module_top") or {}).get("display") or {}).get("album") or {}
        for pic in top.get("pics") or []:
            if isinstance(pic, dict) and isinstance(pic.get("url"), str):
                images.append(pic["url"])
        # author
        ma = mod.get("module_author") or {}
        if isinstance(ma, dict) and ma.get("name"):
            author = str(ma.get("name") or author)
            avatar = str(ma.get("face") or avatar)
            try:
                ts = int(ma.get("pub_ts") or ts or 0) or ts
            except Exception:
                pass
        # content paragraphs
        mc = mod.get("module_content") or {}
        for para in mc.get("paragraphs") or []:
            if not isinstance(para, dict):
                continue
            pic = para.get("pic") or {}
            for p in pic.get("pics") or []:
                if isinstance(p, dict) and isinstance(p.get("url"), str):
                    images.append(p["url"])
            # text nodes
            text = para.get("text") or {}
            for node in text.get("nodes") or []:
                if not isinstance(node, dict):
                    continue
                if node.get("type") == "TEXT_NODE_TYPE_WORD":
                    w = (node.get("word") or {}).get("words")
                    if w:
                        texts.append(str(w))
                elif node.get("type") == "TEXT_NODE_TYPE_RICH":
                    rich = node.get("rich") or {}
                    w = rich.get("text") or rich.get("orig_text")
                    if w:
                        texts.append(str(w))
        # title sometimes in basic / module_title
        for key in ("module_title", "basic"):
            block = mod.get(key) or {}
            if isinstance(block, dict) and block.get("title"):
                title = str(block.get("title") or title)

    # fallback title from page state
    if not title:
        for cand in (
            ((detail.get("basic") or {}).get("title") if isinstance(detail, dict) else None),
            state.get("title"),
        ):
            if cand:
                title = str(cand)
                break
    if not title and texts:
        title = texts[0][:80]
    if not title:
        # last resort from HTML title handled earlier externally
        title = "B站图文"

    # clean images: keep article/album content, drop vip icons/app logo
    clean: list[str] = []
    seen: set[str] = set()
    for u in images:
        if not isinstance(u, str) or not u.startswith("http"):
            continue
        low = u.lower()
        if any(x in low for x in ("vip/", "app_logo", "emote", "/face/", "activity-plat")):
            continue
        key = u.split("@")[0].split("?")[0]
        if key in seen:
            continue
        seen.add(key)
        clean.append(u)

    meta["title"] = title.strip() or "B站图文"
    meta["author"] = author
    meta["desc"] = ("".join(texts).strip() or title)[:400]
    meta["avatar_url"] = avatar or None
    try:
        meta["timestamp"] = int(ts) if ts else None
    except Exception:
        meta["timestamp"] = None
    meta["image_urls"] = clean[:18]
    meta["video_urls"] = []
    if not meta["image_urls"]:
        meta["error"] = "B站图文未解析到图片"
    return meta


def parse_bilibili(url: str) -> dict[str, Any]:
    meta = _base_meta("bilibili", url)
    final = expand_url(url)
    meta["url"] = final

    # 图文动态 / opus 图集（无 BV 号）
    if "/opus/" in final or "/opus/" in url:
        return _parse_bilibili_opus(url, final)

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
            # 页面可能是 opus 跳转残留
            if not bvid and ("/opus/" in html or "window.__INITIAL_STATE__" in html):
                om = re.search(r"/opus/(\d+)", html) or re.search(r"/opus/(\d+)", final)
                if om:
                    return _parse_bilibili_opus(url, f"https://www.bilibili.com/opus/{om.group(1)}")
        except Exception as e:
            meta["error"] = f"B站页面读取失败：{e}"
            return meta
    if not bvid and not aid:
        # 再尝试按 opus 解析展开后的 URL
        if "opus" in final:
            return _parse_bilibili_opus(url, final)
        meta["error"] = "未能识别 B 站 BV/av/opus"
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
        if owner.get("face"):
            meta["avatar_url"] = str(owner.get("face"))
        # 发布时间
        try:
            ts = int(info.get("pubdate") or info.get("ctime") or 0)
            meta["timestamp"] = ts if ts > 0 else None
        except Exception:
            meta["timestamp"] = None
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
            # 多档清晰度：80(1080)→64(720)→32(480)→16，发送侧再按 20MB 降档
            videos: list[str] = []
            for qn in (80, 64, 32, 16):
                try:
                    play = http_client.get_json(
                        "https://api.bilibili.com/x/player/playurl",
                        params={
                            "avid": aid_n,
                            "cid": cid,
                            "qn": qn,
                            "fnval": 1,
                            "fourk": 1,
                        },
                        timeout=15,
                        headers={
                            "User-Agent": _DESKTOP_UA,
                            "Referer": final
                            if "bilibili.com" in final
                            else "https://www.bilibili.com",
                        },
                        use_config_proxy=False,
                    )
                except Exception as e:
                    logger.debug(f"B站 playurl qn={qn} 失败: {e}")
                    continue
                if int(play.get("code", -1)) != 0:
                    continue
                durl = ((play.get("data") or {}).get("durl") or [])
                for x in durl:
                    if x.get("url"):
                        videos.append(str(x["url"]))
                dash = ((play.get("data") or {}).get("dash") or {})
                for v in (dash.get("video") or [])[:3]:
                    if v.get("baseUrl"):
                        videos.append(str(v["baseUrl"]))
                    elif v.get("base_url"):
                        videos.append(str(v["base_url"]))
            meta["video_urls"] = sort_media_urls_by_quality(videos, kind="video")[:5]
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


def _find_kuaishou_photo(state: Any) -> dict[str, Any] | None:
    """在 INIT_STATE 中定位 photo 对象。"""
    found: dict[str, Any] | None = None

    def dig(o: Any) -> None:
        nonlocal found
        if found is not None:
            return
        if isinstance(o, dict):
            if (
                "caption" in o
                and "userName" in o
                and ("mainMvUrls" in o or "coverUrls" in o or "ext_params" in o)
            ):
                found = o
                return
            for v in o.values():
                dig(v)
        elif isinstance(o, list):
            for v in o[:80]:
                dig(v)

    dig(state)
    return found


def _kuaishou_cdn_urls(items: Any) -> list[str]:
    out: list[str] = []
    if not isinstance(items, list):
        return out
    for it in items:
        if isinstance(it, dict):
            u = it.get("url")
            if isinstance(u, str) and u.startswith("http"):
                out.append(u)
        elif isinstance(it, str) and it.startswith("http"):
            out.append(it)
    return out


def _kuaishou_atlas_urls(photo: dict[str, Any]) -> list[str]:
    """图集：ext_params.atlas.list + cdnList 拼完整 URL。"""
    ext = photo.get("ext_params") or {}
    if not isinstance(ext, dict):
        return []
    atlas = ext.get("atlas") or {}
    if not isinstance(atlas, dict):
        return []
    routes = atlas.get("list") or []
    cdns = atlas.get("cdnList") or atlas.get("cdn_list") or []
    if not routes:
        return []
    cdn_host = None
    if isinstance(cdns, list) and cdns:
        first = cdns[0]
        if isinstance(first, dict):
            cdn_host = first.get("cdn") or first.get("url")
        elif isinstance(first, str):
            cdn_host = first
    if not cdn_host:
        cdn_host = atlas.get("cdn")
    if not cdn_host:
        return []
    host = str(cdn_host).removeprefix("https://").removeprefix("http://").strip("/")
    urls: list[str] = []
    for route in routes:
        if not isinstance(route, str) or not route:
            continue
        if route.startswith("http"):
            urls.append(route)
        else:
            path = route if route.startswith("/") else f"/{route}"
            urls.append(f"https://{host}{path}")
    return urls


def parse_kuaishou(url: str) -> dict[str, Any]:
    """快手：INIT_STATE.photo

    - 视频：mainMvUrls（高清优先，只取 1 路）+ coverUrls 作封面
    - 图集：ext_params.atlas.list 全量图片（不是 coverUrls 那 1 张封面）
    - 附带 headUrl / timestamp 供卡片展示
    """
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
        return _parse_html_og("kuaishou", url)

    photo = _find_kuaishou_photo(state)
    if not isinstance(photo, dict):
        return _parse_html_og("kuaishou", final)

    caption = str(photo.get("caption") or "").strip()
    if caption in {"去快手享超清画质", "参与免费领道具！", "快手", "快手视频"}:
        caption = ""
    author = str(photo.get("userName") or "").replace("\u3164", "").strip()
    meta["title"] = caption or "快手内容"
    meta["author"] = author
    meta["desc"] = caption[:400] if caption else ""
    meta["avatar_url"] = str(photo.get("headUrl") or "") or None
    if not meta["avatar_url"]:
        heads = _kuaishou_cdn_urls(photo.get("headUrls"))
        meta["avatar_url"] = heads[0] if heads else None
    ts = photo.get("timestamp")
    try:
        ts_i = int(ts)
        # 毫秒 → 秒
        if ts_i > 10_000_000_000:
            ts_i //= 1000
        meta["timestamp"] = ts_i
    except Exception:
        meta["timestamp"] = None

    videos = _kuaishou_cdn_urls(photo.get("mainMvUrls"))
    covers = _kuaishou_cdn_urls(photo.get("coverUrls")) or _kuaishou_cdn_urls(
        photo.get("webpCoverUrls")
    )
    atlas = _kuaishou_atlas_urls(photo)
    single_pic = bool(photo.get("singlePicture"))
    photo_type = str(photo.get("photoType") or photo.get("type") or "")

    # 保留多档清晰度（高清优先排序），发送侧按 20MB 降档
    videos = sort_media_urls_by_quality(videos, kind="video")

    # 图集优先用 atlas 全量；视频作品封面只留最佳 1 张
    is_atlas = bool(atlas) or single_pic or "ATLAS" in photo_type.upper()
    if is_atlas and atlas:
        meta["video_urls"] = []
        meta["image_urls"] = sort_media_urls_by_quality(
            list(dict.fromkeys(atlas)), kind="image"
        )[:12]
        if not meta["image_urls"] and covers:
            meta["image_urls"] = covers[:1]
    else:
        meta["video_urls"] = videos[:5]
        meta["image_urls"] = (covers[:1] if covers else [])

    if not meta["video_urls"] and not meta["image_urls"]:
        fallback = _parse_html_og("kuaishou", final)
        if fallback.get("video_urls") or fallback.get("image_urls"):
            fallback["source_url"] = url
            fallback.setdefault("avatar_url", meta.get("avatar_url"))
            fallback.setdefault("timestamp", meta.get("timestamp"))
            return fallback
        meta["error"] = "快手未解析到可发送媒体（可能风控/登录）"
    return meta


def parse_xiaohongshu(url: str) -> dict[str, Any]:
    """小红书：参考上游，从 __INITIAL_STATE__ 抽 noteDetailMap / noteData。

    国内直连常不可达时，若配置了 custom_proxy 则走代理重试。
    """
    meta = _base_meta("xiaohongshu", url)
    # 小红书在部分机房直连不通，有代理时优先代理
    use_proxy = bool(get_custom_proxy_url())
    final = expand_url(url, use_proxy=use_proxy, platform="xiaohongshu")
    meta["url"] = final

    m = re.search(r"/(?:explore|discovery/item)/([0-9a-zA-Z]+)", final) or re.search(
        r"/(?:explore|discovery/item)/([0-9a-zA-Z]+)", url
    )
    note_id = m.group(1) if m else None
    pages: list[str] = []
    if note_id:
        # 保留 query（含 xsec_token 时更稳）
        if "xiaohongshu.com" in final and note_id in final:
            pages.append(final)
        pages.append(f"https://www.xiaohongshu.com/explore/{note_id}")
        pages.append(f"https://www.xiaohongshu.com/discovery/item/{note_id}")
    else:
        pages.append(final)

    state = None
    for page in pages:
        for proxy_flag in ((True, False) if use_proxy else (False,)):
            try:
                _, html = _get_text(
                    page,
                    headers={
                        "User-Agent": _DESKTOP_UA if not proxy_flag else _MOBILE_UA,
                        "Referer": "https://www.xiaohongshu.com/",
                        "Accept": "text/html,application/xhtml+xml",
                    },
                    timeout=20,
                    use_proxy=proxy_flag,
                )
            except Exception as e:
                logger.debug(f"小红书页面失败 proxy={proxy_flag} {page}: {e}")
                continue
            state = _extract_json_object_after(html, "window.__INITIAL_STATE__")
            if isinstance(state, dict):
                # 兼容 undefined 已被 json 解析失败的情况：再做一次文本替换
                meta["url"] = page
                break
            # 有时 JSON 含 undefined，手动修
            try:
                idx = html.find("window.__INITIAL_STATE__")
                if idx >= 0:
                    raw = _extract_json_object_after(
                        html.replace("undefined", "null"), "window.__INITIAL_STATE__"
                    )
                    if isinstance(raw, dict):
                        state = raw
                        meta["url"] = page
                        break
            except Exception:
                pass
        if isinstance(state, dict):
            break

    note: dict[str, Any] | None = None
    if isinstance(state, dict):
        # PC explore: note.noteDetailMap[id].note
        ndm = ((state.get("note") or {}).get("noteDetailMap") or {}) if isinstance(
            state.get("note"), dict
        ) else {}
        if note_id and isinstance(ndm, dict) and note_id in ndm:
            node = ndm.get(note_id) or {}
            note = node.get("note") if isinstance(node, dict) else None
            if not isinstance(note, dict):
                note = node if isinstance(node, dict) else None
        if not note and isinstance(ndm, dict) and ndm:
            first = next(iter(ndm.values()))
            if isinstance(first, dict):
                note = first.get("note") if isinstance(first.get("note"), dict) else first
        # mobile discovery: noteData.data.noteData
        if not note:
            nd = state.get("noteData") or {}
            if isinstance(nd, dict):
                data = nd.get("data") or {}
                if isinstance(data, dict):
                    cand = data.get("noteData") or data.get("note") or data
                    if isinstance(cand, dict) and (
                        cand.get("imageList") or cand.get("title") or cand.get("video")
                    ):
                        note = cand
                preload = nd.get("normalNotePreloadData")
                if not note and isinstance(preload, dict):
                    note = preload

    if isinstance(note, dict):
        meta["title"] = str(note.get("title") or note.get("desc") or "")[:200]
        meta["desc"] = str(note.get("desc") or "")[:400]
        user = note.get("user") or note.get("userInfo") or {}
        if isinstance(user, dict):
            meta["author"] = str(
                user.get("nickname") or user.get("nickName") or user.get("name") or ""
            )
            meta["avatar_url"] = (
                user.get("avatar") or user.get("avatarUrl") or user.get("images")
            )
        # images
        images: list[str] = []
        for im in note.get("imageList") or note.get("imagesList") or []:
            if isinstance(im, dict):
                u = (
                    im.get("urlDefault")
                    or im.get("urlSizeLarge")
                    or im.get("url")
                    or im.get("infoList", [{}])[0].get("url")
                    if isinstance(im.get("infoList"), list) and im.get("infoList")
                    else None
                )
                if isinstance(u, str) and u.startswith("http"):
                    images.append(u)
            elif isinstance(im, str) and im.startswith("http"):
                images.append(im)
        # video
        videos: list[str] = []
        video = note.get("video") or {}
        if isinstance(video, dict):
            media = video.get("media") or {}
            stream = media.get("stream") or video.get("stream") or {}
            # h264 list
            for key in ("h264", "h265", "av1", "h266"):
                for item in (stream.get(key) or []) if isinstance(stream, dict) else []:
                    if isinstance(item, dict):
                        for kk in ("masterUrl", "master_url", "url", "backupUrls"):
                            val = item.get(kk)
                            if isinstance(val, str) and val.startswith("http"):
                                videos.append(val)
                            elif isinstance(val, list):
                                videos.extend(
                                    [x for x in val if isinstance(x, str) and x.startswith("http")]
                                )
            # consumer originVideoKey sometimes
            consumer = video.get("consumer") or {}
            if isinstance(consumer, dict):
                ovk = consumer.get("originVideoKey")
                if isinstance(ovk, str) and ovk.startswith("http"):
                    videos.append(ovk)
            for kk in ("url", "videoUrl", "masterUrl"):
                if isinstance(video.get(kk), str) and video[kk].startswith("http"):
                    videos.append(video[kk])
        meta["image_urls"] = list(dict.fromkeys(images))[:12]
        # 小红书 CDN 用 https 更稳
        meta["image_urls"] = [
            u.replace("http://", "https://", 1) if u.startswith("http://") else u
            for u in meta["image_urls"]
        ]
        meta["video_urls"] = list(dict.fromkeys(videos))[:3]
        if meta["image_urls"] or meta["video_urls"]:
            meta["error"] = None
            return meta
        if meta.get("title"):
            meta["error"] = "小红书拿到标题但无媒体（可能需 xsec_token/登录）"
            return meta

    # 回退 OG（代理）
    page = _parse_html_og("xiaohongshu", final, use_proxy=use_proxy)
    if page.get("video_urls") or page.get("image_urls"):
        return page
    meta["error"] = page.get("error") or "小红书未解析到媒体（直连失败或页面无 note 数据）"
    if not use_proxy:
        meta["error"] = "小红书建议配置 custom_proxy；" + str(meta["error"])
    return meta


def _weibo_strip_html(text: str) -> str:
    text = (text or "").replace("<br />", "\n").replace("<br/>", "\n")
    text = re.sub(r"<[^>]+>", "", text)
    return text.replace("\u200b", "").strip()


def _weibo_fill_from_status(meta: dict[str, Any], data: dict[str, Any]) -> None:
    """从 m.weibo.cn statuses/show 的 data 填充媒体。含转发链。"""
    if not isinstance(data, dict):
        return
    # 优先当前层，否则吃 retweeted_status
    layers = [data]
    if isinstance(data.get("retweeted_status"), dict):
        layers.append(data["retweeted_status"])

    images: list[str] = list(meta.get("image_urls") or [])
    videos: list[str] = list(meta.get("video_urls") or [])
    title = meta.get("title") or ""
    desc = meta.get("desc") or ""
    author = meta.get("author") or ""
    avatar = meta.get("avatar_url")
    ts = meta.get("timestamp")

    for layer in layers:
        user = layer.get("user") or {}
        if isinstance(user, dict) and not author:
            author = str(user.get("screen_name") or "")
            avatar = user.get("profile_image_url") or avatar
        text = _weibo_strip_html(str(layer.get("text") or ""))
        if text and not desc:
            desc = text[:400]
        if not title:
            title = str(layer.get("status_title") or text[:80] or "")
        for pic in layer.get("pics") or []:
            if not isinstance(pic, dict):
                continue
            large = pic.get("large") or {}
            u = (large.get("url") if isinstance(large, dict) else None) or pic.get("url")
            if isinstance(u, str) and u.startswith("http"):
                images.append(u)
        page = layer.get("page_info") or {}
        if isinstance(page, dict):
            if page.get("title") and not title:
                title = str(page.get("title"))
            urls = page.get("urls") or {}
            if isinstance(urls, dict):
                for key in ("mp4_720p_mp4", "mp4_hd_mp4", "mp4_ld_mp4"):
                    v = urls.get(key)
                    if isinstance(v, str) and v:
                        if v.startswith("//"):
                            v = "https:" + v
                        elif not v.startswith("http"):
                            v = "https:" + v
                        videos.append(v)
            # stream_url fallback
            for key in ("stream_url_hd", "stream_url", "media_info"):
                v = page.get(key)
                if isinstance(v, str) and ".mp4" in v:
                    videos.append(v if v.startswith("http") else "https:" + v)
                if isinstance(v, dict):
                    for kk in ("stream_url_hd", "stream_url", "mp4_720p_mp4", "mp4_hd_mp4"):
                        vv = v.get(kk)
                        if isinstance(vv, str) and vv:
                            videos.append(vv if vv.startswith("http") else "https:" + vv)
            pic = page.get("page_pic") or {}
            if isinstance(pic, dict) and isinstance(pic.get("url"), str):
                images.append(pic["url"])
        # created_at: "Tue Nov 18 16:19:12 +0800 2025"
        if not ts and layer.get("created_at"):
            try:
                from email.utils import parsedate_to_datetime

                ts = int(parsedate_to_datetime(str(layer["created_at"])).timestamp())
            except Exception:
                pass

    meta["title"] = (title or desc or "微博")[:200]
    meta["desc"] = (desc or title)[:400]
    meta["author"] = author
    meta["avatar_url"] = avatar
    meta["timestamp"] = ts
    meta["image_urls"] = sort_media_urls_by_quality(images, kind="image")[:12]
    # 微博多档：720p > hd > ld
    meta["video_urls"] = sort_media_urls_by_quality(videos, kind="video")[:5]


def parse_weibo(url: str) -> dict[str, Any]:
    """微博：m.weibo.cn/statuses/show 接口（对齐上游），支持图/视频/转发。"""
    meta = _base_meta("weibo", url)
    final = expand_url(url, use_proxy=False, platform="weibo")
    meta["url"] = final

    wid = None
    for pat in (
        r"weibo\.com/\d+/([0-9a-zA-Z]+)",
        r"weibo\.cn/(?:status|detail|\d+)/([0-9a-zA-Z]+)",
        r"[?&]id=([0-9a-zA-Z]+)",
        r"/([0-9a-zA-Z]{6,})$",
    ):
        m = re.search(pat, final) or re.search(pat, url)
        if m:
            wid = m.group(1)
            break
    # mid numeric from tv show
    if not wid:
        m = re.search(r"[?&]mid=(\d+)", final) or re.search(r"[?&]mid=(\d+)", url)
        if m:
            wid = m.group(1)

    if wid:
        import time as _time

        api = f"https://m.weibo.cn/statuses/show?id={wid}&_={int(_time.time() * 1000)}"
        try:
            resp = http_client.request(
                "GET",
                api,
                timeout=15,
                check_status=False,
                use_config_proxy=False,
                headers={
                    "User-Agent": _MOBILE_UA,
                    "Accept": "application/json, text/plain, */*",
                    "Referer": f"https://m.weibo.cn/detail/{wid}",
                    "Origin": "https://m.weibo.cn",
                    "X-Requested-With": "XMLHttpRequest",
                    "MWeibo-PWA": "1",
                },
            )
            raw = getattr(resp, "text", "") or ""
            data = json.loads(raw) if raw.strip().startswith("{") else {}
            if int(data.get("ok") or 0) == 1 and isinstance(data.get("data"), dict):
                _weibo_fill_from_status(meta, data["data"])
                if meta.get("video_urls") or meta.get("image_urls") or meta.get("title"):
                    meta["error"] = None
                    return meta
            else:
                meta["error"] = f"微博接口：{data.get('msg') or data.get('errno') or 'ok!=1'}"
        except Exception as e:
            meta["error"] = f"微博接口失败：{e}"

    # video.weibo.com/show?fid=
    m = re.search(r"fid=(\d+:\d+)", final) or re.search(r"fid=(\d+:\d+)", url)
    if m:
        fid = m.group(1)
        try:
            req_url = f"https://h5.video.weibo.com/api/component?page=/show/{fid}"
            post = 'data={"Component_Play_Playinfo":{"oid":"' + fid + '"}}'
            resp = http_client.request(
                "POST",
                req_url,
                data=post,
                timeout=15,
                check_status=False,
                use_config_proxy=False,
                headers={
                    "User-Agent": _MOBILE_UA,
                    "Referer": f"https://h5.video.weibo.com/show/{fid}",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            raw = getattr(resp, "text", "") or ""
            data = json.loads(raw) if raw.strip().startswith("{") else {}
            info = ((data.get("data") or {}).get("Component_Play_Playinfo") or {})
            if isinstance(info, dict) and info:
                meta["title"] = str(info.get("title") or "")[:200]
                text = re.sub(r"<[^>]+>", "", str(info.get("text") or ""))
                meta["desc"] = text[:400]
                user = ((info.get("reward") or {}).get("user") or {})
                if isinstance(user, dict):
                    meta["author"] = str(user.get("name") or "")
                    meta["avatar_url"] = user.get("profile_image_url")
                cover = info.get("cover_image")
                if isinstance(cover, str) and cover:
                    if cover.startswith("//"):
                        cover = "https:" + cover
                    meta["image_urls"] = [cover]
                urls = info.get("urls") or {}
                vids = []
                if isinstance(urls, dict):
                    for v in urls.values():
                        if isinstance(v, str) and v:
                            vids.append(v if v.startswith("http") else "https:" + v)
                if not vids and info.get("stream_url"):
                    v = info["stream_url"]
                    vids.append(v if str(v).startswith("http") else "https:" + str(v))
                meta["video_urls"] = list(dict.fromkeys(vids))[:3]
                if meta["video_urls"] or meta["image_urls"]:
                    meta["error"] = None
                    return meta
        except Exception as e:
            logger.debug(f"微博视频 fid 接口失败: {e}")

    page = _parse_html_og("weibo", final, use_proxy=False)
    if page.get("video_urls") or page.get("image_urls"):
        return page
    if not meta.get("error"):
        meta["error"] = page.get("error") or "微博未解析到媒体"
    return meta


def _ytdlp_extract(url: str, *, use_proxy: bool = True) -> dict[str, Any] | None:
    """可选依赖 yt-dlp；失败返回 None。"""
    try:
        import yt_dlp  # type: ignore
    except Exception:
        return None
    proxy = get_custom_proxy_url() if use_proxy else None
    opts: dict[str, Any] = {
        "quiet": True,
        "noprogress": True,
        "skip_download": True,
        "noplaylist": True,
        "socket_timeout": 20,
        "retries": 0,
        "fragment_retries": 0,
        "extractor_retries": 0,
    }
    if proxy:
        opts["proxy"] = proxy
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:  # type: ignore
            info = ydl.extract_info(url, download=False)
        return info if isinstance(info, dict) else None
    except Exception as e:
        logger.debug(f"yt-dlp 失败 {url}: {e}")
        return None


def _meta_from_ytdlp(platform: str, url: str, info: dict[str, Any]) -> dict[str, Any]:
    meta = _base_meta(platform, url)
    meta["title"] = str(info.get("title") or "")[:200]
    meta["author"] = str(
        info.get("uploader") or info.get("channel") or info.get("creator") or ""
    )
    meta["desc"] = str(info.get("description") or "")[:400]
    if info.get("thumbnail"):
        meta["image_urls"] = [str(info["thumbnail"])]
    ts = info.get("timestamp") or info.get("release_timestamp")
    try:
        meta["timestamp"] = int(ts) if ts else None
    except Exception:
        meta["timestamp"] = None

    videos: list[str] = []
    audios: list[str] = []
    # 收集带 height 的 format，按清晰度排序
    ranked: list[tuple[int, int, str]] = []  # (-height, -tbr, url)
    if isinstance(info.get("url"), str) and info["url"].startswith("http"):
        if info.get("vcodec") not in (None, "none") or any(
            x in info["url"].lower() for x in (".mp4", ".m3u8", ".webm")
        ):
            h = int(info.get("height") or 0)
            tbr = int(info.get("tbr") or info.get("vbr") or 0)
            ranked.append((-h, -tbr, info["url"]))
        elif info.get("acodec") not in (None, "none"):
            audios.append(info["url"])
    for fmt in info.get("formats") or []:
        if not isinstance(fmt, dict):
            continue
        u = fmt.get("url")
        if not isinstance(u, str) or not u.startswith("http"):
            continue
        protocol = str(fmt.get("protocol") or "http")
        if not protocol.startswith("http"):
            continue
        vcodec = fmt.get("vcodec")
        acodec = fmt.get("acodec")
        if vcodec not in (None, "none"):
            # 优先 progressive 容器
            if str(fmt.get("ext") or "").lower() in ("mp4", "m4v", "webm") or "mp4" in u:
                h = int(fmt.get("height") or 0)
                tbr = int(fmt.get("tbr") or fmt.get("vbr") or 0)
                ranked.append((-h, -tbr, u))
        elif acodec not in (None, "none"):
            audios.append(u)
    ranked.sort()
    videos = [u for _, __, u in ranked]
    # playlist entry
    if info.get("_type") == "playlist":
        for entry in info.get("entries") or []:
            if isinstance(entry, dict):
                sub = _meta_from_ytdlp(platform, url, entry)
                videos = list(sub.get("video_urls") or []) + videos
                audios.extend(sub.get("audio_urls") or [])
                if not meta.get("image_urls") and sub.get("image_urls"):
                    meta["image_urls"] = sub["image_urls"]
                if not meta.get("title") and sub.get("title"):
                    meta["title"] = sub["title"]
                break
    meta["video_urls"] = sort_media_urls_by_quality(videos, kind="video")[:5]
    meta["audio_urls"] = list(dict.fromkeys(audios))[:2]
    if not meta["video_urls"] and not meta["audio_urls"] and not meta["image_urls"]:
        meta["error"] = "yt-dlp 未提取到可发送媒体"
    return meta


def parse_youtube(url: str) -> dict[str, Any]:
    """YouTube：yt-dlp 提取（需代理/cookie 时按环境）。"""
    meta = _base_meta("youtube", url)
    use_proxy = _use_proxy_for("youtube", url) or bool(get_custom_proxy_url())
    final = expand_url(url, use_proxy=use_proxy, platform="youtube")
    meta["url"] = final
    info = _ytdlp_extract(final, use_proxy=use_proxy)
    if info:
        out = _meta_from_ytdlp("youtube", url, info)
        out["source_url"] = url
        out["url"] = final
        if out.get("video_urls") or out.get("audio_urls") or out.get("image_urls"):
            return out
        meta["error"] = out.get("error") or "YouTube 未解析到媒体"
    else:
        meta["error"] = "YouTube 解析失败（需 yt-dlp，且可能需代理/cookie）"
    # 不二次反转代理硬重试（机房直连 YouTube 常不可达，会拖很久）
    page = _parse_html_og("youtube", final, use_proxy=use_proxy)
    if page.get("image_urls") and not meta.get("image_urls"):
        meta["image_urls"] = page["image_urls"]
        meta["title"] = meta.get("title") or page.get("title") or ""
    return meta


def parse_tiktok(url: str) -> dict[str, Any]:
    """TikTok：优先 yt-dlp，再 OG。"""
    meta = _base_meta("tiktok", url)
    use_proxy = _use_proxy_for("tiktok", url) or bool(get_custom_proxy_url())
    final = expand_url(url, use_proxy=use_proxy, platform="tiktok")
    meta["url"] = final
    info = _ytdlp_extract(final, use_proxy=use_proxy)
    if info:
        out = _meta_from_ytdlp("tiktok", url, info)
        out["source_url"] = url
        out["url"] = final
        if out.get("video_urls") or out.get("image_urls"):
            return out
        meta["error"] = out.get("error")
    page = _parse_html_og("tiktok", final, use_proxy=use_proxy)
    if page.get("video_urls") or page.get("image_urls"):
        return page
    meta["error"] = meta.get("error") or page.get("error") or "TikTok 未解析到媒体"
    return meta


def parse_instagram(url: str) -> dict[str, Any]:
    """Instagram：yt-dlp 回退 + OG。"""
    meta = _base_meta("instagram", url)
    use_proxy = _use_proxy_for("instagram", url) or bool(get_custom_proxy_url())
    final = expand_url(url, use_proxy=use_proxy, platform="instagram")
    meta["url"] = final
    info = _ytdlp_extract(final, use_proxy=use_proxy)
    if info:
        out = _meta_from_ytdlp("instagram", url, info)
        out["source_url"] = url
        out["url"] = final
        if out.get("video_urls") or out.get("image_urls"):
            return out
        meta["error"] = out.get("error")
    page = _parse_html_og("instagram", final, use_proxy=use_proxy)
    if page.get("video_urls") or page.get("image_urls"):
        return page
    meta["error"] = meta.get("error") or page.get("error") or "Instagram 未解析到媒体（常需登录 cookie）"
    return meta


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
        # 若只有 title 无媒体，再试 yt-dlp
        if page.get("video_urls") or page.get("image_urls"):
            return page
    # 3) yt-dlp 回退（部分视频推文 xdown 不稳）
    info = _ytdlp_extract(query_url, use_proxy=bool(use_proxy or get_custom_proxy_url()))
    if info:
        out = _meta_from_ytdlp("twitter", url, info)
        out["source_url"] = url
        out["url"] = final
        if out.get("video_urls") or out.get("image_urls"):
            return out
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
    "tiktok": parse_tiktok,
    "instagram": parse_instagram,
    "youtube": parse_youtube,
}


def parse_url(url: str, platform: str | None = None) -> dict[str, Any]:
    plat = platform or detect_platform(url) or "unknown"
    fn = _PARSERS.get(plat)
    if fn:
        meta = fn(url)
    elif plat != "unknown":
        meta = parse_generic(plat, url)
    else:
        meta = _base_meta("unknown", url)
        meta["error"] = "不支持的平台"
    # 始终保留用户输入的原始短链，供文案展示；展开落地页放在 url 字段
    if isinstance(meta, dict):
        meta["source_url"] = url
        meta.setdefault("url", url)
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
