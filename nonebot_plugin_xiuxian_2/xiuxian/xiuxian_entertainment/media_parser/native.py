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
    ("tiktokv.com", "tiktok"),
    ("youtu.be", "youtube"),
    ("youtube.com", "youtube"),
    ("goofish.com", "xianyu"),
    ("instagram.com", "instagram"),
]

# 仅海外站在开启 custom_proxy 时走代理；国内站一律直连
_PROXY_PLATFORMS = frozenset({"twitter", "tiktok", "youtube"})

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
    allow_redirects: bool = True,
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
        allow_redirects=allow_redirects,
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
    """跟随短链，失败则返回原 URL。

    小红书 xhslink：只取 302 Location，不自动跟到 www（部分机房 www 不可达会卡死）。
    """
    if use_proxy is None:
        use_proxy = _use_proxy_for(platform, url)
    try:
        host = (urlparse(url).hostname or "").lower()
        # xhslink 短链：禁止自动跳 www，避免 Network unreachable 拖满超时
        if "xhslink.com" in host:
            resp = _http_get(
                url,
                headers={"User-Agent": _MOBILE_UA, "Accept": "*/*"},
                timeout=min(int(timeout), 10),
                use_proxy=bool(use_proxy),
                allow_redirects=False,
            )
            loc = ""
            try:
                loc = str((getattr(resp, "headers", {}) or {}).get("Location") or "")
            except Exception:
                loc = ""
            if loc.startswith("http"):
                return loc
            final = str(getattr(resp, "url", "") or "")
            return final or url

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
        # 图片：大图/原图优先；抖音图集无水印 > 水印；更高分辨率参数优先
        if any(x in low for x in ("original", "origin", "large", "orj1080", "orj960", "1080", "raw", "urldefault")):
            score += 60
        if any(x in low for x in ("orj480", "mw690", "thumbnail", "thumb", "small", "avatar", "face", "resize-walign")):
            score -= 30
        if "water" in low or "watermark" in low:
            score -= 40
        # 路径里 :宽:高:q80 这类尺寸
        m = re.search(r":(\d{3,4}):(\d{3,4}):", low)
        if m:
            try:
                score += min(int(m.group(1)) * int(m.group(2)) // 50000, 40)
            except Exception:
                pass
        if any(x in low for x in (".png", ".jpg", ".jpeg", ".webp")):
            score += 5
        if low.endswith(".webp") or ".webp?" in low:
            score += 2
        score += min(len(url) // 100, 5)
    return score


def _media_object_key(url: str) -> str:
    """同一资源在多 CDN/多清晰度下的稳定键。

    抖音图集常见：同 object id 有 p3/p5/p11 多域名 + webp/jpeg + 水印/无水印。
    去 query 后取 path 中 object 段（~ 模板前）作为主键。
    """
    if not isinstance(url, str) or not url.startswith("http"):
        return ""
    try:
        path = urlparse(url).path or ""
    except Exception:
        return url
    # /tos-.../OBJECT~tplv-... 或 /.../OBJECT.webp
    name = path.rsplit("/", 1)[-1]
    if "~" in name:
        name = name.split("~", 1)[0]
    # 去掉纯扩展名
    name = re.sub(r"\.(?:jpg|jpeg|png|webp|gif|bmp)$", "", name, flags=re.I)
    return name or path


def dedupe_media_urls_by_object(urls: list[str], *, kind: str = "image") -> list[str]:
    """按资源对象去重：每个 object 只保留质量最高的一条 URL。"""
    best: dict[str, str] = {}
    order: list[str] = []
    for u in urls:
        if not isinstance(u, str) or not u.startswith("http"):
            continue
        key = _media_object_key(u)
        if not key:
            key = u
        if key not in best:
            best[key] = u
            order.append(key)
            continue
        old = best[key]
        if media_quality_score(u, kind=kind) > media_quality_score(old, kind=kind):
            best[key] = u
        elif media_quality_score(u, kind=kind) == media_quality_score(old, kind=kind) and len(u) > len(old):
            best[key] = u
    return [best[k] for k in order]


def sort_media_urls_by_quality(urls: list[str], *, kind: str = "video") -> list[str]:
    """对象级去重后按质量分从高到低排序。"""
    # 视频仍按完整 URL 去重即可；图片按 object 去重避免同图多档重复发
    if kind == "image":
        uniq = dedupe_media_urls_by_object(urls, kind="image")
    else:
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

    # 2) share 页抽 _ROUTER_DATA（图集优先 note 页，视频优先 video 页）
    share_urls: list[str] = []
    is_note_share = bool(re.search(r"/share/note/|/note/\d+", cur or "")) or (
        "type=note" in (cur or "").lower()
    )
    if aweme_id:
        note_u = f"https://www.iesdouyin.com/share/note/{aweme_id}/"
        video_u = f"https://www.iesdouyin.com/share/video/{aweme_id}/"
        m_note_u = f"https://m.douyin.com/share/note/{aweme_id}/"
        m_video_u = f"https://m.douyin.com/share/video/{aweme_id}/"
        if is_note_share:
            share_urls.extend([note_u, m_note_u, video_u, m_video_u])
        else:
            share_urls.extend([video_u, note_u, m_video_u, m_note_u])
    if cur and cur not in share_urls:
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

            # 图集 aweme_type=2/68 等：优先 images，勿把配乐 play 链当视频
            aweme_type = item.get("aweme_type")
            try:
                aweme_type_i = int(aweme_type) if aweme_type is not None else None
            except (TypeError, ValueError):
                aweme_type_i = None
            is_atlas = aweme_type_i in {2, 42, 68} or bool(item.get("images"))

            images: list[str] = []
            for im in item.get("images") or item.get("image_list") or []:
                if isinstance(im, dict):
                    for key in ("url_list", "download_url_list"):
                        for x in im.get(key) or []:
                            if isinstance(x, str) and x.startswith("http"):
                                images.append(x)
                    for nest in ("display_image", "owner_watermark_image", "thumbnail", "download_addr"):
                        node = im.get(nest) or {}
                        if isinstance(node, dict):
                            for x in node.get("url_list") or []:
                                if isinstance(x, str) and x.startswith("http"):
                                    images.append(x)
                elif isinstance(im, str) and im.startswith("http"):
                    images.append(im)

            video = item.get("video") or {}
            play = video.get("play_addr") or video.get("play_addr_h264") or {}
            urls = list(play.get("url_list") or []) if isinstance(play, dict) else []
            # play token -> snssdk play endpoint（仅非图集）
            uri = play.get("uri") if isinstance(play, dict) else None
            if not is_atlas:
                if not uri and urls:
                    for u in urls:
                        qs = parse_qs(urlparse(u).query)
                        if qs.get("video_id"):
                            uri = qs["video_id"][0]
                            break
                if uri and not str(uri).startswith("http"):
                    for ratio in ("1080p", "720p", "540p", "360p"):
                        play_u = (
                            f"https://aweme.snssdk.com/aweme/v1/play/"
                            f"?video_id={uri}&ratio={ratio}&line=0"
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
                if not (isinstance(u, str) and u.startswith("http")):
                    continue
                low = u.lower()
                # 图集配乐 / 非视频链过滤
                if is_atlas and (".mp3" in low or "ies-music" in low or "music" in low):
                    continue
                clean.append(u.replace("playwm", "play"))

            # 封面仅作无图时的兜底，图集以 images 为准
            if not images:
                cover = video.get("cover") or video.get("origin_cover") or {}
                if isinstance(cover, dict):
                    images.extend(
                        [x for x in (cover.get("url_list") or []) if isinstance(x, str)]
                    )

            # 去头像/表情噪声；同图多 CDN/多清晰度/水印只留 1 条
            filtered_imgs = []
            for u in images:
                if not isinstance(u, str) or not u.startswith("http"):
                    continue
                low = u.lower()
                if any(x in low for x in ("avatar", "aweme-avatar", "emotion", "emoji")):
                    continue
                filtered_imgs.append(u)

            meta["video_urls"] = [] if is_atlas else list(dict.fromkeys(clean))[:3]
            # 先按对象去重再截断：图集最多 18 张「内容图」
            deduped = dedupe_media_urls_by_object(filtered_imgs, kind="image")
            img_cap = 18 if is_atlas else 6
            meta["image_urls"] = deduped[:img_cap]
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
    """小红书：从页面 __INITIAL_STATE__ 抽 noteDetailMap / noteData。

    国内直连：不走 custom_proxy（代理仅给国外站）。
    注意：部分机房对 www.xiaohongshu.com 不可达；短链只取 Location，页面请求超时压短，
    避免整条解析卡满 60s。图集/视频同一套页面解析，差异主要在 note 结构字段。
    """
    meta = _base_meta("xiaohongshu", url)
    # 国内站：强制直连
    use_proxy = False
    final = expand_url(url, use_proxy=False, platform="xiaohongshu")
    meta["url"] = final

    m = re.search(r"/(?:explore|discovery/item)/([0-9a-zA-Z]+)", final) or re.search(
        r"/(?:explore|discovery/item)/([0-9a-zA-Z]+)", url
    )
    note_id = m.group(1) if m else None
    # 保留 xsec_token 等 query（无 token 时 note 数据常为空）
    q = urlparse(final if "xsec_token=" in final else url).query
    q_suffix = f"?{q}" if q else ""
    pages: list[str] = []
    if note_id:
        if "xiaohongshu.com" in final and note_id in final:
            pages.append(final)
        # 优先 discovery/item（分享链常见），再 explore
        pages.append(f"https://www.xiaohongshu.com/discovery/item/{note_id}{q_suffix}")
        pages.append(f"https://www.xiaohongshu.com/explore/{note_id}{q_suffix}")
    else:
        pages.append(final)
    pages = list(dict.fromkeys(pages))

    state = None
    for page in pages:
        try:
            _, html = _get_text(
                page,
                headers={
                    "User-Agent": _DESKTOP_UA,
                    "Referer": "https://www.xiaohongshu.com/",
                    "Accept": "text/html,application/xhtml+xml",
                },
                # www 不可达时快速失败，避免拖死整次解析
                timeout=8,
                use_proxy=False,
            )
        except Exception as e:
            logger.debug(f"小红书页面失败 {page}: {e}")
            continue
        state = _extract_json_object_after(html, "window.__INITIAL_STATE__")
        if isinstance(state, dict):
            meta["url"] = page
            break
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

    # 回退 OG
    page = _parse_html_og("xiaohongshu", final, use_proxy=False)
    if page.get("video_urls") or page.get("image_urls"):
        return page
    # 本机若 www 不可达，短链虽能展开也会失败；给出可定位原因
    err = page.get("error") or "小红书未解析到媒体（页面无 note 数据或 www 不可达）"
    if "www.xiaohongshu.com" in (final or url or ""):
        err = "本机无法访问 www.xiaohongshu.com（Network unreachable），图集/视频页面均拉不到；" + str(err)
    meta["error"] = err
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
    """TikTok：优先 yt-dlp，再 OG / 页面 playAddr。

    兼容 app-va.tiktokv.com / snssdk 跳转链：从路径抽 aweme id 归一成 www.tiktok.com 视频页。
    """
    meta = _base_meta("tiktok", url)
    use_proxy = _use_proxy_for("tiktok", url) or bool(get_custom_proxy_url())

    # 跳转链/深链里常夹着 aweme id：.../aweme/detail/7631... 或 video/7631...
    aweme_id = None
    m = re.search(r"(?:aweme/detail|video|photo)/(\d{10,})", url) or re.search(
        r"[?&](?:item_id|aweme_id|id)=(\d{10,})", url
    )
    if m:
        aweme_id = m.group(1)
    # 也从 URL 解码后的 query 再扫一次
    if not aweme_id:
        try:
            from urllib.parse import unquote

            decoded = unquote(url)
            m = re.search(r"(?:aweme/detail|video|photo)/(\d{10,})", decoded)
            if m:
                aweme_id = m.group(1)
        except Exception:
            pass

    seed = url
    if aweme_id and ("tiktokv.com" in (urlparse(url).hostname or "").lower() or "snssdk" in url.lower()):
        seed = f"https://www.tiktok.com/@/video/{aweme_id}"

    final = expand_url(seed, use_proxy=use_proxy, platform="tiktok")
    # expand 失败仍可用归一后的 seed
    if aweme_id and ("tiktok.com" not in (urlparse(final).hostname or "").lower()):
        final = f"https://www.tiktok.com/@/video/{aweme_id}"
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

    # yt-dlp 常 403；OG 也常无 og:video。直接从 HTML 抽 playAddr/downloadAddr
    try:
        _, html = _get_text(
            final,
            headers={
                "User-Agent": _MOBILE_UA,
                "Referer": "https://www.tiktok.com/",
                "Accept": "text/html,application/xhtml+xml",
            },
            timeout=25 if use_proxy else 20,
            use_proxy=bool(use_proxy),
        )

        def _dec(s: str) -> str:
            try:
                return (
                    s.encode("utf-8")
                    .decode("unicode_escape", errors="ignore")
                    .replace("\\/", "/")
                    .replace("\\u002F", "/")
                )
            except Exception:
                return s.replace("\\/", "/")

        vids: list[str] = []
        imgs: list[str] = []
        for key in ("playAddr", "downloadAddr", "play_addr", "download_addr"):
            for mm in re.finditer(rf'"{key}"\s*:\s*"((?:\\.|[^"\\])*)"', html):
                u = _dec(mm.group(1)).strip()
                if u.startswith("http"):
                    vids.append(u)
        for key in ("cover", "originCover", "dynamicCover"):
            for mm in re.finditer(rf'"{key}"\s*:\s*"((?:\\.|[^"\\])*)"', html):
                u = _dec(mm.group(1)).strip()
                if u.startswith("http"):
                    imgs.append(u)
        title = _meta_content(html, "og:title") or ""
        if not title:
            mm = re.search(r'"desc"\s*:\s*"((?:\\.|[^"\\])*)"', html)
            if mm:
                title = _dec(mm.group(1))
        meta["title"] = (title or meta.get("title") or "")[:200]
        meta["video_urls"] = list(dict.fromkeys(vids))[:3]
        meta["image_urls"] = list(dict.fromkeys(imgs))[:6]
        if meta["video_urls"] or meta["image_urls"]:
            meta["error"] = None
            return meta
        meta["error"] = meta.get("error") or page.get("error") or "TikTok 未解析到媒体"
    except Exception as e:
        meta["error"] = meta.get("error") or f"TikTok 页面抽取失败：{e}"
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


# ---------- 小黑盒签名（对齐 Zhalslar/astrbot_plugin_parser） ----------

_XHH_CHAR_TABLE = "AB45STUVWZEFGJ6CH01D237IXYPQRKLMN89"
_XHH_V4_EP = (
    "V1ZCERzVgMWrKv+VcTl5QmS9JuPWLOQ8A0mACeTyYXtTbiguOrHhwaqnagZ6zdAgF"
    "4WpAYBvUH3EDnPRlNWut4CTDU1tCa80BSnvTMC9X1j9Kh6IMlGmzPIqpBzzx9r7Nt"
    "9XtUhv2WiQ2BgPnUwOFe7gN9r8Yj3184qxn1btJL8="
)
_XHH_V4_DATA = (
    "abbbe96a1579aa6fe4fa84e875851b7d7a843a14c5c9573c771d9c1443c9b3a"
    "d7603a8d9d67dbc9bd001bf42702ac82e4a6979323ff305eecd74b9620ee140"
    "0c135f840b35d9402ec3e3a93fcb3d0d3d6b3e740f5176b72225b6fb8a0d483"
    "cab753aa71062dc9b59bc8de950628f23607301c6cd94e75f680b86485a11ac"
    "36eba1413e9f14b274eadff30114dfb1cedadc4bd08ef83c5b2d048970d07d3"
    "943afef809b44e3b9fee602c91e274fee1523a8beee7e7cec85680b279d616d"
    "da15e98b1b0aa718276bcdb05d4ac3e44e72da220e0ea798ad7452aec01d0db"
    "c31ad6bf147eab7f7e539d35fe5149110aae5c7069a67eba4aae638505819f8"
    "9e2a58bc3b5001c8a5045334121ef04a8e442d7dbb7776bd6013674d2c0028a"
    "f131bf6bde47b90dce5c8b9463c9f83d0e7264145c2f6f259d70c4d63a4996b"
    "b7c0074e8a59fa298ad144ec139cb29bc94074fbe2f4a88400d85c003793e2b"
    "e2077184c3ba2e792926fce25f24d3a764a7c2667446173c74aa704d0d517f2"
    "10926aaef05376230b43c3a676dad6ff1c9603553d66eadfb492445eac44745"
    "acc620b325560d4941c10e05f3099a17a553fd763a1b7d6ef29f512e436bdfa"
    "9fa7c5a70b6a5f91bbcb21946fc2ce92db0c92930008b0fc82e90c3c73f9265"
    "2ca388f77b262a918cf59160fa88e481138ee7fe9a9b51d7949a74d22d1dab4"
    "e865c12325bfb5b9e748526afb6d8a05c543fd6dc72e81b06a4ebbf8149fca5"
    "37a19330da2011eec0229e2302babe239397aa1c2292ab3807cf0aa129d078a"
    "a9da010003eac5bb2c06435fbbe9bee7543290c1224745bb485d78f42ee4e82"
    "afb27a38befc60a688fb2514795064926bf205357bd46b7c14dd15aea2cab48"
    "5c993f0df5a20811d0a7b3bfb1fcb0737c8305675e9bdac396ef8cffb0b6bc4"
    "700c3d881c1945329b721b9080bed46b18105b7c9fea4f8276f0fcd09fe99ec"
    "52fa50b11e12a19eb9d091ecde701ab2879e2d7727386b28bbde8d62832e1ad"
    "822ea57b383cdd3767e8ee64e201bf00fe9cc8428ece3262550764fea47c69e"
    "e4339de98767f034d8852993fdefa315d9dcda71a74b665804706d4f9a8c139"
    "3670c2220e4ceac833620e0dc8175eb7a77b8b37c1a9d9940c67d44c8bc6b5f"
    "9e46273e2f5149d3d3148e8f7a02c4a4c3c998924b7d0e93528952034adc20d"
    "c342404a8606f0c07cb2b98c4a5434e69b69282daf952f586b9eed4b4f1ef0c"
    "fe5c6d156d14fb5057c8c32a355d07e2f56737d1ccfad573d42c840bbe8b750"
    "388211f2c0c5d6a1e34e7741389a742dff58bb0b9f339707a349a09519ca78d"
    "5e4f1baaf2598ab9001c15824494eecc17735e69a193e5437cbe44c6f156a0b"
    "b8df4fed5edefd4f56f4ef0b4d8cc40fe623836da3c5e662005825c9d344074"
    "be2306d6241c163fe92a6ce40ff60538d7464f5a06b6bb9ca1e6f18491ca3c7"
    "d6c00e299cbb1ca1c525a981fc6c6f2bb05f709101099b8bd0d2c2a628d94c6"
    "1aa97fdd58c9f357359fbd5be9e8f0f534f4481fb780d58e3e599e01fdd5a7f"
    "c5fb7e01b76fd58b2f264947d2149fefa57577ef326e264fc827939329031d9"
    "01be7579ecf5fccdab11c615c1a053f198297c0723faf8b17ea3335d49df2bf"
    "dd17271c2b64745b1f412d87297edd4404a4ae5312debf73b66afcc3d884b93"
    "8de41b6ee87265ce624897f3557ebe2d97e6fb17f1dc6a893e48dfa16ef2bff"
    "d8f3e06f0a1fcf44c7f2efa372e0ff61344c93f4a2a66538fcc134cd0bf94d5"
    "4c969cda4392af70608cbab6cfa340b674ba3a59385c0ed9bb236ff6ed10e1e"
    "5a9d4b6529c075dc1ac23cfdae18ab1651a5ee747322e51e3cc6035ca929789"
    "00924e661a2694a47873569baa95fd821711dc53a1e0299ed707e337b570591"
    "a3f61a5e39f8a75771da1613e8236c9b1b94cb5617fdaf2424d68a7fbd83ebf"
    "356fc87e8a805bee5bbd20a55a70881394d7624b1dcf5a135f1cf40b842eca3"
    "3d46b72447e0a2e85adf6c26efa6cc73b63573840f7b6229fb03ab45a8b639b"
    "5a66bbd6f63d10e59db49d7a9c9af3e3aeb79b7b756e24d5002917e7e788018"
    "4f80fcc605a1ba825c779e6083fd7fb0920bbcee021ec8e35427391b871b149"
    "c306c2dbda602044cd53ec424dd70cfd1c14a23c9964c039258cff4b75112f8"
    "15d9717433c1989ec398cd2acd67c89be82a409e0ef8f3e9ea8ec8b51b5ea5a"
    "005b5e735978d9a2987a76d62a2af230e30dc6327f7c0d153add27c7e8a320e"
    "4df6c05ab91fe0b9f6f9e13c50f39454066776503eb2ec84b74b4b2d5228627"
    "d81c938f7201610c9b703e4fd283a94835b7387db2880443a050d3eb0859aa1"
    "efd0f9bb7613b6b918ec2f7b5bb3e7722105b595e7973a93e3de8153a0f8e5b"
    "fd1aa6cefc6285fea85e8381ddcce98b31dda33db2a3c80ac04df14b872c805"
    "15373f231c3653fb2db799b32e83e59fb0f5763febca3d291b49bf83dd7ebd6"
    "1229300b65d44964d9e679f6061a0b2ea1bcd9f5af9bf710047237d87d13394"
    "ea8b4627c6997589d0b58379d025b076460eab88d6615ee92b0aa6c47f721f9"
    "7e0b5bbe721f06544d0a1bb81402697f2d72ad32c791dab45064b4d18460602"
    "9494b268feaebb268e7f92352dc3482f857c14885aabbad98a43e5f8fa5d77d"
    "61dc22f23080b9e6403c76f5fb862d7520ab85ae7c1d0e339729f664e7d668f"
    "4b9d1301acabb62fda5940db236ea9d2ca896cbb6a13eda6120fa5881453cb4"
    "490438460c00db4cd4bdf5df993d3a8d5726c756015eed542e0a4b910570f39"
    "7211c3f84f6a0d038e82270f94543e8da1e8d0cffd8f4f561daaf6003ad1fad"
    "fdd89c50f057a79225d8647aead74b33216e328c4204686b4ae93ce5f7ee25e"
    "1c83fe2cb72c67589aa4865d278ff7a112d09c16707de8acd61b49b901a3266"
    "e8ef55f1351fdc3013154635e51e649cbf31fc9b32f6956800834ca73e0b75b"
    "2b54d7125257eb6c24ebff52b741109be6da99bb6e0ffab85c3c219550ec3fc"
    "b12e2e4d0234627b061193c290baa1be73241be70925c08d33e6efdd44eca9a"
    "5160bdc5b47bd1f9d3f2cbf38848cf1aaa2a4827f86e43e06246b3bf94cb0b9"
    "f050c89533a3be9ffecefebd1a92e04197f18d7fadc0bfc8664de18425d5c03"
    "59b58049267934756f513bd68ea427b38f15213f42cce05cd59f5ea502967ec"
    "6a096daaa5e5d2a373227f2fe4514e27dfa012d708f7e94a286452972b5fab4"
    "581ecee3df40bad802cbb50b1a5d9dd3323a5f7c61ab893b16782a0ba64fd42"
    "10c30ac00f9d21b9124e5e5b323f43badf56761e1eea5c86ff61f19ce1485f4"
    "2cf6cadd751bbfb2ef87229eee5068ef6e209f123d29a571a374974ceac2e77"
    "f143faba60fc5d16f88d801fa01d879420b5d1393ad5b2bc913e3b0ba7155a6"
    "7648196573126273cccc79f2eac32ab68d72cc0f7170feca9c9726af9d65962"
    "663d5281372386ec88bd2fa82316f687535ecd39f00658523708ca4785529f5"
    "93baf100597ed00c15ae8ff87baa295871680b4096ac03a550f0f015297198b"
    "1a93f38cfefbeceabc099c1026664d77f616b4f069cf8bf53d2684b9a4d933c"
    "3c65a3aef21559527bfc6586e0247efa244a0a355b43751bc09be8012699468"
    "a8c332d60b11bb4881bf56b92ead10e059ac40f83a4d6725cacbc1bb307c839"
    "c4edc8b5484b9e2935842e867e739223f2eaaaff04d9701cfa49e3f80be4f2d"
    "1b7e8eb76fd7f33dfa79831f75ee65a75b7c7fff98254818f1ab77bca856656"
    "4d48e0012733dd426bf841f27f960394b1bacb8a3e36b96c41d751584cd580f"
    "ef1b6a8bf990487268348f682a27549ecbb9674b14f2fc97f203f3468f248ec"
    "3cf5171aa5e8a8d31a9a433c4f7644736aaf6695b28771fe66b4736e3afb322"
    "11ad534b05641600d2cdc79a251fc4c4e5540df9a40aaad329fedd49a429b20"
    "70e1345a4146c297ee2a03f056675054e83207d17de21242032c30398259440"
    "84e60cbd70eb4c469859824cd7d04340de0d19e614a0826a63c63e15c3372b1"
    "7515d4b6951ff6c612f65c3e6538fd0515bcb4814bb641fca5a45c7dae9"
)


def _xhh_xtime(value: int) -> int:
    return ((value << 1) ^ 27) & 0xFF if value & 128 else value << 1


def _xhh_mul3(value: int) -> int:
    return _xhh_xtime(value) ^ value


def _xhh_mul6(value: int) -> int:
    return _xhh_mul3(_xhh_xtime(value))


def _xhh_mul12(value: int) -> int:
    return _xhh_mul6(_xhh_mul3(_xhh_xtime(value)))


def _xhh_mul14(value: int) -> int:
    return _xhh_mul12(value) ^ _xhh_mul6(value) ^ _xhh_mul3(value)


def _xhh_mix_columns(col: list[int]) -> list[int]:
    values = list(col)
    while len(values) < 4:
        values.append(0)
    mixed = [
        _xhh_mul14(values[0])
        ^ _xhh_mul12(values[1])
        ^ _xhh_mul6(values[2])
        ^ _xhh_mul3(values[3]),
        _xhh_mul3(values[0])
        ^ _xhh_mul14(values[1])
        ^ _xhh_mul12(values[2])
        ^ _xhh_mul6(values[3]),
        _xhh_mul6(values[0])
        ^ _xhh_mul3(values[1])
        ^ _xhh_mul14(values[2])
        ^ _xhh_mul12(values[3]),
        _xhh_mul12(values[0])
        ^ _xhh_mul6(values[1])
        ^ _xhh_mul3(values[2])
        ^ _xhh_mul14(values[3]),
    ]
    if len(values) > 4:
        mixed.extend(values[4:])
    return mixed


def _xhh_av(text: str, cut: int) -> str:
    table = _XHH_CHAR_TABLE[:cut]
    return "".join(table[ord(c) % len(table)] for c in text)


def _xhh_sv(text: str) -> str:
    return "".join(_XHH_CHAR_TABLE[ord(c) % len(_XHH_CHAR_TABLE)] for c in text)


def _xhh_interleave(parts: list[str]) -> str:
    result: list[str] = []
    max_len = max(len(part) for part in parts)
    for i in range(max_len):
        for part in parts:
            if i < len(part):
                result.append(part[i])
    return "".join(result)


def _xhh_ov(path: str, ts: int, nonce: str) -> str:
    path = "/" + "/".join(p for p in path.split("/") if p) + "/"
    interleaved = _xhh_interleave(
        [_xhh_av(str(ts), -2), _xhh_sv(path), _xhh_sv(nonce)]
    )[:20]
    md5_hex = __import__("hashlib").md5(interleaved.encode()).hexdigest()
    prefix = _xhh_av(md5_hex[:5], -4)
    suffix = str(
        sum(_xhh_mix_columns([ord(c) for c in md5_hex[-6:]])) % 100
    ).zfill(2)
    return prefix + suffix


def _xhh_sign_path(path: str) -> dict[str, Any]:
    import random
    import time as _time
    import hashlib as _hashlib

    now = int(_time.time())
    nonce = _hashlib.md5((str(now) + str(random.random())).encode()).hexdigest().upper()
    return {"hkey": _xhh_ov(path, now + 1, nonce), "_time": now, "nonce": nonce}


def _xhh_request_json(
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
    timeout: int = 20,
) -> dict[str, Any]:
    """优先 curl_cffi chrome 伪装；失败回退 http_client/requests。"""
    hdrs = {
        "accept": "application/json, text/plain, */*",
        "referer": "https://www.xiaoheihe.cn/",
        "origin": "https://www.xiaoheihe.cn",
        "user-agent": _DESKTOP_UA,
    }
    if headers:
        hdrs.update(headers)
    # 1) curl_cffi
    try:
        from curl_cffi import requests as curl_requests  # type: ignore

        resp = curl_requests.request(
            method,
            url,
            params=params,
            json=json_body,
            headers=hdrs,
            cookies=cookies,
            timeout=timeout,
            impersonate="chrome131",
        )
        data = resp.json()
        if isinstance(data, dict):
            return data
    except Exception as e:
        logger.debug(f"小黑盒 curl_cffi 请求失败: {e}")
    # 2) http_client
    try:
        if method.upper() == "GET":
            resp = http_client.request(
                "GET",
                url,
                params=params,
                timeout=timeout,
                check_status=False,
                use_config_proxy=False,
                headers=hdrs,
            )
        else:
            resp = http_client.request(
                method.upper(),
                url,
                params=params,
                timeout=timeout,
                check_status=False,
                use_config_proxy=False,
                headers=hdrs,
                data=None if json_body is None else json.dumps(json_body),
            )
            # some clients need json= ; fallback below
        raw = getattr(resp, "text", "") or ""
        if raw.strip().startswith("{"):
            data = json.loads(raw)
            if isinstance(data, dict):
                return data
    except Exception as e:
        logger.debug(f"小黑盒 http_client 请求失败: {e}")
    # 3) requests
    try:
        import requests as _requests

        resp = _requests.request(
            method,
            url,
            params=params,
            json=json_body,
            headers=hdrs,
            cookies=cookies,
            timeout=timeout,
        )
        data = resp.json()
        if isinstance(data, dict):
            return data
    except Exception as e:
        logger.debug(f"小黑盒 requests 请求失败: {e}")
    return {}


def _xhh_fetch_device_token() -> tuple[str, str]:
    """返回 (x_xhh_tokenid, device_id)。"""
    payload = {
        "appId": "heybox_website",
        "organization": "0yD85BjYvGFAvHaSQ1mc",
        "ep": _XHH_V4_EP,
        "data": _XHH_V4_DATA,
        "os": "web",
        "encode": 5,
        "compress": 2,
    }
    data = _xhh_request_json(
        "POST",
        "https://fp-it.portal101.cn/deviceprofile/v4",
        json_body=payload,
        headers={"accept": "application/json, text/plain, */*"},
        timeout=20,
    )
    detail = data.get("detail") if isinstance(data, dict) else None
    device_id = ""
    if isinstance(detail, dict) and detail.get("deviceId"):
        device_id = str(detail["deviceId"])
    if not device_id:
        raise RuntimeError("小黑盒 deviceprofile 未返回 deviceId")
    return f"B{device_id}", device_id


def _xhh_fetch_link_tree(link_id: str) -> dict[str, Any]:
    token, device_id = _xhh_fetch_device_token()
    sig = _xhh_sign_path("/bbs/app/link/tree")
    params: dict[str, Any] = {
        "os_type": "web",
        "app": "heybox",
        "client_type": "web",
        "version": "999.0.4",
        "web_version": "2.5",
        "x_client_type": "web",
        "x_app": "heybox_website",
        "heybox_id": "",
        "x_os_type": "Windows",
        "device_info": "Chrome",
        "device_id": device_id,
        "link_id": str(link_id),
        "owner_only": "1",
        **sig,
    }
    payload = _xhh_request_json(
        "GET",
        "https://api.xiaoheihe.cn/bbs/app/link/tree",
        params=params,
        cookies={"x_xhh_tokenid": token},
        timeout=20,
    )
    return payload if isinstance(payload, dict) else {}


def _xhh_clean_text(text: str) -> str:
    import html as _html

    text = _html.unescape((text or "").replace("\xa0", " "))
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _xhh_normalize_image_url(url: str) -> str:
    import html as _html

    if not url:
        return ""
    url = _html.unescape(url)
    if not url.startswith("http"):
        return ""
    # 正文图通常在 /bbs/ 路径；也接受 heybox/max-c 图床
    low = url.lower()
    if "/bbs/" in url or "max-c.com" in low or "heybox" in low:
        return url
    if any(x in low for x in (".jpg", ".jpeg", ".png", ".webp")):
        return url
    return ""


def _xhh_parse_body(link: dict[str, Any]) -> tuple[str, list[str]]:
    raw_text = link.get("text")
    if not isinstance(raw_text, str) or not raw_text.strip():
        return "", []
    try:
        blocks = json.loads(raw_text)
    except Exception:
        return _xhh_clean_text(raw_text), []
    if not isinstance(blocks, list):
        return _xhh_clean_text(raw_text), []

    text_parts: list[str] = []
    images: list[str] = []
    seen: set[str] = set()
    for block in blocks:
        if not isinstance(block, dict):
            continue
        btype = str(block.get("type") or "")
        if btype == "img":
            u = _xhh_normalize_image_url(str(block.get("url") or "").strip())
            key = u.split("?", 1)[0]
            if u and key not in seen:
                seen.add(key)
                images.append(u)
            continue
        html_text = str(block.get("text") or "")
        if not html_text:
            continue
        # 抽 html 内图片
        for m in re.finditer(
            r'data-original="([^"]+)"|src="([^"]+)"', html_text, re.I
        ):
            cand = m.group(1) or m.group(2) or ""
            u = _xhh_normalize_image_url(cand)
            key = u.split("?", 1)[0]
            if u and key not in seen:
                seen.add(key)
                images.append(u)
        # 文本
        frag = re.sub(r"<br\s*/?>", "\n", html_text, flags=re.I)
        frag = re.sub(r"<[^>]+>", "", frag)
        cleaned = _xhh_clean_text(frag)
        if cleaned:
            text_parts.append(cleaned)
    return "\n\n".join(text_parts).strip(), images


def parse_xiaoheihe(url: str) -> dict[str, Any]:
    """小黑盒：对齐上游，走签名 link/tree + device token。

    分享页是 SPA 无 OG；旧 web/share 接口已 404。
    若风控返回 show_captcha，给出可读错误。
    """
    meta = _base_meta("xiaoheihe", url)
    final = expand_url(url, use_proxy=False, platform="xiaoheihe")
    meta["url"] = final
    m = (
        re.search(r"link_id=(\d+)", final)
        or re.search(r"/link/(\d+)", final)
        or re.search(r"/link/(\d+)", url)
        or re.search(r"link/(\d+)", url)
    )
    if not m:
        meta["error"] = "未能识别小黑盒 link_id"
        return meta
    link_id = m.group(1)
    meta["url"] = f"https://www.xiaoheihe.cn/app/bbs/link/{link_id}"

    try:
        payload = _xhh_fetch_link_tree(link_id)
    except Exception as e:
        meta["error"] = f"小黑盒鉴权/签名请求失败：{e}"
        return meta

    status = str(payload.get("status") or "")
    if status == "show_captcha":
        meta["error"] = (
            "小黑盒触发验证码风控(show_captcha)，"
            "当前环境无法自动过验证；可稍后重试或换网络"
        )
        return meta
    if status != "ok":
        meta["error"] = f"小黑盒 link/tree 失败：{payload.get('msg') or status or 'unknown'}"
        return meta

    result = payload.get("result") or {}
    link = result.get("link") if isinstance(result, dict) else None
    if not isinstance(link, dict):
        meta["error"] = "小黑盒返回缺少 link 节点"
        return meta

    title = _xhh_clean_text(str(link.get("title") or ""))
    body, images = _xhh_parse_body(link)
    user = link.get("user") or {}
    if isinstance(user, dict):
        meta["author"] = _xhh_clean_text(
            str(user.get("username") or user.get("nickname") or "")
        )
        if user.get("avatar"):
            meta["avatar_url"] = str(user.get("avatar"))
    meta["title"] = title or (body[:80] if body else "小黑盒帖子")
    meta["desc"] = (body or title)[:400]

    videos: list[str] = []
    if link.get("has_video") and link.get("video_url"):
        vu = str(link.get("video_url")).strip()
        if vu.startswith("http"):
            videos.append(vu)
    # 兜底从 link 树抽
    found: list[str] = []
    _collect_http_urls(link, found)
    for u in found:
        low = u.lower()
        if any(x in low for x in (".mp4", ".m3u8", ".mov")) and u not in videos:
            videos.append(u)
        if any(x in low for x in (".jpg", ".jpeg", ".png", ".webp")):
            nu = _xhh_normalize_image_url(u)
            if nu and nu not in images:
                images.append(nu)

    meta["image_urls"] = sort_media_urls_by_quality(images, kind="image")[:12]
    meta["video_urls"] = sort_media_urls_by_quality(videos, kind="video")[:5]
    if meta["image_urls"] or meta["video_urls"]:
        meta["error"] = None
        return meta
    if meta.get("title") and meta["title"] != "小黑盒帖子":
        meta["error"] = "小黑盒拿到标题但无媒体"
        return meta
    meta["error"] = "小黑盒未解析到媒体"
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
