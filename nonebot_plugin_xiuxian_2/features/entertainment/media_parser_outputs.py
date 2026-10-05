from __future__ import annotations

import hashlib
from pathlib import Path

from .media_parser_provider import EntertainmentMediaParserProvider

MEDIA_PARSE_VIDEO_MAX_BYTES = 20 * 1024 * 1024


def _media_headers(url: str) -> dict[str, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
    }
    low = (url or "").lower()
    if any(value in low for value in ("bilivideo", "hdslb", "bilibili")):
        headers["Referer"] = "https://www.bilibili.com"
        headers["Origin"] = "https://www.bilibili.com"
    elif "weibo" in low or "sinaimg" in low:
        headers["Referer"] = "https://weibo.com/"
    elif "xhscdn" in low or "xiaohongshu" in low:
        headers["Referer"] = "https://www.xiaohongshu.com/"
    elif "douyin" in low or "byte" in low:
        headers["Referer"] = "https://www.douyin.com/"
    elif "kuaishou" in low or "kwimgs" in low or "yximgs" in low:
        headers["Referer"] = "https://www.kuaishou.com/"
    return headers


def probe_media_size(provider: EntertainmentMediaParserProvider, url: str) -> int | None:
    """Probe HEAD, then a one-byte range request; both are single-attempt and bounded."""
    headers = _media_headers(url)
    with provider.operation():
        try:
            response = provider.request(
                "HEAD",
                url,
                timeout=5,
                max_bytes=1,
                check_status=False,
                use_config_proxy=False,
                headers=headers,
            )
            try:
                length = response.headers.get("Content-Length") or response.headers.get(
                    "content-length"
                )
                if length and str(length).isdigit():
                    return int(length)
            finally:
                response.close()
        except Exception:
            pass

        range_headers = dict(headers)
        range_headers["Range"] = "bytes=0-0"
        try:
            response = provider.request(
                "GET",
                url,
                timeout=5,
                max_bytes=1,
                check_status=False,
                use_config_proxy=False,
                headers=range_headers,
            )
            try:
                content_range = response.headers.get("Content-Range") or response.headers.get(
                    "content-range"
                ) or ""
                if "/" in content_range:
                    total = content_range.rsplit("/", 1)[-1]
                    if total.isdigit():
                        return int(total)
                length = response.headers.get("Content-Length") or response.headers.get(
                    "content-length"
                )
                if (
                    length
                    and str(length).isdigit()
                    and int(getattr(response, "status_code", 0) or 0) != 206
                ):
                    return int(length)
            finally:
                response.close()
        except Exception:
            pass
    return None


def download_video_local(
    provider: EntertainmentMediaParserProvider,
    url: str,
    referer: str = "https://www.bilibili.com",
    max_bytes: int = MEDIA_PARSE_VIDEO_MAX_BYTES,
) -> Path:
    """Download a supported video to the feature cache with an atomic bounded write."""
    from ...paths import get_paths
    from ...xiuxian.xiuxian_entertainment.media_parser.config import media_parser_cache_dir

    cache = media_parser_cache_dir() / "videos"
    cache.mkdir(parents=True, exist_ok=True)
    name = hashlib.sha1(url.encode("utf-8")).hexdigest()[:20] + ".mp4"
    output = cache / name
    paths = get_paths()
    if not output.is_file():
        legacy = paths.data / "media_parser_cache" / "videos" / name
        if legacy.is_file() and 1024 < legacy.stat().st_size <= max_bytes:
            return legacy
    if output.is_file() and 1024 < output.stat().st_size <= max_bytes:
        return output
    if output.is_file() and output.stat().st_size > max_bytes:
        output.unlink(missing_ok=True)

    headers = _media_headers(url)
    if referer:
        headers["Referer"] = referer
    temporary = output.with_suffix(".tmp")
    size = 0
    try:
        with provider.operation():
            response = provider.request(
                "GET",
                url,
                timeout=8,
                max_bytes=max_bytes,
                stream=True,
                check_status=False,
                use_config_proxy=False,
                headers=headers,
            )
            try:
                code = int(getattr(response, "status_code", 0) or 0)
                if code >= 400:
                    raise RuntimeError(f"视频下载 HTTP {code}")
                with temporary.open("wb") as output_file:
                    for chunk in response.iter_content(256 * 1024):
                        if not chunk:
                            continue
                        size += len(chunk)
                        if size > max_bytes:
                            raise RuntimeError(
                                f"视频超过 {max_bytes // (1024 * 1024)}MB"
                            )
                        output_file.write(chunk)
            finally:
                response.close()
        if size < 1024:
            raise RuntimeError("视频下载内容过小")
        temporary.replace(output)
        return output
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


__all__ = [
    "MEDIA_PARSE_VIDEO_MAX_BYTES",
    "download_video_local",
    "probe_media_size",
]
