from __future__ import annotations

from typing import Any
from urllib.parse import urljoin, urlsplit

from ...xiuxian.xiuxian_utils.http_proxy import HttpClient, http_client

MAX_MEDIA_PROXY_BYTES = 30 * 1024 * 1024
MEDIA_PROXY_CHUNK_BYTES = 64 * 1024
MAX_MEDIA_PROXY_REDIRECTS = 5
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_ALLOWED_EXACT_HOSTS = frozenset(
    {
        "multimedia.nt.qq.com.cn",
        "q.qlogo.cn",
        "q1.qlogo.cn",
    }
)


class MediaProxyError(ValueError):
    """A media proxy URL or response violated its safety limits."""


class MediaProxyTooLarge(MediaProxyError):
    """The upstream media response exceeded the configured byte limit."""


def is_allowed_media_proxy_url(url: str) -> bool:
    if not isinstance(url, str) or not url or url != url.strip():
        return False
    try:
        parsed = urlsplit(url)
        if parsed.scheme.lower() not in {"http", "https"}:
            return False
        if not parsed.netloc or parsed.username is not None or parsed.password is not None:
            return False
        if parsed.fragment:
            return False
        host = parsed.hostname
        if not host:
            return False
        # The proxy serves public QQ media endpoints only; explicit ports are not needed.
        if ":" in parsed.netloc or parsed.port is not None:
            return False
        host = host.lower()
        return host in _ALLOWED_EXACT_HOSTS or host.endswith(".qpic.cn")
    except (UnicodeError, ValueError):
        return False


def _header_value(headers: Any, name: str) -> str:
    value = headers.get(name)
    if value is None:
        value = headers.get(name.lower())
    if value is None:
        value = headers.get(name.title())
    return str(value) if value is not None else ""


def _redirect_key(url: str) -> tuple[str, str, str, str]:
    parsed = urlsplit(url)
    return (
        parsed.scheme.lower(),
        (parsed.hostname or "").lower(),
        parsed.path or "/",
        parsed.query,
    )


def download_media_bytes(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    timeout: float | tuple[float, float] = 15,
    http: HttpClient | Any | None = None,
    max_bytes: int = MAX_MEDIA_PROXY_BYTES,
) -> bytes:
    """Download an allowlisted QQ media URL with bounded streaming and redirects."""
    if not is_allowed_media_proxy_url(url):
        raise MediaProxyError("不允许的媒体地址")
    limit = min(int(max_bytes), MAX_MEDIA_PROXY_BYTES)
    if limit < 1:
        raise ValueError("max_bytes must be positive")

    client = http or http_client
    current_url = url
    seen = {_redirect_key(current_url)}

    for redirect_count in range(MAX_MEDIA_PROXY_REDIRECTS + 1):
        response = client.request(
            "GET",
            current_url,
            headers=headers,
            timeout=timeout,
            stream=True,
            allow_redirects=False,
        )
        next_url: str | None = None
        try:
            response_url = str(getattr(response, "url", None) or current_url)
            if not is_allowed_media_proxy_url(response_url):
                raise MediaProxyError("上游响应 URL 不在允许的媒体域名内")

            status = int(getattr(response, "status_code", 200))
            if status in _REDIRECT_STATUSES:
                if redirect_count >= MAX_MEDIA_PROXY_REDIRECTS:
                    raise MediaProxyError("媒体地址重定向次数超过限制")
                location = _header_value(getattr(response, "headers", {}), "Location")
                if not location:
                    raise MediaProxyError("媒体地址重定向缺少 Location")
                next_url = urljoin(current_url, location)
                if not is_allowed_media_proxy_url(next_url):
                    raise MediaProxyError("媒体地址重定向到不允许的域名")
                key = _redirect_key(next_url)
                if key in seen:
                    raise MediaProxyError("媒体地址检测到重定向循环")
                seen.add(key)
            else:
                response.raise_for_status()
                content_length = _header_value(
                    getattr(response, "headers", {}), "Content-Length"
                )
                if content_length:
                    try:
                        declared_length = int(content_length)
                    except ValueError as exc:
                        raise MediaProxyError("上游 Content-Length 无效") from exc
                    if declared_length > limit:
                        raise MediaProxyTooLarge(
                            f"媒体内容超过大小限制: {declared_length} > {limit}"
                        )

                content = bytearray()
                for chunk in response.iter_content(
                    chunk_size=MEDIA_PROXY_CHUNK_BYTES
                ):
                    if not chunk:
                        continue
                    if len(content) + len(chunk) > limit:
                        raise MediaProxyTooLarge(
                            f"媒体下载过程中超过大小限制: {limit} bytes"
                        )
                    content.extend(chunk)
                return bytes(content)
        finally:
            response.close()

        if next_url is None:
            raise MediaProxyError("媒体地址重定向流程异常结束")
        current_url = next_url

    raise MediaProxyError("媒体地址重定向次数超过限制")


__all__ = [
    "MAX_MEDIA_PROXY_BYTES",
    "MAX_MEDIA_PROXY_REDIRECTS",
    "MediaProxyError",
    "MediaProxyTooLarge",
    "download_media_bytes",
    "is_allowed_media_proxy_url",
]
