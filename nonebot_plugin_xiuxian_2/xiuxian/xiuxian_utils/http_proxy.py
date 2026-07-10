from __future__ import annotations

import re
import time
from typing import Any

from ..xiuxian_config import XiuConfig

_PROXY_IN_TEXT_RE = re.compile(
    r"(?i)(?:socks5h?|socks4|https?)://[^\s\"']+|"
    r"(?:\d{1,3}\.){3}\d{1,5}:\d{1,5}"
)


def _strip_wrapping_quotes(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        return s[1:-1].strip()
    return s


def _clean_proxy_string(raw: str) -> str:
    s = _strip_wrapping_quotes(str(raw or ""))
    if not s:
        return ""
    if s.lower() in ("none", "off", "false", "0", "直连", "direct"):
        return ""
    return s


def _ensure_socks_support(proxy_url: str) -> None:
    scheme = (proxy_url.split(":", 1)[0] or "").lower()
    if scheme not in ("socks5", "socks5h", "socks4"):
        return
    try:
        import socks  # noqa: F401
    except ImportError as e:
        raise RuntimeError(
            "已配置 SOCKS 代理但未安装 PySocks，请执行: pip install PySocks"
        ) from e


def get_custom_proxy_url() -> str:
    cfg = XiuConfig()
    if not getattr(cfg, "custom_proxy_enabled", False):
        return ""
    return _clean_proxy_string(getattr(cfg, "custom_proxy", "") or "")


def build_requests_proxies(proxy_url: str | None = None) -> dict[str, str] | None:
    if proxy_url is not None:
        url = _clean_proxy_string(proxy_url)
    else:
        url = get_custom_proxy_url()
    if not url:
        return None
    _ensure_socks_support(url)
    return {"http": url, "https": url}


def _redact_sensitive_from_message(text: str) -> str:
    if not text:
        return text
    out = _PROXY_IN_TEXT_RE.sub("[已隐藏]", text)
    out = re.sub(r"proxies\s*=\s*\{[^}]*\}", "proxies={...}", out, flags=re.I)
    return out.strip()


def describe_proxy_request_error(exc: BaseException) -> str:
    text = _redact_sensitive_from_message(str(exc).strip())
    errno = getattr(exc, "errno", None)
    if errno is None and exc.__cause__ is not None:
        errno = getattr(exc.__cause__, "errno", None)

    hints: list[str] = []
    if errno == 104 or "Errno 104" in text or "Connection reset by peer" in text:
        hints.append("连接被对端重置，请检查代理是否可用、地址与端口是否正确")
    if "Missing dependencies for SOCKS" in text or "SOCKS support" in text:
        hints.append("SOCKS 代理需安装 PySocks: pip install PySocks")
    if "Connection refused" in text or errno == 111:
        hints.append("连接被拒绝，请检查代理是否在监听、IP 是否可达")
    if "timed out" in text.lower() or "timeout" in text.lower():
        hints.append("请求超时，请检查网络或代理")

    base = text or type(exc).__name__
    if hints:
        return f"{base}\n" + "\n".join(f"· {h}" for h in hints)
    return base


def _requests_call(
    method: str,
    url: str,
    req_kwargs: dict[str, Any],
    proxies: dict[str, str] | None,
):
    import requests

    with requests.Session() as session:
        session.trust_env = False
        return session.request(method, url, proxies=proxies, **req_kwargs)


def requests_get(
    url: str,
    *,
    timeout: float | tuple[float, float] = 30,
    headers: dict[str, str] | None = None,
    params: Any = None,
    allow_redirects: bool = True,
    stream: bool = False,
    proxy_url: str | None = None,
    use_config_proxy: bool = True,
    **kwargs: Any,
):
    proxies = None
    if use_config_proxy:
        proxies = build_requests_proxies(proxy_url)
    req_kwargs: dict[str, Any] = {
        "timeout": timeout,
        "headers": headers,
        "params": params,
        "allow_redirects": allow_redirects,
        "stream": stream,
    }
    req_kwargs.update(kwargs)
    return _requests_call("GET", url, req_kwargs, proxies)


def requests_post(
    url: str,
    *,
    timeout: float | tuple[float, float] = 30,
    headers: dict[str, str] | None = None,
    data: Any = None,
    json: Any = None,
    proxy_url: str | None = None,
    use_config_proxy: bool = True,
    **kwargs: Any,
):
    proxies = None
    if use_config_proxy:
        proxies = build_requests_proxies(proxy_url)
    req_kwargs: dict[str, Any] = {
        "timeout": timeout,
        "headers": headers,
        "data": data,
        "json": json,
    }
    req_kwargs.update(kwargs)
    return _requests_call("POST", url, req_kwargs, proxies)


class HttpClient:
    def __init__(self, *, timeout: float = 30, retries: int = 2) -> None:
        self.timeout = timeout
        self.retries = max(0, int(retries))

    def request(self, method: str, url: str, **kwargs: Any):
        import requests

        timeout = kwargs.pop("timeout", self.timeout)
        check_status = bool(kwargs.pop("check_status", True))
        use_config_proxy = kwargs.pop("use_config_proxy", True)
        proxy_url = kwargs.pop("proxy_url", None)
        proxies = build_requests_proxies(proxy_url) if use_config_proxy else None
        last_error: BaseException | None = None
        for attempt in range(self.retries + 1):
            try:
                response = _requests_call(
                    method.upper(),
                    url,
                    {"timeout": timeout, **kwargs},
                    proxies,
                )
                if check_status:
                    response.raise_for_status()
                return response
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = exc
                if attempt >= self.retries:
                    raise
                time.sleep(0.05 * (attempt + 1))
        raise RuntimeError("HTTP 请求重试流程异常结束") from last_error

    def get_json(
        self,
        url: str,
        *,
        expected_type: type | tuple[type, ...] = dict,
        **kwargs: Any,
    ) -> Any:
        response = self.request("GET", url, **kwargs)
        try:
            data = response.json()
        except ValueError as exc:
            raise ValueError("HTTP 响应不是合法 JSON") from exc
        if not isinstance(data, expected_type):
            raise ValueError(f"HTTP JSON 根类型不是 {expected_type!r}")
        return data

    def post_json(
        self,
        url: str,
        payload: Any = None,
        *,
        expected_type: type | tuple[type, ...] = dict,
        **kwargs: Any,
    ) -> Any:
        response = self.request("POST", url, json=payload, **kwargs)
        try:
            data = response.json()
        except ValueError as exc:
            raise ValueError("HTTP 响应不是合法 JSON") from exc
        if not isinstance(data, expected_type):
            raise ValueError(f"HTTP JSON 根类型不是 {expected_type!r}")
        return data

    def download(self, url: str, *, max_bytes: int, **kwargs: Any) -> bytes:
        response = self.request("GET", url, stream=True, **kwargs)
        length = int(response.headers.get("content-length", 0) or 0)
        if length > max_bytes:
            raise ValueError(f"下载内容超过大小限制: {length} > {max_bytes}")
        content = bytearray()
        for chunk in response.iter_content(chunk_size=64 * 1024):
            content.extend(chunk)
            if len(content) > max_bytes:
                raise ValueError("下载过程中超过大小限制")
        return bytes(content)


class AsyncHttpClient:
    def __init__(self, *, timeout: float = 30, retries: int = 2) -> None:
        self.timeout = timeout
        self.retries = max(0, int(retries))

    async def request(self, method: str, url: str, **kwargs: Any):
        import asyncio
        import httpx

        timeout = kwargs.pop("timeout", self.timeout)
        last_error: BaseException | None = None
        for attempt in range(self.retries + 1):
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.request(method.upper(), url, **kwargs)
                response.raise_for_status()
                return response
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = exc
                if attempt >= self.retries:
                    raise
                await asyncio.sleep(0.05 * (attempt + 1))
        raise RuntimeError("异步 HTTP 请求重试流程异常结束") from last_error

    async def get_json(
        self,
        url: str,
        *,
        expected_type: type | tuple[type, ...] = dict,
        **kwargs: Any,
    ) -> Any:
        response = await self.request("GET", url, **kwargs)
        try:
            data = response.json()
        except ValueError as exc:
            raise ValueError("HTTP 响应不是合法 JSON") from exc
        if not isinstance(data, expected_type):
            raise ValueError(f"HTTP JSON 根类型不是 {expected_type!r}")
        return data


http_client = HttpClient()
async_http_client = AsyncHttpClient()
