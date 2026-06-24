from __future__ import annotations

import re
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