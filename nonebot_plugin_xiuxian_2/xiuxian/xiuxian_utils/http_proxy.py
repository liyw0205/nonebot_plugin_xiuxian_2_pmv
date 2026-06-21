"""根据 XiuConfig 为 requests 组装代理（HTTP/HTTPS/SOCKS5 自动识别）。"""
from __future__ import annotations

import re
from typing import Any

from ..xiuxian_config import XiuConfig

_SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*://")


def _strip_wrapping_quotes(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        return s[1:-1].strip()
    return s


def normalize_proxy_url(raw: str) -> str:
    """
    将用户配置规范为 requests 可用的代理 URL。

    支持示例：
    - socks5://127.0.0.1:1080
    - socks5h://user:pass@host:1080  （DNS 也走代理）
    - http://127.0.0.1:7890
    - https://host:443
    - 127.0.0.1:1080 → 默认 socks5://
    - user:pass@host:1080 → 默认 socks5://
    """
    raw = _strip_wrapping_quotes(str(raw or ""))
    if not raw:
        return ""

    lower = raw.lower()
    if lower in ("none", "off", "false", "0", "直连", "direct"):
        return ""

    if _SCHEME_RE.match(raw):
        scheme = lower.split(":", 1)[0]
        if scheme in ("socks5", "socks5h", "socks4", "http", "https"):
            return raw
        if scheme == "socket5":  # 常见拼写
            return "socks5://" + raw.split("://", 1)[1]
        return raw

    # 无协议：番剧等场景默认 SOCKS5
    return f"socks5://{raw}"


def get_custom_proxy_url() -> str:
    cfg = XiuConfig()
    if not getattr(cfg, "custom_proxy_enabled", False):
        return ""
    return normalize_proxy_url(getattr(cfg, "custom_proxy", "") or "")


def build_requests_proxies(proxy_url: str | None = None) -> dict[str, str] | None:
    """返回 requests 的 proxies 参数字典，未启用时返回 None。"""
    url = normalize_proxy_url(proxy_url) if proxy_url is not None else get_custom_proxy_url()
    if not url:
        return None
    return {"http": url, "https": url}


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
    """带修仙自定义代理的 GET（与 requests.get 参数兼容）。"""
    import requests

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
    if proxies:
        req_kwargs["proxies"] = proxies
    req_kwargs.update(kwargs)
    return requests.get(url, **req_kwargs)


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
    import requests

    proxies = None
    if use_config_proxy:
        proxies = build_requests_proxies(proxy_url)
    req_kwargs: dict[str, Any] = {
        "timeout": timeout,
        "headers": headers,
        "data": data,
        "json": json,
    }
    if proxies:
        req_kwargs["proxies"] = proxies
    req_kwargs.update(kwargs)
    return requests.post(url, **req_kwargs)