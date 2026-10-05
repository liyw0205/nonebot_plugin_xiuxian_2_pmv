from __future__ import annotations

import copy
import json
import threading
import time
from typing import Any

from ...xiuxian.xiuxian_utils.http_proxy import HttpClient

DEFAULT_JSON_RESPONSE_MAX_BYTES = 2 * 1024 * 1024
DEFAULT_TEXT_RESPONSE_MAX_BYTES = 256 * 1024
DEFAULT_MEDIA_DOWNLOAD_MAX_BYTES = 20 * 1024 * 1024
MAX_BANGUMI_RESPONSE_BYTES = 1024 * 1024
MAX_BANGUMI_PAGES = 6
BANGUMI_CACHE_TTL_SECONDS = 900.0
BANGUMI_TOTAL_BUDGET_SECONDS = 28.0
_BANGUMI_URL = "https://api.jikan.moe/v4/seasons/now"
_BANGUMI_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    ),
    "Accept": "application/json",
}


class EntertainmentExternalQueryProvider:
    """Bounded external reads owned by the entertainment feature."""

    def __init__(
        self,
        *,
        http_client: Any | None = None,
        clock=time.monotonic,
        sleep=time.sleep,
    ) -> None:
        self.http_client = http_client or HttpClient(timeout=15, retries=0)
        # Injected clients obey the same single-attempt policy as the default client.
        try:
            self.http_client.retries = 0
        except (AttributeError, TypeError):
            pass
        self._clock = clock
        self._sleep = sleep
        self._bangumi_lock = threading.Lock()
        self._bangumi_cached_at = 0.0
        self._bangumi_cached_items: list[dict[str, Any]] | None = None

    def get_json(
        self,
        api_url: str,
        params: dict | None = None,
        timeout: float = 15,
        max_bytes: int | None = None,
        **request_options: Any,
    ) -> Any:
        return self.http_client.get_json(
            api_url,
            params=params,
            timeout=timeout,
            max_bytes=(
                DEFAULT_JSON_RESPONSE_MAX_BYTES if max_bytes is None else max_bytes
            ),
            **request_options,
        )

    def post_form_json(
        self,
        api_url: str,
        data: dict[str, Any],
        *,
        timeout: float = 15,
        total_timeout: float | None = None,
        max_bytes: int = DEFAULT_JSON_RESPONSE_MAX_BYTES,
        expected_type: type | tuple[type, ...] = dict,
        **request_options: Any,
    ) -> Any:
        deadline = (
            self._clock() + total_timeout
            if total_timeout is not None
            else None
        )
        response = self.http_client.request(
            "POST",
            api_url,
            data=data,
            timeout=timeout,
            stream=True,
            **request_options,
        )
        try:
            content = _read_bounded_response(
                response,
                max_bytes,
                deadline=deadline,
                clock=self._clock,
            )
            try:
                result = json.loads(content)
            except (TypeError, ValueError, RecursionError) as exc:
                raise ValueError("HTTP 响应不是合法 JSON") from exc
            if not isinstance(result, expected_type):
                raise ValueError(f"HTTP JSON 根类型不是 {expected_type!r}")
            return result
        finally:
            response.close()

    def get_text(
        self,
        api_url: str,
        params: dict | None = None,
        timeout: float = 15,
        max_bytes: int = DEFAULT_TEXT_RESPONSE_MAX_BYTES,
    ) -> str:
        response = self.http_client.request(
            "GET", api_url, params=params, timeout=timeout, stream=True
        )
        try:
            content = _read_bounded_response(response, max_bytes)
            encoding = getattr(response, "encoding", None) or "utf-8"
            return content.decode(encoding, errors="replace").strip()
        finally:
            response.close()

    def get_media_url(
        self,
        api_url: str,
        params: dict | None = None,
        timeout: float = 20,
    ) -> str:
        response = self.http_client.request(
            "GET",
            api_url,
            params=params,
            timeout=timeout,
            allow_redirects=True,
            stream=True,
        )
        try:
            content_type = str(response.headers.get("Content-Type", "")).lower()
            if "application/json" not in content_type:
                return str(response.url)
            result = json.loads(
                _read_bounded_response(response, DEFAULT_JSON_RESPONSE_MAX_BYTES)
            )
            if isinstance(result, dict):
                media_url = (
                    result.get("url")
                    or result.get("image")
                    or result.get("image_url")
                    or result.get("data")
                )
                if media_url:
                    return str(media_url)
            raise ValueError("接口未返回媒体地址")
        finally:
            response.close()

    def get_bytes(
        self,
        url: str,
        *,
        max_bytes: int = DEFAULT_MEDIA_DOWNLOAD_MAX_BYTES,
        timeout: float = 20,
        **request_options: Any,
    ) -> bytes:
        response = self.http_client.request(
            "GET", url, timeout=timeout, stream=True, **request_options
        )
        try:
            return _read_bounded_response(response, max_bytes)
        finally:
            response.close()

    def fetch_bangumi_seasons_now(self) -> list[dict[str, Any]]:
        with self._bangumi_lock:
            now = self._clock()
            if (
                self._bangumi_cached_items is not None
                and now - self._bangumi_cached_at < BANGUMI_CACHE_TTL_SECONDS
            ):
                return copy.deepcopy(self._bangumi_cached_items)

            items = self._fetch_bangumi_seasons_now(now)
            self._bangumi_cached_items = items
            self._bangumi_cached_at = self._clock()
            return copy.deepcopy(items)

    def _fetch_bangumi_seasons_now(self, started_at: float) -> list[dict[str, Any]]:
        deadline = started_at + BANGUMI_TOTAL_BUDGET_SECONDS
        last_error: BaseException | None = None

        for use_proxy in (False, True):
            collected: list[dict[str, Any]] = []
            for page in range(1, MAX_BANGUMI_PAGES + 1):
                remaining = deadline - self._clock()
                if remaining <= 0:
                    if collected:
                        return collected
                    raise TimeoutError("获取番剧日历超过总时间预算") from last_error
                try:
                    payload = self.get_json(
                        _BANGUMI_URL,
                        params={"sfw": "true", "page": page},
                        timeout=min(5.0, remaining),
                        max_bytes=MAX_BANGUMI_RESPONSE_BYTES,
                        headers=_BANGUMI_HEADERS,
                        use_config_proxy=use_proxy,
                    )
                    chunk = payload.get("data") or []
                    if not isinstance(chunk, list):
                        chunk = []
                    collected.extend(item for item in chunk if isinstance(item, dict))
                    pagination = payload.get("pagination") or {}
                    if not pagination.get("has_next_page"):
                        if collected:
                            return collected
                        last_error = ValueError("Jikan 当季列表为空")
                        break
                except Exception as exc:
                    last_error = exc
                    if collected:
                        return collected
                    break

                remaining = deadline - self._clock()
                if remaining <= 0:
                    if collected:
                        return collected
                    raise TimeoutError("获取番剧日历超过总时间预算") from last_error
                if page < MAX_BANGUMI_PAGES:
                    self._sleep(min(0.45, remaining))

            if collected:
                return collected

        if last_error is not None:
            raise last_error
        return []


def _read_bounded_response(
    response: Any,
    max_bytes: int,
    *,
    deadline: float | None = None,
    clock=time.monotonic,
) -> bytes:
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    headers = getattr(response, "headers", {}) or {}
    try:
        length = int(headers.get("Content-Length", headers.get("content-length", 0)) or 0)
    except (TypeError, ValueError):
        length = 0
    if length > max_bytes:
        raise ValueError(f"HTTP response exceeds size limit: {length} > {max_bytes}")

    content = bytearray()
    for chunk in response.iter_content(chunk_size=64 * 1024):
        if deadline is not None and clock() >= deadline:
            raise TimeoutError("HTTP response exceeded total time budget")
        if not chunk:
            continue
        if len(content) + len(chunk) > max_bytes:
            raise ValueError("HTTP response exceeded size limit while streaming")
        content.extend(chunk)
    if deadline is not None and clock() >= deadline:
        raise TimeoutError("HTTP response exceeded total time budget")
    return bytes(content)


__all__ = [
    "BANGUMI_CACHE_TTL_SECONDS",
    "BANGUMI_TOTAL_BUDGET_SECONDS",
    "DEFAULT_JSON_RESPONSE_MAX_BYTES",
    "DEFAULT_MEDIA_DOWNLOAD_MAX_BYTES",
    "DEFAULT_TEXT_RESPONSE_MAX_BYTES",
    "EntertainmentExternalQueryProvider",
    "MAX_BANGUMI_PAGES",
]
