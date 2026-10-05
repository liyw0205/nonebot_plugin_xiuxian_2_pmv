from __future__ import annotations

import json
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator

from ...xiuxian.xiuxian_utils.http_proxy import HttpClient

MEDIA_PARSER_RESPONSE_MAX_BYTES = 4 * 1024 * 1024
MEDIA_PARSER_REQUEST_TIMEOUT_SECONDS = 8.0
MEDIA_PARSER_TOTAL_BUDGET_SECONDS = 30.0
MEDIA_PARSER_MAX_REQUESTS = 24


class _BoundedResponse:
    def __init__(self, response: Any, max_bytes: int, deadline: float | None, clock):
        self._response = response
        self._max_bytes = max_bytes
        self._deadline = deadline
        self._clock = clock
        self._body: bytes | None = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._response, name)

    def _read(self) -> bytes:
        if self._body is not None:
            return self._body
        self._body = b"".join(self.iter_content(64 * 1024))
        return self._body

    @property
    def content(self) -> bytes:
        return self._read()

    @property
    def text(self) -> str:
        encoding = getattr(self._response, "encoding", None) or "utf-8"
        return self._read().decode(encoding, errors="replace")

    def json(self) -> Any:
        return json.loads(self._read())

    def iter_content(self, chunk_size: int = 64 * 1024) -> Iterator[bytes]:
        if self._body is not None:
            size = max(1, int(chunk_size))
            for offset in range(0, len(self._body), size):
                yield self._body[offset : offset + size]
            return
        headers = getattr(self._response, "headers", {}) or {}
        try:
            length = int(headers.get("Content-Length", headers.get("content-length", 0)) or 0)
        except (TypeError, ValueError):
            length = 0
        if length > self._max_bytes:
            self.close()
            raise ValueError(f"HTTP response exceeds size limit: {length} > {self._max_bytes}")

        total = 0
        try:
            for chunk in self._response.iter_content(chunk_size=max(1, int(chunk_size))):
                if self._deadline is not None and self._clock() >= self._deadline:
                    raise TimeoutError("媒体解析超过总时间预算")
                if not chunk:
                    continue
                total += len(chunk)
                if total > self._max_bytes:
                    raise ValueError(
                        f"HTTP response exceeds size limit: > {self._max_bytes}"
                    )
                yield chunk
        finally:
            self.close()

    def close(self) -> None:
        try:
            self._response.close()
        except Exception:
            pass

    def __del__(self) -> None:
        self.close()


class EntertainmentMediaParserProvider:
    """Single-attempt, bounded HTTP transport for the entertainment media parser."""

    def __init__(self, *, http_client: Any | None = None, clock=time.monotonic):
        self.http_client = http_client or HttpClient(
            timeout=MEDIA_PARSER_REQUEST_TIMEOUT_SECONDS, retries=0
        )
        try:
            self.http_client.retries = 0
        except (AttributeError, TypeError):
            pass
        self._clock = clock
        self._local = threading.local()

    @contextmanager
    def operation(self):
        previous = getattr(self._local, "state", None)
        self._local.state = {
            "deadline": self._clock() + MEDIA_PARSER_TOTAL_BUDGET_SECONDS,
            "requests": 0,
        }
        try:
            yield self
        finally:
            self._local.state = previous

    def _request_policy(self, timeout: float | tuple[float, float] | None) -> tuple[float, float | None]:
        state = getattr(self._local, "state", None)
        deadline = state["deadline"] if state else None
        if state is not None:
            if state["requests"] >= MEDIA_PARSER_MAX_REQUESTS:
                raise RuntimeError("媒体解析请求次数超过上限")
            state["requests"] += 1
        remaining = deadline - self._clock() if deadline is not None else None
        if remaining is not None and remaining <= 0:
            raise TimeoutError("媒体解析超过总时间预算")
        if remaining is not None and remaining < 0.05:
            raise TimeoutError("媒体解析超过总时间预算")
        if isinstance(timeout, tuple):
            requested = max(float(timeout[0]), float(timeout[1]))
        else:
            requested = float(timeout or MEDIA_PARSER_REQUEST_TIMEOUT_SECONDS)
        effective = min(requested, MEDIA_PARSER_REQUEST_TIMEOUT_SECONDS)
        if remaining is not None:
            effective = min(effective, remaining)
        return max(0.05, effective), deadline

    def request(self, method: str, url: str, *, max_bytes: int = MEDIA_PARSER_RESPONSE_MAX_BYTES, **kwargs: Any):
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        timeout, deadline = self._request_policy(kwargs.get("timeout"))
        kwargs["timeout"] = timeout
        kwargs["stream"] = True
        response = self.http_client.request(method, url, **kwargs)
        return _BoundedResponse(response, max_bytes, deadline, self._clock)

    def get_json(self, url: str, *, max_bytes: int = MEDIA_PARSER_RESPONSE_MAX_BYTES, **kwargs: Any) -> Any:
        response = self.request("GET", url, max_bytes=max_bytes, **kwargs)
        try:
            return response.json()
        finally:
            response.close()

    def xiaoheihe_json(
        self,
        method: str,
        url: str,
        *,
        params=None,
        json_body=None,
        headers=None,
        cookies=None,
        timeout=MEDIA_PARSER_REQUEST_TIMEOUT_SECONDS,
    ) -> dict[str, Any]:
        request_timeout, deadline = self._request_policy(timeout)
        try:
            from curl_cffi import requests as curl_requests  # type: ignore

            response = curl_requests.request(
                method,
                url,
                params=params,
                json=json_body,
                headers=headers,
                cookies=cookies,
                timeout=request_timeout,
                impersonate="chrome131",
                stream=True,
            )
            wrapped = _BoundedResponse(
                response, MEDIA_PARSER_RESPONSE_MAX_BYTES, deadline, self._clock
            )
            result = wrapped.json()
            if isinstance(result, dict):
                return result
        except Exception:
            pass

        kwargs: dict[str, Any] = {
            "params": params,
            "timeout": request_timeout,
            "check_status": False,
            "use_config_proxy": False,
            "headers": headers,
        }
        if cookies:
            kwargs["cookies"] = cookies
        if method.upper() == "GET":
            response = self.request("GET", url, **kwargs)
        else:
            kwargs["data"] = None if json_body is None else json.dumps(json_body)
            response = self.request(method.upper(), url, **kwargs)
        try:
            result = response.json()
            return result if isinstance(result, dict) else {}
        except Exception:
            return {}
        finally:
            response.close()


__all__ = [
    "EntertainmentMediaParserProvider",
    "MEDIA_PARSER_MAX_REQUESTS",
    "MEDIA_PARSER_REQUEST_TIMEOUT_SECONDS",
    "MEDIA_PARSER_RESPONSE_MAX_BYTES",
    "MEDIA_PARSER_TOTAL_BUDGET_SECONDS",
]
