from __future__ import annotations

import pytest

from nonebot_plugin_xiuxian_2.adapters.web.media_proxy import (
    MediaProxyError,
    MediaProxyTooLarge,
    download_media_bytes,
    is_allowed_media_proxy_url,
)


class _Response:
    def __init__(
        self,
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        chunks: tuple[bytes, ...] = (),
        url: str | None = None,
    ) -> None:
        self.status_code = status_code
        self.headers = headers or {}
        self.chunks = chunks
        self.url = url
        self.closed = False
        self.chunk_size = None

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, *, chunk_size: int):
        self.chunk_size = chunk_size
        yield from self.chunks

    def close(self) -> None:
        self.closed = True


class _HttpClient:
    def __init__(self, responses: list[_Response]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, dict]] = []

    def request(self, method: str, url: str, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


@pytest.mark.parametrize(
    "url",
    [
        "https://multimedia.nt.qq.com.cn/media?id=1",
        "http://q.qlogo.cn/avatar",
        "https://a.b.qpic.cn/path",
    ],
)
def test_allowlist_accepts_qq_media_hosts(url: str) -> None:
    assert is_allowed_media_proxy_url(url)


@pytest.mark.parametrize(
    "url",
    [
        "https://qq.com/media",
        "https://qpic.cn/path",
        "https://qpic.cn.evil.invalid/path",
        "https://multimedia.nt.qq.com.cn:443/media",
        "https://multimedia.nt.qq.com.cn:80/media",
        "https://user@multimedia.nt.qq.com.cn/media",
        "https://user:pass@multimedia.nt.qq.com.cn/media",
        "ftp://multimedia.nt.qq.com.cn/media",
        "file:///etc/passwd",
        "https:///media",
    ],
)
def test_allowlist_rejects_unsafe_authorities_and_schemes(url: str) -> None:
    assert not is_allowed_media_proxy_url(url)


def test_download_disables_automatic_redirects_and_preserves_headers_timeout() -> None:
    response = _Response(chunks=(b"image",))
    client = _HttpClient([response])
    headers = {"User-Agent": "media-test", "Referer": "https://im.qq.com/"}

    assert download_media_bytes(
        "https://multimedia.nt.qq.com.cn/media",
        headers=headers,
        timeout=(2, 4),
        http=client,
    ) == b"image"

    method, url, kwargs = client.calls[0]
    assert (method, url) == ("GET", "https://multimedia.nt.qq.com.cn/media")
    assert kwargs == {
        "headers": headers,
        "timeout": (2, 4),
        "stream": True,
        "allow_redirects": False,
    }
    assert response.chunk_size == 64 * 1024
    assert response.closed


def test_redirect_to_non_allowlisted_host_is_rejected_and_response_closed() -> None:
    redirect = _Response(
        status_code=302,
        headers={"Location": "http://127.0.0.1/private"},
    )
    client = _HttpClient([redirect])

    with pytest.raises(MediaProxyError, match="不允许"):
        download_media_bytes("https://a.qpic.cn/image", http=client)

    assert len(client.calls) == 1
    assert redirect.closed


def test_redirect_loop_is_rejected() -> None:
    redirect = _Response(
        status_code=301,
        headers={"Location": "https://a.qpic.cn/image"},
    )
    client = _HttpClient([redirect])

    with pytest.raises(MediaProxyError, match="循环"):
        download_media_bytes("https://a.qpic.cn/image", http=client)

    assert len(client.calls) == 1
    assert redirect.closed


def test_redirect_limit_allows_five_hops_then_rejects_sixth() -> None:
    responses = [
        _Response(
            status_code=302,
            headers={"Location": f"https://hop{index + 1}.qpic.cn/image"},
        )
        for index in range(6)
    ]
    client = _HttpClient(responses)

    with pytest.raises(MediaProxyError, match="次数超过限制"):
        download_media_bytes("https://hop0.qpic.cn/image", http=client)

    assert len(client.calls) == 6
    assert all(response.closed for response in responses)


def test_content_length_over_limit_is_rejected_without_reading_chunks() -> None:
    response = _Response(headers={"Content-Length": "4"}, chunks=(b"data",))
    client = _HttpClient([response])

    with pytest.raises(MediaProxyTooLarge, match="超过大小限制"):
        download_media_bytes("https://a.qpic.cn/image", http=client, max_bytes=3)

    assert response.chunk_size is None
    assert response.closed


def test_chunked_body_over_limit_is_rejected_and_response_closed() -> None:
    response = _Response(chunks=(b"ab", b"cd"))
    client = _HttpClient([response])

    with pytest.raises(MediaProxyTooLarge, match="下载过程中"):
        download_media_bytes("https://a.qpic.cn/image", http=client, max_bytes=3)

    assert response.chunk_size == 64 * 1024
    assert response.closed


def test_download_follows_validated_redirect_and_returns_bounded_body() -> None:
    first = _Response(status_code=302, headers={"Location": "/final"})
    final = _Response(
        chunks=(b"image",),
        url="https://a.qpic.cn/final",
    )
    client = _HttpClient([first, final])

    assert download_media_bytes("https://a.qpic.cn/start", http=client) == b"image"
    assert [call[1] for call in client.calls] == [
        "https://a.qpic.cn/start",
        "https://a.qpic.cn/final",
    ]
    assert first.closed and final.closed
