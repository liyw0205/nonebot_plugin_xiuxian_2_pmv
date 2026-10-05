from __future__ import annotations

import unittest

from ..external_query import (
    DEFAULT_JSON_RESPONSE_MAX_BYTES,
    EntertainmentExternalQueryProvider,
)


class _Response:
    def __init__(
        self,
        chunks: list[bytes],
        *,
        headers: dict[str, str] | None = None,
        url: str = "https://example.invalid/final",
        encoding: str | None = "utf-8",
        on_chunk=None,
    ) -> None:
        self.chunks = chunks
        self.headers = headers or {}
        self.url = url
        self.encoding = encoding
        self.on_chunk = on_chunk
        self.closed = False
        self.iterated = False

    def iter_content(self, *, chunk_size: int):
        self.iterated = True
        for chunk in self.chunks:
            if self.on_chunk is not None:
                self.on_chunk()
            yield chunk

    def close(self) -> None:
        self.closed = True


class _HttpClient:
    def __init__(
        self,
        responses: list[_Response],
        json_results: list[dict] | None = None,
    ) -> None:
        self.responses = responses
        self.json_results = list(json_results or [])
        self.requests: list[tuple[str, dict]] = []
        self.json_calls: list[tuple[str, dict]] = []

    def request(self, method: str, url: str, **kwargs):
        self.requests.append((url, kwargs))
        return self.responses.pop(0)

    def get_json(self, url: str, **kwargs):
        self.json_calls.append((url, kwargs))
        if self.json_results:
            return self.json_results.pop(0)
        return {"data": []}


class EntertainmentExternalQueryProviderTests(unittest.TestCase):
    def test_json_requests_always_have_a_default_response_limit_and_no_retry_client(self) -> None:
        client = _HttpClient([])
        provider = EntertainmentExternalQueryProvider(http_client=client)

        self.assertEqual(provider.get_json("https://example.invalid/api"), {"data": []})
        self.assertEqual(provider.http_client.retries, 0)
        self.assertEqual(
            client.json_calls[0][1]["max_bytes"],
            DEFAULT_JSON_RESPONSE_MAX_BYTES,
        )

    def test_text_response_is_streamed_bounded_and_closed(self) -> None:
        response = _Response([b"hello ", b"world"], headers={"Content-Length": "11"})
        provider = EntertainmentExternalQueryProvider(http_client=_HttpClient([response]))

        result = provider.get_text(
            "https://example.invalid/text",
            max_bytes=16,
        )

        self.assertEqual(result, "hello world")
        self.assertTrue(response.iterated)
        self.assertTrue(response.closed)

    def test_form_post_json_is_streamed_bounded_and_uses_no_retry_client(self) -> None:
        response = _Response([b'{"data":[]}'], headers={"Content-Length": "11"})
        client = _HttpClient([response])
        provider = EntertainmentExternalQueryProvider(http_client=client)

        result = provider.post_form_json(
            "https://example.invalid/search",
            {"input": "song"},
            max_bytes=64,
        )

        self.assertEqual(result, {"data": []})
        self.assertEqual(provider.http_client.retries, 0)
        self.assertTrue(client.requests[0][1]["stream"])
        self.assertEqual(client.requests[0][1]["data"], {"input": "song"})
        self.assertTrue(response.iterated)
        self.assertTrue(response.closed)

    def test_form_post_json_rejects_streamed_overflow_and_closes(self) -> None:
        response = _Response([b'{"data":', b'[]}' ])
        provider = EntertainmentExternalQueryProvider(http_client=_HttpClient([response]))

        with self.assertRaisesRegex(ValueError, "size limit"):
            provider.post_form_json(
                "https://example.invalid/search",
                {"input": "song"},
                max_bytes=8,
            )

        self.assertTrue(response.closed)

    def test_form_post_json_enforces_total_deadline_during_streaming(self) -> None:
        now = [0.0]
        response = _Response(
            [b'{"data":', b"[]}"],
            on_chunk=lambda: now.__setitem__(0, now[0] + 0.6),
        )
        provider = EntertainmentExternalQueryProvider(
            http_client=_HttpClient([response]),
            clock=lambda: now[0],
        )

        with self.assertRaisesRegex(TimeoutError, "total time budget"):
            provider.post_form_json(
                "https://example.invalid/search",
                {"input": "song"},
                total_timeout=1,
                max_bytes=64,
            )

        self.assertTrue(response.closed)

    def test_text_response_rejects_streamed_overflow_and_closes(self) -> None:
        response = _Response([b"a" * 10, b"b" * 7])
        provider = EntertainmentExternalQueryProvider(http_client=_HttpClient([response]))

        with self.assertRaisesRegex(ValueError, "size limit"):
            provider.get_text(
                "https://example.invalid/text",
                max_bytes=16,
            )

        self.assertTrue(response.closed)

    def test_media_url_lookup_does_not_download_non_json_media_body(self) -> None:
        response = _Response(
            [b"x" * (2 * DEFAULT_JSON_RESPONSE_MAX_BYTES)],
            headers={"Content-Type": "video/mp4"},
        )
        client = _HttpClient([response])
        provider = EntertainmentExternalQueryProvider(http_client=client)

        result = provider.get_media_url("https://example.invalid/random")

        self.assertEqual(result, "https://example.invalid/final")
        self.assertTrue(client.requests[0][1]["stream"])
        self.assertFalse(response.iterated)
        self.assertTrue(response.closed)

    def test_media_url_json_descriptor_is_bounded(self) -> None:
        response = _Response(
            [b'{"url":"https://cdn.invalid/image.jpg"}'],
            headers={"Content-Type": "application/json"},
        )
        provider = EntertainmentExternalQueryProvider(http_client=_HttpClient([response]))

        result = provider.get_media_url("https://example.invalid/random")

        self.assertEqual(result, "https://cdn.invalid/image.jpg")
        self.assertTrue(response.iterated)
        self.assertTrue(response.closed)

    def test_media_bytes_download_is_bounded_and_closed(self) -> None:
        response = _Response([b"a" * 8, b"b" * 3])
        provider = EntertainmentExternalQueryProvider(http_client=_HttpClient([response]))

        with self.assertRaisesRegex(ValueError, "size limit"):
            provider.get_bytes("https://example.invalid/image", max_bytes=10)

        self.assertTrue(response.iterated)
        self.assertTrue(response.closed)

    def test_bangumi_fetch_is_page_bounded_and_cached(self) -> None:
        client = _HttpClient(
            [],
            json_results=[
                {"data": [{"mal_id": page}], "pagination": {"has_next_page": True}}
                for page in range(1, 7)
            ],
        )
        provider = EntertainmentExternalQueryProvider(
            http_client=client,
            sleep=lambda _seconds: None,
        )

        first = provider.fetch_bangumi_seasons_now()
        second = provider.fetch_bangumi_seasons_now()

        self.assertEqual(len(first), 6)
        self.assertEqual(second, first)
        self.assertEqual(len(client.json_calls), 6)
        self.assertTrue(
            all(
                call[1]["max_bytes"] <= DEFAULT_JSON_RESPONSE_MAX_BYTES
                for call in client.json_calls
            )
        )

    def test_bangumi_total_budget_stops_before_another_page(self) -> None:
        now = [0.0]
        client = _HttpClient(
            [],
            json_results=[
                {"data": [{"mal_id": 1}], "pagination": {"has_next_page": True}}
            ],
        )

        def sleep(seconds: float) -> None:
            now[0] += seconds + 28.0

        provider = EntertainmentExternalQueryProvider(
            http_client=client,
            clock=lambda: now[0],
            sleep=sleep,
        )

        result = provider.fetch_bangumi_seasons_now()

        self.assertEqual(result, [{"mal_id": 1}])
        self.assertEqual(len(client.json_calls), 1)


if __name__ == "__main__":
    unittest.main()
