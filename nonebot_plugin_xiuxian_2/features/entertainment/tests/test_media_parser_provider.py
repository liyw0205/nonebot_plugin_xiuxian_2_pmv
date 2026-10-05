from __future__ import annotations

import unittest

from ..media_parser_provider import (
    EntertainmentMediaParserProvider,
    MEDIA_PARSER_MAX_REQUESTS,
    MEDIA_PARSER_REQUEST_TIMEOUT_SECONDS,
)


class _Response:
    def __init__(self, chunks, *, headers=None, on_chunk=None):
        self.chunks = list(chunks)
        self.headers = headers or {}
        self.encoding = "utf-8"
        self.status_code = 200
        self.url = "https://example.invalid/final"
        self.on_chunk = on_chunk
        self.iterated = False
        self.closed = False

    def iter_content(self, chunk_size):
        self.iterated = True
        for chunk in self.chunks:
            if self.on_chunk:
                self.on_chunk()
            yield chunk

    def close(self):
        self.closed = True


class _Client:
    def __init__(self, responses):
        self.responses = list(responses)
        self.retries = 2
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


class MediaParserProviderTests(unittest.TestCase):
    def test_request_is_single_attempt_streamed_and_timeout_is_capped(self):
        response = _Response([b"<html>ok</html>"])
        client = _Client([response])
        provider = EntertainmentMediaParserProvider(http_client=client)

        with provider.operation():
            actual = provider.request("GET", "https://example.invalid", timeout=30)
            self.assertEqual(actual.text, "<html>ok</html>")

        self.assertEqual(provider.http_client.retries, 0)
        self.assertTrue(client.calls[0][2]["stream"])
        self.assertEqual(
            client.calls[0][2]["timeout"], MEDIA_PARSER_REQUEST_TIMEOUT_SECONDS
        )
        self.assertTrue(response.iterated)
        self.assertTrue(response.closed)

    def test_json_response_is_bounded_even_without_content_length(self):
        response = _Response([b'{"data":', b"[]}"])
        client = _Client([response])
        provider = EntertainmentMediaParserProvider(http_client=client)

        with provider.operation():
            self.assertEqual(
                provider.get_json(
                    "https://example.invalid/api", max_bytes=32, timeout=2
                ),
                {"data": []},
            )
        self.assertTrue(response.closed)

    def test_declared_and_streamed_overflow_are_rejected_and_closed(self):
        known = _Response([b"ignored"], headers={"Content-Length": "10"})
        provider = EntertainmentMediaParserProvider(http_client=_Client([known]))
        with provider.operation():
            response = provider.request("GET", "https://example.invalid", max_bytes=4)
            with self.assertRaisesRegex(ValueError, "size limit"):
                _ = response.content
        self.assertFalse(known.iterated)
        self.assertTrue(known.closed)

        unknown = _Response([b"123", b"456"])
        provider = EntertainmentMediaParserProvider(http_client=_Client([unknown]))
        with provider.operation():
            response = provider.request("GET", "https://example.invalid", max_bytes=4)
            with self.assertRaisesRegex(ValueError, "size limit"):
                _ = response.content
        self.assertTrue(unknown.closed)

    def test_streaming_checks_total_budget_and_request_count(self):
        now = [0.0]
        response = _Response([b"first", b"second"], on_chunk=lambda: now.__setitem__(0, 31.0))
        provider = EntertainmentMediaParserProvider(
            http_client=_Client([response]), clock=lambda: now[0]
        )
        with provider.operation():
            wrapped = provider.request("GET", "https://example.invalid")
            with self.assertRaisesRegex(TimeoutError, "总时间预算"):
                _ = wrapped.content
        self.assertTrue(response.closed)

        client = _Client([_Response([]) for _ in range(MEDIA_PARSER_MAX_REQUESTS)])
        provider = EntertainmentMediaParserProvider(http_client=client)
        with provider.operation():
            for _ in range(MEDIA_PARSER_MAX_REQUESTS):
                provider.request("GET", "https://example.invalid").close()
            with self.assertRaisesRegex(RuntimeError, "请求次数"):
                provider.request("GET", "https://example.invalid")
        self.assertEqual(len(client.calls), MEDIA_PARSER_MAX_REQUESTS)


if __name__ == "__main__":
    unittest.main()
