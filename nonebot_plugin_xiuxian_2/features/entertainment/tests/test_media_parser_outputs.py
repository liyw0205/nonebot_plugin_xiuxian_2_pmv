from __future__ import annotations

import unittest
from contextlib import contextmanager

from ..media_parser_outputs import probe_media_size


class _Response:
    def __init__(self, headers, status_code=200):
        self.headers = headers
        self.status_code = status_code
        self.closed = False

    def close(self):
        self.closed = True


class _Provider:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    @contextmanager
    def operation(self):
        yield self

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


class MediaParserOutputTests(unittest.TestCase):
    def test_size_probe_uses_head_and_closes_response(self):
        response = _Response({"Content-Length": "2048"})
        provider = _Provider([response])

        self.assertEqual(
            probe_media_size(provider, "https://cdn.invalid/video.mp4"), 2048
        )
        self.assertEqual([call[0] for call in provider.calls], ["HEAD"])
        self.assertTrue(response.closed)

    def test_size_probe_falls_back_to_single_byte_range(self):
        head = _Response({})
        ranged = _Response({"Content-Range": "bytes 0-0/9876"}, status_code=206)
        provider = _Provider([head, ranged])

        self.assertEqual(
            probe_media_size(provider, "https://cdn.invalid/video.mp4"), 9876
        )
        self.assertEqual([call[0] for call in provider.calls], ["HEAD", "GET"])
        self.assertEqual(provider.calls[1][2]["headers"]["Range"], "bytes=0-0")
        self.assertTrue(head.closed)
        self.assertTrue(ranged.closed)


if __name__ == "__main__":
    unittest.main()
