from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment.mod import newapi_client


class _Response:
    def __init__(self, chunks: list[bytes], *, content_length: str = "", text: str = "") -> None:
        self.headers = {"content-length": content_length} if content_length else {}
        self.encoding = "utf-8"
        self.status_code = 200
        self.text = text
        self._chunks = chunks
        self.closed = False

    def iter_content(self, chunk_size: int):
        assert chunk_size == 64 * 1024
        return iter(self._chunks)

    def close(self) -> None:
        self.closed = True


class NewApiCheckinClientTests(unittest.TestCase):
    def test_checkin_streams_and_parses_json_under_the_response_cap(self) -> None:
        payload = json.dumps({"success": True, "message": "checked"}).encode()
        response = _Response([payload], content_length=str(len(payload)))
        with patch.object(newapi_client.http_client, "request", return_value=response) as request:
            result = newapi_client.do_checkin("token", "123", "secret", "https://api.test")

        self.assertTrue(result["success"])
        self.assertTrue(request.call_args.kwargs["stream"])
        self.assertTrue(response.closed)

    def test_checkin_rejects_oversized_stream_and_closes_response(self) -> None:
        response = _Response([b"not read"], content_length=str(newapi_client.MAX_CHECKIN_RESPONSE_BYTES + 1))
        with patch.object(newapi_client.http_client, "request", return_value=response):
            result = newapi_client.do_checkin("token", "123", "secret", "https://api.test")

        self.assertIn("超过 1 MiB", result["_error"])
        self.assertTrue(response.closed)

    def test_checkin_rejects_oversized_chunk_without_retaining_body(self) -> None:
        response = _Response([b"x" * 64 * 1024] * 17)
        with patch.object(newapi_client.http_client, "request", return_value=response):
            result = newapi_client.do_checkin("token", "123", "secret", "https://api.test")

        self.assertIn("超过 1 MiB", result["_error"])
        self.assertEqual(result["_raw"], "")
        self.assertTrue(response.closed)


if __name__ == "__main__":
    unittest.main()
