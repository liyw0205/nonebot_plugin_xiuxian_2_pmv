from __future__ import annotations

import json
import unittest
from unittest.mock import patch

import nonebot

nonebot.init()

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
        with patch.object(newapi_client.checkin_http_client, "request", return_value=response) as request:
            result = newapi_client.do_checkin("token", "123", "secret", "https://api.test")

        self.assertTrue(result["success"])
        self.assertEqual(newapi_client.checkin_http_client.retries, 0)
        self.assertEqual(request.call_args.kwargs["timeout"], newapi_client.CHECKIN_REQUEST_TIMEOUT)
        self.assertTrue(request.call_args.kwargs["stream"])
        self.assertTrue(response.closed)

    def test_checkin_rejects_oversized_stream_and_closes_response(self) -> None:
        response = _Response([b"not read"], content_length=str(newapi_client.MAX_CHECKIN_RESPONSE_BYTES + 1))
        with patch.object(newapi_client.checkin_http_client, "request", return_value=response):
            result = newapi_client.do_checkin("token", "123", "secret", "https://api.test")

        self.assertIn("超过 1 MiB", result["_error"])
        self.assertTrue(response.closed)

    def test_checkin_rejects_oversized_chunk_without_retaining_body(self) -> None:
        response = _Response([b"x" * 64 * 1024] * 17)
        with patch.object(newapi_client.checkin_http_client, "request", return_value=response):
            result = newapi_client.do_checkin("token", "123", "secret", "https://api.test")

        self.assertIn("超过 1 MiB", result["_error"])
        self.assertEqual(result["_raw"], "")
        self.assertTrue(response.closed)

    def test_user_info_uses_bounded_no_retry_client(self) -> None:
        payload = json.dumps({"success": True, "data": {"username": "u"}}).encode()
        response = _Response([payload], content_length=str(len(payload)))
        with patch.object(newapi_client.info_http_client, "request", return_value=response) as request:
            result = newapi_client.fetch_user_self("token", "123", "secret", "https://api.test")

        self.assertTrue(result["success"])
        self.assertEqual(newapi_client.info_http_client.retries, 0)
        self.assertEqual(request.call_args.kwargs["timeout"], newapi_client.INFO_REQUEST_TIMEOUT)
        self.assertTrue(request.call_args.kwargs["stream"])
        self.assertTrue(response.closed)

    def test_user_info_rejects_oversized_response(self) -> None:
        response = _Response([b"not read"], content_length=str(newapi_client.MAX_INFO_RESPONSE_BYTES + 1))
        with patch.object(newapi_client.info_http_client, "request", return_value=response):
            result = newapi_client.fetch_user_self("token", "123", "secret", "https://api.test")

        self.assertIn("超过 512 KiB", result["_error"])
        self.assertTrue(response.closed)

    def test_user_info_reply_keeps_truncation_notice_inside_byte_limit(self) -> None:
        title = "【NewAPI 用户信息】"
        block = "昵称：用户" * 100
        notice = "部分用户信息因消息长度限制未展示。"
        max_bytes = sum(
            len(value.encode("utf-8"))
            for value in (title + "\n\n", block + "\n\n", notice)
        )

        omitted_block = "second account " * 10
        reply = newapi_client.format_user_info_reply([block, omitted_block], max_bytes=max_bytes)

        self.assertLessEqual(len(reply.encode("utf-8")), max_bytes)
        self.assertIn(block, reply)
        self.assertIn(notice, reply)
        self.assertNotIn("second account", reply)


if __name__ == "__main__":
    unittest.main()
