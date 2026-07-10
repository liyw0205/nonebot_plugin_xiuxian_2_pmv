from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

import nonebot
import requests

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.http_proxy import HttpClient


class HttpClientTests(unittest.TestCase):
    def test_get_json_validates_root_type(self) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"ok": True}
        target = "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.http_proxy._requests_call"
        with patch(target, return_value=response):
            self.assertEqual(
                HttpClient(retries=0).get_json("https://example.invalid"),
                {"ok": True},
            )

        response.json.return_value = []
        with patch(target, return_value=response):
            with self.assertRaises(ValueError):
                HttpClient(retries=0).get_json("https://example.invalid")

    def test_transient_connection_error_is_retried(self) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        target = "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.http_proxy._requests_call"
        with patch(
            target,
            side_effect=[requests.ConnectionError("temporary"), response],
        ) as call:
            result = HttpClient(retries=1).request("GET", "https://example.invalid")
        self.assertIs(result, response)
        self.assertEqual(call.call_count, 2)

    def test_download_enforces_streaming_size_limit(self) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.headers = {}
        response.iter_content.return_value = [b"12", b"34"]
        target = "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.http_proxy._requests_call"
        with patch(target, return_value=response):
            with self.assertRaises(ValueError):
                HttpClient(retries=0).download(
                    "https://example.invalid",
                    max_bytes=3,
                )

    def test_status_check_can_be_deferred_to_protocol_client(self) -> None:
        response = Mock()
        target = "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.http_proxy._requests_call"
        with patch(target, return_value=response):
            self.assertIs(
                HttpClient(retries=0).request(
                    "PROPFIND",
                    "https://example.invalid",
                    check_status=False,
                ),
                response,
            )
        response.raise_for_status.assert_not_called()


if __name__ == "__main__":
    unittest.main()
