from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import patch

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.http_proxy import HttpClient


class _Response:
    def __init__(self, chunks: list[bytes], headers: dict[str, str] | None = None) -> None:
        self._chunks = chunks
        self.headers = headers or {}
        self.closed = False
        self.chunk_size = None

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, *, chunk_size: int):
        self.chunk_size = chunk_size
        yield from self._chunks

    def close(self) -> None:
        self.closed = True


def test_get_json_streams_and_closes_bounded_response() -> None:
    response = _Response([b'{"data":', b"[]}"], {"content-length": "11"})
    client = HttpClient(retries=0)

    with patch(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.http_proxy._requests_call",
        return_value=response,
    ) as request:
        result = client.get_json(
            "https://example.invalid/data",
            max_bytes=32,
            timeout=3,
            use_config_proxy=False,
            stream=False,
        )

    assert result == {"data": []}
    assert response.chunk_size == 64 * 1024
    assert response.closed
    assert request.call_args.args[2]["stream"] is True


def test_get_json_rejects_large_content_length_and_closes_response() -> None:
    response = _Response([], {"content-length": "33"})
    client = HttpClient(retries=0)

    with patch(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.http_proxy._requests_call",
        return_value=response,
    ):
        with pytest.raises(ValueError, match="size limit"):
            client.get_json(
                "https://example.invalid/data",
                max_bytes=32,
                use_config_proxy=False,
            )

    assert response.closed
    assert response.chunk_size is None


def test_get_json_rejects_streamed_overflow_without_content_length() -> None:
    response = _Response([b"a" * 20, b"b" * 13])
    client = HttpClient(retries=0)

    with patch(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.http_proxy._requests_call",
        return_value=response,
    ):
        with pytest.raises(ValueError, match="while streaming"):
            client.get_json(
                "https://example.invalid/data",
                max_bytes=32,
                use_config_proxy=False,
            )

    assert response.closed


def test_entertainment_json_helper_passes_response_limit() -> None:
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment import command

    with patch.object(
        command.entertainment_application.external_query_provider.http_client,
        "get_json",
        return_value={"data": []},
    ) as get_json:
        result = command._get_json_api_sync(
            "https://example.invalid/data", timeout=15, max_bytes=1024
        )

    assert result == {"data": []}
    get_json.assert_called_once_with(
        "https://example.invalid/data", params=None, timeout=15, max_bytes=1024
    )


def test_steam_compatibility_path_sets_a_bounded_response_limit() -> None:
    source = (
        Path(__file__).resolve().parents[1]
        / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_entertainment/mod/steam_plus_one.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)

    assert "STEAM_RESPONSE_MAX_BYTES = 1024 * 1024" in source
    bounded_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "get_json_api"
        and any(keyword.arg == "max_bytes" for keyword in node.keywords)
    ]
    assert len(bounded_calls) == 1
    assert next(
        keyword.value
        for keyword in bounded_calls[0].keywords
        if keyword.arg == "max_bytes"
    ).id == "STEAM_RESPONSE_MAX_BYTES"
