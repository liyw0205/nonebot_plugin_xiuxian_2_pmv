from __future__ import annotations

import asyncio
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot
nonebot.init()

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils


class _StatisticsReader:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error

    def get_fields(self, user_id, table_name, *, raise_errors=False):
        assert user_id == "u"
        assert table_name == "statistics"
        if self.error is not None:
            if raise_errors:
                raise self.error
            return None
        return self.result


def test_statistics_empty_row_remains_an_empty_state():
    reader = _StatisticsReader()
    with (
        patch.object(utils, "_player_data_manager", return_value=reader),
        patch.object(utils, "get_impersonating_target", return_value=None),
    ):
        assert utils.get_statistics_data("u", raise_errors=True) == {}


def test_statistics_read_failure_preserves_specific_reason_and_cause():
    source_error = sqlite3.OperationalError("isolated statistics read refusal")
    reader = _StatisticsReader(error=source_error)
    with (
        patch.object(utils, "_player_data_manager", return_value=reader),
        patch.object(utils, "get_impersonating_target", return_value=None),
        patch.object(utils.logger, "opt") as log_opt,
    ):
        try:
            utils.get_statistics_data("u", raise_errors=True)
        except DomainError as error:
            assert error.code == "statistics_read_failed"
            assert error.details == {
                "reason": "statistics_read_failed",
                "subject": "statistics",
            }
            assert error.__cause__ is source_error
        else:
            raise AssertionError("statistics read failure was suppressed")
    log_opt.assert_called_once_with(exception=source_error)
    log_opt.return_value.error.assert_called_once_with(
        "修仙数据统计表读取失败 subject=statistics"
    )


def test_statistics_read_failure_message_keeps_plain_markdown_and_fallback_contracts():
    bot_config = SimpleNamespace(
        markdown_status=True,
        markdown_id="",
        markdown_id2="",
        markdown_button_status=False,
        message_optimization=False,
        img=False,
        at_sender=False,
    )
    bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    event = SimpleNamespace(message_id=1001, get_user_id=lambda: "u")
    reply = "修仙数据处理提示：统计数据读取失败，请稍后重试或联系管理员核查数据源。"

    plain = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=bot_config),
        patch.object(utils, "_allow_native_markdown", return_value=False),
        patch.object(utils, "_send_event_message", plain),
    ):
        asyncio.run(utils.handle_send(bot, event, reply))
    plain.assert_awaited_once()
    assert reply in str(plain.await_args.kwargs["message"])

    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    markdown = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=bot_config),
        patch.object(utils, "_allow_native_markdown", return_value=True),
        patch.object(utils, "handle_send_markdown", markdown),
    ):
        asyncio.run(utils.handle_send(qq, event, reply))
    markdown.assert_awaited_once_with(qq, event, reply, title=None, button_id=None)

    fallback = AsyncMock()
    with (
        patch.object(utils, "XiuConfig", return_value=bot_config),
        patch.object(utils, "_allow_native_markdown", return_value=True),
        patch.object(
            utils,
            "handle_send_markdown",
            AsyncMock(side_effect=RuntimeError("isolated markdown failure")),
        ),
        patch.object(utils, "handle_send2", fallback),
    ):
        asyncio.run(utils.handle_send(qq, event, reply))
    fallback.assert_awaited_once_with(qq, event, reply)
