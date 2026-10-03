import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as dungeon


def test_exit_schema_missing_never_reads_manager_or_reports_success():
    bot = object()
    event = SimpleNamespace(message_id="message-1")
    application = Mock()
    application.session_operation.return_value = {
        "status": "schema_missing", "dungeon_status": "",
    }
    manager = Mock()
    send = AsyncMock()
    with patch.object(dungeon, "assign_bot", AsyncMock(return_value=(bot, "g"))), patch.object(
        dungeon, "check_user", return_value=(True, {"user_id": "u"}, "")
    ), patch.object(dungeon, "dungeon_application", application), patch.object(
        dungeon, "dungeon_manager", manager
    ), patch.object(dungeon, "handle_send", send), patch.object(
        dungeon.dungeon_exit, "finish", AsyncMock()
    ):
        asyncio.run(dungeon.handle_dungeon_exit(bot, event))
    manager.get_player_status.assert_not_called()
    manager.get_dungeon_progress.assert_not_called()
    application.session_transition.assert_not_called()
    send.assert_awaited_once_with(bot, event, "副本数据结构未就绪，请稍后重试。")
