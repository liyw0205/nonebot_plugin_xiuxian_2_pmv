"""两处仍可达的指令崩溃：缺失传承行、空消息事件。

两者都来自 10/30–11/08 线上的真实 traceback，且**在当前源码里仍然可达**
（重启后没再触发 ≠ 已修复）：

1. ``features/impart/card_bonus.py`` 在玩家没有 ``xiuxian_impart`` 行时
   ``UPDATE`` 影响 0 行 → 抛 ``ValueError: impart user is missing``，
   整条传承指令（抽卡/祈愿/合成）失败；
2. ``xiuxian/on_compat.py`` 的 ``handle_event`` 包装层把空消息事件放行给
   nonebot，``TrieRule.get_value`` 直接取 ``message[0]`` → ``IndexError``，
   日志刷 ``Error while parsing command for event``。
"""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.impart.card_bonus import (
    BONUS_FIELDS,
    calculate_card_bonuses,
    refresh_card_bonuses,
)
from nonebot_plugin_xiuxian_2.xiuxian import on_compat
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import db_backend

DEFINITIONS = {
    "攻击卡": {"type": "impart_atk_per", "vale": 5},
    "双修卡": {"type": "impart_two_exp", "vale": 10},
}


def _prepare(root: Path, *, with_user: bool) -> Path:
    database = root / "impart.db"
    with db_backend.transaction(database) as conn:
        columns = ", ".join(f'"{field}" INTEGER NOT NULL DEFAULT 0' for field in BONUS_FIELDS)
        conn.execute(
            f"CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,{columns})"
        )
        conn.execute(
            "CREATE TABLE impart_cards(user_id TEXT,card_name TEXT,quantity INTEGER,"
            "PRIMARY KEY(user_id,card_name))"
        )
        conn.execute(
            "INSERT INTO impart_cards VALUES(%s,%s,%s)", ("u", "攻击卡", 5)
        )
        if with_user:
            conn.execute("INSERT INTO xiuxian_impart(user_id) VALUES(%s)", ("u",))
    return database


class CardBonusMissingUserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_missing_impart_row_does_not_raise(self) -> None:
        """没有 xiuxian_impart 行时跳过刷新，不再抛 ValueError 打崩指令。"""
        database = _prepare(self.root, with_user=False)
        with db_backend.transaction(database) as conn:
            bonuses = refresh_card_bonuses(conn, "u", DEFINITIONS, schema="main")

        self.assertEqual(bonuses["impart_atk_per"], 5 * (1 + 5 // 5))
        self.assertEqual(bonuses["impart_two_exp"], 0)
        self.assertEqual(set(bonuses), set(BONUS_FIELDS))

    def test_existing_row_is_still_refreshed(self) -> None:
        database = _prepare(self.root, with_user=True)
        with db_backend.transaction(database) as conn:
            refresh_card_bonuses(conn, "u", DEFINITIONS, schema="main")
        with db_backend.connection(database) as conn:
            row = conn.execute(
                'SELECT "impart_atk_per","impart_two_exp" FROM xiuxian_impart WHERE user_id=%s',
                ("u",),
            ).fetchone()
        self.assertEqual((10, 0), (row[0], row[1]))

    def test_pure_calculation_is_unchanged(self) -> None:
        """玩法上限保留：单卡有效张数封顶 25，加成按每 5 张 +1 档。"""
        bonuses = calculate_card_bonuses({"攻击卡": 999}, DEFINITIONS)
        self.assertEqual(5 * (1 + 25 // 5), bonuses["impart_atk_per"])


class EmptyMessageGuardTests(unittest.TestCase):
    def test_empty_message_is_skipped(self) -> None:
        """空消息事件不再交给 nonebot 的 TrieRule（原 IndexError 现场）。"""
        calls: list[object] = []

        async def handle_event(bot, event):
            calls.append(event)
            return "handled"

        wrapped = on_compat._wrap_handle_event(handle_event)
        result = asyncio.run(wrapped(object(), SimpleEvent(message=[])))

        self.assertIsNone(result)
        self.assertEqual([], calls)

    def test_non_message_events_still_delegate(self) -> None:
        """notice / request / meta 没有 ``message``，必须照常分发到 handler。"""
        calls: list[object] = []

        async def handle_event(bot, event):
            calls.append(event)
            return "handled"

        wrapped = on_compat._wrap_handle_event(handle_event)
        for event in (
            NoticeEvent(),      # 入群/退群等 notice
            RequestEvent(),     # 好友/入群请求
            MetaEvent(),        # meta_event（心跳不会被分发，但语义必须保留）
            SimpleEvent(message=None),  # 显式 message=None 的消息事件
        ):
            with self.subTest(event=type(event).__name__):
                self.assertEqual("handled", asyncio.run(wrapped(object(), event)))
        self.assertEqual(4, len(calls))

    def test_empty_check_distinguishes_missing_from_empty(self) -> None:
        self.assertFalse(on_compat._message_is_empty(NoticeEvent()))
        self.assertFalse(on_compat._message_is_empty(SimpleEvent(message=None)))
        self.assertTrue(on_compat._message_is_empty(SimpleEvent(message=[])))
        self.assertFalse(on_compat._message_is_empty(SimpleEvent(message=["签到"])))

    def test_normal_message_still_reaches_handle_event(self) -> None:
        calls: list[object] = []

        async def handle_event(bot, event):
            calls.append(event)
            return "handled"

        wrapped = on_compat._wrap_handle_event(handle_event)
        result = asyncio.run(wrapped(object(), SimpleEvent(message=["签到"])))

        self.assertEqual("handled", result)
        self.assertEqual(1, len(calls))

    def test_wrapper_is_idempotent(self) -> None:
        async def handle_event(bot, event):
            return "handled"

        once = on_compat._wrap_handle_event(handle_event)
        twice = on_compat._wrap_handle_event(once)

        self.assertIs(once, twice)
        self.assertTrue(getattr(once, "_xiuxian_on_compat_wrapped", False))


class SimpleEvent:
    """足够让包装层跑起来的最小消息事件。"""

    def __init__(self, *, message) -> None:
        self.message = message
        self.raw_message = ""
        self.is_tome = False
        self.message_type = "group"
        self.group_id = 1
        self.user_id = 2


class NoticeEvent:
    """notice 事件：没有 message 属性，只有各自字段。"""

    notice_type = "group_increase"
    group_id = 1
    user_id = 2


class RequestEvent:
    request_type = "friend"
    user_id = 2


class MetaEvent:
    meta_event_type = "heartbeat"


if __name__ == "__main__":
    unittest.main()
