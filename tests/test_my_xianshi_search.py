"""`我的仙肆` 的关键词筛选与翻页续查。

`docs/goals/goal-list-search-remaining.md` 把 `我的仙肆` 明确排除在六个视图之外
（「`我的仙肆` 只收页码不在本卡范围」）。它因此保留着旧行为：`int(command_text)`
抛 `ValueError` 后静默回到第 1 页未筛选集合，翻页按钮也写死页码——玩家输入物品名
毫无反应，筛选后一翻页就丢掉筛选。

本套件驱动注册的 handler（patch `check_user`/`assign_bot`/数据源/发送缝隙），钉住：
关键词过滤、筛选后第 2 页仍是筛选结果、翻页命令携带关键词、无关键词保持旧页码，
以及「筛选无命中」与「摊位本来就为空」两种提示的区分。
"""

from __future__ import annotations

import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()

from nonebot.exception import FinishedException

from nonebot_plugin_xiuxian_2.features.base.keyword_page import empty_notice

PACKAGE = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2"
TRADE_SRC = (PACKAGE / "xiuxian" / "xiuxian_trade" / "__init__.py").read_text(encoding="utf-8")

BOT = SimpleNamespace(self_id="bot")
EVENT = SimpleNamespace(get_user_id=lambda: "u1")
USER = {"user_id": "u1", "user_name": "道友"}


def _args(text: str):
    return SimpleNamespace(extract_plain_text=lambda: text)


def _listing(name: str, price: int = 600000, quantity: int = 1, listing_id: str = ""):
    return {
        "id": listing_id or name,
        "name": name,
        "price": price,
        "quantity": quantity,
        "user_id": "u1",
    }


class MyXianshiHarness(unittest.TestCase):
    """真实 handler + 受控数据源/发送缝隙。"""

    def _run(self, text: str, rows, *, trades=None):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_trade as module

        captured = {"msgs": [], "sends": [], "page": None, "title": None}

        async def send_msg(bot, event, name, self_id, msg_list, title=None, page=None):
            captured["msgs"] = list(msg_list)
            captured["page"] = page
            captured["title"] = title

        async def handle_send(bot, event, msg, *args, **kwargs):
            captured["sends"].append(msg)

        with (
            patch.object(module, "check_user", lambda ev: (True, dict(USER), "")),
            patch.object(module, "assign_bot", AsyncMock(return_value=(BOT, None))),
            patch.object(
                module.trade_application,
                "xianshi_get_items",
                lambda **kwargs: [dict(row) for row in rows],
            ),
            patch.object(
                module.trade_application,
                "xianshi_my_trades",
                lambda **kwargs: list(trades or []),
            ),
            patch.object(module, "send_msg_handler", send_msg),
            patch.object(module, "handle_send", handle_send),
        ):
            try:
                asyncio.run(module.my_xian_shop_(BOT, EVENT, _args(text)))
            except FinishedException:
                pass
        captured["joined"] = "\n".join(captured["msgs"])
        return captured


class KeywordFilterTests(MyXianshiHarness):
    def setUp(self) -> None:
        self.rows = [
            _listing("灵芝草"),
            _listing("灵芝露"),
            _listing("玄铁"),
            _listing("Sword Fragment"),
        ]

    def test_keyword_filters_own_listings(self) -> None:
        result = self._run("灵芝", self.rows)
        self.assertIn("灵芝草", result["joined"])
        self.assertIn("灵芝露", result["joined"])
        self.assertNotIn("玄铁", result["joined"])
        self.assertIn("筛选「灵芝」", result["joined"])
        self.assertIn("第 1/1 页", result["joined"])

    def test_keyword_is_case_insensitive(self) -> None:
        result = self._run("sword", self.rows)
        self.assertIn("Sword Fragment", result["joined"])
        self.assertNotIn("灵芝草", result["joined"])

    def test_keyword_matches_listing_id(self) -> None:
        rows = [_listing("玄铁", listing_id="listing-77"), _listing("灵芝草")]
        result = self._run("listing-77", rows)
        self.assertIn("玄铁", result["joined"])
        self.assertNotIn("灵芝草", result["joined"])

    def test_filtered_second_page_stays_filtered(self) -> None:
        rows = [_listing(f"灵芝{i:02d}") for i in range(1, 22)] + [_listing("玄铁")]
        result = self._run("灵芝 2", rows)
        self.assertIn("灵芝21", result["joined"])
        self.assertNotIn("灵芝01", result["joined"])
        self.assertNotIn("玄铁", result["joined"])
        self.assertIn("第 2/2 页", result["joined"])

    def test_page_hint_and_button_carry_the_keyword(self) -> None:
        rows = [_listing(f"灵芝{i:02d}") for i in range(1, 22)] + [_listing("玄铁")]
        first = self._run("灵芝", rows)
        self.assertEqual(first["page"][1], "我的仙肆 灵芝 2")
        second = self._run("灵芝 2", rows)
        self.assertEqual(second["page"][1], "我的仙肆 灵芝 2")

    def test_no_match_uses_the_filtered_empty_notice(self) -> None:
        result = self._run("不存在", self.rows)
        self.assertEqual(result["sends"], [empty_notice("不存在", "仙肆摊位")])
        self.assertEqual(result["msgs"], [])

    def test_overflow_page_clamps_to_the_last_page(self) -> None:
        rows = [_listing(f"灵芝{i:02d}") for i in range(1, 26)]
        result = self._run("999", rows)
        self.assertIn("第 2/2 页", result["joined"])
        self.assertIn("灵芝21", result["joined"])


class BackwardCompatibilityTests(MyXianshiHarness):
    def setUp(self) -> None:
        self.rows = [_listing(f"物品{i:02d}") for i in range(1, 26)]

    def test_page_only_output_is_unchanged(self) -> None:
        result = self._run("2", self.rows)
        self.assertIn("第 2/2 页", result["joined"])
        self.assertIn("物品21", result["joined"])
        self.assertNotIn("筛选「", result["joined"])
        self.assertEqual(result["page"][1], "我的仙肆 2")
        self.assertEqual(result["page"][0], "翻页")
        self.assertEqual(result["page"][-1], "2/2")

    def test_no_arguments_shows_first_page_without_filter_line(self) -> None:
        result = self._run("", self.rows)
        self.assertIn("物品01", result["joined"])
        self.assertNotIn("筛选「", result["joined"])
        self.assertEqual(result["page"][1], "我的仙肆 2")

    def test_empty_stall_keeps_the_original_notice(self) -> None:
        result = self._run("灵芝", [])
        self.assertEqual(
            result["sends"], ["您在仙肆中没有上架任何物品！使用【我的仙肆 记录】查看最近成交。"]
        )

    def test_history_keywords_still_route_to_trades(self) -> None:
        for text in ("记录", "成交", "历史"):
            with self.subTest(text=text):
                result = self._run(text, self.rows, trades=[])
                self.assertEqual(result["sends"], ["您最近没有仙肆成交记录。"])


class SourceContractTests(unittest.TestCase):
    def test_handler_uses_the_shared_page_command(self) -> None:
        self.assertIn(
            'next_page_command("我的仙肆", keyword, current_page, total_pages)', TRADE_SRC
        )
        self.assertNotIn('f"我的仙肆 {current_page + 1}"', TRADE_SRC)

    def test_handler_parses_keyword_and_page(self) -> None:
        self.assertIn('parse_page_arg(command_text)', TRADE_SRC)
        self.assertNotIn("current_page = int(command_text)", TRADE_SRC)

    def test_handler_filters_with_the_shared_matcher(self) -> None:
        self.assertIn("matches_keyword(", TRADE_SRC)
        self.assertIn('empty_notice(keyword, "仙肆摊位")', TRADE_SRC)


if __name__ == "__main__":
    unittest.main()
