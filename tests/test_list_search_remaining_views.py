"""剩余六处列表视图的检索与翻页续查。

前一切片只覆盖背包/宠物/宗门等视图；本套件钉住最后六处：
`世界BOSS商店`、`通天塔商店`、`历练商店`、`传承背包`、`仙肆查看`、
`查看修仙界物品`。每个视图都真实调用注册的 handler（patch 数据源与发送缝隙），
断言筛选命中、筛选后第 2 页仍是筛选结果、翻页提示/按钮携带关键词、无关键词保持
旧页码形式，以及筛选无结果与集合为空分开提示。
"""

from __future__ import annotations

import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from urllib.parse import quote

import nonebot

nonebot.init()

from nonebot.exception import FinishedException

from nonebot_plugin_xiuxian_2.features.base.keyword_page import (
    page_command,
    parse_typed_page_arg,
)

BOT = SimpleNamespace(self_id="bot")
EVENT = SimpleNamespace(get_user_id=lambda: "u1")


class FakeItems:
    def __init__(self, rows):
        self.rows = rows

    def get_data_by_item_id(self, item_id):
        return self.rows.get(str(item_id)) or self.rows.get(item_id)

    def get_data_by_item_type(self, types):
        return {key: row for key, row in self.rows.items() if row.get("type") in types}


def _args(text: str):
    return SimpleNamespace(extract_plain_text=lambda: text)


def _shop(ids, *, prefix="灵草"):
    rows, shop = {}, {}
    for index, item_id in enumerate(ids, 1):
        rows[str(item_id)] = {"name": f"{prefix}{index}号", "desc": "一株灵草"}
        shop[str(item_id)] = {"cost": 10, "weekly_limit": 1}
    return rows, shop


class TypedArgTests(unittest.TestCase):
    TYPES = ["功法", "辅修功法", "神通", "丹药", "合成丹药", "药材"]

    def test_type_only(self) -> None:
        self.assertEqual(
            parse_typed_page_arg("神通", self.TYPES),
            {"type": "神通", "keyword": "", "page": 1},
        )

    def test_type_with_adjacent_page(self) -> None:
        self.assertEqual(
            parse_typed_page_arg("神通2", self.TYPES),
            {"type": "神通", "keyword": "", "page": 2},
        )

    def test_type_keyword_and_page(self) -> None:
        self.assertEqual(
            parse_typed_page_arg("神通 剑法 3", self.TYPES),
            {"type": "神通", "keyword": "剑法", "page": 3},
        )

    def test_longest_type_wins(self) -> None:
        self.assertEqual(
            parse_typed_page_arg("辅修功法 2", self.TYPES)["type"], "辅修功法"
        )
        self.assertEqual(
            parse_typed_page_arg("合成丹药2", self.TYPES),
            {"type": "合成丹药", "keyword": "", "page": 2},
        )

    def test_type_with_keyword_only(self) -> None:
        self.assertEqual(
            parse_typed_page_arg("药材 灵芝", self.TYPES),
            {"type": "药材", "keyword": "灵芝", "page": 1},
        )

    def test_unknown_type_is_empty(self) -> None:
        self.assertEqual(
            parse_typed_page_arg("灵草", self.TYPES),
            {"type": "", "keyword": "", "page": 1},
        )
        self.assertEqual(
            parse_typed_page_arg("", self.TYPES),
            {"type": "", "keyword": "", "page": 1},
        )

    def test_page_command_round_trips_through_typed_parser(self) -> None:
        """翻页命令必须能被同一个解析器读回：类型/关键词/页码都不丢。"""
        for prefix, item_type, keyword, page in (
            ("仙肆查看", "药材", "灵芝", 2),
            ("仙肆查看", "药材", "", 3),
            ("查看修仙界物品", "神通", "剑法", 2),
            ("查看修仙界物品", "神通", "", 2),
        ):
            with self.subTest(prefix=prefix, item_type=item_type, keyword=keyword, page=page):
                rendered = page_command(f"{prefix}{item_type}", keyword, page)
                args = rendered[len(prefix):].strip()
                parsed = parse_typed_page_arg(args, self.TYPES + ["药材", "神通"])
                self.assertEqual(
                    parsed,
                    {"type": item_type, "keyword": keyword, "page": page},
                )


class BossShopSearchTests(unittest.TestCase):
    def _run(self, text: str) -> dict:
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_boss as module

        captured: dict = {}

        async def send_msg(*args, **kwargs):
            captured["msgs"] = list(args[4]) if len(args) > 4 else []
            captured["page"] = kwargs.get("page")

        async def handle_send(bot, event, message, **kwargs):
            captured["notice"] = message

        rows, shop = _shop(range(1, 13))
        extra, extra_shop = _shop(range(13, 16), prefix="玄铁")
        rows.update(extra)
        shop.update(extra_shop)
        handler = module.boss_integral_store.handlers[0].call
        with (
            patch.object(module, "check_user", lambda event: (True, {"user_id": "u1"}, "")),
            patch.object(module, "assign_bot", AsyncMock(return_value=(BOT, None))),
            patch.object(module, "config", {"世界积分商品": shop}),
            patch.object(module, "_items", lambda: FakeItems(rows)),
            patch.object(
                module.boss_integral_application,
                "get_integral",
                lambda uid: SimpleNamespace(integral=100),
            ),
            patch.object(module.boss_application, "weekly_purchases", lambda uid: {}),
            patch.object(module, "send_msg_handler", send_msg),
            patch.object(module, "handle_send", handle_send),
        ):
            try:
                asyncio.run(handler(BOT, EVENT, _args(text)))
            except FinishedException:
                pass
        return captured

    def test_keyword_filters_and_pages_carry_it(self) -> None:
        first = self._run("灵草")
        self.assertEqual("世界BOSS商店 灵草 2", first["page"][1])
        joined = "\n".join(first["msgs"])
        self.assertIn("灵草1号", joined)
        self.assertNotIn("玄铁", joined)
        self.assertIn("第1/2页", joined)

        second = self._run("灵草 2")
        joined = "\n".join(second["msgs"])
        self.assertIn("灵草11号", joined)
        self.assertIn("灵草12号", joined)
        self.assertNotIn("灵草1号", joined)
        self.assertEqual("世界BOSS商店 灵草 2", second["page"][1])

    def test_page_only_query_keeps_old_form(self) -> None:
        captured = self._run("2")
        self.assertEqual("世界BOSS商店 2", captured["page"][1])
        self.assertIn("灵草11号", "\n".join(captured["msgs"]))

    def test_no_match_uses_filter_notice(self) -> None:
        captured = self._run("不存在")
        self.assertIn("不存在", captured["notice"])
        self.assertIn("世界积分商店", captured["notice"])


class TowerShopSearchTests(unittest.TestCase):
    def _run(self, text: str) -> dict:
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_tower as module

        captured: dict = {}

        async def send_msg(*args, **kwargs):
            captured["msgs"] = list(args[4]) if len(args) > 4 else []
            captured["page"] = kwargs.get("page")

        async def handle_send(bot, event, message, **kwargs):
            captured["notice"] = message

        rows, shop = _shop(range(1, 16))
        handler = module.tower_shop.handlers[0].call
        with (
            patch.object(module, "check_user", lambda event: (True, {"user_id": "u1"}, "")),
            patch.object(module, "assign_bot", AsyncMock(return_value=(BOT, None))),
            patch.object(module.tower_data, "config", {"商店商品": shop}),
            patch.object(
                module.tower_limit, "get_user_tower_info", lambda uid: {"score": 0}
            ),
            patch.object(module.tower_limit, "get_weekly_purchases", lambda uid, iid: 0),
            patch.object(module, "_items", lambda: FakeItems(rows)),
            patch.object(module, "send_msg_handler", send_msg),
            patch.object(module, "handle_send", handle_send),
        ):
            try:
                asyncio.run(handler(BOT, EVENT, _args(text)))
            except FinishedException:
                pass
        return captured

    def test_keyword_filters_and_pages_carry_it(self) -> None:
        first = self._run("灵草")
        self.assertEqual("通天塔商店 灵草 2", first["page"][1])
        self.assertIn("灵草1号", "\n".join(first["msgs"]))

        second = self._run("灵草 2")
        self.assertEqual("通天塔商店 灵草 2", second["page"][1])
        joined = "\n".join(second["msgs"])
        self.assertIn("灵草11号", joined)
        self.assertNotIn("灵草1号", joined)

    def test_no_match_uses_filter_notice(self) -> None:
        self.assertIn("不存在", self._run("不存在")["notice"])


class TrainingShopSearchTests(unittest.TestCase):
    def _run(self, text: str) -> dict:
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_training as module

        captured: dict = {}

        async def send_msg(*args, **kwargs):
            captured["msgs"] = list(args[4]) if len(args) > 4 else []
            captured["page"] = kwargs.get("page")

        async def handle_send(bot, event, message, **kwargs):
            captured["notice"] = message

        rows, shop = _shop(range(1, 16))
        handler = module.training_shop.handlers[0].call
        with (
            patch.object(module, "check_user", lambda event: (True, {"user_id": "u1"}, "")),
            patch.object(module, "assign_bot", AsyncMock(return_value=(BOT, None))),
            patch.object(module.training_data, "config", {"商店商品": shop}),
            patch.object(
                module.training_application,
                "get_state",
                lambda uid: {"weekly_purchases": {}, "points": 0},
            ),
            patch.object(module, "_items", lambda: FakeItems(rows)),
            patch.object(module, "send_msg_handler", send_msg),
            patch.object(module, "handle_send", handle_send),
        ):
            try:
                asyncio.run(handler(BOT, EVENT, _args(text)))
            except FinishedException:
                pass
        return captured

    def test_keyword_filters_and_pages_carry_it(self) -> None:
        first = self._run("灵草")
        self.assertEqual("历练商店 灵草 2", first["page"][1])
        second = self._run("灵草 2")
        self.assertEqual("历练商店 灵草 2", second["page"][1])
        self.assertIn("灵草11号", "\n".join(second["msgs"]))

    def test_no_match_uses_filter_notice(self) -> None:
        self.assertIn("不存在", self._run("不存在")["notice"])


class ImpartBagSearchTests(unittest.TestCase):
    def _run(self, text: str) -> dict:
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_impart as module

        captured: dict = {}

        async def send_msg(*args, **kwargs):
            captured["msgs"] = list(args[4]) if len(args) > 4 else []
            captured["page"] = kwargs.get("page")

        async def handle_send(bot, event, message, **kwargs):
            captured["notice"] = message

        cards = {f"画屏春-{index:02d}": 1 for index in range(1, 32)}
        cards.update({f"其他-{index}": 2 for index in range(1, 5)})
        handler = module.impart_back.handlers[0].call
        with (
            patch.object(module, "check_user", lambda event: (True, {"user_id": "u1"}, "")),
            patch.object(module, "assign_bot", AsyncMock(return_value=(BOT, None))),
            patch.object(module, "_impart_state", lambda uid: SimpleNamespace()),
            patch.object(module.impart_application, "cards", lambda uid: dict(cards)),
            patch.object(module, "send_msg_handler", send_msg),
            patch.object(module, "handle_send", handle_send),
        ):
            try:
                asyncio.run(handler(BOT, EVENT, _args(text)))
            except FinishedException:
                pass
        return captured

    def test_keyword_filters_and_stats_follow_it(self) -> None:
        first = self._run("画屏春")
        joined = "\n".join(first["msgs"])
        self.assertIn("画屏春-01", joined)
        self.assertNotIn("其他-1", joined)
        self.assertIn("卡片种类：31/106", joined)
        self.assertIn("总卡片数：31", joined)
        self.assertIn("筛选：「画屏春」", joined)
        self.assertEqual("传承背包 画屏春 2", first["page"][1])
        self.assertIn("输入【传承背包 画屏春 2】查看其他页", joined)

        # 排序沿用 (-数量, 卡名)：补零后第 2 页只剩最后一张
        second = self._run("画屏春 2")
        joined = "\n".join(second["msgs"])
        self.assertIn("画屏春-31", joined)
        self.assertNotIn("画屏春-30", joined)
        self.assertNotIn("其他-", joined)

    def test_no_match_uses_filter_notice(self) -> None:
        self.assertIn("不存在", self._run("不存在")["notice"])

    def test_page_only_keeps_old_form(self) -> None:
        captured = self._run("2")
        self.assertEqual("传承背包 2", captured["page"][1])
        self.assertIn("第2/2页", "\n".join(captured["msgs"]))


class XianshiViewSearchTests(unittest.TestCase):
    def _items(self):
        rows = []
        for index in range(1, 13):
            rows.append(
                {
                    "id": index,
                    "name": f"灵芝{index}",
                    "price": 10,
                    "quantity": 1,
                    "user_id": 0,
                }
            )
        for index in range(13, 16):
            rows.append(
                {
                    "id": index,
                    "name": f"人参{index}",
                    "price": 10,
                    "quantity": 1,
                    "user_id": 0,
                }
            )
        return rows

    def _run(self, text: str, *, markdown: bool = False) -> dict:
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_trade as module

        captured: dict = {}

        async def send_msg(*args, **kwargs):
            captured["msgs"] = list(args[4]) if len(args) > 4 else []
            captured["page"] = kwargs.get("page")

        async def handle_send(bot, event, message, **kwargs):
            captured["message"] = message
            captured["fallback"] = kwargs.get("fallback_msg")

        class _XiuConfig:
            def __init__(self, *args, **kwargs) -> None:
                self.markdown_status = markdown

        handler = module.xiuxian_shop_view.handlers[0].call
        with (
            patch.object(module, "check_user", lambda event: (True, {"user_id": "u1"}, "")),
            patch.object(module, "assign_bot", AsyncMock(return_value=(BOT, None))),
            patch.object(module, "XiuConfig", _XiuConfig),
            patch.object(
                module.trade_application,
                "xianshi_get_items",
                lambda **kwargs: list(self._items()),
            ),
            patch.object(module, "send_msg_handler", send_msg),
            patch.object(module, "handle_send", handle_send),
        ):
            try:
                asyncio.run(handler(BOT, EVENT, _args(text)))
            except FinishedException:
                pass
        return captured

    def test_keyword_filters_and_button_carries_it(self) -> None:
        first = self._run("药材 灵芝")
        self.assertEqual("仙肆查看药材 灵芝 2", first["page"][1])
        joined = "\n".join(first["msgs"])
        self.assertIn("灵芝1 ", joined)
        self.assertNotIn("人参", joined)
        self.assertIn("第 1/2 页", joined)

        # 排序沿用物品名（字典序），筛选后第 2 页只有最后两件
        second = self._run("药材 灵芝 2")
        self.assertEqual("仙肆查看药材 灵芝 2", second["page"][1])
        joined = "\n".join(second["msgs"])
        self.assertIn("灵芝8", joined)
        self.assertIn("灵芝9", joined)
        self.assertNotIn("灵芝1 ", joined)
        self.assertNotIn("人参", joined)
        self.assertIn("第 2/2 页", joined)

    def test_legacy_type_adjacent_page(self) -> None:
        captured = self._run("药材2")
        self.assertEqual("仙肆查看药材 2", captured["page"][1])
        joined = "\n".join(captured["msgs"])
        # 旧写法不带关键词：仍是未筛选集合的第 2 页
        self.assertIn("灵芝5", joined)
        self.assertIn("第 2/2 页", joined)

    def test_markdown_next_link_carries_keyword(self) -> None:
        captured = self._run("药材 灵芝", markdown=True)
        expected = quote("仙肆查看药材 灵芝 2")
        self.assertIn(expected, captured["message"])
        self.assertIn("](mqqapi", captured["message"])

    def test_no_match_uses_filter_notice(self) -> None:
        self.assertIn("不存在", self._run("药材 不存在")["message"])


class BackItemViewSearchTests(unittest.TestCase):
    def _rows(self):
        rows = {}
        for index in range(1, 17):
            rows[str(index)] = {"name": f"剑法{index}", "type": "神通", "desc": "剑法"}
        rows["90"] = {"name": "刀法", "type": "神通", "desc": "刀法"}
        return rows

    def _run(self, text: str) -> dict:
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_back as module

        captured: dict = {}

        async def send_msg(*args, **kwargs):
            captured["msgs"] = list(args[4]) if len(args) > 4 else []
            captured["page"] = kwargs.get("page")

        async def handle_send(bot, event, message, **kwargs):
            captured["notice"] = message

        fake = FakeItems(self._rows())
        handler = module.chakan_wupin.handlers[0].call
        with (
            patch.object(module, "check_user", lambda event: (True, {"user_id": "u1"}, "")),
            patch.object(module, "assign_bot", AsyncMock(return_value=(BOT, None))),
            patch.object(module.items, "get_data_by_item_type", fake.get_data_by_item_type),
            patch.object(module, "get_item_msg", lambda item_id: f"效果{item_id}"),
            patch.object(module, "send_msg_handler", send_msg),
            patch.object(module, "handle_send", handle_send),
        ):
            try:
                asyncio.run(handler(BOT, EVENT, _args(text)))
            except FinishedException:
                pass
        return captured

    def test_keyword_filters_and_hint_uses_full_command(self) -> None:
        first = self._run("神通 剑法")
        self.assertEqual("查看修仙界物品神通 剑法 2", first["page"][1])
        joined = "\n".join(first["msgs"])
        self.assertIn("输入 查看修仙界物品神通 剑法 2 查看下一页", joined)
        self.assertIn("第 1/2 页", joined)

        # 神通列表只渲染 ID 与效果，名称不出现；16 条命中，每页 15 条，第 2 页是最后 1 条
        second = self._run("神通 剑法 2")
        self.assertEqual("查看修仙界物品神通 剑法 2", second["page"][1])
        joined = "\n".join(second["msgs"])
        self.assertIn("ID：16", joined)
        self.assertNotIn("ID：90", joined)
        self.assertIn("第 2/2 页", joined)

    def test_legacy_type_adjacent_page(self) -> None:
        captured = self._run("神通2")
        self.assertEqual("查看修仙界物品神通 2", captured["page"][1])
        joined = "\n".join(captured["msgs"])
        # 旧写法不带关键词：17 条全量列表的第 2 页
        self.assertIn("ID：16", joined)
        self.assertIn("ID：90", joined)
        self.assertIn("第 2/2 页", joined)

    def test_no_match_uses_filter_notice(self) -> None:
        self.assertIn("不存在", self._run("神通 不存在")["notice"])


class SourceContractTests(unittest.TestCase):
    XIUXIAN = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"

    def test_no_literal_command_page_buttons_remain(self) -> None:
        root = self.XIUXIAN
        offenders = []
        for name, old in (
            ("xiuxian_boss/__init__.py", 'f"世界BOSS商店 {page + 1}"'),
            ("xiuxian_tower/__init__.py", 'f"通天塔商店 {page + 1}"'),
            ("xiuxian_training/__init__.py", 'f"历练商店 {page + 1}"'),
            ("xiuxian_impart/__init__.py", 'f"传承背包 {page + 1}"'),
            ("xiuxian_trade/__init__.py", 'f"仙肆查看{item_type} {current_page + 1}"'),
            ("xiuxian_back/__init__.py", 'f"查看修仙界物品{item_type} {current_page + 1}"'),
        ):
            if old in (root / name).read_text(encoding="utf-8"):
                offenders.append(name)
        self.assertEqual(offenders, [])

    def test_six_views_use_the_shared_parser_and_page_builder(self) -> None:
        """六个视图必须共用 keyword_page，不能各自写一套解析/翻页拼接。"""
        for name in (
            "xiuxian_boss/__init__.py",
            "xiuxian_tower/__init__.py",
            "xiuxian_training/__init__.py",
            "xiuxian_impart/__init__.py",
            "xiuxian_trade/__init__.py",
            "xiuxian_back/__init__.py",
        ):
            source = (self.XIUXIAN / name).read_text(encoding="utf-8")
            with self.subTest(file=name):
                self.assertIn("from ...features.base.keyword_page import (", source)
                self.assertIn("page_command(", source)
                self.assertIn("matches_keyword(", source)

    def test_help_texts_document_the_keyword_argument(self) -> None:
        """帮助文本必须写明关键词入口，否则玩家只知道页码用法。"""
        root = self.XIUXIAN
        cases = (
            ("xiuxian_trade/trade_help.py", "- 仙肆查看 类型 [关键词] [页码]"),
            ("xiuxian_back/__init__.py", "- 查看修仙界物品+类型 [关键词] [页码]"),
            ("xiuxian_boss/__init__.py", "- 世界BOSS商店 [关键词|页码]"),
            ("xiuxian_tower/__init__.py", "- 通天塔商店 [关键词|页码]"),
            ("xiuxian_training/__init__.py", "- 历练商店 [关键词|页码]"),
            ("xiuxian_impart/__init__.py", "- 传承背包 [关键词|页码]"),
        )
        for name, entry in cases:
            source = (root / name).read_text(encoding="utf-8")
            with self.subTest(file=name):
                self.assertIn(entry, source)


if __name__ == "__main__":
    unittest.main()
