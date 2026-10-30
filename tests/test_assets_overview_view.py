"""玩家「我的资产总览」视图（跨库个人资产一览）。

The gap this feature closes: a player's holdings live in two databases and five
tables — 灵石 (`user_xiuxian`), 灵庄 (`bank_accounts`), 宗门 (`sects`),
背包 (`back`), 竞技场 (`arena`) — and before this slice there was no single
command that showed them together.  `grep` over every `on_command`/`on_regex`
in `xiuxian/` for 资产/总览/一览 returned nothing.

This suite pins the composition against REAL column sets (the legacy
`user_xiuxian`/`sects`/`back` shape from `xiuxian2_handle` + `XiuConfig` ALTERs,
and the migrated `bank_accounts`/`arena` shape), the real
`PlayerAssetsOverviewApplication`, and a pure-rendering contract.
"""

from __future__ import annotations

import ast
import sqlite3
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.base.assets_overview import (
    PlayerAssetsOverviewApplication,
    format_assets_overview,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

# Authoritative legacy shapes (xiuxian2_handle DDL + XiuConfig().sql_user_xiuxian).
USER_XIUXIAN_DDL = (
    "CREATE TABLE user_xiuxian("
    "id INTEGER PRIMARY KEY,user_id TEXT UNIQUE,user_name TEXT,stone INTEGER,"
    "sect_id INTEGER,sect_position INTEGER,sect_contribution INTEGER)"
)
SECTS_DDL = "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT,sect_owner TEXT)"
BACK_DDL = (
    "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_num INTEGER,"
    "bind_num INTEGER)"
)
# Migrated shapes (features/bank/migrations.py, features/arena/migrations.py).
BANK_DDL = (
    "CREATE TABLE bank_accounts(user_id TEXT PRIMARY KEY,saved_stone INTEGER NOT NULL,"
    "bank_level TEXT NOT NULL,updated_at TEXT NOT NULL)"
)
ARENA_DDL = (
    "CREATE TABLE arena(user_id TEXT PRIMARY KEY,score INTEGER,rank TEXT,honor_points INTEGER)"
)


def _fmt(value) -> str:
    return f"{int(value):,}"


class AssetsOverviewTests(unittest.TestCase):
    """Drive the overview the way the ``我的资产总览`` handler drives it."""

    def build(
        self,
        temp: str,
        *,
        with_bank: bool = True,
        with_arena: bool = True,
        with_player_db: bool = True,
        with_sects: bool = True,
        with_back: bool = True,
    ) -> tuple[Path, Path]:
        game = Path(temp) / "game.db"
        player = Path(temp) / "player.db"
        with DatabaseUnitOfWork(str(game)) as uow:
            uow.execute(USER_XIUXIAN_DDL)
            if with_sects:
                uow.execute(SECTS_DDL)
            if with_back:
                uow.execute(BACK_DDL)
            if with_bank:
                uow.execute(BANK_DDL)
            # 目标玩家：有宗门、有贡献
            uow.execute(
                "INSERT INTO user_xiuxian(id,user_id,user_name,stone,sect_id,sect_position,"
                "sect_contribution) VALUES(1,'u1','甲',123456,7,0,8800)"
            )
            # 另一名玩家：全部资产都很大，用于验证玩家隔离
            uow.execute(
                "INSERT INTO user_xiuxian(id,user_id,user_name,stone,sect_id,sect_position,"
                "sect_contribution) VALUES(2,'u2','乙',999999,8,0,777777)"
            )
            if with_sects:
                uow.execute("INSERT INTO sects(sect_id,sect_name,sect_owner) VALUES(7,'青云宗','u1')")
                uow.execute("INSERT INTO sects(sect_id,sect_name,sect_owner) VALUES(8,'黑水宗','u2')")
            if with_back:
                uow.execute("INSERT INTO back VALUES('u1',1,'丹药',5,0)")
                uow.execute("INSERT INTO back VALUES('u1',2,'材料',12,0)")
                uow.execute("INSERT INTO back VALUES('u1',3,'空物',0,0)")  # goods_num=0 不计
                uow.execute("INSERT INTO back VALUES('u2',9,'巨款',5000,0)")
            if with_bank:
                uow.execute("INSERT INTO bank_accounts VALUES('u1',500000,'黄金','2026-10-30T00:00:00')")
                uow.execute("INSERT INTO bank_accounts VALUES('u2',888888,'钻石','2026-10-30T00:00:00')")

        if not with_player_db:
            player.write_bytes(b"")
        else:
            with DatabaseUnitOfWork(str(player)) as uow:
                if with_arena:
                    uow.execute(ARENA_DDL)
                    uow.execute("INSERT INTO arena VALUES('u1',1520,'白银',3300)")
                    uow.execute("INSERT INTO arena VALUES('u2',9999,'王者',99999)")
        return game, player

    def app(self, temp: str, **kwargs) -> PlayerAssetsOverviewApplication:
        game, player = self.build(temp, **kwargs)
        return PlayerAssetsOverviewApplication(str(game), str(player))

    # ------------------------------------------------------------ 完整取数
    def test_overview_reads_all_five_sections(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp).overview("u1")
            self.assertTrue(payload["found"])
            self.assertEqual(payload["stone"], 123456)
            self.assertEqual(payload["bank_saved"], 500000)
            self.assertEqual(payload["bank_level"], "黄金")
            self.assertEqual(payload["sect_name"], "青云宗")
            self.assertEqual(payload["sect_contribution"], 8800)
            self.assertEqual(payload["bag_kinds"], 2)
            self.assertEqual(payload["bag_items"], 17)  # 5 + 12，goods_num=0 的行不计
            self.assertEqual(payload["arena_rank"], "白银")
            self.assertEqual(payload["arena_score"], 1520)
            self.assertEqual(payload["arena_honor"], 3300)

    def test_full_render_contains_every_line(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            text = format_assets_overview(self.app(temp).overview("u1"), format_number=_fmt)
            self.assertIn("【我的资产总览】", text)
            self.assertIn("灵石：123,456", text)
            self.assertIn("灵庄存款：500,000（黄金）", text)
            self.assertIn("宗门：青云宗（贡献 8,800）", text)
            self.assertIn("背包：2 种 / 共 17 个", text)
            self.assertIn("竞技场：白银 · 1,520 分 · 荣誉 3,300", text)

    # ------------------------------------------------------------ 玩家隔离
    def test_other_player_assets_never_leak(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp).overview("u1")
            text = format_assets_overview(payload, format_number=_fmt)
            for leaked in ("999,999", "888,888", "777,777", "黑水宗", "王者", "9,999", "99,999", "5,000"):
                self.assertNotIn(leaked, text)
            self.assertEqual(payload["sect_name"], "青云宗")

    # ---------------------------------------------------------- 分段省略
    def test_missing_bank_row_omits_bank_line(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            application = self.app(temp)
            payload = application.overview("u2")
            # u2 有 bank 行；改用只有 u1 的库验证「无行」分支
            self.assertIsNotNone(payload["bank_saved"])
            with tempfile.TemporaryDirectory() as temp2:
                game, player = self.build(temp2)
                with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                    uow.execute("DELETE FROM bank_accounts WHERE user_id='u1'")
                payload = PlayerAssetsOverviewApplication(str(game), str(player)).overview("u1")
                self.assertIsNone(payload["bank_saved"])
                self.assertNotIn("灵庄存款", format_assets_overview(payload, format_number=_fmt))

    def test_no_sect_omits_sect_line(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                uow.execute("UPDATE user_xiuxian SET sect_id=NULL WHERE user_id='u1'")
            payload = PlayerAssetsOverviewApplication(str(game), str(player)).overview("u1")
            self.assertIsNone(payload["sect_name"])
            self.assertNotIn("宗门：", format_assets_overview(payload, format_number=_fmt))

    def test_empty_bag_omits_bag_line(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                uow.execute("DELETE FROM back WHERE user_id='u1'")
            payload = PlayerAssetsOverviewApplication(str(game), str(player)).overview("u1")
            self.assertIsNone(payload["bag_kinds"])
            self.assertNotIn("背包：", format_assets_overview(payload, format_number=_fmt))

    def test_no_arena_row_omits_arena_line(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            with DatabaseUnitOfWork(str(player), immediate=True) as uow:
                uow.execute("DELETE FROM arena WHERE user_id='u1'")
            payload = PlayerAssetsOverviewApplication(str(game), str(player)).overview("u1")
            self.assertIsNone(payload["arena_rank"])
            self.assertNotIn("竞技场：", format_assets_overview(payload, format_number=_fmt))

    def test_all_optional_sections_absent_still_shows_stone(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            with DatabaseUnitOfWork(str(game), immediate=True) as uow:
                uow.execute("UPDATE user_xiuxian SET sect_id=NULL WHERE user_id='u1'")
                uow.execute("DELETE FROM back WHERE user_id='u1'")
                uow.execute("DELETE FROM bank_accounts WHERE user_id='u1'")
            with DatabaseUnitOfWork(str(player), immediate=True) as uow:
                uow.execute("DELETE FROM arena WHERE user_id='u1'")
            text = format_assets_overview(
                PlayerAssetsOverviewApplication(str(game), str(player)).overview("u1"),
                format_number=_fmt,
            )
            self.assertEqual(text, "【我的资产总览】\n灵石：123,456")

    # ---------------------------------------------------------- 错误响应
    def test_unknown_user_returns_guidance(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp).overview("nobody")
            self.assertFalse(payload["found"])
            self.assertEqual(
                format_assets_overview(payload), "未找到角色数据，请先发送【我要修仙】。"
            )

    def test_blank_user_id_returns_guidance(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            payload = self.app(temp).overview("   ")
            self.assertFalse(payload["found"])
            self.assertEqual(
                format_assets_overview(payload), "未找到角色数据，请先发送【我要修仙】。"
            )

    def test_missing_bank_table_degrades_without_creating_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, with_bank=False)
            payload = PlayerAssetsOverviewApplication(str(game), str(player)).overview("u1")
            self.assertTrue(payload["found"])
            self.assertIsNone(payload["bank_saved"])
            self.assertEqual(payload["sect_name"], "青云宗")  # 其他段不受影响
            with sqlite3.connect(game) as conn:
                created = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bank_accounts'"
                ).fetchone()
            self.assertIsNone(created, "read path must not create tables")

    def test_missing_arena_table_degrades(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, with_arena=False)
            payload = PlayerAssetsOverviewApplication(str(game), str(player)).overview("u1")
            self.assertTrue(payload["found"])
            self.assertIsNone(payload["arena_rank"])
            self.assertEqual(payload["stone"], 123456)

    def test_absent_player_database_degrades(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, with_player_db=False)
            payload = PlayerAssetsOverviewApplication(str(game), str(player)).overview("u1")
            self.assertTrue(payload["found"])
            self.assertIsNone(payload["arena_rank"])
            self.assertEqual(payload["bank_saved"], 500000)

    def test_missing_sects_and_back_tables_degrade(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, with_sects=False, with_back=False)
            payload = PlayerAssetsOverviewApplication(str(game), str(player)).overview("u1")
            self.assertTrue(payload["found"])
            self.assertIsNone(payload["sect_name"])
            self.assertIsNone(payload["bag_kinds"])
            self.assertEqual(payload["stone"], 123456)

    # ------------------------------------------------------------ 纯函数
    def test_render_omits_bank_level_when_blank(self) -> None:
        payload = {
            "found": True, "stone": 10, "bank_saved": 500, "bank_level": None,
            "sect_name": None, "sect_contribution": None,
            "bag_kinds": None, "bag_items": None,
            "arena_rank": None, "arena_score": None, "arena_honor": None,
        }
        text = format_assets_overview(payload, format_number=_fmt)
        self.assertIn("灵庄存款：500", text)
        self.assertNotIn("（", text)

    def test_render_sect_without_contribution(self) -> None:
        payload = {
            "found": True, "stone": 10, "bank_saved": None, "bank_level": None,
            "sect_name": "青云宗", "sect_contribution": None,
            "bag_kinds": None, "bag_items": None,
            "arena_rank": None, "arena_score": None, "arena_honor": None,
        }
        self.assertIn("宗门：青云宗", format_assets_overview(payload, format_number=_fmt))

    # ------------------------------------------------------- handler 源合同
    def test_handler_source_contract(self) -> None:
        path = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_base"
            / "__init__.py"
        )
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)

        matchers = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
                fn = node.value.func
                if getattr(fn, "id", None) == "on_command" and node.value.args:
                    name = node.value.args[0]
                    if isinstance(name, ast.Constant):
                        for target in node.targets:
                            if isinstance(target, ast.Name):
                                matchers[name.value] = target.id
        self.assertEqual(matchers.get("我的资产总览"), "assets_overview")

        handler = None
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for dec in node.decorator_list:
                    seg = ast.get_source_segment(source, dec) or ""
                    if "assets_overview.handle" in seg:
                        handler = node
        self.assertIsNotNone(handler, "assets_overview handler must be registered")
        body = ast.get_source_segment(source, handler) or ""
        self.assertIn('overview(str(user_info["user_id"]))', body)
        self.assertIn("format_assets_overview(", body)
        # 安全：不得把命令参数里的 user_id 传给应用。
        self.assertNotIn("extract_plain_text", body)
        self.assertNotIn("args", body)


if __name__ == "__main__":
    unittest.main()
