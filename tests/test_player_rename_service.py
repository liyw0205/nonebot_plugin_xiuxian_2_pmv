from __future__ import annotations

import tempfile
import unittest
import importlib
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base.transaction_service import (
    PlayerRenameService,
)
from tests.test_db_backend import db_backend


def test_base_facade_defers_player_rename_service_construction():
    base = importlib.import_module(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base"
    )
    assert base._player_rename_service_instance is None


def test_player_rename_handlers_use_lazy_service():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_base/__init__.py"
    ).read_text(encoding="utf-8")
    handler = source[source.index("async def remaname_"):source.index("@run_xiuxian.handle")]
    assert "_player_rename_service().get_result(" in handler
    assert "base_application.rename(" in handler
    assert "_player_rename_service().rename_user(" not in handler
    assert "_player_rename_service().rename_root(" not in source
    assert "_player_rename_service_instance = None" in source
    assert "def _player_rename_service(" in source
    assert "player_rename_service.rename_user(" not in source


def test_player_rename_handlers_use_feature_application():
    source = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_base/__init__.py"
    text = source.read_text(encoding="utf-8")
    assert "base_application.rename(" in text


def test_player_rename_compatibility_implementation_is_isolated():
    root = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2"
    transaction_source = (root / "xiuxian/xiuxian_base/transaction_service.py").read_text(encoding="utf-8")
    compatibility_source = (root / "compatibility/legacy_base_player_rename.py").read_text(encoding="utf-8")
    assert "class PlayerRenameService" not in transaction_source
    assert "class PlayerRenameService" in compatibility_source


class PlayerRenameServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "player-rename.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, user_name TEXT, "
                "root TEXT, stone INTEGER)"
            )
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, "
                "bind_num INTEGER, UNIQUE(user_id, goods_id))"
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s)",
                ("user", "旧道号", "混沌灵根", 1000),
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s, %s, %s, %s)",
                ("other", "已有道号", "火灵根", 1000),
            )
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s)", ("user", 20011, 2, 1))
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s)", ("user", 20025, 1, 1))
        self.service = PlayerRenameService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def player(self):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT user_name, root, stone FROM user_xiuxian WHERE user_id=%s",
                ("user",),
            ).fetchone()
        return str(row[0]), str(row[1]), int(row[2])

    def inventory(self, goods_id: int):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(
                "SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("user", goods_id),
            ).fetchone()
        return None if row is None else tuple(map(int, row))

    def operation_count(self) -> int:
        with db_backend.connection(self.database) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("player_rename_operations",),
            ).fetchone()
            if not exists:
                return 0
            return int(conn.execute("SELECT COUNT(*) FROM player_rename_operations").fetchone()[0])

    def test_user_name_item_cost_and_rename_are_atomic(self) -> None:
        result = self.service.rename_user(
            "rename-item", "user", "新道号", item_id=20011
        )
        self.assertEqual((result.status, result.previous_name), ("renamed", "旧道号"))
        self.assertEqual(self.player(), ("新道号", "混沌灵根", 1000))
        self.assertEqual(self.inventory(20011), (1, 0))
        self.assertEqual(self.operation_count(), 1)

    def test_random_name_stone_cost_and_rename_are_atomic(self) -> None:
        result = self.service.rename_user(
            "rename-stone", "user", "随机道号", stone_cost=300
        )
        self.assertEqual(result.status, "renamed")
        self.assertEqual(self.player(), ("随机道号", "混沌灵根", 700))
        self.assertEqual(self.inventory(20011), (2, 1))

    def test_root_card_and_rename_are_atomic(self) -> None:
        result = self.service.rename_root(
            "rename-root", "user", "太初灵根", item_id=20025
        )
        self.assertEqual(result.status, "renamed")
        self.assertEqual(self.player(), ("旧道号", "太初灵根", 1000))
        self.assertEqual(self.inventory(20025), (0, 0))

    def test_duplicate_does_not_charge_twice(self) -> None:
        first = self.service.rename_user(
            "rename-repeat", "user", "新道号", item_id=20011
        )
        second = self.service.rename_user(
            "rename-repeat", "user", "另一个道号", item_id=20011
        )
        self.assertEqual((first.status, second.status), ("renamed", "duplicate"))
        self.assertEqual(second.new_name, "新道号")
        self.assertEqual(self.player()[0], "新道号")
        self.assertEqual(self.inventory(20011), (1, 0))
        self.assertEqual(self.operation_count(), 1)

    def test_conflict_or_missing_cost_does_not_change_player(self) -> None:
        conflict = self.service.rename_user(
            "rename-conflict", "user", "已有道号", item_id=20011
        )
        poor = self.service.rename_user(
            "rename-poor", "user", "昂贵道号", stone_cost=1001
        )
        missing = self.service.rename_root(
            "rename-no-card", "other", "新灵根", item_id=20025
        )
        self.assertEqual(conflict.status, "name_conflict")
        self.assertEqual(poor.status, "stone_insufficient")
        self.assertEqual(missing.status, "item_missing")
        self.assertEqual(self.player(), ("旧道号", "混沌灵根", 1000))
        self.assertEqual(self.inventory(20011), (2, 1))
        self.assertEqual(self.operation_count(), 0)

    def test_operation_failure_rolls_back_cost_and_name(self) -> None:
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_operations(conn)
            conn.execute(
                "CREATE TRIGGER fail_player_rename BEFORE INSERT ON player_rename_operations "
                "BEGIN SELECT RAISE(ABORT, 'rename failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.rename_user(
                "rename-fail", "user", "新道号", item_id=20011
            )
        self.assertEqual(self.player(), ("旧道号", "混沌灵根", 1000))
        self.assertEqual(self.inventory(20011), (2, 1))
        self.assertEqual(self.operation_count(), 0)


if __name__ == "__main__":
    unittest.main()
