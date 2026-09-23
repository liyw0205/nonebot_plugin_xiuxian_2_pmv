from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.dongfu.application import DongfuApplication
from nonebot_plugin_xiuxian_2.features.dongfu.infiltrate_failure_repository import (
    DongfuInfiltrateFailureSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.dongfu.migrations import (
    apply_dongfu_infiltrate_failure,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class DongfuInfiltrateFailureSqlRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.player_database = root / "player.sqlite3"
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian(user_id,stone) VALUES(?,?)", ("u", 1000))
            apply_dongfu_infiltrate_failure(uow)
        with DatabaseUnitOfWork(self.player_database) as uow:
            uow.execute(
                "CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,infiltrate_date TEXT,"
                "infiltrate_active_count INTEGER,infiltrate_random_count INTEGER,intrude_date TEXT,"
                "intrude_count INTEGER,patrol_guard INTEGER)"
            )
            uow.executemany(
                "INSERT INTO dongfu_status VALUES(?,?,?,?,?,?,?,?)",
                (
                    ("u", 1, "", 0, 0, "", 0, 0),
                    ("t", 1, "", 0, 0, "", 0, 1),
                ),
            )
        self.repository = DongfuInfiltrateFailureSqlRepository(
            self.game_database, self.player_database
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def settle(self, operation_id: str = "op", **overrides: object):
        values: dict[str, object] = {
            "day": "2026-07-13",
            "mode_field": "infiltrate_active_count",
            "loss": 200,
            "consume_guard": True,
        }
        values.update(overrides)
        return self.repository.settle(
            operation_id,
            "u",
            "t",
            str(values["day"]),
            str(values["mode_field"]),
            3,
            3,
            int(values["loss"]),
            bool(values["consume_guard"]),
        )

    def state(self) -> tuple[int, tuple[str, int], tuple[str, int, int]]:
        with DatabaseUnitOfWork(self.game_database) as uow:
            stone = int(
                uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", ("u",))["stone"]
            )
        with DatabaseUnitOfWork(self.player_database) as uow:
            visitor = uow.query_one(
                "SELECT infiltrate_date,infiltrate_active_count FROM dongfu_status WHERE user_id=?",
                ("u",),
            )
            target = uow.query_one(
                "SELECT intrude_date,intrude_count,patrol_guard FROM dongfu_status WHERE user_id=?",
                ("t",),
            )
        return (
            stone,
            (str(visitor["infiltrate_date"]), int(visitor["infiltrate_active_count"])),
            (
                str(target["intrude_date"]),
                int(target["intrude_count"]),
                int(target["patrol_guard"]),
            ),
        )

    def test_settles_replays_and_rejects_conflicting_replay(self) -> None:
        self.assertEqual(self.settle("same").status, "settled")
        self.assertEqual(self.settle("same").status, "duplicate")
        self.assertEqual(self.settle("same", loss=300).status, "state_changed")
        self.assertEqual(
            self.state(),
            (800, ("2026-07-13", 1), ("2026-07-13", 1, 0)),
        )

    def test_daily_limit_preserves_state_and_reports_remaining_counts(self) -> None:
        with DatabaseUnitOfWork(self.player_database) as uow:
            uow.execute(
                "UPDATE dongfu_status SET infiltrate_date=?,infiltrate_active_count=? WHERE user_id=?",
                ("2026-07-13", 3, "u"),
            )
        result = self.settle("limit")
        self.assertEqual((result.status, result.infiltrate_left, result.intrude_left), ("daily_limit", 0, 3))
        self.assertEqual(self.state(), (1000, ("2026-07-13", 3), ("", 0, 1)))

    def test_operation_insert_failure_rolls_back_both_databases(self) -> None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "CREATE TRIGGER reject_infiltrate BEFORE INSERT ON dongfu_infiltrate_failure_operations "
                "BEGIN SELECT RAISE(ABORT,'reject'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.settle("rollback")
        self.assertEqual(self.state(), (1000, ("", 0), ("", 0, 1)))

    def test_asset_rowcount_failure_rolls_back_prior_player_updates(self) -> None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "CREATE TRIGGER ignore_stone BEFORE UPDATE ON user_xiuxian "
                "BEGIN SELECT RAISE(IGNORE); END"
            )
        self.assertEqual(self.settle("rowcount").status, "state_changed")
        self.assertEqual(self.state(), (1000, ("", 0), ("", 0, 1)))

    def test_application_default_uses_feature_repository(self) -> None:
        result = DongfuApplication(
            self.game_database, self.player_database
        ).infiltrate_failure(
            operation_id="application",
            visitor_id="u",
            target_id="t",
            day="2026-07-13",
            mode_field="infiltrate_active_count",
            mode_limit=3,
            target_limit=3,
            loss=200,
            consume_guard=True,
        )
        self.assertEqual(result.status, "settled")
        self.assertEqual(
            self.state(),
            (800, ("2026-07-13", 1), ("2026-07-13", 1, 0)),
        )


if __name__ == "__main__":
    unittest.main()
