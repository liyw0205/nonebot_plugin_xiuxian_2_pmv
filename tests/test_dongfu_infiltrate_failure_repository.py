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

    def test_write_refusals_and_projection_rewrites_keep_failure_reasons_specific(self) -> None:
        faults = (
            (self.player_database, "BEFORE UPDATE ON dongfu_status WHEN NEW.user_id='u' "
             "BEGIN SELECT RAISE(IGNORE); END", "dongfu_infiltrate_failure_visitor_write_rejected", "visitor"),
            (self.player_database, "AFTER UPDATE ON dongfu_status WHEN NEW.user_id='u' "
             "BEGIN UPDATE dongfu_status SET infiltrate_active_count=9 WHERE user_id='u'; END",
             "dongfu_infiltrate_failure_visitor_projection_mismatch", "visitor"),
            (self.player_database, "BEFORE UPDATE ON dongfu_status WHEN NEW.user_id='t' "
             "BEGIN SELECT RAISE(IGNORE); END", "dongfu_infiltrate_failure_target_write_rejected", "target"),
            (self.player_database, "AFTER UPDATE ON dongfu_status WHEN NEW.user_id='t' "
             "BEGIN UPDATE dongfu_status SET patrol_guard=9 WHERE user_id='t'; END",
             "dongfu_infiltrate_failure_target_projection_mismatch", "target"),
            (self.game_database, "AFTER UPDATE ON user_xiuxian WHEN NEW.user_id='u' "
             "BEGIN UPDATE user_xiuxian SET stone=1 WHERE user_id='u'; END",
             "dongfu_infiltrate_failure_stone_projection_mismatch", "stone_wallet"),
            (self.game_database, "BEFORE INSERT ON dongfu_infiltrate_failure_operations "
             "BEGIN SELECT RAISE(IGNORE); END", "dongfu_infiltrate_failure_receipt_write_rejected", "receipt"),
            (self.game_database, "AFTER INSERT ON dongfu_infiltrate_failure_operations "
             "BEGIN UPDATE dongfu_infiltrate_failure_operations SET payload='changed' "
             "WHERE operation_id=NEW.operation_id; END",
             "dongfu_infiltrate_failure_receipt_projection_mismatch", "receipt"),
        )
        for index, (database, trigger, reason, subject) in enumerate(faults):
            with self.subTest(reason=reason):
                before = self.state()
                with DatabaseUnitOfWork(database) as uow:
                    uow.execute("CREATE TRIGGER infiltration_fault " + trigger)
                result = self.settle(f"fault-{index}")
                self.assertEqual((result.status, result.reason, result.subject),
                                 ("state_changed", reason, subject))
                self.assertEqual(self.state(), before)
                with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                    self.assertEqual(uow.query_one(
                        "SELECT COUNT(*) AS n FROM dongfu_infiltrate_failure_operations"
                    )["n"], 0)
                with DatabaseUnitOfWork(database) as uow:
                    uow.execute("DROP TRIGGER infiltration_fault")

    def test_abort_at_each_failure_write_rolls_back_both_databases(self) -> None:
        faults = (
            (self.player_database, "BEFORE UPDATE ON dongfu_status WHEN NEW.user_id='u'"),
            (self.player_database, "BEFORE UPDATE ON dongfu_status WHEN NEW.user_id='t'"),
            (self.game_database, "BEFORE UPDATE ON user_xiuxian"),
            (self.game_database, "BEFORE INSERT ON dongfu_infiltrate_failure_operations"),
        )
        for index, (database, event) in enumerate(faults):
            with self.subTest(event=event):
                before = self.state()
                with DatabaseUnitOfWork(database) as uow:
                    uow.execute("CREATE TRIGGER infiltration_abort " + event +
                                " BEGIN SELECT RAISE(ABORT,'infiltrate refused'); END")
                with self.assertRaisesRegex(sqlite3.IntegrityError, "infiltrate refused"):
                    self.settle(f"abort-{index}")
                self.assertEqual(self.state(), before)
                with DatabaseUnitOfWork(database) as uow:
                    uow.execute("DROP TRIGGER infiltration_abort")

    def test_asset_rowcount_failure_rolls_back_prior_player_updates(self) -> None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute(
                "CREATE TRIGGER ignore_stone BEFORE UPDATE ON user_xiuxian "
                "BEGIN SELECT RAISE(IGNORE); END"
            )
        result = self.settle("rowcount")
        self.assertEqual(
            (result.status, result.reason, result.subject),
            ("state_changed", "dongfu_infiltrate_failure_stone_write_rejected", "stone_wallet"),
        )
        self.assertEqual(self.state(), (1000, ("", 0), ("", 0, 1)))
        with DatabaseUnitOfWork(self.game_database) as uow:
            self.assertEqual(uow.query_one("SELECT COUNT(*) AS n FROM dongfu_infiltrate_failure_operations")["n"], 0)

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
