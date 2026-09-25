from __future__ import annotations

import sqlite3
import importlib
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.buff.migrations import (
    apply_partner_cultivation_operations,
    apply_partner_cultivation_player_schema,
    apply_partner_token_usage,
)
from nonebot_plugin_xiuxian_2.features.buff.partner_cultivation_application import (
    PartnerCultivationApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from tests.test_db_backend import db_backend


class FixedClock:
    def __init__(self, timestamp: float) -> None:
        self.timestamp = timestamp

    def now(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp, timezone.utc)


class PartnerCultivationApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT PRIMARY KEY,exp INTEGER,power INTEGER,hp INTEGER,"
                "mp INTEGER,atk INTEGER,level_up_rate INTEGER)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(?,?,0,1,1,1,0)",
                [("a", 1000), ("b", 2000)],
            )
        with DatabaseUnitOfWork(self.game) as uow:
            apply_partner_cultivation_operations(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            apply_partner_token_usage(uow)
            apply_partner_cultivation_player_schema(uow)
            uow.executemany(
                "INSERT INTO partner(user_id,partner_id,affection) VALUES(?,?,?)",
                [("a", "b", 3), ("b", "a", 4)],
            )
            uow.executemany(
                "INSERT INTO partner_two_exp_usage(user_id,used_count) VALUES(?,0)",
                [("a",), ("b",)],
            )
        self.app = PartnerCultivationApplication(
            self.game, self.player, clock=FixedClock(1000)
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def apply(self, operation_id: str = "op", **overrides):
        values = {
            "expected_exp_1": 1000,
            "expected_exp_2": 2000,
            "exp_1": 100,
            "exp_2": 200,
            "used_count": 2,
            "power_1": 1,
            "power_2": 2,
            "hp_1": 3,
            "mp_1": 4,
            "atk_1": 5,
            "hp_2": 6,
            "mp_2": 7,
            "atk_2": 8,
            "expected_affection_1": 3,
            "expected_affection_2": 4,
            "affection_1": 40,
            "affection_2": 20,
            "expected_used_count_1": 0,
            "expected_used_count_2": 0,
        }
        values.update(overrides)
        return self.app.apply(operation_id, "a", "b", **values)

    @staticmethod
    def ensure_nonebot_initialized() -> None:
        import nonebot

        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()

    def state(self):
        with db_backend.connection(self.game) as conn:
            users = conn.execute(
                "SELECT user_id,exp,power FROM user_xiuxian ORDER BY user_id"
            ).fetchall()
        with db_backend.connection(self.player) as conn:
            usage = conn.execute(
                "SELECT user_id,used_count FROM partner_two_exp_usage ORDER BY user_id"
            ).fetchall()
            affection = conn.execute(
                "SELECT user_id,affection FROM partner ORDER BY user_id"
            ).fetchall()
            statistics = conn.execute(
                'SELECT user_id,"双修次数" FROM statistics ORDER BY user_id'
            ).fetchall()
        return users, usage, affection, statistics

    def test_settlement_replay_updates_all_projections_once(self) -> None:
        self.assertEqual("applied", self.apply().status)
        self.assertEqual("duplicate", self.apply().status)
        self.assertEqual("operation_conflict", self.apply(exp_1=101).status)
        users, usage, affection, statistics = self.state()
        self.assertEqual([1100, 2200], [int(row[1]) for row in users])
        self.assertEqual([2, 2], [row[1] for row in usage])
        self.assertEqual([43, 24], [row[1] for row in affection])
        self.assertEqual([2, 2], [row[1] for row in statistics])

    def test_large_combat_values_and_snapshot_rejection(self) -> None:
        huge = 10**24
        self.assertEqual(
            "applied",
            self.apply(
                "large",
                expected_affection_1=None,
                expected_affection_2=None,
                affection_1=0,
                affection_2=0,
                power_1=huge,
                power_2=huge,
                hp_1=huge,
                mp_1=huge,
                atk_1=huge,
                hp_2=huge,
                mp_2=huge,
                atk_2=huge,
            ).status,
        )
        users, _, _, _ = self.state()
        self.assertGreaterEqual(float(users[0][2]), 1e20)
        self.assertEqual("state_changed", self.apply("stale", expected_used_count_1=1).status)

    def test_invite_expiry_protection_and_acceptance_share_the_transaction(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "INSERT INTO status(user_id,two_exp_protect) VALUES('b','on')"
            )
        self.assertEqual(
            "protection_changed",
            self.apply("protection-op", expected_target_protection="off").status,
        )
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "INSERT INTO partner_cultivation_invites VALUES"
                "('expired','a','b',2,'pending',900,1000,NULL)"
            )
        self.assertEqual(
            "invitation_changed",
            self.apply(
                "expired-op",
                invite_id="expired",
                expected_target_protection="on",
            ).status,
        )
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "UPDATE partner_cultivation_invites SET status='expired' WHERE invite_id='expired'"
            )
            conn.execute(
                "INSERT INTO partner_cultivation_invites VALUES"
                "('valid','a','b',2,'pending',900,1100,NULL)"
            )
        self.assertEqual(
            "applied",
            self.apply(
                "valid-op",
                invite_id="valid",
                expected_target_protection="on",
            ).status,
        )
        with db_backend.connection(self.player) as conn:
            self.assertEqual(
                "accepted",
                conn.execute(
                    "SELECT status FROM partner_cultivation_invites WHERE invite_id='valid'"
                ).fetchone()[0],
            )

    def test_failure_rolls_back_game_and_player_writes(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TRIGGER fail_partner_usage BEFORE UPDATE ON partner_two_exp_usage "
                "BEGIN SELECT RAISE(ABORT,'forced'); END"
            )
        with self.assertRaises(sqlite3.DatabaseError):
            self.apply("rollback")
        users, usage, affection, statistics = self.state()
        self.assertEqual([1000, 2000], [int(row[1]) for row in users])
        self.assertEqual([0, 0], [row[1] for row in usage])
        self.assertEqual([3, 4], [row[1] for row in affection])
        self.assertEqual([], statistics)

    def test_legacy_direct_operation_payload_remains_replay_compatible(self) -> None:
        self.ensure_nonebot_initialized()
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import (
            PartnerCultivationService,
        )

        legacy = PartnerCultivationService(self.game, self.player)
        self.assertEqual(
            "applied",
            legacy.apply(
                "op", "a", "b", expected_exp_1=1000, expected_exp_2=2000,
                exp_1=100, exp_2=200, used_count=2, power_1=1, power_2=2,
                hp_1=3, mp_1=4, atk_1=5, hp_2=6, mp_2=7, atk_2=8,
                expected_affection_1=3, expected_affection_2=4,
                affection_1=40, affection_2=20,
            ).status,
        )
        self.assertEqual("duplicate", self.apply().status)
        users, usage, _, statistics = self.state()
        self.assertEqual([1100, 2200], [int(row[1]) for row in users])
        self.assertEqual([0, 0], [row[1] for row in usage])
        self.assertEqual([2, 2], [row[1] for row in statistics])

    def test_missing_operation_migration_is_rejected_without_ddl(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game, player = Path(directory) / "game.db", Path(directory) / "player.db"
            with db_backend.transaction(game):
                pass
            with db_backend.transaction(player):
                pass
            app = PartnerCultivationApplication(game, player)
            with self.assertRaisesRegex(RuntimeError, "partner_cultivation_operations"):
                app.apply(
                    "missing", "a", "b", expected_exp_1=0, expected_exp_2=0,
                    exp_1=1, exp_2=1, used_count=1, power_1=0, power_2=0,
                    hp_1=0, mp_1=0, atk_1=0, hp_2=0, mp_2=0, atk_2=0,
                )
            with db_backend.connection(game) as conn:
                self.assertIsNone(conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE name='partner_cultivation_operations'"
                ).fetchone())

    def test_legacy_usage_import_is_kept_without_request_time_ddl(self) -> None:
        self.ensure_nonebot_initialized()
        usage_module = importlib.import_module(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.two_exp_cd"
        )
        tracker = usage_module.two_exp_cd
        previous_data = tracker.data
        tracker.data = {"two_exp_cd": {"legacy": 7}}
        with tempfile.TemporaryDirectory() as directory:
            player = Path(directory) / "player.db"
            try:
                with patch.object(
                    usage_module,
                    "get_paths",
                    return_value=SimpleNamespace(player_db=player),
                ):
                    with self.assertRaisesRegex(RuntimeError, "partner_two_exp_usage"):
                        tracker.find_user("legacy")
                    with db_backend.connection(player) as conn:
                        self.assertIsNone(conn.execute(
                            "SELECT 1 FROM sqlite_master WHERE name='partner_two_exp_usage'"
                        ).fetchone())
                    with DatabaseUnitOfWork(player) as uow:
                        apply_partner_token_usage(uow)
                    self.assertEqual(7, tracker.find_user("legacy"))
                    tracker.data["two_exp_cd"]["legacy"] = 99
                    self.assertEqual(7, tracker.find_user("legacy"))
            finally:
                tracker.data = previous_data

    def test_migration_versions_are_owned_by_game_and_player_databases(self) -> None:
        migrations = build_migrations()
        routed = {
            key: {item.version for item in migrations_for_database(migrations, key)}
            for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db")
        }
        self.assertIn("buff.004", routed["game_db"])
        self.assertNotIn("buff.004", routed["player_db"])
        self.assertIn("buff.005", routed["player_db"])
        self.assertNotIn("buff.005", routed["game_db"])
        for key in ("trade_db", "impart_db", "message_db"):
            self.assertNotIn("buff.004", routed[key])
            self.assertNotIn("buff.005", routed[key])


if __name__ == "__main__":
    unittest.main()
