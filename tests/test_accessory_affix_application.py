from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.accessory_affix_application import (
    AccessoryAffixApplication,
)
from nonebot_plugin_xiuxian_2.features.back.migrations import apply_accessory_affix_operations
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


class AccessoryAffixApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        self.accessory = {
            "uid": "acc-1",
            "name": "测试戒指",
            "quality": 4,
            "affixes": [{"type": "攻击", "value": 0.1}, {"type": "速度", "value": 20}],
        }
        with sqlite3.connect(self.player) as conn:
            conn.execute(
                "CREATE TABLE player_accessory(user_id TEXT PRIMARY KEY,equipped TEXT,bag TEXT)"
            )
            conn.execute(
                "INSERT INTO player_accessory VALUES(?,?,?)",
                ("u", "{}", json.dumps([self.accessory], ensure_ascii=False)),
            )
        with DatabaseUnitOfWork(self.game) as uow:
            apply_accessory_affix_operations(uow)
        self.application = AccessoryAffixApplication(self.game, self.player)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def state(self):
        with sqlite3.connect(self.player) as conn:
            row = conn.execute(
                "SELECT equipped,bag FROM player_accessory WHERE user_id='u'"
            ).fetchone()
        return json.loads(row[0]), json.loads(row[1])

    def set_locks(self, operation_id="affix-lock-1", indexes=(0,)):
        return self.application.set_locks(
            operation_id,
            "lock",
            "u",
            "acc-1",
            self.accessory,
            indexes,
        )

    def test_lock_replay_payload_conflict_and_unlock(self) -> None:
        first = self.set_locks()
        replay = self.set_locks()
        conflict = self.set_locks(indexes=(1,))
        self.assertEqual((first.status, replay.status, conflict.status), ("applied", "duplicate", "state_changed"))
        locked = first.accessory
        self.assertEqual(locked["locked_affixes"], [0])
        self.assertEqual(self.application.replay("affix-lock-1", "lock").status, "duplicate")

        unlocked = self.application.set_locks(
            "affix-unlock-1", "unlock", "u", "acc-1", locked, ()
        )
        self.assertEqual(unlocked.status, "applied")
        self.assertNotIn("locked_affixes", unlocked.accessory)
        self.assertNotIn("locked_affixes", self.state()[1][0])

    def test_stale_snapshot_and_insert_failure_do_not_mutate(self) -> None:
        stale = dict(self.accessory, wash_count=2)
        self.assertEqual(
            self.application.set_locks("stale", "lock", "u", "acc-1", stale, (0,)).status,
            "state_changed",
        )
        before = self.state()
        with sqlite3.connect(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER fail_affix_operation BEFORE INSERT ON accessory_transaction_operations "
                "BEGIN SELECT RAISE(ABORT,'reject operation'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.set_locks("rollback")
        self.assertEqual(self.state(), before)
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM accessory_transaction_operations").fetchone()[0],
                0,
            )

    def test_request_requires_startup_migration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            with self.assertRaises(sqlite3.OperationalError):
                AccessoryAffixApplication(game, self.player).set_locks(
                    "no-ddl", "lock", "u", "acc-1", self.accessory, (0,)
                )
            with sqlite3.connect(game) as conn:
                self.assertIsNone(
                    conn.execute(
                        "SELECT name FROM sqlite_master WHERE name='accessory_transaction_operations'"
                    ).fetchone()
                )

    def test_migration_is_routed_only_to_game_database(self) -> None:
        migrations = build_migrations()
        routed = {
            key: {migration.version for migration in migrations_for_database(migrations, key)}
            for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db")
        }
        self.assertIn("back.017", routed["game_db"])
        self.assertTrue(all("back.017" not in versions for key, versions in routed.items() if key != "game_db"))

    def test_migration_preserves_existing_legacy_operation_table(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            with sqlite3.connect(game) as conn:
                conn.execute(
                    "CREATE TABLE accessory_transaction_operations("
                    "operation_id TEXT PRIMARY KEY,action TEXT NOT NULL,payload TEXT NOT NULL,"
                    "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                conn.execute(
                    "INSERT INTO accessory_transaction_operations"
                    "(operation_id,action,payload,result_json) VALUES('old','lock','{}','{}')"
                )
            with DatabaseUnitOfWork(game) as uow:
                apply_accessory_affix_operations(uow)
            with sqlite3.connect(game) as conn:
                self.assertEqual(
                    conn.execute(
                        "SELECT action FROM accessory_transaction_operations WHERE operation_id='old'"
                    ).fetchone()[0],
                    "lock",
                )


if __name__ == "__main__":
    unittest.main()
