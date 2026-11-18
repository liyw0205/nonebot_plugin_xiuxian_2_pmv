from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..demon_token_repository import RiftDemonTokenBattleSqlRepository
from ..migrations import apply_rift_demon_token_operations, apply_rift_demon_token_player_schema
from tests.test_db_backend import db_backend


class FixedClock:
    def now(self):
        return datetime(2026, 9, 25, tzinfo=timezone.utc)


class RiftDemonTokenRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="rift-demon-token-")
        root = Path(self.temp.name)
        self.game_db, self.player_db = root / "game.db", root / "player.db"
        self.rift = {"name": "boss", "rank": 4}
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,exp INTEGER,hp INTEGER,mp INTEGER)")
            conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            conn.execute("CREATE TABLE rift_entries(user_id TEXT PRIMARY KEY,rift_data TEXT,status TEXT)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',1000,500,100,80)")
            conn.execute("INSERT INTO user_cd VALUES('u',3,'now','30')")
            conn.execute("INSERT INTO rift_entries VALUES('u',%s,'active')", (json.dumps(self.rift),))
            conn.execute("INSERT INTO back VALUES('u',20018,'token','item',1,'','',1)")
        with db_backend.transaction(self.player_db) as conn:
            conn.execute('CREATE TABLE rift(user_id TEXT PRIMARY KEY,"legacy_field" TEXT)')
            conn.execute("INSERT INTO rift VALUES('u','preserved')")
            conn.execute('CREATE TABLE statistics(user_id TEXT PRIMARY KEY,"existing_stat" INTEGER)')
            conn.execute("INSERT INTO statistics VALUES('u',7)")
        with DatabaseUnitOfWork(self.game_db) as uow:
            apply_rift_demon_token_operations(uow)
        with DatabaseUnitOfWork(self.player_db) as uow:
            apply_rift_demon_token_player_schema(uow)
        with db_backend.transaction(self.player_db) as conn:
            conn.execute('UPDATE rift SET "explore_count"=3 WHERE user_id=\'u\'')
        self.repository = RiftDemonTokenBattleSqlRepository(
            self.game_db, self.player_db, clock=FixedClock()
        )
        self.user = {"stone": 1000, "exp": 500, "hp": 100, "mp": 80}
        self.outcome = {
            "delta": {"stone": 250, "exp": 75, "hp": -30, "mp": -20},
            "items": [{"id": 300, "name": "loot", "type": "weapon", "amount": 1}],
            "statistics": {"秘境打怪": 1, "秘境次数": 1},
            "message": "fixed win",
        }

    def tearDown(self) -> None:
        self.temp.cleanup()

    def settle(self, operation_id="boss-op", **changes):
        values = {
            "user_id": "u",
            "item_id": 20018,
            "expected_rift": self.rift,
            "expected_user": self.user,
            "expected_explore_count": 3,
            "outcome": self.outcome,
            "max_goods_num": 1000,
        }
        values.update(changes)
        return self.repository.settle(operation_id, **values)

    def test_settlement_commits_battle_and_legacy_compatible_replay(self) -> None:
        first = self.settle()
        duplicate = self.settle()
        replay = self.repository.replay("boss-op")
        self.assertEqual(
            ("applied", 4, "duplicate", "duplicate"),
            (first.status, first.explore_count, duplicate.status, replay.status),
        )
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(tuple(conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian").fetchone()), (1250, 575, 70, 60))
            self.assertEqual(tuple(conn.execute("SELECT goods_num,bind_num FROM back WHERE goods_id=20018").fetchone()), (0, 0))
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "settled")
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM back WHERE goods_id=300").fetchone()[0], 1)
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(tuple(conn.execute('SELECT "legacy_field","explore_count" FROM rift WHERE user_id=\'u\'').fetchone()), ("preserved", 4))
            self.assertEqual(tuple(conn.execute('SELECT "existing_stat","秘境打怪","秘境次数" FROM statistics WHERE user_id=\'u\'').fetchone()), (7, 1, 1))

    def test_preexisting_legacy_operation_replays_with_the_same_payload(self) -> None:
        from ....xiuxian.xiuxian_rift.transaction_service import RiftDemonTokenBattleSettlementService

        legacy = RiftDemonTokenBattleSettlementService(self.game_db, self.player_db)
        first = legacy.settle("legacy-op", "u", 20018, self.rift, self.user, 3, self.outcome, 1000)
        replay = self.settle("legacy-op")
        self.assertEqual((first.status, replay.status, replay.explore_count), ("applied", "duplicate", 4))

    def test_missing_operation_migration_is_not_created_by_request_path(self) -> None:
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("DROP TABLE rift_demon_token_battle_operations")
        result = self.settle("without-migration")
        self.assertEqual("schema_missing", result.status)
        with db_backend.connection(self.game_db) as conn:
            self.assertFalse(conn.table_exists("rift_demon_token_battle_operations"))

    def test_tenth_completion_reward_and_inventory_limit(self) -> None:
        with db_backend.transaction(self.player_db) as conn:
            conn.execute('UPDATE rift SET "explore_count"=9 WHERE user_id=\'u\'')
        reward = {"id": 20018, "name": "斩妖令", "type": "特殊道具", "amount": 1}
        result = self.settle(
            "tenth",
            expected_explore_count=9,
            outcome={**self.outcome, "progress_reward": reward},
            max_goods_num=1,
        )
        self.assertEqual((result.status, result.explore_count), ("applied", 0))
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(tuple(conn.execute("SELECT goods_num,bind_num FROM back WHERE goods_id=20018").fetchone()), (1, 1))

    def test_missing_item_and_stats_failure_leave_all_state_unchanged(self) -> None:
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("UPDATE back SET goods_num=0 WHERE goods_id=20018")
        missing = self.settle("missing")
        self.assertEqual("item_missing", missing.status)
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("UPDATE back SET goods_num=1,bind_num=1 WHERE goods_id=20018")
        with db_backend.transaction(self.player_db) as conn:
            conn.execute('CREATE TRIGGER fail_stats BEFORE UPDATE OF "秘境打怪","秘境次数" ON statistics BEGIN SELECT RAISE(ABORT,\'fail\'); END')
        with self.assertRaises(Exception):
            self.settle("rollback")
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(tuple(conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian").fetchone()), (1000, 500, 100, 80))
            self.assertEqual(tuple(conn.execute("SELECT goods_num,bind_num FROM back WHERE goods_id=20018").fetchone()), (1, 1))
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "active")
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(conn.execute('SELECT "explore_count" FROM rift WHERE user_id=\'u\'').fetchone()[0], 3)

    def _snapshot_state(self):
        with db_backend.connection(self.game_db) as conn:
            game = {
                "user": tuple(conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id='u'").fetchone()),
                "item": tuple(conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id='u' AND goods_id=20018").fetchone()),
                "loot": conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id='u' AND goods_id=300").fetchone(),
                "entry": tuple(conn.execute("SELECT rift_data,status FROM rift_entries WHERE user_id='u'").fetchone()),
                "cd": tuple(conn.execute("SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id='u'").fetchone()),
                "receipt": conn.execute("SELECT COUNT(*) FROM rift_demon_token_battle_operations").fetchone()[0],
            }
        with db_backend.connection(self.player_db) as conn:
            player = {
                "rift": tuple(conn.execute("SELECT explore_count FROM rift WHERE user_id='u'").fetchone()),
                "stats": tuple(conn.execute("SELECT \"existing_stat\",\"秘境打怪\",\"秘境次数\" FROM statistics WHERE user_id='u'").fetchone()),
            }
        return game, player

    def test_known_write_rejections_are_specific_and_atomic(self) -> None:
        cases = (
            (
                "CREATE TRIGGER reject_item BEFORE UPDATE OF goods_num ON back BEGIN SELECT RAISE(IGNORE); END",
                "rift_demon_token_item_write_rejected",
                "u",
            ),
            (
                "CREATE TRIGGER rewrite_user AFTER UPDATE OF exp ON user_xiuxian BEGIN UPDATE user_xiuxian SET stone=999 WHERE user_id='u'; END",
                "rift_demon_token_user_write_rejected",
                "u",
            ),
            (
                "CREATE TRIGGER reject_reward BEFORE INSERT ON back WHEN NEW.goods_id=300 BEGIN SELECT RAISE(IGNORE); END",
                "rift_demon_token_reward_write_rejected",
                "300",
            ),
            (
                "CREATE TRIGGER reject_progress BEFORE UPDATE OF explore_count ON rift BEGIN SELECT RAISE(IGNORE); END",
                "rift_demon_token_progress_write_rejected",
                "u",
            ),
            (
                "CREATE TRIGGER rewrite_statistics AFTER UPDATE OF \"秘境打怪\" ON statistics BEGIN UPDATE statistics SET \"秘境打怪\"=99 WHERE user_id='u'; END",
                "rift_demon_token_statistics_write_rejected",
                "秘境打怪",
            ),
            (
                "CREATE TRIGGER rewrite_entry AFTER UPDATE OF status ON rift_entries BEGIN UPDATE rift_entries SET status='active' WHERE user_id='u'; END",
                "rift_demon_token_entry_write_rejected",
                "u",
            ),
            (
                "CREATE TRIGGER reject_cd BEFORE UPDATE OF type ON user_cd BEGIN SELECT RAISE(IGNORE); END",
                "rift_demon_token_cooldown_write_rejected",
                "u",
            ),
            (
                "CREATE TRIGGER reject_receipt BEFORE INSERT ON rift_demon_token_battle_operations BEGIN SELECT RAISE(IGNORE); END",
                "rift_demon_token_receipt_write_rejected",
                "boss-op",
            ),
            (
                "CREATE TRIGGER rewrite_receipt AFTER INSERT ON rift_demon_token_battle_operations BEGIN UPDATE rift_demon_token_battle_operations SET message='rewritten' WHERE operation_id='boss-op'; END",
                "rift_demon_token_receipt_write_rejected",
                "boss-op",
            ),
            (
                "CREATE TRIGGER rewrite_receipt_asset AFTER INSERT ON rift_demon_token_battle_operations BEGIN UPDATE user_xiuxian SET stone=999 WHERE user_id='u'; END",
                "rift_demon_token_user_write_rejected",
                "u",
            ),
        )
        for trigger, reason, subject in cases:
            with self.subTest(reason=reason):
                database = self.player_db if ("statistics" in trigger or "progress" in trigger) else self.game_db
                with db_backend.transaction(database) as conn:
                    conn.execute(trigger)
                before = self._snapshot_state()
                result = self.settle()
                self.assertEqual((result.status, result.reason, result.subject), ("state_changed", reason, subject))
                self.assertEqual(self._snapshot_state(), before)
                with db_backend.transaction(database) as conn:
                    conn.execute(f"DROP TRIGGER {trigger.split()[2]}")

    def test_receipt_rejection_recovers_and_replays_without_duplicate_writes(self) -> None:
        with db_backend.transaction(self.game_db) as conn:
            conn.execute(
                "CREATE TRIGGER reject_receipt BEFORE INSERT ON rift_demon_token_battle_operations "
                "BEGIN SELECT RAISE(IGNORE); END"
            )
        first = self.settle("recover")
        self.assertEqual((first.status, first.reason, first.subject), (
            "state_changed", "rift_demon_token_receipt_write_rejected", "recover"
        ))
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("DROP TRIGGER reject_receipt")
        recovered = self.settle("recover")
        replayed = self.settle("recover")
        self.assertEqual((recovered.status, recovered.explore_count, replayed.status), ("applied", 4, "duplicate"))
        self.assertEqual(self._snapshot_state()[0]["item"], (0, 0))

    def test_abort_write_errors_still_propagate_and_roll_back(self) -> None:
        with db_backend.transaction(self.player_db) as conn:
            conn.execute(
                'CREATE TRIGGER fail_stats BEFORE UPDATE OF "秘境打怪" ON statistics '
                "BEGIN SELECT RAISE(ABORT,'injected stats failure'); END"
            )
        before = self._snapshot_state()
        with self.assertRaises(Exception):
            self.settle("abort")
        self.assertEqual(self._snapshot_state(), before)


if __name__ == "__main__":
    unittest.main()
