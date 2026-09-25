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


if __name__ == "__main__":
    unittest.main()
