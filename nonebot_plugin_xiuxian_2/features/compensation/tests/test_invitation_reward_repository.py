from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..invitation_repository import InvitationRewardClaimSqlRepository
from ..migrations import apply_compensation_invitation_reward_schema
from tests.test_db_backend import db_backend


class InvitationRewardRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES(?,?)", ("u1", 10))
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
        with DatabaseUnitOfWork(self.database) as uow:
            apply_compensation_invitation_reward_schema(uow)
        self.repository = InvitationRewardClaimSqlRepository(self.database)
        self.rewards = {
            "1": [{"type": "stone", "id": "stone", "name": "灵石", "quantity": 50}],
            "3": [{"type": "道具", "id": 101, "name": "邀请令", "quantity": 2}],
        }

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_claim_replay_and_legacy_snapshot_are_idempotent(self) -> None:
        result = self.repository.claim(
            "op-1", "u1", ["a", "b", "c", "u1"], self.rewards, [1, 3], [1], 1000
        )
        replay = self.repository.claim(
            "op-1", "u1", ["changed"], {"1": self.rewards["1"]}, [1, 3], [], 1000
        )

        self.assertEqual(("applied", (3,), 3), (result.status, result.thresholds, result.invitation_count))
        self.assertEqual("duplicate", replay.status)
        self.assertEqual({1, 3}, self.repository.claimed_thresholds("u1"))
        with db_backend.connection(self.database) as conn:
            self.assertEqual(10, conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='u1'").fetchone()[0])

    def test_missing_migration_returns_schema_missing_without_creating_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES(?,?)", ("u1", 0))
            result = InvitationRewardClaimSqlRepository(database).claim(
                "op", "u1", ["a"], self.rewards, [1], [], 100
            )
            self.assertEqual("schema_missing", result.status)
            with db_backend.connection(database) as conn:
                tables = {
                    row[0] for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
            self.assertNotIn("invitation_reward_operations", tables)

    def test_operation_failure_rolls_back_rewards_and_claims(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_invitation_operation BEFORE INSERT ON "
                "invitation_reward_operations BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(Exception):
            self.repository.claim("op-fail", "u1", ["a"], self.rewards, [1], [], 1000)
        with db_backend.connection(self.database) as conn:
            self.assertEqual(10, conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='u1'").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM invitation_reward_claims").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
