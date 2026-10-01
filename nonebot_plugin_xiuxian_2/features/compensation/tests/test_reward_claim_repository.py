import tempfile
import unittest
from pathlib import Path
from ..reward_claim_repository import CompensationRewardClaimSqlRepository
from ..migrations import apply_compensation_reward_claim_schema
from ....infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend

class CompensationRewardClaimRepositoryTests(unittest.TestCase):
    def test_zero_legacy_baseline_does_not_create_counter_row(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as c:
                c.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                c.execute("INSERT INTO user_xiuxian VALUES('u',10)")
            with DatabaseUnitOfWork(db) as uow:
                apply_compensation_reward_claim_schema(uow)
            repo = CompensationRewardClaimSqlRepository(db, max_goods_num=99)

            result = repo.claim(
                "redeem",
                "兑换码",
                "R1",
                "u",
                [{"type": "stone", "id": "stone", "name": "灵石", "quantity": 1}],
                usage_limit=5,
            )

            self.assertEqual(result.status, "claimed")
            with DatabaseUnitOfWork(db, read_only=True) as uow:
                count = int(
                    uow.execute(
                        "SELECT COUNT(*) FROM reward_claim_counters "
                        "WHERE reward_type='兑换码' AND record_id='R1'"
                    ).fetchone()[0]
                )
            self.assertEqual(count, 0)

    def test_claim_replay_version_and_inventory_are_atomic(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'game.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('u',10)")
                c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))')
                c.execute('CREATE TABLE compensation_definitions(record_id TEXT PRIMARY KEY,version INTEGER)'); c.execute("INSERT INTO compensation_definitions VALUES('r',1)")
            with DatabaseUnitOfWork(db) as uow:
                apply_compensation_reward_claim_schema(uow)
            repo=CompensationRewardClaimSqlRepository(db,max_goods_num=99)
            items=[{'type':'stone','id':'stone','name':'灵石','quantity':5}]
            first=repo.claim('op','补偿','r','u',items,expected_definition_version=1)
            duplicate=repo.claim('op','补偿','r','u',items,expected_definition_version=1)
            changed=repo.claim('op2','补偿','r','u',items,expected_definition_version=2)
            self.assertEqual((first.status,duplicate.status,changed.status),('claimed','duplicate','definition_changed'))

    def test_missing_migration_rejects_without_request_ddl(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / 'game.db'
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)')
                c.execute("INSERT INTO user_xiuxian VALUES('u',10)")
            repo = CompensationRewardClaimSqlRepository(db, max_goods_num=99)

            result = repo.claim('op', '补偿', 'r', 'u', [])

            self.assertEqual(result.status, 'schema_missing')
            with db_backend.connection(db) as c:
                tables = {
                    row[0] for row in c.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
            self.assertNotIn('reward_claims', tables)
            self.assertNotIn('reward_claim_counters', tables)

    def test_missing_database_does_not_get_created_by_claim(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / 'missing.db'
            result = CompensationRewardClaimSqlRepository(db, 99).claim(
                'op', '补偿', 'r', 'u', []
            )

            self.assertEqual(result.status, 'schema_missing')
            self.assertFalse(db.exists())
