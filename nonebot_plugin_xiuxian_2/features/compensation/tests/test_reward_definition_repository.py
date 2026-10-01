from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ..application import CompensationApplication
from ..migrations import (
    apply_compensation_reward_claim_schema,
    apply_compensation_reward_catalog_schema,
)
from ..reward_claim_repository import CompensationRewardClaimSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger


class CompensationRewardDefinitionRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="compensation-reward-definitions-")
        self.root = Path(self.temp.name)
        self.database = self.root / "game.db"
        self.gift_definitions = self.root / "gift.json"
        self.gift_claims = self.root / "gift-claims.json"
        self.redeem_definitions = self.root / "redeem.json"
        self.redeem_claims = self.root / "redeem-claims.json"
        self.gift_definitions.write_text(
            json.dumps({"G1": {"items": [], "reason": "old gift"}}),
            encoding="utf-8",
        )
        self.gift_claims.write_text(
            json.dumps({"u1": ["G1"], "u2": ["G1"]}), encoding="utf-8"
        )
        self.redeem_definitions.write_text(
            json.dumps(
                {
                    "R1": {
                        "items": [{"type": "stone", "id": "stone", "name": "灵石", "quantity": 5}],
                        "usage_limit": 5,
                        "used_count": 3,
                    }
                }
            ),
            encoding="utf-8",
        )
        self.redeem_claims.write_text(
            json.dumps({"u1": ["R1"], "u3": ["R1"]}), encoding="utf-8"
        )
        self.migrate()
        self.application = CompensationApplication(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def migrate(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            apply_compensation_reward_claim_schema(uow)
            OperationLedger().ensure_schema(uow)
            apply_compensation_reward_catalog_schema(
                uow,
                self.gift_definitions,
                self.gift_claims,
                self.redeem_definitions,
                self.redeem_claims,
                occurred_at="2026-10-02 12:00:00",
            )

    def scalar(self, sql: str, params: tuple = ()) -> int:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return int(uow.execute(sql, params).fetchone()[0])

    def test_startup_imports_definitions_claims_and_redeem_baseline_once(self) -> None:
        gifts = self.application.reward_definitions("礼包")
        redeem = self.application.reward_definitions("兑换码")

        self.assertEqual(gifts["G1"]["reason"], "old gift")
        self.assertEqual(gifts["G1"]["_definition_version"], 1)
        self.assertEqual(redeem["R1"]["_definition_version"], 1)
        self.assertEqual(
            self.application.reward_definition("兑换码", "R1"), redeem["R1"]
        )
        self.assertEqual(self.application.get_claim_count("礼包", "G1"), 2)
        self.assertEqual(
            self.application.list_claims("礼包"),
            {"u1": ["G1"], "u2": ["G1"]},
        )
        self.assertEqual(self.application.has_claimed("礼包", "G1", "u2"), True)
        self.assertEqual(self.application.has_claimed("兑换码", "R1", "u3"), True)
        self.assertEqual(self.application.get_used_count("兑换码", "R1"), 3)

        self.gift_definitions.write_text(
            json.dumps({"CHANGED": {"items": []}}), encoding="utf-8"
        )
        self.gift_claims.write_text(json.dumps({"u9": ["CHANGED"]}), encoding="utf-8")
        self.migrate()

        self.assertEqual(set(self.application.reward_definitions("礼包")), {"G1"})
        self.assertTrue(self.application.has_claimed("礼包", "G1", "u2"))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM compensation_reward_catalog_migrations"), 1)

    def test_migration_reconciles_existing_sql_claims_and_zero_baseline(self) -> None:
        database = self.root / "existing-claims.db"
        with DatabaseUnitOfWork(database) as uow:
            apply_compensation_reward_claim_schema(uow)
            OperationLedger().ensure_schema(uow)
            uow.execute(
                "INSERT INTO reward_claims(reward_type,record_id,user_id) "
                "VALUES('兑换码','R1','u-existing')"
            )
            uow.execute(
                "INSERT INTO reward_claim_counters(reward_type,record_id,baseline_count) "
                "VALUES('兑换码','R1',8)"
            )
            apply_compensation_reward_catalog_schema(
                uow,
                self.gift_definitions,
                self.gift_claims,
                self.redeem_definitions,
                self.redeem_claims,
                occurred_at="2026-10-02 12:00:00",
            )

        claims = CompensationRewardClaimSqlRepository(database, 99)
        self.assertEqual(claims.get_used_count("兑换码", "R1"), 3)
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            counter_count = int(
                uow.execute(
                    "SELECT COUNT(*) FROM reward_claim_counters "
                    "WHERE reward_type='兑换码' AND record_id='R1'"
                ).fetchone()[0]
            )
        self.assertEqual(counter_count, 0)

    def test_malformed_snapshot_does_not_record_completed_migration(self) -> None:
        database = self.root / "malformed.db"
        self.gift_definitions.write_text("{broken", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "invalid compensation reward snapshot"):
            with DatabaseUnitOfWork(database) as uow:
                apply_compensation_reward_claim_schema(uow)
                OperationLedger().ensure_schema(uow)
                apply_compensation_reward_catalog_schema(
                    uow,
                    self.gift_definitions,
                    self.gift_claims,
                    self.redeem_definitions,
                    self.redeem_claims,
                    occurred_at="2026-10-02 12:00:00",
                )

        with DatabaseUnitOfWork(database, read_only=True) as uow:
            tables = {
                str(row["name"])
                for row in uow.query_all(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        self.assertNotIn("compensation_reward_catalog_migrations", tables)

    def test_upsert_replays_and_checks_definition_version(self) -> None:
        record = {"items": [], "reason": "replacement", "expire_time": "无限"}
        created = self.application.upsert_reward_definition(
            "upsert-g1", "礼包", "G1", "G1 replacement", record, 1
        )
        replay = self.application.upsert_reward_definition(
            "upsert-g1", "礼包", "G1", "G1 replacement", record, 1
        )
        conflict = self.application.upsert_reward_definition(
            "upsert-g1", "礼包", "G1", "different request", record, 1
        )
        stale = self.application.upsert_reward_definition(
            "upsert-g2", "礼包", "G1", "stale", record, 1
        )

        self.assertEqual((created.status, created.version), ("updated", 2))
        self.assertTrue(replay.replayed)
        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual(stale.status, "definition_changed")
        self.assertEqual(
            self.application.reward_definitions("礼包")["G1"]["reason"],
            "replacement",
        )

    def test_delete_and_clear_remove_definitions_and_claims_atomically(self) -> None:
        deleted = self.application.delete_reward_definition("delete-g1", "礼包", "G1")
        replay = self.application.delete_reward_definition("delete-g1", "礼包", "G1")

        self.assertTrue(deleted.succeeded)
        self.assertTrue(replay.replayed)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM reward_claims WHERE reward_type='礼包'"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM reward_claim_counters WHERE reward_type='礼包'"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM compensation_reward_definitions WHERE reward_type='礼包'"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM reward_claims WHERE reward_type='兑换码'"), 2)

        cleared = self.application.clear_reward_definitions("clear-redeem", "兑换码")
        self.assertTrue(cleared.succeeded)
        self.assertEqual(self.application.reward_definitions("兑换码"), {})
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM reward_claims WHERE reward_type='兑换码'"), 0)

    def test_missing_schema_fails_closed_without_creating_tables(self) -> None:
        database = self.root / "unmigrated.db"
        with DatabaseUnitOfWork(database) as uow:
            OperationLedger().ensure_schema(uow)
        application = CompensationApplication(database)

        result = application.delete_reward_definition("delete", "礼包", "G1")

        self.assertEqual(result.status, "schema_missing")
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            tables = {
                str(row[0])
                for row in uow.execute("SELECT name FROM sqlite_master WHERE type='table'")
                if not str(row[0]).startswith("sqlite_")
            }
        self.assertEqual(tables, {"operation_ledger", "operation_audit"})

    def test_reward_claim_checks_catalog_version_and_commits_asset(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER NOT NULL)"
            )
            uow.execute("INSERT INTO user_xiuxian VALUES('u4', 10)")
        claims = CompensationRewardClaimSqlRepository(self.database, 99)
        reward = [{"type": "stone", "quantity": 5}]

        stale = claims.claim(
            "claim-stale", "礼包", "G1", "u4", reward, expected_definition_version=2
        )
        claimed = claims.claim(
            "claim-valid", "礼包", "G1", "u4", reward, expected_definition_version=1
        )

        self.assertEqual(stale.status, "definition_changed")
        self.assertEqual(claimed.status, "claimed")
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id='u4'"), 15)


if __name__ == "__main__":
    unittest.main()
