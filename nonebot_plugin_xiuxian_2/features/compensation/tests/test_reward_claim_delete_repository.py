from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from ..application import CompensationApplication
from ..migrations import (
    apply_compensation_reward_claim_schema,
    apply_compensation_reward_catalog_schema,
)
from ..reward_claim_repository import CompensationRewardClaimSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ....xiuxian.xiuxian_compensation import common as compensation_common


class CompensationRewardClaimDeleteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="compensation-claim-delete-")
        self.root = Path(self.temp.name)
        self.database = self.root / "game.db"
        self.catalog_snapshots = [self.root / f"snapshot-{index}.json" for index in range(4)]
        for path in self.catalog_snapshots:
            path.write_text("{}", encoding="utf-8")
        with DatabaseUnitOfWork(self.database) as uow:
            apply_compensation_reward_claim_schema(uow)
            OperationLedger().ensure_schema(uow)
            apply_compensation_reward_catalog_schema(
                uow,
                *self.catalog_snapshots,
                occurred_at="2026-10-02 12:00:00",
            )
        self.repository = CompensationRewardClaimSqlRepository(self.database, 0)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def seed(self) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.executemany(
                "INSERT INTO compensation_reward_definitions("
                "reward_type,record_id,version,record_json,created_at,updated_at) "
                "VALUES('礼包',?,1,'{}','2026-10-02','2026-10-02')",
                (("G1",), ("G2",)),
            )
            uow.executemany(
                "INSERT INTO reward_claims(reward_type,record_id,user_id) VALUES(?,?,?)",
                (
                    ("礼包", "G1", "u1"),
                    ("礼包", "G1", "u2"),
                    ("礼包", "G2", "u1"),
                    ("兑换码", "G1", "u1"),
                ),
            )
            uow.executemany(
                "INSERT INTO reward_claim_counters(reward_type,record_id,baseline_count) VALUES(?,?,?)",
                (("礼包", "G1", 7), ("礼包", "G2", 3), ("兑换码", "G1", 4)),
            )

    def scalar(self, sql: str, params: tuple = ()) -> int:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return int(uow.execute(sql, params).fetchone()[0])

    def test_delete_one_record_replays_and_rejects_payload_conflict(self) -> None:
        self.seed()

        first = self.repository.delete_claims("delete-g1", "礼包", "G1")
        replay = self.repository.delete_claims("delete-g1", "礼包", "G1")
        conflict = self.repository.delete_claims("delete-g1", "礼包", "G2")

        self.assertEqual((first.status, first.deleted_claims, first.deleted_counters), ("applied", 2, 1))
        self.assertEqual((replay.status, replay.deleted_claims, replay.deleted_counters), ("replayed", 2, 1))
        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM reward_claims WHERE reward_type='礼包' AND record_id='G2'"), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM reward_claims WHERE reward_type='兑换码'"), 1)

    def test_delete_type_removes_only_matching_claims_and_counters(self) -> None:
        self.seed()

        result = self.repository.delete_claims("clear-gifts", "礼包")

        self.assertEqual((result.status, result.deleted_claims, result.deleted_counters), ("applied", 3, 2))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM reward_claims WHERE reward_type='礼包'"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM reward_claim_counters WHERE reward_type='礼包'"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM reward_claims WHERE reward_type='兑换码'"), 1)

    def test_delete_record_updates_sql_catalog_without_writing_json(self) -> None:
        self.seed()
        definitions_path = self.root / "gift-records.json"
        claimed_path = self.root / "claimed.json"
        definitions_path.write_text(
            json.dumps({"G1": {"items": []}, "G2": {"items": []}}),
            encoding="utf-8",
        )
        claimed_path.write_text(
            json.dumps({"u1": ["G1", "G2"], "u2": ["G1"]}),
            encoding="utf-8",
        )
        definitions_before = definitions_path.read_bytes()
        claims_before = claimed_path.read_bytes()
        config = {
            "type_key": "礼包",
            "data_path": definitions_path,
            "claimed_path": claimed_path,
        }
        application = CompensationApplication(self.database)

        with patch.object(compensation_common, "_compensation_application", return_value=application):
            result = compensation_common.delete_record("G1", config, "event-delete")

        self.assertTrue(result.succeeded)
        self.assertEqual(definitions_path.read_bytes(), definitions_before)
        self.assertEqual(claimed_path.read_bytes(), claims_before)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM compensation_reward_definitions WHERE reward_type='礼包' AND record_id='G1'"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM compensation_reward_definitions WHERE reward_type='礼包' AND record_id='G2'"), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM reward_claims WHERE reward_type='礼包' AND record_id='G1'"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM reward_claims WHERE reward_type='兑换码' AND record_id='G1'"), 1)

    def test_clear_records_updates_sql_catalog_without_writing_json(self) -> None:
        self.seed()
        definitions_path = self.root / "gift-records.json"
        claimed_path = self.root / "claimed.json"
        definitions_path.write_text(json.dumps({"G1": {"items": []}}), encoding="utf-8")
        claimed_path.write_text(json.dumps({"u1": ["G1"]}), encoding="utf-8")
        definitions_before = definitions_path.read_bytes()
        claims_before = claimed_path.read_bytes()
        config = {
            "type_key": "礼包",
            "data_path": definitions_path,
            "claimed_path": claimed_path,
        }
        application = CompensationApplication(self.database)

        with patch.object(compensation_common, "_compensation_application", return_value=application):
            result = compensation_common.clear_records(config, "event-clear")

        self.assertTrue(result.succeeded)
        self.assertEqual(definitions_path.read_bytes(), definitions_before)
        self.assertEqual(claimed_path.read_bytes(), claims_before)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM compensation_reward_definitions WHERE reward_type='礼包'"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM reward_claims WHERE reward_type='礼包'"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM reward_claims WHERE reward_type='兑换码'"), 1)

    def test_missing_schema_does_not_create_tables_or_change_json_definitions(self) -> None:
        incomplete_database = self.root / "incomplete.db"
        with DatabaseUnitOfWork(incomplete_database) as uow:
            OperationLedger().ensure_schema(uow)
        with DatabaseUnitOfWork(incomplete_database, read_only=True) as uow:
            before_tables = {
                str(row[0])
                for row in uow.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
        incomplete = CompensationRewardClaimSqlRepository(incomplete_database, 0)

        missing = incomplete.delete_claims("delete", "礼包", "G1")

        with DatabaseUnitOfWork(incomplete_database, read_only=True) as uow:
            after_tables = {
                str(row[0])
                for row in uow.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
        self.assertEqual(missing.status, "schema_missing")
        self.assertEqual(after_tables, before_tables)

        definitions_path = self.root / "gift-records.json"
        claimed_path = self.root / "claimed.json"
        original = {"G1": {"items": [], "reason": "test"}}
        definitions_path.write_text(json.dumps(original), encoding="utf-8")
        original_bytes = definitions_path.read_bytes()
        config = {
            "type_key": "礼包",
            "data_path": definitions_path,
            "claimed_path": claimed_path,
        }
        application = CompensationApplication(self.root / "not-created.db")

        with patch.object(compensation_common, "_compensation_application", return_value=application):
            result = compensation_common.delete_record("G1", config, "event-delete")

        self.assertEqual(result.status, "schema_missing")
        self.assertEqual(definitions_path.read_bytes(), original_bytes)
        self.assertFalse((self.root / "not-created.db").exists())


if __name__ == "__main__":
    unittest.main()
