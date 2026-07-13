from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_compensation.definition_service import (
    CompensationDefinitionConflict,
    CompensationDefinitionService,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_compensation.reward_service import (
    RewardClaimService,
)
from tests.test_db_backend import db_backend


class CompensationDefinitionServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.database = root / "compensation.db"
        self.definitions_path = root / "compensation_records.json"
        self.claims_path = root / "claimed_records.json"
        self.write_legacy(
            {
                "C1": {
                    "items": [
                        {
                            "type": "stone",
                            "id": "stone",
                            "name": "灵石",
                            "quantity": 50,
                        }
                    ],
                    "reason": "维护补偿",
                    "start_time": None,
                    "expire_time": "无限",
                }
            },
            {"u1": ["C1"]},
        )
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT PRIMARY KEY,stone INTEGER NOT NULL)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(%s,%s)",
                (("u1", 100), ("u2", 200)),
            )
            conn.execute(
                "CREATE TABLE back("
                "user_id TEXT NOT NULL,goods_id INTEGER NOT NULL,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER DEFAULT 0,UNIQUE(user_id,goods_id))"
            )
        self.service = CompensationDefinitionService(
            self.database, self.definitions_path, self.claims_path
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_legacy(self, definitions, claims) -> None:
        self.definitions_path.write_text(
            json.dumps(definitions, ensure_ascii=False), encoding="utf-8"
        )
        self.claims_path.write_text(
            json.dumps(claims, ensure_ascii=False), encoding="utf-8"
        )

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def test_migrates_legacy_definitions_and_claims_only_once(self) -> None:
        definitions = self.service.list(occurred_at="2026-07-14 09:00:00")

        self.assertEqual(definitions["C1"]["_definition_version"], 1)
        self.assertEqual(definitions["C1"]["reason"], "维护补偿")
        self.assertEqual(self.service.claimed_data(), {"u1": ["C1"]})
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM compensation_legacy_migrations"), 1)

        self.write_legacy({"CHANGED": {"items": []}}, {"u2": ["CHANGED"]})
        restarted = CompensationDefinitionService(
            self.database, self.definitions_path, self.claims_path
        )
        self.assertEqual(set(restarted.list()), {"C1"})
        self.assertEqual(restarted.claimed_data(), {"u1": ["C1"]})

    def test_sync_increments_revision_and_rejects_stale_definition(self) -> None:
        current = self.service.list()
        changed = dict(current["C1"])
        changed["reason"] = "追加补偿"

        updated = self.service.sync({"C1": changed})
        self.assertEqual(updated["C1"]["_definition_version"], 2)

        stale = dict(current["C1"])
        stale["reason"] = "陈旧修改"
        with self.assertRaises(CompensationDefinitionConflict):
            self.service.sync({"C1": stale})
        self.assertEqual(self.service.get("C1").record["reason"], "追加补偿")

    def test_delete_atomically_removes_definition_claims_and_counter(self) -> None:
        definition = self.service.get("C1")
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO reward_claim_counters VALUES('补偿','C1',3)"
            )

        first = self.service.delete(
            "delete:C1:v1", "C1", definition.version,
            occurred_at="2026-07-14 09:10:00",
        )
        duplicate = self.service.delete(
            "delete:C1:v1", "C1", definition.version,
            occurred_at="2026-07-14 09:11:00",
        )

        self.assertEqual(
            (first.status, first.removed_definitions, first.removed_claims),
            ("deleted", 1, 1),
        )
        self.assertEqual(duplicate.status, "duplicate")
        self.assertIsNone(self.service.get("C1"))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM reward_claims"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM reward_claim_counters"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM compensation_definition_operations"), 1)

    def test_delete_rechecks_version_and_recreated_record_has_new_revision(self) -> None:
        current = self.service.get("C1")
        mismatch = self.service.delete("delete:stale", "C1", 99)
        self.assertEqual(mismatch.status, "definition_changed")
        self.assertIsNotNone(self.service.get("C1"))

        self.service.delete("delete:v1", "C1", current.version)
        recreated_record = {
            key: value
            for key, value in current.record.items()
            if key != "_definition_version"
        }
        recreated = self.service.sync({"C1": recreated_record})
        self.assertEqual(recreated["C1"]["_definition_version"], 2)

        old_operation = self.service.delete("delete:v1", "C1", 1)
        self.assertEqual(old_operation.status, "duplicate")
        self.assertIsNotNone(self.service.get("C1"))

    def test_missing_definition_delete_cleans_orphan_claims_idempotently(self) -> None:
        self.service.list()
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO reward_claims(reward_type,record_id,user_id) "
                "VALUES('补偿','ORPHAN','u2')"
            )

        first = self.service.delete("delete:orphan", "ORPHAN", None)
        duplicate = self.service.delete("delete:orphan", "ORPHAN", None)

        self.assertEqual(
            (first.status, first.removed_definitions, first.removed_claims),
            ("missing", 0, 1),
        )
        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual(
            self.scalar(
                "SELECT COUNT(*) FROM reward_claims WHERE record_id='ORPHAN'"
            ),
            0,
        )

    def test_delete_failure_rolls_back_definition_and_claim_cleanup(self) -> None:
        definition = self.service.get("C1")
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_compensation_delete "
                "BEFORE INSERT ON compensation_definition_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.delete("delete:failed", "C1", definition.version)

        self.assertIsNotNone(self.service.get("C1"))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM reward_claims"), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM compensation_definition_operations"), 0)

    def test_clear_rechecks_catalog_and_is_idempotent(self) -> None:
        definitions = self.service.list()
        self.service.sync(
            {
                **definitions,
                "C2": {
                    "items": [],
                    "reason": "第二份补偿",
                    "start_time": None,
                    "expire_time": "无限",
                },
            }
        )
        catalog = self.service.catalog_version()

        changed = self.service.clear("clear:stale", "not-the-catalog")
        first = self.service.clear("clear:catalog", catalog)
        duplicate = self.service.clear("clear:catalog", catalog)

        self.assertEqual(changed.status, "definition_changed")
        self.assertEqual(
            (first.status, first.removed_definitions, first.removed_claims),
            ("cleared", 2, 1),
        )
        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual(self.service.list(), {})

    def test_claim_transaction_rechecks_definition_version(self) -> None:
        definition = self.service.get("C1")
        reward_service = RewardClaimService(self.database, max_goods_num=999)
        first = reward_service.claim(
            "补偿",
            "C1",
            "u2",
            definition.record["items"],
            expected_definition_version=definition.version,
        )
        self.assertEqual(first.status, "claimed")
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id='u2'"), 250)

        current = self.service.get("C1")
        changed_record = dict(current.record)
        changed_record["reason"] = "定义已更新"
        updated = self.service.sync({"C1": changed_record})["C1"]
        stale = reward_service.claim(
            "补偿",
            "C1",
            "u1",
            updated["items"],
            expected_definition_version=current.version,
        )
        self.assertEqual(stale.status, "definition_changed")
        self.assertEqual(self.scalar("SELECT stone FROM user_xiuxian WHERE user_id='u1'"), 100)

        self.service.delete("delete:updated", "C1", updated["_definition_version"])
        missing = reward_service.claim(
            "补偿",
            "C1",
            "u1",
            updated["items"],
            expected_definition_version=updated["_definition_version"],
        )
        self.assertEqual(missing.status, "record_missing")

    def test_production_compensation_paths_use_definition_service(self) -> None:
        root = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_compensation"
        )
        common = (root / "common.py").read_text(encoding="utf-8")
        compensation = (root / "compensation.py").read_text(encoding="utf-8")
        delete_body = common.split("def delete_record", 1)[1].split(
            "def clear_records", 1
        )[0]
        compensation_delete = delete_body.split("data = load_data(config)", 1)[0]

        self.assertIn("compensation_definition_service.delete(", compensation_delete)
        self.assertIn("compensation_definition_service.clear(", common)
        self.assertIn("expected_definition_version=", common)
        self.assertIn("result = delete_record(comp_id, config)", compensation)
        self.assertNotIn("reward_claim_service.delete_claims", compensation_delete)


if __name__ == "__main__":
    unittest.main()
