from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_compensation.definition_service import (
    CompensationDefinitionConflict,
    CompensationDefinitionService,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_compensation import (
    common as compensation_common,
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

    def test_existing_operation_table_gains_result_snapshot_column(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE compensation_definition_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,action TEXT NOT NULL,"
                "record_id TEXT NOT NULL DEFAULT '',version INTEGER NOT NULL DEFAULT 0,"
                "outcome TEXT NOT NULL,removed_definitions INTEGER NOT NULL DEFAULT 0,"
                "removed_claims INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL)"
            )

        self.service.list()

        with db_backend.connection(self.database) as conn:
            self.assertIn(
                "result_json",
                conn.column_names("compensation_definition_operations"),
            )

    def test_upsert_creates_and_updates_definition_with_stable_versions(self) -> None:
        created_record = {
            "items": [],
            "reason": "新补偿",
            "create_time": "2026-07-14 10:00:00",
            "start_time": "2026-07-14 10:00:00",
            "expire_time": "无限",
        }
        created = self.service.upsert(
            "upsert:create:C2",
            "0 灵石x10 新补偿 无限 0",
            "C2",
            created_record,
            occurred_at="2026-07-14 10:00:00",
        )
        current = self.service.get("C1")
        updated_record = dict(current.record)
        updated_record["reason"] = "更新补偿"
        updated = self.service.upsert(
            "upsert:update:C1",
            "C1 灵石x50 更新补偿 无限 0",
            "C1",
            updated_record,
            current.version,
            occurred_at="2026-07-14 10:01:00",
        )

        self.assertEqual(
            (created.status, created.record_id, created.version),
            ("created", "C2", 1),
        )
        self.assertEqual(created.record["_definition_version"], 1)
        self.assertEqual(
            (updated.status, updated.record_id, updated.version),
            ("updated", "C1", 2),
        )
        self.assertEqual(self.service.get("C1").record["reason"], "更新补偿")

    def test_production_create_replays_before_generating_random_id(self) -> None:
        class Event:
            message_id = "message-1"

            @staticmethod
            def get_user_id():
                return "admin-1"

        send = AsyncMock()
        generate = unittest.mock.Mock(side_effect=["RANDOM1", "RANDOM2"])
        args = "0 灵石x10 维护补偿 1天 0"
        with (
            patch.object(
                compensation_common,
                "compensation_definition_service",
                self.service,
            ),
            patch.object(compensation_common, "generate_unique_id", generate),
            patch.object(compensation_common, "handle_send", send),
        ):
            asyncio.run(
                compensation_common.create_reward_record(
                    object(), Event(), {"type_key": "补偿"}, args
                )
            )
            asyncio.run(
                compensation_common.create_reward_record(
                    object(), Event(), {"type_key": "补偿"}, args
                )
            )

        self.assertEqual(generate.call_count, 1)
        self.assertEqual(send.await_count, 2)
        first_message = send.await_args_list[0].args[2]
        second_message = send.await_args_list[1].args[2]
        self.assertEqual(first_message, second_message)
        self.assertIn("ID：RANDOM1", first_message)
        self.assertIsNotNone(self.service.get("RANDOM1"))
        self.assertIsNone(self.service.get("RANDOM2"))

    def test_upsert_replays_first_random_result_after_change_and_delete(self) -> None:
        first_record = {
            "items": [],
            "reason": "随机补偿",
            "create_time": "2026-07-14 10:10:00",
            "start_time": "2026-07-14 10:10:00",
            "expire_time": "2026-07-15 10:10:00",
        }
        first = self.service.upsert(
            "upsert:message-1:admin",
            "0 灵石x10 随机补偿 1天 0",
            "RANDOM1",
            first_record,
        )
        duplicate_with_new_random_id = self.service.upsert(
            "upsert:message-1:admin",
            "0 灵石x10 随机补偿 1天 0",
            "RANDOM2",
            {**first_record, "create_time": "2026-07-14 10:11:00"},
        )
        changed_record = dict(first.record)
        changed_record["reason"] = "后来更新"
        changed = self.service.upsert(
            "upsert:message-2:admin",
            "RANDOM1 灵石x10 后来更新 1天 0",
            "RANDOM1",
            changed_record,
            first.version,
        )
        self.service.delete(
            "delete:RANDOM1:v2", "RANDOM1", changed.version
        )
        replay = self.service.replay_upsert(
            "upsert:message-1:admin", "0 灵石x10 随机补偿 1天 0"
        )

        self.assertEqual(duplicate_with_new_random_id.status, "duplicate")
        self.assertEqual(duplicate_with_new_random_id.record_id, "RANDOM1")
        self.assertEqual(duplicate_with_new_random_id.record, first.record)
        self.assertEqual(replay.status, "duplicate")
        self.assertEqual(replay.record, first.record)
        self.assertIsNone(self.service.get("RANDOM1"))
        self.assertIsNone(self.service.get("RANDOM2"))

    def test_upsert_rejects_changed_request_for_same_operation(self) -> None:
        record = {"items": [], "reason": "首次", "expire_time": "无限"}
        self.service.upsert("upsert:same", "C2 items 首次", "C2", record)

        conflict = self.service.replay_upsert(
            "upsert:same", "C2 items 已变更"
        )
        conflict_on_upsert = self.service.upsert(
            "upsert:same", "C2 items 已变更", "C3", record
        )

        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual(conflict_on_upsert.status, "operation_conflict")
        self.assertIsNone(self.service.get("C3"))

    def test_upsert_operation_failure_rolls_back_definition_and_revision(self) -> None:
        self.service.list()
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_compensation_upsert "
                "BEFORE INSERT ON compensation_definition_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.upsert(
                "upsert:failed",
                "C2 items failed",
                "C2",
                {"items": [], "reason": "失败", "expire_time": "无限"},
            )

        self.assertIsNone(self.service.get("C2"))
        self.assertIsNone(
            self.scalar(
                "SELECT last_version FROM compensation_definition_revisions "
                "WHERE record_id='C2'"
            )
        )
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM compensation_definition_operations"),
            0,
        )

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
        self.assertIn("compensation_definition_service.upsert(", common)
        self.assertIn("compensation_definition_service.replay_upsert(", common)
        self.assertIn("expected_definition_version=", common)
        self.assertIn("result = delete_record(comp_id, config)", compensation)
        self.assertNotIn("reward_claim_service.delete_claims", compensation_delete)


if __name__ == "__main__":
    unittest.main()
