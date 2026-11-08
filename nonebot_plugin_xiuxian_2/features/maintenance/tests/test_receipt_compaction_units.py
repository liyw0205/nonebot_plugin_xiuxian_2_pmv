"""Converter-level units: what each task rewrites, and what it refuses to touch.

The batch engine is exercised end to end in
``tests/test_receipt_compaction_startup.py``; these tests pin the pure conversion
decisions (skip compact rows, skip corrupt rows, never return replay columns) so a
future edit cannot quietly start deleting or rewriting replay data.
"""

from __future__ import annotations

import json
import unittest

from ...base.receipt_payload import RECEIPT_VERSION
from ..receipt_compaction import (
    already_compact,
    convert_past_life,
    convert_pet_hatch,
    convert_world_boss,
)
from ..schemas import TASKS


class ConverterTests(unittest.TestCase):
    def test_task_table_only_rewrites_snapshot_columns(self) -> None:
        columns = {(task.table, task.column) for task in TASKS}
        self.assertNotIn(("past_life_choice_operations", "response_json"), columns)
        self.assertNotIn(("past_life_choice_operations", "user_id"), columns)
        self.assertEqual(len(TASKS), 5)

    def test_compact_rows_are_skipped(self) -> None:
        compact = json.dumps({"v": RECEIPT_VERSION, "kind": "advance", "digest": "x"})
        self.assertTrue(already_compact(compact))
        self.assertIsNone(convert_past_life({"payload": compact, "user_id": "u"}, "advance"))
        self.assertIsNone(convert_pet_hatch({"result_json": compact}))
        self.assertIsNone(convert_world_boss({"payload": compact}))

    def test_corrupt_rows_are_skipped(self) -> None:
        for bad in ("", "not json", "[1,2,3]", "{broken", '{"v": 99, "pets": []}'):
            with self.subTest(payload=bad):
                self.assertIsNone(convert_pet_hatch({"result_json": bad}))
                self.assertIsNone(convert_past_life({"payload": bad, "user_id": "u"}, "advance"))
                self.assertIsNone(convert_world_boss({"payload": bad}))

    def test_non_string_values_are_skipped(self) -> None:
        self.assertIsNone(convert_pet_hatch({"result_json": None}))
        self.assertIsNone(convert_past_life({"payload": 42, "user_id": "u"}, "start"))
        self.assertIsNone(convert_world_boss({"payload": {"a": 1}}))

    def test_past_life_advance_keeps_identity_fields(self) -> None:
        legacy = json.dumps(
            {"user_id": "u", "choice_idx": 3, "expected": {"stage": 7, "revision": 41},
             "final": {"stage": 8, "revision": 42}},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        change = convert_past_life({"payload": legacy, "user_id": "u"}, "advance")
        document = json.loads(change["payload"])
        self.assertEqual(document["v"], RECEIPT_VERSION)
        self.assertEqual(document["kind"], "advance")
        self.assertEqual(document["user_id"], "u")
        self.assertEqual(document["choice_idx"], 3)
        self.assertEqual(document["stage"], 7)
        self.assertEqual(document["revision"], 41)
        self.assertNotIn("expected", change["payload"])

    def test_past_life_final_keeps_reward_totals(self) -> None:
        legacy = json.dumps(
            {"user_id": "u", "ending": "飞升成仙", "score": 95, "exp": 1200,
             "stone": 300, "points": 12, "completed_at": "2026-11-07 12:00:00",
             "expected": {"stage": 9}, "choice_response": {"message": "x"}},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        document = json.loads(convert_past_life({"payload": legacy, "user_id": "u"}, "final")["payload"])
        self.assertEqual(document["kind"], "final")
        self.assertEqual(document["ending"], "飞升成仙")
        self.assertEqual((document["exp"], document["stone"], document["points"]), (1200, 300, 12))
        self.assertEqual(document["completed_at"], "2026-11-07 12:00:00")

    def test_past_life_start_keeps_slot_identity(self) -> None:
        legacy = json.dumps(
            {"user_id": "u", "plan": {"alloc": {}}, "choices_count": 10,
             "refresh_slot_start": "2026-11-07 12:00:00", "message": "第一幕"},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        document = json.loads(convert_past_life({"payload": legacy, "user_id": "u"}, "start")["payload"])
        self.assertEqual(document["kind"], "start")
        self.assertEqual(document["choices_count"], 10)
        self.assertEqual(document["refresh_slot_start"], "2026-11-07 12:00:00")

    def test_world_boss_keeps_trigger_and_revision(self) -> None:
        legacy = json.dumps(
            {"trigger": "scheduled", "expected_revision": 2983, "expected_bosses": [],
             "bosses": [{"jj": "感气境"}, {"jj": "感气境"}]},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        document = json.loads(convert_world_boss({"payload": legacy})["payload"])
        self.assertEqual(document["kind"], "full_refresh")
        self.assertEqual(document["trigger"], "scheduled")
        self.assertEqual(document["expected_revision"], 2983)
        self.assertEqual(document["boss_count"], 2)

    def test_conversion_never_returns_replay_columns(self) -> None:
        legacy = json.dumps({"user_id": "u", "choice_idx": 1, "expected": {}, "final": {}})
        for change in (
            convert_past_life({"payload": legacy, "user_id": "u"}, "advance"),
            convert_world_boss({"payload": legacy}),
        ):
            self.assertEqual(set(change), {"payload"})


if __name__ == "__main__":
    unittest.main()
