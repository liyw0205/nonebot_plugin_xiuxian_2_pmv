from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.past_life_state import (
    INTEGER_FIELDS,
    PAST_LIFE_FIELDS,
    encode_field,
    new_default_state,
    normalize_state,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.reset_service import (
    PastLifeResetService,
)
from tests.test_db_backend import db_backend


class PastLifeResetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE marker(value INTEGER)")
        with db_backend.transaction(self.player) as conn:
            definitions = []
            for field_name in PAST_LIFE_FIELDS:
                data_type = "INTEGER" if field_name in INTEGER_FIELDS else "TEXT"
                definitions.append(
                    f"{db_backend.quote_ident(field_name)} {data_type}"
                )
            conn.execute(
                "CREATE TABLE past_life(user_id TEXT PRIMARY KEY,"
                + ",".join(definitions)
                + ")"
            )
        self.service = PastLifeResetService(self.game, self.player)

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def active_state(revision=5):
        state = new_default_state()
        state.update(
            {
                "state": 2,
                "stage": 4,
                "revision": revision,
                "alloc": {"悟性": 4},
                "accumulated": {"悟性": 9},
                "talent": "旧天赋",
                "birth_scenario": "旧出生",
                "total_score": 12,
                "score_breakdown": {"choice": 4},
                "event_indices": [0, 1],
                "event_snapshots": [{"text": "旧事件"}],
                "early_death_rolls": {"early:悟性": {"roll": 90}},
                "history": [{"stage": 0}],
                "last_run_time": "2026-07-14 12:00:00",
                "total_runs": 2,
                "best_ending": "旧结局",
                "best_score": 66,
                "endings_log": [{"name": "旧结局", "score": 66}],
                "achievement_points": 88,
            }
        )
        return state

    def insert_state(self, user_id: str, state=None) -> None:
        state = normalize_state(state or self.active_state())
        values = [encode_field(name, state[name]) for name in PAST_LIFE_FIELDS]
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "INSERT INTO past_life(user_id,"
                + ",".join(db_backend.quote_ident(name) for name in PAST_LIFE_FIELDS)
                + ") VALUES("
                + ",".join("%s" for _ in range(len(PAST_LIFE_FIELDS) + 1))
                + ")",
                (user_id, *values),
            )

    def read_state(self, user_id: str):
        with db_backend.connection(self.player) as conn:
            cursor = conn.execute(
                "SELECT * FROM past_life WHERE user_id=%s", (user_id,)
            )
            row = cursor.fetchone()
            if row is None:
                return None
            columns = [str(column[0]) for column in cursor.description]
        value = dict(zip(columns, row))
        value.pop("user_id")
        return normalize_state(value)

    def operation_count(self):
        with db_backend.connection(self.game) as conn:
            if not conn.table_exists("past_life_reset_operations"):
                return 0
            return int(conn.execute(
                "SELECT COUNT(*) FROM past_life_reset_operations"
            ).fetchone()[0])

    def test_single_reset_preserves_history_and_is_idempotent(self):
        self.insert_state("u")
        applied = self.service.reset_one("single-1", "u", False)
        duplicate = self.service.reset_one("single-1", "u", False)
        self.assertEqual("applied", applied.status)
        self.assertEqual("duplicate", duplicate.status)
        state = self.read_state("u")
        self.assertEqual((0, 0, 6), (state["state"], state["stage"], state["revision"]))
        self.assertEqual({}, state["alloc"])
        self.assertEqual([], state["history"])
        self.assertIsNone(state["last_run_time"])
        self.assertEqual(2, state["total_runs"])
        self.assertEqual("旧结局", state["best_ending"])
        self.assertEqual(88, state["achievement_points"])
        self.assertEqual(1, self.operation_count())

    def test_single_clear_removes_history_and_missing_record_is_created(self):
        result = self.service.reset_one("single-clear", "new-user", True)
        self.assertEqual("applied", result.status)
        state = self.read_state("new-user")
        self.assertEqual(1, state["revision"])
        self.assertEqual(0, state["total_runs"])
        self.assertEqual([], state["endings_log"])

        self.insert_state("u")
        self.assertEqual(
            "applied", self.service.reset_one("single-clear-u", "u", True).status
        )
        cleared = self.read_state("u")
        self.assertEqual(0, cleared["total_runs"])
        self.assertEqual("", cleared["best_ending"])
        self.assertEqual(0, cleared["best_score"])
        self.assertEqual([], cleared["endings_log"])
        self.assertEqual(0, cleared["achievement_points"])

    def test_single_operation_payload_conflict_does_not_reset_twice(self):
        self.insert_state("u")
        self.assertEqual("applied", self.service.reset_one("same", "u", False).status)
        before = self.read_state("u")
        conflict = self.service.reset_one("same", "u", True)
        self.assertEqual("operation_conflict", conflict.status)
        self.assertEqual(before, self.read_state("u"))

    def test_all_reset_freezes_users_and_runs_in_chunks(self):
        self.insert_state("u", self.active_state(1))
        self.insert_state("v", self.active_state(2))
        created = self.service.create_all("all-1", False)
        self.assertEqual("created", created.status)
        self.assertEqual(2, created.total)
        self.insert_state("w", self.active_state(3))

        first = self.service.run_batch("all-1", batch_size=1)
        self.assertFalse(first.complete)
        self.assertEqual(1, first.processed)
        final = self.service.run_batch("all-1", batch_size=10)
        self.assertTrue(final.complete)
        self.assertEqual((2, 2, 0, 0), (
            final.total, final.applied, final.conflicted, final.missing
        ))
        self.assertEqual(2, self.read_state("u")["revision"])
        self.assertEqual(3, self.read_state("v")["revision"])
        self.assertEqual(2, self.read_state("u")["total_runs"])
        self.assertEqual(2, self.read_state("v")["total_runs"])
        self.assertEqual(2, self.read_state("w")["state"])
        self.assertEqual("duplicate", self.service.create_all("all-1", False).status)

    def test_only_one_all_reset_task_can_be_running(self):
        self.insert_state("u")
        first = self.service.create_all("all-first", False)
        resumed = self.service.create_all("all-second", False)
        conflict = self.service.create_all("all-clear", True)
        self.assertEqual("created", first.status)
        self.assertEqual("resumed", resumed.status)
        self.assertEqual("all-first", resumed.operation_id)
        self.assertEqual("operation_conflict", conflict.status)
        self.assertEqual("all-first", conflict.operation_id)
        self.assertEqual(1, self.operation_count())

    def test_all_reset_marks_changed_and_missing_targets_without_overwrite(self):
        self.insert_state("u", self.active_state(1))
        self.insert_state("v", self.active_state(2))
        self.insert_state("w", self.active_state(3))
        self.service.create_all("all-conflict", True)
        changed = self.read_state("v")
        changed["revision"] = 99
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "UPDATE past_life SET revision=%s WHERE user_id=%s", (99, "v")
            )
            conn.execute("DELETE FROM past_life WHERE user_id=%s", ("w",))

        result = self.service.run_batch("all-conflict", batch_size=10)
        self.assertTrue(result.complete)
        self.assertEqual((1, 1, 1), (
            result.applied, result.conflicted, result.missing
        ))
        self.assertEqual(99, self.read_state("v")["revision"])
        self.assertIsNone(self.read_state("w"))

    def test_failed_later_batch_records_error_and_resumes(self):
        self.insert_state("u", self.active_state(1))
        self.insert_state("v", self.active_state(2))
        self.service.create_all("all-resume", False)
        first = self.service.run_batch("all-resume", batch_size=1)
        self.assertEqual(1, first.processed)

        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TRIGGER reject_v_reset BEFORE UPDATE ON past_life "
                "WHEN OLD.user_id='v' BEGIN SELECT RAISE(ABORT,'reject v'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.run_batch("all-resume", batch_size=1)
        pending = self.service.find_pending_all()
        self.assertIsNotNone(pending)
        self.assertEqual("all-resume", pending.operation_id)
        self.assertEqual(1, pending.processed)
        self.assertIn("reject v", pending.last_error)

        with db_backend.transaction(self.player) as conn:
            conn.execute("DROP TRIGGER reject_v_reset")
        completed = self.service.run_batch("all-resume", batch_size=1)
        self.assertTrue(completed.complete)
        self.assertEqual(2, completed.applied)
        self.assertEqual("", completed.last_error)

    def test_handler_has_no_legacy_reset_write_path(self):
        root = Path(__file__).resolve().parents[1]
        source = (
            root
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_past_life"
            / "__init__.py"
        ).read_text(encoding="utf-8")
        handler = source[source.index("@reset_past_life_cmd.handle"):source.index("# ═══ 工具函数")]
        self.assertIn("past_life_reset_service.reset_one(", handler)
        self.assertIn("past_life_reset_service.create_all(", handler)
        self.assertIn("past_life_reset_service.run_batch(", handler)
        self.assertNotIn("past_life_limit.reset_", handler)


if __name__ == "__main__":
    unittest.main()
