from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.past_life_state import (
    INTEGER_FIELDS,
    PAST_LIFE_FIELDS,
    encode_field,
    new_default_state,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.transaction_service import (
    STATISTICS_FIELD,
    PastLifeStartResult,
    PastLifeStartService,
)
from tests.test_db_backend import db_backend


class PastLifeStartTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES(%s,%s)", ("u", 100))
            conn.execute("INSERT INTO user_xiuxian VALUES(%s,%s)", ("v", 100))
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
            conn.execute("CREATE TABLE statistics(user_id TEXT PRIMARY KEY)")
        self.service = PastLifeStartService(self.game, self.player)
        self.slot = datetime(2026, 7, 14, 12, 0, 0)

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def plan():
        event = {
            "text": "你在山村醒来。",
            "choices": [
                {"text": "入山", "effects": {"悟性": 1}},
                {"text": "留村", "effects": {"心性": 1}},
            ],
        }
        return {
            "alloc": {"悟性": 4, "机缘": 4, "根骨": 4, "气运": 3, "心性": 3},
            "accumulated": {"悟性": 5, "机缘": 4, "根骨": 4, "气运": 3, "心性": 3},
            "talent": "天生慧根",
            "birth_scenario": "你出生在山脚下的村庄。",
            "event_indices": [0, 1],
            "event_snapshots": [event, {**event, "text": "你踏入了宗门。"}],
            "first_stage_message": "第一幕：你在山村醒来。\n[1] 入山\n[2] 留村",
            "choices_count": 2,
        }

    def start(self, operation="start-1", user_id="u", expected=None, **overrides):
        args = self.plan()
        args.update(overrides)
        return self.service.start(
            operation,
            user_id,
            expected if expected is not None else new_default_state(),
            refresh_slot_start=self.slot,
            **args,
        )

    def insert_state(self, user_id: str, state: dict) -> None:
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

    def read(self, user_id="u"):
        with db_backend.connection(self.player) as conn:
            state = conn.execute(
                "SELECT state,stage,revision,alloc,talent,birth_scenario,"
                "event_indices,event_snapshots,last_run_time,total_runs "
                "FROM past_life WHERE user_id=%s",
                (user_id,),
            ).fetchone()
            columns = {
                str(row[1])
                for row in conn.execute("PRAGMA table_info(statistics)").fetchall()
            }
            statistic = None
            if STATISTICS_FIELD in columns:
                statistic = conn.execute(
                    "SELECT " + db_backend.quote_ident(STATISTICS_FIELD)
                    + " FROM statistics WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
        with db_backend.connection(self.game) as conn:
            operations = int(conn.execute(
                "SELECT COUNT(*) FROM past_life_start_operations"
            ).fetchone()[0]) if conn.table_exists("past_life_start_operations") else 0
        return state, statistic, operations

    def test_start_is_atomic_idempotent_and_returns_same_first_stage(self):
        applied = self.start()
        duplicate = self.start()
        self.assertEqual("applied", applied.status)
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual(applied.message, duplicate.message)
        self.assertEqual(applied.alloc, duplicate.alloc)
        self.assertEqual(1, duplicate.revision)

        state, statistic, operations = self.read()
        self.assertEqual((2, 0, 1), tuple(map(int, state[:3])))
        self.assertEqual(self.plan()["alloc"], json.loads(state[3]))
        self.assertEqual("天生慧根", state[4])
        self.assertEqual("你出生在山脚下的村庄。", state[5])
        self.assertEqual([0, 1], json.loads(state[6]))
        self.assertEqual(2, len(json.loads(state[7])))
        self.assertIsNone(state[8])
        self.assertEqual(0, int(state[9]))
        self.assertEqual(1, int(statistic[0]))
        self.assertEqual(1, operations)

    def test_same_operation_for_another_user_is_conflict(self):
        self.assertEqual("applied", self.start().status)
        self.assertEqual(
            "operation_conflict",
            self.start(user_id="v").status,
        )
        self.assertEqual((None, None, 1), self.read("v"))

    def test_existing_active_run_rejects_new_operation(self):
        self.assertEqual("applied", self.start().status)
        self.assertEqual("already_started", self.start("start-2").status)
        state, statistic, operations = self.read()
        self.assertEqual(1, int(state[2]))
        self.assertEqual(1, int(statistic[0]))
        self.assertEqual(1, operations)

    def test_current_refresh_slot_is_cooldown(self):
        state = new_default_state()
        state["last_run_time"] = "2026-07-14 12:00:00"
        self.insert_state("u", state)
        self.assertEqual("cooldown", self.start(expected=state).status)
        persisted, statistic, operations = self.read()
        self.assertEqual(0, int(persisted[0]))
        self.assertIsNone(statistic)
        self.assertEqual(0, operations)

    def test_stale_snapshot_rejects_all_writes(self):
        current = new_default_state()
        current["revision"] = 7
        self.insert_state("u", current)
        self.assertEqual("state_changed", self.start().status)
        state, statistic, operations = self.read()
        self.assertEqual(7, int(state[2]))
        self.assertIsNone(statistic)
        self.assertEqual(0, operations)

    def test_operation_insert_failure_rolls_back_state_and_statistic(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE past_life_start_operations("
                "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,"
                "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER reject_past_life_start BEFORE INSERT ON "
                "past_life_start_operations BEGIN SELECT RAISE(ABORT,'reject start'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.start()
        self.assertEqual((None, None, 0), self.read())

    def test_engine_freezes_same_plan_for_same_operation(self):
        from unittest.mock import patch

        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life import past_life_events

        class RecordingService:
            def __init__(self):
                self.calls = []

            def start(self, operation_id, user_id, expected_state, **kwargs):
                self.calls.append((operation_id, user_id, expected_state, kwargs))
                return PastLifeStartResult(
                    "applied",
                    kwargs["first_stage_message"],
                    kwargs["choices_count"],
                    kwargs["alloc"],
                    kwargs["talent"],
                    kwargs["birth_scenario"],
                    1,
                )

        class FixedLimit:
            @staticmethod
            def get_user_state(user_id):
                return new_default_state()

            @staticmethod
            def get_refresh_slot_start(now=None):
                return datetime(2026, 7, 14, 12, 0, 0)

        recorder = RecordingService()
        engine = past_life_events.PastLifeEngine()
        with (
            patch.object(past_life_events, "start_service", recorder),
            patch.object(past_life_events, "past_life_limit", FixedLimit()),
        ):
            first = engine.start_new_life("u", "past-life-start:u:message-1")
            second = engine.start_new_life("u", "past-life-start:u:message-1")

        self.assertEqual(first, second)
        self.assertEqual(recorder.calls[0], recorder.calls[1])


if __name__ == "__main__":
    unittest.main()
