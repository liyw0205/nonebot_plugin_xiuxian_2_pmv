from __future__ import annotations

import copy
import json
import random
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.choice_service import (
    PastLifeChoiceService,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.past_life_data import (
    check_early_death,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.past_life_state import (
    INTEGER_FIELDS,
    PAST_LIFE_FIELDS,
    encode_field,
    new_default_state,
)
from tests.test_db_backend import db_backend


class PastLifeChoiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            conn.execute("INSERT INTO user_xiuxian VALUES(%s)", ("u",))
            conn.execute("INSERT INTO user_xiuxian VALUES(%s)", ("v",))
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
        self.service = PastLifeChoiceService(self.game, self.player)
        self.initial = self.make_state()
        self.insert_state("u", self.initial)

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def event(text="第一幕事件"):
        return {
            "text": text,
            "choices": [
                {
                    "text": "继续前行",
                    "branches": {
                        "normal": {
                            "result": "你继续前行。",
                            "effects": {"悟性": 1},
                            "score": 2,
                        }
                    },
                }
            ],
        }

    @classmethod
    def make_state(cls):
        state = new_default_state()
        state.update(
            {
                "state": 2,
                "stage": 0,
                "revision": 1,
                "alloc": {"悟性": 4, "机缘": 4, "根骨": 4, "气运": 4, "心性": 4},
                "accumulated": {"悟性": 20, "机缘": 20, "根骨": 20, "气运": 20, "心性": 20},
                "talent": "定数",
                "birth_scenario": "山村出生",
                "event_indices": [0] * 10,
                "event_snapshots": [cls.event(f"第{index + 1}幕事件") for index in range(10)],
            }
        )
        return state

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

    def final_state(self):
        state = copy.deepcopy(self.initial)
        state.update(
            {
                "stage": 1,
                "revision": 2,
                "accumulated": {**state["accumulated"], "悟性": 21},
                "total_score": 2,
                "history": [{"stage": 0, "choice_text": "继续前行"}],
                "early_death_rolls": {"early:悟性": {"value": 5, "roll": 80, "chance": 10, "triggered": False}},
            }
        )
        return state

    @staticmethod
    def response():
        return {"message": "第一幕完成，进入第二幕。", "is_end": False, "ending": None}

    def advance(self, operation="choice-1", user_id="u", expected=None, final=None):
        return self.service.advance(
            operation,
            user_id,
            1,
            expected or self.initial,
            final or self.final_state(),
            self.response(),
        )

    def read(self, user_id="u"):
        with db_backend.connection(self.player) as conn:
            state = conn.execute(
                "SELECT stage,revision,accumulated,history,early_death_rolls "
                "FROM past_life WHERE user_id=%s",
                (user_id,),
            ).fetchone()
        with db_backend.connection(self.game) as conn:
            count = int(conn.execute(
                "SELECT COUNT(*) FROM past_life_choice_operations"
            ).fetchone()[0]) if conn.table_exists("past_life_choice_operations") else 0
        return state, count

    def test_advance_is_atomic_idempotent_and_replays_reply(self):
        applied = self.advance()
        duplicate = self.advance()
        self.assertEqual("applied", applied.status)
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual(applied.response, duplicate.response)
        state, count = self.read()
        self.assertEqual((1, 2), tuple(map(int, state[:2])))
        self.assertEqual(21, json.loads(state[2])["悟性"])
        self.assertEqual("继续前行", json.loads(state[3])[0]["choice_text"])
        self.assertFalse(json.loads(state[4])["early:悟性"]["triggered"])
        self.assertEqual(1, count)

    def test_stale_full_snapshot_rejects_all_writes(self):
        stale = copy.deepcopy(self.initial)
        stale["total_score"] = 99
        before = self.read()
        self.assertEqual("state_changed", self.advance(expected=stale).status)
        self.assertEqual(before, self.read())

    def test_different_operation_cannot_advance_same_revision_twice(self):
        self.assertEqual("applied", self.advance().status)
        self.assertEqual("state_changed", self.advance("choice-2").status)
        state, count = self.read()
        self.assertEqual((1, 2), tuple(map(int, state[:2])))
        self.assertEqual(1, count)

    def test_same_operation_for_another_user_is_conflict(self):
        self.insert_state("v", self.initial)
        self.assertEqual("applied", self.advance().status)
        self.assertEqual(
            "operation_conflict", self.advance(user_id="v").status
        )
        state, count = self.read("v")
        self.assertEqual((0, 1), tuple(map(int, state[:2])))
        self.assertEqual(1, count)

    def test_operation_insert_failure_rolls_back_stage(self):
        with db_backend.transaction(self.game) as conn:
            PastLifeChoiceService.ensure_operation_schema(conn)
            conn.execute(
                "CREATE TRIGGER reject_past_choice BEFORE INSERT ON "
                "past_life_choice_operations BEGIN SELECT RAISE(ABORT,'reject choice'); END"
            )
        before = self.read()
        with self.assertRaises(db_backend.IntegrityError):
            self.advance()
        self.assertEqual(before, self.read())

    def test_early_death_rolls_are_deterministic_and_auditable(self):
        attrs = {name: -100 for name in ("悟性", "机缘", "根骨", "气运", "心性")}
        first_rolls, second_rolls = {}, {}
        first = check_early_death(
            0, attrs, attrs, self.event(), first_rolls, random.Random(12345)
        )
        second = check_early_death(
            0, attrs, attrs, self.event(), second_rolls, random.Random(12345)
        )
        self.assertEqual(first, second)
        self.assertEqual(first_rolls, second_rolls)
        self.assertTrue(first_rolls["early:悟性"]["triggered"])
        self.assertEqual(100, first_rolls["early:悟性"]["chance"])

    def test_engine_replays_after_stage_has_already_advanced(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life import past_life_events

        class FixedLimit:
            def get_user_state(self, user_id):
                return copy.deepcopy(self_state)

        self_state = self.initial
        engine = past_life_events.PastLifeEngine()
        with (
            patch.object(past_life_events, "choice_service", self.service),
            patch.object(past_life_events, "past_life_limit", FixedLimit()),
        ):
            applied = engine.process_choice("u", 1, "past-life-choice:u:message-1")
            duplicate = engine.process_choice("u", 1, "past-life-choice:u:message-1")
        self.assertEqual("applied", applied["operation_status"])
        self.assertEqual("duplicate", duplicate["operation_status"])
        self.assertEqual(applied["message"], duplicate["message"])
        self.assertEqual(applied["ending"], duplicate["ending"])


if __name__ == "__main__":
    unittest.main()
