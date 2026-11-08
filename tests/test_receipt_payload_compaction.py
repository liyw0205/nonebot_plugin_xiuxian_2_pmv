"""Compact idempotency payloads: keep the equality contract, drop the snapshot.

The read-only audit of the deployment found four ``payload`` columns whose only
code use is ``stored == candidate`` (duplicate / conflict detection) while the
parts a player can see live in separate ``response_json``/``result_json``:

    past_life_choice_operations          60,943 B/row   0.93 MB/day
    past_life_final_operations           34,804 B/row   0.08 MB/day
    past_life_start_operations           30,597 B/row   0.07 MB/day
    world_boss_full_refresh_operations   12,842 B/row   0.40 MB/day

These tests pin the replacement contract: identity + SHA-256 of the exact
canonical snapshot, legacy rows still comparing as replays, conflicts still
detected, and the player-visible replay payload untouched.
"""

from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.base.receipt_payload import (
    RECEIPT_VERSION,
    build_payload,
    payload_identity,
    payload_matches,
    snapshot_digest,
)
from nonebot_plugin_xiuxian_2.features.boss.migrations import (
    apply_boss_full_refresh_player_schema,
    apply_boss_punishment_operations,
)
from nonebot_plugin_xiuxian_2.features.boss.world_boss_repository import (
    WorldBossFullRefreshSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.past_life_state import (
    PAST_LIFE_FIELDS,
    INTEGER_FIELDS,
    encode_field,
    new_default_state,
)
from tests.test_db_backend import db_backend


class ReceiptPayloadHelperTests(unittest.TestCase):
    def test_digest_is_stable_and_sensitive_to_content(self) -> None:
        a = '{"user_id":"u","stage":1}'
        self.assertEqual(snapshot_digest(a), snapshot_digest(a))
        self.assertNotEqual(snapshot_digest(a), snapshot_digest('{"user_id":"u","stage":2}'))

    def test_compact_payload_carries_identity_but_not_the_snapshot(self) -> None:
        snapshot = json.dumps({"expected": {"stage": 3}, "final": {"stage": 4}})
        payload = build_payload(kind="advance", snapshot=snapshot, user_id="u", choice_idx=2, stage=3)
        document = json.loads(payload)
        self.assertEqual(document["v"], RECEIPT_VERSION)
        self.assertEqual(document["kind"], "advance")
        self.assertEqual(document["user_id"], "u")
        self.assertEqual(document["choice_idx"], 2)
        self.assertEqual(document["stage"], 3)
        self.assertEqual(document["digest"], snapshot_digest(snapshot))
        self.assertNotIn("expected", payload)
        self.assertNotIn("final", payload)
        self.assertLess(len(payload.encode("utf-8")), 400)

    def test_v2_payloads_match_only_on_equal_digest(self) -> None:
        a = build_payload(kind="advance", snapshot='{"x":1}', user_id="u")
        b = build_payload(kind="advance", snapshot='{"x":1}', user_id="u")
        c = build_payload(kind="advance", snapshot='{"x":2}', user_id="u")
        self.assertTrue(payload_matches(a, b))
        self.assertFalse(payload_matches(a, c))

    def test_legacy_row_is_still_a_replay(self) -> None:
        legacy = '{"choice_idx":1,"expected":{"stage":1},"final":{"stage":2},"user_id":"u"}'
        self.assertTrue(payload_matches(legacy, build_payload(kind="advance", snapshot=legacy, user_id="u")))
        other = build_payload(
            kind="advance",
            snapshot=legacy.replace('"stage":2', '"stage":3'),
            user_id="u",
        )
        self.assertFalse(payload_matches(legacy, other))

    def test_legacy_candidate_keeps_byte_equality(self) -> None:
        legacy = '{"a":1}'
        self.assertTrue(payload_matches(legacy, legacy))
        self.assertFalse(payload_matches(legacy, '{"a":2}'))

    def test_identity_reads_both_shapes(self) -> None:
        self.assertEqual(
            payload_identity(build_payload(kind="final", snapshot="{}", user_id="u")),
            {"user_id": "u", "kind": "final"},
        )
        self.assertEqual(payload_identity('{"user_id":"legacy","plan":{}}'), {"user_id": "legacy"})
        self.assertEqual(payload_identity("not json"), {})
        self.assertEqual(payload_identity(None), {})


class PastLifePayloadCompactionTests(unittest.TestCase):
    """Drive the real ``PastLifeChoiceService`` against a production-shaped schema."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            conn.execute("INSERT INTO user_xiuxian VALUES(%s)", ("u",))
        with db_backend.transaction(self.player) as conn:
            definitions = []
            for field_name in PAST_LIFE_FIELDS:
                data_type = "INTEGER" if field_name in INTEGER_FIELDS else "TEXT"
                definitions.append(f"{db_backend.quote_ident(field_name)} {data_type}")
            conn.execute(
                "CREATE TABLE past_life(user_id TEXT PRIMARY KEY," + ",".join(definitions) + ")"
            )
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.transaction_service import (
            PastLifeChoiceService,
        )

        self.service = PastLifeChoiceService(self.game, self.player)
        self.initial = self.make_state()
        self.insert_state("u", self.initial)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    @staticmethod
    def make_state() -> dict:
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
                "event_snapshots": [
                    {"text": f"第{index + 1}幕事件", "choices": [{"text": "继续前行"}]}
                    for index in range(10)
                ],
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

    def final_state(self) -> dict:
        state = copy.deepcopy(self.initial)
        state.update(
            {
                "stage": 1,
                "revision": 2,
                "accumulated": {**state["accumulated"], "悟性": 21},
                "total_score": 2,
                "history": [{"stage": 0, "choice_text": "继续前行"}],
            }
        )
        return state

    @staticmethod
    def response() -> dict:
        return {"message": "第一幕完成，进入第二幕。", "is_end": False, "ending": None}

    @staticmethod
    def legacy_snapshot(user_id: str, choice_idx: int, expected: dict, final: dict) -> str:
        return json.dumps(
            {"user_id": user_id, "choice_idx": choice_idx, "expected": expected, "final": final},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    def read_receipt(self, operation_id: str = "choice-1") -> tuple[str, dict]:
        with db_backend.connection(self.game) as conn:
            payload, response_json = conn.execute(
                "SELECT payload,response_json FROM past_life_choice_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
        return str(payload), json.loads(str(response_json))

    def test_choice_receipt_is_a_digest_and_replay_still_works(self) -> None:
        final = self.final_state()
        applied = self.service.advance("choice-1", "u", 1, self.initial, final, self.response())
        self.assertEqual(applied.status, "applied")

        payload, response = self.read_receipt()
        document = json.loads(payload)
        self.assertEqual(document["v"], RECEIPT_VERSION)
        self.assertEqual(document["kind"], "advance")
        self.assertEqual(document["user_id"], "u")
        self.assertEqual(document["choice_idx"], 1)
        self.assertEqual(document["stage"], 0)
        self.assertEqual(
            document["digest"], snapshot_digest(self.legacy_snapshot("u", 1, self.initial, final))
        )
        self.assertNotIn("event_snapshots", payload)
        self.assertNotIn("山村出生", payload)
        legacy = self.legacy_snapshot("u", 1, self.initial, final)
        self.assertLess(len(payload.encode("utf-8")), len(legacy.encode("utf-8")) // 10)
        self.assertLess(len(payload.encode("utf-8")), 600)
        self.assertEqual(response["message"], "第一幕完成，进入第二幕。")

        again = self.service.advance("choice-1", "u", 1, self.initial, final, self.response())
        self.assertEqual(again.status, "duplicate")
        self.assertEqual(again.response["message"], "第一幕完成，进入第二幕。")

    def test_legacy_choice_row_is_treated_as_a_replay(self) -> None:
        final = self.final_state()
        # The service owns the receipt table; one real advance installs it.
        self.service.advance("warmup", "u", 1, self.initial, final, self.response())
        legacy = self.legacy_snapshot("u", 1, self.initial, final)
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "INSERT INTO past_life_choice_operations"
                "(operation_id,user_id,kind,payload,response_json) VALUES(%s,%s,%s,%s,%s)",
                ("choice-legacy", "u", "advance", legacy,
                 json.dumps({"message": "旧回执", "is_end": False}, ensure_ascii=False)),
            )
        replay = self.service.advance(
            "choice-legacy", "u", 1, self.initial, final, {"message": "新请求", "is_end": False}
        )
        self.assertEqual(replay.status, "duplicate")
        self.assertEqual(replay.response["message"], "旧回执")

    def test_different_user_on_same_operation_is_still_a_conflict(self) -> None:
        final = self.final_state()
        self.service.advance("choice-1", "u", 1, self.initial, final, self.response())
        conflict = self.service.advance(
            "choice-1", "someone-else", 1, self.initial, final, self.response()
        )
        self.assertEqual(conflict.status, "operation_conflict")


class WorldBossPayloadCompactionTests(unittest.TestCase):
    # The repository compares against ``config_snapshot`` of this raw config.
    RAW_CONFIG = {
        "Boss名字": ["神风王"],
        "Boss灵石": {"感气境": [10000000]},
        "Boss倍率": {},
    }
    BOSSES = [{
        "jj": "感气境", "name": "神风王", "气血": 720000, "总血量": 720000,
        "真元": 24000, "攻击": 1200, "max_stone": 10000000, "stone": 10000000,
    }]

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "player.db"
        with DatabaseUnitOfWork(self.db) as uow:
            apply_boss_full_refresh_player_schema(uow)
            apply_boss_punishment_operations(uow)
        self.repository = WorldBossFullRefreshSqlRepository(
            self.db, config_loader=lambda: self.RAW_CONFIG
        )
        self.config = self.repository.config_snapshot(self.RAW_CONFIG, ["感气境"])

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def refresh(self, *, operation_id="boss-1", trigger="scheduled", bosses=None):
        return self.repository.refresh(
            operation_id=operation_id,
            trigger=trigger,
            expected_revision=0,
            expected_bosses=[],
            expected_config=self.config,
            bosses=bosses or self.BOSSES,
        )

    def test_refresh_payload_is_a_digest_and_replay_returns_bosses(self) -> None:
        first = self.refresh()
        self.assertEqual(first.status, "refreshed")
        with DatabaseUnitOfWork(self.db) as uow:
            payload = uow.query_one(
                "SELECT payload FROM world_boss_full_refresh_operations WHERE operation_id='boss-1'"
            )["payload"]
        document = json.loads(payload)
        self.assertEqual(document["v"], RECEIPT_VERSION)
        self.assertEqual(document["kind"], "full_refresh")
        self.assertEqual(document["trigger"], "scheduled")
        self.assertEqual(document["expected_revision"], 0)
        self.assertEqual(document["boss_count"], 1)
        self.assertNotIn("神风王", payload)
        self.assertNotIn("气血", payload)
        self.assertLess(len(payload.encode("utf-8")), 300)

        replay = self.refresh()
        self.assertEqual(replay.status, "duplicate")
        self.assertEqual([dict(boss) for boss in replay.bosses], self.BOSSES)
        self.assertEqual(replay.revision, first.revision)

        conflict = self.refresh(trigger="manual")
        self.assertEqual(conflict.status, "operation_conflict")

    def test_legacy_refresh_row_is_treated_as_a_replay(self) -> None:
        snapshot = json.dumps(
            {
                "trigger": "scheduled",
                "expected_revision": 0,
                "expected_bosses": [],
                "expected_config": self.config,
                "bosses": self.BOSSES,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        result_json = json.dumps(
            {"revision": 1, "bosses": self.BOSSES, "trigger": "scheduled"},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.db) as uow:
            uow.execute(
                "INSERT INTO world_boss_full_refresh_operations"
                "(operation_id,payload,result_json,created_at) VALUES('boss-legacy',?,?,CURRENT_TIMESTAMP)",
                (snapshot, result_json),
            )
        replay = self.refresh(operation_id="boss-legacy")
        self.assertEqual(replay.status, "duplicate")
        self.assertEqual(replay.revision, 1)
        self.assertEqual([dict(boss) for boss in replay.bosses], self.BOSSES)


if __name__ == "__main__":
    unittest.main()
