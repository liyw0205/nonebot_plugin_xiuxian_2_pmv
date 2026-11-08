"""Startup compaction of historical receipts: bounded, resumable, non-destructive.

The audited deployment holds pre-v1.2.10/v1.2.11 rows whose snapshot columns were
never rewritten (94.2 MB of pet hatch receipts and ~140 MB of past-life /
world-boss payloads).  The startup pass rewrites them in bounded batches, keeps
operation ids and replay columns byte-identical, and leaves corrupt rows alone.

These tests use isolated databases only and cover old/new/mixed rows, an
interrupted run that resumes, a second boot that does no work, and the guarded
space reclaim (including the refusal to run without headroom).
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.base.receipt_payload import RECEIPT_VERSION
from nonebot_plugin_xiuxian_2.features.boss.migrations import apply_boss_full_refresh_player_schema
from nonebot_plugin_xiuxian_2.features.boss.world_boss_repository import (
    WorldBossFullRefreshSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.maintenance.application import compact_receipts
from nonebot_plugin_xiuxian_2.features.past_life.migrations import apply_past_life_game
from nonebot_plugin_xiuxian_2.features.pet.migrations import apply_pet_hatch
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database.maintenance import (
    database_space,
    reclaim_space,
)
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.past_life_state import (
    INTEGER_FIELDS,
    PAST_LIFE_FIELDS,
    encode_field,
    new_default_state,
)
from tests.test_db_backend import db_backend

_CATALOG_SKILL = {
    "skill_id": "skill_0021", "name": "青木回春", "type": "增益", "scope": "自身",
    "category": "木系", "base_power": 120, "star_bonus": 0.15, "effect": "heal",
    "desc": "回复自身气血", "hp_percent": 0.12, "weight": 30,
}


def _legacy_pet(index: int) -> dict:
    skill = dict(_CATALOG_SKILL)
    return {
        "uid": f"pet_1700000000_{3000 + index}", "pet_id": "pet_001", "name": "青羽雀",
        "rarity": "常见", "race": "凡兽", "type": "增益",
        "forms": ["青羽雏雀", "青羽雀", "青羽灵雀", "青羽王雀", "青羽神雀"],
        "stars": 1, "exp": 0, "total_exp": 0, "max_stars": 5, "form_index": 0,
        "form_name": "青羽雏雀", "skills": [skill], "skill": skill,
    }


def _legacy_pet_receipt(count: int = 3) -> str:
    return json.dumps(
        {
            "pets": [[_legacy_pet(index), index == 0] for index in range(count)],
            "updated_meta": ["pet_1700000000_3000", 1, 0],
            "bag_limit": 1000,
        },
        ensure_ascii=True,
        separators=(",", ":"),
    )


class CompactionFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game = root / "xiuxian.db"
        self.player = root / "player.db"
        self.state = root / "maintenance" / "receipt_compaction.json"
        with DatabaseUnitOfWork(self.game) as uow:
            apply_platform_schema(uow)
            apply_pet_hatch(uow)
            apply_past_life_game(uow)
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER DEFAULT 0)")
            uow.execute("INSERT INTO user_xiuxian(user_id) VALUES('u')")
        with DatabaseUnitOfWork(self.player) as uow:
            apply_boss_full_refresh_player_schema(uow)
            uow.execute(
                "CREATE TABLE IF NOT EXISTS world_boss_state(state_key TEXT PRIMARY KEY,"
                "bosses TEXT NOT NULL DEFAULT '[]',updated_at TEXT NOT NULL DEFAULT '',"
                "revision INTEGER NOT NULL DEFAULT 0)"
            )
            self._create_past_life(uow)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    @staticmethod
    def _create_past_life(uow: DatabaseUnitOfWork) -> None:
        columns = []
        for field in PAST_LIFE_FIELDS:
            kind = "INTEGER" if field in INTEGER_FIELDS else "TEXT"
            columns.append('"%s" %s' % (field, kind))
        uow.execute("CREATE TABLE IF NOT EXISTS past_life(user_id TEXT PRIMARY KEY,%s)"
                    % ",".join(columns))

    def seed_past_life_state(self) -> dict:
        state = new_default_state()
        state.update({
            "state": 2, "stage": 1, "revision": 6, "talent": "定数",
            "birth_scenario": "山村出生",
            "alloc": {"悟性": 4, "机缘": 4, "根骨": 4, "气运": 4, "心性": 4},
            "accumulated": {"悟性": 21, "机缘": 20, "根骨": 20, "气运": 20, "心性": 20},
            "event_indices": [0] * 10,
            "event_snapshots": [{"text": "第%d幕事件" % (i + 1)} for i in range(10)],
            "history": [{"stage": 0, "choice_text": "继续前行"}],
        })
        values = [encode_field(name, state[name]) for name in PAST_LIFE_FIELDS]
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "INSERT INTO past_life(user_id,"
                + ",".join(db_backend.quote_ident(name) for name in PAST_LIFE_FIELDS)
                + ") VALUES(" + ",".join("%s" for _ in range(len(PAST_LIFE_FIELDS) + 1)) + ")",
                ("u", *values),
            )
        return state

    def seed_legacy_rows(self) -> dict[str, str]:
        """Insert one legacy row per task; return the replay columns to compare."""

        pet_receipt = _legacy_pet_receipt()
        choice = json.dumps(
            {"user_id": "u", "choice_idx": 2, "expected": {"stage": 1, "revision": 6},
             "final": {"stage": 2, "revision": 7}, "padding": "x" * 3000},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        final_payload = json.dumps(
            {"user_id": "u", "expected": {"stage": 9}, "ending": "飞升成仙", "score": 95,
             "exp": 1200, "stone": 300, "points": 12, "completed_at": "2026-11-07 12:00:00",
             "padding": "y" * 2000},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        start_payload = json.dumps(
            {"user_id": "u", "plan": {"alloc": {}}, "message": "第一幕", "choices_count": 10,
             "refresh_slot_start": "2026-11-07 12:00:00", "padding": "z" * 2000},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        boss_payload = json.dumps(
            {"trigger": "scheduled", "expected_revision": 3, "expected_bosses": [],
             "expected_config": {"realms": ["感气境"]},
             "bosses": [{"jj": "感气境", "name": "神风王", "气血": 720000}] * 5},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        boss_result = json.dumps({"revision": 4, "trigger": "scheduled",
                                  "bosses": [{"jj": "感气境", "name": "神风王"}]},
                                 ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        choice_response = json.dumps({"message": "第一幕完成", "is_end": False}, ensure_ascii=False)
        final_result = json.dumps({"exp": 1200, "stone": 300, "points": 12,
                                   "item": None}, ensure_ascii=False)
        start_result = json.dumps({"message": "第一幕", "choices_count": 10}, ensure_ascii=False)
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "INSERT INTO pet_hatch_operations(operation_id,payload,result_json) VALUES(%s,%s,%s)",
                ("pet-1", json.dumps(["u", 1000, 3], separators=(",", ":")), pet_receipt),
            )
            conn.execute(
                "INSERT INTO past_life_choice_operations(operation_id,user_id,kind,payload,response_json)"
                " VALUES(%s,%s,%s,%s,%s)",
                ("choice-1", "u", "advance", choice, choice_response),
            )
            conn.execute(
                "INSERT INTO past_life_final_operations(operation_id,user_id,payload,result_json)"
                " VALUES(%s,%s,%s,%s)",
                ("final-1", "u", final_payload, final_result),
            )
            conn.execute(
                "INSERT INTO past_life_start_operations(operation_id,user_id,payload,result_json)"
                " VALUES(%s,%s,%s,%s)",
                ("start-1", "u", start_payload, start_result),
            )
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "INSERT INTO world_boss_full_refresh_operations(operation_id,payload,result_json,created_at)"
                " VALUES(%s,%s,%s,CURRENT_TIMESTAMP)",
                ("boss-1", boss_payload, boss_result),
            )
        return {
            "pet": pet_receipt,
            "choice_response": choice_response,
            "final_result": final_result,
            "start_result": start_result,
            "boss_result": boss_result,
        }

    def column_value(self, db: Path, table: str, column: str, operation_id: str) -> str:
        with db_backend.connection(db) as conn:
            row = conn.execute(
                'SELECT "%s" FROM "%s" WHERE operation_id=%%s' % (column, table),
                (operation_id,),
            ).fetchone()
        return str(row[0]) if row else ""

    def run_compaction(self, **kwargs):
        return compact_receipts(
            self.game, self.player, state_path=self.state, logger=None, **kwargs
        )


class HistoricalCompactionTests(CompactionFixture):
    def test_old_rows_are_converted_and_replay_is_preserved(self) -> None:
        replay = self.seed_legacy_rows()
        before_state = self.seed_past_life_state()
        report = self.run_compaction(allow_reclaim=False)

        self.assertEqual(report.converted, 5, report.summary())
        self.assertEqual(report.skipped, 0)
        for key in ("pet_hatch", "past_life_choice", "past_life_final",
                    "past_life_start", "world_boss_refresh"):
            self.assertTrue(report.tasks[key].done, key)

        # Snapshot columns are now compact documents.
        for db, table, column, op in (
            (self.game, "pet_hatch_operations", "result_json", "pet-1"),
            (self.game, "past_life_choice_operations", "payload", "choice-1"),
            (self.game, "past_life_final_operations", "payload", "final-1"),
            (self.game, "past_life_start_operations", "payload", "start-1"),
            (self.player, "world_boss_full_refresh_operations", "payload", "boss-1"),
        ):
            document = json.loads(self.column_value(db, table, column, op))
            self.assertEqual(document["v"], RECEIPT_VERSION, "%s.%s" % (table, column))

        # Replay columns are byte-identical.
        self.assertEqual(self.column_value(self.game, "past_life_choice_operations",
                                           "response_json", "choice-1"), replay["choice_response"])
        self.assertEqual(self.column_value(self.game, "past_life_final_operations",
                                           "result_json", "final-1"), replay["final_result"])
        self.assertEqual(self.column_value(self.game, "past_life_start_operations",
                                           "result_json", "start-1"), replay["start_result"])
        self.assertEqual(self.column_value(self.player, "world_boss_full_refresh_operations",
                                           "result_json", "boss-1"), replay["boss_result"])

        # Player state untouched.
        with db_backend.connection(self.player) as conn:
            row = conn.execute("SELECT stage,revision,history FROM past_life WHERE user_id=%s",
                               ("u",)).fetchone()
        self.assertEqual((int(row[0]), int(row[1])), (1, 6))
        self.assertEqual(json.loads(row[2]), before_state["history"])

        # Operation ids still replay through the real readers.
        from nonebot_plugin_xiuxian_2.features.pet.repository import PetHatchSqlRepository
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life.transaction_service import (
            PastLifeChoiceService,
        )

        prior = PetHatchSqlRepository(self.game, self.player).get_result("pet-1")
        self.assertEqual(prior.status, "duplicate")
        self.assertEqual(len(prior.pets), 3)
        self.assertEqual(prior.pets[0][0]["form_name"], "青羽雏雀")
        self.assertEqual(prior.pets[0][0]["skill"]["name"], "青木回春")
        choice = PastLifeChoiceService(self.game, self.player).get_result("choice-1", "u")
        self.assertEqual(choice.status, "duplicate")
        self.assertEqual(choice.response["message"], "第一幕完成")
        boss = WorldBossFullRefreshSqlRepository(self.player, config_loader=lambda: {}).get_result("boss-1")
        self.assertEqual(boss.status, "duplicate")
        self.assertEqual(boss.revision, 4)

    def test_second_boot_does_no_work(self) -> None:
        self.seed_legacy_rows()
        first = self.run_compaction(allow_reclaim=False)
        self.assertEqual(first.converted, 5)
        second = self.run_compaction(allow_reclaim=False)
        self.assertEqual(second.converted, 0)
        self.assertEqual(second.skipped, 0)
        self.assertTrue(second.finished)
        self.assertEqual(second.elapsed_seconds >= 0, True)

    def test_mixed_new_and_legacy_rows_skip_compact_ones(self) -> None:
        self.seed_legacy_rows()
        compact = json.dumps({"v": RECEIPT_VERSION, "kind": "advance", "digest": "d" * 64})
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "INSERT INTO past_life_choice_operations(operation_id,user_id,kind,payload,response_json)"
                " VALUES(%s,%s,%s,%s,%s)",
                ("choice-new", "u", "advance", compact, json.dumps({"message": "新"})),
            )
        report = self.run_compaction(allow_reclaim=False)
        self.assertEqual(report.tasks["past_life_choice"].converted, 1)
        self.assertEqual(report.tasks["past_life_choice"].skipped, 1)
        self.assertEqual(self.column_value(self.game, "past_life_choice_operations",
                                           "payload", "choice-new"), compact)

    def test_corrupt_rows_are_left_untouched(self) -> None:
        self.seed_legacy_rows()
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "INSERT INTO pet_hatch_operations(operation_id,payload,result_json) VALUES(%s,%s,%s)",
                ("pet-corrupt", json.dumps(["u", 1, 1]), "not-json-at-all"),
            )
            conn.execute(
                "INSERT INTO past_life_choice_operations(operation_id,user_id,kind,payload,response_json)"
                " VALUES(%s,%s,%s,%s,%s)",
                ("choice-corrupt", "u", "advance", "{broken", json.dumps({"message": "坏"})),
            )
        report = self.run_compaction(allow_reclaim=False)
        self.assertEqual(self.column_value(self.game, "pet_hatch_operations",
                                           "result_json", "pet-corrupt"), "not-json-at-all")
        self.assertEqual(self.column_value(self.game, "past_life_choice_operations",
                                           "payload", "choice-corrupt"), "{broken")
        self.assertGreaterEqual(report.skipped, 2)
        self.assertEqual(report.failed, 0)

    def test_interrupted_run_resumes_and_completes(self) -> None:
        self.seed_legacy_rows()
        # budget 0 stops before the first batch of the first unfinished task.
        partial = self.run_compaction(allow_reclaim=False, batch_size=1, budget_seconds=0.0)
        self.assertEqual(partial.converted, 0)
        self.assertFalse(partial.finished)

        resumed = self.run_compaction(allow_reclaim=False, batch_size=1)
        self.assertEqual(resumed.converted, 5)
        self.assertTrue(resumed.finished)
        again = self.run_compaction(allow_reclaim=False, batch_size=1)
        self.assertEqual(again.converted, 0)

    def test_missing_tables_mark_tasks_done(self) -> None:
        # A database without any of the receipt tables must not create them.
        empty = Path(self.tmp.name) / "empty.db"
        empty.write_bytes(b"")
        report = compact_receipts(empty, empty, state_path=self.state, allow_reclaim=False)
        self.assertTrue(report.finished)
        self.assertEqual(report.converted, 0)
        with db_backend.connection(empty) as conn:
            names = {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn("pet_hatch_operations", names)


class SpaceReclaimTests(CompactionFixture):
    def test_database_space_reports_freelist_and_unused(self) -> None:
        space = database_space(self.game)
        self.assertTrue(space["exists"])
        self.assertGreater(space["page_size"], 0)
        self.assertEqual(space["reclaimable_bytes"], space["page_size"] * space["freelist_pages"])

    def test_reclaim_refuses_without_a_backup(self) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE filler(a TEXT)")
            conn.executemany("INSERT INTO filler(a) VALUES(%s)", [("x" * 200,) for _ in range(50)])
        with db_backend.transaction(self.game) as conn:
            conn.execute("DELETE FROM filler")
        result = reclaim_space(self.game, free_bytes=10 ** 12, require_backup_dir=self.tmp.name)
        self.assertEqual(result["status"], "no_backup")

    def test_reclaim_refuses_without_headroom(self) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE filler(a TEXT)")
            conn.executemany("INSERT INTO filler(a) VALUES(%s)", [("x" * 200,) for _ in range(50)])
        with db_backend.transaction(self.game) as conn:
            conn.execute("DELETE FROM filler")
        self.assertGreater(database_space(self.game)["freelist_pages"], 0)
        result = reclaim_space(self.game, free_bytes=1024)
        self.assertEqual(result["status"], "no_headroom")
        self.assertEqual(result["before"]["file_bytes"], result["after"]["file_bytes"])

    def test_reclaim_shrinks_the_file_with_headroom(self) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE filler(a TEXT)")
            conn.executemany("INSERT INTO filler(a) VALUES(%s)", [("x" * 400,) for _ in range(400)])
        before = database_space(self.game)["file_bytes"]
        with db_backend.transaction(self.game) as conn:
            conn.execute("DELETE FROM filler")
        self.assertGreater(database_space(self.game)["freelist_pages"], 0)
        result = reclaim_space(self.game, free_bytes=10 ** 12)
        self.assertEqual(result["status"], "reclaimed")
        self.assertLess(result["after"]["file_bytes"], before)
        self.assertGreater(result["reclaimed_bytes"], 0)
        # Nothing left to reclaim afterwards.
        self.assertEqual(reclaim_space(self.game, free_bytes=10 ** 12)["status"], "no_free_pages")

    def test_compaction_without_conversion_never_reclaims(self) -> None:
        report = self.run_compaction(allow_reclaim=True)
        self.assertEqual(report.converted, 0)
        self.assertEqual(report.reclaim, {})

    def test_one_time_reclaim_runs_once_and_is_recorded(self) -> None:
        self.seed_legacy_rows()
        backup = Path(self.tmp.name) / "backups" / "db_backup"
        backup.mkdir(parents=True)
        (backup / "db_backup_20260101_000000.zip").write_bytes(b"PK\x03\x04placeholder")
        first = self.run_compaction(allow_reclaim=True, free_bytes=10 ** 12,
                                    backup_dir=backup)
        self.assertEqual(first.converted, 5)
        self.assertTrue(first.reclaim, "compaction should attempt the guarded reclaim")
        self.assertIn(first.reclaim["game"]["status"], {"reclaimed", "no_free_pages"})
        state = json.loads(self.state.read_text(encoding="utf-8"))
        self.assertTrue(state["vacuum_done"])
        second = self.run_compaction(allow_reclaim=True, free_bytes=10 ** 12, backup_dir=backup)
        self.assertEqual(second.reclaim, {})

    def test_reclaim_stays_pending_without_a_backup(self) -> None:
        self.seed_legacy_rows()
        # Force real freelist pages so the reclaim has something to do.
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE filler(a TEXT)")
            conn.executemany("INSERT INTO filler(a) VALUES(%s)", [("x" * 400,) for _ in range(400)])
        with db_backend.transaction(self.game) as conn:
            conn.execute("DELETE FROM filler")
        report = self.run_compaction(allow_reclaim=True, free_bytes=10 ** 12,
                                     backup_dir=Path(self.tmp.name) / "no-backups")
        self.assertEqual(report.converted, 5)
        self.assertEqual(report.reclaim["game"]["status"], "no_backup")
        state = json.loads(self.state.read_text(encoding="utf-8"))
        self.assertFalse(state["vacuum_done"], "a skipped reclaim must retry on a later boot")


if __name__ == "__main__":
    unittest.main()
