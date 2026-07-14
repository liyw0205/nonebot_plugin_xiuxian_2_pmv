from __future__ import annotations

import asyncio
import random
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import nonebot

nonebot.init()

import nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift as rift_module
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift import jsondata
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_rift.entry_service import (
    RiftEntryService,
)
from tests.test_db_backend import db_backend


class RiftEntryServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db = self.root / "game.db"
        with db_backend.transaction(self.db) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT PRIMARY KEY,user_stamina INTEGER DEFAULT 100)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian(user_id) VALUES(%s)",
                [("u",), ("v",)],
            )
            conn.execute(
                "CREATE TABLE user_cd("
                "user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
            )
            conn.executemany(
                "INSERT INTO user_cd(user_id,type) VALUES(%s,0)",
                [("u",), ("v",)],
            )
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                "bind_num INTEGER DEFAULT 0,"
                "PRIMARY KEY(user_id,goods_id))"
            )
            conn.executemany(
                "INSERT INTO back(user_id,goods_id,goods_num,bind_num) "
                "VALUES(%s,7,1,1)",
                [("u",), ("v",)],
            )
        self.service = RiftEntryService(self.db)

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def plan(name="东玄域"):
        return {
            "name": name,
            "rank": 1,
            "time": 60,
            "target_nodes": [
                {
                    "realm": "东胜神洲",
                    "heaven": "第一重天",
                    "node_id": "trial-1",
                    "node_name": "问心台",
                    "node_type": "试炼",
                }
            ],
            "target_realm": "东胜神洲",
            "target_heaven": "第一重天",
            "target_node_id": "trial-1",
            "target_node_name": "问心台",
        }

    def generate(self, operation_id="generation-1", plan=None):
        result = self.service.generate(
            operation_id,
            "global",
            plan or self.plan(),
        )
        self.assertTrue(result.succeeded)
        self.assertIsNotNone(result.state)
        return result.state

    def enter(
        self,
        operation_id,
        user_id,
        state,
        ticket_id=0,
        *,
        stamina_cost=0,
        expected_stamina=None,
    ):
        return self.service.enter(
            operation_id,
            user_id,
            "global",
            state.rift_data,
            state.rift_data["time"],
            ticket_id,
            expected_generation_id=state.generation_id,
            expected_revision=state.revision,
            stamina_cost=stamina_cost,
            expected_stamina=expected_stamina,
        )

    def test_generation_is_idempotent_and_replaces_one_snapshot(self):
        first = self.service.generate("generation-1", "global", self.plan())
        duplicate = self.service.generate("generation-1", "global", self.plan())
        conflict = self.service.generate(
            "generation-1", "global", self.plan("西玄域")
        )
        self.assertEqual("applied", first.status)
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual("state_changed", conflict.status)
        self.assertEqual(1, first.state.revision)

        entered = self.enter("entry-1", "u", first.state)
        self.assertEqual(("u",), entered.world.participants)
        second = self.service.generate(
            "generation-2", "global", self.plan("西玄域")
        )
        self.assertEqual("applied", second.status)
        self.assertEqual(3, second.state.revision)
        self.assertEqual((), second.state.participants)
        self.assertEqual("西玄域", second.state.rift_data["name"])

    def test_generation_failure_keeps_previous_world_snapshot(self):
        original = self.generate()
        with db_backend.transaction(self.db) as conn:
            conn.execute(
                "CREATE TRIGGER reject_generation_operation BEFORE INSERT "
                "ON rift_generation_operations WHEN NEW.operation_id='generation-2' "
                "BEGIN SELECT RAISE(ABORT,'reject generation'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.generate(
                "generation-2", "global", self.plan("西玄域")
            )
        self.assertEqual(original, self.service.get_current("global"))

    def test_production_generation_plan_is_operation_deterministic(self):
        def assign_fixed_random_node(rift):
            node_id = f"trial-{random.randint(1, 999999)}"
            rift.target_nodes = [
                {
                    "realm": "测试界",
                    "heaven": "测试天",
                    "node_id": node_id,
                    "node_name": "测试台",
                    "node_type": "试炼",
                }
            ]
            rift.target_node_id = node_id
            return rift

        random.seed(20260714)
        original_random_state = random.getstate()
        with patch.object(
            rift_module, "assign_rift_trial_node", assign_fixed_random_node
        ):
            first = rift_module._rift_world_snapshot(
                rift_module._build_fixed_rift("fixed-operation")
            )
            second = rift_module._rift_world_snapshot(
                rift_module._build_fixed_rift("fixed-operation")
            )
        self.assertEqual(first, second)
        self.assertEqual(original_random_state, random.getstate())

    def test_runtime_timestamp_parser_accepts_sqlite_timestamp_variants(self):
        with_microseconds = "2026-07-14 15:39:09.123456"
        without_microseconds = "2026-07-14 15:39:09"
        expected = datetime(2026, 7, 14, 15, 39, 9)

        self.assertEqual(
            expected.replace(microsecond=123456),
            rift_module._parse_rift_datetime(with_microseconds),
        )
        self.assertEqual(
            expected,
            rift_module._parse_rift_datetime(without_microseconds),
        )
        self.assertIs(
            expected,
            rift_module._parse_rift_datetime(expected),
        )

    def test_runtime_elapsed_uses_sqlite_utc_clock(self):
        now = datetime(2026, 7, 14, 16, 22, 47, tzinfo=timezone.utc)
        self.assertEqual(
            3,
            rift_module._rift_elapsed_minutes(
                "2026-07-14 16:19:47",
                now=now,
            ),
        )
        self.assertEqual(
            3,
            rift_module._rift_elapsed_minutes(
                "2026-07-14T16:19:47Z",
                now=now,
            ),
        )

    def test_runtime_event_roll_is_operation_deterministic(self):
        user = {"user_id": "u", "exp": 100, "hp": 80, "mp": 60}
        rift = {"rank": 1}
        original_random_state = random.getstate()
        try:
            with patch.object(rift_module, "get_story_type", return_value="无事"):
                first = asyncio.run(
                    rift_module._roll_rift_event(
                        user, rift, "bot", "fixed-op"
                    )
                )
                self.assertEqual(original_random_state, random.getstate())
                random.seed(999)
                ambient_state = random.getstate()
                second = asyncio.run(
                    rift_module._roll_rift_event(
                        user, rift, "bot", "fixed-op"
                    )
                )
                self.assertEqual(ambient_state, random.getstate())
            self.assertEqual(first, second)

            progress_first = rift_module._roll_rift_progress(9, "fixed-op")
            random.seed(123)
            progress_second = rift_module._roll_rift_progress(9, "fixed-op")
            self.assertEqual(progress_first, progress_second)
        finally:
            random.setstate(original_random_state)

    def test_bootstrap_imports_legacy_snapshot_only_once(self):
        legacy = self.plan()
        legacy["l_user_id"] = [12, "13", 12]
        imported = self.service.bootstrap("global", legacy)
        current = self.service.bootstrap("global", self.plan("西玄域"))
        self.assertEqual(("12", "13"), imported.participants)
        self.assertEqual(imported, current)

    def test_existing_entry_tables_are_migrated_in_place(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute(
                "CREATE TABLE rift_entries("
                "user_id TEXT PRIMARY KEY,rift_key TEXT NOT NULL,rift_data TEXT NOT NULL,"
                "status TEXT NOT NULL,duration INTEGER NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            conn.execute(
                "CREATE TABLE rift_entry_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                "entry_count INTEGER NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
        state = self.generate()
        applied = self.enter("entry-migrated", "u", state)
        self.assertEqual("applied", applied.status)
        with db_backend.connection(self.db) as conn:
            self.assertTrue(conn.column_exists("rift_entries", "generation_id"))
            self.assertTrue(
                conn.column_exists("rift_entry_operations", "generation_id")
            )
            self.assertTrue(conn.column_exists("rift_entry_operations", "rift_data"))
            self.assertTrue(
                conn.column_exists("rift_entry_operations", "global_revision")
            )

    def test_entry_atomically_updates_ticket_cooldown_count_and_world(self):
        state = self.generate()
        applied = self.enter("entry-ticket", "u", state, ticket_id=7)
        duplicate = self.enter("entry-ticket", "u", state, ticket_id=7)
        self.assertEqual("applied", applied.status)
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual(1, duplicate.entries)
        self.assertEqual(("u",), applied.world.participants)
        self.assertEqual(2, applied.world.revision)

        with db_backend.connection(self.db) as conn:
            self.assertEqual(
                (0, 0),
                tuple(
                    conn.execute(
                        "SELECT goods_num,bind_num FROM back "
                        "WHERE user_id='u' AND goods_id=7"
                    ).fetchone()
                ),
            )
            self.assertEqual(
                (3, 60),
                tuple(
                    map(
                        int,
                        conn.execute(
                            "SELECT type,scheduled_time FROM user_cd WHERE user_id='u'"
                        ).fetchone(),
                    )
                ),
            )
            entry = conn.execute(
                "SELECT generation_id,status,duration FROM rift_entries WHERE user_id='u'"
            ).fetchone()
            self.assertEqual(("generation-1", "active", 60), tuple(entry))
            self.assertEqual(
                1,
                int(
                    conn.execute(
                        "SELECT entry_count FROM rift_entry_counts WHERE user_id='u'"
                    ).fetchone()[0]
                ),
            )

    def test_concurrent_entries_merge_within_the_same_generation(self):
        original = self.generate()
        first = self.enter("entry-u", "u", original)
        concurrent = self.enter("entry-v", "v", original, ticket_id=7)
        self.assertEqual("applied", first.status)
        self.assertEqual("applied", concurrent.status)
        self.assertEqual(("u", "v"), concurrent.world.participants)
        self.assertEqual(3, concurrent.world.revision)

    def test_stamina_cost_is_atomic_and_duplicate_does_not_charge_again(self):
        state = self.generate()
        applied = self.enter(
            "entry-stamina",
            "u",
            state,
            stamina_cost=6,
            expected_stamina=100,
        )
        duplicate = self.enter(
            "entry-stamina",
            "u",
            state,
            stamina_cost=6,
            expected_stamina=94,
        )
        self.assertEqual(("applied", "duplicate"), (applied.status, duplicate.status))
        with db_backend.connection(self.db) as conn:
            self.assertEqual(
                94,
                int(
                    conn.execute(
                        "SELECT user_stamina FROM user_xiuxian WHERE user_id='u'"
                    ).fetchone()[0]
                ),
            )

    def test_stamina_snapshot_and_insufficient_stamina_are_rejected(self):
        state = self.generate()
        changed = self.enter(
            "changed-stamina",
            "u",
            state,
            stamina_cost=6,
            expected_stamina=99,
        )
        with db_backend.transaction(self.db) as conn:
            conn.execute(
                "UPDATE user_xiuxian SET user_stamina=5 WHERE user_id='u'"
            )
        missing = self.enter(
            "missing-stamina",
            "u",
            state,
            stamina_cost=6,
            expected_stamina=5,
        )
        self.assertEqual("state_changed", changed.status)
        self.assertEqual("stamina_missing", missing.status)
        self.assertEqual((), self.service.get_current("global").participants)

    def test_new_generation_and_repeat_participation_are_rejected(self):
        first = self.generate()
        self.enter("entry-u", "u", first)
        with db_backend.transaction(self.db) as conn:
            conn.execute(
                "UPDATE rift_entries SET status='settled' WHERE user_id='u'"
            )
            conn.execute("UPDATE user_cd SET type=0 WHERE user_id='u'")
        current = self.service.get_current("global")
        repeated = self.enter("entry-u-again", "u", current)
        self.assertEqual("already_joined", repeated.status)

        replaced = self.service.generate(
            "generation-2", "global", self.plan("西玄域")
        ).state
        stale = self.enter("entry-v-old", "v", current, ticket_id=7)
        self.assertEqual("rift_changed", stale.status)
        self.assertEqual("西玄域", replaced.rift_data["name"])
        with db_backend.connection(self.db) as conn:
            self.assertEqual(
                1,
                int(
                    conn.execute(
                        "SELECT goods_num FROM back WHERE user_id='v' AND goods_id=7"
                    ).fetchone()[0]
                ),
            )

    def test_busy_and_missing_ticket_leave_state_unchanged(self):
        state = self.generate()
        missing = self.service.enter(
            "missing",
            "u",
            "global",
            state.rift_data,
            60,
            8,
            expected_generation_id=state.generation_id,
            expected_revision=state.revision,
        )
        self.assertEqual("ticket_missing", missing.status)
        with db_backend.transaction(self.db) as conn:
            conn.execute("UPDATE user_cd SET type=1 WHERE user_id='u'")
        busy = self.enter("busy", "u", state)
        self.assertEqual("busy", busy.status)
        self.assertEqual((), self.service.get_current("global").participants)

    def test_failure_rolls_back_every_entry_write(self):
        state = self.generate()
        with db_backend.transaction(self.db) as conn:
            conn.execute(
                "CREATE TRIGGER reject_entry_operation BEFORE INSERT "
                "ON rift_entry_operations BEGIN SELECT RAISE(ABORT,'reject entry'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.enter(
                "failed",
                "u",
                state,
                ticket_id=7,
                stamina_cost=6,
                expected_stamina=100,
            )

        with db_backend.connection(self.db) as conn:
            self.assertEqual(
                (1, 1),
                tuple(
                    conn.execute(
                        "SELECT goods_num,bind_num FROM back "
                        "WHERE user_id='u' AND goods_id=7"
                    ).fetchone()
                ),
            )
            self.assertEqual(
                0,
                int(
                    conn.execute(
                        "SELECT type FROM user_cd WHERE user_id='u'"
                    ).fetchone()[0]
                ),
            )
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM rift_entries WHERE user_id='u'"
                ).fetchone()
            )
            self.assertEqual(
                100,
                int(
                    conn.execute(
                        "SELECT user_stamina FROM user_xiuxian WHERE user_id='u'"
                    ).fetchone()[0]
                ),
            )
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM rift_entry_counts WHERE user_id='u'"
                ).fetchone()
            )
        current = self.service.get_current("global")
        self.assertEqual((1, ()), (current.revision, current.participants))

    def test_database_entry_is_authoritative_over_legacy_player_file(self):
        state = self.generate()
        self.enter("entry-u", "u", state)
        players = self.root / "players"
        stale_file = players / "u" / "riftinfo.json"
        stale_file.parent.mkdir(parents=True)
        stale_file.write_text('{"name":"stale","time":1}', encoding="utf-8")
        with patch.object(jsondata, "_rift_entry_reader", self.service), patch.object(
            jsondata, "PLAYERSDATA", players
        ):
            loaded = jsondata.read_rift_data("u")
        self.assertEqual("东玄域", loaded["name"])
        self.assertEqual(60, loaded["time"])

    def test_progress_query_does_not_create_missing_table_or_field(self):
        player_db = self.root / "empty-player.db"
        with db_backend.transaction(player_db) as conn:
            conn.execute("CREATE TABLE unrelated(value INTEGER)")
        with patch.object(
            rift_module,
            "get_paths",
            return_value=SimpleNamespace(player_db=player_db),
        ):
            self.assertEqual(0, rift_module._rift_progress_snapshot("missing"))
        with db_backend.connection(player_db) as conn:
            self.assertFalse(conn.table_exists("rift"))
            self.assertEqual(
                ["unrelated"],
                [
                    str(row[0])
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' "
                        "ORDER BY name"
                    ).fetchall()
                ],
            )

    def test_production_entry_has_no_precommit_json_or_list_write(self):
        source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_rift"
            / "__init__.py"
        ).read_text(encoding="utf-8")
        handler = source[
            source.index("@explore_rift.handle") : source.index(
                "def _rift_progress_snapshot"
            )
        ]
        self.assertIn("rift_entry_service.enter(", handler)
        self.assertIn("expected_generation_id=", handler)
        self.assertIn("expected_revision=", handler)
        self.assertIn("stamina_cost=stamina_cost", handler)
        self.assertIn("_sync_entry_projection(user_id, entry)", handler)
        self.assertNotIn("Cooldown(stamina_cost=6)", handler)
        self.assertNotIn("l_user_id.append", handler)
        self.assertNotIn("old_rift_info.save_rift", handler)


if __name__ == "__main__":
    unittest.main()
