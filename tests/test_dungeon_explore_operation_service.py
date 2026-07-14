import asyncio
import importlib
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import nonebot
from nonebot.exception import FinishedException

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.explore_operation_service import (
    DungeonExploreOperationService,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.player_fight import (
    resolve_final_user_statuses,
)
from tests.test_db_backend import db_backend

dungeon_plugin = importlib.import_module(
    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon"
)


class DungeonExploreOperationServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,hp INTEGER,"
                "mp INTEGER,stone INTEGER,exp INTEGER)"
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s,%s,%s,%s,%s)",
                ("u", 80, 30, 100, 1000),
            )
            conn.execute(
                "CREATE TABLE user_cd (user_id TEXT,type INTEGER,create_time TEXT)"
            )
            conn.execute("INSERT INTO user_cd VALUES (%s,%s,%s)", ("u", 0, "0"))
            conn.execute(
                "CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TABLE player_dungeon_status (user_id TEXT PRIMARY KEY,"
                "dungeon_id TEXT,dungeon_name TEXT,dungeon_status TEXT,current_layer INTEGER,"
                "total_layers INTEGER,last_reset_date TEXT,reset_generation INTEGER,"
                "reset_operation_id TEXT)"
            )
            conn.execute(
                "INSERT INTO player_dungeon_status VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                ("u", "d", "D", "not_started", 0, 3, "2026-07-15", 1, "reset-1"),
            )
            conn.execute(
                "CREATE TABLE teams (user_id TEXT PRIMARY KEY,leader TEXT,members TEXT,"
                "version INTEGER DEFAULT 0)"
            )
        self.service = DungeonExploreOperationService(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def plan(self, **changes):
        plan = {
            "expected_status": {
                "dungeon_id": "d",
                "dungeon_name": "D",
                "dungeon_status": "not_started",
                "current_layer": 0,
                "total_layers": 3,
                "last_reset_date": "2026-07-15",
                "reset_generation": 1,
                "reset_operation_id": "reset-1",
            },
            "team": None,
            "members": [
                {
                    "user_id": "u",
                    "expected": {
                        "hp": 80,
                        "mp": 30,
                        "stone": 100,
                        "exp": 1000,
                        "cd_type": 0,
                    },
                    "final_hp": 55,
                    "final_mp": 12,
                    "stone_delta": 7,
                    "exp_delta": 9,
                    "items": [
                        {
                            "id": 9,
                            "name": "奖品",
                            "type": "道具",
                            "amount": 1,
                            "expected_num": 0,
                            "expected_bind_num": 0,
                        }
                    ],
                }
            ],
            "advance": True,
            "complete": False,
            "resolved": {"kind": "monster", "winner": 0},
            "response": {"battle_messages": [{"data": {"content": "战报"}}], "message": "首次响应"},
        }
        plan.update(changes)
        return plan

    def team_plan(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "INSERT INTO user_xiuxian VALUES (%s,%s,%s,%s,%s)",
                ("v", 60, 20, 200, 800),
            )
            conn.execute("INSERT INTO user_cd VALUES (%s,%s,%s)", ("v", 0, "0"))
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "INSERT INTO teams VALUES (%s,%s,%s,%s)",
                ("team", "u", '["u","v"]', 1),
            )
        plan = self.plan(
            team={
                "team_id": "team",
                "leader": "u",
                "members": ["u", "v"],
                "version": 1,
            }
        )
        plan["members"].append(
            {
                "user_id": "v",
                "expected": {
                    "hp": 60,
                    "mp": 20,
                    "stone": 200,
                    "exp": 800,
                    "cd_type": 0,
                },
                "final_hp": 45,
                "final_mp": 10,
                "stone_delta": 5,
                "exp_delta": 6,
                "items": [],
            }
        )
        return plan

    def state(self):
        with db_backend.connection(self.game) as conn:
            user = tuple(
                conn.execute(
                    "SELECT hp,mp,stone,exp FROM user_xiuxian WHERE user_id='u'"
                ).fetchone()
            )
            item = conn.execute(
                "SELECT goods_num,bind_num FROM back WHERE user_id='u' AND goods_id=9"
            ).fetchone()
            operation = conn.execute(
                "SELECT phase,result_status FROM dungeon_explore_operations "
                "WHERE operation_id='op'"
            ).fetchone() if conn.table_exists("dungeon_explore_operations") else None
        with db_backend.connection(self.player) as conn:
            dungeon = tuple(
                conn.execute(
                    "SELECT dungeon_status,current_layer FROM player_dungeon_status "
                    "WHERE user_id='u'"
                ).fetchone()
            )
        return user, tuple(item) if item else None, dungeon, tuple(operation) if operation else None

    def test_preflight_rejection_is_replayed_without_session_mutation(self):
        response = {"battle_messages": [], "message": "重伤未愈"}
        first = self.service.complete_without_writes(
            "reject", "u", "member_injured", response
        )
        replay = self.service.replay("reject", "u")
        self.assertEqual((first.status, replay.status), ("applied", "duplicate"))
        self.assertEqual(replay.response, response)
        with db_backend.connection(self.player) as conn:
            status = conn.execute(
                "SELECT dungeon_status,current_layer FROM player_dungeon_status WHERE user_id='u'"
            ).fetchone()
        self.assertEqual(tuple(status), ("not_started", 0))

    def test_concurrent_rejection_resumes_prepared_winner_and_replays_its_response(self):
        plan = self.plan(response={"battle_messages": [], "message": "prepared winner"})
        self.assertEqual(self.service.prepare("op", "u", plan).phase, "prepared")

        with ThreadPoolExecutor(max_workers=2) as executor:
            rejection = executor.submit(
                self.service.resolve_rejection,
                "op",
                "u",
                "member_injured",
                {"battle_messages": [], "message": "transient rejection"},
                99,
            )
            duplicate = executor.submit(self.service.settle, "op", "u", 99)
            results = (rejection.result(), duplicate.result())

        self.assertEqual(
            [result.response for result in results],
            [plan["response"], plan["response"]],
        )
        self.assertEqual(
            self.state(),
            ((55, 12, 107, 1009), (1, 1), ("exploring", 1), ("completed", "applied")),
        )

    def test_resolved_plan_settles_resources_progress_and_response_once(self):
        self.assertEqual(self.service.prepare("op", "u", self.plan()).status, "prepared")
        first = self.service.settle("op", "u", 99)
        replay = self.service.settle("op", "u", 99)
        self.assertEqual((first.status, replay.status), ("applied", "duplicate"))
        self.assertEqual(first.response, self.plan()["response"])
        self.assertEqual(
            self.state(),
            ((55, 12, 107, 1009), (1, 1), ("exploring", 1), ("completed", "applied")),
        )

    def test_noncombat_plan_preserves_legitimate_zero_mp(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE user_xiuxian SET mp=0 WHERE user_id='u'")
        plan = self.plan(
            resolved={"kind": "nothing", "event": {"type": "nothing"}},
            response={"battle_messages": [], "message": "无事发生"},
        )
        plan["members"][0].update(
            {
                "expected": {
                    "hp": 80,
                    "mp": 0,
                    "stone": 100,
                    "exp": 1000,
                    "cd_type": 0,
                },
                "final_hp": 80,
                "final_mp": 0,
                "stone_delta": 0,
                "exp_delta": 0,
                "items": [],
            }
        )

        self.assertEqual(self.service.prepare("op", "u", plan).status, "prepared")
        result = self.service.settle("op", "u", 99)

        self.assertEqual(result.result_status, "applied")
        self.assertEqual(self.state()[0], (80, 0, 100, 1000))
        self.assertEqual(self.state()[2], ("exploring", 1))

    def test_invalid_final_hp_or_mp_is_rejected_without_business_writes(self):
        for operation_id, field, value in (
            ("invalid-hp", "final_hp", 0),
            ("invalid-mp", "final_mp", -1),
        ):
            with self.subTest(field=field, value=value):
                plan = self.plan()
                plan["members"][0][field] = value
                self.service.prepare(operation_id, "u", plan)

                result = self.service.settle(operation_id, "u", 99)

                self.assertEqual(result.status, "invalid_plan")
                self.assertEqual(self.state()[0], (80, 30, 100, 1000))
                self.assertEqual(self.state()[1], None)
                self.assertEqual(self.state()[2], ("not_started", 0))

    def test_team_plan_settles_every_member_but_only_advances_leader(self):
        plan = self.team_plan()
        self.service.prepare("op", "u", plan)

        result = self.service.settle("op", "u", 99)

        self.assertEqual(result.result_status, "applied")
        with db_backend.connection(self.game) as conn:
            users = [
                tuple(row)
                for row in conn.execute(
                    "SELECT user_id,hp,mp,stone,exp FROM user_xiuxian ORDER BY user_id"
                ).fetchall()
            ]
        with db_backend.connection(self.player) as conn:
            statuses = [
                tuple(row)
                for row in conn.execute(
                    "SELECT user_id,dungeon_status,current_layer "
                    "FROM player_dungeon_status ORDER BY user_id"
                ).fetchall()
            ]
        self.assertEqual(
            users,
            [("u", 55, 12, 107, 1009), ("v", 45, 10, 205, 806)],
        )
        self.assertEqual(statuses, [("u", "exploring", 1)])

    def test_team_member_snapshot_conflict_leaves_every_member_unchanged(self):
        plan = self.team_plan()
        self.service.prepare("op", "u", plan)
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE user_cd SET type=9 WHERE user_id='v'")

        result = self.service.settle("op", "u", 99)

        self.assertEqual(result.result_status, "state_changed")
        with db_backend.connection(self.game) as conn:
            users = [
                tuple(row)
                for row in conn.execute(
                    "SELECT user_id,hp,mp,stone,exp FROM user_xiuxian ORDER BY user_id"
                ).fetchall()
            ]
            items = conn.execute("SELECT COUNT(*) FROM back").fetchone()[0]
        self.assertEqual(
            users,
            [("u", 80, 30, 100, 1000), ("v", 60, 20, 200, 800)],
        )
        self.assertEqual(items, 0)
        self.assertEqual(self.state()[2], ("not_started", 0))

    def test_first_prepared_random_plan_wins_and_resumes(self):
        original = self.plan(response={"battle_messages": [], "message": "first"})
        changed = self.plan(response={"battle_messages": [], "message": "second"})
        self.service.prepare("op", "u", original)
        duplicate = self.service.prepare("op", "u", changed)
        self.assertEqual(duplicate.phase, "prepared")
        self.assertEqual(duplicate.plan["response"]["message"], "first")
        settled = self.service.settle("op", "u", 99)
        self.assertEqual(settled.response["message"], "first")

    def test_snapshot_conflict_completes_fixed_rejection_without_rewards(self):
        self.service.prepare("op", "u", self.plan())
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=101 WHERE user_id='u'")
        result = self.service.settle("op", "u", 99)
        replay = self.service.replay("op", "u")
        self.assertEqual(result.result_status, "state_changed")
        self.assertEqual(replay.response, result.response)
        self.assertEqual(self.state()[0], (80, 30, 101, 1000))
        self.assertEqual(self.state()[2], ("not_started", 0))

    def test_invalid_binding_inventory_is_rejected_without_business_writes(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "INSERT INTO back VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                ("u", 9, "奖品", "道具", 1, "0", "0", 2),
            )
        plan = self.plan()
        plan["members"][0]["items"][0].update(
            {"expected_num": 1, "expected_bind_num": 2}
        )
        self.service.prepare("op", "u", plan)

        result = self.service.settle("op", "u", 99)

        self.assertEqual(result.result_status, "state_changed")
        self.assertEqual(self.state()[0], (80, 30, 100, 1000))
        self.assertEqual(self.state()[1], (1, 2))
        self.assertEqual(self.state()[2], ("not_started", 0))

    def test_reset_generation_blocks_same_dungeon_layer_aba(self):
        self.service.prepare("op", "u", self.plan())
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "UPDATE player_dungeon_status SET reset_generation=2,"
                "reset_operation_id='reset-2' WHERE user_id='u'"
            )
        result = self.service.settle("op", "u", 99)
        self.assertEqual(result.result_status, "state_changed")
        self.assertEqual(self.state()[0], (80, 30, 100, 1000))
        self.assertEqual(self.state()[2], ("not_started", 0))

    def test_missing_generation_snapshot_cannot_bypass_aba_guard(self):
        plan = self.plan()
        plan["expected_status"].pop("reset_generation")
        plan["expected_status"].pop("reset_operation_id")
        self.service.prepare("op", "u", plan)

        result = self.service.settle("op", "u", 99)

        self.assertEqual(result.result_status, "state_changed")
        self.assertEqual(self.state()[0], (80, 30, 100, 1000))
        self.assertEqual(self.state()[2], ("not_started", 0))

    def test_completion_failure_rolls_back_business_but_keeps_prepared_task(self):
        self.service.prepare("op", "u", self.plan())
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER fail_complete BEFORE UPDATE OF phase ON "
                "dungeon_explore_operations WHEN NEW.phase='completed' "
                "BEGIN SELECT RAISE(ABORT,'fail'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.settle("op", "u", 99)
        self.assertEqual(
            self.state(),
            ((80, 30, 100, 1000), None, ("not_started", 0), ("prepared", "")),
        )
        with db_backend.transaction(self.game) as conn:
            conn.execute("DROP TRIGGER fail_complete")
        self.assertEqual(self.service.settle("op", "u", 99).result_status, "applied")

    def test_prepare_operation_failure_leaves_no_task(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE dungeon_explore_operations (operation_id TEXT PRIMARY KEY,"
                "request_identity TEXT,phase TEXT,prepared_json TEXT,result_status TEXT,"
                "result_json TEXT,current_layer INTEGER,dungeon_status TEXT,created_at TEXT,"
                "updated_at TEXT)"
            )
            conn.execute(
                "CREATE TRIGGER fail_prepare BEFORE INSERT ON dungeon_explore_operations "
                "BEGIN SELECT RAISE(ABORT,'fail'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.service.prepare("op", "u", self.plan())
        with db_backend.connection(self.game) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM dungeon_explore_operations"
            ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_final_status_resolution_is_pure_and_handles_string_ratio_keys(self):
        status = [
            {
                "U": {
                    "user_id": "u",
                    "hp": 40,
                    "mp": 20,
                    "hp_multiplier": 2,
                    "mp_multiplier": 2,
                }
            }
        ]
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.player_fight."
            "sql_message.update_user_hp_mp"
        ) as update_status:
            self.assertEqual(
                resolve_final_user_statuses(status, "bot", {"u": 0.5}),
                {"u": {"hp": 40, "mp": 20}},
            )
        update_status.assert_not_called()

    def test_team_reward_distribution_does_not_lose_integer_stone_to_float_error(self):
        distribution = dungeon_plugin._calc_team_distribution(
            ["leader", "member"],
            "leader",
            {"leader": 1000, "member": 1000},
        )

        self.assertEqual(int(100_000 * distribution["leader"]), 50_000)
        self.assertEqual(int(100_000 * distribution["member"]), 45_000)

    def test_real_handler_replays_before_random_and_uses_single_service(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_dungeon/__init__.py"
        ).read_text(encoding="utf-8")
        handler = source[
            source.index("async def handle_explore_dungeon") : source.index(
                "async def handle_dungeon_status"
            )
        ]
        self.assertLess(
            handler.index("dungeon_explore_operation_service.replay"),
            handler.index("dungeon_manager.trigger_event"),
        )
        self.assertIn("dungeon_explore_operation_service.prepare", handler)
        self.assertIn("dungeon_explore_operation_service.settle", handler)
        self.assertIn("dungeon_explore_operation_service.resolve_rejection", handler)
        self.assertIn("type_in=0", handler)
        self.assertNotIn("dungeon_session_service.enter", handler)
        self.assertNotIn("dungeon_battle_progress_service.settle", handler)
        self.assertNotIn("dungeon_explore_event_service.settle", handler)

    def test_completed_and_prepared_handler_replay_finish_without_false_failure(self):
        response = {"battle_messages": [], "message": "stored response"}
        bot = SimpleNamespace(self_id="bot")
        event = SimpleNamespace(message_id="same-event")

        for phase in ("completed", "prepared"):
            with self.subTest(phase=phase):
                replay = SimpleNamespace(
                    status="duplicate" if phase == "completed" else "prepared",
                    phase=phase,
                    response=response if phase == "completed" else {},
                )
                resumed = SimpleNamespace(phase="completed", response=response)
                operation_service = SimpleNamespace(
                    replay=Mock(return_value=replay),
                    settle=Mock(return_value=resumed),
                )
                sent_response = AsyncMock()
                sent_error = AsyncMock()
                with (
                    patch.object(
                        dungeon_plugin,
                        "assign_bot",
                        AsyncMock(return_value=(bot, None)),
                    ),
                    patch.object(
                        dungeon_plugin,
                        "check_user",
                        Mock(return_value=(True, {"user_id": "u"}, "")),
                    ),
                    patch.object(
                        dungeon_plugin,
                        "dungeon_explore_operation_service",
                        operation_service,
                    ),
                    patch.object(
                        dungeon_plugin, "_send_explore_response", sent_response
                    ),
                    patch.object(dungeon_plugin, "handle_send", sent_error),
                ):
                    with self.assertRaises(FinishedException):
                        asyncio.run(
                            dungeon_plugin.handle_explore_dungeon(bot, event)
                        )

                sent_response.assert_awaited_once_with(bot, event, response)
                sent_error.assert_not_awaited()
                if phase == "prepared":
                    operation_service.settle.assert_called_once()
                else:
                    operation_service.settle.assert_not_called()


if __name__ == "__main__":
    unittest.main()
