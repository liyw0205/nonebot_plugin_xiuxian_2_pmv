import tempfile
import sys
import unittest
from importlib import import_module
from types import ModuleType
from pathlib import Path
from unittest.mock import Mock, patch

from ....compatibility.game_event_effects import LegacyGameEventEffects
from ....infrastructure.database import DatabaseUnitOfWork, OutboxStore
from ....plugin import build_migrations, migrations_for_database
from ..migrations import apply_game_event_statistics_player
from ..statistics import GameEventStatisticsRepository


class GameEventProjectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            OutboxStore().ensure_schema(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            apply_game_event_statistics_player(uow)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_statistics_receipt_makes_replay_idempotent(self) -> None:
        repository = GameEventStatisticsRepository(self.player)
        increments = {"宠物游历领取": 1, "宠物游历次数": 1, "宠物游历时长": 8}
        self.assertTrue(repository.record(
            event_id="pet.travel.effects:op",
            user_id="u",
            increments=increments,
            occurred_at="2026-10-02T12:00:00+00:00",
        ))
        self.assertFalse(repository.record(
            event_id="pet.travel.effects:op",
            user_id="u",
            increments=increments,
            occurred_at="2026-10-02T12:00:00+00:00",
        ))
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            row = uow.query_one(
                'SELECT "宠物游历领取","宠物游历次数","宠物游历时长" FROM statistics WHERE user_id=?',
                ("u",),
            )
            self.assertEqual((1, 1, 8), tuple(row.values()))

    def test_migrations_are_routed_to_their_owned_databases(self) -> None:
        migrations = build_migrations()
        game = {item.version for item in migrations_for_database(migrations, "game_db")}
        player = {item.version for item in migrations_for_database(migrations, "player_db")}
        self.assertIn("game_events.001", player)
        self.assertNotIn("game_events.001", game)
        self.assertIn("pet.004", game)
        self.assertNotIn("pet.004", player)

    def test_dispatch_acknowledges_once_and_retries_failed_projection(self) -> None:
        outbox = OutboxStore()
        event_id = "map.mission.effects:op"
        with DatabaseUnitOfWork(self.game) as uow:
            outbox.append(
                uow,
                event_id=event_id,
                aggregate_type="player",
                aggregate_id="u",
                event_type="game_event.projection",
                payload={"user_id": "u", "event_key": "map_mission_complete", "occurred_at": "2026-10-02T12:00:00+00:00"},
            )
        effects = LegacyGameEventEffects(self.game, self.player)
        with patch.object(effects, "on_outbox_event", side_effect=[RuntimeError("projection failed"), None]) as project:
            self.assertFalse(effects.dispatch(event_id))
            self.assertTrue(effects.dispatch(event_id))
            self.assertTrue(effects.dispatch(event_id))
        self.assertEqual(2, project.call_count)
        self.assertEqual(event_id, project.call_args_list[0].args[0]["event_id"])
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            event = outbox.get(uow, event_id)
            self.assertEqual("sent", event["status"])
            self.assertEqual(1, event["attempts"])

    def test_partial_game_event_projection_replay_reuses_all_effect_ids(self) -> None:
        effects = LegacyGameEventEffects(self.game, self.player)
        record = {
            "event_id": "pet.travel.effects:projection-replay",
            "event_type": "game_event.projection",
            "payload": {
                "user_id": "u",
                "event_key": "pet_travel_claim",
                "amount": 1,
                "occurred_at": "2026-10-02T12:00:00+00:00",
                "stat_increments": {"宠物游历次数": 1, "宠物游历时长": 8},
                "meta": {
                    "source": "pet",
                    "action": "travel_claim",
                    "trace_id": "projection-replay",
                    "skip_sect_weekly": True,
                    "skip_season_rank": True,
                    "check_titles": False,
                },
            },
        }
        activity_error = RuntimeError("simulate interruption after task receipt")
        xiuxian = ModuleType("nonebot_plugin_xiuxian_2.xiuxian")
        xiuxian.__path__ = [str(Path(__file__).resolve().parents[3] / "xiuxian")]
        xiuxian_utils = ModuleType("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils")
        xiuxian_utils.__path__ = [str(Path(__file__).resolve().parents[3] / "xiuxian" / "xiuxian_utils")]
        legacy_utils = ModuleType("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils")
        legacy_utils._impersonating_users = {}
        legacy_utils.invalidate_player_data_cache = Mock()
        legacy_utils.update_statistics_value = Mock()
        economy_log = ModuleType("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.economy_log")
        economy_log.safe_log_economy_change = Mock(return_value=7)
        tasks = ModuleType("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tasks")
        tasks.__path__ = []
        task_data = ModuleType("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tasks.task_data")
        task_data.record_task_progress_event = Mock(return_value=[])
        task_data.record_task_progress_event_strict = Mock(return_value=[])
        activity = ModuleType("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity")
        activity.__path__ = []
        activity_service = ModuleType("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.service")
        activity_service.record_activity_event = Mock(side_effect=[activity_error, []])
        task_modules = {
            "nonebot_plugin_xiuxian_2.xiuxian": xiuxian,
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils": xiuxian_utils,
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils": legacy_utils,
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.economy_log": economy_log,
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tasks": tasks,
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tasks.task_data": task_data,
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity": activity,
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.service": activity_service,
        }
        game_events_name = "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.game_events"
        with patch.dict(sys.modules, task_modules):
            game_events = import_module(game_events_name)
            # The event module may already hold its import-time economy binding.
            with patch.object(game_events, "safe_log_economy_change", economy_log.safe_log_economy_change):
                with self.assertRaisesRegex(RuntimeError, "simulate interruption"):
                    effects.on_outbox_event(record)
                effects.on_outbox_event(record)

        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            row = uow.query_one(
                'SELECT "宠物游历次数","宠物游历时长" FROM statistics WHERE user_id=?',
                ("u",),
            )
            self.assertEqual((1, 8), tuple(row.values()))
        task_projection = task_data.record_task_progress_event_strict
        activity_projection = activity_service.record_activity_event
        economy_projection = economy_log.safe_log_economy_change
        self.assertEqual(2, task_projection.call_count)
        self.assertEqual(
            task_projection.call_args_list[0].kwargs["operation_id"],
            task_projection.call_args_list[1].kwargs["operation_id"],
        )
        activity_ids = [item.kwargs["event_id"] for item in activity_projection.call_args_list]
        self.assertEqual(["pet.travel.effects:projection-replay:activity:pet_travel_claim"] * 2, activity_ids)
        self.assertEqual(1, economy_projection.call_count)
        self.assertEqual("pet.travel.effects:projection-replay", economy_projection.call_args.kwargs["event_id"])


if __name__ == "__main__":
    unittest.main()
