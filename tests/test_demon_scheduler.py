from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import tempfile
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import nonebot
import pytest
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.asyncio import AsyncIOScheduler

nonebot.init()
try:
    nonebot.load_plugin("nonebot_plugin_apscheduler")
except RuntimeError:
    # The plugin may already be loaded when the complete test suite imports it.
    pass

from nonebot.exception import FinishedException

from nonebot_plugin_xiuxian_2.compatibility.legacy_jobs import _TARGETS
from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
from nonebot_plugin_xiuxian_2.features.world_events.application import DemonEventLifecycleApplication
from nonebot_plugin_xiuxian_2.features.world_events.attack_application import DemonAttackApplication
from nonebot_plugin_xiuxian_2.features.world_events.event_state import read_event_state
from nonebot_plugin_xiuxian_2.features.world_events.migrations import (
    apply_world_events_lifecycle,
    apply_world_events_player,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import shutdown, startup
from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_world_events as world_events


EVENT_TZ = ZoneInfo("Asia/Shanghai")


def _utc(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 11, 17, hour, minute, tzinfo=timezone.utc)


def _active_state(period: str = "2026-11-17") -> dict:
    return {
        "active": 1,
        "status": "active",
        "event_id": "demon_invasion:2026-11-17",
        "event_type": "demon_invasion",
        "name": "魔修入侵",
        "period": period,
        "manual": 0,
        "bosses": {
            "练气境": {
                "wave": 1,
                "boss_hp": 100,
                "battle_max_hp": 100,
                "battle_hp": 100,
            }
        },
        "participants": {},
        "claimed": {},
        "started_at": "2026-11-17 18:00:00",
        "ends_at": "2026-11-17 22:00:00",
        "last_result": "",
    }


def test_demon_jobs_are_declared_and_registered_on_live_scheduler() -> None:
    assert _TARGETS["demon_invasion_schedule"] == (
        "xiuxian.xiuxian_world_events",
        "demon_invasion_schedule_job",
    )
    assert _TARGETS["demon_invasion_refresh_schedule"] == (
        "xiuxian.xiuxian_world_events",
        "demon_invasion_refresh_schedule_job",
    )

    bridge = world_events.scheduler
    if hasattr(bridge, "activate"):
        bridge.activate()
    real_scheduler = getattr(bridge, "_scheduler", bridge)
    assert real_scheduler.get_job("demon_invasion_schedule") is not None
    assert real_scheduler.get_job("demon_invasion_refresh_schedule") is not None
    lifecycle_trigger = real_scheduler.get_job("demon_invasion_schedule").trigger
    assert lifecycle_trigger.get_next_fire_time(None, _utc(9, 59)) == _utc(10)
    assert lifecycle_trigger.get_next_fire_time(_utc(10), _utc(10)) == _utc(14)
    refresh_trigger = real_scheduler.get_job("demon_invasion_refresh_schedule").trigger
    assert refresh_trigger.get_next_fire_time(None, _utc(13, 31)) == datetime(
        2026, 11, 18, 10, 30, tzinfo=timezone.utc
    )
    assert _TARGETS["spirit_vein_schedule"] == (
        "xiuxian.xiuxian_world_events",
        "spirit_vein_schedule_job",
    )
    assert real_scheduler.get_job("spirit_vein_schedule") is not None


def test_runtime_startup_activates_world_event_jobs() -> None:
    async def run() -> None:
        with tempfile.TemporaryDirectory(prefix="world-event-startup-") as directory:
            state, _readiness, context, lifecycle = await startup(
                build_runtime_context(data_dir=directory, legacy_startup=False)
            )
            try:
                assert state.phase.value in {"ready", "not_ready"}
                assert {job.id for job in context.jobs.list()} >= {
                    "demon_invasion_schedule",
                    "demon_invasion_refresh_schedule",
                    "spirit_vein_schedule",
                }
                real_scheduler = getattr(world_events.scheduler, "_scheduler", world_events.scheduler)
                assert real_scheduler.get_job("demon_invasion_schedule") is not None
                assert real_scheduler.get_job("demon_invasion_refresh_schedule") is not None
                assert real_scheduler.get_job("spirit_vein_schedule") is not None
            finally:
                await shutdown(lifecycle)

    asyncio.run(run())


def test_demon_schedule_drives_isolated_sqlite_state_and_replays(tmp_path) -> None:
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        apply_world_events_player(uow)
        apply_world_events_lifecycle(uow)

    class SqlStateManager:
        def get_fields(self, event_key, table):
            assert table == world_events.EVENT_TABLE
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                return read_event_state(uow, event_key) or {}

    scheduler = SimpleNamespace(timezone=EVENT_TZ)
    application = DemonEventLifecycleApplication(database)
    with patch.object(world_events, "scheduler", scheduler), patch.object(
        world_events, "demon_event_lifecycle_application", application
    ), patch.object(world_events, "_player_data_manager", return_value=SqlStateManager()), patch.object(
        world_events, "_create_all_bosses", return_value={"练气境": {"wave": 1, "boss_hp": 100}}
    ), patch.object(world_events, "_now", return_value=_utc(10)):
        asyncio.run(world_events.demon_invasion_schedule_job())
        asyncio.run(world_events.demon_invasion_schedule_job())

    with DatabaseUnitOfWork(database, read_only=True) as uow:
        state = read_event_state(uow, world_events.EVENT_KEY)
        operation_count = uow.query_one(
            "SELECT COUNT(*) AS n FROM demon_event_lifecycle_operations"
        )["n"]
    assert state["status"] == "active"
    assert state["event_id"] == "demon_invasion:2026-11-17"
    assert state["started_at"] == "2026-11-17 18:00:00"
    assert state["ends_at"] == "2026-11-17 22:00:00"
    assert operation_count == 1

    with patch.object(world_events, "scheduler", scheduler), patch.object(
        world_events, "demon_event_lifecycle_application", application
    ), patch.object(world_events, "_player_data_manager", return_value=SqlStateManager()), patch.object(
        world_events, "_now", return_value=_utc(14)
    ):
        asyncio.run(world_events.demon_invasion_schedule_job())

    with DatabaseUnitOfWork(database, read_only=True) as uow:
        state = read_event_state(uow, world_events.EVENT_KEY)
        operation_count = uow.query_one(
            "SELECT COUNT(*) AS n FROM demon_event_lifecycle_operations"
        )["n"]
    assert state["status"] == "finished"
    assert state["active"] == 0
    assert operation_count == 2


@pytest.mark.parametrize("schema_ready", (True, False))
def test_scheduler_drives_persistence_attack_and_close_or_reports_start_failure(tmp_path, schema_ready):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        apply_world_events_player(uow)
        if schema_ready:
            apply_world_events_lifecycle(uow)

    class SqlStateManager:
        def get_fields(self, event_key, table):
            assert table == world_events.EVENT_TABLE
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                return read_event_state(uow, event_key) or {}

    async def run():
        scheduler = AsyncIOScheduler(timezone=EVENT_TZ)
        events = asyncio.Queue()
        scheduler.add_listener(events.put_nowait, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        clock = SimpleNamespace(moment=_utc(10))
        boss = {
            "wave": 1, "boss_hp": 1_000_000, "boss_max_hp": 1_000_000,
            "battle_hp": 100, "battle_max_hp": 100, "reward_multiplier": 1.0,
        }
        fight = AsyncMock(return_value=(
            [], "群友赢了", {}, [{"player": {"team_id": 0, "user_id": "u", "total_dmg": 1}}]
        ))
        sender = AsyncMock()
        bot = SimpleNamespace(self_id="test-bot")
        event = SimpleNamespace(message_id="attack-1")

        async def fire():
            scheduler.add_job(world_events.demon_invasion_schedule_job, "date", id="demon_invasion_schedule")
            if not scheduler.running:
                scheduler.start()
            return await asyncio.wait_for(events.get(), timeout=3)

        with patch.object(world_events, "scheduler", scheduler), patch.object(
            world_events, "_now", side_effect=lambda: clock.moment
        ), patch.object(world_events, "_player_data_manager", return_value=SqlStateManager()), patch.object(
            world_events, "demon_event_lifecycle_application", DemonEventLifecycleApplication(database)
        ), patch.object(world_events, "demon_attack_application", DemonAttackApplication(database)), patch.object(
            world_events, "_create_all_bosses", return_value={"练气境": boss}
        ), patch.object(world_events, "assign_bot", new=AsyncMock(return_value=(bot, None))), patch.object(
            world_events, "check_user", return_value=(True, {"user_id": "u", "hp": 100,
                                                           "exp": 100, "level": "练气境"}, "")
        ), patch.object(world_events, "check_user_type", return_value=(True, "")), patch.object(
            world_events, "update_last_check_info_time"
        ), patch.object(world_events, "Boss_fight", fight), patch.object(
            world_events, "send_msg_handler", new=AsyncMock()
        ), patch.object(world_events, "handle_send", sender), patch.object(
            world_events, "log_message"
        ), patch.object(world_events, "logger") as logger:
            try:
                started = await fire()
                assert started.job_id == "demon_invasion_schedule"
                if not schema_ready:
                    assert started.code == EVENT_JOB_ERROR
                    assert "schema_missing" in str(started.exception)
                    with DatabaseUnitOfWork(database, read_only=True) as uow:
                        assert read_event_state(uow, world_events.EVENT_KEY) is None
                    assert not any("已自动开启" in str(call) for call in logger.info.call_args_list)
                    fight.assert_not_awaited()
                    return

                assert started.code == EVENT_JOB_EXECUTED
                assert (await fire()).code == EVENT_JOB_EXECUTED
                with DatabaseUnitOfWork(database, read_only=True) as uow:
                    state = read_event_state(uow, world_events.EVENT_KEY)
                    assert state["status"] == "active"
                    assert state["started_at"] == "2026-11-17 18:00:00"
                    assert uow.query_one("SELECT COUNT(*) AS n FROM demon_event_lifecycle_operations")["n"] == 1

                clock.moment = _utc(11)
                with pytest.raises(FinishedException):
                    await world_events.attack_demon_invasion_(bot, event)
                reply = sender.call_args.args[2]
                assert "本次贡献：1万" in reply
                assert "没有正在进行" not in reply
                with pytest.raises(FinishedException):
                    await world_events.attack_demon_invasion_(bot, event)
                assert "讨伐已结算（重放）" in sender.call_args.args[2]
                fight.assert_awaited_once()

                clock.moment = _utc(14)
                assert (await fire()).code == EVENT_JOB_EXECUTED
                event.message_id = "attack-after-close"
                with pytest.raises(FinishedException):
                    await world_events.attack_demon_invasion_(bot, event)
                assert "没有正在进行" in sender.call_args.args[2]
                fight.assert_awaited_once()
                with DatabaseUnitOfWork(database, read_only=True) as uow:
                    state = read_event_state(uow, world_events.EVENT_KEY)
                    assert state["status"] == "finished"
                    assert state["bosses"]["练气境"]["boss_hp"] == 990_000
                    assert state["participants"]["练气境:1:u"]["attacks"] == 1
                    assert uow.query_one("SELECT COUNT(*) AS n FROM demon_event_lifecycle_operations")["n"] == 2
                    assert uow.query_one("SELECT COUNT(*) AS n FROM demon_attack_settlement_operations")["n"] == 1
                    assert uow.query_one(
                        'SELECT "魔修入侵参与" AS attacks,"魔修入侵伤害" AS damage FROM statistics WHERE user_id=?',
                        ("u",),
                    ) == {"attacks": 1, "damage": 10_000}
                    assert uow.query_all("PRAGMA integrity_check") == [{"integrity_check": "ok"}]
            finally:
                if scheduler.running:
                    scheduler.shutdown(wait=False)

    asyncio.run(run())


def test_spirit_vein_trigger_slot_uses_scheduler_business_date() -> None:
    scheduler = SimpleNamespace(timezone=EVENT_TZ)
    state = {"status": "finished", "event_id": "spirit_vein:old"}
    with patch.object(world_events, "scheduler", scheduler), patch.object(
        world_events, "_now", return_value=_utc(16, 30)
    ), patch.object(world_events, "_load_state", return_value=state), patch.object(
        world_events, "spirit_vein_lifecycle_application"
    ) as lifecycle, patch.object(world_events, "runtime_random") as rng:
        lifecycle.replay.return_value = None
        lifecycle.transition.return_value = SimpleNamespace(
            status="not_triggered", action="auto_miss", state=state
        )
        rng.random.return_value = 1.0
        world_events._try_start_auto_spirit_vein()

    operation_id = lifecycle.transition.call_args.args[0]
    assert operation_id == "spirit-vein:auto-trigger:202611180030"


@pytest.mark.parametrize(
    ("now", "expected"),
    ((_utc(9, 59), False), (_utc(10), True), (_utc(13, 59), True), (_utc(14), False)),
)
def test_demon_window_uses_scheduler_timezone_and_half_open_end(
    now: datetime, expected: bool
) -> None:
    scheduler = SimpleNamespace(timezone=EVENT_TZ)
    with patch.object(world_events, "scheduler", scheduler):
        assert world_events._is_auto_window(now) is expected


def test_demon_period_and_persisted_window_follow_local_business_date() -> None:
    scheduler = SimpleNamespace(timezone=EVENT_TZ)
    with patch.object(world_events, "scheduler", scheduler), patch.object(
        world_events, "_now", return_value=_utc(16)
    ), patch.object(world_events, "_create_all_bosses", return_value={}):
        state = world_events._build_active_state()

    assert state["period"] == "2026-11-18"
    assert state["event_id"] == "demon_invasion:2026-11-18"
    assert state["started_at"] == "2026-11-18 18:00:00"
    assert state["ends_at"] == "2026-11-18 22:00:00"


def test_demon_schedule_job_triggers_start_and_finish_at_local_boundaries() -> None:
    scheduler = SimpleNamespace(timezone=EVENT_TZ)
    with patch.object(world_events, "scheduler", scheduler), patch.object(
        world_events, "_now", side_effect=[_utc(10), _utc(14), _utc(9, 59)]
    ), patch.object(world_events, "_start_auto_demon_invasion", return_value={"bosses": {}}) as start, patch.object(
        world_events, "_finish_auto_demon_invasion", return_value=({}, True)
    ) as finish, patch.object(world_events, "logger"):
        asyncio.run(world_events.demon_invasion_schedule_job())
        asyncio.run(world_events.demon_invasion_schedule_job())
        asyncio.run(world_events.demon_invasion_schedule_job())

    start.assert_called_once_with()
    finish.assert_called_once_with()


def test_demon_state_persistence_is_not_auto_finished_inside_local_window() -> None:
    state = _active_state()
    scheduler = SimpleNamespace(timezone=EVENT_TZ)
    with patch.object(world_events, "scheduler", scheduler), patch.object(
        world_events, "_now", return_value=_utc(10, 30)
    ), patch.object(world_events, "_load_state", return_value=dict(state)), patch.object(
        world_events, "demon_event_lifecycle_application"
    ) as lifecycle:
        actual = world_events._ensure_daily_state()

    assert actual["status"] == "active"
    lifecycle.transition.assert_not_called()


def test_attack_handler_reads_active_persisted_state_inside_effective_window() -> None:
    state = _active_state()
    bot = SimpleNamespace(self_id="bot-1")
    event = SimpleNamespace(message_id="message-1", id="event-1")
    replay = SimpleNamespace(status="applied", real_damage=1)
    sender = AsyncMock()

    async def assign_bot(*, bot, event):
        return bot, None

    with patch.object(world_events, "scheduler", SimpleNamespace(timezone=EVENT_TZ)), patch.object(
        world_events, "_now", return_value=_utc(11)
    ), patch.object(world_events, "_load_state", return_value=dict(state)), patch.object(
        world_events, "assign_bot", side_effect=assign_bot
    ), patch.object(
        world_events,
        "check_user",
        return_value=(True, {"user_id": "u", "hp": 100, "exp": 100, "level": "练气境"}, ""),
    ), patch.object(world_events, "check_user_type", return_value=(True, "")), patch.object(
        world_events, "update_last_check_info_time"
    ), patch.object(world_events.demon_attack_application, "get_result", return_value=replay), patch.object(
        world_events, "handle_send", sender
    ):
        with pytest.raises(FinishedException):
            asyncio.run(world_events.attack_demon_invasion_(bot, event))

    message = sender.call_args.args[2]
    assert "讨伐已结算" in message
    assert "没有正在进行" not in message


def test_schedule_failure_is_propagated_instead_of_logged_as_success() -> None:
    scheduler = SimpleNamespace(timezone=EVENT_TZ)
    with patch.object(world_events, "scheduler", scheduler), patch.object(
        world_events, "_now", return_value=_utc(10)
    ), patch.object(
        world_events, "_start_auto_demon_invasion", side_effect=RuntimeError("db unavailable")
    ):
        with pytest.raises(RuntimeError, match="db unavailable"):
            asyncio.run(world_events.demon_invasion_schedule_job())


def test_demon_start_rejects_persisted_transition_failure() -> None:
    failed = SimpleNamespace(status="schema_missing", state=None)
    with patch.object(world_events, "_current_period", return_value="2026-11-17"), patch.object(
        world_events, "_load_state", return_value={"status": "idle"}
    ), patch.object(world_events.demon_event_lifecycle_application, "replay", return_value=None), patch.object(
        world_events.demon_event_lifecycle_application, "transition", return_value=failed
    ):
        with pytest.raises(RuntimeError, match="schema_missing"):
            world_events._start_auto_demon_invasion()


def test_demon_finish_and_wave_refresh_reject_persisted_transition_failures() -> None:
    active = _active_state()
    failed = SimpleNamespace(status="state_changed", state=active, refreshed_realms=())
    with patch.object(world_events, "_load_state", return_value=active), patch.object(
        world_events.demon_event_lifecycle_application, "replay", return_value=None
    ), patch.object(world_events.demon_event_lifecycle_application, "transition", return_value=failed):
        with pytest.raises(RuntimeError, match="state_changed"):
            world_events._finish_auto_demon_invasion()

    defeated = _active_state()
    defeated["bosses"]["练气境"]["boss_hp"] = 0
    with patch.object(world_events, "_ensure_daily_state", return_value=defeated), patch.object(
        world_events, "_event_now", return_value=_utc(11, 30)
    ), patch.object(world_events, "_create_demon_boss", return_value={"wave": 2, "boss_hp": 100}), patch.object(
        world_events.demon_wave_refresh_application, "replay", return_value=None
    ), patch.object(world_events.demon_wave_refresh_application, "refresh", return_value=failed):
        with pytest.raises(RuntimeError, match="state_changed"):
            world_events._refresh_defeated_demon_bosses()


def test_spirit_vein_scheduler_rejects_persisted_transition_failure() -> None:
    failed = SimpleNamespace(status="schema_missing", state={"status": "finished"})
    with patch.object(world_events, "_now", return_value=_utc(10, 30)), patch.object(
        world_events, "_ensure_spirit_vein_state", return_value={"status": "finished"}
    ), patch.object(world_events.spirit_vein_lifecycle_application, "replay", return_value=None), patch.object(
        world_events.spirit_vein_lifecycle_application, "transition", return_value=failed
    ), patch.object(world_events.runtime_random, "random", return_value=0.0):
        with pytest.raises(RuntimeError, match="schema_missing"):
            world_events._try_start_auto_spirit_vein()
