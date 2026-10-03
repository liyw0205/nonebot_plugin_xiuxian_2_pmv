from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from importlib import import_module
from threading import RLock
from types import SimpleNamespace
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.features.dungeon.application import DungeonApplication
from nonebot_plugin_xiuxian_2.features.dungeon.migrations import apply_dungeon_explore_player_schema
from nonebot_plugin_xiuxian_2.features.dungeon.reset_repository import DungeonResetSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as dungeon
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.dungeon_manager import DungeonManager, DungeonTemplate

manager_module = import_module("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dungeon.dungeon_manager")


class Clock:
    def __init__(self, *values):
        self.values = iter(values)
        self.value = values[0]
        self.calls = 0

    def now(self):
        self.calls += 1
        self.value = next(self.values, self.value)
        return self.value


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "player.db"
    with DatabaseUnitOfWork(path) as uow:
        apply_dungeon_explore_player_schema(uow)
    return path


def manager(database, clock):
    instance = object.__new__(DungeonManager)
    instance._lock = RLock()
    instance.clock = clock
    instance.business_timezone = ZoneInfo("Asia/Shanghai")
    instance.ids = SimpleNamespace(new_id=lambda: "generated")
    template = DungeonTemplate({"id": "d1", "name": "Trial", "total_layers": 3})
    instance.random = SimpleNamespace(choice=Mock(return_value=template))
    instance.dungeon_templates = [template]
    instance.current_dungeon = None
    instance.dungeon_application = DungeonApplication(database.parent / "missing-game.db", database, clock=clock)
    return instance


@pytest.mark.parametrize(
    ("instant", "zone", "expected"),
    [
        ("2026-07-13T15:59:59+00:00", "Asia/Shanghai", "2026-07-13"),
        ("2026-07-13T16:01:00+00:00", "Asia/Shanghai", "2026-07-14"),
        ("2026-07-14T00:01:00+00:00", "America/Los_Angeles", "2026-07-13"),
        ("2026-11-01T05:30:00+00:00", "America/New_York", "2026-11-01"),
        ("2026-11-01T06:30:00+00:00", "America/New_York", "2026-11-01"),
    ],
)
def test_manager_business_date_uses_injected_timezone(instant, zone, expected):
    instance = object.__new__(DungeonManager)
    instance.clock = Clock(datetime.fromisoformat(instant))
    instance.business_timezone = ZoneInfo(zone)
    assert instance._get_current_date() == expected


def test_runtime_lazy_manager_injects_clock_and_scheduler_timezone(monkeypatch):
    clock = Clock(datetime(2026, 7, 13, 16, 1, tzinfo=timezone.utc))
    zone = ZoneInfo("Asia/Shanghai")
    constructor = Mock()
    monkeypatch.setattr(dungeon, "_dungeon_manager_instance", None)
    monkeypatch.setattr(dungeon, "runtime_clock", clock)
    monkeypatch.setattr(dungeon, "scheduler", SimpleNamespace(timezone=zone))
    monkeypatch.setattr(dungeon, "DungeonManager", constructor)
    assert dungeon._dungeon_manager() is constructor.return_value
    assert dungeon._dungeon_manager() is constructor.return_value
    constructor.assert_called_once_with(clock=clock, business_timezone=zone)


def test_manager_constructor_forwards_clock_to_application(tmp_path, monkeypatch):
    clock = Clock(datetime(2026, 7, 13, 16, 1, tzinfo=timezone.utc))
    monkeypatch.setattr(DungeonManager, "_has_init", False)
    monkeypatch.setattr(DungeonManager, "_load_dungeon_templates", lambda self: [])
    monkeypatch.setattr(DungeonManager, "_load_or_init_today_dungeon", lambda self: None)
    monkeypatch.setattr(manager_module, "get_paths", lambda: SimpleNamespace(
        data=tmp_path, game_db=tmp_path / "game.db", player_db=tmp_path / "player.db",
    ))
    instance = object.__new__(DungeonManager)
    DungeonManager.__init__(instance, clock=clock, business_timezone=ZoneInfo("Asia/Shanghai"))
    assert instance.dungeon_application.clock is clock
    assert instance.dungeon_application.ledger.clock is clock
    assert not (tmp_path / "player.db").exists()


def test_application_reset_receipt_uses_injected_clock(database):
    instant = datetime(2026, 7, 13, 16, 1, tzinfo=timezone.utc)
    instance = manager(database, Clock(instant))
    result = instance.reset_dungeon(source="daily")
    assert result.business_date == "2026-07-14"
    assert result.operation_id == "dungeon-reset:auto:2026-07-14"
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        row = uow.query_one("SELECT created_at,updated_at FROM dungeon_reset_operations")
    assert row == {"created_at": instant.isoformat(), "updated_at": instant.isoformat()}


def test_daily_crossday_and_utc_midnight_share_one_business_day(database):
    clock = Clock(datetime(2026, 7, 13, 16, 1, tzinfo=timezone.utc))
    instance = manager(database, clock)
    first = instance.reset_dungeon(source="daily")
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(
            "INSERT INTO player_dungeon_status(user_id,dungeon_id,dungeon_status,current_layer,last_reset_date,reset_generation,reset_operation_id) "
            "VALUES('u','d1','exploring',2,'2026-07-14',1,?)", (first.operation_id,),
        )
    clock.value = datetime(2026, 7, 14, 0, 1, tzinfo=timezone.utc)
    duplicate = instance.reset_dungeon(source="crossday")
    assert duplicate.status == "duplicate"
    assert duplicate.operation_id == first.operation_id
    instance.random.choice.assert_called_once()
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT current_layer FROM player_dungeon_status WHERE user_id='u'")["current_layer"] == 2
        assert uow.query_one("SELECT COUNT(*) AS n FROM dungeon_reset_operations")["n"] == 1


def test_real_daily_scheduler_uses_shared_business_day(database, monkeypatch):
    instance = manager(database, Clock(datetime(2026, 7, 13, 16, 1, tzinfo=timezone.utc)))
    monkeypatch.setattr(dungeon, "dungeon_manager", instance)
    asyncio.run(dungeon.daily_dungeon_reset())
    state = instance.dungeon_application.global_state()
    assert state["date"] == "2026-07-14"
    assert state["reset_operation_id"] == "dungeon-reset:auto:2026-07-14"
    instance.sync_current_dungeon()
    instance.random.choice.assert_called_once()


def test_crossday_freezes_id_and_payload_date_across_midnight(database):
    clock = Clock(
        datetime(2026, 7, 13, 15, 59, 59, tzinfo=timezone.utc),
        datetime(2026, 7, 13, 16, 0, 1, tzinfo=timezone.utc),
    )
    instance = manager(database, clock)
    instance._load_or_init_today_dungeon()
    state = instance.dungeon_application.global_state()
    assert state["date"] == "2026-07-13"
    assert state["reset_operation_id"] == "dungeon-reset:auto:2026-07-13"
    instance.sync_current_dungeon()
    state = instance.dungeon_application.global_state()
    assert state["date"] == "2026-07-14"
    assert state["reset_operation_id"] == "dungeon-reset:auto:2026-07-14"


def test_progress_uses_publication_date_not_second_clock_read(database):
    instance = manager(database, Clock(datetime(2026, 7, 13, 15, 59, 59, tzinfo=timezone.utc)))
    published = instance.reset_dungeon(source="daily")
    clock = Clock(
        datetime(2026, 7, 13, 15, 59, 59, tzinfo=timezone.utc),
        datetime(2026, 7, 13, 16, 0, 1, tzinfo=timezone.utc),
    )
    instance.clock = instance.dungeon_application.clock = clock
    progress = instance.get_dungeon_progress()
    assert progress["date"] == published.business_date == "2026-07-13"
    assert progress["reset_generation"] == published.generation
    assert progress["reset_operation_id"] == published.operation_id
    assert clock.calls == 1


def test_progress_template_and_generation_come_from_same_publication(database, monkeypatch):
    instance = manager(database, Clock(datetime(2026, 7, 13, 16, 1, tzinfo=timezone.utc)))
    instance.reset_dungeon("original", source="manual")
    published = instance.dungeon_application.reset(
        "other-worker", "2026-07-14", "manual",
        lambda: {"dungeon_id": "d2", "dungeon_name": "Other", "total_layers": 6},
    )
    monkeypatch.setattr(instance, "sync_current_dungeon", lambda: None)
    progress = instance.get_dungeon_progress()
    assert progress["dungeon_id"] == "d2"
    assert progress["name"] == "Other"
    assert progress["date"] == published.business_date
    assert progress["reset_generation"] == published.generation
    assert progress["reset_operation_id"] == published.operation_id


@pytest.mark.parametrize("state", [None, {"date": "2026-07-13"}])
def test_unpublished_progress_never_invents_today(state):
    instance = object.__new__(DungeonManager)
    instance._lock = RLock()
    instance.current_dungeon = None
    instance.sync_current_dungeon = lambda: None
    instance._get_global_state = lambda: state
    instance.clock = SimpleNamespace(now=Mock(side_effect=AssertionError("no synthetic date")))
    progress = instance.get_dungeon_progress()
    assert progress["total_layers"] == 0
    assert "dungeon_id" not in progress
    assert progress["date"] == (state or {}).get("date", "")
    instance.clock.now.assert_not_called()


def test_manual_crossday_replay_preserves_current_publication(database):
    clock = Clock(datetime(2026, 7, 13, 16, 1, tzinfo=timezone.utc))
    instance = manager(database, clock)
    original = instance.reset_dungeon("message-1", source="manual")
    clock.value = datetime(2026, 7, 14, 16, 1, tzinfo=timezone.utc)
    current = DungeonTemplate({"id": "d2", "name": "Current", "total_layers": 4})
    instance.dungeon_templates = [current]
    instance.random.choice.return_value = current
    instance.reset_dungeon("message-2", source="manual")
    instance.random.choice.reset_mock()
    instance.random.choice.side_effect = AssertionError("replay must not reroll")
    before = instance.dungeon_application.global_state()
    replay = instance.reset_dungeon(" message-1 ", source=" MANUAL ")
    assert replay.status == "duplicate"
    assert replay.business_date == original.business_date == "2026-07-14"
    assert instance.dungeon_application.global_state() == before
    assert instance.current_dungeon.id == "d2"
    instance.random.choice.assert_not_called()
    with pytest.raises(RuntimeError, match="operation_conflict"):
        instance.reset_dungeon("message-1", source="manual", business_date="2026-07-15")
    with pytest.raises(RuntimeError, match="operation_conflict"):
        instance.reset_dungeon("message-1", source="daily")
    assert instance.dungeon_application.global_state() == before


def test_manual_replay_needs_no_live_templates(database):
    clock = Clock(datetime(2026, 7, 13, 16, 1, tzinfo=timezone.utc))
    instance = manager(database, clock)
    instance.reset_dungeon("message", source="manual")
    instance.dungeon_templates = []
    instance.random.choice.side_effect = AssertionError("replay must not reroll")
    clock.value = datetime(2026, 7, 14, 16, 1, tzinfo=timezone.utc)
    assert instance.reset_dungeon("message", source="manual").status == "duplicate"
    assert instance.reset_dungeon("message", source="manual", business_date="2026-07-14").status == "duplicate"
    with pytest.raises(RuntimeError, match="operation_conflict"):
        instance.reset_dungeon("message", source="manual", business_date="2026-07-15")
    with pytest.raises(RuntimeError, match="operation_conflict"):
        instance.reset_dungeon("message", source="daily", business_date="2026-07-14")
    assert instance.current_dungeon.id == "d1"


def test_automatic_operation_id_requires_explicit_business_date():
    with pytest.raises(TypeError):
        DungeonResetSqlRepository.automatic_operation_id()
    with pytest.raises(ValueError):
        DungeonResetSqlRepository.automatic_operation_id(None)
