from __future__ import annotations

from pathlib import Path

import pytest

from nonebot_plugin_xiuxian_2.core.errors import OperationConflictError
from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_event_receipts,
    apply_activity_state_schema,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import service
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_pass


def _configure_activity(monkeypatch, database: Path, activities):
    with DatabaseUnitOfWork(database) as uow:
        apply_activity_state_schema(uow)
        apply_activity_event_receipts(uow)
    monkeypatch.setattr(service, "DB_PATH", database)
    monkeypatch.setattr(service, "ensure_activity_files", lambda: None)
    monkeypatch.setattr(service, "load_config", lambda: {"configured": True})
    monkeypatch.setattr(service, "get_gameplay_activities", lambda _config: activities)
    monkeypatch.setattr(service, "get_activity_tasks", lambda _config: [])
    monkeypatch.setattr(service, "_activity_pass_config", lambda _config: {"enabled": False})
    monkeypatch.setattr(
        service,
        "activity_runtime_state",
        lambda _config: {"ok": True, "can_produce": True, "features": {"points"}, "multiplier": 1},
    )
    monkeypatch.setattr(service, "activity_state", lambda _activity: (True, ""))
    monkeypatch.setattr(activity_pass, "_activity_pass_config", lambda _config: {"enabled": False})
    monkeypatch.setattr(activity_pass, "activity_runtime_state", lambda _config: {"ok": True, "features": set()})


def test_activity_effect_event_is_atomic_and_idempotent(monkeypatch, tmp_path):
    database = tmp_path / "game.db"
    _configure_activity(
        monkeypatch,
        database,
        [{
            "key": "test-event",
            "name": "测试活动",
            "type": "event_points",
            "event_rules": [{"event": "out_closing", "points": 3, "daily_limit": 100}],
        }],
    )

    first = service.record_activity_event(
        "u", "out_closing", 10, event_id="closing-effects:activity", occurred_at="2026-10-02T10:00:00+08:00"
    )
    duplicate = service.record_activity_event(
        "u", "out_closing", 10, event_id="closing-effects:activity", occurred_at="2026-10-02T10:00:00+08:00"
    )

    assert len(first) == 1
    assert duplicate == []
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT points,total_points FROM activity_point_balance WHERE user_id='u'") == {
            "points": 30,
            "total_points": 30,
        }
        assert uow.query_one("SELECT COUNT(*) AS count FROM activity_event_operations")["count"] == 1
        assert uow.query_one("SELECT COUNT(*) AS count FROM activity_point_event_log")["count"] == 1

    with pytest.raises(OperationConflictError):
        service.record_activity_event(
            "u", "out_closing", 11, event_id="closing-effects:activity", occurred_at="2026-10-02T10:00:00+08:00"
        )


def test_inactive_activity_event_is_receipted_before_future_replay(monkeypatch, tmp_path):
    database = tmp_path / "game.db"
    _configure_activity(monkeypatch, database, [])
    first = service.record_activity_event(
        "u", "out_closing", 10, event_id="closing-effects:inactive", occurred_at="2026-10-02T10:00:00+08:00"
    )
    assert first == []

    activity = {
        "key": "later-enabled",
        "name": "稍后启用",
        "type": "event_points",
        "event_rules": [{"event": "out_closing", "points": 3}],
    }
    monkeypatch.setattr(service, "get_gameplay_activities", lambda _config: [activity])
    assert service.record_activity_event(
        "u", "out_closing", 10, event_id="closing-effects:inactive", occurred_at="2026-10-02T10:00:00+08:00"
    ) == []
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT COUNT(*) AS count FROM activity_point_balance")["count"] == 0
