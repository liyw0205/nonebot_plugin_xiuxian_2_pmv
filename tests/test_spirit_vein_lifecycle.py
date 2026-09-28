from __future__ import annotations

import json
import importlib
import sqlite3
from pathlib import Path

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_world_events.transaction_service import (
    STATE_FIELDS,
)
from nonebot_plugin_xiuxian_2.compatibility.legacy_spirit_vein_lifecycle import (
    SpiritVeinLifecycleResult as LegacySpiritVeinLifecycleResult,
    SpiritVeinLifecycleService as LegacySpiritVeinLifecycleService,
)
from nonebot_plugin_xiuxian_2.features.world_events.application import (
    SpiritVeinLifecycleApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import (
    DatabaseUnitOfWork,
    MigrationRunner,
)
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_world_events.transaction_service import (
    SpiritVeinLifecycleService,
)


def test_world_events_facade_uses_spirit_vein_feature_application():
    world_events = importlib.import_module(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_world_events"
    )
    assert isinstance(
        world_events.spirit_vein_lifecycle_application,
        SpiritVeinLifecycleApplication,
    )
    assert not hasattr(world_events, "_spirit_vein_lifecycle_service")
    assert SpiritVeinLifecycleService is LegacySpiritVeinLifecycleService


def test_legacy_lifecycle_imports_keep_identity():
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_world_events.transaction_service import (
        SpiritVeinLifecycleResult,
    )

    assert SpiritVeinLifecycleService is LegacySpiritVeinLifecycleService
    assert SpiritVeinLifecycleResult is LegacySpiritVeinLifecycleResult


def idle():
    state = {
        field: (
            {}
            if field in {"bosses", "participants", "claimed"}
            else 0
            if field in {"active", "manual"}
            else ""
        )
        for field in STATE_FIELDS
    }
    state.update({"status": "idle", "event_type": "spirit_vein", "name": "天降灵脉"})
    return state


def active(event_id="spirit_vein:202607140830", manual=0):
    state = idle()
    state.update(
        {
            "active": 1,
            "status": "active",
            "event_id": event_id,
            "period": "2026-07-14",
            "manual": manual,
            "started_at": "2026-07-14 08:30:00",
            "ends_at": "2026-07-14 09:30:00",
            "last_result": "started",
        }
    )
    return state


def create_db(path, snapshot):
    with sqlite3.connect(path) as conn:
        definitions = ",".join(
            f'"{field}" {"INTEGER" if field in {"active", "manual"} else "TEXT"}'
            for field in STATE_FIELDS
        )
        conn.execute(
            f"CREATE TABLE world_event_state (user_id TEXT PRIMARY KEY,{definitions})"
        )
        if snapshot is not None:
            values = [
                json.dumps(snapshot[field], ensure_ascii=False)
                if field in {"bosses", "participants", "claimed"}
                else snapshot[field]
                for field in STATE_FIELDS
            ]
            conn.execute(
                f"INSERT INTO world_event_state VALUES "
                f"({','.join('?' for _ in range(len(STATE_FIELDS) + 1))})",
                ("spirit_vein", *values),
            )
    migrations = build_migrations()
    migration = next(item for item in migrations if item.version == "world_events.006")
    assert migration in migrations_for_database(migrations, "player_db")
    assert migration not in migrations_for_database(migrations, "game_db")
    with DatabaseUnitOfWork(path) as uow:
        MigrationRunner((migration,)).apply(uow)


def read_state(path):
    with sqlite3.connect(path) as conn:
        row = conn.execute(
            "SELECT status,event_id,active,manual,started_at,ends_at,last_result "
            "FROM world_event_state WHERE user_id='spirit_vein'"
        ).fetchone()
    return row


def test_start_replay_fixes_time_window_and_rejects_payload_conflict(tmp_path):
    database = tmp_path / "player.db"
    expected = idle()
    target = active()
    create_db(database, expected)
    service = SpiritVeinLifecycleApplication(database)

    result = service.transition(
        "auto-slot",
        "spirit_vein",
        "auto_start",
        expected,
        target,
    )
    assert result.status == "applied"
    assert result.state == target
    assert service.transition(
        "auto-slot",
        "spirit_vein",
        "auto_start",
        expected,
        target,
    ) == result
    changed_window = dict(target, ends_at="2026-07-14 10:30:00")
    assert service.transition(
        "auto-slot",
        "spirit_vein",
        "auto_start",
        expected,
        changed_window,
    ).status == "operation_conflict"
    assert read_state(database)[4:6] == (
        "2026-07-14 08:30:00",
        "2026-07-14 09:30:00",
    )


def test_feature_application_replays_legacy_spirit_vein_operation(tmp_path):
    database = tmp_path / "player.db"
    expected = idle()
    target = active(manual=1)
    create_db(database, expected)

    legacy_result = LegacySpiritVeinLifecycleService(database).transition(
        "legacy-start", "spirit_vein", "manual_start", expected, target
    )
    application = SpiritVeinLifecycleApplication(database)

    assert application.replay("legacy-start") == legacy_result
    assert application.transition(
        "legacy-start", "spirit_vein", "manual_start", expected, target
    ) == legacy_result


def test_miss_and_active_skip_are_persisted_without_changing_state(tmp_path):
    database = tmp_path / "player.db"
    expected = idle()
    create_db(database, expected)
    service = SpiritVeinLifecycleApplication(database)

    missed = service.transition(
        "miss-slot",
        "spirit_vein",
        "auto_miss",
        expected,
        expected,
    )
    assert (missed.status, missed.action) == ("not_triggered", "auto_miss")
    assert service.replay("miss-slot") == missed
    assert read_state(database)[:3] == ("idle", "", 0)

    started = service.transition(
        "manual-start",
        "spirit_vein",
        "manual_start",
        expected,
        active(manual=1),
    )
    skipped = service.transition(
        "active-slot",
        "spirit_vein",
        "auto_skip",
        started.state,
        started.state,
    )
    manual_skipped = service.transition(
        "manual-start-again",
        "spirit_vein",
        "manual_start_skip",
        started.state,
        started.state,
    )
    assert skipped.status == "already_active"
    assert manual_skipped.status == "already_active"
    assert read_state(database)[4:6] == (
        "2026-07-14 08:30:00",
        "2026-07-14 09:30:00",
    )


def test_expire_and_manual_finish_preserve_original_event_window(tmp_path):
    database = tmp_path / "player.db"
    expected = active()
    create_db(database, expected)
    service = SpiritVeinLifecycleApplication(database)

    expired = dict(expected, active=0, status="finished", last_result="expired")
    result = service.transition(
        "expire-event",
        "spirit_vein",
        "expire",
        expected,
        expired,
    )
    assert result.status == "applied"
    assert read_state(database) == (
        "finished",
        expected["event_id"],
        0,
        0,
        expected["started_at"],
        expected["ends_at"],
        "expired",
    )

    skipped = service.transition(
        "finish-again",
        "spirit_vein",
        "manual_finish_skip",
        expired,
        expired,
    )
    assert skipped.status == "already_finished"


def test_manual_finish_preserves_original_event_window(tmp_path):
    database = tmp_path / "player.db"
    expected = active(manual=1)
    create_db(database, expected)
    result = SpiritVeinLifecycleApplication(database).transition(
        "manual-finish",
        "spirit_vein",
        "manual_finish",
        expected,
        dict(
            expected,
            active=0,
            status="finished",
            manual=1,
            last_result="天降灵脉已手动关闭。",
        ),
    )

    assert result.status == "applied"
    assert result.state["event_id"] == expected["event_id"]
    assert result.state["started_at"] == expected["started_at"]
    assert result.state["ends_at"] == expected["ends_at"]


def test_operation_write_failure_rolls_back_state_transition(tmp_path):
    database = tmp_path / "player.db"
    expected = idle()
    create_db(database, expected)
    with sqlite3.connect(database) as conn:
        conn.execute(
            "CREATE TRIGGER reject_spirit_lifecycle BEFORE INSERT "
            "ON spirit_vein_lifecycle_operations "
            "BEGIN SELECT RAISE(ABORT,'reject lifecycle'); END"
        )

    with pytest.raises(Exception, match="reject lifecycle"):
        SpiritVeinLifecycleApplication(database).transition(
            "failed-start",
            "spirit_vein",
            "manual_start",
            expected,
            active(manual=1),
        )
    assert read_state(database)[:3] == ("idle", "", 0)


def test_invalid_time_window_is_rejected(tmp_path):
    database = tmp_path / "player.db"
    expected = idle()
    create_db(database, expected)
    invalid = active()
    invalid["ends_at"] = invalid["started_at"]
    result = SpiritVeinLifecycleApplication(database).transition(
        "invalid-window",
        "spirit_vein",
        "auto_start",
        expected,
        invalid,
    )
    assert result.status == "invalid_transition"
    assert read_state(database)[:3] == ("idle", "", 0)


def test_all_production_entries_use_lifecycle_operations():
    source = (
        Path(__file__).parents[1]
        / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_world_events/__init__.py"
    ).read_text(encoding="utf-8")
    assert "_save_state(state, SPIRIT_VEIN_EVENT_KEY)" not in source

    for start, end in (
        ("def _ensure_spirit_vein_state", "def _is_spirit_vein_active"),
        ("def _try_start_auto_spirit_vein", "def _start_spirit_vein_manual"),
        ("def _start_spirit_vein_manual", "def _close_spirit_vein_manual"),
        ("def _close_spirit_vein_manual", "def _ensure_daily_state"),
    ):
        section = source[source.index(start) : source.index(end, source.index(start))]
        assert "spirit_vein_lifecycle_application." in section

    assert "spirit_vein_lifecycle_application = SpiritVeinLifecycleApplication(" in source
    assert "SpiritVeinLifecycleService" not in source

    start_handler = source[
        source.index("async def start_spirit_vein_") : source.index(
            "@close_world_event.handle"
        )
    ]
    close_handler = source[
        source.index("async def close_spirit_vein_") : source.index(
            "@attack_demon_invasion.handle"
        )
    ]
    assert "spirit-vein:manual-start:{message_id}" in start_handler
    assert "_start_spirit_vein_manual(operation_id)" in start_handler
    assert "spirit-vein:manual-finish:{message_id}" in close_handler
    assert "_close_spirit_vein_manual(operation_id)" in close_handler
