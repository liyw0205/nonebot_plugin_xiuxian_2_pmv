import json
import sqlite3

import pytest

from nonebot_plugin_xiuxian_2.features.world_events.application import DemonEventLifecycleApplication
from nonebot_plugin_xiuxian_2.features.world_events.migrations import (
    apply_world_events_lifecycle,
    apply_world_events_player,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


STATE_FIELDS = (
    "active", "status", "event_id", "event_type", "name", "period", "manual",
    "bosses", "participants", "claimed", "started_at", "ends_at", "last_result",
)
JSON_FIELDS = {"bosses", "participants", "claimed"}


def idle_state():
    state = {
        field: {} if field in JSON_FIELDS else 0 if field in {"active", "manual"} else ""
        for field in STATE_FIELDS
    }
    state["status"] = "idle"
    return state


def active_state(event_id="event-1", manual=0):
    state = idle_state()
    state.update({
        "active": 1,
        "status": "active",
        "event_id": event_id,
        "event_type": "demon_invasion",
        "name": "魔修入侵",
        "period": "2026-07-14",
        "manual": manual,
        "bosses": {"练气境": {"wave": 1}},
        "started_at": "18:00",
        "ends_at": "22:00",
    })
    return state


def apply_schema(path, state=None):
    with DatabaseUnitOfWork(path, immediate=True) as uow:
        apply_world_events_player(uow)
        apply_world_events_lifecycle(uow)
        if state is not None:
            columns = ",".join(f'"{field}"' for field in STATE_FIELDS)
            values = [
                json.dumps(state[field], ensure_ascii=False) if field in JSON_FIELDS else state[field]
                for field in STATE_FIELDS
            ]
            uow.execute(
                f"INSERT INTO world_event_state(user_id,{columns}) VALUES(?,{','.join('?' for _ in STATE_FIELDS)})",
                ("global", *values),
            )


def test_lifecycle_application_applies_replays_and_rejects_payload_conflicts(tmp_path):
    db = tmp_path / "player.db"
    expected, target = idle_state(), active_state()
    apply_schema(db, expected)
    app = DemonEventLifecycleApplication(db)

    first = app.transition("start-day", "global", "auto_start", expected, target)
    replay = app.replay("start-day")
    same_request = app.transition("start-day", "global", "auto_start", expected, target)
    conflict = app.transition("start-day", "global", "auto_start", expected, active_state("other"))

    assert first.status == "applied" and first.state == target
    assert replay == first
    assert same_request == first
    assert conflict.status == "operation_conflict"
    with DatabaseUnitOfWork(db, read_only=True) as uow:
        row = uow.query_one("SELECT status,event_id,bosses FROM world_event_state WHERE user_id=?", ("global",))
        assert (row["status"], row["event_id"], json.loads(row["bosses"])) == (
            "active", "event-1", target["bosses"]
        )


def test_lifecycle_application_can_start_from_an_unpersisted_idle_snapshot(tmp_path):
    db = tmp_path / "player.db"
    expected, target = idle_state(), active_state()
    apply_schema(db)

    result = DemonEventLifecycleApplication(db).transition(
        "first-start", "global", "auto_start", expected, target
    )

    assert result.status == "applied" and result.state == target
    with DatabaseUnitOfWork(db, read_only=True) as uow:
        assert uow.query_one("SELECT COUNT(*) AS n FROM world_event_state")["n"] == 1


def test_lifecycle_application_preserves_manual_finish_snapshot_and_rejects_stale_state(tmp_path):
    db = tmp_path / "player.db"
    expected = active_state(manual=1)
    target = dict(expected, active=0, status="finished", last_result="manual finish")
    apply_schema(db, expected)
    app = DemonEventLifecycleApplication(db)

    result = app.transition("finish-1", "global", "manual_finish", expected, target)
    stale = app.transition(
        "finish-old", "global", "auto_finish", active_state("old-event"), dict(target, event_id="old-event")
    )

    assert result.status == "applied"
    assert result.state["bosses"] == expected["bosses"]
    assert stale.status == "state_changed"


def test_lifecycle_application_rolls_back_state_when_operation_write_fails(tmp_path):
    db = tmp_path / "player.db"
    expected, target = idle_state(), active_state()
    apply_schema(db, expected)
    with DatabaseUnitOfWork(db) as uow:
        uow.execute(
            "CREATE TRIGGER reject_lifecycle BEFORE INSERT ON demon_event_lifecycle_operations "
            "BEGIN SELECT RAISE(ABORT, 'reject lifecycle'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="reject lifecycle"):
        DemonEventLifecycleApplication(db).transition(
            "start", "global", "manual_start", expected, target
        )

    with DatabaseUnitOfWork(db, read_only=True) as uow:
        state = uow.query_one("SELECT status,event_id FROM world_event_state WHERE user_id=?", ("global",))
        assert (state["status"], state["event_id"]) == ("idle", "")
        assert uow.query_one("SELECT COUNT(*) AS n FROM demon_event_lifecycle_operations")["n"] == 0


def test_lifecycle_verification_failure_rolls_back_state_and_operation(tmp_path):
    db = tmp_path / "player.db"
    expected, target = idle_state(), active_state()
    apply_schema(db, expected)
    with DatabaseUnitOfWork(db) as uow:
        uow.execute(
            "CREATE TRIGGER tamper_lifecycle_state AFTER UPDATE ON world_event_state "
            "BEGIN UPDATE world_event_state SET status='tampered' WHERE user_id=NEW.user_id; END"
        )

    with pytest.raises(RuntimeError, match="state verification failed"):
        DemonEventLifecycleApplication(db).transition(
            "start-verify", "global", "manual_start", expected, target
        )

    with DatabaseUnitOfWork(db, read_only=True) as uow:
        row = uow.query_one("SELECT status,event_id FROM world_event_state WHERE user_id=?", ("global",))
        assert (row["status"], row["event_id"]) == ("idle", "")
        assert uow.query_one("SELECT COUNT(*) AS n FROM demon_event_lifecycle_operations")["n"] == 0


def test_lifecycle_missing_migration_is_rejected_without_request_ddl(tmp_path):
    db = tmp_path / "player.db"
    expected, target = idle_state(), active_state()
    with DatabaseUnitOfWork(db, immediate=True) as uow:
        apply_world_events_player(uow)
        columns = ",".join(f'"{field}"' for field in STATE_FIELDS)
        values = [
            json.dumps(expected[field], ensure_ascii=False) if field in JSON_FIELDS else expected[field]
            for field in STATE_FIELDS
        ]
        uow.execute(
            f"INSERT INTO world_event_state(user_id,{columns}) VALUES(?,{','.join('?' for _ in STATE_FIELDS)})",
            ("global", *values),
        )

    before = sqlite3.connect(db)
    existing_tables = {
        row[0] for row in before.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    before.close()
    result = DemonEventLifecycleApplication(db).transition(
        "start", "global", "manual_start", expected, target
    )
    after = sqlite3.connect(db)
    current_tables = {row[0] for row in after.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    after.close()

    assert result.status == "schema_missing"
    assert current_tables == existing_tables


def test_lifecycle_migration_preserves_existing_operations_and_routes_to_player_only(tmp_path):
    db = tmp_path / "player.db"
    with DatabaseUnitOfWork(db, immediate=True) as uow:
        apply_world_events_player(uow)
        uow.execute(
            "CREATE TABLE demon_event_lifecycle_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TEXT NOT NULL)"
        )
        uow.execute(
            "INSERT INTO demon_event_lifecycle_operations VALUES(?,?,?,?)",
            ("old-op", "payload", '{"status":"applied","action":"auto_start","state":null}', "old-time"),
        )
        apply_world_events_lifecycle(uow)

    with DatabaseUnitOfWork(db, read_only=True) as uow:
        row = uow.query_one(
            "SELECT payload,result_json,created_at FROM demon_event_lifecycle_operations WHERE operation_id=?",
            ("old-op",),
        )
        assert row == {
            "payload": "payload",
            "result_json": '{"status":"applied","action":"auto_start","state":null}',
            "created_at": "old-time",
        }

    migrations = build_migrations()
    game_versions = {item.version for item in migrations_for_database(migrations, "game_db")}
    player_versions = {item.version for item in migrations_for_database(migrations, "player_db")}
    assert "world_events.004" not in game_versions
    assert "world_events.004" in player_versions
