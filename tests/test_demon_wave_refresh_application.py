from __future__ import annotations

import json
import sqlite3

import pytest

from nonebot_plugin_xiuxian_2.features.world_events.application import (
    DemonWaveRefreshApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import (
    DatabaseUnitOfWork,
    MigrationRunner,
)
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


STATE_FIELDS = (
    "active", "status", "event_id", "event_type", "name", "period", "manual",
    "bosses", "participants", "claimed", "started_at", "ends_at", "last_result",
)
JSON_FIELDS = {"bosses", "participants", "claimed"}


def event_state():
    return {
        "active": 1,
        "status": "active",
        "event_id": "event-1",
        "event_type": "demon_invasion",
        "name": "魔修入侵",
        "period": "2026-07-14",
        "manual": 0,
        "bosses": {"练气境": {"wave": 1, "boss_hp": 0, "boss_max_hp": 1000}},
        "participants": {
            "p1": {
                "realm": "练气境",
                "wave": 1,
                "damage": 100,
                "reward_multiplier": 1.0,
            }
        },
        "claimed": {},
        "started_at": "2026-07-14 18:00:00",
        "ends_at": "2026-07-14 22:00:00",
        "last_result": "",
    }


def create_state_table(path, snapshot):
    with sqlite3.connect(path) as conn:
        definitions = ",".join(
            f'"{field}" {"INTEGER" if field in {"active", "manual"} else "TEXT"}'
            for field in STATE_FIELDS
        )
        conn.execute(f"CREATE TABLE world_event_state (user_id TEXT PRIMARY KEY,{definitions})")
        values = [
            json.dumps(snapshot[field], ensure_ascii=False)
            if field in JSON_FIELDS
            else snapshot[field]
            for field in STATE_FIELDS
        ]
        conn.execute(
            f"INSERT INTO world_event_state VALUES ({','.join('?' for _ in range(14))})",
            ("global", *values),
        )


def install_wave_migration(path):
    migrations = build_migrations()
    wave_migration = next(
        migration for migration in migrations if migration.version == "world_events.005"
    )
    assert wave_migration in migrations_for_database(migrations, "player_db")
    assert wave_migration not in migrations_for_database(migrations, "game_db")
    with DatabaseUnitOfWork(path) as uow:
        MigrationRunner((wave_migration,)).apply(uow)


def test_application_refresh_replays_and_keeps_reward_snapshot_atomic(tmp_path):
    database = tmp_path / "player.db"
    initial = event_state()
    create_state_table(database, initial)
    install_wave_migration(database)
    application = DemonWaveRefreshApplication(database)
    replacement = {"wave": 2, "boss_hp": 2000, "boss_max_hp": 2000}

    result = application.refresh(
        "slot-1", "global", initial, {"练气境": replacement}, "refreshed"
    )
    replay = application.replay("slot-1")
    repeated = application.refresh(
        "slot-1", "global", initial, {"练气境": replacement}, "refreshed"
    )

    assert result.status == "applied"
    assert result.refreshed_realms == ("练气境",)
    assert result.state["bosses"]["练气境"] == replacement
    assert result.state["participants"]["p1"]["reward_ready"] == 1
    assert result.state["participants"]["p1"]["reward_base_hp"] == 1000
    assert replay == repeated == result


def test_application_rejects_state_and_operation_conflicts(tmp_path):
    database = tmp_path / "player.db"
    initial = event_state()
    create_state_table(database, initial)
    install_wave_migration(database)
    application = DemonWaveRefreshApplication(database)
    replacement = {"wave": 2, "boss_hp": 2000, "boss_max_hp": 2000}
    changed = dict(initial, participants={})

    assert application.refresh(
        "stale", "global", changed, {"练气境": replacement}, "refreshed"
    ).status == "state_changed"
    assert application.refresh(
        "invalid", "global", initial, {"练气境": {"wave": 3}}, "refreshed"
    ).status == "invalid_plan"
    assert application.refresh(
        "slot-1", "global", initial, {"练气境": replacement}, "refreshed"
    ).status == "applied"
    assert application.refresh(
        "slot-1", "global", initial, {"练气境": replacement}, "different"
    ).status == "operation_conflict"


def test_application_reports_missing_startup_schema_without_ddl(tmp_path):
    database = tmp_path / "player.db"
    initial = event_state()
    create_state_table(database, initial)

    result = DemonWaveRefreshApplication(database).refresh(
        "slot-1",
        "global",
        initial,
        {"练气境": {"wave": 2}},
        "refreshed",
    )

    assert result.status == "schema_missing"
    with sqlite3.connect(database) as conn:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name='demon_wave_refresh_operations'"
        ).fetchone() is None


def test_application_rolls_back_state_if_operation_write_fails(tmp_path):
    database = tmp_path / "player.db"
    initial = event_state()
    create_state_table(database, initial)
    install_wave_migration(database)
    with sqlite3.connect(database) as conn:
        conn.execute(
            "CREATE TRIGGER reject_wave_operation BEFORE INSERT "
            "ON demon_wave_refresh_operations "
            "BEGIN SELECT RAISE(ABORT, 'reject wave operation'); END"
        )

    with pytest.raises(Exception, match="reject wave operation"):
        DemonWaveRefreshApplication(database).refresh(
            "slot-1",
            "global",
            initial,
            {"练气境": {"wave": 2, "boss_hp": 2, "boss_max_hp": 2}},
            "refreshed",
        )

    with sqlite3.connect(database) as conn:
        row = conn.execute(
            "SELECT bosses,participants,last_result FROM world_event_state WHERE user_id='global'"
        ).fetchone()
        assert json.loads(row[0]) == initial["bosses"]
        assert json.loads(row[1]) == initial["participants"]
        assert row[2] == initial["last_result"]
        assert conn.execute(
            "SELECT COUNT(*) FROM demon_wave_refresh_operations"
        ).fetchone()[0] == 0
