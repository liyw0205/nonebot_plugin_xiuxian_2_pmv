from __future__ import annotations

import json
import sqlite3

from ..migrations import apply_boss_player_schema
from ..world_boss_repository import (
    WorldBossDailyLimitResetSqlRepository,
    WorldBossManualSpawnSqlRepository,
)
from ....infrastructure.database import DatabaseUnitOfWork


CONFIG = {
    "Boss名字": ["玄狼"],
    "Boss灵石": {"练气境": [100]},
    "Boss倍率": {},
}
BOSS = {
    "name": "玄狼",
    "jj": "练气境",
    "气血": 100,
    "总血量": 100,
    "真元": 20,
    "攻击": 10,
    "max_stone": 100,
    "stone": 100,
}


def _migrate(database):
    with DatabaseUnitOfWork(database) as uow:
        apply_boss_player_schema(uow)


def test_request_paths_fail_closed_without_startup_schema(tmp_path):
    database = tmp_path / "player.db"
    manual = WorldBossManualSpawnSqlRepository(database, lambda: CONFIG)
    daily = WorldBossDailyLimitResetSqlRepository(database)

    assert manual.snapshot() == ([], 0)
    assert manual.spawn(
        operation_id="spawn-1",
        expected_revision=0,
        expected_bosses=[],
        expected_config=manual.config_snapshot(CONFIG, "练气境"),
        boss=BOSS,
    ).status == "schema_missing"
    assert daily.reset("2026-10-01").status == "schema_missing"
    assert not database.exists()


def test_manual_spawn_replay_and_daily_reset_use_migrated_schema(tmp_path):
    database = tmp_path / "player.db"
    _migrate(database)
    with sqlite3.connect(database) as conn:
        conn.execute("INSERT INTO boss VALUES('u1',12,300,2)")
        conn.execute("INSERT INTO boss VALUES('u2',0,0,0)")

    manual = WorldBossManualSpawnSqlRepository(database, lambda: CONFIG)
    expected_config = manual.config_snapshot(CONFIG, "练气境")
    first = manual.spawn(
        operation_id="spawn-1",
        expected_revision=0,
        expected_bosses=[],
        expected_config=expected_config,
        boss=BOSS,
    )
    duplicate = manual.spawn(
        operation_id="spawn-1",
        expected_revision=0,
        expected_bosses=[],
        expected_config=expected_config,
        boss=BOSS,
    )
    assert (first.status, duplicate.status, duplicate.revision) == ("spawned", "duplicate", 1)
    assert json.loads(sqlite3.connect(database).execute("SELECT bosses FROM world_boss_state").fetchone()[0]) == [BOSS]

    daily = WorldBossDailyLimitResetSqlRepository(database)
    first_reset = daily.reset("2026-10-01", chunk_size=1)
    second_reset = daily.reset("2026-10-01", chunk_size=1)
    assert (first_reset.task_status, first_reset.completed, second_reset.status) == ("running", 1, "applied")
    assert daily.reset("2026-10-01").status == "duplicate"


def test_boss_player_migration_is_routed_only_to_player_database():
    from ....plugin import build_migrations, migrations_for_database

    migrations = build_migrations()
    game = {item.version for item in migrations_for_database(migrations, "game_db")}
    player = {item.version for item in migrations_for_database(migrations, "player_db")}
    assert "boss.004" not in game
    assert "boss.004" in player
