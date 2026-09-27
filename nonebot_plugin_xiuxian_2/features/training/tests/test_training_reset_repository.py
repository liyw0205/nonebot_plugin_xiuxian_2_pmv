from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import build_migrations, migrations_for_database
from ..migrations import apply_training_event_player, apply_training_reset_operations
from ..reset_repository import TrainingResetSqlRepository


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 27, 12, 34, 56, tzinfo=timezone.utc)


def _databases(tmp_path: Path) -> tuple[Path, Path]:
    game, player = tmp_path / "game.db", tmp_path / "player.db"
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
        uow.executemany(
            "INSERT INTO user_xiuxian VALUES (?)", (("u1",), ("u2",), ("u3",))
        )
        apply_training_reset_operations(uow)
    with DatabaseUnitOfWork(player) as uow:
        apply_training_event_player(uow)
        uow.executemany(
            "INSERT INTO training(user_id,progress,last_time,points,completed,max_progress,last_event,weekly_purchases) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (
                ("u1", 8, "2026-09-26 01:00:00", 91, 4, 12, "旧事件", '{"1":2}'),
                ("u2", 0, None, 0, 0, 0, "", '{"_last_reset":"2026-09-27"}'),
            ),
        )
    return game, player


def _repo(tmp_path: Path) -> tuple[TrainingResetSqlRepository, Path, Path]:
    game, player = _databases(tmp_path)
    return TrainingResetSqlRepository(game, player, clock=FixedClock()), game, player


def test_reset_freezes_targets_and_resumes_in_chunks(tmp_path: Path) -> None:
    repo, game, player = _repo(tmp_path)
    first = repo.reset("reset-1", "admin", chunk_size=1)
    assert (first.status, first.task_status, first.reset_date, first.total, first.completed) == (
        "applied", "running", "2026-09-27", 3, 1
    )

    with sqlite3.connect(game) as connection:
        connection.execute("INSERT INTO user_xiuxian VALUES ('u4')")
    with sqlite3.connect(player) as connection:
        connection.execute("INSERT INTO training(user_id,progress) VALUES ('u4', 9)")

    done = repo.reset("reset-1", "admin", chunk_size=10)
    assert (done.task_status, done.total, done.completed, done.changed, done.skipped) == (
        "completed", 3, 3, 1, 1
    )
    assert repo.reset("reset-1", "admin").status == "duplicate"
    assert repo.reset("reset-1", "other").status == "operation_conflict"
    with sqlite3.connect(player) as connection:
        assert connection.execute(
            "SELECT progress,last_time,points,completed,max_progress,last_event,weekly_purchases "
            "FROM training WHERE user_id='u1'"
        ).fetchone() == (0, None, 0, 0, 0, "", '{"_last_reset":"2026-09-27"}')


def test_reset_skips_deleted_frozen_users(tmp_path: Path) -> None:
    repo, game, player = _repo(tmp_path)
    repo.reset("deleted", "admin", chunk_size=1)
    with sqlite3.connect(game) as connection:
        connection.execute("DELETE FROM user_xiuxian WHERE user_id='u2'")
    with sqlite3.connect(player) as connection:
        before = connection.execute(
            "SELECT progress,weekly_purchases FROM training WHERE user_id='u2'"
        ).fetchone()
    result = repo.reset("deleted", "admin", chunk_size=10)
    assert (result.task_status, result.completed, result.skipped) == ("completed", 3, 2)
    with sqlite3.connect(player) as connection:
        assert connection.execute(
            "SELECT progress,weekly_purchases FROM training WHERE user_id='u2'"
        ).fetchone() == before


def test_failed_target_update_rolls_back_and_resumes(tmp_path: Path) -> None:
    repo, game, player = _repo(tmp_path)
    repo.reset("resume", "admin", chunk_size=1)
    with sqlite3.connect(game) as connection:
        connection.execute(
            "CREATE TRIGGER fail_reset BEFORE UPDATE OF status ON admin_training_reset_targets "
            "WHEN NEW.user_id='u2' AND NEW.status='applied' "
            "BEGIN SELECT RAISE(ABORT,'failed'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="failed"):
        repo.reset("resume", "admin", chunk_size=1)
    with sqlite3.connect(player) as connection:
        assert connection.execute(
            "SELECT progress FROM training WHERE user_id='u2'"
        ).fetchone()[0] == 0
    with sqlite3.connect(game) as connection:
        assert connection.execute(
            "SELECT status FROM admin_training_reset_targets WHERE operation_id='resume' AND user_id='u2'"
        ).fetchone()[0] == "pending"
        connection.execute("DROP TRIGGER fail_reset")
    assert repo.reset("resume", "admin", chunk_size=10).task_status == "completed"


def test_missing_schema_does_not_create_reset_tables(tmp_path: Path) -> None:
    game, player = tmp_path / "game.db", tmp_path / "player.db"
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("CREATE TABLE training(user_id TEXT PRIMARY KEY)")
    result = TrainingResetSqlRepository(game, player, clock=FixedClock()).reset("missing", "admin")
    assert result.status == "schema_missing"
    with sqlite3.connect(game) as connection:
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE name='admin_training_reset_operations'"
        ).fetchone() is None


def test_reset_migration_backfills_legacy_columns_and_is_game_only(tmp_path: Path) -> None:
    game = tmp_path / "game.db"
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE admin_training_reset_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL)")
        uow.execute("CREATE TABLE admin_training_reset_targets(operation_id TEXT,user_id TEXT)")
        apply_training_reset_operations(uow)
        operation_columns = {
            str(row["name"])
            for row in uow.query_all("PRAGMA table_info(admin_training_reset_operations)")
        }
        target_columns = {
            str(row["name"])
            for row in uow.query_all("PRAGMA table_info(admin_training_reset_targets)")
        }
    assert {"reset_date", "total", "completed", "changed", "skipped", "status", "updated_at"}.issubset(operation_columns)
    assert {"status", "previous_state", "updated_at"}.issubset(target_columns)
    migrations = build_migrations()
    assert [m.version for m in migrations_for_database(migrations, "game_db") if m.version.startswith("training.")] == [
        "training.001", "training.003", "training.004"
    ]
    assert [m.version for m in migrations_for_database(migrations, "player_db") if m.version.startswith("training.")] == [
        "training.002"
    ]
