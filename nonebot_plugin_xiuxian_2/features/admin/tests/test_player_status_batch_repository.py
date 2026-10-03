from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import build_migrations, migrations_for_database
from ..application import AdminApplication
from ..migrations import apply_admin_player_status_batch_reset
from ..player_status_batch_repository import AdminPlayerStatusBatchResetSqlRepository


def _create_players(database: Path, count: int = 3) -> None:
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(
            "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,hp INTEGER,"
            "mp INTEGER,atk INTEGER,user_stamina INTEGER)"
        )
        uow.executemany(
            "INSERT INTO user_xiuxian VALUES(?,?,?,?,?,?)",
            ((f"u{i:03}", 100 + i, 1, 2, 3, 4) for i in range(count)),
        )


def _migrate(database: Path) -> None:
    with DatabaseUnitOfWork(database) as uow:
        apply_admin_player_status_batch_reset(uow)


def test_legacy_batches_backfill_targets_and_keep_progress(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _create_players(database)
    payload = json.dumps(
        {"request": {"operator_id": "admin", "max_stamina": 20}, "users": ["u000", "u001", "u002"]},
        sort_keys=True,
        separators=(",", ":"),
    )
    result_json = json.dumps(
        {"status": "reset", "previous_state": [100, 1, 2, 3, 4], "final_state": [100, 50, 100, 10, 20]},
        sort_keys=True,
        separators=(",", ":"),
    )
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(
            "CREATE TABLE admin_player_status_batch_reset_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,total INTEGER NOT NULL,"
            "status TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        uow.execute(
            "CREATE TABLE admin_player_status_batch_reset_progress("
            "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL,"
            "reset_applied INTEGER NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(operation_id,user_id))"
        )
        uow.execute(
            "INSERT INTO admin_player_status_batch_reset_operations"
            "(operation_id,payload,total,status) VALUES('old',?,3,'running')",
            (payload,),
        )
        uow.execute(
            "INSERT INTO admin_player_status_batch_reset_progress"
            "(operation_id,user_id,status,reset_applied,result_json) VALUES('old','u000','reset',1,?)",
            (result_json,),
        )

    _migrate(database)
    _migrate(database)
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        operation = uow.query_one(
            "SELECT payload,operator_id,max_stamina FROM "
            "admin_player_status_batch_reset_operations WHERE operation_id='old'"
        )
        targets = uow.query_all(
            "SELECT user_id,ordinal FROM admin_player_status_batch_reset_targets "
            "WHERE operation_id='old' ORDER BY ordinal"
        )
        progress = uow.query_one(
            "SELECT status,reset_applied,result_json FROM "
            "admin_player_status_batch_reset_progress WHERE operation_id='old' AND user_id='u000'"
        )
    assert operation == {"payload": payload, "operator_id": "admin", "max_stamina": 20}
    assert [(row["user_id"], row["ordinal"]) for row in targets] == [
        ("u000", 0), ("u001", 1), ("u002", 2)
    ]
    assert progress == {"status": "reset", "reset_applied": 1, "result_json": result_json}

    result = AdminPlayerStatusBatchResetSqlRepository(database).reset("old", "admin", 20)
    assert (result.total, result.completed, result.reset_users, result.skipped_users) == (3, 3, 3, 0)


def test_new_batch_freezes_targets_and_caps_each_chunk_at_100(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _create_players(database, count=101)
    _migrate(database)
    repository = AdminPlayerStatusBatchResetSqlRepository(database)

    first = repository.reset("bounded", "admin", 20, chunk_size=10_000)
    assert (first.total, first.completed) == (101, 100)
    assert repository.find_running("admin", 20) == "bounded"
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("DELETE FROM user_xiuxian WHERE user_id='u100'")
        uow.execute("INSERT INTO user_xiuxian VALUES('new',900,1,2,3,4)")
    final = repository.reset("bounded", "admin", 20, chunk_size=10_000)
    assert (final.total, final.completed, final.reset_users, final.skipped_users) == (101, 101, 100, 1)
    assert repository.find_running("admin", 20) is None
    assert repository.reset("bounded", "admin", 20).status == "duplicate"
    assert repository.reset("bounded", "admin", 30).status == "operation_conflict"
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one(
            "SELECT 1 AS present FROM admin_player_status_batch_reset_targets "
            "WHERE operation_id='bounded' AND user_id='new'"
        ) is None


def test_new_batch_checks_disk_before_freezing_targets(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _create_players(database, count=1)
    _migrate(database)
    repository = AdminPlayerStatusBatchResetSqlRepository(database)
    with patch(
        "nonebot_plugin_xiuxian_2.features.admin.player_status_batch_repository.shutil.disk_usage",
        return_value=SimpleNamespace(free=1),
    ):
        result = repository.reset("low-disk", "admin", 20)
    assert result.status == "insufficient_storage"
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one(
            "SELECT 1 AS present FROM admin_player_status_batch_reset_operations "
            "WHERE operation_id='low-disk'"
        ) is None


def test_progress_failure_replays_stable_child_receipt(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _create_players(database, count=1)
    _migrate(database)
    repository = AdminPlayerStatusBatchResetSqlRepository(database)
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(
            "CREATE TRIGGER reject_status_batch_progress BEFORE INSERT ON "
            "admin_player_status_batch_reset_progress "
            "BEGIN SELECT RAISE(ABORT,'failed'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="failed"):
        repository.reset("resume", "admin", 20)
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        child = uow.query_one(
            "SELECT operation_id FROM admin_player_status_reset_operations"
        )
        pending = uow.query_one(
            "SELECT COUNT(*) AS count FROM admin_player_status_batch_reset_progress"
        )
    assert child["operation_id"] == "admin-player-status-reset-batch:resume:u000"
    assert pending["count"] == 0

    with DatabaseUnitOfWork(database) as uow:
        uow.execute("DROP TRIGGER reject_status_batch_progress")
    result = repository.reset("resume", "admin", 20)
    assert (result.completed, result.reset_users) == (1, 1)
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        progress = uow.query_one(
            "SELECT status FROM admin_player_status_batch_reset_progress"
        )
        player = uow.query_one("SELECT exp,hp,mp,atk,user_stamina FROM user_xiuxian")
    assert progress["status"] == "duplicate"
    assert tuple(player.values()) == (100, 50, 100, 10, 20)


def test_legacy_forced_child_receipt_remains_replayable(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _create_players(database, count=1)
    _migrate(database)
    previous_state = json.dumps([100, 1, 2, 3, 4], separators=(",", ":"))
    final_state = json.dumps([100, 50, 100, 10, 20], separators=(",", ":"))
    child_id = "admin-player-status-reset-batch:legacy:u000"
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(
            "UPDATE user_xiuxian SET hp=50,mp=100,atk=10,user_stamina=20 WHERE user_id='u000'"
        )
        uow.execute(
            "INSERT INTO admin_player_status_reset_operations"
            "(operation_id,payload,previous_state,final_state) VALUES(?,?,?,?)",
            (child_id, '["admin","u000",20,1]', previous_state, final_state),
        )

    result = AdminPlayerStatusBatchResetSqlRepository(database).reset("legacy", "admin", 20)
    assert (result.completed, result.reset_users) == (1, 1)
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        progress = uow.query_one(
            "SELECT status FROM admin_player_status_batch_reset_progress"
        )
    assert progress["status"] == "duplicate"


def test_missing_schema_and_legacy_migration_fail_closed(tmp_path: Path) -> None:
    database = tmp_path / "unmigrated.db"
    _create_players(database, count=1)
    result = AdminPlayerStatusBatchResetSqlRepository(database).reset("op", "admin", 20)
    assert result.status == "schema_missing"
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE name='admin_player_status_batch_reset_operations'"
        ) is None

    low_ram = tmp_path / "low-ram.db"
    with DatabaseUnitOfWork(low_ram) as uow:
        uow.execute(
            "CREATE TABLE admin_player_status_batch_reset_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,total INTEGER NOT NULL,status TEXT NOT NULL)"
        )
        uow.execute(
            "INSERT INTO admin_player_status_batch_reset_operations"
            "(operation_id,payload,total,status) VALUES('op',?,1,'running')",
            (json.dumps({"request": {"operator_id": "admin", "max_stamina": 20}, "users": ["u"]}),),
        )
    with patch(
        "nonebot_plugin_xiuxian_2.features.admin.migrations._available_memory",
        return_value=1,
    ):
        with pytest.raises(RuntimeError, match="available RAM"):
            _migrate(low_ram)
    with DatabaseUnitOfWork(low_ram, read_only=True) as uow:
        assert uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE name='admin_player_status_batch_reset_targets'"
        ) is None

    corrupt = tmp_path / "corrupt.db"
    _create_players(corrupt, count=1)
    with DatabaseUnitOfWork(corrupt) as uow:
        uow.execute(
            "CREATE TABLE admin_player_status_batch_reset_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,total INTEGER NOT NULL,status TEXT NOT NULL)"
        )
        uow.execute(
            "INSERT INTO admin_player_status_batch_reset_operations"
            "(operation_id,payload,total,status) VALUES('bad','{',1,'running')"
        )
    with pytest.raises(RuntimeError, match="legacy admin reset payload is invalid"):
        _migrate(corrupt)
    with DatabaseUnitOfWork(corrupt, read_only=True) as uow:
        assert uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE name='admin_player_status_batch_reset_targets'"
        ) is None


def test_migration_capacity_and_database_routing(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    with patch(
        "nonebot_plugin_xiuxian_2.features.admin.migrations.shutil.disk_usage",
        return_value=SimpleNamespace(free=1),
    ):
        with pytest.raises(RuntimeError, match="admin reset migration needs about"):
            _migrate(database)
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE name='admin_player_status_batch_reset_operations'"
        ) is None

    migrations = build_migrations()
    for key, expected in (("game_db", True), ("player_db", False), ("trade_db", False)):
        versions = {item.version for item in migrations_for_database(migrations, key)}
        assert ("legacy.admin.002" in versions) is expected


def test_application_and_default_handler_use_bounded_feature_batch(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _create_players(database, count=1)
    _migrate(database)
    app = AdminApplication(database)
    result = app.reset_player_status_batch("app", "admin", 20)
    assert (result.status, result.total, result.completed) == ("applied", 1, 1)

    facade = Path(__file__).parents[3] / "xiuxian" / "xiuxian_admin" / "__init__.py"
    source = facade.read_text(encoding="utf-8")
    compile(source, str(facade), "exec")
    start = source.index("async def restate_")
    handler = source[start:source.index("@set_xiuxian.handle", start)]
    all_branch = handler[handler.index("if not plain_args and not give_qq:"):handler.index("nick_name =")]
    assert "admin_application.find_player_status_batch(" in all_branch
    assert "admin_application.reset_player_status_batch(" in all_branch
    assert "get_all_user_id()" not in all_branch
    assert "tuple(all_users" not in all_branch
