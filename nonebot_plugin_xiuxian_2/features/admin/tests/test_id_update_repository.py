from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger, OutboxStore
from ....infrastructure.database.backup_capacity import InsufficientBackupSpace
from ....plugin import build_migrations, migrations_for_database
from .. import id_update_repository as id_update_repository_module
from ..id_swap_repository import DATABASE_ORDER
from ..id_update_repository import ACTION, AdminIdUpdateSqlRepository
from ..migrations import (
    apply_admin_id_swap_operations,
    apply_admin_id_swap_receipts,
    apply_admin_id_update_operations,
    apply_admin_id_update_step_receipts,
)


def _databases(root: Path, *, migrated: bool = True) -> dict[str, Path]:
    result = {key: root / f"{key}.db" for key in DATABASE_ORDER}
    for key, path in result.items():
        with DatabaseUnitOfWork(path) as uow:
            if key == "game_db":
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,name TEXT)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u1','one'),('u2','two')")
                uow.execute(
                    "CREATE TABLE player_data(sect_owner TEXT REFERENCES user_xiuxian(user_id))"
                )
                uow.execute("INSERT INTO player_data VALUES('u1')")
                if migrated:
                    OperationLedger().ensure_schema(uow)
                    OutboxStore().ensure_schema(uow)
                    apply_admin_id_swap_receipts(uow)
                    apply_admin_id_swap_operations(uow)
                    apply_admin_id_update_step_receipts(uow)
                    apply_admin_id_update_operations(uow)
            elif key in {"impart_db", "trade_db"}:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u1'),('u2')")
                if migrated:
                    apply_admin_id_swap_receipts(uow)
                    apply_admin_id_update_step_receipts(uow)
            else:
                uow.execute(
                    "CREATE TABLE user_xiuxian("
                    "user_id TEXT PRIMARY KEY,partner_id TEXT,group_id TEXT,main_id TEXT,active_id TEXT)"
                )
                uow.execute("INSERT INTO user_xiuxian VALUES('u1','u1','u1','u1','u1')")
                if migrated:
                    apply_admin_id_swap_receipts(uow)
                    apply_admin_id_update_step_receipts(uow)
    return result


def _repository(root: Path, *, migrated: bool = True, **kwargs) -> AdminIdUpdateSqlRepository:
    return AdminIdUpdateSqlRepository(
        _databases(root, migrated=migrated), root / "players", **kwargs
    )


def _values(database: Path, sql: str) -> list[tuple]:
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        return connection.execute(sql).fetchall()


def test_update_migrates_frozen_columns_once_and_invalidates_caches(tmp_path: Path) -> None:
    roster_invalidations: list[str] = []
    player_invalidations: list[tuple[str, tuple[str, ...]]] = []
    repository = _repository(
        tmp_path,
        invalidate_user_id_cache=lambda: roster_invalidations.append("cleared"),
        invalidate_player_data_cache=lambda table, fields: player_invalidations.append((table, fields)),
    )
    players = tmp_path / "players"
    (players / "u1").mkdir(parents=True)
    (players / "u1" / "marker").write_text("profile", encoding="utf-8")

    result = repository.update("rename-ok", "u1", "u3")

    assert result.status == "applied"
    assert result.data["updated_cells"] == 9
    assert _values(tmp_path / "game_db.db", "SELECT user_id FROM user_xiuxian WHERE name='one'") == [("u3",)]
    assert _values(tmp_path / "game_db.db", "SELECT sect_owner FROM player_data") == [("u3",)]
    for key in ("impart_db", "trade_db"):
        assert _values(tmp_path / f"{key}.db", "SELECT user_id FROM user_xiuxian ORDER BY rowid") == [
            ("u3",), ("u2",)
        ]
    assert _values(
        tmp_path / "player_db.db",
        "SELECT user_id,partner_id,group_id,main_id,active_id FROM user_xiuxian",
    ) == [("u3", "u3", "u3", "u3", "u3")]
    assert (players / "u3" / "marker").read_text(encoding="utf-8") == "profile"
    assert not (players / "u1").exists()
    assert roster_invalidations == ["cleared"]
    assert {field for _table, fields in player_invalidations for field in fields} == {
        "user_id", "partner_id", "group_id", "main_id", "active_id"
    }

    replay = repository.update("rename-ok", "u1", "u3")
    assert replay.replayed
    assert replay.status == "replayed"
    assert roster_invalidations == ["cleared"]
    conflict = repository.update("rename-ok", "u1", "u4")
    assert conflict.code == "operation_payload_conflict"
    assert _values(tmp_path / "game_db.db", "SELECT user_id FROM user_xiuxian WHERE name='one'") == [("u3",)]


class _FailOnceAfterImpart(AdminIdUpdateSqlRepository):
    failed = False

    def _after_database_step(self, database_key: str) -> None:
        if database_key == "impart_db" and not self.failed:
            self.failed = True
            raise OSError("injected crash after impart commit")


def test_committed_steps_recover_without_double_rewriting(tmp_path: Path) -> None:
    databases = _databases(tmp_path)
    repository = _FailOnceAfterImpart(databases, tmp_path / "players")

    failed = repository.update("rename-retry", "u1", "u3")

    assert failed.code == "needs_reconcile"
    assert _values(databases["game_db"], "SELECT user_id FROM user_xiuxian WHERE name='one'") == [("u3",)]
    assert _values(databases["impart_db"], "SELECT user_id FROM user_xiuxian ORDER BY rowid") == [
        ("u3",), ("u2",)
    ]
    assert _values(databases["trade_db"], "SELECT user_id FROM user_xiuxian ORDER BY rowid") == [
        ("u1",), ("u2",)
    ]
    assert _values(databases["game_db"], "SELECT status FROM admin_id_update_operations") == [
        ("needs_reconcile",)
    ]

    assert repository.reconcile_pending() == {"recovered": 1, "pending": 0, "failed": 0}
    assert _values(databases["trade_db"], "SELECT user_id FROM user_xiuxian ORDER BY rowid") == [
        ("u3",), ("u2",)
    ]
    assert [
        _values(databases[key], "SELECT COUNT(*) FROM admin_id_update_step_receipts")[0][0]
        for key in DATABASE_ORDER
    ] == [1, 1, 1, 1]
    assert repository.update("rename-retry", "u1", "u3").replayed


class _FailAfterDirectoryRename(AdminIdUpdateSqlRepository):
    failed = False

    def _rename_directory(self, source: Path, target: Path) -> None:
        source.rename(target)
        if not self.failed:
            self.failed = True
            raise OSError("injected crash after directory rename")


def test_directory_rename_before_phase_commit_is_detected_and_resumed(tmp_path: Path) -> None:
    databases = _databases(tmp_path)
    players = tmp_path / "players"
    (players / "u1").mkdir(parents=True)
    (players / "u1" / "marker").write_text("profile", encoding="utf-8")
    repository = _FailAfterDirectoryRename(databases, players)

    failed = repository.update("rename-dir", "u1", "u3")

    assert failed.code == "needs_reconcile"
    assert _values(databases["game_db"], "SELECT directory_phase FROM admin_id_update_operations") == [
        ("pending",)
    ]
    assert repository.reconcile_pending() == {"recovered": 1, "pending": 0, "failed": 0}
    assert (players / "u3" / "marker").read_text(encoding="utf-8") == "profile"
    assert not (players / "u1").exists()
    assert _values(databases["game_db"], "SELECT directory_phase FROM admin_id_update_operations") == [
        ("completed",)
    ]


@pytest.mark.parametrize(
    ("old_id", "new_id", "expected_code"),
    [
        ("", "u3", "invalid_id"),
        ("u1", "u1", "invalid_id"),
        ("../u1", "u3", "invalid_id"),
        ("u1", "C:profile", "invalid_id"),
    ],
)
def test_invalid_paths_are_rejected_without_writes(
    tmp_path: Path, old_id: str, new_id: str, expected_code: str
) -> None:
    repository = _repository(tmp_path)

    result = repository.update(f"invalid-{old_id}-{new_id}", old_id, new_id)

    assert result.status == "rejected"
    assert result.code == expected_code
    assert _values(tmp_path / "game_db.db", "SELECT user_id FROM user_xiuxian ORDER BY name") == [
        ("u1",), ("u2",)
    ]


def test_existing_destination_id_or_directory_is_rejected(tmp_path: Path) -> None:
    repository = _repository(tmp_path)
    (tmp_path / "players" / "u3").mkdir(parents=True)

    result = repository.update("rename-destination-used", "u1", "u3")

    assert result.code == "user_id_conflict"
    assert _values(tmp_path / "game_db.db", "SELECT user_id FROM user_xiuxian WHERE name='one'") == [("u1",)]
    assert _values(tmp_path / "game_db.db", "SELECT COUNT(*) FROM admin_id_update_operations") == [(0,)]


def test_missing_startup_migrations_fail_closed_without_request_ddl(tmp_path: Path) -> None:
    databases = _databases(tmp_path, migrated=False)
    repository = AdminIdUpdateSqlRepository(databases, tmp_path / "players")

    result = repository.update("rename-unmigrated", "u1", "u3")

    assert result.code == "schema_missing"
    assert _values(databases["game_db"], "SELECT name FROM sqlite_master WHERE type='table' AND name='admin_id_update_operations'") == []
    assert _values(databases["impart_db"], "SELECT name FROM sqlite_master WHERE type='table' AND name='admin_id_update_step_receipts'") == []


def test_capacity_failure_defers_update_and_retry_resumes(tmp_path: Path, monkeypatch) -> None:
    databases = _databases(tmp_path)
    repository = AdminIdUpdateSqlRepository(databases, tmp_path / "players")

    def reject_capacity(_requirements, *, operation):
        raise InsufficientBackupSpace(f"not enough space for {operation}")

    monkeypatch.setattr(id_update_repository_module, "preflight_capacity", reject_capacity)
    deferred = repository.update("rename-capacity", "u1", "u3")

    assert deferred.code == "insufficient_storage"
    assert _values(databases["game_db"], "SELECT user_id FROM user_xiuxian WHERE name='one'") == [("u1",)]
    assert _values(databases["game_db"], "SELECT status FROM admin_id_update_operations") == [("started",)]
    assert repository.reconcile_pending() == {"recovered": 0, "pending": 1, "failed": 1}

    monkeypatch.setattr(id_update_repository_module, "preflight_capacity", lambda *_args, **_kwargs: None)
    resumed = repository.update("rename-capacity", "u1", "u3")
    assert resumed.status == "applied"
    assert _values(databases["game_db"], "SELECT user_id FROM user_xiuxian WHERE name='one'") == [("u3",)]


def test_pending_update_blocks_id_swap_and_shares_mutation_lock(tmp_path: Path) -> None:
    databases = _databases(tmp_path)
    repository = _FailOnceAfterImpart(databases, tmp_path / "players")
    assert repository.update("rename-pending", "u1", "u3").code == "needs_reconcile"
    assert (tmp_path / ".admin-id-mutation.lock").is_file()

    from ..id_swap_repository import AdminIdSwapSqlRepository

    swap = AdminIdSwapSqlRepository(databases, tmp_path / "players")
    result = swap.swap("swap-blocked", "u1", "u2")

    assert result.code == "reconcile_pending"
    assert _values(databases["game_db"], "SELECT user_id FROM user_xiuxian ORDER BY name") == [
        ("u3",), ("u2",)
    ]


def test_update_migrations_route_receipts_and_plan_to_intended_databases() -> None:
    migrations = build_migrations()
    for key in DATABASE_ORDER:
        versions = {item.version for item in migrations_for_database(migrations, key)}
        assert "legacy.admin.005" in versions
    game_versions = {item.version for item in migrations_for_database(migrations, "game_db")}
    assert "legacy.admin.006" in game_versions
    for key in ("player_db", "trade_db", "impart_db"):
        versions = {item.version for item in migrations_for_database(migrations, key)}
        assert "legacy.admin.006" not in versions


def test_default_handler_routes_id_update_through_admin_application() -> None:
    source = Path("nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py").read_text(encoding="utf-8")
    handler = source.split("async def update_id_cmd_", 1)[1].split("@swap_id_cmd.handle", 1)[0]
    assert "admin_application.update_user_id" in handler
    assert "migrate_single_user_id" not in handler
