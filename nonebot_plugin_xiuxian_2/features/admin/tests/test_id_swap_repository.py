from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger, OutboxStore
from ....infrastructure.database.backup_capacity import InsufficientBackupSpace
from ....plugin import build_migrations, migrations_for_database
from .. import id_swap_repository as id_swap_repository_module
from ..id_swap_repository import ACTION, DATABASE_ORDER, AdminIdSwapSqlRepository
from ..migrations import apply_admin_id_swap_operations, apply_admin_id_swap_receipts


def _databases(root: Path, *, migrated: bool = True) -> dict[str, Path]:
    result = {key: root / f"{key}.db" for key in DATABASE_ORDER}
    for key, path in result.items():
        with DatabaseUnitOfWork(path) as uow:
            if key == "game_db":
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,name TEXT)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u1','one'),('u2','two')")
                uow.execute(
                    "CREATE TABLE player_data(sect_owner TEXT "
                    "REFERENCES user_xiuxian(user_id))"
                )
                uow.execute("INSERT INTO player_data VALUES('u1'),('u2')")
                if migrated:
                    OperationLedger().ensure_schema(uow)
                    OutboxStore().ensure_schema(uow)
                    apply_admin_id_swap_receipts(uow)
                    apply_admin_id_swap_operations(uow)
            elif key == "impart_db":
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u1'),('u2')")
                if migrated:
                    apply_admin_id_swap_receipts(uow)
            elif key == "trade_db":
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u1'),('u2')")
                if migrated:
                    apply_admin_id_swap_receipts(uow)
            else:
                uow.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,partner_id TEXT)"
                )
                uow.execute("INSERT INTO user_xiuxian VALUES('u1','u2'),('u2','u1')")
                if migrated:
                    apply_admin_id_swap_receipts(uow)
    return result


def _repository(root: Path, *, migrated: bool = True, **kwargs) -> AdminIdSwapSqlRepository:
    return AdminIdSwapSqlRepository(
        _databases(root, migrated=migrated), root / "players", **kwargs
    )


def _values(database: Path, sql: str) -> list[tuple]:
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        return connection.execute(sql).fetchall()


def test_id_swap_updates_all_four_databases_and_player_directories(tmp_path: Path) -> None:
    cache_invalidations: list[str] = []
    repository = _repository(
        tmp_path,
        invalidate_user_id_cache=lambda: cache_invalidations.append("cleared"),
    )
    players = tmp_path / "players"
    (players / "u1").mkdir(parents=True)
    (players / "u2").mkdir()
    (players / "u1" / "marker").write_text("first", encoding="utf-8")
    (players / "u2" / "marker").write_text("second", encoding="utf-8")

    result = repository.swap("swap-ok", "u1", "u2")

    assert result.status == "applied"
    assert result.data["updated_cells"] == 18
    assert _values(tmp_path / "game_db.db", "SELECT user_id,name FROM user_xiuxian ORDER BY name") == [
        ("u2", "one"), ("u1", "two")
    ]
    assert _values(tmp_path / "game_db.db", "SELECT sect_owner FROM player_data ORDER BY rowid") == [
        ("u2",), ("u1",)
    ]
    for key in ("impart_db", "trade_db"):
        assert _values(tmp_path / f"{key}.db", "SELECT user_id FROM user_xiuxian ORDER BY rowid") == [
            ("u2",), ("u1",)
        ]
    assert _values(tmp_path / "player_db.db", "SELECT user_id,partner_id FROM user_xiuxian ORDER BY rowid") == [
        ("u2", "u1"), ("u1", "u2")
    ]
    assert (players / "u1" / "marker").read_text(encoding="utf-8") == "second"
    assert (players / "u2" / "marker").read_text(encoding="utf-8") == "first"
    assert cache_invalidations == ["cleared"]

    replay = repository.swap("swap-ok", "u1", "u2")
    assert replay.replayed
    assert replay.status == "replayed"
    assert cache_invalidations == ["cleared"]
    conflict = repository.swap("swap-ok", "u2", "u1")
    assert conflict.code == "operation_payload_conflict"
    assert _values(tmp_path / "game_db.db", "SELECT user_id,name FROM user_xiuxian ORDER BY name") == [
        ("u2", "one"), ("u1", "two")
    ]


class _FailOnceAfterImpart(AdminIdSwapSqlRepository):
    failed = False

    def _after_database_step(self, database_key: str) -> None:
        if database_key == "impart_db" and not self.failed:
            self.failed = True
            raise OSError("injected crash after impart commit")


def test_committed_database_step_is_receipted_and_retry_recovers(tmp_path: Path) -> None:
    databases = _databases(tmp_path)
    repository = _FailOnceAfterImpart(databases, tmp_path / "players")

    failed = repository.swap("swap-retry", "u1", "u2")
    assert failed.status == "failed"
    assert failed.code == "needs_reconcile"
    assert _values(databases["game_db"], "SELECT user_id FROM user_xiuxian ORDER BY name") == [
        ("u2",), ("u1",)
    ]
    assert _values(databases["impart_db"], "SELECT user_id FROM user_xiuxian ORDER BY rowid") == [
        ("u2",), ("u1",)
    ]
    assert _values(databases["trade_db"], "SELECT user_id FROM user_xiuxian ORDER BY rowid") == [
        ("u1",), ("u2",)
    ]
    assert _values(databases["game_db"], "SELECT status FROM admin_id_swap_operations") == [
        ("needs_reconcile",)
    ]

    recovered = repository.reconcile_pending()

    assert recovered == {"recovered": 1, "pending": 0, "failed": 0}
    assert _values(databases["impart_db"], "SELECT user_id FROM user_xiuxian ORDER BY rowid") == [
        ("u2",), ("u1",)
    ]
    receipt_counts = [
        _values(databases[key], "SELECT COUNT(*) FROM admin_id_swap_step_receipts")[0][0]
        for key in DATABASE_ORDER
    ]
    assert receipt_counts == [1, 1, 1, 1]
    assert repository.swap("swap-retry", "u1", "u2").replayed


class _FailOnceDirectoryRename(AdminIdSwapSqlRepository):
    failed = False

    def _rename_directory(self, source: Path, target: Path) -> None:
        if not self.failed:
            self.failed = True
            raise OSError("injected directory interruption")
        super()._rename_directory(source, target)


class _FailOnceAfterDirectoryRename(AdminIdSwapSqlRepository):
    failed = False

    def _rename_directory(self, source: Path, target: Path) -> None:
        super()._rename_directory(source, target)
        if not self.failed:
            self.failed = True
            raise OSError("injected interruption after directory rename")


def test_directory_failure_keeps_pending_recovery_and_resumes(tmp_path: Path) -> None:
    databases = _databases(tmp_path)
    players = tmp_path / "players"
    (players / "u1").mkdir(parents=True)
    (players / "u2").mkdir()
    (players / "u1" / "marker").write_text("first", encoding="utf-8")
    (players / "u2" / "marker").write_text("second", encoding="utf-8")
    repository = _FailOnceDirectoryRename(databases, players)

    failed = repository.swap("swap-dir", "u1", "u2")

    assert failed.code == "needs_reconcile"
    assert _values(databases["game_db"], "SELECT directory_phase FROM admin_id_swap_operations") == [
        ("pending",)
    ]
    report = repository.reconcile_pending()
    assert report == {"recovered": 1, "pending": 0, "failed": 0}
    assert (players / "u1" / "marker").read_text(encoding="utf-8") == "second"
    assert (players / "u2" / "marker").read_text(encoding="utf-8") == "first"
    temp_id = _values(databases["game_db"], "SELECT temp_id FROM admin_id_swap_operations")[0][0]
    assert not (players / temp_id).exists()
    assert (players / "u1").is_dir()
    assert (players / "u2").is_dir()
    assert repository.swap("swap-dir", "u1", "u2").replayed


def test_directory_rename_before_phase_commit_is_detected_and_resumed(tmp_path: Path) -> None:
    databases = _databases(tmp_path)
    players = tmp_path / "players"
    (players / "u1").mkdir(parents=True)
    (players / "u2").mkdir()
    (players / "u1" / "marker").write_text("first", encoding="utf-8")
    (players / "u2" / "marker").write_text("second", encoding="utf-8")
    repository = _FailOnceAfterDirectoryRename(databases, players)

    failed = repository.swap("swap-dir-after-rename", "u1", "u2")

    assert failed.code == "needs_reconcile"
    assert _values(databases["game_db"], "SELECT directory_phase FROM admin_id_swap_operations") == [
        ("pending",)
    ]
    report = repository.reconcile_pending()

    assert report == {"recovered": 1, "pending": 0, "failed": 0}
    assert _values(databases["game_db"], "SELECT directory_phase FROM admin_id_swap_operations") == [
        ("completed",)
    ]
    assert (players / "u1" / "marker").read_text(encoding="utf-8") == "second"
    assert (players / "u2" / "marker").read_text(encoding="utf-8") == "first"
    temp_id = _values(databases["game_db"], "SELECT temp_id FROM admin_id_swap_operations")[0][0]
    assert not (players / temp_id).exists()


@pytest.mark.parametrize(
    ("id1", "id2", "expected_code"),
    [("", "u2", "rejected"), ("u1", "u1", "rejected"), ("../u1", "u2", "rejected")],
)
def test_invalid_input_is_rejected_without_state_changes(
    tmp_path: Path, id1: str, id2: str, expected_code: str
) -> None:
    repository = _repository(tmp_path)

    result = repository.swap(f"reject-{id1}-{id2}", id1, id2)

    assert result.status == "rejected"
    assert result.code == expected_code
    assert _values(tmp_path / "game_db.db", "SELECT user_id FROM user_xiuxian ORDER BY name") == [
        ("u1",), ("u2",)
    ]


def test_missing_startup_migrations_fail_closed_without_request_ddl(tmp_path: Path) -> None:
    databases = _databases(tmp_path, migrated=False)
    repository = AdminIdSwapSqlRepository(databases, tmp_path / "players")

    result = repository.swap("swap-unmigrated", "u1", "u2")

    assert result.code == "schema_missing"
    assert _values(
        databases["game_db"],
        "SELECT name FROM sqlite_master WHERE type='table' AND name='admin_id_swap_operations'",
    ) == []


def test_insufficient_wal_capacity_defers_swap_and_recovery(tmp_path: Path, monkeypatch) -> None:
    databases = _databases(tmp_path)
    repository = AdminIdSwapSqlRepository(databases, tmp_path / "players")

    def reject_capacity(_requirements, *, operation):
        raise InsufficientBackupSpace(f"not enough space for {operation}")

    monkeypatch.setattr(id_swap_repository_module, "preflight_capacity", reject_capacity)

    deferred = repository.swap("swap-capacity", "u1", "u2")

    assert deferred.code == "insufficient_storage"
    assert _values(databases["game_db"], "SELECT user_id FROM user_xiuxian ORDER BY name") == [
        ("u1",), ("u2",)
    ]
    assert _values(databases["game_db"], "SELECT status FROM admin_id_swap_operations") == [
        ("started",)
    ]
    assert repository.reconcile_pending() == {"recovered": 0, "pending": 1, "failed": 1}
    assert _values(databases["impart_db"], "SELECT COUNT(*) FROM admin_id_swap_step_receipts") == [
        (0,)
    ]

    monkeypatch.setattr(id_swap_repository_module, "preflight_capacity", lambda *_args, **_kwargs: None)
    resumed = repository.swap("swap-capacity", "u1", "u2")

    assert resumed.status == "applied"
    assert _values(databases["game_db"], "SELECT user_id FROM user_xiuxian ORDER BY name") == [
        ("u2",), ("u1",)
    ]


def test_database_step_failure_rolls_back_that_database(tmp_path: Path) -> None:
    databases = _databases(tmp_path)
    repository = AdminIdSwapSqlRepository(databases, tmp_path / "players")
    original = repository._apply_database_step

    def fail_on_trade(database_key, *args):
        if database_key == "trade_db":
            with DatabaseUnitOfWork(databases[database_key], immediate=True) as uow:
                uow.execute("UPDATE user_xiuxian SET user_id='broken' WHERE user_id='u1'")
                raise OSError("injected SQL failure")
        return original(database_key, *args)

    repository._apply_database_step = fail_on_trade
    result = repository.swap("swap-db-failure", "u1", "u2")

    assert result.code == "needs_reconcile"
    assert _values(databases["trade_db"], "SELECT user_id FROM user_xiuxian ORDER BY rowid") == [
        ("u1",), ("u2",)
    ]


def test_id_swap_migrations_route_to_exact_four_databases() -> None:
    migrations = build_migrations()
    for key in DATABASE_ORDER:
        versions = {migration.version for migration in migrations_for_database(migrations, key)}
        assert "legacy.admin.003" in versions
    for key in ("player_db", "trade_db", "impart_db"):
        versions = {migration.version for migration in migrations_for_database(migrations, key)}
        assert "legacy.admin.004" not in versions
    game_versions = {
        migration.version for migration in migrations_for_database(migrations, "game_db")
    }
    assert "legacy.admin.004" in game_versions
