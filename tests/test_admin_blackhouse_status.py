from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import tests  # Establish isolated paths before importing plugin modules.
import pytest

from nonebot_plugin_xiuxian_2.features.admin.application import AdminApplication
from nonebot_plugin_xiuxian_2.features.admin.blackhouse_repository import (
    AdminBlackhouseSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.admin.migrations import apply_admin_blackhouse
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


def prepare_database(database: Path) -> None:
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(
            "CREATE TABLE user_xiuxian("
            "user_id TEXT PRIMARY KEY,user_name TEXT,is_ban INTEGER)"
        )
        uow.executemany(
            "INSERT INTO user_xiuxian VALUES(?,?,?)",
            (("free", "Free", 0), ("banned", "Banned", 1)),
        )
        apply_admin_blackhouse(uow)


@pytest.fixture
def repository(tmp_path):
    database = tmp_path / "game.db"
    prepare_database(database)
    return AdminBlackhouseSqlRepository(database)


def _player_banned(repository, user_id):
    with DatabaseUnitOfWork(repository.database, read_only=True) as uow:
        row = uow.query_one(
            "SELECT is_ban FROM user_xiuxian WHERE user_id=?", (user_id,)
        )
        return bool(row["is_ban"]) if row else None


def _receipt_count(repository):
    with DatabaseUnitOfWork(repository.database, read_only=True) as uow:
        return uow.query_one(
            "SELECT COUNT(*) AS count FROM admin_blackhouse_status_operations"
        )["count"]


def test_default_application_uses_feature_repository(repository):
    application = AdminApplication(repository.database)
    assert not application.blackhouse_snapshot("free")
    assert application.is_user_blackhoused("banned")
    result = application.set_blackhouse_status(
        "app-ban", "admin", "free", False, True, name="Free", reason="test"
    )
    assert result.succeeded
    assert repository.is_banned("free")
    assert application.list_blackhoused_users() == repository.list_banned()


def test_ban_and_unban_are_atomic_and_idempotent(repository):
    banned = repository.set_banned("ban", "admin", "free", False, True)
    duplicate = repository.set_banned("ban", "admin", "free", True, True)
    unbanned = repository.set_banned("unban", "admin", "free", True, False)

    assert (banned.status, duplicate.status, unbanned.status) == (
        "changed", "duplicate", "changed"
    )
    assert banned.changed and unbanned.changed
    assert duplicate.changed == banned.changed
    assert banned.previous_banned is False and banned.final_banned is True
    assert unbanned.previous_banned is True and unbanned.final_banned is False
    assert not _player_banned(repository, "free")
    assert not repository.snapshot("free")
    assert "free" not in {row["user_id"] for row in repository.list_banned()}
    assert _receipt_count(repository) == 2


@pytest.mark.parametrize("initially_banned", [False, True])
def test_old_receipt_replay_never_reverses_a_later_unban(repository, initially_banned):
    user_id = "banned" if initially_banned else "free"
    first = repository.set_banned("already", "admin", user_id, initially_banned, True)
    assert first.status == ("unchanged" if initially_banned else "changed")
    repository.set_banned("reverse", "admin", user_id, True, False)
    duplicate = repository.set_banned("already", "admin", user_id, False, True)

    assert duplicate.status == "duplicate"
    assert duplicate.changed == first.changed
    assert not repository.snapshot(user_id)
    assert not _player_banned(repository, user_id)
    assert user_id not in {row["user_id"] for row in repository.list_banned()}
    assert _receipt_count(repository) == 2


def test_state_and_operation_conflicts_are_rejected(repository):
    stale = repository.set_banned("stale", "admin", "free", True, True)
    assert stale.status == "state_changed" and not stale.succeeded
    assert _receipt_count(repository) == 0
    assert not repository.snapshot("free")

    assert repository.set_banned("conflict", "admin", "free", False, True).succeeded
    for operator, user_id, banned in (
        ("admin", "free", False),
        ("other-admin", "free", True),
        ("admin", "banned", True),
    ):
        conflict = repository.set_banned("conflict", operator, user_id, True, banned)
        assert conflict.status == "operation_conflict" and not conflict.succeeded
    assert repository.snapshot("free")
    assert _player_banned(repository, "free")
    assert _receipt_count(repository) == 1


def test_unregistered_users_share_the_same_persisted_roster(repository):
    assert repository.snapshot("guest") is False
    result = repository.set_banned(
        "guest-ban", "admin", "guest", False, True, name="Guest", reason="abuse"
    )
    assert result.status == "changed" and result.succeeded
    assert _player_banned(repository, "guest") is None

    restarted = AdminBlackhouseSqlRepository(repository.database)
    assert restarted.snapshot("guest") and restarted.is_banned("guest")
    row = next(row for row in restarted.list_banned() if row["user_id"] == "guest")
    assert (row["name"], row["reason"]) == ("Guest", "abuse")
    assert restarted.set_banned("guest-unban", "admin", "guest", True, False).succeeded
    assert not repository.is_banned("guest")
    assert _player_banned(repository, "guest") is None


@pytest.mark.parametrize("user_id", ["free", "guest"])
def test_receipt_failure_rolls_back_roster_and_player_projection(repository, user_id):
    original = repository.list_banned()
    with DatabaseUnitOfWork(repository.database) as uow:
        uow.execute(
            "CREATE TRIGGER fail_blackhouse_operation BEFORE INSERT ON "
            "admin_blackhouse_status_operations "
            "BEGIN SELECT RAISE(ABORT,'failed'); END"
        )
    with pytest.raises(sqlite3.IntegrityError):
        repository.set_banned("failed", "admin", user_id, False, True)
    assert repository.list_banned() == original
    assert not repository.is_banned(user_id)
    assert _player_banned(repository, user_id) == (False if user_id == "free" else None)
    assert _receipt_count(repository) == 0


@pytest.mark.parametrize("existing", [False, True])
def test_missing_schema_is_rejected_without_creating_database_or_tables(tmp_path, existing):
    database = tmp_path / "missing.db"
    if existing:
        with DatabaseUnitOfWork(database) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,is_ban INTEGER)")
    repository = AdminBlackhouseSqlRepository(database)
    for read in (lambda: repository.snapshot("free"),
                 lambda: repository.is_banned("free"), repository.list_banned):
        with pytest.raises(RuntimeError, match="schema_missing"):
            read()
    result = repository.set_banned("missing", "admin", "free", False, True)
    assert result.status == "schema_missing" and not result.succeeded
    assert database.exists() == existing
    if existing:
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            tables = uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        assert tables == [{"name": "user_xiuxian"}]


def test_runtime_reads_and_writes_never_issue_schema_ddl(repository):
    original_execute = DatabaseUnitOfWork.execute

    def execute(uow, sql, params=()):
        assert sql.lstrip().split()[0].upper() not in {"CREATE", "ALTER", "DROP"}
        return original_execute(uow, sql, params)

    with patch.object(DatabaseUnitOfWork, "execute", execute):
        assert not repository.snapshot("free")
        assert repository.is_banned("banned")
        assert repository.list_banned()
        assert repository.set_banned("no-ddl", "admin", "free", False, True).succeeded


def test_parallel_repository_instances_record_one_effect(repository):
    def ban(_):
        return AdminBlackhouseSqlRepository(repository.database).set_banned(
            "parallel", "admin", "free", False, True
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(ban, range(4)))
    assert [result.status for result in results].count("changed") == 1
    assert [result.status for result in results].count("duplicate") == 3
    assert repository.is_banned("free") and _player_banned(repository, "free")
    assert _receipt_count(repository) == 1
