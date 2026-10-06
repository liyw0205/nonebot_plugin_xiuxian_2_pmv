from __future__ import annotations

import json
from pathlib import Path

import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ..blackhouse_repository import AdminBlackhouseSqlRepository
from ..migrations import apply_admin_blackhouse


def _players(database: Path, *, migrated: bool = False) -> None:
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(
            "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,is_ban INTEGER)"
        )
        uow.execute("INSERT INTO user_xiuxian VALUES('u','Player',0)")
        if migrated:
            apply_admin_blackhouse(uow)


def _migrate(database: Path) -> None:
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        apply_admin_blackhouse(uow)


def test_migration_merges_json_and_sql_once_without_rewriting_json(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _players(database)
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES('sql','SQL player',1)")
    legacy = tmp_path / "blackhouse.json"
    original = json.dumps({"users": {
        "u": {"name": "Legacy player", "reason": "legacy reason", "updated_at": "old"},
        "unregistered": {"name": "Guest", "reason": "guest reason"},
    }}).encode()
    legacy.write_bytes(original)

    _migrate(database)
    repository = AdminBlackhouseSqlRepository(database)
    assert repository.list_banned() == [
        {"user_id": "sql", "name": "SQL player", "reason": "legacy_is_ban"},
        {"user_id": "u", "name": "Legacy player", "reason": "legacy reason"},
        {"user_id": "unregistered", "name": "Guest", "reason": "guest reason"},
    ]
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT is_ban FROM user_xiuxian WHERE user_id='u'")["is_ban"] == 1
    assert legacy.read_bytes() == original

    assert repository.set_banned("unban", "admin", "u", True, False).succeeded
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("UPDATE user_xiuxian SET is_ban=1 WHERE user_id='u'")
    _migrate(database)
    assert repository.snapshot("u") is False
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT is_ban FROM user_xiuxian WHERE user_id='u'")["is_ban"] == 0
        assert uow.query_one("SELECT COUNT(*) AS n FROM admin_blackhouse_imports")["n"] == 1
    assert legacy.read_bytes() == original
    legacy.write_text("not valid JSON", encoding="utf-8")
    _migrate(database)
    assert repository.snapshot("u") is False


def test_migration_accepts_legacy_map_and_membership_only_values(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _players(database)
    (tmp_path / "blackhouse.json").write_text(json.dumps({
        "u": {"name": 123, "reason": False, "updated_at": 42},
        "old-bool": True,
        "old-number": 1,
        "old-string": "legacy",
        "old-null": None,
    }), encoding="utf-8")

    _migrate(database)

    users = {row["user_id"]: row for row in AdminBlackhouseSqlRepository(database).list_banned()}
    assert users["u"] == {"user_id": "u", "name": "123", "reason": ""}
    for user_id in ("old-bool", "old-number", "old-string", "old-null"):
        assert users[user_id] == {"user_id": user_id, "name": user_id, "reason": ""}


@pytest.mark.parametrize("payload", [
    b"not-json", b"[]", b'{"users":[]}', b'{"users":{" ":{}}}',
    b'{"u":{}," u ":{}}', b'{"u":{},"u":{}}',
    b'{"u":{"name":[]}}', b'{"u":{"reason":{}}}',
])
def test_invalid_legacy_json_aborts_migration_without_partial_state(
    tmp_path: Path, payload: bytes,
) -> None:
    database = tmp_path / "game.db"
    _players(database)
    legacy = tmp_path / "blackhouse.json"
    legacy.write_bytes(payload)

    with pytest.raises(RuntimeError, match="legacy blackhouse"):
        _migrate(database)

    assert legacy.read_bytes() == payload
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_all(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'admin_blackhouse_%'"
        ) == []
        assert uow.query_one("SELECT is_ban FROM user_xiuxian WHERE user_id='u'")["is_ban"] == 0


def test_legacy_json_over_limit_is_rejected_not_truncated(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _players(database)
    legacy = tmp_path / "blackhouse.json"
    legacy.write_bytes(b"{}" + b" " * (16 * 1024 * 1024))

    with pytest.raises(RuntimeError, match="exceeds 16 MiB"):
        _migrate(database)

    assert legacy.stat().st_size == 16 * 1024 * 1024 + 2
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one(
            "SELECT name FROM sqlite_master WHERE name='admin_blackhouse_imports'"
        ) is None


def test_migration_retains_legacy_receipt_and_replay_only_reports_history(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _players(database)
    payload = '["admin","u","ban"]'
    result_json = json.dumps({
        "action": "ban", "previous_banned": False, "final_banned": True, "changed": True,
    }, separators=(",", ":"))
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(
            "CREATE TABLE admin_blackhouse_status_operations(operation_id TEXT PRIMARY KEY,"
            "payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        uow.execute(
            "INSERT INTO admin_blackhouse_status_operations VALUES(?,?,?,'legacy-time')",
            ("old-operation", payload, result_json),
        )
    _migrate(database)
    repository = AdminBlackhouseSqlRepository(database)

    replay = repository.set_banned(
        "old-operation", "admin", "u", False, True, name="Different", reason="New description",
    )

    assert replay.status == "duplicate"
    assert (replay.action, replay.previous_banned, replay.final_banned, replay.changed) == (
        "ban", False, True, True,
    )
    assert repository.is_banned("u") is False
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        receipt = uow.query_one("SELECT * FROM admin_blackhouse_status_operations")
    assert receipt == {
        "operation_id": "old-operation", "payload": payload,
        "result_json": result_json, "created_at": "legacy-time",
    }


def test_feature_tables_can_migrate_before_players_but_runtime_requires_player_schema(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    (tmp_path / "blackhouse.json").write_text('{"unregistered":true}', encoding="utf-8")
    _migrate(database)
    repository = AdminBlackhouseSqlRepository(database)

    assert repository.set_banned("op", "admin", "u", False, True).status == "schema_missing"
    for read in (lambda: repository.snapshot("u"), lambda: repository.is_banned("u"), repository.list_banned):
        with pytest.raises(RuntimeError, match="schema_missing"):
            read()
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT user_id FROM admin_blackhouse_users")["user_id"] == "unregistered"
        assert uow.query_one("SELECT name FROM sqlite_master WHERE name='user_xiuxian'") is None


@pytest.mark.parametrize("definition", ["user_id TEXT PRIMARY KEY", "is_ban INTEGER"])
def test_existing_incomplete_player_schema_is_explicitly_rejected(tmp_path: Path, definition: str) -> None:
    database = tmp_path / "game.db"
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(f"CREATE TABLE user_xiuxian({definition})")

    with pytest.raises(RuntimeError, match="admin blackhouse schema incomplete for user_xiuxian"):
        _migrate(database)

    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT name FROM sqlite_master WHERE name='admin_blackhouse_users'") is None


def test_missing_membership_primary_key_is_rejected_by_migration_and_runtime(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _players(database, migrated=True)
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("DROP TABLE admin_blackhouse_users")
        uow.execute(
            "CREATE TABLE admin_blackhouse_users(user_id TEXT,name TEXT,reason TEXT,updated_at TEXT)"
        )

    with pytest.raises(RuntimeError, match="requires primary key"):
        _migrate(database)
    repository = AdminBlackhouseSqlRepository(database)
    assert repository.set_banned("op", "admin", "u", False, True).status == "schema_missing"
    with pytest.raises(RuntimeError, match="schema_missing"):
        repository.snapshot("u")


def test_ignored_legacy_import_cannot_mark_migration_complete(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _players(database, migrated=True)
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("DELETE FROM admin_blackhouse_imports")
        uow.execute(
            "CREATE TRIGGER ignore_import BEFORE INSERT ON admin_blackhouse_users "
            "BEGIN SELECT RAISE(IGNORE); END"
        )
    (tmp_path / "blackhouse.json").write_text('{"u":true}', encoding="utf-8")

    with pytest.raises(RuntimeError, match="membership import failed"):
        _migrate(database)

    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_all("SELECT * FROM admin_blackhouse_imports") == []
        assert uow.query_all("SELECT * FROM admin_blackhouse_users") == []
        assert uow.query_one("SELECT is_ban FROM user_xiuxian WHERE user_id='u'")["is_ban"] == 0


def test_ignored_legacy_projection_rolls_back_import_and_marker(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _players(database)
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(
            "CREATE TRIGGER ignore_projection BEFORE UPDATE ON user_xiuxian BEGIN SELECT RAISE(IGNORE); END"
        )
    (tmp_path / "blackhouse.json").write_text('{"u":true}', encoding="utf-8")

    with pytest.raises(RuntimeError, match="projection synchronization failed"):
        _migrate(database)

    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT name FROM sqlite_master WHERE name='admin_blackhouse_imports'") is None
        assert uow.query_one("SELECT is_ban FROM user_xiuxian WHERE user_id='u'")["is_ban"] == 0


@pytest.mark.parametrize("marker_change", ["missing_table", "missing_row", "wrong_key", "missing_column", "missing_pk"])
def test_runtime_requires_complete_import_marker(tmp_path: Path, marker_change: str) -> None:
    database = tmp_path / "game.db"
    _players(database, migrated=True)
    with DatabaseUnitOfWork(database) as uow:
        if marker_change == "missing_table":
            uow.execute("DROP TABLE admin_blackhouse_imports")
        elif marker_change == "missing_row":
            uow.execute("DELETE FROM admin_blackhouse_imports")
        elif marker_change == "wrong_key":
            uow.execute("UPDATE admin_blackhouse_imports SET import_key='unknown'")
        elif marker_change == "missing_column":
            uow.execute("ALTER TABLE admin_blackhouse_imports DROP COLUMN imported_at")
        else:
            uow.execute("DROP TABLE admin_blackhouse_imports")
            uow.execute("CREATE TABLE admin_blackhouse_imports(import_key TEXT,imported_at TEXT)")
            uow.execute("INSERT INTO admin_blackhouse_imports VALUES('legacy_json_and_is_ban_v1','old')")
        before = uow.query_all("SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name")
    repository = AdminBlackhouseSqlRepository(database)

    assert repository.set_banned("op", "admin", "u", False, True).status == "schema_missing"
    for read in (lambda: repository.snapshot("u"), lambda: repository.is_banned("u"), repository.list_banned):
        with pytest.raises(RuntimeError, match="schema_missing"):
            read()

    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_all("SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name") == before
        assert uow.query_all("SELECT * FROM admin_blackhouse_users") == []
        assert uow.query_all("SELECT * FROM admin_blackhouse_status_operations") == []
        assert uow.query_one("SELECT is_ban FROM user_xiuxian WHERE user_id='u'")["is_ban"] == 0


def test_ignored_import_marker_rolls_back_membership_and_cannot_report_success(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _players(database, migrated=True)
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("DELETE FROM admin_blackhouse_imports")
        uow.execute(
            "CREATE TRIGGER ignore_marker BEFORE INSERT ON admin_blackhouse_imports "
            "BEGIN SELECT RAISE(IGNORE); END"
        )
    (tmp_path / "blackhouse.json").write_text('{"u":true}', encoding="utf-8")

    with pytest.raises(RuntimeError, match="import marker was not saved"):
        _migrate(database)

    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_all("SELECT * FROM admin_blackhouse_imports") == []
        assert uow.query_all("SELECT * FROM admin_blackhouse_users") == []
        assert uow.query_one("SELECT is_ban FROM user_xiuxian WHERE user_id='u'")["is_ban"] == 0
    repository = AdminBlackhouseSqlRepository(database)
    assert repository.set_banned("op", "admin", "u", False, True).status == "schema_missing"
    with pytest.raises(RuntimeError, match="schema_missing"):
        repository.snapshot("u")

    with DatabaseUnitOfWork(database) as uow:
        uow.execute("DROP TRIGGER ignore_marker")
    _migrate(database)
    assert repository.snapshot("u") is True
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT COUNT(*) AS n FROM admin_blackhouse_imports")["n"] == 1
        assert uow.query_one("SELECT is_ban FROM user_xiuxian WHERE user_id='u'")["is_ban"] == 1


@pytest.mark.parametrize("table,event", [
    ("admin_blackhouse_users", "INSERT"),
    ("user_xiuxian", "UPDATE"),
    ("admin_blackhouse_status_operations", "INSERT"),
])
def test_ignored_mutation_cannot_commit_half_applied_state(tmp_path: Path, table: str, event: str) -> None:
    database = tmp_path / "game.db"
    _players(database, migrated=True)
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(
            f"CREATE TRIGGER ignore_mutation BEFORE {event} ON {table} BEGIN SELECT RAISE(IGNORE); END"
        )

    with pytest.raises(RuntimeError, match="blackhouse"):
        AdminBlackhouseSqlRepository(database).set_banned("op", "admin", "u", False, True)

    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_all("SELECT * FROM admin_blackhouse_users") == []
        assert uow.query_all("SELECT * FROM admin_blackhouse_status_operations") == []
        assert uow.query_one("SELECT is_ban FROM user_xiuxian WHERE user_id='u'")["is_ban"] == 0


@pytest.mark.parametrize("column", ["name", "reason", "updated_at"])
def test_runtime_checks_all_membership_columns_without_request_ddl(tmp_path: Path, column: str) -> None:
    database = tmp_path / "game.db"
    _players(database, migrated=True)
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(f"ALTER TABLE admin_blackhouse_users DROP COLUMN {column}")
    repository = AdminBlackhouseSqlRepository(database)

    assert repository.set_banned("op", "admin", "u", False, True).status == "schema_missing"
    with pytest.raises(RuntimeError, match="schema_missing"):
        repository.snapshot("u")
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert column not in {row["name"] for row in uow.query_all("PRAGMA table_info(admin_blackhouse_users)")}


@pytest.mark.parametrize("changes", [
    {"operation_id": None}, {"operator_id": " "}, {"user_id": False},
    {"banned": "false"}, {"expected_banned": "true"}, {"name": {}}, {"reason": []},
])
def test_invalid_inputs_do_not_create_database(tmp_path: Path, changes: dict) -> None:
    database = tmp_path / "absent" / "game.db"
    arguments = {
        "operation_id": "op", "operator_id": "admin", "user_id": "u",
        "expected_banned": False, "banned": True,
    }
    arguments.update(changes)

    with pytest.raises(ValueError):
        AdminBlackhouseSqlRepository(database).set_banned(**arguments)

    assert not database.parent.exists()
