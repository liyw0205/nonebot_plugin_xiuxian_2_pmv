from __future__ import annotations

import sqlite3

import pytest

from ..id_swap_repository import DATABASE_ORDER
from ..qqid_candidate_repository import AdminQqidCandidateRepository, QqidCandidateSchemaError


def databases(root):
    paths = {key: root / f"{key}.db" for key in DATABASE_ORDER}
    for path in paths.values():
        with sqlite3.connect(path) as connection:
            connection.execute('CREATE TABLE "odd""table"(user_id TEXT,sect_owner TEXT,partner_id TEXT,group_id TEXT,main_id TEXT,active_id TEXT,ignored TEXT)')
    return paths


def test_snapshot_uses_shared_database_and_column_scope_without_writes(tmp_path):
    paths = databases(tmp_path)
    for key, path in paths.items():
        with sqlite3.connect(path) as connection:
            connection.execute(
                'INSERT INTO "odd""table" VALUES(?,?,?,?,?,?,?)',
                (" 100 ", "200", "300", "400", "500", "600", "not-an-id"),
            )
            connection.execute('INSERT INTO "odd""table"(user_id) VALUES(NULL),(\'\'),(\'  \'),(\'100\')')
            if key != "player_db":
                connection.execute('UPDATE "odd""table" SET partner_id=\'not-in-scope\'')
    before = {key: path.read_bytes() for key, path in paths.items()}

    assert AdminQqidCandidateRepository(paths).snapshot() == ("100", "200", "300", "400", "500", "600")
    assert {key: path.read_bytes() for key, path in paths.items()} == before


def test_empty_business_tables_are_valid_empty_snapshot(tmp_path):
    assert AdminQqidCandidateRepository(databases(tmp_path)).snapshot() == ()


def test_missing_database_is_not_created(tmp_path):
    paths = databases(tmp_path)
    paths["trade_db"].unlink()

    with pytest.raises(QqidCandidateSchemaError) as caught:
        AdminQqidCandidateRepository(paths).snapshot()

    assert caught.value.code == "schema_missing"
    assert not paths["trade_db"].exists()


def test_missing_target_schema_is_not_reported_as_empty_success(tmp_path):
    paths = databases(tmp_path)
    with sqlite3.connect(paths["player_db"]) as connection:
        connection.execute('DROP TABLE "odd""table"')
    before = paths["player_db"].read_bytes()

    with pytest.raises(QqidCandidateSchemaError, match="player_db"):
        AdminQqidCandidateRepository(paths).snapshot()

    assert paths["player_db"].read_bytes() == before
