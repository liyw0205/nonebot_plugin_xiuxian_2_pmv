from __future__ import annotations

import ast
from collections import Counter
import inspect
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from ..application import TitleGrantTargetApplication
from ..repository import MAX_TITLE_GRANT_USER_ID_BYTES, TitleGrantTargetSqlRepository
from ....infrastructure.database.backup_capacity import (
    InsufficientBackupSpace,
    backup_reserve_bytes,
)
from ....infrastructure.database.uow import DatabaseUnitOfWork


def _game_database(path: Path, user_ids: list[str]) -> None:
    with DatabaseUnitOfWork(path, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT NOT NULL)")
        uow.executemany(
            "INSERT INTO user_xiuxian(user_id) VALUES (?)",
            [(user_id,) for user_id in user_ids],
        )


def test_snapshot_preserves_all_rows_and_duplicate_ids_without_database_mutation(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _game_database(database, ["11", "22", "11", "雪"])
    app = TitleGrantTargetApplication(database, temp_directory=tmp_path)

    snapshot = app.snapshot()
    try:
        ids = list(snapshot.iter_user_ids())
        assert snapshot.count == 4
        assert Counter(ids) == Counter({"11": 2, "22": 1, "雪": 1})
        assert list(snapshot.iter_user_ids()) == ids
    finally:
        snapshot.close()

    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()[0] == 4
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
        ).fetchone()[0] == 1


def test_empty_roster_does_not_require_disk_reserve(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _game_database(database, [])
    app = TitleGrantTargetApplication(database, temp_directory=tmp_path)

    with patch(
        "nonebot_plugin_xiuxian_2.features.title.repository.preflight_capacity"
    ) as preflight:
        snapshot = app.snapshot()

    try:
        assert snapshot.count == 0
        assert list(snapshot.iter_user_ids()) == []
        preflight.assert_not_called()
    finally:
        snapshot.close()


def test_missing_database_or_roster_fails_without_creating_schema(tmp_path: Path) -> None:
    missing_database = tmp_path / "missing.db"
    with pytest.raises(sqlite3.OperationalError):
        TitleGrantTargetApplication(missing_database, temp_directory=tmp_path).snapshot()
    assert not missing_database.exists()

    empty_database = tmp_path / "empty.db"
    with sqlite3.connect(empty_database):
        pass
    with pytest.raises(sqlite3.OperationalError):
        TitleGrantTargetApplication(empty_database, temp_directory=tmp_path).snapshot()
    with sqlite3.connect(empty_database) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
        ).fetchone()[0] == 0


def test_oversized_user_id_fails_before_returning_a_snapshot(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _game_database(database, ["x" * (MAX_TITLE_GRANT_USER_ID_BYTES * 2)])

    with pytest.raises(ValueError, match="user id exceeds the title grant snapshot limit"):
        TitleGrantTargetApplication(database, temp_directory=tmp_path).snapshot()


def test_capacity_failure_closes_temporary_snapshot_before_any_grant_can_start(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _game_database(database, ["11"])
    created_files = []
    real_temporary_file = tempfile.TemporaryFile

    def create_temporary_file(*args, **kwargs):
        stream = real_temporary_file(dir=tmp_path)
        created_files.append(stream)
        return stream

    with (
        patch(
            "nonebot_plugin_xiuxian_2.features.title.repository.tempfile.TemporaryFile",
            side_effect=create_temporary_file,
        ),
        patch(
            "nonebot_plugin_xiuxian_2.features.title.repository.preflight_capacity",
            side_effect=InsufficientBackupSpace("not enough temporary storage"),
        ),
        pytest.raises(InsufficientBackupSpace),
    ):
        TitleGrantTargetApplication(database, temp_directory=tmp_path).snapshot()

    assert len(created_files) == 1
    assert created_files[0].closed


def test_failed_roster_query_closes_temporary_snapshot(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    with sqlite3.connect(database):
        pass
    created_files = []
    real_temporary_file = tempfile.TemporaryFile

    def create_temporary_file(*args, **kwargs):
        stream = real_temporary_file(dir=tmp_path)
        created_files.append(stream)
        return stream

    with (
        patch(
            "nonebot_plugin_xiuxian_2.features.title.repository.tempfile.TemporaryFile",
            side_effect=create_temporary_file,
        ),
        pytest.raises(sqlite3.OperationalError),
    ):
        TitleGrantTargetApplication(database, temp_directory=tmp_path).snapshot()

    assert len(created_files) == 1
    assert created_files[0].closed


def test_incremental_capacity_check_accounts_for_current_spool_and_preserves_reserve(
    tmp_path: Path,
) -> None:
    repository = TitleGrantTargetSqlRepository(temp_directory=tmp_path)
    written_bytes = 1024**3
    next_record_bytes = 4096

    with patch(
        "nonebot_plugin_xiuxian_2.features.title.repository.preflight_capacity"
    ) as preflight:
        repository._preflight(next_record_bytes, cumulative_bytes=written_bytes)

    requirements = preflight.call_args.args[0]
    kwargs = preflight.call_args.kwargs
    assert requirements == {tmp_path: next_record_bytes}
    assert kwargs["additional_reserve_bytes"] == (
        backup_reserve_bytes(written_bytes + next_record_bytes)
        - backup_reserve_bytes(next_record_bytes)
    )


def test_default_all_grant_uses_snapshot_without_legacy_full_list_or_reordering() -> None:
    package_root = Path(__file__).resolve().parents[4] / "nonebot_plugin_xiuxian_2"
    source_path = package_root / "xiuxian" / "xiuxian_title" / "__init__.py"
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(source_path))
    handler = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "title_grant_"
    )
    handler_source = ast.get_source_segment(source, handler) or ""
    query_source = inspect.getsource(TitleGrantTargetSqlRepository.snapshot_user_ids)

    assert "_sql_message().get_all_user_id()" not in handler_source
    assert "title_grant_target_application.snapshot" in source
    assert "snapshot.iter_user_ids()" in handler_source
    assert "snapshot.close()" in handler_source
    assert "substr(user_id, 1, ?) AS user_id FROM user_xiuxian" in query_source
    assert "MAX_TITLE_GRANT_USER_ID_BYTES + 1" in query_source
    assert "DISTINCT" not in query_source.upper()
    assert "ORDER BY" not in query_source.upper()
    assert "WHERE" not in query_source.upper()
