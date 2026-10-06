from __future__ import annotations

import shutil
import sqlite3
import zipfile
from contextlib import contextmanager
from pathlib import Path

import pytest

from nonebot_plugin_xiuxian_2.features.database_backups.application import DatabaseBackupApplication
from nonebot_plugin_xiuxian_2.features.database_backups.repository import (
    DatabaseBackupRepository,
    InvalidDatabaseBackup,
    PartialDatabaseRestoreError,
    StagedDatabaseBackup,
)


class FakeRuntime:
    def __init__(self, data_directory: Path) -> None:
        self.data_directory = data_directory
        self.restored: list[str] = []
        self.after_restore_calls: list[list[str]] = []
        self.fail_restore: str | None = None
        self.snapshot_calls = 0
        self.validation_calls = 0

    def database_backup_database_names(self) -> list[str]:
        return ["xiuxian.db", "player.db"]

    def database_backup_database_path(self, name: str) -> Path:
        return self.data_directory / name

    def database_backup_snapshot_sqlite(
        self, source: Path, destination: Path
    ) -> tuple[bool, str]:
        self.snapshot_calls += 1
        destination.parent.mkdir(parents=True, exist_ok=True)
        source_connection = None
        destination_connection = None
        try:
            source_connection = sqlite3.connect(source)
            destination_connection = sqlite3.connect(destination)
            source_connection.backup(destination_connection)
            result = destination_connection.execute("PRAGMA quick_check").fetchone()
            return (result == ("ok",), "invalid SQLite snapshot")
        except sqlite3.DatabaseError as exc:
            return False, str(exc)
        finally:
            if destination_connection is not None:
                destination_connection.close()
            if source_connection is not None:
                source_connection.close()

    def database_backup_validate_sqlite(self, path: Path) -> tuple[bool, str]:
        self.validation_calls += 1
        try:
            connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            try:
                result = connection.execute("PRAGMA quick_check").fetchone()
                return (result == ("ok",), "invalid SQLite database")
            finally:
                connection.close()
        except sqlite3.DatabaseError as exc:
            return False, str(exc)

    def database_backup_restore_sqlite(
        self, source: Path, target: Path, name: str
    ) -> None:
        if name == self.fail_restore:
            raise RuntimeError("restore failed")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        self.restored.append(name)

    def database_backup_after_restore(self, names: list[str]) -> None:
        self.after_restore_calls.append(list(names))

    def database_backup_keep_days(self) -> int:
        return 10

    def database_backup_cloud_enabled(self) -> bool:
        return False

    def database_backup_webdav_paths(self):
        return False, "WebDAV unavailable", None

    def database_backup_webdav_join_url(self, base_url: str, relative_path: str) -> str:
        return f"{base_url.rstrip('/')}/{relative_path}"

    def database_backup_webdav_make_directories(self, base_url, relative_path, auth):
        return True, "ok"

    def database_backup_cleanup_cloud(self):
        return True, "ok"

    def database_backup_format_time(self, value: str) -> str:
        return value or "未知"


def _create_database(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE sample (value TEXT)")
        connection.execute("INSERT INTO sample VALUES (?)", (value,))


def _repository(tmp_path: Path, runtime: FakeRuntime) -> DatabaseBackupRepository:
    backup_directory = tmp_path / "backups" / "db_backup"
    return DatabaseBackupRepository(backup_directory, runtime.data_directory, runtime)


def test_create_local_backup_contains_only_existing_core_databases(tmp_path: Path) -> None:
    runtime = FakeRuntime(tmp_path / "data")
    _create_database(runtime.database_backup_database_path("xiuxian.db"), "saved")
    repository = _repository(tmp_path, runtime)

    ok, result, names = repository.create_local_backup()

    assert ok is True
    assert names == ["xiuxian.db"]
    with zipfile.ZipFile(result) as archive:
        assert archive.namelist() == ["xiuxian.db"]


def test_restore_stages_validated_database_and_reconnects_once(tmp_path: Path) -> None:
    runtime = FakeRuntime(tmp_path / "data")
    backup_directory = tmp_path / "backups" / "db_backup"
    backup_directory.mkdir(parents=True)
    source = tmp_path / "source.db"
    _create_database(source, "restored")
    archive_path = backup_directory / "db_backup_20261006_010203.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.write(source, "xiuxian.db")

    application = DatabaseBackupApplication(
        DatabaseBackupRepository(backup_directory, runtime.data_directory, runtime),
        runtime,
    )
    ok, message = application.restore_local_backup(
        archive_path.name, ["xiuxian"]
    )

    assert ok is True
    assert "xiuxian.db" in message
    assert runtime.restored == ["xiuxian.db"]
    assert runtime.after_restore_calls == [["xiuxian.db"]]
    assert runtime.validation_calls == 1
    assert runtime.snapshot_calls == 0


def test_restore_rejects_traversal_before_replacing_any_database(tmp_path: Path) -> None:
    runtime = FakeRuntime(tmp_path / "data")
    backup_directory = tmp_path / "backups" / "db_backup"
    backup_directory.mkdir(parents=True)
    archive_path = backup_directory / "db_backup_20261006_010203.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("../outside.db", b"not a database")

    repository = DatabaseBackupRepository(
        backup_directory, runtime.data_directory, runtime
    )
    with pytest.raises(InvalidDatabaseBackup, match="成员路径非法"):
        with repository.stage_restore(archive_path.name, ["xiuxian.db"]):
            pytest.fail("unsafe archive must not be staged")
    assert runtime.restored == []


def test_restore_uses_snapshot_recovery_only_after_read_only_check_fails(
    tmp_path: Path,
) -> None:
    runtime = FakeRuntime(tmp_path / "data")
    original_validate = runtime.database_backup_validate_sqlite

    def fail_once(path: Path):
        if runtime.validation_calls == 0:
            runtime.validation_calls += 1
            return False, "quick_check failed"
        return original_validate(path)

    runtime.database_backup_validate_sqlite = fail_once  # type: ignore[method-assign]
    backup_directory = tmp_path / "backups" / "db_backup"
    backup_directory.mkdir(parents=True)
    source = tmp_path / "source.db"
    _create_database(source, "recoverable")
    archive_path = backup_directory / "db_backup_20261006_010203.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.write(source, "xiuxian.db")
    repository = DatabaseBackupRepository(
        backup_directory, runtime.data_directory, runtime
    )

    with repository.stage_restore(archive_path.name, ["xiuxian.db"]) as staged:
        assert staged.databases["xiuxian.db"].name.endswith(".clean")
    assert runtime.snapshot_calls == 1


def test_failed_restore_reconnects_attempted_databases_once(tmp_path: Path) -> None:
    runtime = FakeRuntime(tmp_path / "data")
    runtime.fail_restore = "player.db"
    repository = _repository(tmp_path, runtime)
    first_source = tmp_path / "one"
    second_source = tmp_path / "two"
    first_source.write_bytes(b"snapshot")
    second_source.write_bytes(b"snapshot")
    staged = StagedDatabaseBackup(
        {"xiuxian.db": first_source, "player.db": second_source}, ()
    )

    with pytest.raises(PartialDatabaseRestoreError) as error:
        repository.restore_staged(staged)

    assert error.value.restored == ("xiuxian.db",)
    assert runtime.after_restore_calls == [["xiuxian.db", "player.db"]]


def test_cloud_restore_keeps_local_fallback_after_download_failure(tmp_path: Path) -> None:
    runtime = FakeRuntime(tmp_path / "data")

    class Repository:
        calls: list[object] = []

        def download_cloud_backup(self, filename, *, overwrite):
            self.calls.append(("download", filename, overwrite))
            return False, "remote unavailable"

        def local_backup_exists(self, filename):
            self.calls.append(("exists", filename))
            return True

        @contextmanager
        def stage_restore(self, filename, selected):
            self.calls.append(("stage", filename, selected))
            yield StagedDatabaseBackup({}, ())

        def restore_staged(self, staged):
            self.calls.append(("restore",))
            return [], list(staged.skipped)

    repository = Repository()
    application = DatabaseBackupApplication(repository, runtime)  # type: ignore[arg-type]

    ok, message = application.restore_cloud_backup(
        "db_backup_20261006_010203.zip", ["player"]
    )

    assert ok is True
    assert message == "恢复完成，已恢复: []"
    assert repository.calls == [
        ("download", "db_backup_20261006_010203.zip", True),
        ("exists", "db_backup_20261006_010203.zip"),
        ("stage", "db_backup_20261006_010203.zip", ["player.db"]),
        ("restore",),
    ]


def test_cloud_restore_rejects_empty_selection_before_download(tmp_path: Path) -> None:
    runtime = FakeRuntime(tmp_path / "data")

    class Repository:
        called = False

        def download_cloud_backup(self, filename, *, overwrite):
            self.called = True
            return True, "downloaded"

    repository = Repository()
    application = DatabaseBackupApplication(repository, runtime)  # type: ignore[arg-type]

    ok, message = application.restore_cloud_backup(
        "db_backup_20261006_010203.zip", ["unknown"]
    )

    assert ok is False
    assert message == "至少选择一个数据库进行恢复"
    assert repository.called is False


def test_batch_delete_bounds_work_and_rejects_other_zip_files(tmp_path: Path) -> None:
    runtime = FakeRuntime(tmp_path / "data")

    class Repository:
        deleted: list[str] = []

        def delete_local_backup(self, filename):
            self.deleted.append(filename)
            return True, "deleted"

    repository = Repository()
    application = DatabaseBackupApplication(repository, runtime)  # type: ignore[arg-type]
    filenames = [f"db_backup_20261006_0102{index:02d}.zip" for index in range(101)]
    filenames.insert(0, "plugin_backup.zip")

    deleted, failed = application.delete_local_backups(filenames)

    assert len(repository.deleted) == 99
    assert deleted == repository.deleted
    assert failed[0] == {"filename": "plugin_backup.zip", "reason": "无效文件名"}
    assert failed[-1]["reason"] == "单次最多删除 100 个文件"
