from __future__ import annotations

import re
from pathlib import Path
from typing import Callable, Protocol

from .restore_repository import (
    InvalidPluginBackupArchive,
    PluginBackupArchiveNotFound,
    PluginBackupRestoreRepository,
)


_BACKUP_VERSION_RE = re.compile(r"backup_.*_(v?[\d.]+)\.zip\Z")


class PluginBackupRestoreRuntime(Protocol):
    def plugin_backup_sqlite_database_names(self) -> list[str]: ...

    def restore_plugin_backup_database(
        self, source: Path, target: Path, database_name: str
    ) -> None: ...

    def after_plugin_backup_restore(self, database_names: list[str]) -> None: ...


class PluginBackupRestoreApplication:
    def __init__(
        self,
        repository: PluginBackupRestoreRepository,
        runtime: PluginBackupRestoreRuntime,
        version_file: str | Path,
    ) -> None:
        self._repository = repository
        self._runtime = runtime
        self._version_file = Path(version_file)

    def local_backup_exists(self, filename: str) -> bool:
        try:
            return self._repository.backup_exists(filename)
        except OSError as exc:
            raise InvalidPluginBackupArchive(f"无法读取备份文件: {exc}") from exc

    def restore_backup(self, filename: str) -> tuple[bool, str]:
        try:
            database_names = set(self._runtime.plugin_backup_sqlite_database_names())
            restored_databases: list[str] = []
            with self._repository.stage_backup(filename, database_names) as staged:
                if staged.data_root is not None:
                    restored_databases = self._repository.merge_data(
                        staged.data_root,
                        database_names,
                        self._runtime.restore_plugin_backup_database,
                    )
                if staged.plugin_root is not None:
                    self._repository.merge_plugin(staged.plugin_root)

            restored_databases = list(dict.fromkeys(restored_databases))
            if restored_databases:
                self._runtime.after_plugin_backup_restore(restored_databases)

            version_match = _BACKUP_VERSION_RE.fullmatch(str(filename))
            if version_match:
                self._repository.write_version(self._version_file, version_match.group(1))
            return True, f"成功从备份 {filename} 恢复"
        except PluginBackupArchiveNotFound:
            return False, f"备份文件不存在: {filename}"
        except (InvalidPluginBackupArchive, OSError, RuntimeError, ValueError) as exc:
            return False, f"恢复备份失败: {exc}"
        except Exception as exc:
            return False, f"恢复备份失败: {exc}"


class PluginBackupRestoreRuntimeAdapter:
    def __init__(
        self,
        database_names: Callable[[], list[str]],
        restore_database: Callable[[Path, Path, str], None],
        after_restore: Callable[[list[str]], None],
    ) -> None:
        self._database_names = database_names
        self._restore_database = restore_database
        self._after_restore = after_restore

    def plugin_backup_sqlite_database_names(self) -> list[str]:
        return self._database_names()

    def restore_plugin_backup_database(
        self, source: Path, target: Path, database_name: str
    ) -> None:
        self._restore_database(source, target, database_name)

    def after_plugin_backup_restore(self, database_names: list[str]) -> None:
        self._after_restore(database_names)


__all__ = [
    "PluginBackupRestoreApplication",
    "PluginBackupRestoreRuntime",
    "PluginBackupRestoreRuntimeAdapter",
]
