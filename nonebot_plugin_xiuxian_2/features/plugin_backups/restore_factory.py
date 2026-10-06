from __future__ import annotations

from typing import Protocol

from ...paths import get_paths
from .restore_application import (
    PluginBackupRestoreApplication,
    PluginBackupRestoreRuntimeAdapter,
)
from .restore_repository import PluginBackupRestoreRepository


class PluginBackupRestoreProvider(Protocol):
    def plugin_backup_sqlite_database_names(self) -> list[str]: ...

    def restore_plugin_backup_database(self, source, target, database_name: str) -> None: ...

    def after_plugin_backup_restore(self, database_names: list[str]) -> None: ...


def build_plugin_backup_restore_application(
    provider: PluginBackupRestoreProvider,
) -> PluginBackupRestoreApplication:
    paths = get_paths()
    repository = PluginBackupRestoreRepository(
        paths.backups,
        paths.data_root,
        paths.package_root,
    )
    runtime = PluginBackupRestoreRuntimeAdapter(
        provider.plugin_backup_sqlite_database_names,
        provider.restore_plugin_backup_database,
        provider.after_plugin_backup_restore,
    )
    return PluginBackupRestoreApplication(
        repository,
        runtime,
        paths.data / "version.txt",
    )


__all__ = ["build_plugin_backup_restore_application"]
