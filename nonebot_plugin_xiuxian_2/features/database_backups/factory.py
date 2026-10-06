from __future__ import annotations

from typing import Protocol

from ...paths import get_paths
from .application import DatabaseBackupApplication
from .repository import DatabaseBackupRepository


class DatabaseBackupProvider(Protocol):
    def database_backup_database_names(self) -> list[str]: ...

    def database_backup_database_path(self, name: str): ...

    def database_backup_snapshot_sqlite(self, source, destination): ...

    def database_backup_validate_sqlite(self, path): ...

    def database_backup_restore_sqlite(self, source, target, name: str) -> None: ...

    def database_backup_after_restore(self, names: list[str]) -> None: ...

    def database_backup_keep_days(self) -> int: ...

    def database_backup_cloud_enabled(self) -> bool: ...

    def database_backup_webdav_paths(self): ...

    def database_backup_webdav_join_url(self, base_url: str, relative_path: str) -> str: ...

    def database_backup_webdav_make_directories(self, base_url: str, relative_path: str, auth): ...

    def database_backup_cleanup_cloud(self): ...

    def database_backup_format_time(self, value: str) -> str: ...


def build_database_backup_application(
    provider: DatabaseBackupProvider,
) -> DatabaseBackupApplication:
    paths = get_paths()
    repository = DatabaseBackupRepository(
        paths.backups / "db_backup", paths.data, provider
    )
    return DatabaseBackupApplication(repository, provider)


__all__ = ["build_database_backup_application"]
