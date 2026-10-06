from __future__ import annotations

from typing import Protocol

from ...paths import get_paths
from .creation_application import (
    PluginBackupCreationApplication,
    PluginBackupCreationRuntime,
)
from .creation_repository import PluginBackupCreationRepository


class PluginBackupCreationProvider(PluginBackupCreationRuntime, Protocol):
    pass


def build_plugin_backup_creation_application(
    provider: PluginBackupCreationProvider,
) -> PluginBackupCreationApplication:
    paths = get_paths()
    package_root = paths.package_root
    repository = PluginBackupCreationRepository(
        paths.backups,
        paths.data,
        package_root,
        package_root.parents[2],
    )
    return PluginBackupCreationApplication(repository, provider)


__all__ = ["PluginBackupCreationProvider", "build_plugin_backup_creation_application"]
