from __future__ import annotations

from typing import Protocol

from ...paths import get_paths
from .cloud_application import PluginBackupCloudApplication
from .cloud_repository import PluginBackupCloudRepository


class PluginBackupCloudProvider(Protocol):
    def plugin_backup_webdav_paths(self): ...

    def plugin_backup_webdav_join_url(self, base_url: str, relative_path: str) -> str: ...

    def plugin_backup_webdav_format_time(self, value: str) -> str: ...


def build_plugin_backup_cloud_application(
    provider: PluginBackupCloudProvider,
) -> PluginBackupCloudApplication:
    repository = PluginBackupCloudRepository(get_paths().backups, provider)
    return PluginBackupCloudApplication(repository)


__all__ = ["build_plugin_backup_cloud_application"]
