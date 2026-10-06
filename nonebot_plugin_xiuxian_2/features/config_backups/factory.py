from __future__ import annotations

from typing import Protocol

from ...paths import get_paths
from .application import ConfigBackupApplication, ConfigBackupRuntime
from .repository import ConfigBackupRepository, ConfigBackupRuntime as ConfigBackupWebDavRuntime


class ConfigurationBackupProvider(ConfigBackupRuntime, ConfigBackupWebDavRuntime, Protocol):
    pass


def build_config_backup_application(
    provider: ConfigurationBackupProvider,
) -> ConfigBackupApplication:
    repository = ConfigBackupRepository(
        get_paths().backups / "config_backups",
        provider,
    )
    return ConfigBackupApplication(repository, provider)


__all__ = ["ConfigurationBackupProvider", "build_config_backup_application"]
