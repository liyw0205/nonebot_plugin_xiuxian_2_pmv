"""Persistence ports of the manual-backup orchestrator.

A manual backup writes nothing itself: it asks the plugin-backup creation owner
and the configuration-backup owner to produce their artifacts and then triggers
one shared cloud cleanup.  Those two delegates are the whole repository boundary
of this slice, so they are declared here as protocols and the application no
longer imports a concrete sibling feature.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from ..plugin_backups.creation_application import PluginBackupCreationResult


class PluginBackupCreationPort(Protocol):
    def create_backup_with_details(
        self, *, defer_cloud_cleanup: bool = False
    ) -> PluginBackupCreationResult: ...

    def cleanup_cloud_backups(self) -> None: ...


class ConfigBackupProvider(Protocol):
    def backup_all_configs_with_details(
        self, *, defer_cloud_cleanup: bool = False
    ) -> tuple[bool, Path | str, bool]: ...


__all__ = ["ConfigBackupProvider", "PluginBackupCreationPort"]
