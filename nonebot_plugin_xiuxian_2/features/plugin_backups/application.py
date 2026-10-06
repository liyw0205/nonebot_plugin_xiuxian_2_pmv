from __future__ import annotations

from typing import Any

from .repository import PluginBackupCatalogRepository


class PluginBackupCatalogApplication:
    def __init__(self, repository: PluginBackupCatalogRepository) -> None:
        self._repository = repository

    def list_plugin_backups(self) -> list[dict[str, Any]]:
        return self._repository.list_plugin_backups()


__all__ = ["PluginBackupCatalogApplication"]
