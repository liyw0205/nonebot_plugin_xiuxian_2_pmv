from __future__ import annotations

import os
import stat
from datetime import datetime
from pathlib import Path
from typing import Any

from .schemas import ARCHIVE_PREFIX, ARCHIVE_SUFFIX


def is_plugin_backup_filename(value: object) -> bool:
    if (
        not isinstance(value, str)
        or not value.startswith(ARCHIVE_PREFIX)
        or not value.endswith(ARCHIVE_SUFFIX)
    ):
        return False
    if "/" in value or "\\" in value or "\x00" in value:
        return False
    return len(Path(value).stem.split("_")) >= 4


class PluginBackupCatalogRepository:
    def __init__(self, backup_directory: str | Path) -> None:
        self._backup_directory = Path(backup_directory)

    def list_plugin_backups(self) -> list[dict[str, Any]]:
        backups: list[dict[str, Any]] = []
        try:
            entries = os.scandir(self._backup_directory)
        except FileNotFoundError:
            return backups

        with entries:
            for entry in entries:
                if not is_plugin_backup_filename(entry.name):
                    continue

                parts = Path(entry.name).stem.split("_")

                try:
                    metadata = entry.stat(follow_symlinks=False)
                except OSError:
                    continue
                if not stat.S_ISREG(metadata.st_mode):
                    continue

                timestamp = f"{parts[1]}_{parts[2]}"
                backups.append(
                    {
                        "filename": entry.name,
                        "timestamp": timestamp,
                        "version": "_".join(parts[3:]),
                        "size": metadata.st_size,
                        "created_at": datetime.fromtimestamp(metadata.st_ctime).isoformat(),
                    }
                )

        backups.sort(key=lambda item: item["created_at"], reverse=True)
        return backups


__all__ = ["PluginBackupCatalogRepository", "is_plugin_backup_filename"]
