"""Storage contract for configuration backups.

The owner keeps JSON snapshots under ``data/backups/config_backups`` and mirrors
them through WebDAV when cloud backups are enabled.  Size caps, the entry cap of
a remote listing and the accepted filename shape are the whole durable contract,
so they live here once and the repository imports them back.
"""

from __future__ import annotations

from pathlib import Path

MAX_CONFIG_BACKUP_BYTES = 16 * 1024 * 1024
MAX_CONFIG_CLOUD_LIST_BYTES = 2 * 1024 * 1024
MAX_CONFIG_CLOUD_LIST_ENTRIES = 1_000
CONFIG_BACKUP_PREFIX = "config_backup_"
CONFIG_BACKUP_SUFFIX = ".json"


def is_config_backup_filename(value: object) -> bool:
    if not isinstance(value, str) or len(value.encode("utf-8")) > 255:
        return False
    return bool(
        value
        and value not in {".", ".."}
        and "/" not in value
        and "\\" not in value
        and "\x00" not in value
        and Path(value).name == value
        and value.lower().endswith(CONFIG_BACKUP_SUFFIX)
    )


__all__ = [
    "CONFIG_BACKUP_PREFIX",
    "CONFIG_BACKUP_SUFFIX",
    "MAX_CONFIG_BACKUP_BYTES",
    "MAX_CONFIG_CLOUD_LIST_BYTES",
    "MAX_CONFIG_CLOUD_LIST_ENTRIES",
    "is_config_backup_filename",
]
