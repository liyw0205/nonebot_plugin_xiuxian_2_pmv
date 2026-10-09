"""Backup-name, alias and size contract for database backups.

Snapshots are zip archives under ``data/backups/db_backup``; the restore side may
only touch the four catalogued SQLite files and must stay inside the caps below.
These values used to be split between the application and the repository, so a
limit change could be applied to one side only.
"""

from __future__ import annotations

import re

DATABASE_ALIASES = {
    "xiuxian": "xiuxian.db",
    "xiuxian.db": "xiuxian.db",
    "xiuxian_impart": "xiuxian_impart.db",
    "xiuxian_impart.db": "xiuxian_impart.db",
    "player": "player.db",
    "player.db": "player.db",
    "trade": "trade.db",
    "trade.db": "trade.db",
}
MAX_DATABASE_BACKUP_BATCH = 100
MAX_DATABASE_BACKUP_CLOUD_LIST_BYTES = 2 * 1024 * 1024
MAX_DATABASE_BACKUP_CLOUD_LIST_ENTRIES = 1_000
MAX_DATABASE_BACKUP_DOWNLOAD_BYTES = 8 * 1024 * 1024 * 1024
MAX_DATABASE_RESTORE_MEMBERS = 256
MAX_DATABASE_RESTORE_BYTES = 16 * 1024 * 1024 * 1024
DATABASE_BACKUP_PREFIX = "db_backup_"
DATABASE_BACKUP_SUFFIX = ".zip"
DATABASE_BACKUP_ARCHIVE_PATTERN = re.compile(
    rf"{re.escape(DATABASE_BACKUP_PREFIX)}.+{re.escape(DATABASE_BACKUP_SUFFIX)}\Z",
    re.IGNORECASE,
)

__all__ = [
    "DATABASE_ALIASES",
    "DATABASE_BACKUP_ARCHIVE_PATTERN",
    "DATABASE_BACKUP_PREFIX",
    "DATABASE_BACKUP_SUFFIX",
    "MAX_DATABASE_BACKUP_BATCH",
    "MAX_DATABASE_BACKUP_CLOUD_LIST_BYTES",
    "MAX_DATABASE_BACKUP_CLOUD_LIST_ENTRIES",
    "MAX_DATABASE_BACKUP_DOWNLOAD_BYTES",
    "MAX_DATABASE_RESTORE_BYTES",
    "MAX_DATABASE_RESTORE_MEMBERS",
]
