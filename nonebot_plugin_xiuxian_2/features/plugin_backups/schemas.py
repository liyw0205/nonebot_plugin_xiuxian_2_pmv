"""Archive contract for whole-plugin backups.

A plugin backup is a zip of the package tree written under ``data/backups``; a
restore unpacks it below a fixed archive root.  The caps, the skipped
directories, the transient runtime files and the two filename patterns are the
durable contract of this owner.  ``RESTORE_DISK_RESERVE_BYTES`` used to be
declared twice with the same value in the cloud and restore repositories.
"""

from __future__ import annotations

import re
from pathlib import PurePosixPath

MAX_CLOUD_BACKUP_BATCH = 100
MAX_CLOUD_LIST_BYTES = 2 * 1024 * 1024
MAX_CLOUD_LIST_ENTRIES = 1_000
MAX_PLUGIN_BACKUP_DOWNLOAD_BYTES = 4 * 1024 * 1024 * 1024
RESTORE_DISK_RESERVE_BYTES = 64 * 1024 * 1024
PLUGIN_ARCHIVE_ROOT = PurePosixPath("src/plugins/nonebot_plugin_xiuxian_2")
MAX_ARCHIVE_MEMBERS = 100_000
ARCHIVE_PREFIX = "backup_"
ARCHIVE_SUFFIX = ".zip"
ARCHIVE_NAME_PATTERN = re.compile(
    rf"{re.escape(ARCHIVE_PREFIX)}.*_(v?[\d.]+){re.escape(ARCHIVE_SUFFIX)}\Z"
)
ARCHIVE_TIMESTAMP_PATTERN = re.compile(r"\d{8}_\d{6}")
VERSION_SAFE_PATTERN = re.compile(r"[^A-Za-z0-9._+-]+")
SKIP_DIRECTORY_NAMES = frozenset(
    {
        "backups",
        "config_backups",
        "db_backup",
        "cache",
        "media_parser_cache",
        "boss_img",
        "font",
        "\u5361\u56fe",
        "__pycache__",
    }
)
TRANSIENT_DATA_PATHS = frozenset(
    {
        PurePosixPath("message.db"),
        PurePosixPath("activity/activity.db"),
    }
)

__all__ = [
    "ARCHIVE_NAME_PATTERN",
    "ARCHIVE_PREFIX",
    "ARCHIVE_SUFFIX",
    "ARCHIVE_TIMESTAMP_PATTERN",
    "MAX_ARCHIVE_MEMBERS",
    "MAX_CLOUD_BACKUP_BATCH",
    "MAX_CLOUD_LIST_BYTES",
    "MAX_CLOUD_LIST_ENTRIES",
    "MAX_PLUGIN_BACKUP_DOWNLOAD_BYTES",
    "PLUGIN_ARCHIVE_ROOT",
    "RESTORE_DISK_RESERVE_BYTES",
    "SKIP_DIRECTORY_NAMES",
    "TRANSIENT_DATA_PATHS",
    "VERSION_SAFE_PATTERN",
]
