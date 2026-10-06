"""Feature boundaries for local plugin backup archives."""

from .application import PluginBackupCatalogApplication
from .file_application import PluginBackupFileApplication
from .file_repository import (
    InvalidPluginBackupFile,
    PluginBackupFileNotFound,
    PluginBackupFileRepository,
)
from .repository import PluginBackupCatalogRepository
from .restore_application import PluginBackupRestoreApplication
from .restore_factory import build_plugin_backup_restore_application
from .restore_repository import (
    InvalidPluginBackupArchive,
    PluginBackupArchiveNotFound,
    PluginBackupRestoreRepository,
)

__all__ = [
    "PluginBackupCatalogApplication",
    "PluginBackupCatalogRepository",
    "PluginBackupFileApplication",
    "InvalidPluginBackupFile",
    "PluginBackupFileNotFound",
    "PluginBackupFileRepository",
    "InvalidPluginBackupArchive",
    "PluginBackupArchiveNotFound",
    "PluginBackupRestoreApplication",
    "PluginBackupRestoreRepository",
    "build_plugin_backup_restore_application",
]
