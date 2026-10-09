"""Web surface of the plugin-backup owner.

``ROUTES`` is empty: the paths below are still registered by the legacy Flask
module ``xiuxian/xiuxian_web/backups.py`` (admin session plus CSRF guard), which
delegates to the cloud, creation, file and restore applications of this slice.
The ``GET /get_backups`` catalogue read is registered by the legacy module
``xiuxian/xiuxian_web/pages.py``; it is listed here because it reads this owner's
archive directory. ``GET /backups`` is already served by the new adapter
(``adapters/web/blueprints/legacy.py`` redirects it to ``/pages/backups``), which
belongs to the ``runtime_web`` owner, so it is deliberately absent below.
"""

ROUTES = ()
LEGACY_ROUTES = (
    ("GET", "/get_backups", "PluginBackupCatalogApplication.list_plugin_backups"),
    ("GET", "/get_cloud_backups", "PluginBackupCloudApplication.list_cloud_backups"),
    ("POST", "/sync_cloud_backup", "PluginBackupCloudApplication.sync_cloud_backup"),
    ("POST", "/cloud_restore_backup", "PluginBackupRestoreApplication.restore_backup"),
    ("POST", "/restore_backup", "PluginBackupRestoreApplication.restore_backup"),
    ("POST", "/batch_delete_backups", "PluginBackupFileApplication.delete_plugin_backups"),
    ("POST", "/batch_sync_cloud_backups", "PluginBackupCloudApplication.sync_cloud_backups"),
    ("POST", "/batch_delete_cloud_backups", "PluginBackupCloudApplication.delete_cloud_backups"),
    ("GET", "/download_backup/<filename>", "PluginBackupFileApplication.open_plugin_backup"),
    ("POST", "/delete_backup", "PluginBackupFileApplication.delete_plugin_backup"),
)

__all__ = ["LEGACY_ROUTES", "ROUTES"]
