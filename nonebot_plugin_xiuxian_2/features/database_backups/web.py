"""Web surface of the database-backup owner.

The concrete routes are still registered by the legacy Flask module
``xiuxian/xiuxian_web/backups.py`` with its own admin and CSRF guards, so
``ROUTES`` is empty until an adapter blueprint takes them over.
``LEGACY_ROUTES`` names the ``Application.method`` each path delegates to.
"""

ROUTES = ()
LEGACY_ROUTES = (
    ("POST", "/manual_db_backup", "DatabaseBackupApplication.create_backup"),
    ("GET", "/get_db_backups", "DatabaseBackupApplication.list_local_backups"),
    ("POST", "/restore_db_backup", "DatabaseBackupApplication.restore_local_backup"),
    ("GET", "/get_cloud_db_backups", "DatabaseBackupApplication.list_cloud_backups"),
    ("POST", "/sync_cloud_db_backup", "DatabaseBackupApplication.sync_cloud_backup"),
    ("POST", "/cloud_restore_db_backup", "DatabaseBackupApplication.restore_cloud_backup"),
    ("POST", "/batch_delete_db_backups", "DatabaseBackupApplication.delete_local_backups"),
    ("POST", "/batch_sync_cloud_db_backups", "DatabaseBackupApplication.sync_cloud_backups"),
    ("POST", "/batch_delete_cloud_db_backups", "DatabaseBackupApplication.delete_cloud_backups"),
)

__all__ = ["LEGACY_ROUTES", "ROUTES"]
