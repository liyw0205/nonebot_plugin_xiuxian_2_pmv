"""Web surface of the configuration-backup owner.

``ROUTES`` stays empty because the new adapter registry does not serve these
paths yet: every request is still registered by the legacy Flask module
``xiuxian/xiuxian_web/backups.py``, which performs its own admin session and
CSRF checks before delegating to :class:`ConfigBackupApplication`.
``LEGACY_ROUTES`` records that delegation surface so a later adapter cutover can
move the declarations here instead of rediscovering them.

Every target is written as ``<ApplicationClass>.<method>`` so a contract test can
resolve it against the public surface of this package.
"""

ROUTES = ()
LEGACY_ROUTES = (
    ("POST", "/cloud_backup_config", "ConfigBackupApplication.backup_cloud_config"),
    ("GET", "/get_cloud_config_backups", "ConfigBackupApplication.list_cloud_backups"),
    ("POST", "/sync_cloud_config_backup", "ConfigBackupApplication.sync_cloud_backup"),
    ("POST", "/cloud_restore_config_backup", "ConfigBackupApplication.restore_cloud_backup"),
    ("POST", "/export_config", "ConfigBackupApplication.export_config"),
    ("POST", "/import_config", "ConfigBackupApplication.import_config"),
    ("POST", "/backup_config", "ConfigBackupApplication.create_local_backup"),
    ("GET", "/get_config_backups", "ConfigBackupApplication.list_local_backups"),
    ("POST", "/restore_config_backup", "ConfigBackupApplication.restore_local_backup"),
    ("POST", "/delete_config_backup", "ConfigBackupApplication.delete_local_backup"),
)

__all__ = ["LEGACY_ROUTES", "ROUTES"]
