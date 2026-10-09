"""Web surface of the manual-backup owner.

``POST /manual_backup`` is still registered by the legacy Flask module
``xiuxian/xiuxian_web/backups.py`` with its admin and CSRF guards, so no new
adapter route is declared yet.
"""

ROUTES = ()
LEGACY_ROUTES = (
    ("POST", "/manual_backup", "ManualBackupApplication.create_backup"),
)

__all__ = ["LEGACY_ROUTES", "ROUTES"]
