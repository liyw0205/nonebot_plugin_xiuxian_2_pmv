"""This owner declares no job.

Retention is applied inside ``create_backup``, but ``create_backup`` is not only
web-driven: ``legacy_scheduler`` owns the APScheduler job ``backup_database_files``
(cron ``hour="*/4" minute=10``, ``xiuxian/xiuxian_scheduler/__init__.py:523-544``)
which reaches this slice through ``UpdateManager.backup_db_files``.  The job stays
declared in ``compatibility/legacy_manifest.py`` until the scheduler owner moves.
"""

JOBS = ()

__all__ = ["JOBS"]
