"""This owner declares no job.

The scheduled database backup, ``backup_database_files``, is one of the legacy
APScheduler ids owned by ``legacy_scheduler`` in
``nonebot_plugin_xiuxian_2/compatibility/legacy_manifest.py``.  It happens to call
the same provider methods this slice calls during a manual upgrade, but the
schedule declaration must stay in one place, so it is not repeated here.
"""

JOBS = ()

__all__ = ["JOBS"]
