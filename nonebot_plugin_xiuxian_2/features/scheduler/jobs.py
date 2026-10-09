"""This slice administers jobs, it never declares one.

Every legacy APScheduler id is declared once by the compatibility owner
``legacy_scheduler`` in ``nonebot_plugin_xiuxian_2/compatibility/legacy_manifest.py``,
which is the module that still calls ``scheduled_job``/``add_job``.  Repeating an
id here would make ``FeatureRegistry.validate()`` raise a duplicate-job error and
would also break the phase-2 gate that proves each legacy id is still registered
and still reachable through ``JobExecutor.run_sync``.
"""

JOBS = ()

__all__ = ["JOBS"]
