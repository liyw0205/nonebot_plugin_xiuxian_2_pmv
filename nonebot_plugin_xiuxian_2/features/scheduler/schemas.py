"""Durable bounds of the scheduler administration surface.

This owner never executes a job; it lists, pauses, reschedules, and manually
queues runs that ``SchedulerJobManager`` already registered.  The values below
are therefore the whole wire contract between the Web admin page and the
persisted override store: the store file name and shape version decide whether
an existing ``data`` file can be read after an upgrade, the manual-run id prefix
is the only way an operator-run is recognisable inside the run history, and the
cron whitelist plus minimum interval bound what the schedule editor may write.
"""

from __future__ import annotations


SCHEDULE_STORE_NAME = "scheduler_overrides.json"
SCHEDULE_STORE_SCHEMA_VERSION = 1
MANUAL_RUN_ID_PREFIX = "web-manual:"
RUN_HISTORY_LIMIT = 100
CRON_TRIGGER_FIELDS = (
    "year",
    "month",
    "day",
    "week",
    "day_of_week",
    "hour",
    "minute",
    "second",
)
MIN_TRIGGER_INTERVAL_SECONDS = 1

__all__ = [
    "CRON_TRIGGER_FIELDS",
    "MANUAL_RUN_ID_PREFIX",
    "MIN_TRIGGER_INTERVAL_SECONDS",
    "RUN_HISTORY_LIMIT",
    "SCHEDULE_STORE_NAME",
    "SCHEDULE_STORE_SCHEMA_VERSION",
]
