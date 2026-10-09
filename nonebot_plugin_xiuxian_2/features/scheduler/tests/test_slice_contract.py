from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

import nonebot
from apscheduler.triggers.interval import IntervalTrigger

nonebot.init()

from .. import (
    application,
    apscheduler_manager,
    commands,
    jobs,
    migrations,
    repository,
    schemas,
    web,
)
from ..manifest import FEATURE
from tests.slice_contract import assert_contract_is_single_source, assert_no_autonomous_surface

SCHEMA_NAMES = (
    "CRON_TRIGGER_FIELDS",
    "MANUAL_RUN_ID_PREFIX",
    "MIN_TRIGGER_INTERVAL_SECONDS",
    "RUN_HISTORY_LIMIT",
    "SCHEDULE_STORE_NAME",
    "SCHEDULE_STORE_SCHEMA_VERSION",
)


class SchedulerSliceContractTests(unittest.TestCase):
    def test_store_and_trigger_bounds_have_one_home(self) -> None:
        assert_contract_is_single_source(self, schemas, ((apscheduler_manager, SCHEMA_NAMES),))

    def test_manager_port_has_one_home(self) -> None:
        self.assertIs(
            application.SchedulerAdminManager, repository.SchedulerAdminManager
        )

    def test_slice_declares_no_autonomous_surface(self) -> None:
        assert_no_autonomous_surface(
            self,
            commands=commands.COMMANDS,
            routes=web.ROUTES,
            jobs=jobs.JOBS,
            migrations=migrations.MIGRATIONS,
            legacy_routes=web.LEGACY_ROUTES,
        )
        self.assertEqual(
            list(web.LEGACY_ROUTES),
            [
                ("GET", "/api/scheduler/jobs", "SchedulerAdminApplication.list_jobs"),
                ("POST", "/api/scheduler/jobs/<job_id>/enabled", "SchedulerAdminApplication.set_enabled"),
                ("POST", "/api/scheduler/jobs/<job_id>/schedule", "SchedulerAdminApplication.reschedule"),
                ("POST", "/api/scheduler/jobs/<job_id>/run", "SchedulerAdminApplication.queue_manual_run"),
                ("GET", "/api/scheduler/runs/<run_id>", "SchedulerAdminApplication.get_run"),
            ],
        )

    def test_manifest_matches_the_slice(self) -> None:
        self.assertEqual(FEATURE.key, "scheduler")
        self.assertEqual(FEATURE.test_tag, "scheduler")
        self.assertEqual(FEATURE.owner, "operations")
        self.assertEqual(FEATURE.commands, ())
        self.assertEqual(FEATURE.routes, ())
        self.assertEqual(FEATURE.jobs, ())
        self.assertIsNone(FEATURE.migration_version)

    def test_declared_delegation_is_a_real_application_method(self) -> None:
        for _method, _path, target in web.LEGACY_ROUTES:
            class_name, _, attribute = target.partition(".")
            self.assertIs(getattr(application, class_name), application.SchedulerAdminApplication)
            self.assertTrue(callable(getattr(application.SchedulerAdminApplication, attribute)))

    def test_store_defaults_are_written_with_the_declared_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = apscheduler_manager.SchedulerJobManager(
                scheduler=Mock(), store_path=Path(directory) / schemas.SCHEDULE_STORE_NAME
            )
            self.assertEqual(
                manager._load_store(),
                {"version": schemas.SCHEDULE_STORE_SCHEMA_VERSION, "jobs": {}},
            )

    def test_serialized_interval_never_reports_zero_seconds(self) -> None:
        serialized = apscheduler_manager.SchedulerJobManager._serialize_trigger(
            IntervalTrigger(seconds=0)
        )
        self.assertEqual(
            serialized, {"type": "interval", "seconds": schemas.MIN_TRIGGER_INTERVAL_SECONDS}
        )


if __name__ == "__main__":
    unittest.main()
