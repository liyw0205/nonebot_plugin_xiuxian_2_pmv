from __future__ import annotations

import unittest

from .. import application, commands, jobs, migrations, repository, schemas, web
from ..manifest import FEATURE
from tests.slice_contract import assert_no_autonomous_surface


class LogsSliceContractTests(unittest.TestCase):
    def test_slice_has_no_autonomous_schema_or_runtime_surface(self) -> None:
        assert_no_autonomous_surface(
            self,
            commands=commands.COMMANDS,
            routes=web.ROUTES,
            jobs=jobs.JOBS,
            migrations=migrations.MIGRATIONS,
            legacy_routes=web.LEGACY_ROUTES,
        )

    def test_manifest_and_delegations_resolve(self) -> None:
        self.assertEqual(FEATURE.key, "logs")
        self.assertEqual(FEATURE.owner, "operations")
        self.assertEqual(FEATURE.test_tag, "logs")
        self.assertEqual(FEATURE.commands, ())
        self.assertEqual(FEATURE.routes, ())
        self.assertEqual(FEATURE.jobs, ())
        self.assertEqual(FEATURE.config, ())
        for _method, _path, target in web.LEGACY_ROUTES:
            class_name, _, attribute = target.partition(".")
            self.assertIs(getattr(application, class_name), application.LogsApplication)
            self.assertTrue(callable(getattr(application.LogsApplication, attribute)))

    def test_repository_facade_reexports_real_owners(self) -> None:
        self.assertIs(repository.LogFileRepository, application.LogFileRepository)
        self.assertIs(repository.MessageLogsRepository, application.MessageLogsRepository)
        self.assertEqual(schemas.LOG_SCENES, ("group", "private", "channel_group", "channel_private"))
        self.assertEqual(len(web.LEGACY_ROUTES), 5)


if __name__ == "__main__":
    unittest.main()
