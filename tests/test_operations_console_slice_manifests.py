"""Registry and documentation contract for the operations console slices.

These four owners are the control plane: an operator lists and nudges scheduled
jobs, checks a release, edits a table, or opens a PTY shell.  None of them may
invent a route, command, job, or migration, because the transport they use is
still the legacy Flask app and the schedule declarations still belong to the
compatibility owner.  What the registry gains is a named boundary per control
plane plus a recorded delegation target, so a later adapter cutover has one list
per surface to move and a test that proves each target is a real public method.
"""

from __future__ import annotations

import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features import database_console as DATABASE_CONSOLE_PACKAGE
from nonebot_plugin_xiuxian_2.features import scheduler as SCHEDULER_PACKAGE
from nonebot_plugin_xiuxian_2.features import terminal as TERMINAL_PACKAGE
from nonebot_plugin_xiuxian_2.features import updater as UPDATER_PACKAGE

from nonebot_plugin_xiuxian_2.features.database_console.manifest import FEATURE as DATABASE_CONSOLE
from nonebot_plugin_xiuxian_2.features.database_console.web import LEGACY_ROUTES as DATABASE_ROUTES
from nonebot_plugin_xiuxian_2.features.scheduler.manifest import FEATURE as SCHEDULER
from nonebot_plugin_xiuxian_2.features.scheduler.web import LEGACY_ROUTES as SCHEDULER_ROUTES
from nonebot_plugin_xiuxian_2.features.terminal.manifest import FEATURE as TERMINAL
from nonebot_plugin_xiuxian_2.features.terminal.web import LEGACY_ROUTES as TERMINAL_ROUTES
from nonebot_plugin_xiuxian_2.features.updater.manifest import FEATURE as UPDATER
from nonebot_plugin_xiuxian_2.features.updater.web import LEGACY_ROUTES as UPDATER_ROUTES
from nonebot_plugin_xiuxian_2.plugin import build_registry

from tests.docs_contract import DOCUMENTATION_HEADINGS

SLICES = (
    (SCHEDULER, SCHEDULER_ROUTES),
    (UPDATER, UPDATER_ROUTES),
    (DATABASE_CONSOLE, DATABASE_ROUTES),
    (TERMINAL, TERMINAL_ROUTES),
)

OWNER_PACKAGES = {
    SCHEDULER.key: SCHEDULER_PACKAGE,
    UPDATER.key: UPDATER_PACKAGE,
    DATABASE_CONSOLE.key: DATABASE_CONSOLE_PACKAGE,
    TERMINAL.key: TERMINAL_PACKAGE,
}

# ``/database`` is the one console path the new adapter already owns; the slice
# must not add a second declaration for it.
ADAPTER_OWNERS = {"runtime_web"}


class OperationsConsoleSliceManifestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = build_registry()

    def test_registry_holds_the_same_manifest_objects(self) -> None:
        registered = {feature.key: feature for feature in self.registry.features}
        for feature, _routes in SLICES:
            self.assertIs(registered.get(feature.key), feature, feature.key)

    def test_declared_surface_stays_empty(self) -> None:
        command_owner = self.registry.command_index()
        job_owner = self.registry.job_index()
        route_index = self.registry.route_index()
        for feature, legacy_routes in SLICES:
            with self.subTest(feature=feature.key):
                self.assertEqual(feature.commands, ())
                self.assertEqual(feature.routes, ())
                self.assertEqual(feature.jobs, ())
                self.assertEqual(feature.config, ())
                self.assertIsNone(feature.migration_version)
                self.assertNotIn(feature.test_tag, job_owner)
                for name in command_owner:
                    self.assertNotEqual(command_owner[name][0], feature.key)
                for _method, path, _target in legacy_routes:
                    owners = {
                        feature_key
                        for (declared, _method), (feature_key, _permission) in route_index.items()
                        if declared == path
                    }
                    self.assertTrue(
                        owners <= ADAPTER_OWNERS,
                        f"{feature.key} would double-declare {path} against {owners}",
                    )

    def test_documentation_describes_each_slice(self) -> None:
        for feature, legacy_routes in SLICES:
            documentation = Path("docs/features") / f"{feature.key}.md"
            self.assertTrue(documentation.is_file(), documentation)
            text = documentation.read_text(encoding="utf-8")
            self.assertIn(feature.title, text, feature.key)
            for heading in DOCUMENTATION_HEADINGS:
                self.assertIn(heading, text, f"{documentation} is missing {heading}")
            for _method, path, target in legacy_routes:
                self.assertIn(path, text, f"{documentation} omits {path}")
                self.assertIn(target, text, f"{documentation} omits {target}")

    def test_every_legacy_delegation_resolves_publicly(self) -> None:
        """A recorded delegation must be a real public application method."""
        owners: dict[str, str] = {}
        for feature, legacy_routes in SLICES:
            package = OWNER_PACKAGES[feature.key]
            for method, path, target in legacy_routes:
                class_name, sep, attribute = target.partition(".")
                self.assertTrue(sep and attribute, f"{feature.key}: {target} is not Class.method")
                self.assertNotIn(".", attribute, f"{feature.key}: {target} is not one hop")
                application = getattr(package, class_name, None)
                self.assertIsNotNone(application, f"{feature.key}: {class_name} is not public")
                self.assertTrue(
                    callable(getattr(application, attribute, None)),
                    f"{feature.key}: {class_name}.{attribute} is not callable",
                )
                self.assertNotIn(path, owners, f"{path} claimed by {owners.get(path)} and {feature.key}")
                owners[path] = feature.key
        self.assertEqual(len(owners), 16, sorted(owners))


if __name__ == "__main__":
    unittest.main()
