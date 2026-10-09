"""Registry and documentation contract for the backup-owner slices.

These four slices own filesystem artifacts rather than gameplay, so the value of
their manifest is that the registry knows the boundary exists while no phantom
command, route or job is invented for it.  The legacy Flask routes stay where
they are registered until an adapter cutover moves them.
"""

from __future__ import annotations

import unittest

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features import config_backups as CONFIG_BACKUPS_PACKAGE
from nonebot_plugin_xiuxian_2.features import database_backups as DATABASE_BACKUPS_PACKAGE
from nonebot_plugin_xiuxian_2.features import manual_backups as MANUAL_BACKUPS_PACKAGE
from nonebot_plugin_xiuxian_2.features import plugin_backups as PLUGIN_BACKUPS_PACKAGE

from nonebot_plugin_xiuxian_2.features.config_backups.manifest import FEATURE as CONFIG_BACKUPS
from nonebot_plugin_xiuxian_2.features.config_backups.web import LEGACY_ROUTES as CONFIG_ROUTES
from nonebot_plugin_xiuxian_2.features.database_backups.manifest import FEATURE as DATABASE_BACKUPS
from nonebot_plugin_xiuxian_2.features.database_backups.web import LEGACY_ROUTES as DATABASE_ROUTES
from nonebot_plugin_xiuxian_2.features.manual_backups.manifest import FEATURE as MANUAL_BACKUPS
from nonebot_plugin_xiuxian_2.features.manual_backups.web import LEGACY_ROUTES as MANUAL_ROUTES
from nonebot_plugin_xiuxian_2.features.plugin_backups.manifest import FEATURE as PLUGIN_BACKUPS
from nonebot_plugin_xiuxian_2.features.plugin_backups.web import LEGACY_ROUTES as PLUGIN_ROUTES
from nonebot_plugin_xiuxian_2.plugin import build_registry

from tests.docs_contract import DOCUMENTATION_HEADINGS

SLICES = (
    (CONFIG_BACKUPS, CONFIG_ROUTES),
    (DATABASE_BACKUPS, DATABASE_ROUTES),
    (MANUAL_BACKUPS, MANUAL_ROUTES),
    (PLUGIN_BACKUPS, PLUGIN_ROUTES),
)


OWNER_PACKAGES = {
    CONFIG_BACKUPS.key: CONFIG_BACKUPS_PACKAGE,
    DATABASE_BACKUPS.key: DATABASE_BACKUPS_PACKAGE,
    MANUAL_BACKUPS.key: MANUAL_BACKUPS_PACKAGE,
    PLUGIN_BACKUPS.key: PLUGIN_BACKUPS_PACKAGE,
}


class BackupSliceManifestTests(unittest.TestCase):
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
            self.assertEqual(feature.commands, (), feature.key)
            self.assertEqual(feature.jobs, (), feature.key)
            self.assertEqual(feature.config, (), feature.key)
            self.assertEqual(feature.routes, (), feature.key)
            self.assertIsNone(feature.migration_version, feature.key)
            self.assertNotIn(feature.test_tag, job_owner)
            for _method, path, _target in legacy_routes:
                # Until the adapter takes a path over it must not be declared here.
                self.assertEqual(
                    [key for (declared, _method), key in route_index.items() if declared == path],
                    [],
                    f"{feature.key} would double-declare {path}",
                )
        for feature, _routes in SLICES:
            for name in command_owner:
                self.assertNotEqual(command_owner[name][0], feature.key)

    def test_documentation_describes_each_slice(self) -> None:
        from pathlib import Path

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
        self.assertEqual(len(owners), 30, sorted(owners))


if __name__ == "__main__":
    unittest.main()
