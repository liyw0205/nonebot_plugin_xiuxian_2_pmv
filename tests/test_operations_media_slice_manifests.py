"""Registry and documentation contract for the outbound-media operation slices.

These four slices sit on the send/serve side of the product: an installed sticker
pack, an uploaded QQ image, a cached file handed back to a browser and one Web
message send.  None of them owns gameplay state, so the value of the manifest is
that the registry knows the boundary exists while no phantom command, route or
job is invented for it; the legacy Flask routes stay where they are registered
until an adapter cutover moves them.
"""

from __future__ import annotations

import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features import cache_files as CACHE_FILES_PACKAGE
from nonebot_plugin_xiuxian_2.features import messages as MESSAGES_PACKAGE
from nonebot_plugin_xiuxian_2.features import qq_image_upload as QQ_IMAGE_UPLOAD_PACKAGE
from nonebot_plugin_xiuxian_2.features import stickers as STICKERS_PACKAGE

from nonebot_plugin_xiuxian_2.features.cache_files.manifest import FEATURE as CACHE_FILES
from nonebot_plugin_xiuxian_2.features.cache_files.web import LEGACY_ROUTES as CACHE_FILE_ROUTES
from nonebot_plugin_xiuxian_2.features.messages.manifest import FEATURE as MESSAGES
from nonebot_plugin_xiuxian_2.features.messages.web import LEGACY_ROUTES as MESSAGE_ROUTES
from nonebot_plugin_xiuxian_2.features.qq_image_upload.manifest import FEATURE as QQ_IMAGE_UPLOAD
from nonebot_plugin_xiuxian_2.features.qq_image_upload.web import LEGACY_ROUTES as UPLOAD_ROUTES
from nonebot_plugin_xiuxian_2.features.stickers.manifest import FEATURE as STICKERS
from nonebot_plugin_xiuxian_2.features.stickers.web import LEGACY_ROUTES as STICKER_ROUTES
from nonebot_plugin_xiuxian_2.plugin import build_registry

from tests.docs_contract import DOCUMENTATION_HEADINGS

SLICES = (
    (CACHE_FILES, CACHE_FILE_ROUTES),
    (QQ_IMAGE_UPLOAD, UPLOAD_ROUTES),
    (STICKERS, STICKER_ROUTES),
    (MESSAGES, MESSAGE_ROUTES),
)

OWNER_PACKAGES = {
    CACHE_FILES.key: CACHE_FILES_PACKAGE,
    QQ_IMAGE_UPLOAD.key: QQ_IMAGE_UPLOAD_PACKAGE,
    STICKERS.key: STICKERS_PACKAGE,
    MESSAGES.key: MESSAGES_PACKAGE,
}


class OperationsMediaSliceManifestTests(unittest.TestCase):
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
        self.assertEqual(len(owners), 7, sorted(owners))


if __name__ == "__main__":
    unittest.main()
