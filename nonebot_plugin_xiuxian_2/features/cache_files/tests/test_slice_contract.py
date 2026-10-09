from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.cache_files import (
    application,
    commands,
    jobs,
    migrations,
    repository,
    schemas,
    web,
)
from nonebot_plugin_xiuxian_2.features.cache_files.manifest import FEATURE
from tests.slice_contract import assert_contract_is_single_source, assert_no_autonomous_surface

SCHEMA_NAMES = ("CacheFileNotFound", "CacheFileNotRegular", "CacheFileOutsideRoot")


class CacheFileSliceContractTests(unittest.TestCase):
    def test_refusal_contract_has_one_home(self) -> None:
        assert_contract_is_single_source(self, schemas, ((repository, SCHEMA_NAMES),))
        for name in SCHEMA_NAMES:
            self.assertIs(getattr(repository, name), getattr(schemas, name))

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
            [("GET", "/download/<path:filepath>", "CacheFileApplication.resolve_download")],
        )

    def test_manifest_matches_the_slice(self) -> None:
        self.assertEqual(FEATURE.key, "cache_files")
        self.assertEqual(FEATURE.test_tag, "cache_files")
        self.assertEqual(FEATURE.owner, "operations")
        self.assertEqual(FEATURE.commands, ())
        self.assertEqual(FEATURE.routes, ())
        self.assertEqual(FEATURE.jobs, ())
        self.assertIsNone(FEATURE.migration_version)

    def test_declared_delegation_is_a_real_application_method(self) -> None:
        for _method, _path, target in web.LEGACY_ROUTES:
            class_name, _, attribute = target.partition(".")
            self.assertIs(getattr(application, class_name), application.CacheFileApplication)
            self.assertTrue(callable(getattr(application.CacheFileApplication, attribute)))

    def test_confinement_rule_still_rejects_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as cache_root:
            root = Path(cache_root)
            (root / "ok.webp").write_bytes(b"ok")
            app = application.CacheFileApplication()
            self.assertEqual(app.resolve_download(root, "ok.webp").name, "ok.webp")
            with self.assertRaises(schemas.CacheFileNotFound):
                app.resolve_download(root, "missing.webp")
            with self.assertRaises(schemas.CacheFileOutsideRoot):
                app.resolve_download(root, "../outside.webp")
            self.assertEqual((root / "ok.webp").read_bytes(), b"ok")


if __name__ == "__main__":
    unittest.main()
