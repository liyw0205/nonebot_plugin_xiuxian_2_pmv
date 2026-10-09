from __future__ import annotations

import unittest

from .. import (
    application,
    commands,
    jobs,
    migrations,
    repository,
    schemas,
    web,
)
from ..manifest import FEATURE
from tests.slice_contract import assert_contract_is_single_source, assert_no_autonomous_surface

SCHEMA_NAMES = ("DEFAULT_RELEASE_LIST_COUNT", "UPDATE_ASSET_NAME")


class _Provider:
    """Only the first provider call is reached by the refusal under test."""

    def __init__(self, asset_name: str) -> None:
        self.asset_name = asset_name
        self.prepared: list[str] = []
        self.cleaned: list[object] = []

    def prepare_release_asset(self, tag: str):
        self.prepared.append(tag)
        return True, {"name": self.asset_name}

    def cleanup_download(self, path):
        self.cleaned.append(path)


class _CountingProvider:
    def __init__(self) -> None:
        self.counts: list[int] = []

    def get_latest_releases(self, count: int):
        self.counts.append(count)
        return []


class UpdaterSliceContractTests(unittest.TestCase):
    def test_release_bounds_have_one_home(self) -> None:
        assert_contract_is_single_source(self, schemas, ((application, SCHEMA_NAMES),))
        self.assertIs(application._RELEASE_TAG_RE, schemas.RELEASE_TAG_PATTERN)

    def test_provider_port_has_one_home(self) -> None:
        self.assertIs(application.UpdateProvider, repository.UpdateProvider)
        self.assertIs(application.Release, repository.Release)
        self.assertIs(application.ReleaseAsset, repository.ReleaseAsset)

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
                ("GET", "/check_update", "UpdateApplication.check_update"),
                ("GET", "/get_releases", "UpdateApplication.latest_releases"),
                ("POST", "/perform_update", "UpdateApplication.perform_update_with_backup"),
            ],
        )

    def test_manifest_matches_the_slice(self) -> None:
        self.assertEqual(FEATURE.key, "updater")
        self.assertEqual(FEATURE.test_tag, "updater")
        self.assertEqual(FEATURE.owner, "operations")
        self.assertEqual(FEATURE.commands, ())
        self.assertEqual(FEATURE.routes, ())
        self.assertEqual(FEATURE.jobs, ())
        self.assertIsNone(FEATURE.migration_version)

    def test_declared_delegation_is_a_real_application_method(self) -> None:
        for _method, _path, target in web.LEGACY_ROUTES:
            class_name, _, attribute = target.partition(".")
            self.assertIs(getattr(application, class_name), application.UpdateApplication)
            self.assertTrue(callable(getattr(application.UpdateApplication, attribute)))

    def test_release_tag_guard_accepts_wire_tags_and_rejects_paths(self) -> None:
        for tag in ("v1.2.3", "2026.10.9-rc1+build.7", "a"):
            self.assertTrue(application.is_valid_release_tag(tag), tag)
        for tag in ("", "../etc/passwd", "a" * 129, "tag name", None, 7):
            self.assertFalse(application.is_valid_release_tag(tag), repr(tag))

    def test_release_list_default_uses_the_declared_count(self) -> None:
        provider = _CountingProvider()
        application.UpdateApplication(provider).latest_releases()
        self.assertEqual(provider.counts, [schemas.DEFAULT_RELEASE_LIST_COUNT])

    def test_asset_name_is_the_refusal_rule(self) -> None:
        provider = _Provider(asset_name="other.zip")
        ok, message = application.UpdateApplication(provider).perform_update_with_backup("v1.2.3")
        self.assertFalse(ok)
        self.assertIn(schemas.UPDATE_ASSET_NAME, message)
        self.assertEqual(provider.prepared, ["v1.2.3"])
        self.assertEqual(provider.cleaned, [])
        self.assertFalse(application.UpdateApplication._update_lock.locked())


if __name__ == "__main__":
    unittest.main()
