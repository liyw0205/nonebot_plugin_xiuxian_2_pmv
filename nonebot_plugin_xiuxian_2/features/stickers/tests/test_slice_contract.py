from __future__ import annotations

import unittest
from pathlib import Path

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

SCHEMA_NAMES = (
    "ARCHIVE_TIMEOUT_SECONDS",
    "DOWNLOAD_CHUNK_BYTES",
    "DOWNLOAD_PROXY_PREFIX",
    "DOWNLOAD_USER_AGENT",
    "FILE_REPO_NAME",
    "FILE_REPO_OWNER",
    "MANIFEST_TIMEOUT_SECONDS",
    "MAX_MANIFEST_BYTES",
    "MAX_STICKER_ARCHIVE_BYTES",
    "MAX_STICKER_ARCHIVE_MEMBERS",
    "MAX_STICKER_FILE_BYTES",
    "MAX_STICKER_FILES",
    "MAX_STICKER_UNCOMPRESSED_BYTES",
    "STICKERS_MANIFEST_NAME",
    "STICKERS_RELEASE_TAG",
)

# These schema names reach the repository under the private spelling the module
# has always used, so the single-source check cannot compare equal names.
ALIASED = (
    ("_ALLOWED_HOSTS", "ALLOWED_DOWNLOAD_HOSTS"),
    ("_PACK_ID_RE", "PACK_ID_PATTERN"),
    ("_SHA256_RE", "SHA256_PATTERN"),
    ("_STICKER_FILE_RE", "STICKER_FILE_PATTERN"),
    ("_STICKER_TOKEN_RE", "STICKER_TOKEN_PATTERN"),
    ("_ZIP_NAME_RE", "ZIP_NAME_PATTERN"),
)


class StickerSliceContractTests(unittest.TestCase):
    def test_download_contract_has_one_home(self) -> None:
        assert_contract_is_single_source(self, schemas, ((repository, SCHEMA_NAMES),))
        for private, public in ALIASED:
            self.assertIs(getattr(repository, private), getattr(schemas, public))

    def test_release_urls_stay_built_out_of_the_declared_coordinates(self) -> None:
        self.assertEqual(
            repository.StickerRepository.remote_manifest_url(),
            (
                f"https://github.com/{schemas.FILE_REPO_OWNER}/{schemas.FILE_REPO_NAME}"
                f"/releases/download/{schemas.STICKERS_RELEASE_TAG}/{schemas.STICKERS_MANIFEST_NAME}"
            ),
        )
        self.assertEqual(
            repository.StickerRepository.remote_asset_url("pack.zip"),
            (
                f"https://github.com/{schemas.FILE_REPO_OWNER}/{schemas.FILE_REPO_NAME}"
                f"/releases/download/{schemas.STICKERS_RELEASE_TAG}/pack.zip"
            ),
        )

    def test_first_request_hosts_are_a_subset_of_the_download_hosts(self) -> None:
        self.assertTrue(schemas.INITIAL_REQUEST_HOSTS <= schemas.ALLOWED_DOWNLOAD_HOSTS)
        self.assertIn("ghproxy.net", schemas.INITIAL_REQUEST_HOSTS)

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
            sorted(web.LEGACY_ROUTES),
            sorted(
                (
                    ("GET", "/api/messages/stickers", "StickerApplication.catalog"),
                    ("POST", "/api/messages/stickers/install", "StickerApplication.start_install"),
                    ("GET", "/api/messages/stickers/install/<job_id>", "StickerApplication.install_status"),
                    (
                        "GET",
                        "/api/messages/stickers/file/<pack_id>/<path:filename>",
                        "StickerApplication.resolve_file",
                    ),
                )
            ),
        )

    def test_manifest_matches_the_slice(self) -> None:
        self.assertEqual(FEATURE.key, "stickers")
        self.assertEqual(FEATURE.test_tag, "stickers")
        self.assertEqual(FEATURE.owner, "operations")
        self.assertEqual(FEATURE.commands, ())
        self.assertEqual(FEATURE.routes, ())
        self.assertEqual(FEATURE.jobs, ())
        self.assertIsNone(FEATURE.migration_version)

    def test_declared_delegations_are_real_application_methods(self) -> None:
        for _method, _path, target in web.LEGACY_ROUTES:
            class_name, _, attribute = target.partition(".")
            self.assertIs(getattr(application, class_name), application.StickerApplication)
            self.assertTrue(callable(getattr(application.StickerApplication, attribute)))

    def test_documentation_names_the_declared_bounds(self) -> None:
        repository_root = Path(__file__).resolve().parents[4]
        text = (repository_root / "docs" / "features" / "stickers.md").read_text(encoding="utf-8")
        self.assertIn(FEATURE.title, text)
        for name in SCHEMA_NAMES:
            self.assertIn(name, text, name)


if __name__ == "__main__":
    unittest.main()
