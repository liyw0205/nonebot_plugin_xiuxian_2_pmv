"""P0 dependency-lock and documentation-drift guards."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.check_architecture import (
    check_dependency_lock,
    check_documented_migration_count,
)

ROOT = Path(__file__).resolve().parents[1]


class DependencyLockTests(unittest.TestCase):
    def test_repository_lock_pins_runtime_and_test_toolchain(self) -> None:
        self.assertEqual(check_dependency_lock(), [])

    def test_lock_must_include_pytest_and_uvicorn(self) -> None:
        source = (ROOT / "requirements.lock").read_text(encoding="utf-8")
        for package in ("pytest==", "uvicorn==", "nonebot2==", "Flask=="):
            self.assertIn(package, source)

    def test_lock_has_no_unpinned_requirements(self) -> None:
        for line in (ROOT / "requirements.lock").read_text(encoding="utf-8").splitlines():
            entry = line.strip()
            if entry and not entry.startswith("#"):
                self.assertIn("==", entry, entry)


class DocumentedMigrationCountTests(unittest.TestCase):
    def test_docs_match_the_real_migration_registry(self) -> None:
        self.assertEqual(check_documented_migration_count(), [])

    def test_guard_rejects_a_stale_count(self) -> None:
        """The guard must fail when a doc hardcodes a wrong migration total.

        The check is exercised against a temporary root so the repository docs
        are never mutated by a test run.
        """
        import scripts.check_architecture as architecture
        from nonebot_plugin_xiuxian_2.plugin import build_migrations

        actual = len(build_migrations())
        stale = actual + 1
        with tempfile.TemporaryDirectory() as directory:
            docs = Path(directory) / "docs"
            docs.mkdir(parents=True)
            (docs / "refactor_architecture.md").write_text(
                f"启动、CLI 迁移和恢复演练共用完整 {stale} 项清单。", encoding="utf-8"
            )
            original_root = architecture.ROOT
            architecture.ROOT = Path(directory)
            try:
                errors = architecture.check_documented_migration_count()
            finally:
                architecture.ROOT = original_root
        self.assertTrue(errors, "stale migration count was not detected")
        self.assertIn(str(stale), errors[0])


if __name__ == "__main__":
    unittest.main()
