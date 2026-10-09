"""Progress-gate contract for the four backup owners.

Their caps moved into each slice's ``schemas.py``, so the gate may no longer prove
a limit by finding its definition text inside the implementing module.  These
tests pin the five owner flags and prove the replacement helper still rejects a
changed value, a missing binding and an unused import.
"""

from __future__ import annotations

import re
import unittest

from scripts.check_full_refactor_progress import PACKAGE, _schema_constant_bound, _slice_status

OWNER_FLAGS = (
    ("plugin_backup_restore_owner", "repository_bounds_and_validates_zip_before_overlay"),
    ("plugin_backup_cloud_owner", "application_bounds_batches_and_preserves_partial_results"),
    ("plugin_backup_cloud_owner", "repository_bounds_webdav_and_atomically_installs_archives"),
    ("database_backup_owner", "repository_bounds_zip_restore_and_cloud_io"),
    ("config_backup_owner", "repository_bounds_json_and_cloud_io"),
)

# (slice package, consuming module, cap, value the gate must keep pinned)
CAPS = (
    ("plugin_backups", "restore_repository.py", "MAX_ARCHIVE_MEMBERS", 100_000),
    ("plugin_backups", "cloud_application.py", "MAX_CLOUD_BACKUP_BATCH", 100),
    ("plugin_backups", "cloud_repository.py", "MAX_CLOUD_LIST_BYTES", 2 * 1024 * 1024),
    ("plugin_backups", "cloud_repository.py", "MAX_CLOUD_LIST_ENTRIES", 1_000),
    ("plugin_backups", "cloud_repository.py", "MAX_PLUGIN_BACKUP_DOWNLOAD_BYTES", 4 * 1024 * 1024 * 1024),
    ("database_backups", "application.py", "MAX_DATABASE_BACKUP_BATCH", 100),
    ("database_backups", "repository.py", "MAX_DATABASE_RESTORE_MEMBERS", 256),
    ("database_backups", "repository.py", "MAX_DATABASE_RESTORE_BYTES", 16 * 1024 * 1024 * 1024),
    ("config_backups", "repository.py", "MAX_CONFIG_BACKUP_BYTES", 16 * 1024 * 1024),
    ("config_backups", "repository.py", "MAX_CONFIG_CLOUD_LIST_BYTES", 2 * 1024 * 1024),
    ("config_backups", "repository.py", "MAX_CONFIG_CLOUD_LIST_ENTRIES", 1_000),
)


def _source(slice_package: str, name: str) -> str:
    return (PACKAGE / "features" / slice_package / name).read_text(encoding="utf-8")


def _mutate_cap(contract_source: str, name: str) -> str:
    pattern = re.compile(rf"^({re.escape(name)} = ).*$", re.MULTILINE)
    assert pattern.search(contract_source), name
    return pattern.sub(lambda matched: matched.group(1) + "999999", contract_source)


def _detach_binding(consumer_source: str, name: str) -> str:
    """Remove only the schemas binding so the use sites are left dangling."""
    kept: list[str] = []
    for line in consumer_source.splitlines():
        stripped = line.strip()
        if stripped == f"{name},":
            continue
        if stripped.startswith("from .schemas import") and "(" not in stripped and name in stripped:
            siblings = [item.strip() for item in stripped.split("import", 1)[1].split(",")]
            siblings = [item for item in siblings if item and item != name]
            if siblings:
                kept.append("from .schemas import " + ", ".join(siblings))
            continue
        kept.append(line)
    joined = "\n".join(kept)
    assert len(joined) < len(consumer_source), f"{name} binding was not detached"
    return joined


class BackupProgressContractTests(unittest.TestCase):
    def test_backup_owner_flags_stay_green(self) -> None:
        slices = _slice_status()
        for owner, flag in OWNER_FLAGS:
            self.assertTrue(slices[owner][flag], f"{owner}.{flag}")

    def test_every_cap_is_bound_to_its_schema_declaration(self) -> None:
        for slice_package, consumer_name, cap, value in CAPS:
            with self.subTest(cap=cap):
                self.assertTrue(
                    _schema_constant_bound(
                        _source(slice_package, "schemas.py"),
                        _source(slice_package, consumer_name),
                        cap,
                        value,
                    ),
                    f"{consumer_name} no longer binds {cap} from schemas.py",
                )

    def test_gate_rejects_a_changed_cap_value(self) -> None:
        for slice_package, consumer_name, cap, value in CAPS:
            contract = _source(slice_package, "schemas.py")
            mutated = _mutate_cap(contract, cap)
            self.assertNotEqual(mutated, contract, cap)
            self.assertFalse(
                _schema_constant_bound(
                    mutated, _source(slice_package, consumer_name), cap, value
                ),
                f"{cap} can be re-valued without tripping the gate",
            )

    def test_gate_rejects_a_missing_binding_and_an_unused_import(self) -> None:
        for slice_package, consumer_name, cap, value in CAPS:
            contract = _source(slice_package, "schemas.py")
            consumer = _source(slice_package, consumer_name)
            self.assertFalse(
                _schema_constant_bound(contract, _detach_binding(consumer, cap), cap, value),
                f"{consumer_name} passes without importing {cap}",
            )
        unused = "from .schemas import MAX_ARCHIVE_MEMBERS\n\ndef run(limit: int) -> int:\n    return limit\n"
        self.assertFalse(
            _schema_constant_bound(
                _source("plugin_backups", "schemas.py"),
                unused,
                "MAX_ARCHIVE_MEMBERS",
                100_000,
            ),
            "an unused import must not count as an enforced bound",
        )


if __name__ == "__main__":
    unittest.main()
