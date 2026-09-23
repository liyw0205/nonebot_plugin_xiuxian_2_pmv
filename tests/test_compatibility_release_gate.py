from __future__ import annotations

import json
import hashlib
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from nonebot_plugin_xiuxian_2.compatibility.release_gate import CompatibilityReleaseGate
from nonebot_plugin_xiuxian_2.plugin import build_migrations
from scripts.check_compatibility_release import _parser as release_parser
from scripts.recovery_smoke import main as recovery_main


class CompatibilityReleaseGateTests(unittest.TestCase):
    # A rehearsal injects the tags it pretends to have; the default reader only
    # sees tags that really exist in the repository.
    TAGS = ("v1.0.0", "v1.1.0", "v1.2.0", "v1.1.9")

    def _gate(self, directory) -> CompatibilityReleaseGate:
        return CompatibilityReleaseGate(directory, tag_reader=lambda: self.TAGS)

    def test_maintenance_tools_reject_empty_data_directory(self) -> None:
        with self.assertRaisesRegex(ValueError, "non-empty path"):
            CompatibilityReleaseGate("")
        with self.assertRaises(SystemExit):
            release_parser().parse_args(["--data-dir", "", "begin", "--release", "v1.1.0"])
        with self.assertRaises(SystemExit):
            recovery_main(["--data-dir", ""])

    def test_recovery_smoke_restores_and_migrates_every_catalog_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = StringIO()
            with redirect_stdout(output):
                status = recovery_main(["--data-dir", directory])

            receipt = json.loads(output.getvalue())
            database_keys = {"game_db", "player_db", "trade_db", "impart_db", "message_db"}
            self.assertEqual(status, 0)
            self.assertEqual(set(receipt["restore_dry_run"]), database_keys)
            self.assertEqual(set(receipt["restore"]), database_keys)
            self.assertEqual(set(receipt["migrations_by_database"]), database_keys)
            self.assertEqual(
                set(receipt["migrations"]),
                {migration.version for migration in build_migrations()},
            )
            self.assertIn("trade.002", receipt["migrations_by_database"]["game_db"])
            self.assertNotIn("trade.003", receipt["migrations_by_database"]["game_db"])
            self.assertIn("trade.004", receipt["migrations_by_database"]["game_db"])
            self.assertNotIn("trade.005", receipt["migrations_by_database"]["game_db"])
            self.assertIn("trade.003", receipt["migrations_by_database"]["trade_db"])
            self.assertNotIn("trade.002", receipt["migrations_by_database"]["trade_db"])
            self.assertNotIn("trade.004", receipt["migrations_by_database"]["trade_db"])
            self.assertIn("trade.005", receipt["migrations_by_database"]["trade_db"])
            self.assertIn(
                "tianti_training.006", receipt["migrations_by_database"]["game_db"]
            )
            self.assertEqual(
                set(receipt["attached_migrations"]),
                {
                    "accessory_package.player_data.001",
                    "accessory_package.player_data.002",
                },
            )

    def test_release_ids_must_be_semver_and_later(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            gate = self._gate(directory)
            with self.assertRaisesRegex(ValueError, "semantic release"):
                gate.start("nightly")
            gate.start("v1.2.0")
            root = Path(directory)
            log = root / "runtime.log"
            log.write_text("ready\n", encoding="utf-8")
            evidence = root / "recovery.json"
            self._evidence(evidence)
            same_major_lower = gate.evaluate(
                "v1.1.9", log_paths=[log], recovery_evidence=evidence, required_migrations=["m1"]
            )
            self.assertFalse(same_major_lower.checks["release_cycle"])

    def test_start_rejects_a_release_that_was_never_tagged(self) -> None:
        """A rehearsal must not be able to open a P7 baseline on a fake version."""
        with tempfile.TemporaryDirectory() as directory:
            gate = self._gate(directory)
            with self.assertRaisesRegex(RuntimeError, "not a real repository tag"):
                gate.start("v9.9.9")

    def test_close_rejects_a_current_release_that_was_never_tagged(self) -> None:
        """Even with clean evidence, an unreleased version cannot complete P7."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "runtime.log"
            log.write_text("ready\n", encoding="utf-8")
            evidence = root / "recovery.json"
            self._evidence(evidence)
            gate = self._gate(root)
            gate.start("v1.0.0")
            report = gate.evaluate(
                "v2.0.0",
                log_paths=[log],
                recovery_evidence=evidence,
                required_migrations=["m1"],
            )
            self.assertFalse(report.ready)
            self.assertFalse(report.checks["release_cycle"])
            self.assertIn("not a real repository tag", "; ".join(report.reasons))
            with self.assertRaises(RuntimeError):
                gate.close(
                    "v2.0.0",
                    log_paths=[log],
                    recovery_evidence=evidence,
                    required_migrations=["m1"],
                )

    def test_repository_has_no_release_after_the_only_tag(self) -> None:
        """Guards the real audit: this repo currently has a single v1.0.0 tag."""
        from nonebot_plugin_xiuxian_2.compatibility.release_gate import released_tags

        self.assertIn("v1.0.0", released_tags())

    def _evidence(self, path: Path) -> None:
        manifest = path.parent / "backups" / "backup-1" / "manifest.json"
        manifest.parent.mkdir(parents=True)
        manifest.write_text("{}", encoding="utf-8")
        path.write_text(
            json.dumps(
                {
                    "schema": 1,
                    "backup": "backup-1",
                    "backup_manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
                    "restore_dry_run": ["game_db"],
                    "restore": ["game_db"],
                    "migrations": ["m1"],
                    "reconcile": {
                        "clean": True,
                        "operations": 0,
                        "outbox_events": 0,
                        "dead_events": 0,
                    },
                }
            ),
            encoding="utf-8",
        )

    def test_gate_requires_a_later_release_and_clean_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "logs").mkdir()
            log = root / "logs" / "runtime.log"
            log.write_text("ready\n", encoding="utf-8")
            evidence = root / "recovery.json"
            self._evidence(evidence)
            gate = self._gate(root)

            gate.start("v1.0.0")
            same_release = gate.evaluate(
                "v1.0.0",
                log_paths=[log],
                recovery_evidence=evidence,
                required_migrations=["m1"],
            )
            self.assertFalse(same_release.ready)
            self.assertFalse(same_release.checks["release_cycle"])

            later = gate.evaluate(
                "v1.1.0",
                log_paths=[log],
                recovery_evidence=evidence,
                required_migrations=["m1"],
            )
            self.assertTrue(later.ready)
            state = gate.close(
                "v1.1.0",
                log_paths=[log],
                recovery_evidence=evidence,
                required_migrations=["m1"],
            )
            self.assertEqual(state["completed_release"], "v1.1.0")

    def test_new_compatibility_hit_blocks_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "runtime.log"
            log.write_text("ready\n", encoding="utf-8")
            evidence = root / "recovery.json"
            self._evidence(evidence)
            gate = self._gate(root)
            gate.start("v1.0.0")
            (root / "compatibility_hits.json").write_text(
                json.dumps({"web:/config": 1}), encoding="utf-8"
            )
            report = gate.evaluate(
                "v1.1.0",
                log_paths=[log],
                recovery_evidence=evidence,
                required_migrations=["m1"],
            )
            self.assertFalse(report.ready)
            self.assertFalse(report.checks["compatibility_hits"])

    def test_old_url_marker_and_missing_migration_block_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "runtime.log"
            log.write_text("legacy URL /config\n", encoding="utf-8")
            evidence = root / "recovery.json"
            self._evidence(evidence)
            gate = self._gate(root)
            gate.start("v1.0.0")
            report = gate.evaluate(
                "v1.1.0",
                log_paths=[log],
                recovery_evidence=evidence,
                required_migrations=["m1", "m2"],
            )
            self.assertFalse(report.ready)
            self.assertFalse(report.checks["legacy_logs"])
            self.assertFalse(report.checks["historical_migrations"])


if __name__ == "__main__":
    unittest.main()
