from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ReleaseDocumentBoundaryTests(unittest.TestCase):
    def test_internal_quality_inputs_are_excluded_without_hiding_user_docs(self) -> None:
        attributes = (ROOT / ".gitattributes").read_text(encoding="utf-8")
        excluded = {
            "docs/current_project_status.md",
            "docs/refactor_baseline.md",
            "docs/refactor_inventory.json",
            "docs/refactor_phase2_legacy_paths.json",
            "docs/refactor_phase2_legacy_path_items.json",
            "docs/full_refactor_progress.md",
            "docs/refactor_slice_execution_protocol.md",
        }
        for path in excluded:
            with self.subTest(path=path):
                self.assertIn(f"{path} export-ignore", attributes)

        for path in (
            "docs/installation.md",
            "docs/configuration.md",
            "docs/gameplay_notes.md",
            "docs/features/messages.md",
            "docs/message_channel.md",
        ):
            with self.subTest(path=path):
                self.assertNotIn(f"{path} export-ignore", attributes)


if __name__ == "__main__":
    unittest.main()
