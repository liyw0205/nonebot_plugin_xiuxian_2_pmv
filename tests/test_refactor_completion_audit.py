from __future__ import annotations

import unittest

from scripts.refactor_completion_audit import audit


class RefactorCompletionAuditTests(unittest.TestCase):
    def test_static_stages_pass_and_p7_requires_real_release_evidence(self) -> None:
        report = audit()
        self.assertFalse(report["ready"])
        stages = report["stages"]
        self.assertTrue(all(stages[name]["ready"] for name in ("P0", "P1", "P2", "P3", "P4", "P5", "P6")))
        self.assertFalse(stages["P7"]["ready"])
        self.assertIn("real release-cycle evidence", stages["P7"]["errors"][0])


if __name__ == "__main__":
    unittest.main()
