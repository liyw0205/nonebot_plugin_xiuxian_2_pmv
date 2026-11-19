from pathlib import Path
import json
import subprocess
import sys
import tempfile
import unittest


class GuishiParameterDispatchTests(unittest.TestCase):
    def test_bulk_clear_permission_failure_and_recovery(self):
        self.check_group("market-guishi-clear", 2, {"success": 2})

    def test_cancellation_and_expiry_write_recovery(self):
        self.check_group("market-guishi-return", 3, {"success": 3})

    def test_stored_item_recovery_replay_and_boundaries(self):
        self.check_group("market-guishi-take", 5, {"success": 5})

    def test_amount_order_boundaries_and_transfer_rollback(self):
        self.check_group("market-guishi-boundaries", 7, {"success": 7})

    def test_cutoff_failure_cleanup_and_pending_convergence(self):
        self.check_group("market-guishi-cutoff-recovery", 1, {"success": 1})

    def test_immediate_match_failure_and_restart_recovery(self):
        self.check_group("market-guishi-recovery", 2, {"success": 2})

    def test_creation_request_write_failure_recovery_and_adapters(self):
        self.check_group("market-guishi-create-recovery", 1, {"success": 1})

    def test_match_state_write_failure_recovery_and_adapters(self):
        self.check_group("market-guishi-match-state-recovery", 1, {"success": 1})

    def test_creation_replay_and_partial_cancellation_lifecycle(self):
        self.check_group("market-guishi-lifecycle", 5, {"success": 5})

    def test_orders_settlement_take_cancel_and_scheduled_return(self):
        self.check_group("market-guishi-parameters", 6, {"success": 5, "rejection": 1})

    def check_group(self, group, count, outcomes):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "parameters.json"
            result = subprocess.run(
                [sys.executable, "-m", "tests.parameter_probe", "--group", group,
                 "--report", str(report), "--base-sha", "guishi-schedule-regression"],
                cwd=Path(__file__).resolve().parents[1], text=True,
                capture_output=True, timeout=120,
            )
            evidence = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
            failures = [row for row in evidence.get("results", []) if row["status"] != "pass"]
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr + json.dumps(failures, ensure_ascii=False))
            self.assertEqual({"pass": count}, evidence["summary"], evidence["results"])
            self.assertEqual(outcomes, evidence["outcomes"])
            self.assertTrue(all(row["calls"] and row["evidence"] for row in evidence["results"]))


if __name__ == "__main__":
    unittest.main()
