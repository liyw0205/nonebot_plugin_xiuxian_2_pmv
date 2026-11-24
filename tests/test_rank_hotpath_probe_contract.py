from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class RankHotpathProbeContractTests(unittest.TestCase):
    def test_probe_fails_loudly_on_suppressed_info_import(self):
        source = (ROOT / "tests/command_probe.py").read_text(encoding="utf-8")
        probe = (ROOT / "scripts/rank_hotpath_probe.py").read_text(encoding="utf-8")
        self.assertIn("importlib.import_module(", source)
        self.assertIn("_import_info_from_fixture", source)
        self.assertIn("source_root=", source)
        self.assertIn("sys.path=", source)
        self.assertIn("loaded partially", source)
        self.assertIn(".rank-hotpath-source", source)
        self.assertIn(".rank-hotpath-temp", probe)

    def test_command_probe_installs_hooks_before_reusing_matcher_holder(self):
        source = (ROOT / "tests/command_probe.py").read_text(encoding="utf-8")
        self.assertIn("install_driver_hooks(driver)", source)
        self.assertIn("driver._xiuxian_refactored_lifecycle_hooks[0]", source)

    def test_validation_keeps_only_supported_comparisons(self):
        probe = (ROOT / "scripts/rank_hotpath_probe.py").read_text(encoding="utf-8")
        runner = (ROOT / "scripts/run_rank_hotpath_validation.sh").read_text(encoding="utf-8")
        self.assertIn("--title-reuse", probe)
        self.assertIn("--config-reuse", probe)
        self.assertIn("--base-reuse", probe)
        self.assertIn("--state-reuse", probe)
        self.assertIn("title-profile", runner)
        self.assertIn("config-profile", runner)
        self.assertIn("base-profile", runner)
        self.assertIn("state-profile", runner)
        self.assertIn("rank-key FIXTURE", runner)
        self.assertIn("flock -w 600", runner)
        self.assertIn("--rank-key-compare", probe)
        self.assertIn("key_exp_rank", probe)
        self.assertNotIn("dual-rank", probe)
        self.assertNotIn("dual-rank", runner)


if __name__ == "__main__":
    unittest.main()
