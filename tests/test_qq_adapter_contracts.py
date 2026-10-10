from __future__ import annotations

import json
import subprocess
import sys
import unittest
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "tests" / "qq_adapter_contract_runner.py"


def _run_contract(source: str) -> dict:
    process = subprocess.run(
        [sys.executable, str(RUNNER), source],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(process.stdout.splitlines()[-1])


class QQAdapterContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            installed_version = version("nonebot-adapter-qq")
        except PackageNotFoundError as exc:
            raise AssertionError(
                "nonebot-adapter-qq must be installed for the adapter contract matrix"
            ) from exc
        if installed_version != "1.7.1":
            raise AssertionError(
                "adapter contract matrix requires nonebot-adapter-qq==1.7.1, "
                f"found {installed_version}"
            )

        cls.vendor = _run_contract("vendor")
        cls.installed = _run_contract("installed")

    def test_vendor_and_installed_share_event_and_send_contracts(self) -> None:
        comparable_keys = (
            "contexts",
            "interaction",
            "lifecycle",
            "send_results",
            "send_calls",
        )
        for key in comparable_keys:
            with self.subTest(contract=key):
                self.assertEqual(self.vendor[key], self.installed[key])

        contexts = self.vendor["contexts"]
        self.assertEqual(
            {name: context["scene"] for name, context in contexts.items()},
            {
                "group": "group",
                "c2c": "c2c",
                "channel": "channel",
                "interaction": "interaction",
                "lifecycle": "lifecycle",
            },
        )
        self.assertEqual(contexts["group"]["reference_id"], "REFIDX:group-reference-1")
        self.assertEqual(contexts["c2c"]["reference_id"], "REFIDX:c2c-reference-1")
        self.assertEqual(contexts["group"]["attachment_names"], ["status.png"])
        self.assertEqual(self.vendor["interaction"]["button_data"], "/checkin")
        self.assertEqual(self.vendor["lifecycle"]["action"], "bot_join_group")
        self.assertEqual(
            [call["route"] for call in self.vendor["send_calls"]],
            ["group", "c2c", "channel", "group", "group"],
        )

    def test_diagnostics_report_each_explicit_source(self) -> None:
        self.assertEqual(
            self.vendor["diagnostics"]["adapters"]["qq"]["source"],
            "vendor",
        )
        self.assertEqual(
            self.installed["diagnostics"]["adapters"]["qq"]["source"],
            "installed",
        )
        self.assertEqual(
            self.vendor["diagnostics"]["selection"]["effective"]["qq"],
            "vendor",
        )
        self.assertEqual(
            self.installed["diagnostics"]["selection"]["effective"]["qq"],
            "installed",
        )

    def test_payload_guard_handles_preloaded_adapters_without_hiding_invalid_events(self) -> None:
        for source in ("vendor", "installed"):
            with self.subTest(source=source):
                guard = getattr(self, source)["payload_guard"]
                for key in (
                    "enabled",
                    "adapter_identity_preserved",
                    "idempotent",
                    "resumed_input_unchanged",
                    "message_matches_fixture",
                    "message_input_unchanged",
                ):
                    self.assertTrue(guard[key], key)
                self.assertEqual(guard["resumed_type"], "RESUMED")
                self.assertEqual(guard["resumed_id"], "resume-1")
                self.assertEqual(guard["invalid_event_error"], "ValidationError")
                loaded = guard["loaded_file"]
                self.assertEqual("/vendor/" in loaded, source == "vendor")
                self.assertEqual("/site-packages/" in loaded, source == "installed")

    def test_auto_diagnostics_report_actual_installed_source(self) -> None:
        automatic = _run_contract("auto")
        self.assertEqual(automatic["diagnostics"]["selection"]["requested"], "auto")
        self.assertEqual(
            automatic["diagnostics"]["selection"]["effective"]["qq"],
            "installed",
        )
        self.assertEqual(
            automatic["diagnostics"]["adapters"]["qq"]["source"],
            "installed",
        )


if __name__ == "__main__":
    unittest.main()
