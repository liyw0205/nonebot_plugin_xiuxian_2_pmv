from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import nonebot

nonebot.init()


def test_registered_map_mission_claim_rejects_ignored_receipt_and_recovers():
    with tempfile.TemporaryDirectory(prefix="xiuxian-map-mission-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [sys.executable, "-m", "tests.parameter_probe", "--minimal-map", "--group",
             "map-mission", "--report", str(report)],
            cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True, timeout=360,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
        assert result.returncode == 0, result.stdout + result.stderr + json.dumps(failures, ensure_ascii=False, indent=2)
        keys = {row["key"] for row in payload.get("results", [])}
        expected = {
            "map-mission-claim-receipt-ignore",
            "map-mission-empty-query-accept",
            "map-mission-daily-progress-keys",
            "map-mission-cross-day-and-isolation",
            "map-mission-incomplete-claim-no-write",
            "map-mission-claim-send-failure-after-commit",
            "map-mission-claim-existing-inventory-increments-binding",
            "map-mission-claim-inventory-full-rejects-reward",
            "map-mission-query-initialization-write-rejected",
            *{
                f"map-mission-{path}-state-{mode}"
                for path in ("accept", "snapshot")
                for mode in ("ignore", "rewrite", "abort")
            },
            *{
                f"map-mission-claim-{write}-{mode}"
                for write in ("mission", "stone", "inventory", "receipt", "outbox")
                for mode in ("ignore", "rewrite", "abort")
                if not (write == "receipt" and mode == "ignore")
            },
        }
        assert keys == expected, (keys, expected)
        assert payload.get("summary") == {"pass": len(expected)}, result.stdout
        assert payload["outcomes"] == {"recovery": 23, "success": 5, "rejection": 1}
