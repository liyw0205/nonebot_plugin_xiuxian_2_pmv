from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import nonebot

nonebot.init()


def test_registered_map_interactive_parameters_and_sqlite_recovery():
    with tempfile.TemporaryDirectory(prefix="xiuxian-map-interactive-") as directory:
        report = Path(directory) / "report.json"
        result = subprocess.run(
            [
                sys.executable, "-m", "tests.parameter_probe", "--minimal-map",
                "--group", "map-interactive", "--report", str(report),
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            timeout=360,
        )
        payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
        failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
        assert result.returncode == 0, result.stdout + result.stderr + json.dumps(failures, ensure_ascii=False, indent=2)
        assert payload.get("summary") == {"pass": 14}, result.stdout
        assert payload["outcomes"] == {"success": 4, "rejection": 1, "recovery": 9}
