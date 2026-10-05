from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def _run_cli(command: str, data_dir: Path) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "nonebot_plugin_xiuxian_2",
            command,
            "--data-dir",
            str(data_dir),
            "--dry-run",
        ]
        if command == "migrate"
        else [
            sys.executable,
            "-m",
            "nonebot_plugin_xiuxian_2",
            command,
            "--data-dir",
            str(data_dir),
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def test_cli_migrate_dry_run_is_driver_independent_and_machine_readable(tmp_path: Path) -> None:
    result = _run_cli("migrate", tmp_path / "migrate")

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    payload = json.loads(result.stdout)
    assert payload["dry_run"] is True
    assert "game_db" in payload["pending"]


def test_cli_health_does_not_import_driver_bound_web_graph(tmp_path: Path) -> None:
    result = _run_cli("health", tmp_path / "health")

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    payload = json.loads(result.stdout)
    assert payload["state"] == "ready"
    assert payload["ready"] is True
    assert all(payload["checks"].values())
