"""Pytest bootstrap: keep runtime databases outside the repository."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from .bootstrap import copy_static_data


@pytest.fixture(scope="session")
def map_parameter_probe_results(tmp_path_factory):
    root = Path(__file__).resolve().parents[1]
    report = tmp_path_factory.mktemp("map-parameter-probe") / "report.json"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.parameter_probe",
            "--minimal-map",
            "--group",
            "map-dao-view",
            "--group",
            "map-nearby-display",
            "--report",
            str(report),
        ],
        cwd=root,
        text=True,
        capture_output=True,
        timeout=180,
    )
    payload = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
    failures = [row for row in payload.get("results", []) if row["status"] != "pass"]
    assert result.returncode == 0, (
        result.stdout + result.stderr
        + json.dumps(failures, ensure_ascii=False, indent=2)
    )
    assert payload.get("summary") == {"pass": 6}, result.stdout
    assert payload.get("outcomes") == {"success": 2, "recovery": 3, "rejection": 1}
    return payload


@pytest.fixture(autouse=True)
def isolated_xiuxian_data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("XIUXIAN_DATA_DIR", str(tmp_path / "xiuxian-data"))
    copy_static_data(
        Path(__file__).resolve().parents[1] / "data" / "xiuxian",
        tmp_path / "xiuxian-data",
    )
    try:
        from nonebot_plugin_xiuxian_2.paths import reset_paths_for_test

        reset_paths_for_test()
    except Exception:
        pass
    yield
    try:
        from nonebot_plugin_xiuxian_2.paths import reset_paths_for_test

        reset_paths_for_test()
    except Exception:
        pass
