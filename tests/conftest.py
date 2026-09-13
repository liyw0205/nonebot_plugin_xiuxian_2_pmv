"""Pytest bootstrap: keep runtime databases outside the repository."""

from __future__ import annotations

from pathlib import Path

import pytest

from .bootstrap import copy_static_data


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
