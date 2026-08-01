"""L3: percent_exp_reward gap/next_power anchor (pace plan)."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _import_numeric_bind():
    """Init nonebot lightly so package import works, then load numeric_bind."""
    if "nonebot" not in sys.modules or not getattr(
        sys.modules.get("nonebot"), "get_driver", lambda: None
    ):
        pass
    import nonebot

    try:
        nonebot.get_driver()
    except ValueError:
        nonebot.init()
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    # fresh import path
    if "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.numeric_bind" in sys.modules:
        return importlib.reload(
            sys.modules["nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.numeric_bind"]
        )
    return importlib.import_module(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.numeric_bind"
    )


def test_percent_exp_current_anchor_unchanged():
    nb = _import_numeric_bind()
    # high rank → suppress 1.0
    got = nb.percent_exp_reward(1_000_000, 0.01, 20)
    assert got == 10_000
    # low rank suppress 0.1 * 5 = 0.5
    got = nb.percent_exp_reward(1_000_000, 0.01, 5)
    assert got == 5_000


def test_percent_exp_gap_anchor_uses_stage_gap_not_current_exp():
    nb = _import_numeric_bind()
    levels = {
        "祭道境初期": {"power": 10_000_000_000_000, "spend": 1},
        "祭道境中期": {"power": 17_000_000_000_000, "spend": 1},
    }
    gap = 7_000_000_000_000

    def fake_level_data():
        return levels

    with mock.patch.object(nb, "_level_power_table", fake_level_data):
        # current_exp huge — gap anchor must ignore it for base
        got = nb.percent_exp_reward(
            10**30,
            0.001,
            "祭道境初期",
            anchor="gap",
            apply_rank_suppress=False,
        )
    assert got == int(gap * 0.001)
    # next_power anchor
    with mock.patch.object(nb, "_level_power_table", fake_level_data):
        got2 = nb.percent_exp_reward(
            1,
            0.001,
            "祭道境初期",
            anchor="next_power",
            apply_rank_suppress=False,
        )
    assert got2 == int(17_000_000_000_000 * 0.001)


def test_percent_exp_once_cap():
    nb = _import_numeric_bind()
    levels = {
        "A": {"power": 1000, "spend": 1},
        "B": {"power": 2000, "spend": 1},
    }
    with mock.patch.object(nb, "_level_power_table", lambda: levels):
        got = nb.percent_exp_reward(
            1000,
            0.5,
            "A",
            anchor="gap",
            apply_rank_suppress=False,
            once_cap=100,
        )
    assert got == 100
