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


def test_stage_power_gap_caps_huge_jump_vs_prev_gap():
    """永恒→至高 类断层：raw gap 远大于上一境 gap 时 soft cap。"""
    nb = _import_numeric_bind()
    levels = {
        "永恒境中期": {"power": 1000, "spend": 1},
        "永恒境圆满": {"power": 2000, "spend": 1},  # prev gap = 1000
        "至高": {"power": 10**20, "spend": 1},  # raw gap from 圆满 = ~1e20
    }
    with mock.patch.object(nb, "_level_power_table", lambda: levels):
        raw_would = 10**20 - 2000
        prev_gap = 1000
        got = nb.stage_power_gap("永恒境圆满")
    assert got < raw_would
    assert got == prev_gap * nb.GAP_JUMP_SOFT_MULT
    # 正常相邻 gap 不砍
    with mock.patch.object(nb, "_level_power_table", lambda: levels):
        assert nb.stage_power_gap("永恒境中期") == 1000


def test_stage_power_gap_final_realm_not_full_power():
    """至高无下一境：禁止 gap=full power；沿用上一境有效（已 cap）gap。"""
    nb = _import_numeric_bind()
    levels = {
        "永恒境中期": {"power": 1000, "spend": 1},
        "永恒境圆满": {"power": 2000, "spend": 1},
        "至高": {"power": 10**20, "spend": 1},
    }
    with mock.patch.object(nb, "_level_power_table", lambda: levels):
        final_gap = nb.stage_power_gap("至高")
        prev_eff = nb.stage_power_gap("永恒境圆满")
    assert final_gap == prev_eff
    assert final_gap != 10**20
    assert final_gap == 1000 * nb.GAP_JUMP_SOFT_MULT


def test_percent_exp_gap_final_realm_uses_capped_gap():
    nb = _import_numeric_bind()
    levels = {
        "永恒境中期": {"power": 1000, "spend": 1},
        "永恒境圆满": {"power": 2000, "spend": 1},
        "至高": {"power": 10**20, "spend": 1},
    }
    with mock.patch.object(nb, "_level_power_table", lambda: levels):
        got = nb.percent_exp_reward(
            10**30,
            0.01,
            "至高",
            anchor="gap",
            apply_rank_suppress=False,
        )
    assert got == int(1000 * nb.GAP_JUMP_SOFT_MULT * 0.01)
