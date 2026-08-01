"""L2 虚神界成长曲线：卡 concavity + lv 系数（非结算硬限速）。

不经 nonebot_plugin_xiuxian_2 包入口（会要求 get_driver），直接加载 card_bonus 文件。
"""

from __future__ import annotations

import importlib.util
import math
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CARD_BONUS_PATH = (
    ROOT
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "xiuxian_impart"
    / "card_bonus.py"
)
PK_INIT_PATH = (
    ROOT
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "xiuxian_impart_pk"
    / "__init__.py"
)


def _load_card_bonus():
    spec = importlib.util.spec_from_file_location(
        "xu_card_bonus_under_test", CARD_BONUS_PATH
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_effective_xu_impart_exp_up_zero_and_bad():
    cb = _load_card_bonus()
    assert cb.effective_xu_impart_exp_up(0) == 0.0
    assert cb.effective_xu_impart_exp_up(None) == 0.0
    assert cb.effective_xu_impart_exp_up(-1) == 0.0
    assert cb.effective_xu_impart_exp_up("x") == 0.0


def test_effective_xu_impart_exp_up_concave_and_bounded():
    cb = _load_card_bonus()
    raw_full = 3.0
    eff = cb.effective_xu_impart_exp_up(raw_full)
    expected = cb.XU_EXP_UP_SOFT_MAX * (1.0 - math.exp(-raw_full / cb.XU_EXP_UP_TAU))
    assert eff == pytest.approx(expected)
    assert eff < raw_full
    assert eff < cb.XU_EXP_UP_SOFT_MAX
    assert eff > 0.5
    half = cb.effective_xu_impart_exp_up(1.5)
    assert 0 < half < eff
    assert (eff - half) < (half - 0.0)


def test_xu_impart_lv_bonus_rate_and_clamp():
    cb = _load_card_bonus()
    assert cb.xu_impart_lv_bonus(0) == 0.0
    assert cb.xu_impart_lv_bonus(10) == pytest.approx(10 * cb.XU_IMPART_LV_RATE)
    assert cb.XU_IMPART_LV_RATE == pytest.approx(0.03)
    assert cb.xu_impart_lv_bonus(10) < 10 * 0.1
    assert cb.xu_impart_lv_bonus(cb.XU_IMPART_LV_MAX + 50) == pytest.approx(
        cb.XU_IMPART_LV_MAX * cb.XU_IMPART_LV_RATE
    )
    assert cb.xu_impart_lv_bonus(-3) == 0.0
    assert cb.xu_impart_lv_bonus("bad") == 0.0


def test_impart_pk_source_wires_growth_helpers():
    """Catch rename/import-path drift without loading full NoneBot pk module."""
    pk_src = PK_INIT_PATH.read_text(encoding="utf-8")
    assert (
        "from ..xiuxian_impart.card_bonus import effective_xu_impart_exp_up, xu_impart_lv_bonus"
        in pk_src
    )
    assert "effective_xu_impart_exp_up(impart_exp_up_raw)" in pk_src
    assert "xu_impart_lv_bonus(impart_lv)" in pk_src
    assert "impart_lv * 0.1" not in pk_src
    assert "impart_lv*0.1" not in pk_src
