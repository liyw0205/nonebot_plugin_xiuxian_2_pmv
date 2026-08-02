"""命运道果 get_root_rate：阶梯累加，默认无软顶（与历史一致）。"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NB_PATH = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_utils" / "numeric_bind.py"


def _load_nb():
    spec = importlib.util.spec_from_file_location("nb_fate_under_test", NB_PATH)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    try:
        spec.loader.exec_module(mod)
    except ImportError:
        import nonebot

        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        for k in list(sys.modules):
            if k.startswith("nonebot_plugin_xiuxian_2"):
                del sys.modules[k]
        return importlib.import_module(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.numeric_bind"
        )
    return mod


def test_fate_root_rate_ladder_no_default_soft_cap():
    nb = _load_nb()
    eternal = 7.0
    step = 2.0
    assert nb.compute_fate_root_rate(0, eternal, step) == eternal
    assert nb.compute_fate_root_rate(1, eternal, step) == 9.0
    # lv40 ≈ 旧「约 5000%」口径
    high = nb.compute_fate_root_rate(40, eternal, step)
    assert abs(high - 49.5) < 1e-9
    # 默认不再顶在 永恒×1.5
    assert high > eternal * 1.5
    # 可选 soft_cap 仍可用（实验/审计）
    capped = nb.compute_fate_root_rate(40, eternal, step, soft_cap_ratio=1.5)
    assert capped == eternal * 1.5


def test_fate_root_rate_bad_inputs():
    nb = _load_nb()
    assert nb.compute_fate_root_rate(-3, 7.0, 2.0) == 7.0
    assert nb.compute_fate_root_rate("x", 7.0, 2.0) == 7.0
