"""L5: 命运道果 get_root_rate 阶梯 + 软顶（统一 handle/web）。"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NB_PATH = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_utils" / "numeric_bind.py"


def _load_nb():
    # pure functions only — load file without package __init__
    spec = importlib.util.spec_from_file_location("nb_fate_under_test", NB_PATH)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    # as_int_like and friends need module exec; numeric_bind has no nonebot at import
    # but may relative-import later; inject package stubs only if needed
    sys.modules[spec.name] = mod
    try:
        spec.loader.exec_module(mod)
    except ImportError:
        # relative imports fail outside package — init nonebot path
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


def test_fate_root_rate_ladder_early_and_soft_cap():
    nb = _load_nb()
    eternal = 7.0
    step = 2.0
    # lv0 = base
    assert nb.compute_fate_root_rate(0, eternal, step) == eternal
    # lv1 = 7+2 = 9
    assert nb.compute_fate_root_rate(1, eternal, step) == 9.0
    # soft cap at eternal * 1.5 = 10.5
    soft = eternal * nb.FATE_ROOT_SOFT_CAP_RATIO
    high = nb.compute_fate_root_rate(50, eternal, step)
    assert high == soft
    # uncapped ladder would be >> soft
    raw = nb.compute_fate_root_rate(50, eternal, step, soft_cap_ratio=None)
    assert raw > soft
    # web-old bug style linear would explode; soft top must hold
    assert high <= soft + 1e-9


def test_fate_root_rate_bad_inputs():
    nb = _load_nb()
    assert nb.compute_fate_root_rate(-3, 7.0, 2.0) == 7.0
    assert nb.compute_fate_root_rate("x", 7.0, 2.0) == 7.0
