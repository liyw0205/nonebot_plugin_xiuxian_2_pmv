"""轮回印记取回门槛：convert_rank 偏移与脏 memory_level。"""

from __future__ import annotations

from pathlib import Path


def _load_convert_rank():
    text = (
        Path(__file__).parents[1]
        / "nonebot_plugin_xiuxian_2"
        / "xiuxian"
        / "xiuxian_config.py"
    ).read_text(encoding="utf-8")
    start = text.index("def convert_rank")
    end = text.index("\ndef added_ranks", start)
    ns: dict = {}
    exec(text[start:end], ns)
    return ns["convert_rank"]


def _required_level(old_level: str, root_level: int = 0):
    convert_rank = _load_convert_rank()
    old_rank_score, rank_list = convert_rank(old_level)
    total_offset = 9 + min(max(root_level, 0), 9)
    required_rank_score = min(old_rank_score + total_offset, convert_rank("江湖好手")[0])
    target_idx = len(rank_list) - required_rank_score - 1
    target_idx = max(0, min(target_idx, len(rank_list) - 1))
    return rank_list[target_idx], required_rank_score, old_rank_score


def test_poxu_yuanman_root0_needs_yaori_yuanman():
    need, req_sc, old_sc = _required_level("破虚境圆满", 0)
    assert need == "耀日境圆满"
    assert old_sc == 13
    assert req_sc == 22


def test_zaohua_high_root_needs_yaori():
    # 造化圆满 + 高轮回等级 → 门槛降到耀日圆满
    need, _, _ = _required_level("造化境圆满", 9)
    assert need == "耀日境圆满"


def test_dirty_memory_level_zero_not_in_ranks():
    convert_rank = _load_convert_rank()
    assert convert_rank("0")[0] is None
    assert convert_rank("")[0] is None


def test_can_retrieve_source_guards_dirty_level():
    text = (
        Path(__file__).parents[1]
        / "nonebot_plugin_xiuxian_2"
        / "xiuxian"
        / "xiuxian_lunhui"
        / "__init__.py"
    ).read_text(encoding="utf-8")
    assert 'old_level in (None, "", 0, "0")' in text
    assert "印记境界：" in text
