from pathlib import Path

import nonebot

nonebot.init()


HANDLE_SOURCE = Path(
    "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/xiuxian2_handle.py"
).read_text(encoding="utf-8")
HELPER_SOURCE = Path(
    "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_back/accessory_helpers.py"
).read_text(encoding="utf-8")


def test_accessory_effects_use_pure_rule_module():
    function = HANDLE_SOURCE[HANDLE_SOURCE.index("def calc_accessory_effects") :]
    assert "from .accessory_rules import AFFIX_KEY_MAP, SET_BONUS" in function
    assert "from ..xiuxian_back import AFFIX_KEY_MAP, SET_BONUS" not in function
    assert "AFFIX_KEY_MAP =" not in HELPER_SOURCE
    assert "SET_BONUS =" not in HELPER_SOURCE
    assert "from ..xiuxian_utils.accessory_rules import AFFIX_KEY_MAP, SET_BONUS" in HELPER_SOURCE


def test_accessory_effects_preserve_rule_results():
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import (
        calc_accessory_effects,
    )

    payload = {
        "equipped": {
            "手镯": {
                "set_type": "烈阳",
                "affixes": [
                    {"type": "气血", "value": 0.1},
                    {"type": "攻击", "value": 0.2},
                ],
            },
            "戒指": {
                "set_type": "烈阳",
                "affixes": [
                    {"type": "会心", "value": 0.03},
                    {"type": "速度", "value": 12},
                ],
            },
            "手环": None,
            "项链": None,
        }
    }

    result = calc_accessory_effects("user-1", accessory_provider=lambda _user_id: payload)

    assert result["hp_pct"] == 0.1
    assert result["atk_pct"] == 0.2
    assert result["crit_rate"] == 0.03
    assert result["speed"] == 12
    assert result["set_count"] == {"烈阳": 2}
    assert result["set_bonus"] == [
        {"set": "烈阳", "pieces": 2, "type": "attack", "value": 0.08}
    ]
