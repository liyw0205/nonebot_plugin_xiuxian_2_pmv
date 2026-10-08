"""Contract and deterministic calculation tests for ordinary closing settlement."""

from __future__ import annotations

from pathlib import Path

from nonebot_plugin_xiuxian_2.features.buff.application import BuffApplication
from nonebot_plugin_xiuxian_2.features.buff.closing_reward import ClosingRewardCalculator


SOURCE = Path("nonebot_plugin_xiuxian_2/xiuxian/xiuxian_buff/__init__.py")


def _closing_handler_source() -> str:
    source = SOURCE.read_text(encoding="utf-8")
    start = source.index("async def out_closing_")
    end = source.index("@mind_state.handle", start)
    return source[start:end]


def _reward_inputs(**overrides):
    values = {
        "exp_time": 10,
        "current_exp": 100,
        "current_stone": 50,
        "current_hp": 10,
        "current_mp": 20,
        "exp_cap": 1000,
        "closing_exp": 2,
        "level_rate": 1.0,
        "realm_rate": 1.5,
        "main_rate": 0.1,
        "closing_rate": 0.2,
        "blessed_rate": 0.1,
    }
    values.update(overrides)
    return values


def test_out_closing_contract_preserves_alias_and_application_boundaries() -> None:
    source = SOURCE.read_text(encoding="utf-8")
    handler = _closing_handler_source()

    assert 'out_closing = on_command("出关", aliases={"灵石出关"}' in source
    assert "buff_application.closing_replay(" in handler
    assert "buff_application.calculate_closing_reward(" in handler
    assert "buff_application.closing_settle(" in handler
    assert "str(event.message) == \"灵石出关\"" in handler
    assert "_closing_settlement_service().settle(" not in handler
    assert "_apply_spirit_vein_exp_bonus(" not in handler
    assert "OtherSet().set_closing_type(" not in handler
    assert "jsondata.level_data()" not in handler


def test_out_closing_replays_before_legacy_snapshot_reads() -> None:
    handler = _closing_handler_source()

    replay_index = handler.index("buff_application.closing_replay(")
    snapshot_index = handler.index("_sql_message().get_user_info_with_id(")
    assert replay_index < snapshot_index


def test_closing_operation_id_is_stable_for_event_and_namespaced() -> None:
    source = SOURCE.read_text(encoding="utf-8")
    assert 'getattr(event, "message_id", "")' in source
    assert 'return f"buff-closing-settle:{event_id}:{user_id}"' in source
    assert 'return f"buff-closing-settle:{user_id}:{runtime_ids.new_id()}"' in source
    assert "closing_operation_id" in _closing_handler_source()


def test_normal_closing_reward_golden_case() -> None:
    result = ClosingRewardCalculator.calculate(**_reward_inputs())

    assert result.as_data() == {
        "base_exp": 43,
        "exp_gain": 43,
        "stone_cost": 0,
        "exp_time": 10,
        "hp_gain": 100,
        "mp_gain": 50,
        "new_exp": 143,
        "new_hp": 71,
        "new_mp": 70,
        "new_atk": 14,
        "new_power": 214,
        "reached_limit": False,
        "efficiency": 1.4000000000000001,
        "spirit_vein_message": "",
    }


def test_stone_closing_reward_golden_case() -> None:
    result = BuffApplication.calculate_closing_reward(
        **_reward_inputs(stone_exit=True)
    )

    assert result.exp_gain == 86
    assert result.stone_cost == 43
    assert result.new_exp == 186
    assert result.new_hp == 93
    assert result.new_mp == 70
    assert result.new_atk == 18
    assert result.new_power == 279


def test_closing_reward_caps_before_stone_exit() -> None:
    result = ClosingRewardCalculator.calculate(
        **_reward_inputs(exp_cap=30, stone_exit=True)
    )

    assert result.reached_limit is True
    assert result.exp_gain == 30
    assert result.stone_cost == 0
    assert result.new_exp == 130


def test_closing_reward_uses_only_available_stones() -> None:
    result = ClosingRewardCalculator.calculate(
        **_reward_inputs(current_stone=7, stone_exit=True)
    )

    assert result.stone_cost == 7
    assert result.exp_gain == 50
    assert result.new_exp == 150


def test_bad_create_time_is_represented_by_zero_minutes_without_negative_gain() -> None:
    result = ClosingRewardCalculator.calculate(
        **_reward_inputs(exp_time=0, current_stone=999, stone_exit=True)
    )

    assert result.exp_time == 0
    assert result.base_exp == 0
    assert result.exp_gain == 0
    assert result.stone_cost == 0
    assert result.new_exp == 100
    assert result.new_hp == 10
    assert result.new_mp == 20


def test_calculator_normalizes_malformed_numeric_snapshot() -> None:
    result = ClosingRewardCalculator.calculate(
        **_reward_inputs(
            exp_time="not-a-time",
            current_exp="bad",
            current_stone="bad",
            current_hp="bad",
            current_mp="bad",
            exp_cap="bad",
        )
    )

    assert result.exp_time == 0
    assert result.exp_gain == 0
    assert result.stone_cost == 0
    assert result.new_exp == 0
