"""Activity-center entry commands must exist and be gated like the card says.

The card is the player's index into the activity system: every entry it offers
has to be a real command, and the lifecycle state it renders has to agree with
the gate the entry flow itself enforces. Shared business behaviour is verified
once here; adapter-specific formatting lives in the delivery contract test.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.center import (
    ENDED,
    IN_PROGRESS,
    NOT_STARTED,
    REWARD_CLAIM,
    commands_for_status,
    resolve_status,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import service
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_config import (
    activity_state,
)

ROOT = Path(__file__).resolve().parents[1]
ACTIVITY_MODULE = (
    ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_activity" / "__init__.py"
)

GAMEPLAY_TYPES = ("collect_words", "event_points", "activity_boss")


def _registered_commands() -> set[str]:
    """Command names and aliases registered by the activity module."""
    source = ACTIVITY_MODULE.read_text(encoding="utf-8")
    names: set[str] = set()
    for match in re.finditer(
        r'on_command\(\s*"([^"]+)"(?:\s*,\s*aliases=\{([^}]*)\})?', source
    ):
        names.add(match.group(1))
        names |= set(re.findall(r'"([^"]+)"', match.group(2) or ""))
    return names


def _card_targets() -> set[str]:
    """Every command the center card can offer, across all lifecycle states."""
    targets: set[str] = set()
    for status in (NOT_STARTED, IN_PROGRESS, REWARD_CLAIM, ENDED):
        targets |= {command for _, command in commands_for_status(status)}
    for activity_type in GAMEPLAY_TYPES:
        targets |= {
            command
            for _, command in commands_for_status(
                IN_PROGRESS, {"type": activity_type, "enabled": True}
            )
        }
    return targets


def test_every_card_entry_is_a_registered_activity_command() -> None:
    registered = _registered_commands()
    # 解析必须真的取到注册表，否则空集会让本测试假绿。
    assert len(registered) > 30
    assert {"活动中心", "活动大厅"} <= registered
    assert _card_targets() <= registered


def test_card_lifecycle_agrees_with_the_entry_flow_gate() -> None:
    now = datetime.now()
    cases = [
        # (config, 卡片状态, 流程闸门是否放行)
        ({"enabled": False, "start_time": "0", "end_time": "无限"}, ENDED, False),
        (
            {"enabled": True, "start_time": "2099-01-01", "end_time": "2099-12-31"},
            NOT_STARTED,
            False,
        ),
        (
            {"enabled": True, "start_time": "2000-01-01", "end_time": "2000-12-31"},
            ENDED,
            False,
        ),
        ({"enabled": True, "start_time": "0", "end_time": "无限"}, IN_PROGRESS, True),
    ]
    for config, expected_status, expected_ok in cases:
        status = resolve_status(
            config["start_time"],
            config["end_time"],
            now,
            enabled=bool(config["enabled"]),
        )
        assert status == expected_status, config
        ok, reason = activity_state(config)
        assert ok is expected_ok, (config, reason)
        if not ok:
            # 拒绝必须给出原因，卡片状态才有对应说法。
            assert reason.strip()


def test_entry_flows_refuse_out_of_window_without_touching_the_database(
    monkeypatch,
) -> None:
    def _no_db(*args, **kwargs):
        raise AssertionError("入口流程在活动窗口外不应访问数据库")

    monkeypatch.setattr(service.db_backend, "connect_readonly", _no_db)
    monkeypatch.setattr(service.db_backend, "connect", _no_db)

    base = {
        "enabled": True,
        "start_time": "2099-01-01",
        "end_time": "2099-12-31",
        "daily_rewards": [],
        "milestone_rewards": [],
        "daily_tasks": [],
        "weekly_tasks": [],
        "extensions": {"activity_pass": {"enabled": True}},
    }
    flows = (
        service.claim_sign,
        service.claim_activity_tasks,
        service.claim_activity_pass_rewards,
    )

    monkeypatch.setattr(service, "load_config", lambda: base)
    for flow in flows:
        ok, reason = flow("u-1")
        assert ok is False, flow.__name__
        assert "尚未开始" in reason, (flow.__name__, reason)

    ended = dict(base, start_time="2000-01-01", end_time="2000-12-31")
    monkeypatch.setattr(service, "load_config", lambda: ended)
    for flow in flows:
        ok, reason = flow("u-1")
        assert ok is False, flow.__name__
        assert "已结束" in reason, (flow.__name__, reason)


def test_help_text_documents_every_card_entry() -> None:
    source = ACTIVITY_MODULE.read_text(encoding="utf-8")
    start = source.index('ACTIVITY_HELP = """') + len('ACTIVITY_HELP = """')
    help_text = source[start : source.index('"""', start)]
    assert "活动中心" in help_text and "活动大厅" in help_text
    missing = sorted(target for target in _card_targets() if target not in help_text)
    assert missing == []
