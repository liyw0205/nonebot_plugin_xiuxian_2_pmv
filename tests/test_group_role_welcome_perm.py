"""群身份识别：QQ author.member_role / OneBot sender.role。"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace


MODULE_PATH = (
    Path(__file__).parents[1]
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "adapter_compat.py"
)


def _load_role_helpers():
    source = MODULE_PATH.read_text(encoding="utf-8")
    # 抽纯函数段，避免加载 nonebot 依赖
    start = source.index("def _normalize_group_role")
    end = source.index("\ndef _patch_qq_reference_fields", start)
    ns: dict = {"Any": object, "BaseEvent": object}
    # CompatSender not needed
    exec(source[start:end], ns)
    return ns


def test_normalize_group_role():
    m = _load_role_helpers()
    assert m["_normalize_group_role"]("owner") == "owner"
    assert m["_normalize_group_role"]("admin") == "admin"
    assert m["_normalize_group_role"]("ADMIN") == "admin"
    assert m["_normalize_group_role"]("member") == "member"
    assert m["_normalize_group_role"](None) == "member"
    assert m["_normalize_group_role"]("4") == "owner"
    assert m["_normalize_group_role"]("5") == "admin"


def test_extract_qq_author_overrides_wrong_sender():
    m = _load_role_helpers()
    event = SimpleNamespace(
        sender=SimpleNamespace(role="member"),
        author=SimpleNamespace(member_role="admin"),
        member=None,
    )
    assert m["_extract_event_member_role"](event) == "admin"
    assert m["is_group_admin_or_owner"](event) is True


def test_extract_ob11_sender_role():
    m = _load_role_helpers()
    event = SimpleNamespace(
        sender=SimpleNamespace(role="admin"),
        author=None,
        member=None,
    )
    assert m["_extract_event_member_role"](event) == "admin"
    assert m["is_group_admin_or_owner"](event) is True
    event.sender.role = "member"
    assert m["is_group_admin_or_owner"](event) is False


def test_extract_guild_roles_list():
    m = _load_role_helpers()
    event = SimpleNamespace(
        sender=None,
        author=None,
        member=SimpleNamespace(roles=["1", "4"]),
    )
    assert m["_extract_event_member_role"](event) == "owner"


def test_welcome_commands_not_superuser_only():
    text = (
        Path(__file__).parents[1]
        / "nonebot_plugin_xiuxian_2"
        / "xiuxian"
        / "xiuxian_admin"
        / "group_welcome.py"
    ).read_text(encoding="utf-8")
    assert "permission=SUPERUSER" not in text.split("welcome_enable_cmd")[1].split("welcome_disable_cmd")[0]
    # both commands no SUPERUSER permission kw
    block = text[text.index("# ---------- 开关指令"):]
    assert "permission=SUPERUSER" not in block
    assert "_can_toggle_welcome" in block
    assert "is_group_admin_or_owner" in block
