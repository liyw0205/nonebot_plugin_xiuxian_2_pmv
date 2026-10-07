from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_tianti/__init__.py"
PRESENTATION = ROOT / "nonebot_plugin_xiuxian_2/features/tianti_training/presentation.py"

COMMANDS = {
    "我的炼体": ("tianti_info", "_"),
    "我的体窍": ("tiqiao_info", "_"),
    "炼体境界": ("tianti_level_help", "_"),
    "炼体帮助": ("tianti_help", "_"),
}

STATE_MUTATIONS = {
    "apply_bath",
    "breakthrough",
    "grant_item_tianti",
    "open_qiaoxue",
    "settle",
    "train",
}


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Call):
        node = node.func
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _calls(node: ast.AST) -> list[ast.Call]:
    return [child for child in ast.walk(node) if isinstance(child, ast.Call)]


def _registered_matchers(tree: ast.Module) -> dict[str, tuple[str, ast.Call]]:
    result: dict[str, tuple[str, ast.Call]] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        if _call_name(node.value) != "on_command" or not node.value.args:
            continue
        command = node.value.args[0]
        if not isinstance(command, ast.Constant) or not isinstance(command.value, str):
            continue
        names = [target.id for target in node.targets if isinstance(target, ast.Name)]
        assert len(names) == 1, command.value
        result[command.value] = (names[0], node.value)
    return result


def _handler(tree: ast.Module, matcher: str) -> ast.AsyncFunctionDef:
    matches = []
    for node in tree.body:
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        for decorator in node.decorator_list:
            if (
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Attribute)
                and decorator.func.attr == "handle"
                and isinstance(decorator.func.value, ast.Name)
                and decorator.func.value.id == matcher
            ):
                matches.append(node)
    assert len(matches) == 1, (matcher, len(matches))
    return matches[0]


def _function(tree: ast.Module, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef:
    matches = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    ]
    assert len(matches) == 1, (name, len(matches))
    return matches[0]


def _has_call(node: ast.AST, name: str) -> bool:
    return any(_call_name(call) == name for call in _calls(node))


def test_frozen_tianti_commands_bind_to_the_expected_handlers() -> None:
    tree = _tree(SOURCE)
    matchers = _registered_matchers(tree)
    assert set(COMMANDS) <= set(matchers)

    for command, (expected_matcher, _) in COMMANDS.items():
        matcher, declaration = matchers[command]
        assert matcher == expected_matcher
        handlers = [
            node
            for node in tree.body
            if isinstance(node, ast.AsyncFunctionDef)
            and any(
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Attribute)
                and decorator.func.attr == "handle"
                and isinstance(decorator.func.value, ast.Name)
                and decorator.func.value.id == matcher
                for decorator in node.decorator_list
            )
        ]
        assert len(handlers) == 1
        assert handlers[0].name == COMMANDS[command][1]
        if command == "我的炼体":
            aliases = next(
                keyword.value
                for keyword in declaration.keywords
                if keyword.arg == "aliases"
            )
            assert isinstance(aliases, ast.Set)
            assert [item.value for item in aliases.elts] == ["炼体状态"]


def test_dynamic_tianti_entries_read_profile_and_delegate_display_rules() -> None:
    tree = _tree(SOURCE)
    status = _handler(tree, "tianti_info")
    qiaoxue = _handler(tree, "tiqiao_info")

    for handler in (status, qiaoxue):
        reads = [
            call
            for call in _calls(handler)
            if _call_name(call) == "tianti_training_application.read_profile"
        ]
        assert len(reads) == 1
        assert len(reads[0].args) == 1
        assert isinstance(reads[0].args[0], ast.Name)
        assert reads[0].args[0].id == "user_id"
        called_methods = {
            call.func.attr
            for call in _calls(handler)
            if isinstance(call.func, ast.Attribute)
            and isinstance(call.func.value, ast.Name)
            and call.func.value.id in {
                "tianti_training_application",
                "tianti_settlement_application",
            }
        }
        assert not (called_methods & STATE_MUTATIONS)

    assert _has_call(status, "calculate_tianti_gain_rate")
    assert _has_call(status, "_get_active_medicine_bath")
    assert _has_call(status, "get_sect_fairyland_bonus")
    assert _has_call(status, "_get_tianti_sect_fairyland_level")
    assert not _has_call(status, "_get_user_sect_fairyland_level")
    assert _has_call(status, "_get_tianti_cap")
    assert _has_call(qiaoxue, "calc_qiaoxue_bonus")

    bath_reader = _function(tree, "_get_active_medicine_bath")
    assert _has_call(bath_reader, "get_active_medicine_bath")
    cap_reader = _function(tree, "_get_tianti_cap")
    assert _has_call(cap_reader, "tianti_training_application.profile_cap")
    sect_level_reader = _function(tree, "_get_tianti_sect_fairyland_level")
    assert _has_call(sect_level_reader, "sect_application.get_sect_info")
    assert not _has_call(sect_level_reader, "get_user_sect_fairyland_level")

    presentation = _tree(PRESENTATION)
    owned_functions = {
        node.name
        for node in presentation.body
        if isinstance(node, ast.FunctionDef)
    }
    assert {
        "calc_qiaoxue_bonus",
        "calculate_tianti_gain_rate",
        "get_active_medicine_bath",
        "get_sect_fairyland_bonus",
    } <= owned_functions


def test_static_tianti_entries_only_render_help_text() -> None:
    tree = _tree(SOURCE)
    expected_renderers = {
        "tianti_help": "send_help_message",
        "tianti_level_help": "handle_send",
    }
    for matcher, renderer in expected_renderers.items():
        handler = _handler(tree, matcher)
        calls = {_call_name(call) for call in _calls(handler)}
        assert renderer in calls
        assert not any(
            name.startswith(("tianti_training_application.", "tianti_settlement_application."))
            for name in calls
        )
        assert not any(
            name.rsplit(".", 1)[-1] in STATE_MUTATIONS
            for name in calls
        )
        assert not any(name.endswith(".read_profile") for name in calls)
