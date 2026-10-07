"""Source contracts for the default ``impart`` command boundary.

The frozen Phase 2 members are all registered by the same compatibility
module.  Keeping this small contract test next to the command entrypoint
makes it difficult to move one command to a second owner (or accidentally
leave a legacy transaction service call in a handler) while the remaining
members are being migrated.
"""

from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
IMPART_SOURCE = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_impart/__init__.py"


COMMAND_HANDLERS = {
    "传承祈愿": "impart_draw_",
    "传承抽卡": "impart_draw2_",
    "传承背包": "impart_back_",
    "传承信息": "impart_info_",
    "传承帮助": "impart_help_",
    "虚神界帮助": "impart_pk_help_",
    "传承合成": "impart_compose_",
    "传承分解": "impart_disassemble_",
    "加载传承数据": "re_impart_load_",
    "传承卡图": "impart_img_",
}


def _tree() -> ast.Module:
    return ast.parse(IMPART_SOURCE.read_text(encoding="utf-8"))


def _functions() -> dict[str, ast.AsyncFunctionDef | ast.FunctionDef]:
    return {
        node.name: node
        for node in _tree().body
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
    }


def _calls(node: ast.AST) -> set[str]:
    names: set[str] = set()
    for call in ast.walk(node):
        if isinstance(call, ast.Call):
            if isinstance(call.func, ast.Name):
                names.add(call.func.id)
            elif isinstance(call.func, ast.Attribute):
                names.add(call.func.attr)
    return names


def _text(node: ast.AST) -> str:
    return ast.unparse(node)


def _registered_matchers() -> dict[str, str]:
    result: dict[str, str] = {}
    for node in _tree().body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        value = node.value
        if not isinstance(value, ast.Call) or not isinstance(value.func, ast.Name):
            continue
        if value.func.id != "on_command" or not value.args:
            continue
        command = value.args[0]
        if not isinstance(command, ast.Constant) or not isinstance(command.value, str):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for target in targets:
            if isinstance(target, ast.Name):
                result[str(command.value)] = target.id
    return result


def test_all_frozen_impart_commands_have_one_default_handler_binding():
    """The ten frozen members must remain visible in the default matcher index."""

    registered = _registered_matchers()
    functions = _functions()
    assert set(COMMAND_HANDLERS) <= set(registered)
    for command, handler in COMMAND_HANDLERS.items():
        matcher_name = registered[command]
        assert matcher_name
        assert handler in functions
        decorated = {
            decorator.func.attr
            for decorator in functions[handler].decorator_list
            if isinstance(decorator, ast.Call)
            and isinstance(decorator.func, ast.Attribute)
            and isinstance(decorator.func.value, ast.Name)
        }
        assert "handle" in decorated


def test_impart_mutation_handlers_use_the_feature_application_boundary():
    """Mutation ingress must terminate at ``ImpartApplication`` methods."""

    functions = _functions()
    expected = {
        "impart_draw_": {"crystal_draw"},
        "impart_draw2_": {"draw"},
        "use_wishing_stone": {"prayer_settle"},
        "use_love_sand": {"love_sand"},
        "impart_compose_": {"compose"},
        "impart_disassemble_": {"disassemble"},
    }
    for handler, owner_names in expected.items():
        calls = _calls(functions[handler])
        assert owner_names <= calls, (handler, owner_names - calls)
        assert "impart_application." in _text(functions[handler]), handler

    # These are compatibility-only services.  No default mutation handler
    # should construct or invoke them after the application hand-off.
    for handler in expected:
        calls = _calls(functions[handler])
        assert not {
            "_impart_draw_service",
            "_card_compose_service",
            "_card_disassemble_service",
            "_love_sand_service",
            "impart_check",
            "data_person_list",
            "data_all",
            "data_all_keys",
        } & calls, handler


def test_impart_display_handlers_are_read_only():
    """Display-only commands must not acquire a transaction service or write."""

    functions = _functions()
    display_handlers = (
        "impart_help_",
        "impart_pk_help_",
        "impart_back_",
        "impart_info_",
        "impart_img_",
    )
    forbidden = {
        "_impart_draw_service",
        "_card_compose_service",
        "_card_disassemble_service",
        "_love_sand_service",
        "update_statistics_value",
        "update_user_impart_data",
        "re_impart_data",
        "impart_check",
        "data_person_list",
        "data_all",
        "data_all_keys",
        "savef",
    }
    expected_owner = {
        "impart_back_": "cards",
        "impart_info_": "state",
        "impart_img_": "card_definitions",
    }
    for handler in display_handlers:
        assert not forbidden & _calls(functions[handler]), handler
    for handler, owner_method in expected_owner.items():
        calls = _calls(functions[handler])
        assert owner_method in calls or "_impart_state" in calls, (handler, owner_method)
        if "_impart_state" in calls:
            assert "impart_application.state" in _text(_tree()), handler
        else:
            assert "impart_application." in _text(functions[handler]), handler
    for handler in display_handlers:
        for call in ast.walk(functions[handler]):
            if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Name):
                continue
            if call.func.id != "_impart_state":
                continue
            assert not any(
                keyword.arg == "ensure"
                and isinstance(keyword.value, ast.Constant)
                and keyword.value.value is True
                for keyword in call.keywords
            ), handler


def test_impart_reload_handler_has_an_explicit_application_owner():
    """The reload command is an effectful command, not an unowned read helper."""

    handler = _functions()["re_impart_load_"]
    assert "refresh" in _calls(handler)
    assert "impart_application." in _text(handler)
    assert not {"re_impart_data", "impart_check"} & _calls(handler)
