from __future__ import annotations

import ast
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py"
SOURCE = ROOT / SOURCE_PATH
SCOPE = ROOT / "docs/refactor_phase2_legacy_path_items.json"


def _tree() -> ast.Module:
    return ast.parse(SOURCE.read_text(encoding="utf-8"), filename=str(SOURCE))


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


def _top_level_functions(tree: ast.Module) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    return [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]


def _matcher_bindings(tree: ast.Module) -> dict[str, tuple[str, int]]:
    bindings: dict[str, tuple[str, int]] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        if _call_name(node.value) != "on_command" or not node.value.args:
            continue
        command = node.value.args[0]
        if not isinstance(command, ast.Constant) or not isinstance(command.value, str):
            continue
        targets = [target for target in node.targets if isinstance(target, ast.Name)]
        if len(targets) == 1:
            bindings[command.value] = (targets[0].id, node.lineno)
    return bindings


def _handler_for(tree: ast.Module, matcher: str) -> ast.AsyncFunctionDef:
    for node in _top_level_functions(tree):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
                continue
            if decorator.func.attr != "handle" or not isinstance(decorator.func.value, ast.Name):
                continue
            if decorator.func.value.id == matcher:
                return node
    raise AssertionError(f"no handler bound to {matcher}")


def _calls(node: ast.AST) -> list[ast.Call]:
    return [value for value in ast.walk(node) if isinstance(value, ast.Call)]


def _frozen_items() -> dict[str, dict]:
    payload = json.loads(SCOPE.read_text(encoding="utf-8"))
    return {item["id"]: item for item in payload["items"]}


def _edge(item: dict, path: str, line: int, owner: str) -> str:
    prefix = f"{path}:{line}"
    edges = [str(value) for value in item["call_graph"] if str(value).startswith(prefix)]
    assert len(edges) == 1, (prefix, edges)
    assert owner in edges[0], edges[0]
    return edges[0]


def _evidence_mentions_line(item: dict, path: str, line: int) -> None:
    pattern = re.compile(rf"^{re.escape(path)}:(\d+)(?:-(\d+))?(?:$|\s)")
    for value in item["evidence"]:
        match = pattern.match(str(value))
        if match and int(match.group(1)) <= line <= int(match.group(2) or match.group(1)):
            return
    raise AssertionError((path, line, item["evidence"]))


def test_sect_frozen_handlers_bind_current_matchers_and_application_owners():
    tree = _tree()
    matchers = _matcher_bindings(tree)
    items = _frozen_items()
    expected = {
        "加入宗门": ("join_sect", "join_sect_", "sect_application.join", "command:sect:加入宗门"),
        "学习宗门功法": (
            "sect_mainbuff_learn",
            "sect_mainbuff_learn_",
            "sect_application.learn_main",
            "command:sect:学习宗门功法",
        ),
    }

    for command, (matcher, handler_name, owner, item_id) in expected.items():
        assert command in matchers
        bound_matcher, matcher_line = matchers[command]
        assert bound_matcher == matcher
        handler = _handler_for(tree, matcher)
        assert handler.name == handler_name
        assert handler.lineno > matcher_line
        assert any(_call_name(call) == owner for call in _calls(handler))

        item = items[item_id]
        assert item["status"] == "已迁移"
        assert "unknown_edge" not in item
        _edge(item, SOURCE_PATH, matcher_line, matcher)
        _edge(item, SOURCE_PATH, handler.lineno, owner.replace("sect_application", "SectApplication"))
        _evidence_mentions_line(item, SOURCE_PATH, matcher_line)
        _evidence_mentions_line(item, SOURCE_PATH, handler.lineno)


def test_sect_mainbuff_handler_does_not_prefetch_legacy_service_or_buffinfo():
    tree = _tree()
    handler = _handler_for(tree, "sect_mainbuff_learn")
    calls = {_call_name(call) for call in _calls(handler)}
    assert "sect_application.learn_main" in calls
    assert "sect_mainbuff_learn_service.learn" not in calls
    assert "sql_message.updata_user_main_buff" not in calls
    assert "sql_message.get_user_buff" not in calls
    assert "get_sect_mainbuff_id_list" not in calls
    source = ast.get_source_segment(SOURCE.read_text(encoding="utf-8"), handler) or ""
    assert "UserBuffDate" not in source
    assert "BuffInfo" not in source


def test_sect_join_handler_does_not_use_legacy_mutation_service():
    handler = _handler_for(_tree(), "join_sect")
    calls = {_call_name(call) for call in _calls(handler)}
    assert "sect_application.join" in calls
    assert "sect_member_join_service.join" not in calls
    assert "sql_message.update_usr_sect" not in calls
    assert "can_join_sect" not in calls
