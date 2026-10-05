from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMMANDS = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_entertainment/mod/newapi_commands.py"
STORE = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_entertainment/mod/newapi_store.py"


def _function(path: Path, name: str):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name
    )


def _called_names(node: ast.AST) -> set[str]:
    return {
        call.func.id
        for call in ast.walk(node)
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
    }


def _referenced_names(node: ast.AST) -> set[str]:
    return {child.id for child in ast.walk(node) if isinstance(child, ast.Name)}


def test_newapi_state_owner_has_no_runtime_json_file_access():
    source = STORE.read_text(encoding="utf-8")
    assert "json_store" not in source
    assert "load_json_file" not in source
    assert "save_json_file" not in source
    assert "glob(" not in source
    assert "EntertainmentApplication" in source


def test_database_work_in_newapi_handlers_is_offloaded_from_event_loop():
    for name in (
        "newapi_bind_",
        "newapi_list_",
        "newapi_checkin_",
        "newapi_info_",
        "newapi_del_",
        "newapi_history_",
        "newapi_auto_",
    ):
        assert "run_blocking_io" in _called_names(_function(COMMANDS, name))


def test_auto_scheduler_uses_bounded_sql_pages_and_target_snapshots():
    scheduler = _function(COMMANDS, "run_scheduled_auto_checkins")
    referenced = _referenced_names(scheduler)
    assert "list_auto_checkin_bindings" in referenced
    assert "iter_all_auto_checkin_bindings" not in referenced
    assert "_run_checkin_for_account" not in referenced


def test_history_query_is_feature_owned_and_reply_is_bounded():
    formatter = _function(COMMANDS, "_format_checkin_history")
    assert "load_checkin_history" in _called_names(formatter)
    assert "_MAX_NEWAPI_LIST_RESPONSE_BYTES" in {
        node.id for node in ast.walk(formatter) if isinstance(node, ast.Name)
    }
