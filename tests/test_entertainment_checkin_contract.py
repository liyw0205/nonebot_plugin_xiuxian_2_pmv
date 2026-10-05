from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMMANDS = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_entertainment/mod/newapi_commands.py"


def _function(source: str, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef:
    tree = ast.parse(source)
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


def test_manual_checkin_uses_feature_target_query_and_does_not_reload_legacy_accounts():
    source = COMMANDS.read_text(encoding="utf-8")
    handler = _function(source, "newapi_checkin_")

    called = _called_names(handler)
    assert "resolve_checkin_targets" in _referenced_names(handler)
    assert "run_blocking_io" in called
    assert "resolve_targets" not in called
    assert "load_accounts" not in called
    assert "_run_manual_checkin_for_target" in _referenced_names(handler)
    assert "_MAX_NEWAPI_CHECKIN_REPLY_BYTES" in source


def test_manual_checkin_worker_records_history_through_feature_application():
    source = COMMANDS.read_text(encoding="utf-8")
    worker = _function(source, "_run_checkin_for_target")

    called = _called_names(worker)
    assert "append_checkin_history" in called
    assert "do_checkin" in called


def test_scheduled_checkin_reads_bounded_feature_owned_pages_without_reloading_accounts():
    source = COMMANDS.read_text(encoding="utf-8")
    worker = _function(source, "_run_checkin_for_target")

    called = _called_names(worker)
    assert "load_accounts" not in called
    assert "append_checkin_history" in called
    scheduler = _function(source, "run_scheduled_auto_checkins")
    scheduler_calls = _referenced_names(scheduler)
    assert "list_auto_checkin_bindings" in scheduler_calls
    assert "iter_all_auto_checkin_bindings" not in scheduler_calls
