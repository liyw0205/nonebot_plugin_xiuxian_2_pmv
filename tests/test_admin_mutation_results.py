from __future__ import annotations

import __future__
import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from nonebot_plugin_xiuxian_2.features.admin_asset.application import AdminAssetApplication


ROOT = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
ADMIN = ROOT / "xiuxian_admin/__init__.py"
CASES = [
    ("_destroy_admin_item", "destroy_item", (None, "u", 101, {"name": "item"}, 1, 2, "name")),
    ("_adjust_admin_exp", "adjust_exp", (None, "u", 100, 10, "name")),
    ("_adjust_admin_level", "change_level", (None, "u", ("level", 100, 50, 100, 10, 100, "root", 0), "new", 200, 1, 1)),
    ("_adjust_admin_root", "change_root", (None, "u", ("root", "type", 0, "level", 100, 100, "name"), 1, 1, 1)),
]


def _function(name, namespace, *, source=ADMIN, parent=None):
    tree = ast.parse(source.read_text(encoding="utf-8"))
    if parent:
        tree = next(node for node in tree.body if getattr(node, "name", None) == parent)
    node = next(node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name)
    node.decorator_list = []
    namespace = {
        "SimpleNamespace": SimpleNamespace, "CommandArg": lambda: None,
        "_admin_operation_id": lambda *args: "operation", "get_user_id": lambda event: "operator",
        **namespace,
    }
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), "exec", flags=__future__.annotations.compiler_flag), namespace)
    return namespace[name]


@pytest.mark.parametrize("name,method,args", CASES)
@pytest.mark.parametrize("status", ["applied", "duplicate", "schema_missing"])
def test_real_asset_helpers_accept_status_in_application_payload(name, method, args, status):
    payload = {"status": status, "value": 7}
    outcome = SimpleNamespace(data=payload, status=status, ok=status != "schema_missing")
    application = SimpleNamespace(**{method: Mock(return_value=outcome)})
    result = _function(name, {"admin_asset_application": application})(*args)
    assert (result.status, result.succeeded, result.value) == (status, outcome.ok, 7)
    assert payload == {"status": status, "value": 7}


@pytest.mark.parametrize("name,method,args", CASES)
def test_real_asset_helpers_call_default_application_for_missing_schema(tmp_path, name, method, args):
    application = AdminAssetApplication(tmp_path / "missing.db")
    result = _function(name, {"admin_asset_application": application})(*args)
    assert (result.status, result.succeeded) == ("schema_missing", False)
    assert not (tmp_path / "missing.db").exists()


@pytest.mark.parametrize("status,ok,success", [
    ("renamed", True, True), ("duplicate", True, True), ("unchanged", True, False),
    ("schema_missing", False, False), ("name_conflict", False, False),
    ("operation_conflict", False, False), ("user_missing", False, False),
])
def test_real_rename_handler_never_prefixes_failure_as_success(status, ok, success):
    output = []

    async def assign_bot(**kwargs):
        return kwargs["bot"], None

    async def handle_send(bot, event, message):
        output.append(message)

    handler = _function("admin_rename_cmd_", {
        "assign_bot": assign_bot, "handle_send": handle_send, "get_at_user_id": lambda args: "u",
        "_sql_message": lambda: SimpleNamespace(
            get_user_info_with_id=lambda uid: {"user_id": "u", "user_name": "old"},
            get_user_info_with_name=lambda name: None,
        ),
        "admin_base_application": SimpleNamespace(rename=lambda **kwargs: SimpleNamespace(
            data={"status": status}, status=status, code=status, ok=ok, message="failure",
        )),
    })
    asyncio.run(handler(None, None, SimpleNamespace(extract_plain_text=lambda: "new")))
    assert len(output) == 1
    assert output[0].startswith("已将") == success


@pytest.mark.parametrize("parent,status", [
    ("ccll_command_", "in_progress"), ("ccll_command_", "schema_missing"),
    ("training_reset_", "schema_missing"),
])
def test_real_batch_completion_callbacks_reject_non_success(parent, status):
    callback = _function("_done", {"action": "增加", "number_to": str}, parent=parent)
    message = callback(SimpleNamespace(status=status, succeeded=False))
    assert "完成！" not in message
    assert "重置完成" not in message


def test_real_boss_reset_loop_logs_failure_instead_of_completion():
    logger = Mock()
    result = SimpleNamespace(status="schema_missing", task_status="", succeeded=False)
    reset = _function("set_boss_limits_reset", {
        "boss_application": SimpleNamespace(reset_daily_limit=lambda *args, **kwargs: result),
        "logger": logger,
    }, source=ROOT / "xiuxian_boss/__init__.py")
    assert asyncio.run(reset("2026-10-07")) is result
    logger.error.assert_called_once()
    logger.opt.assert_not_called()


@pytest.mark.parametrize("error", ["user_missing", "inventory_full"])
def test_real_clear_all_facade_reports_rolled_back_failure(error):
    service = Mock()
    service.clear_all.side_effect = ValueError(error)
    clear = _function("clear_all_xiangyuan", {
        "_xiangyuan_settlement_service": lambda: service,
        "XiuConfig": lambda: SimpleNamespace(max_goods_num=10),
    }, source=ROOT / "xiuxian_base/xiangyuan.py")
    assert "已回滚" in asyncio.run(clear())
