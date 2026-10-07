from __future__ import annotations

import ast
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_lunhui/__init__.py"
SOURCE = ROOT / SOURCE_PATH
FEATURE = ROOT / "nonebot_plugin_xiuxian_2/features/lunhui"
LEGACY = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_lunhui"

COMMANDS = {
    "轮回重修帮助": ("warring_help", "warring_help_"),
    "自废修为": ("resetting", "resetting_"),
    "进入轮回": ("lunhui", "lunhui_"),
    "进入无限轮回": ("Infinite_reincarnation", "Infinite_reincarnation_"),
    "回忆前世": ("retrieve_memory", "_"),
    "轮回印记": ("view_memory", "_"),
    "确认轮回": ("confirm_lunhui", "confirm_lunhui_"),
}

FROZEN_STATUS = {
    "轮回重修帮助": "允许保留的兼容路径",
    "自废修为": "已迁移",
    "进入轮回": "允许保留的兼容路径",
    "进入无限轮回": "允许保留的兼容路径",
    "回忆前世": "允许保留的兼容路径",
    "轮回印记": "已迁移",
    "确认轮回": "已迁移",
}


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _call_name(call: ast.AST) -> str:
    node = call.func if isinstance(call, ast.Call) else call
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Call):
        node = node.func
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _top_level_functions(tree: ast.Module) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    return [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]


def _registered_matchers(tree: ast.Module) -> dict[str, tuple[str, ast.Call]]:
    result: dict[str, tuple[str, ast.Call]] = {}
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        value = node.value
        if not isinstance(value, ast.Call) or _call_name(value) != "on_command" or not value.args:
            continue
        command = value.args[0]
        if not isinstance(command, ast.Constant) or not isinstance(command.value, str):
            continue
        assert command.value not in result, command.value
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        matcher_names = [target.id for target in targets if isinstance(target, ast.Name)]
        assert len(matcher_names) == 1, command.value
        result[command.value] = (matcher_names[0], value)
    return result


def _matcher_handlers(
    tree: ast.Module,
) -> dict[str, list[ast.FunctionDef | ast.AsyncFunctionDef]]:
    result: dict[str, list[ast.FunctionDef | ast.AsyncFunctionDef]] = {}
    for function in _top_level_functions(tree):
        for decorator in function.decorator_list:
            if not isinstance(decorator, ast.Call):
                continue
            if not isinstance(decorator.func, ast.Attribute) or decorator.func.attr != "handle":
                continue
            if isinstance(decorator.func.value, ast.Name):
                result.setdefault(decorator.func.value.id, []).append(function)
    return result


def _function(
    tree: ast.Module, name: str, *, matcher: str | None = None
) -> ast.FunctionDef | ast.AsyncFunctionDef:
    functions = [node for node in _top_level_functions(tree) if node.name == name]
    if matcher is not None:
        functions = [
            node
            for node in functions
            if any(
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Attribute)
                and decorator.func.attr == "handle"
                and isinstance(decorator.func.value, ast.Name)
                and decorator.func.value.id == matcher
                for decorator in node.decorator_list
            )
        ]
    assert len(functions) == 1, (name, matcher, len(functions))
    return functions[0]


def _calls(node: ast.AST) -> list[ast.Call]:
    return [call for call in ast.walk(node) if isinstance(call, ast.Call)]


def _frozen_items() -> dict[str, dict]:
    payload = json.loads(
        (ROOT / "docs/refactor_phase2_legacy_path_items.json").read_text(encoding="utf-8")
    )
    return {item["id"]: item for item in payload["items"]}


def _has_call(node: ast.AST, name: str) -> bool:
    return any(_call_name(call) == name for call in _calls(node))


def _incremented_stat_fields(node: ast.AST) -> set[str]:
    fields = set()
    for call in _calls(node):
        if _call_name(call) != "_increment_stat" or len(call.args) < 3:
            continue
        field = call.args[2]
        fields.add(field.value if isinstance(field, ast.Constant) else "<dynamic>")
    return fields


def test_all_frozen_lunhui_commands_bind_to_the_current_default_matchers_and_handlers():
    tree = _tree(SOURCE)
    matchers = _registered_matchers(tree)
    handlers = _matcher_handlers(tree)
    items = _frozen_items()

    assert {item_id for item_id in items if item_id.startswith("command:lunhui:")} == {
        f"command:lunhui:{command}" for command in COMMANDS
    }
    for command, (expected_matcher, expected_handler) in COMMANDS.items():
        item_id = f"command:lunhui:{command}"
        item = items[item_id]
        matcher, declaration = matchers[command]
        assert matcher == expected_matcher
        bound = handlers[matcher]
        assert len(bound) == 1
        assert bound[0].name == expected_handler
        assert item["status"] == FROZEN_STATUS[command]
        assert item["source"] == {"feature": "lunhui", "name": command}


def test_reset_recall_and_settle_handlers_reach_their_feature_application_owners():
    tree = _tree(SOURCE)
    resetting = _function(tree, "resetting_", matcher="resetting")
    confirming = _function(tree, "confirm_lunhui_", matcher="confirm_lunhui")
    recall = _function(tree, "_", matcher="retrieve_memory")
    retrieve = _function(tree, "retrieve_reincarnation_skill")
    helpers = {node.name: node for node in _top_level_functions(tree)}

    assert _has_call(resetting, "lunhui_application.reset_result")
    assert any(
        _call_name(call) == "_run_lunhui_action"
        and call.args
        and isinstance(call.args[0], ast.Constant)
        and call.args[0].value == "reset"
        for call in _calls(resetting)
    )
    assert _has_call(confirming, "lunhui_application.settle_result")
    assert any(
        _call_name(call) == "_run_lunhui_action"
        and call.args
        and isinstance(call.args[0], ast.Constant)
        and call.args[0].value == "settle"
        for call in _calls(confirming)
    )
    assert _has_call(recall, "retrieve_reincarnation_skill")
    assert _has_call(retrieve, "lunhui_application.recall_result")
    assert _has_call(retrieve, "_run_lunhui_action")
    assert any(
        _call_name(call) == "_run_lunhui_action"
        and call.args
        and isinstance(call.args[0], ast.Constant)
        and call.args[0].value == "recall"
        for call in _calls(retrieve)
    )
    assert _has_call(helpers["_run_lunhui_action"], "lunhui_application.execute")
    assert not _has_call(resetting, "update_statistics_value")

    application = _tree(FEATURE / "application.py")
    # The class methods live under LunhuiApplication, so inspect its AST body.
    app_class = next(node for node in application.body if isinstance(node, ast.ClassDef) and node.name == "LunhuiApplication")
    methods = {node.name: node for node in app_class.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    for method in ("reset_result", "recall_result", "settle_result"):
        assert method in methods
        assert _has_call(methods[method], f"self.repository.{method}")

    repository = _tree(FEATURE / "repository.py")
    repository_class = next(
        node for node in repository.body
        if isinstance(node, ast.ClassDef) and node.name == "LunhuiRepository"
    )
    initializer = next(
        node for node in repository_class.body
        if isinstance(node, ast.FunctionDef) and node.name == "__init__"
    )
    handlers_argument = next(
        keyword.value for call in _calls(initializer)
        if _call_name(call) == "super.__init__"
        for keyword in call.keywords if keyword.arg == "handlers"
    )
    assert isinstance(handlers_argument, ast.Dict)
    handler_map = {
        key.value: _call_name(value)
        for key, value in zip(handlers_argument.keys, handlers_argument.values)
        if isinstance(key, ast.Constant)
    }
    assert handler_map == {
        "reset": "self._reset",
        "recall": "self._recall",
        "settle": "self._settle",
    }
    build_services = next(
        node for node in repository_class.body
        if isinstance(node, ast.FunctionDef) and node.name == "_build_services"
    )
    for service in ("CultivationResetService", "LunhuiRecallService", "LunhuiSettlementService"):
        assert _has_call(build_services, service)

    service_tree = _tree(LEGACY / "transaction_service.py")
    service_classes = {
        node.name: node for node in service_tree.body if isinstance(node, ast.ClassDef)
    }
    methods = {
        class_name: {
            node.name: node
            for node in service.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        for class_name, service in service_classes.items()
    }
    assert "自废修为次数" in _incremented_stat_fields(
        methods["CultivationResetService"]["reset"]
    )
    assert {"回忆前世次数", "<dynamic>"} <= _incremented_stat_fields(
        methods["LunhuiRecallService"]["recall"]
    )
    assert {"轮回次数", "<dynamic>"} <= _incremented_stat_fields(
        methods["LunhuiSettlementService"]["settle"]
    )
    increment_stat = next(
        node for node in service_tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_increment_stat"
    )
    assert "player_data.statistics" in ast.unparse(increment_stat)


def test_memory_display_uses_feature_reader_while_transactions_own_writes():
    tree = _tree(SOURCE)
    view = _function(tree, "_", matcher="view_memory")
    memory_reader = _function(tree, "get_reincarnation_memory")

    assert _has_call(view, "get_reincarnation_memory")
    assert _has_call(memory_reader, "lunhui_application.get_reincarnation_memory")

    application = _tree(FEATURE / "application.py")
    app_class = next(
        node for node in application.body
        if isinstance(node, ast.ClassDef) and node.name == "LunhuiApplication"
    )
    app_reader = next(
        node for node in app_class.body
        if isinstance(node, ast.FunctionDef) and node.name == "get_reincarnation_memory"
    )
    assert _has_call(app_reader, "self.repository.get_reincarnation_memory")

    repository = _tree(FEATURE / "repository.py")
    repository_class = next(
        node for node in repository.body
        if isinstance(node, ast.ClassDef) and node.name == "LunhuiRepository"
    )
    repository_reader = next(
        node for node in repository_class.body
        if isinstance(node, ast.FunctionDef) and node.name == "get_reincarnation_memory"
    )
    repository_source = ast.unparse(repository_reader)
    assert "SELECT * FROM reincarnation_memory WHERE user_id=?" in repository_source
    assert "DatabaseUnitOfWork" in repository_source

    service_tree = _tree(LEGACY / "transaction_service.py")
    services = {node.name: node for node in service_tree.body if isinstance(node, ast.ClassDef)}
    recall_service = services["LunhuiRecallService"]
    recall_method = next(node for node in recall_service.body if isinstance(node, ast.FunctionDef) and node.name == "recall")
    recall_source = ast.unparse(recall_method)
    assert "UPDATE player_data.reincarnation_memory" in recall_source
    settlement_service = services["LunhuiSettlementService"]
    settle_method = next(node for node in settlement_service.body if isinstance(node, ast.FunctionDef) and node.name == "settle")
    assert any(
        _call_name(call) == "_set_player_fields"
        and len(call.args) >= 2
        and isinstance(call.args[1], ast.Constant)
        and call.args[1].value == "reincarnation_memory"
        for call in _calls(settle_method)
    )
    assert _has_call(recall_method, "_increment_stat")
    assert "UPDATE player_data.reincarnation_memory" in ast.unparse(recall_method)


def test_entry_commands_share_the_ephemeral_confirmation_invite_owner():
    tree = _tree(SOURCE)
    regular = _function(tree, "lunhui_", matcher="lunhui")
    infinite = _function(tree, "Infinite_reincarnation_", matcher="Infinite_reincarnation")
    invite = _function(tree, "confirm_lunhui_invite")
    expire = _function(tree, "expire_confirm_lunhui_invite")

    assert _has_call(regular, "confirm_lunhui_invite")
    assert _has_call(infinite, "confirm_lunhui_invite")
    assert "confirm_lunhui_cache" in {node.id for node in ast.walk(invite) if isinstance(node, ast.Name)}
    assert "confirm_lunhui_cache" in {node.id for node in ast.walk(expire) if isinstance(node, ast.Name)}
    assert any(
        isinstance(node, ast.Call)
        and _call_name(node) == "asyncio.create_task"
        and node.args
        and isinstance(node.args[0], ast.Call)
        and _call_name(node.args[0]) == "expire_confirm_lunhui_invite"
        for node in _calls(invite)
    )


def test_help_command_remains_a_static_compatibility_reply():
    tree = _tree(SOURCE)
    help_handler = _function(tree, "warring_help_", matcher="warring_help")
    assert _has_call(help_handler, "send_help_message")
    assert not any(
        _call_name(call).startswith("lunhui_application.")
        or _call_name(call) == "_run_lunhui_action"
        for call in _calls(help_handler)
    )
    assert any(isinstance(node, ast.Name) and node.id == "__warring_help__" for node in ast.walk(help_handler))
