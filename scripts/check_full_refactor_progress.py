#!/usr/bin/env python3
"""Emit quantitative evidence for the second-stage full refactor.

The report distinguishes entry-point cutover from implementation removal and
includes the frozen Phase 2 scope result. Enforce that scope with
``phase2_legacy_path_gate.py --check``; P7 release evidence stays independent.
"""

from __future__ import annotations

import argparse
import ast
from functools import lru_cache
import json
import operator
import re
from pathlib import Path

try:
    from .phase2_legacy_path_gate import load_phase2_scope_report
except ImportError:
    from phase2_legacy_path_gate import load_phase2_scope_report

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "nonebot_plugin_xiuxian_2"

PATTERNS = {
    "db_backend_connect_files": "db_backend.connect",
    "sqlite3_connect_files": "sqlite3.connect",
    "legacy_service_import_files": "transaction_service",
    "legacy_handle_import_files": "xiuxian2_handle",
    "direct_random_files": "random.",
    "datetime_now_files": "datetime.now",
    "time_now_files": "time.time",
}

# The production AST index is only queried for these legacy bank symbols.
# A source-token prefilter avoids compiling unrelated vendor and feature files;
# matching files still go through the same AST checks below.
_PRODUCTION_AST_TOKENS = (
    "get_legacy_info",
    "legacy_record_status",
    "LegacyBankRepository",
    "legacy_bank_account_storage",
    "xiuxian_bank",
    "savef",
)


@lru_cache(maxsize=512)
def _parse_source(source: str) -> ast.Module:
    """Parse one source string once for all slice checks in this process."""
    return ast.parse(source)


@lru_cache(maxsize=512)
def _walk(tree: ast.AST) -> tuple[ast.AST, ...]:
    """Materialize an AST traversal once; checks never mutate their trees."""
    return tuple(ast.walk(tree))


_FOLDED_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.FloorDiv: operator.floordiv,
    ast.Div: operator.truediv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.LShift: operator.lshift,
    ast.RShift: operator.rshift,
    ast.BitOr: operator.or_,
    ast.BitXor: operator.xor,
}


def _constant_value(node: ast.AST) -> object:
    """Fold a literal or arithmetic-on-literals expression, else return ``None``.

    Byte caps are written as ``16 * 1024 * 1024``; ``ast.literal_eval`` refuses
    multiplication, so the gate folds those binary operations itself instead of
    falling back to comparing the source text of the expression.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float, str, bytes, bool, tuple)):
        return node.value
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        inner = _constant_value(node.operand)
        if not isinstance(inner, (int, float)) or isinstance(inner, bool):
            return None
        return -inner if isinstance(node.op, ast.USub) else +inner
    if isinstance(node, ast.BinOp) and type(node.op) in _FOLDED_OPERATORS:
        left, right = _constant_value(node.left), _constant_value(node.right)
        numbers = (int, float)
        if isinstance(left, numbers) and isinstance(right, numbers):
            try:
                return _FOLDED_OPERATORS[type(node.op)](left, right)
            except (ValueError, ZeroDivisionError, OverflowError):
                return None
    return None


def _schema_constant_bound(contract_source: str, consumer_source: str, name: str, value: object) -> bool:
    """Prove one durable cap is declared in ``schemas.py`` and consumed from there.

    These caps used to be proven by matching their literal definition inside the
    implementing module.  Once a slice moved a cap into its ``schemas.py`` that
    text stopped being evidence, so the gate now binds three facts instead: the
    single declaration with the expected value, the package-relative import in the
    consumer, and a real use site so an unused import cannot pass.
    """
    try:
        contract_tree = _parse_source(contract_source)
        consumer_tree = _parse_source(consumer_source)
    except SyntaxError:
        return False
    declared = False
    for node in contract_tree.body:
        targets = (
            [node.target] if isinstance(node, ast.AnnAssign) else list(node.targets)
            if isinstance(node, ast.Assign)
            else []
        )
        if not any(isinstance(target, ast.Name) and target.id == name for target in targets):
            continue
        declared = _constant_value(node.value) == value
        break
    imported = any(
        isinstance(node, ast.ImportFrom)
        and node.level == 1
        and node.module == "schemas"
        and any(alias.name == name for alias in node.names)
        for node in _walk(consumer_tree)
    )
    used = any(
        isinstance(node, ast.Name) and node.id == name for node in _walk(consumer_tree)
    )
    return declared and imported and used


@lru_cache(maxsize=None)
def _read_source(path: Path) -> str | None:
    """Read a package source file once during one progress report."""
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None


@lru_cache(maxsize=1)
def _py_files() -> tuple[Path, ...]:
    return tuple(sorted(PACKAGE.rglob("*.py")))


def _counts() -> dict[str, int]:
    files = _py_files()
    sources = {path: (_read_source(path) or "") for path in files}
    counts = {"python_files": len(files)}
    for name, token in PATTERNS.items():
        counts[name] = sum(token in sources[path] for path in files)
    transaction_files = list(PACKAGE.rglob("*transaction_service.py"))
    counts["transaction_service_files"] = len(transaction_files)
    counts["transaction_service_lines"] = sum(
        len(sources[path].splitlines()) for path in transaction_files
    )
    handle = PACKAGE / "xiuxian" / "xiuxian_utils" / "xiuxian2_handle.py"
    counts["xiuxian2_handle_bytes"] = handle.stat().st_size if handle.is_file() else 0
    return counts


@lru_cache(maxsize=1)
def _production_ast_index() -> tuple[tuple[Path, frozenset[str], bool], ...]:
    """Index calls and bank imports in one package-wide AST traversal."""
    indexed: list[tuple[Path, frozenset[str], bool]] = []
    for path in _py_files():
        source = _read_source(path)
        if source is None:
            continue
        if not any(token in source for token in _PRODUCTION_AST_TOKENS):
            continue
        try:
            tree = _parse_source(source)
        except SyntaxError:
            continue
        call_targets: set[str] = set()
        bank_savef_import = False
        for node in _walk(tree):
            if isinstance(node, ast.Call):
                target = node.func.id if isinstance(node.func, ast.Name) else (
                    node.func.attr if isinstance(node.func, ast.Attribute) else ""
                )
                if target:
                    call_targets.add(target)
            elif isinstance(node, ast.ImportFrom) and node.module:
                if (
                    node.module.endswith("xiuxian_bank")
                    or node.module.endswith("legacy_bank_account_storage")
                ) and any(alias.name == "savef" for alias in node.names):
                    bank_savef_import = True
                if node.module.endswith("compatibility") and any(
                    alias.name == "legacy_bank_account_storage" for alias in node.names
                ):
                    bank_savef_import = True
            elif isinstance(node, ast.Import):
                if any(
                    alias.name.endswith("xiuxian_bank")
                    or alias.name.endswith("legacy_bank_account_storage")
                    for alias in node.names
                ):
                    bank_savef_import = True
        indexed.append((path, frozenset(call_targets), bank_savef_import))
    return tuple(indexed)


def _has_production_call(*names: str, excluding: set[Path] | None = None) -> bool:
    excluded = excluding or set()
    requested = frozenset(names)
    for path, call_targets, _ in _production_ast_index():
        if path in excluded:
            continue
        if call_targets.intersection(requested):
            return True
    return False


def _has_production_bank_savef_import() -> bool:
    facade = PACKAGE / "xiuxian" / "xiuxian_bank" / "__init__.py"
    writer = PACKAGE / "compatibility" / "legacy_bank_account_storage.py"
    for path, _, bank_savef_import in _production_ast_index():
        if path in {facade, writer}:
            continue
        if bank_savef_import:
            return True
    return False


def _admin_broadcast_owner_status(sources: dict[str, str]) -> dict[str, object]:
    """Check the broadcast ownership edges without importing runtime services."""
    trees = {name: _parse_source(source) for name, source in sources.items()}
    functions = {
        name: {
            node.name: node for node in _walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        for name, tree in trees.items()
    }

    def nodes(source, function):
        node = functions.get(source, {}).get(function)
        return list(_walk(node)) if node is not None else []

    def calls(source, function, target):
        return [node for node in nodes(source, function)
                if isinstance(node, ast.Call) and ast.unparse(node.func) == target]

    def expression(source, function, expected):
        return any(ast.unparse(node) == expected for node in nodes(source, function))

    def uses_lock(function):
        return any(
            isinstance(node, ast.With)
            and any(ast.unparse(item.context_expr) == "self._lock" for item in node.items)
            for node in nodes("repository", function)
        )

    def permission(command):
        for node in _walk(trees["handlers"]):
            if not isinstance(node, ast.Call) or ast.unparse(node.func) != "on_command" or not node.args:
                continue
            if not isinstance(node.args[0], ast.Constant) or node.args[0].value != command:
                continue
            return next((ast.unparse(item.value) for item in node.keywords if item.arg == "permission"), "")
        return None

    def identity_forwarded(function, method):
        delegated = calls("facade", function, f"_application().{method}")
        return bool(
            calls("facade", function, "_get_adapter_name")
            and calls("facade", function, "_get_bot_self_id")
            and delegated and delegated[0].args and ast.unparse(delegated[0].args[0]) == "bot"
            and {item.arg: ast.unparse(item.value) for item in delegated[0].keywords}.items()
            >= {"adapter": "adapter", "bot_id": "bot_id"}.items()
            and all(node.args and ast.unparse(node.args[0]) == "bot" for name in ("_get_adapter_name", "_get_bot_self_id")
                    for node in calls("facade", function, name))
        )

    def ledger_branch(status, prefix):
        return any(
            isinstance(node, ast.If) and ast.unparse(node.test) == f"status == '{status}'"
            and any(
                isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute)
                and child.func.attr == "add" and ast.unparse(child.func.value).startswith(f"task[f'{prefix}_")
                for statement in node.body for child in _walk(statement)
            )
            for node in nodes("repository", "finish")
        )

    def scoped_history(function):
        queries = calls("history", function, "uow.query_all")
        literals = " ".join(node.value for node in nodes("history", function)
                            if isinstance(node, ast.Constant) and isinstance(node.value, str))
        return bool(queries and "adapter=? AND bot_id=?" in literals and all(
            len(query.args) >= 2 and isinstance(query.args[1], ast.Tuple)
            and [ast.unparse(value) for value in query.args[1].elts[:2]] == ["adapter", "bot_id"]
            for query in queries
        ))

    handler_edges = {
        "group_broadcast_cmd_": "start_broadcast", "private_broadcast_cmd_": "start_broadcast",
        "global_broadcast_cmd_": "start_broadcast", "view_broadcast_cmd_": "format_broadcast_status",
        "cancel_broadcast_cmd_": "cancel_broadcast", "clear_broadcast_cmd_": "clear_broadcast",
    }
    delegates = {
        "start_broadcast": "start", "format_broadcast_status": "status", "cancel_broadcast": "cancel",
        "clear_broadcast": "clear", "auto_patch_broadcast_for_event": "patch_event",
    }
    facade_constructors = [node for node in _walk(trees["facade"])
                           if isinstance(node, ast.Call) and ast.unparse(node.func) == "AdminBroadcastRepository"]
    application_constructors = [node for node in _walk(trees["facade"])
                                if isinstance(node, ast.Call) and ast.unparse(node.func) == "AdminBroadcastApplication"]
    history_calls = calls("application", "start", "self.history")
    create_calls = calls("application", "start", "self.repository.create")
    claim_calls = calls("application", "_deliver", "self.repository.claim")
    send_calls = calls("application", "_deliver", "self.sender")
    cancellation_handlers = [node for node in nodes("application", "_deliver")
                             if isinstance(node, ast.ExceptHandler) and node.type is not None
                             and ast.unparse(node.type) == "asyncio.CancelledError"]
    readonly_uows = calls("history", "targets", "DatabaseUnitOfWork")
    qq_literals = " ".join(node.value for node in nodes("history", "_qq_targets")
                           if isinstance(node, ast.Constant) and isinstance(node.value, str))
    report = {
        "six_admin_handlers_and_compatibility_calls_use_one_memory_owner": (
            all(calls("handlers", handler, target) for handler, target in handler_edges.items())
            and all(permission(command) == "SUPERUSER" for command in
                    ("群聊广播", "私聊广播", "全局广播", "查看广播", "取消广播", "清空广播"))
            and all(calls("facade", function, f"_application().{method}") for function, method in delegates.items())
            and len(facade_constructors) == len(application_constructors) == 1
            and bool(application_constructors[0].args)
            and ast.unparse(application_constructors[0].args[0]) == "_broadcast_repository"
            and {item.arg: ast.unparse(item.value) for item in application_constructors[0].keywords}.items()
            >= {"history": "_history_targets", "sender": "_send_broadcast_to_target"}.items()
            and expression("facade", "_application", "return _broadcast_application")
            and calls("web", "api_messages_broadcast", "start_broadcast")
            and calls("web", "api_messages_broadcast_status", "format_broadcast_status")
            and calls("events", "do_something", "auto_patch_broadcast_for_event")
            and not any(isinstance(node, ast.Name) and node.id in {"BROADCAST_TASKS", "connect_message_db"}
                        for node in _walk(trees["facade"]))
        ),
        "feature_lifecycle_claims_and_inflight_cancellation_are_atomic": (
            all(uses_lock(name) for name in ("create", "claim", "finish", "cancel", "clear", "status"))
            and not any(isinstance(node, ast.Await) for node in _walk(trees["repository"]))
            and bool(history_calls and create_calls and history_calls[0].lineno < create_calls[0].lineno)
            and bool(claim_calls and send_calls and claim_calls[0].lineno < send_calls[0].lineno)
            and expression("repository", "claim", "task['_generation'] != handle.generation")
            and expression("repository", "claim", "task['canceled']")
            and expression("repository", "claim", "task['_inflight'][key] = claim")
            and expression("repository", "finish", "del task['_inflight'][key]")
            and expression("repository", "cancel", "task['canceled'] = True")
            and calls("repository", "claim", "self._cleanup")
            and not calls("repository", "claim", "self._snapshot")
            and calls("repository", "_snapshot", "copy.deepcopy")
        ),
        "pending_failures_and_cancelled_coroutines_do_not_report_false_success": (
            ledger_branch("sent", "sent") and ledger_branch("pending_audit", "pending")
            and expression("repository", "claim", "key in task[f'pending_{bucket}']")
            and any(
                any(isinstance(child, ast.Raise) for child in _walk(handler))
                and any(isinstance(child, ast.Call) and ast.unparse(child.func) == "self.repository.finish"
                        and any(item.arg == "status" and isinstance(item.value, ast.Constant)
                                and item.value.value == "failed" for item in child.keywords)
                        for child in _walk(handler))
                for handler in cancellation_handlers
            )
            and expression("application", "_deliver", "status not in {'sent', 'pending_audit'}")
            and not any(ast.unparse(node) == "str(exc)" for node in nodes("application", "_deliver"))
            and expression("repository", "finish", "del task['errors'][:-50]")
        ),
        "history_queries_are_readonly_bot_scoped_and_off_the_event_loop": (
            bool(readonly_uows) and all(any(item.arg == "read_only" and isinstance(item.value, ast.Constant)
                                           and item.value.value is True for item in node.keywords)
                                      for node in readonly_uows)
            and scoped_history("_qq_targets") and scoped_history("_ob11_targets")
            and all(token in qq_literals for token in ("direction='recv'", "created_at>=?", "message_id<>''"))
            and calls("history", "targets", "adapter_family")
            and calls("facade", "_history_targets", "asyncio.to_thread")
            and not any(isinstance(node, ast.Constant) and isinstance(node.value, str)
                        and any(token in node.value.upper() for token in ("CREATE TABLE", "ALTER TABLE", "SELECT *"))
                        for node in _walk(trees["history"]))
        ),
        "sender_identity_and_result_status_are_preserved_through_ports": (
            identity_forwarded("start_broadcast", "start")
            and identity_forwarded("auto_patch_broadcast_for_event", "patch_event")
            and expression("repository", "claim", "adapter != task['adapter']")
            and expression("repository", "claim", "bot_id != task['bot_id']")
            and calls("facade", "_send_qq_broadcast_by_reply", "delivery_service.send")
            and any(isinstance(node, ast.Return) and isinstance(node.value, ast.Await)
                    and isinstance(node.value.value, ast.Call)
                    and ast.unparse(node.value.value.func) == "delivery_service.send"
                    for node in nodes("facade", "_send_qq_broadcast_by_reply"))
            and not any(isinstance(node, ast.ImportFrom) and (node.module or "").startswith("nonebot")
                        for name in ("application", "repository") for node in _walk(trees[name]))
        ),
        "entrypoint_and_race_regressions_have_behavioral_tests": (
            all(name in functions["entry_tests"] for name in (
                "test_six_handlers_share_facade_and_state_owner",
                "test_qq_delivery_preserves_reply_markdown_and_pending_state",
                "test_event_patch_rejects_other_bot_and_sends_new_targets",
            ))
            and all(name in functions["core_tests"] for name in (
                "test_two_patch_events_claim_the_same_target_once",
                "test_stopping_initial_send_prevents_all_later_targets_but_cannot_revoke_inflight",
                "test_cancelled_coroutine_releases_claim_and_propagates_cancellation",
            ))
            and "test_adapter_and_bot_id_are_strictly_isolated" in functions["history_tests"]
        ),
        "status": "broadcast_lifecycle_and_target_claims_are_feature_owned_with_readonly_bot_scoped_history",
    }
    return {key: value if key == "status" else bool(value) for key, value in report.items()}


def _admin_qqid_owner_status(sources: dict[str, str]) -> dict[str, object]:
    """Check ownership edges; recovery behavior is exercised with real databases."""
    trees = {name: _parse_source(source) for name, source in sources.items()}

    def calls(source, function=None):
        tree = trees[source]
        if function is not None:
            tree = next((node for node in _walk(tree)
                         if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                         and node.name == function), ast.Module(body=[], type_ignores=[]))
        return [node for node in _walk(tree) if isinstance(node, ast.Call)]

    def methods(source):
        return {node.func.attr for node in calls(source) if isinstance(node.func, ast.Attribute)}

    registrations = [node for node in calls("handlers")
                     if ast.unparse(node.func) == "on_command" and node.args
                     and isinstance(node.args[0], ast.Constant) and node.args[0].value == "转换QQID"]
    candidate_uows = [node for node in calls("candidate")
                      if ast.unparse(node.func) == "DatabaseUnitOfWork"]
    compatibility_calls = calls("compatibility", "migrate_user_id_to_openid")
    return {
        "superuser_handler_and_compatibility_use_feature_application": bool(
            len(registrations) == 1
            and any(item.arg == "permission" and ast.unparse(item.value) == "SUPERUSER"
                    for item in registrations[0].keywords)
            and any(ast.unparse(node.func) == "asyncio.to_thread" and node.args
                    and ast.unparse(node.args[0]) == "admin_qqid_application.run"
                    for node in calls("handlers", "migrate_qqid_cmd_"))
            and any(isinstance(node.func, ast.Attribute) and node.func.attr == "run"
                    for node in compatibility_calls)
            and not any(ast.unparse(node.func) == "_update_ids" for node in compatibility_calls)
        ),
        "batch_plans_and_checkpoints_delegate_to_existing_id_writer": (
            {"exclusive_run", "get_active", "create", "bind_request", "freeze_resolution", "record_result", "update_user_id"}
            <= methods("application")
            and not {"connect", "rename", "execute", "executemany"}.intersection(methods("application"))
            and "admin_qqid_batches" in sources["batch"]
            and "admin_qqid_batch_entries" in sources["batch"]
            and "admin_qqid_batch_requests" in sources["batch"]
        ),
        "candidate_scan_is_readonly_and_request_paths_do_not_create_schema": bool(
            candidate_uows and all(any(item.arg == "read_only" and isinstance(item.value, ast.Constant)
                                      and item.value.value is True for item in node.keywords)
                                   for node in candidate_uows)
            and not any(token in sources[name].upper() for name in ("candidate", "batch", "application")
                        for token in ("CREATE TABLE", "ALTER TABLE"))
        ),
        "batch_schema_is_registered_at_startup": (
            '("legacy.admin.008", apply_admin_qqid_batch)' in sources["registry"]
            and "def apply_admin_qqid_batch(" in sources["migrations"]
            and all(table in sources["migrations"] for table in ("admin_qqid_batches", "admin_qqid_batch_entries"))
        ),
        "status": "qqid_batch_plan_is_durable_and_reuses_recoverable_single_id_writer",
    }


def _avatar_identity_priority(source: str) -> bool:
    avatar = source.find("_player_avatar().get_active_user_id(original_user_id)")
    impersonation = source.find("get_impersonating_target(original_user_id)")
    return 0 <= avatar < impersonation


def _admin_runtime_owner_status(sources: dict[str, str]) -> dict[str, object]:
    """Check the three admin runtime commands without loading production state."""
    trees = {name: _parse_source(source) for name, source in sources.items()}
    functions = {
        name: {node.name: node for node in _walk(tree)
               if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
        for name, tree in trees.items()
    }

    @lru_cache(maxsize=None)
    def nodes(source, function=None):
        node = trees[source] if function is None else functions[source].get(function)
        return list(_walk(node)) if node is not None else []

    def calls(source, function, target):
        return [node for node in nodes(source, function)
                if isinstance(node, ast.Call) and ast.unparse(node.func) == target]

    def expression(source, function, expected):
        target = _parse_source(expected).body[0]
        if isinstance(target, ast.Expr):
            target = target.value
        expected_tree = ast.dump(target)
        return any(type(node) is type(target) and ast.dump(node) == expected_tree
                   for node in nodes(source, function))

    def assignments(source, function, target):
        return [node for node in nodes(source, function) if isinstance(node, ast.Assign)
                and any(ast.unparse(item) == target for item in node.targets)]

    def registered(matcher, command, handler):
        bindings = assignments("handlers", None, matcher)
        return bool(
            len(bindings) == 1 and isinstance(bindings[0].value, ast.Call)
            and ast.unparse(bindings[0].value.func) == "on_command"
            and bindings[0].value.args and isinstance(bindings[0].value.args[0], ast.Constant)
            and bindings[0].value.args[0].value == command
            and any(item.arg == "permission" and ast.unparse(item.value) == "SUPERUSER"
                    for item in bindings[0].value.keywords)
            and calls("handlers", handler, f"{matcher}.handle")
        )

    catalog_writes = assignments("catalog_repository", "_load", "self._state")
    catalog_builds = calls("catalog_repository", "_load", "self._normalize_item")
    current_reads = calls("rift", "create_rift", "rift_application.current_world")
    projections = calls("rift", "create_rift", "_sync_world_projection")
    stale_guards = [node for node in nodes("rift", "create_rift") if isinstance(node, ast.If)
                    and ast.unparse(node.test) ==
                    "current_state is None or current_state['generation_id'] != result.state.generation_id"]
    report = {
        "three_superuser_handlers_reach_feature_owners": (
            all(registered(*entry) for entry in (
                ("create_new_rift", "生成秘境", "create_new_rift_"),
                ("items_refresh", "重载items", "items_refresh_"),
                ("impersonate_user_command", "用户伪装", "impersonate_user_command_"),
            ))
            and expression("handlers", "create_new_rift_", "await create_rift(bot, event)")
            and expression("handlers", "items_refresh_", "await asyncio.to_thread(items.refresh)")
            and all(calls("handlers", "impersonate_user_command_", f"admin_impersonation_application.{method}")
                    for method in ("get_target", "set_target", "cancel", "resolve_target"))
            and calls("rift", "create_rift", "rift_application.generate")
            and expression("rift_application", "generate", "repository = RiftGenerationSqlRepository(self.database)")
            and calls("rift_application", "generate", "repository.generate")
            and calls("catalog_facade", "refresh", "AdminItemCatalogApplication(self.repository).reload")
            and expression("catalog_application", "reload", "return self.repository.reload()")
            and expression("impersonation_application", "set_target", "return self.repository.set(admin_id, target_id)")
        ),
        "item_catalog_strict_reload_publishes_once_after_build_without_clearing": (
            expression("catalog_facade", None, "ITEMS_CACHE = _ITEM_CATALOG_REPOSITORY.items")
            and expression("catalog_facade", "__init__", "self.repository = _ITEM_CATALOG_REPOSITORY")
            and expression("catalog_facade", None, "_ITEM_CATALOG_REPOSITORY = get_item_catalog_repository(READPATH)")
            and calls("catalog_repository", "get_item_catalog_repository", "AdminItemCatalogRepository")
            and expression("catalog_repository", "reload", "return self._load(strict=True)")
            and expression("catalog_repository", "ensure_loaded", "return self._load(strict=False)")
            and len(catalog_writes) == 1 and bool(catalog_builds)
            and ast.unparse(catalog_writes[0]) == "self._state = (items, sources)"
            and catalog_writes[0].lineno > max(node.lineno for node in catalog_builds)
            and any(isinstance(node, ast.With) and any(ast.unparse(item.context_expr) == "self.lock" for item in node.items)
                    and catalog_writes[0] in _walk(node) for node in nodes("catalog_repository", "_load"))
            and all(any(isinstance(node, ast.If) and ast.unparse(node.test) == "strict"
                        and any(isinstance(child, ast.Raise) for child in node.body)
                        for node in _walk(handler))
                    for handler in nodes("catalog_repository", "_load") if isinstance(handler, ast.ExceptHandler))
            and not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "clear"
                        for source, function in (("catalog_facade", "refresh"), ("catalog_facade", "_load_items"),
                                                 ("catalog_repository", "reload"), ("catalog_repository", "_load"))
                        for node in nodes(source, function))
        ),
        "impersonation_uses_real_identity_and_one_atomic_shared_mapping": (
            expression("handlers", None, "admin_impersonation_application = AdminImpersonationApplication()")
            and expression("handlers", "impersonate_user_command_", "admin_user_id = str(event.get_user_id())")
            and expression("utils", None, "impersonation_application = AdminImpersonationApplication()")
            and len(assignments("utils", None, "_impersonating_users")) == 1
            and expression("utils", None, "_impersonating_users = impersonation_application.mapping")
            and expression("utils", "get_impersonating_target", "return impersonation_application.get_target(str(user_id))")
            and expression("impersonation_application", "__init__", "repository if repository is not None else default_impersonation_repository")
            and expression("impersonation_application", "mapping", "return self.repository")
            and expression("impersonation_repository", None, "default_impersonation_repository = AdminImpersonationRepository()")
            and all(any(isinstance(node, ast.With) and any(ast.unparse(item.context_expr) == "self._lock" for item in node.items)
                        for node in nodes("impersonation_repository", method)) for method in ("get", "set", "cancel"))
            and all(calls("utils", function, "get_impersonating_target") for function in
                    ("check_user", "check_user_type", "check_user_md_type", "handle_pic_msg_send",
                     "log_message", "get_logs", "get_statistics_data", "update_statistics_value"))
            and not any(isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name)
                        and node.value.id == "_impersonating_users" for source in ("handlers", "utils")
                        for node in nodes(source))
        ),
        "manual_rift_success_projects_current_world_not_historical_receipt": (
            len(current_reads) == len(projections) == len(stale_guards) == 1
            and current_reads[0].lineno < stale_guards[0].lineno < projections[0].lineno
            and any(isinstance(node, ast.Return) for node in stale_guards[0].body)
            and ast.unparse(projections[0]) == "_sync_world_projection(SimpleNamespace(**current_state), save_legacy=False)"
            and expression("rift_application", "current_world", "return RiftGenerationSqlRepository(self.database).get_current(rift_key)")
            and calls("rift", "create_rift", "old_rift_info.save_rift")
        ),
        "status": "admin_runtime_commands_use_feature_owners_with_atomic_catalog_and_current_world_projection",
    }
    return {key: value if key == "status" else bool(value) for key, value in report.items()}


def _arena_owner_status(sources: dict[str, str]) -> dict[str, bool]:
    """Bind the frozen arena commands to their actual state and receipt owners."""
    trees = {name: _parse_source(source) for name, source in sources.items()}
    functions = {
        name: {node.name: node for node in _walk(tree)
               if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
        for name, tree in trees.items()
    }

    @lru_cache(maxsize=None)
    def nodes(source, function=None):
        node = trees[source] if function is None else functions[source].get(function)
        return _walk(node) if node is not None else ()

    def calls(source, function, target):
        return [node for node in nodes(source, function)
                if isinstance(node, ast.Call) and ast.unparse(node.func) == target]

    def expression(source, function, expected):
        target = _parse_source(expected).body[0]
        if isinstance(target, ast.Expr):
            target = target.value
        expected_tree = ast.dump(target)
        return any(type(node) is type(target) and ast.dump(node) == expected_tree
                   for node in nodes(source, function))

    def literals(source, function=None):
        return " ".join(node.value for node in nodes(source, function)
                        if isinstance(node, ast.Constant) and isinstance(node.value, str))

    def before(source, function, first, second):
        left, right = calls(source, function, first), calls(source, function, second)
        return bool(left and right and left[0].lineno < right[0].lineno)

    opponent_connection_keywords = [
        {keyword.arg: ast.unparse(keyword.value) for keyword in node.keywords if keyword.arg}
        for node in calls("opponent_repository", "_connection", "DatabaseUnitOfWork")
    ]
    opponent_attach_keywords = [
        {keyword.arg: ast.unparse(keyword.value) for keyword in node.keywords if keyword.arg}
        for node in calls("opponent_repository", "_connection", "uow.attach_database")
    ]
    purchase_calls = calls("handlers", "arena_buy_", "arena_application.purchase")
    purchase_keywords = {item.arg: ast.unparse(item.value) for item in purchase_calls[0].keywords} if purchase_calls else {}
    handler_nodes = {name: nodes("handlers", name) for name in ("arena_buy_", "arena_challenge_", "arena_view_")}
    guarded_returns = all(any(
        isinstance(node, ast.ExceptHandler) and node.type is not None and ast.unparse(node.type) == error
        and any(isinstance(child, ast.Return) for child in node.body)
        for node in handler_nodes[handler]
    ) for handler in ("arena_buy_", "arena_challenge_") for error in ("ConflictError", "Exception"))
    cache_methods = ("set_cache", "get_cache", "clear_cache")
    repository_sql = literals("repository", "_record_purchase")
    report = {
        "purchase_and_challenge_handlers_reach_feature_sql_owners": (
            calls("handlers", "arena_buy_", "arena_buy.handle")
            and calls("handlers", "arena_challenge_", "arena_challenge.handle")
            and all(calls("handlers", function, target) for function, target in (
                ("arena_buy_", "arena_application.purchase_result"),
                ("arena_buy_", "arena_application.purchase"),
                ("arena_challenge_", "arena_application.settlement_result"),
                ("arena_challenge_", "arena_application.settle"),
                ("arena_challenge_", "arena_profile_application.get_user_profile"),
            ))
            and calls("handlers", None, "ArenaChallengePurchaseSqlRepository")
            and calls("handlers", None, "PlayerProfileApplication")
            and all(calls("application", method, f"self._repository().{method}")
                    for method in ("purchase", "purchase_result", "settle", "settlement_result"))
            and all(not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                            and node.func.id in {"_sql_message", "_player_data_manager"} for node in tree)
                    for tree in handler_nodes.values())
        ),
        "purchase_replays_before_live_catalog_and_preserves_raw_quantity": (
            all(before("handlers", "arena_buy_", "arena_application.purchase_result", target)
                for target in ("items.get_data_by_item_id", "arena_opponent_application.state", "check_rank_requirement"))
            and calls("handlers", "arena_buy_", "re.fullmatch")
            and expression("handlers", "arena_buy_", "len(msg_text) <= 64")
            and expression("handlers", "arena_buy_", "quantity <= 0 or int(shop_id) <= 0")
            and purchase_keywords.get("quantity") == "quantity"
            and purchase_keywords.get("clamp_quantity") == "True"
            and expression("repository", "purchase_result", "(str(payload[0]), int(payload[1]), int(payload[4])) != (str(user_id), int(item_id), int(quantity))")
            and expression("repository", "purchase", "quantity = min(quantity, max(0, weekly_limit - purchased))")
            and expression("repository", "purchase", "payload_values = [user_id, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, max_goods_num, int(bind_flag)]")
            and calls("repository", "purchase", "self._purchase_replay")
        ),
        "opponent_queries_and_all_cache_consumers_share_one_bounded_owner": (
            len(calls("handlers", None, "ArenaOpponentRepository")) == 1
            and len(calls("handlers", None, "ArenaOpponentApplication")) == 1
            and calls("handlers", "find_arena_opponent", "arena_opponent_application.find")
            and calls("handlers", "arena_view_", "arena_opponent_application.view")
            and all(calls("handlers", wrapper, f"arena_opponent_application.{method}") for wrapper, method in (
                ("set_arena_opponent_cache", "set_cache"), ("get_arena_opponent_cache", "get_cache"),
                ("clear_arena_opponent_cache", "clear_cache"),
            ))
            and not any(isinstance(node, ast.Name) and node.id == "arena_opponent_cache" for node in nodes("handlers"))
            and all(calls("opponent_application", method, f"self.repository.{method}") for method in cache_methods)
            and all(any(isinstance(node, ast.With) and any(ast.unparse(item.context_expr) == "self._lock" for item in node.items)
                        for node in nodes("opponent_repository", method)) for method in cache_methods)
            and expression("opponent_repository", "set_cache", "targets[:3]")
            and expression("opponent_repository", "set_cache", "len(self._cache) > self.capacity")
            and expression("opponent_repository", "get_cache", "cached[0] <= self.clock.now().timestamp()")
            and len(opponent_connection_keywords) == 1
            and opponent_connection_keywords[0].get("read_only") == "True"
            and opponent_connection_keywords[0].get("query_only") == "True"
            and len(opponent_attach_keywords) == 1
            and opponent_attach_keywords[0].get("read_only") == "True"
            and "profiles" in literals("opponent_repository", "_connection")
            and "JOIN profiles.user_xiuxian" in literals("opponent_repository", "candidates")
            and expression("opponent_application", "find", "abs(row['score'] - score) <= 200")
            and calls("opponent_application", "find", "random.Random(operation_id).choice")
            and calls("opponent_application", "find", "min")
        ),
        "challenge_replay_and_failures_cannot_fall_through_to_nomatch_rewards": (
            guarded_returns
            and before("handlers", "arena_challenge_", "arena_application.settlement_result", "_arena_fight")
            and before("handlers", "arena_challenge_", "arena_application.settlement_result", "arena_application.settle")
            and any(isinstance(node, ast.If) and ast.unparse(node.test) == "opponent_player is None"
                    and any(isinstance(child, ast.Return) for child in node.body) for node in handler_nodes["arena_challenge_"])
            and calls("opponent_application", "find", "self.repository.candidates")
            and not any(isinstance(node, ast.ExceptHandler) for source, function in (
                ("opponent_application", "find"), ("opponent_repository", "_connection"),
            ) for node in nodes(source, function))
            and calls("opponent_repository", "_connection", "self.require_available")
        ),
        "atomic_receipts_iso_week_and_scoped_started_recovery_are_feature_owned": (
            expression("repository", "_replay_result", "result['status'] in {'applied', 'duplicate'}")
            and expression("repository", "_purchase_replay", "row['status'] == 'needs_reconcile' or row['result_json'] is None")
            and "INSERT INTO arena_purchase_operations" in repository_sql and "status,result_json" in repository_sql
            and calls("repository", "purchase", "ArenaStateRepository._weekly")
            and expression("state_repository", "_weekly", "reset.isocalendar()[:2] != today.isocalendar()[:2]")
            and expression("application", "_execute", "action in {'arena.purchase', 'arena.settle'}")
            and expression("application", "_execute", "type(self.repository) is ArenaChallengePurchaseSqlRepository")
            and expression("application", "_execute", "if not recoverable:\n    raise ConflictError('操作正在处理中')")
            and "arena.010" in literals("plugin")
            and "apply_arena_purchase_receipt" in functions["migrations"]
            and not any(token in literals("repository") for token in ("CREATE TABLE", "ALTER TABLE"))
        ),
    }
    return {key: bool(value) for key, value in report.items()}


def _bank_command_owner_status(sources: dict[str, str]) -> dict[str, bool]:
    """Follow the real regex handler through the command owner to existing writers."""
    trees = {name: _parse_source(source) for name, source in sources.items()}
    functions = {
        name: {node.name: node for node in _walk(tree)
               if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
        for name, tree in trees.items()
    }

    def nodes(source, function=None):
        node = trees[source] if function is None else functions[source].get(function)
        return _walk(node) if node is not None else ()

    def calls(source, function, target):
        return [node for node in nodes(source, function)
                if isinstance(node, ast.Call) and ast.unparse(node.func) == target]

    def literals(source):
        return " ".join(node.value for node in nodes(source)
                        if isinstance(node, ast.Constant) and isinstance(node.value, str))

    bindings = {
        ast.unparse(node.value.func): ast.unparse(node.targets[0])
        for node in nodes("command", "__init__")
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
    }
    reachable = {"execute"}
    pending = ["execute"]
    while pending:
        for node in nodes("command", pending.pop()):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and ast.unparse(node.func.value) == "self"
                    and node.func.attr in functions["command"] and node.func.attr not in reachable):
                reachable.add(node.func.attr)
                pending.append(node.func.attr)

    def owned_calls(class_name, method):
        binding = bindings.get(class_name)
        return [] if binding is None else [
            call for function in reachable for call in calls("command", function, f"{binding}.{method}")
        ]

    dispatch = calls("handlers", "bank_", "bank_command_application.execute")
    keywords = {item.arg: ast.unparse(item.value) for item in dispatch[0].keywords} if dispatch else {}
    handler_calls = [node for node in nodes("handlers", "bank_") if isinstance(node, ast.Call)]
    facade_owned = bool(
        any(isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
            and ast.unparse(node.value.func) == "BankCommandApplication"
            and any(isinstance(target, ast.Name) and target.id == "bank_command_application"
                    for target in node.targets) for node in trees["handlers"].body)
        and calls("handlers", "bank_", "bank.handle") and dispatch
        and keywords == {name: name for name in ("operation_id", "user_id", "mode", "argument")}
        and calls("handlers", "bank_", "render_bank_reply")
        and not any(ast.unparse(node.func).startswith(("bank_application.", "bank_account_", "bank_deposit_",
                                                       "bank_withdrawal_", "bank_upgrade_", "bank_interest_"))
                    for node in handler_calls)
    )
    app_methods = {
        "deposit": ("BankDepositApplication", "deposit", "save_deposit"),
        "withdrawal": ("BankWithdrawalApplication", "withdraw", "save_withdrawal"),
        "upgrade": ("BankUpgradeApplication", "upgrade", "save_upgrade"),
        "interest": ("BankInterestApplication", "settle_interest", "save_interest"),
    }
    report = {
        f"{action}_application_owned": bool(
            facade_owned and owned_calls(class_name, method)
            and calls(action, method, f"self.repository.{writer}")
        )
        for action, (class_name, method, writer) in app_methods.items()
    }
    receipt_calls = owned_calls("BankCommandReceiptRepository", "find")
    info_calls = owned_calls("BankAccountInfoApplication", "get_info")
    level_reads = [node for node in nodes("command", "execute") if isinstance(node, ast.Subscript)
                   and ast.unparse(node.value) == "self.bank_levels"]
    receipt_first = bool(
        receipt_calls and info_calls and level_reads
        and receipt_calls[0].lineno < min(node.lineno for node in [*info_calls, *level_reads])
        and any(isinstance(node, ast.Assign) and node.value is receipt_calls[0]
                and any(isinstance(target, ast.Name) and target.id == "previous" for target in node.targets)
                for node in nodes("command", "execute"))
        and any(isinstance(node, ast.If) and ast.unparse(node.test) == "previous is not None"
                and any(isinstance(child, ast.Return) and isinstance(child.value, ast.Name)
                        and child.value.id == "previous"
                        for child in node.body)
                and node.lineno < info_calls[0].lineno for node in nodes("command", "execute"))
    )
    reader_uows = calls("receipts", None, "DatabaseUnitOfWork")
    reader_read_only = bool(reader_uows and all(
        any(item.arg == "read_only" and isinstance(item.value, ast.Constant) and item.value.value is True
            for item in call.keywords) for call in reader_uows
    ) and not any(token in literals("receipts") for token in ("CREATE TABLE", "ALTER TABLE", "INSERT INTO", "UPDATE ")))
    def resolved_keywords(call):
        keywords = {item.arg: item.value for item in call.keywords if item.arg is not None}
        for keyword in call.keywords:
            if keyword.arg is not None or not isinstance(keyword.value, ast.Name):
                continue
            for method in reachable:
                function = functions["command"].get(method)
                if function is None or not function.lineno <= call.lineno <= function.end_lineno:
                    continue
                assignments = [node for node in nodes("command", method)
                               if isinstance(node, ast.Assign) and node.lineno < call.lineno
                               and any(isinstance(target, ast.Name) and target.id == keyword.value.id
                                       for target in node.targets)]
                latest = max(assignments, key=lambda node: node.lineno, default=None)
                if latest is not None and isinstance(latest.value, ast.Dict):
                    keywords.update({key.value: value for key, value in zip(latest.value.keys, latest.value.values)
                                     if isinstance(key, ast.Constant) and isinstance(key.value, str)})
        return keywords

    snapshot_calls = [owned_calls(*app_methods[action][:2]) for action in ("deposit", "withdrawal", "interest")]
    snapshot_forwarded = all(group and all(
        {name: ast.unparse(value) for name, value in resolved_keywords(call).items()}.items()
        >= {"expected_saved_stone": "saved", "expected_saved_at": "saved_at", "bank_level": "level"}.items()
        for call in group
    ) for group in snapshot_calls)
    reply_nodes = nodes("replies", "render_bank_reply")
    failure_guards = [node for node in reply_nodes if isinstance(node, ast.If)
                      and ast.unparse(node.test) == "status not in {'applied', 'duplicate'}"
                      and any(isinstance(child, ast.Return) for child in node.body)]
    success_indexes = [node for node in reply_nodes if isinstance(node, ast.Subscript)
                       and ast.unparse(node.value) == "result"]
    report.update({
        "command_facade_orchestration_feature_owned": facade_owned and all(report.values()),
        "command_receipts_precede_live_account_reads": receipt_first and reader_read_only,
        "command_receipts_validate_original_identity": bool(
            calls("receipts", "find", "self._unified") and calls("receipts", "find", "self._legacy")
            and any(isinstance(node, ast.Compare)
                    and ast.unparse(node) == "(previous_user, previous_action, previous_amount) != (user_id, action, amount)"
                    for node in nodes("receipts", "find"))
            and all(table in literals("receipts") for table in (
                "bank_account_operations", "bank_deposit_operations", "bank_withdrawal_operations",
                "bank_upgrade_operations", "bank_interest_operations",
            ))
        ),
        "command_snapshot_cas_forwarded_to_existing_writers": snapshot_forwarded and all(
            calls(action, app_methods[action][1], "self.repository.assert_schema_ready")
            and "expected_saved_stone" in sources[action] and "expected_saved_at" in sources[action]
            for action in ("deposit", "withdrawal", "interest")
        ),
        "command_automatic_interest_and_account_info_feature_owned": bool(
            info_calls and any(calls("command", method, "calculate_interest") for method in reachable)
            and "account_missing" in literals("command") and "info" in literals("command")
            and any(isinstance(node, ast.If) and ast.unparse(node.test) == "action == 'info'"
                    and any(isinstance(child, ast.Return) for child in node.body)
                    for node in nodes("command", "execute"))
        ),
        "command_reply_rejections_precede_success_fields": bool(
            failure_guards and success_indexes
            and max(node.end_lineno for node in failure_guards) < min(node.lineno for node in success_indexes)
        ),
        "legacy_operation_receipts_read_only": bool(receipt_calls and reader_read_only),
    })
    return report


def _beg_command_owner_status(sources: dict[str, str]) -> dict[str, bool]:
    """Check the three frozen entrypoints without reclassifying the daily reset."""
    trees = {name: _parse_source(source) for name, source in sources.items()}
    functions = {name: {node.name: node for node in _walk(tree)
                       if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
                 for name, tree in trees.items()}

    def calls(source, function, target):
        root = trees[source] if function is None else functions[source][function]
        return [node for node in _walk(root)
                if isinstance(node, ast.Call) and ast.unparse(node.func) == target]

    def code(source, function=None):
        return ast.unparse(trees[source] if function is None else functions[source][function])

    entries = (("beg_stone_", "beg_stone", "daily_settle"),
               ("novice_", "novice", "novice_claim"), ("beg_help_", "beg_help", "help"))
    registrations = {target.id: ast.unparse(node.value) for node in trees["facade"].body if isinstance(node, ast.Assign)
                     for target in node.targets if isinstance(target, ast.Name)}
    constructors = calls("facade", None, "BegCommandApplication")
    lazy_providers = len(constructors) == 1 and len(constructors[0].args) == 4 and (
        ast.unparse(constructors[0].args[1]) == "XiuConfig"
        and ast.unparse(constructors[0].args[2]) == "jsondata.level_data"
        and isinstance(constructors[0].args[3], ast.Lambda)
    )
    execute = functions["command"]["execute"]
    help_branches = [node for node in execute.body if isinstance(node, ast.If)
                     and ast.unparse(node.test) == "action == 'help'"]
    mutation = ast.Module(body=[node for node in execute.body if node not in help_branches], type_ignores=[])
    positions = {ast.unparse(node.func): node.lineno for node in _walk(mutation) if isinstance(node, ast.Call)}
    ordered = ("self.repository.receipt", "self.repository.profile", "self.clock.now",
               "self.activity.update_last_check_info_time", "self.config_provider", "self.application.execute")
    receipt_return = any(isinstance(node, ast.If) and ast.unparse(node.test) == "previous is not None"
                         and len(node.body) == 1 and ast.unparse(node.body[0]) == "return previous"
                         for node in _walk(execute))
    atomic = [node for node in _walk(functions["application"]["execute"]) if isinstance(node, ast.With)
              and any(ast.unparse(item.context_expr) == "DatabaseUnitOfWork(self.database, immediate=True)" for item in node.items)]
    reply = functions["replies"]["render_beg_reply"]
    guards = [node for node in _walk(reply) if isinstance(node, ast.If)
              and ast.unparse(node.test) == "status not in {'applied', 'duplicate'}"
              and any(isinstance(child, ast.Return) for child in node.body)]
    assets = [node for node in _walk(reply) if isinstance(node, ast.Subscript)
              and ast.unparse(node.value) == "result" and isinstance(node.slice, ast.Constant)
              and node.slice.value in {"stone", "stone_reward"}]
    return {
        "three_command_handlers_reach_one_feature_owner": bool(
            lazy_providers and "beg_command_application = BegCommandApplication(" in code("facade")
            and all(registrations.get(matcher, "").startswith(f"on_command({name!r},")
                    for matcher, name in (("beg_stone", "仙途奇缘"), ("novice", "新手礼包"), ("beg_help", "仙途奇缘帮助")))
            and all(calls("facade", handler, f"{matcher}.handle")
                    and calls("facade", handler, "render_beg_reply")
                    and any(any(keyword.arg == "action" and isinstance(keyword.value, ast.Constant)
                                and keyword.value.value == action for keyword in call.keywords)
                            for call in calls("facade", handler, "beg_command_application.execute"))
                    for handler, matcher, action in entries)
            and all(token not in code("facade") for token in ("XiuxianDateManage", "_sql_message", "update_last_check_info_time"))
        ),
        "command_receipts_precede_live_inputs_and_activity": bool(
            receipt_return and all(name in positions for name in ordered)
            and all(positions[first] < positions[second] for first, second in zip(ordered, ordered[1:]))
            and all(name in positions and positions["self.repository.receipt"] < positions[name]
                    for name in ("self.levels_provider", "self.rng.randint", "self._gift"))
            and "repository or BegCommandRepository(database)" in code("command", "__init__")
            and "activity or PlayerActivityApplication(database, clock=self.clock)" in code("command", "__init__")
        ),
        "command_reads_validate_receipts_without_schema_writes": bool(
            calls("reads", "receipt", "self._ledger_result") and calls("reads", "profile", "self._read")
            and "DatabaseUnitOfWork(self.database, read_only=True)" in code("reads", "_read")
            and "request_hash({'user_id': user_id})" in code("reads", "_ledger_result")
            and "payload != [user_id]" in code("reads", "receipt")
            and all(token not in code("reads") for token in ("CREATE TABLE", "ALTER TABLE"))
        ),
        "command_reuses_existing_atomic_claim_writers": bool(
            calls("command", "execute", "self.application.execute")
            and "application or BegApplication(database)" in code("command", "__init__")
            and any(all(target in {ast.unparse(child.func) for child in _walk(node) if isinstance(child, ast.Call)}
                        for target in ("self.ledger.begin", "self.repository.settle_daily", "self.repository.claim_novice", "self.ledger.finish"))
                    for node in atomic)
        ),
        "dynamic_help_and_rejected_replies_do_not_need_claim_effects": bool(
            len(help_branches) == 1
            and not any(isinstance(node, ast.Call) and ast.unparse(node.func).startswith(
                ("self.repository.", "self.application.", "self.activity.")) for node in _walk(help_branches[0]))
            and "self.config_provider()" in ast.unparse(help_branches[0])
            and all(f"result['{field}']" in code("replies", "render_beg_reply") for field in ("max_age_days", "max_level", "current_time"))
            and guards and assets and max(node.end_lineno for node in guards) < min(node.lineno for node in assets)
        ),
    }


@lru_cache(maxsize=1)
def _slice_status() -> dict[str, dict[str, object]]:
    @lru_cache(maxsize=None)
    def output_tree(source: str) -> ast.Module:
        return _parse_source(source)

    def command_permission(source: str, command: str) -> str | None:
        """Return a command registration's permission without matching handler text."""
        try:
            tree = output_tree(source)
        except SyntaxError:
            return None
        for node in _walk(tree):
            if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
                continue
            if not isinstance(node.value.func, ast.Name) or node.value.func.id != "on_command":
                continue
            if not node.value.args or not isinstance(node.value.args[0], ast.Constant):
                continue
            if node.value.args[0].value != command:
                continue
            for keyword in node.value.keywords:
                if keyword.arg == "permission":
                    return ast.unparse(keyword.value)
            return ""
        return None

    def output_function(source: str, name: str) -> str:
        return next((
            ast.unparse(node) for node in output_tree(source).body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name
        ), "")

    base = (PACKAGE / "xiuxian" / "xiuxian_base" / "__init__.py").read_text(encoding="utf-8")
    base_root_reroll_handler = base[base.index("@restart.handle"):base.index("@rank.handle")]
    interactive_facade = (PACKAGE / "xiuxian" / "xiuxian_Interactive" / "__init__.py").read_text(encoding="utf-8")
    interactive_application_source = (PACKAGE / "features" / "interactive" / "application.py").read_text(encoding="utf-8")
    interactive_repository_source = (PACKAGE / "features" / "interactive" / "repository.py").read_text(encoding="utf-8")
    interactive_migrations_source = (PACKAGE / "features" / "interactive" / "migrations.py").read_text(encoding="utf-8")
    interactive_manifest_source = (PACKAGE / "features" / "interactive" / "manifest.py").read_text(encoding="utf-8")
    interactive_application_tests = (PACKAGE / "features" / "interactive" / "tests" / "test_interactive_application.py").read_text(encoding="utf-8")
    interactive_plugin_source = (PACKAGE / "plugin.py").read_text(encoding="utf-8")
    interactive_command_adapter_source = (PACKAGE / "adapters" / "nonebot" / "commands.py").read_text(encoding="utf-8")
    interactive_tree = _parse_source(interactive_facade)
    interactive_functions = {
        node.name: node
        for node in interactive_tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    def interactive_call_names(handler_name: str) -> set[str]:
        handler = interactive_functions.get(handler_name)
        if handler is None:
            return set()
        names: set[str] = set()
        for statement in handler.body:
            for node in _walk(statement):
                if not isinstance(node, ast.Call):
                    continue
                if isinstance(node.func, ast.Name):
                    names.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    names.add(node.func.attr)
        return names

    def interactive_delegates_to(handler_name: str, action: str) -> bool:
        handler = interactive_functions.get(handler_name)
        if handler is None:
            return False
        return any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "_run_interactive_action"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and node.args[0].value == action
            for node in _walk(handler)
        )

    interactive_static_handlers = (
        "handle_interaction",
        "handle_what_to_eat",
        "handle_rest",
        "handle_hello",
        "handle_how_are_you",
        "handle_bye",
        "handle_encourage",
        "handle_cute",
        "handle_eat",
        "handle_love_sentence",
        "handle_weather",
        "handle_study",
        "handle_work",
        "handle_time",
        "handle_funny_story",
        "handle_joke",
        "handle_thanks",
    )
    interactive_effect_handlers = {
        "handle_good_morning": "greeting_claim",
        "handle_good_night": "greeting_claim",
        "handle_give_exp": "exp_settle",
        "handle_give_stone": "stone_settle",
    }
    interactive_help_source = ast.get_source_segment(
        interactive_facade, interactive_functions.get("handle_interaction")
    ) or ""
    legacy_handle = (PACKAGE / "xiuxian" / "xiuxian_utils" / "xiuxian2_handle.py").read_text(encoding="utf-8")
    wishing_stone_writer = legacy_handle.split("def convert_stone_to_wishing_stone", 1)[1].split(
        "def add_impart_exp_day", 1
    )[0]
    base_transaction = (PACKAGE / "xiuxian" / "xiuxian_base" / "transaction_service.py").read_text(encoding="utf-8")
    base_application_source = (PACKAGE / "features" / "base" / "application.py").read_text(encoding="utf-8")
    base_rename_repository = (PACKAGE / "features" / "base" / "rename_repository.py").read_text(encoding="utf-8")
    base_theft_repository = (PACKAGE / "features" / "base" / "theft_repository.py").read_text(encoding="utf-8")
    base_contest_repository = (PACKAGE / "features" / "base" / "contest_repository.py").read_text(encoding="utf-8")
    base_robbery_repository = (PACKAGE / "features" / "base" / "robbery_repository.py").read_text(encoding="utf-8")
    base_root_reroll_repository = (PACKAGE / "features" / "base" / "root_reroll_repository.py").read_text(encoding="utf-8")
    base_direct_breakthrough_repository = (PACKAGE / "features" / "base" / "breakthrough_repository.py").read_text(encoding="utf-8")
    base_direct_breakthrough_relations = (PACKAGE / "features" / "base" / "breakthrough_relations.py").read_text(encoding="utf-8")
    base_direct_breakthrough_effects = (PACKAGE / "compatibility" / "base_breakthrough_effects.py").read_text(encoding="utf-8")
    base_xiangyuan_repository = (PACKAGE / "features" / "base" / "xiangyuan_repository.py").read_text(encoding="utf-8")
    base_xiangyuan_application = (PACKAGE / "features" / "base" / "xiangyuan_application.py").read_text(encoding="utf-8")
    base_stamina_application = (PACKAGE / "features" / "base" / "stamina_application.py").read_text(encoding="utf-8")
    base_stamina_repository = (PACKAGE / "features" / "base" / "stamina_repository.py").read_text(encoding="utf-8")
    player_state_repository = (PACKAGE / "features" / "player_state" / "repository.py").read_text(encoding="utf-8")
    layout_source = (PACKAGE / "xiuxian" / "xiuxian_utils" / "lay_out.py").read_text(encoding="utf-8")
    breakthrough_facade = (PACKAGE / "xiuxian" / "xiuxian_base" / "breakthrough_tribulation.py").read_text(encoding="utf-8")
    xiangyuan_facade = (PACKAGE / "xiuxian" / "xiuxian_base" / "xiangyuan.py").read_text(encoding="utf-8")
    base_migrations = (PACKAGE / "features" / "base" / "migrations.py").read_text(encoding="utf-8")
    base_manifest = (PACKAGE / "features" / "base" / "manifest.py").read_text(encoding="utf-8")
    stone_contest_compatibility = (PACKAGE / "compatibility" / "legacy_base_stone_contest.py").read_text(encoding="utf-8")
    stone_robbery_compatibility = (PACKAGE / "compatibility" / "legacy_base_stone_robbery.py").read_text(encoding="utf-8")
    xiangyuan_compatibility = (PACKAGE / "compatibility" / "legacy_base_xiangyuan.py").read_text(encoding="utf-8")
    breakthrough_compatibility = (PACKAGE / "compatibility" / "legacy_base_breakthrough.py").read_text(encoding="utf-8")
    ordinary_tribulation_compatibility = (PACKAGE / "compatibility" / "legacy_base_ordinary_tribulation.py").read_text(encoding="utf-8")
    destiny_tribulation_compatibility = (PACKAGE / "compatibility" / "legacy_base_destiny_tribulation.py").read_text(encoding="utf-8")
    heart_devil_tribulation_compatibility = (PACKAGE / "compatibility" / "legacy_base_heart_devil_tribulation.py").read_text(encoding="utf-8")
    pill_fusion_compatibility = (PACKAGE / "compatibility" / "legacy_base_pill_fusion.py").read_text(encoding="utf-8")
    tribulation_state_migration_compatibility = (PACKAGE / "compatibility" / "legacy_base_tribulation_state_migration.py").read_text(encoding="utf-8")
    adapter = (PACKAGE / "adapters" / "nonebot" / "commands.py").read_text(encoding="utf-8")
    web = (PACKAGE / "adapters" / "web" / "api.py").read_text(encoding="utf-8")
    legacy_transaction = (PACKAGE / "xiuxian" / "xiuxian_base" / "transaction_service.py").read_text(encoding="utf-8")
    tianti_facade = (PACKAGE / "xiuxian" / "xiuxian_tianti" / "__init__.py").read_text(encoding="utf-8")
    tianti_data = (PACKAGE / "xiuxian" / "xiuxian_tianti" / "tianti_data.py").read_text(encoding="utf-8")
    tianti_presentation = (PACKAGE / "features" / "tianti_training" / "presentation.py").read_text(encoding="utf-8")
    tianti_training_repository = (PACKAGE / "features" / "tianti_training" / "repository.py").read_text(encoding="utf-8")
    tianti_training_application = (PACKAGE / "features" / "tianti_training" / "application.py").read_text(encoding="utf-8")
    tianti_my_profile_handler = tianti_facade[
        tianti_facade.index("@tianti_info.handle") : tianti_facade.index("@tianti_chongqiao.handle")
    ]
    tianti_qiaoxue_profile_handler = tianti_facade[
        tianti_facade.index("@tiqiao_info.handle") : tianti_facade.index("@tianti_level_help.handle")
    ]
    tianti_help_handler = tianti_facade[
        tianti_facade.index("@tianti_help.handle") : tianti_facade.index("@tianti_settle.handle")
    ]
    tianti_level_help_handler = tianti_facade[tianti_facade.index("@tianti_level_help.handle") :]
    tianti_static_handler_awaits = {
        "help": re.findall(r"\bawait\s+([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\s*\(", tianti_help_handler),
        "level_help": re.findall(r"\bawait\s+([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\s*\(", tianti_level_help_handler),
    }
    tianti_static_handler_calls = {
        "help": re.findall(r"\b([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\s*\(", tianti_help_handler),
        "level_help": re.findall(r"\b([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\s*\(", tianti_level_help_handler),
    }
    tianti_training_stone_repository = tianti_training_repository[
        tianti_training_repository.index("class StoneTrainingSqlRepository") : tianti_training_repository.index(
            "class TiantiMedicineBathSqlRepository"
        )
    ]
    tianti_training_writer = (PACKAGE / "features" / "tianti_training" / "profile_persistence.py").read_text(encoding="utf-8")
    tianti_settlement_repository = (PACKAGE / "features" / "tianti_settlement" / "repository.py").read_text(encoding="utf-8")
    tianti_settlement_application = (PACKAGE / "features" / "tianti_settlement" / "application.py").read_text(encoding="utf-8")
    sect_fairyland_claim_repository = (PACKAGE / "features" / "sect_fairyland" / "claim_repository.py").read_text(encoding="utf-8")
    sign_effects = (PACKAGE / "features" / "sign_in" / "application_effects.py").read_text(encoding="utf-8")
    sign_application = (PACKAGE / "features" / "sign_in" / "application.py").read_text(encoding="utf-8")
    sign_daily_reset_repository = (PACKAGE / "features" / "sign_in" / "daily_reset_repository.py").read_text(encoding="utf-8")
    sign_daily_reset_tests = (PACKAGE / "features" / "sign_in" / "tests" / "test_daily_reset_repository.py").read_text(encoding="utf-8")
    sign_in_docs = (ROOT / "docs" / "features" / "sign_in.md").read_text(encoding="utf-8")
    tasks_entry = (PACKAGE / "xiuxian" / "xiuxian_tasks" / "task_data.py").read_text(encoding="utf-8")
    tasks_transaction = (PACKAGE / "xiuxian" / "xiuxian_tasks" / "transaction_service.py").read_text(encoding="utf-8")
    tasks_legacy_transactions = (PACKAGE / "compatibility" / "legacy_task_transactions.py").read_text(encoding="utf-8")
    tasks_claim = tasks_legacy_transactions[
        tasks_legacy_transactions.index("class TaskRewardClaimService") : tasks_legacy_transactions.index(
            "class LegacyTaskProgressEventService"
        )
    ]
    tasks_claim_application = (PACKAGE / "features" / "tasks" / "application.py").read_text(encoding="utf-8")
    tasks_claim_repository = (PACKAGE / "features" / "tasks" / "claim_repository.py").read_text(encoding="utf-8")
    tasks_entry = (PACKAGE / "xiuxian" / "xiuxian_tasks" / "task_data.py").read_text(encoding="utf-8")
    tasks_command = (PACKAGE / "xiuxian" / "xiuxian_tasks" / "__init__.py").read_text(encoding="utf-8")
    tasks_progress = (PACKAGE / "features" / "tasks" / "progress.py").read_text(encoding="utf-8")
    tasks_read_states_start = tasks_progress.index("    def read_states(")
    tasks_read_states = tasks_progress[
        tasks_read_states_start : tasks_progress.index("    def record(", tasks_read_states_start)
    ]
    tasks_migrations = (PACKAGE / "features" / "tasks" / "migrations.py").read_text(encoding="utf-8")
    compensation_repository = (PACKAGE / "features" / "compensation" / "reward_claim_repository.py").read_text(encoding="utf-8")
    compensation_invitation_repository = (PACKAGE / "features" / "compensation" / "invitation_repository.py").read_text(encoding="utf-8")
    compensation_migrations = (PACKAGE / "features" / "compensation" / "migrations.py").read_text(encoding="utf-8")
    compensation_legacy_migrated = (PACKAGE / "features" / "_legacy_migrated.py").read_text(encoding="utf-8")
    compensation_reward_definition_repository = (PACKAGE / "features" / "compensation" / "reward_definition_repository.py").read_text(encoding="utf-8")
    compensation_definition_service = (
        (PACKAGE / "xiuxian" / "xiuxian_compensation" / "transaction_service.py")
        .read_text(encoding="utf-8")
        .split("class RewardClaimService", 1)[0]
    )
    compensation_redeem_code = (PACKAGE / "xiuxian" / "xiuxian_compensation" / "redeem_code.py").read_text(encoding="utf-8")
    compensation_reward_center = (PACKAGE / "xiuxian" / "xiuxian_web" / "reward_center.py").read_text(encoding="utf-8")
    compensation_invitation = (PACKAGE / "xiuxian" / "xiuxian_compensation" / "invitation.py").read_text(encoding="utf-8")
    compensation_common = (PACKAGE / "xiuxian" / "xiuxian_compensation" / "common.py").read_text(encoding="utf-8")
    compensation_reward_writer = compensation_common.split("def send_reward_to_user", 1)[1].split(
        "def format_reward_delivery", 1
    )[0]
    plugin = (PACKAGE / "plugin.py").read_text(encoding="utf-8")
    info_attribute_application = (PACKAGE / "features" / "info" / "attribute_application.py").read_text(encoding="utf-8")
    info_attribute_compatibility = (PACKAGE / "compatibility" / "legacy_player_attributes.py").read_text(encoding="utf-8")
    info_utils = (PACKAGE / "xiuxian" / "xiuxian_utils" / "utils.py").read_text(encoding="utf-8")
    info_user_handler = (PACKAGE / "xiuxian" / "xiuxian_info" / "user_info.py").read_text(encoding="utf-8")
    buff_handler = (PACKAGE / "xiuxian" / "xiuxian_buff" / "__init__.py").read_text(encoding="utf-8")
    player_fight = (PACKAGE / "xiuxian" / "xiuxian_utils" / "player_fight.py").read_text(encoding="utf-8")
    json_config = (PACKAGE / "xiuxian" / "xiuxian_utils" / "xiuxian_json_config.py").read_text(encoding="utf-8")
    title_data_source = (PACKAGE / "xiuxian" / "xiuxian_title" / "title_data.py").read_text(encoding="utf-8")
    title_application_source = (PACKAGE / "features" / "title" / "application.py").read_text(encoding="utf-8")
    title_repository_source = (PACKAGE / "features" / "title" / "repository.py").read_text(encoding="utf-8")
    title_eligibility_source = (PACKAGE / "features" / "title" / "eligibility.py").read_text(encoding="utf-8")
    title_handler_facade_source = (PACKAGE / "xiuxian" / "xiuxian_title" / "__init__.py").read_text(encoding="utf-8")
    title_condition_readers = title_data_source[
        title_data_source.index("def check_condition_for_user") : title_data_source.index(
            "def check_and_unlock_titles"
        )
    ]
    title_state_readers = title_data_source[
        title_data_source.index("def get_user_unlocked_titles") : title_data_source.index(
            "def grant_title_to_user"
        )
    ]
    title_repository_state_reader = title_repository_source[
        title_repository_source.index("    def get_state(") : title_repository_source.index(
            "    def ensure_schema("
        )
    ]
    arena = (PACKAGE / "xiuxian" / "xiuxian_arena" / "__init__.py").read_text(encoding="utf-8")
    arena_transaction_service = (PACKAGE / "xiuxian" / "xiuxian_arena" / "transaction_service.py").read_text(encoding="utf-8")
    arena_legacy_transaction_service = (PACKAGE / "compatibility" / "legacy_arena_transactions.py").read_text(encoding="utf-8")
    arena_repository = (PACKAGE / "features" / "arena" / "repository.py").read_text(encoding="utf-8")
    arena_limit = (PACKAGE / "xiuxian" / "xiuxian_arena" / "arena_limit.py").read_text(encoding="utf-8")
    arena_owner_sources = {
        "handlers": arena, "repository": arena_repository, "plugin": plugin,
        **{name: (PACKAGE / "features/arena" / filename).read_text(encoding="utf-8") for name, filename in {
            "application": "application.py", "opponent_application": "opponent_application.py",
            "opponent_repository": "opponent_repository.py", "state_repository": "state_repository.py",
            "migrations": "migrations.py",
        }.items()},
    }
    tower_facade = (PACKAGE / "xiuxian" / "xiuxian_tower" / "__init__.py").read_text(encoding="utf-8")
    tower_limit = (PACKAGE / "xiuxian" / "xiuxian_tower" / "tower_limit.py").read_text(encoding="utf-8")
    tower_state_application = (PACKAGE / "features" / "tower" / "state_application.py").read_text(encoding="utf-8")
    tower_state_repository = (PACKAGE / "features" / "tower" / "state_repository.py").read_text(encoding="utf-8")
    tower_migrations = (PACKAGE / "features" / "tower" / "migrations.py").read_text(encoding="utf-8")
    tower_scheduler = (PACKAGE / "xiuxian" / "xiuxian_scheduler" / "__init__.py").read_text(encoding="utf-8")
    tower_admin = (PACKAGE / "xiuxian" / "xiuxian_admin" / "__init__.py").read_text(encoding="utf-8")
    tower_reset_facade = tower_facade[
        tower_facade.index("async def reset_tower_floors(") : tower_facade.index("@tower_boss_info.handle")
    ]
    tower_reset_scheduler = tower_scheduler[
        tower_scheduler.index("async def weekly_reset_tower_floors(") : tower_scheduler.index("# =========================", tower_scheduler.index("async def weekly_reset_tower_floors("))
    ]
    tower_reset_admin = tower_admin[
        tower_admin.index("async def tower_reset_(") : tower_admin.index("@boss_reset.handle", tower_admin.index("async def tower_reset_("))
    ]
    training_limit = (PACKAGE / "xiuxian" / "xiuxian_training" / "training_limit.py").read_text(encoding="utf-8")
    training_facade = (PACKAGE / "xiuxian" / "xiuxian_training" / "__init__.py").read_text(encoding="utf-8")
    training_events = (PACKAGE / "xiuxian" / "xiuxian_training" / "training_events.py").read_text(encoding="utf-8")
    training_event_resolver = (PACKAGE / "features" / "training" / "event_resolver.py").read_text(encoding="utf-8")
    training_application = (PACKAGE / "features" / "training" / "application.py").read_text(encoding="utf-8")
    training_repository = (PACKAGE / "features" / "training" / "repository.py").read_text(encoding="utf-8")
    training_event_repository = (PACKAGE / "features" / "training" / "event_repository.py").read_text(encoding="utf-8")
    training_event_context_repository = (PACKAGE / "features" / "training" / "event_context_repository.py").read_text(encoding="utf-8")
    training_event_planner = (PACKAGE / "features" / "training" / "event_planner.py").read_text(encoding="utf-8")
    training_leaderboard_repository = (PACKAGE / "features" / "training" / "leaderboard_repository.py").read_text(encoding="utf-8")
    training_plugin = (PACKAGE / "plugin.py").read_text(encoding="utf-8")
    training_purchase_repository = (PACKAGE / "features" / "training" / "purchase_repository.py").read_text(encoding="utf-8")
    training_reset_repository = (PACKAGE / "features" / "training" / "reset_repository.py").read_text(encoding="utf-8")
    training_migrations = (PACKAGE / "features" / "training" / "migrations.py").read_text(encoding="utf-8")
    work_facade = (PACKAGE / "xiuxian" / "xiuxian_work" / "__init__.py").read_text(encoding="utf-8")
    work_handle_source = (PACKAGE / "xiuxian" / "xiuxian_work" / "work_handle.py").read_text(encoding="utf-8")
    workmake_source = (PACKAGE / "xiuxian" / "xiuxian_work" / "workmake.py").read_text(encoding="utf-8")
    work_reward_source = (PACKAGE / "xiuxian" / "xiuxian_work" / "reward_data_source.py").read_text(encoding="utf-8")
    work_claim_repository = (PACKAGE / "features" / "work" / "claim_repository.py").read_text(encoding="utf-8")
    work_claim_application_source = (PACKAGE / "features" / "work" / "application.py").read_text(encoding="utf-8")
    work_refresh_application_source = (PACKAGE / "features" / "work" / "refresh_application.py").read_text(encoding="utf-8")
    work_status_application_source = (PACKAGE / "features" / "work" / "status_application.py").read_text(encoding="utf-8")
    work_item_use_application_source = (PACKAGE / "features" / "work" / "work_item_use_application.py").read_text(encoding="utf-8")
    work_legacy_offer_adapter = (PACKAGE / "compatibility" / "legacy_work_offer_json.py").read_text(encoding="utf-8")
    work_settlement_repository = (PACKAGE / "features" / "work" / "settlement_repository.py").read_text(encoding="utf-8")
    work_admin_reset_repository = (PACKAGE / "features" / "work" / "admin_refresh_reset_repository.py").read_text(encoding="utf-8")
    work_admin_reset_tests = (PACKAGE / "features" / "work" / "tests" / "test_admin_refresh_reset_repository.py").read_text(encoding="utf-8")
    work_legacy_item_use = (PACKAGE / "compatibility" / "legacy_work_item_use.py").read_text(encoding="utf-8")
    work_legacy_daily_refresh = (PACKAGE / "compatibility" / "legacy_work_daily_refresh_reset.py").read_text(encoding="utf-8")
    work_legacy_settlement = (PACKAGE / "compatibility" / "legacy_work_settlement.py").read_text(encoding="utf-8")
    work_refresh_repository = (PACKAGE / "features" / "work" / "refresh_repository.py").read_text(encoding="utf-8")
    work_abort_repository = (PACKAGE / "features" / "work" / "abort_cleanup_repository.py").read_text(encoding="utf-8")
    work_migrations = (PACKAGE / "features" / "work" / "migrations.py").read_text(encoding="utf-8")
    work_abort_legacy = (PACKAGE / "compatibility" / "legacy_work_abort_cleanup.py").read_text(encoding="utf-8")
    work_legacy_refresh = (PACKAGE / "compatibility" / "legacy_work_refresh.py").read_text(encoding="utf-8")
    work_plugin_source = (PACKAGE / "plugin.py").read_text(encoding="utf-8")
    work_transaction_shim = (PACKAGE / "xiuxian" / "xiuxian_work" / "transaction_service.py").read_text(encoding="utf-8")
    work_accelerate_handler = work_facade[
        work_facade.index("async def use_work_order") : work_facade.index(
            "async def use_work_capture_order", work_facade.index("async def use_work_order")
        )
    ]
    work_capture_handler = work_facade[
        work_facade.index("async def use_work_capture_order") :
    ]
    work_claim_projection_wiring = work_facade[
        work_facade.index("work_claim_application = WorkClaimApplication(") : work_facade.index(
            "work_settlement_application = WorkSettlementApplication("
        )
    ]
    work_refresh_projection_wiring = work_facade[
        work_facade.index("work_refresh_application = WorkRefreshApplication(") : work_facade.index(
            "work_status_application = WorkStatusApplication("
        )
    ]
    work_status_projection_wiring = work_facade[
        work_facade.index("work_status_application = WorkStatusApplication(") : work_facade.index(
            "work_abort_cleanup_application = WorkAbortCleanupApplication("
        )
    ]
    work_item_use_projection_wiring = work_facade[
        work_facade.index("work_item_use_application = WorkItemUseApplication(") : work_facade.index(
            "work_daily_refresh_application = WorkDailyRefreshResetApplication("
        )
    ]
    activity_service = (PACKAGE / "xiuxian" / "xiuxian_activity" / "service.py").read_text(encoding="utf-8")
    activity_commands = (PACKAGE / "xiuxian" / "xiuxian_activity" / "__init__.py").read_text(encoding="utf-8")
    activity_application = (PACKAGE / "features" / "activity" / "application.py").read_text(encoding="utf-8")
    activity_read_model_application = (PACKAGE / "features" / "activity" / "read_model_application.py").read_text(encoding="utf-8")
    activity_read_model_repository = (PACKAGE / "features" / "activity" / "read_model_repository.py").read_text(encoding="utf-8")
    activity_read_model_tests = (ROOT / "tests" / "test_activity_read_model.py").read_text(encoding="utf-8")
    activity_admin_data_application = (PACKAGE / "features" / "activity" / "admin_data_application.py").read_text(encoding="utf-8")
    activity_admin_data_repository = (PACKAGE / "features" / "activity" / "admin_data_repository.py").read_text(encoding="utf-8")
    activity_admin_data_tests = (ROOT / "tests" / "test_activity_admin_data.py").read_text(encoding="utf-8")
    activity_config_application = (PACKAGE / "features" / "activity" / "config_application.py").read_text(encoding="utf-8")
    activity_config_repository = (PACKAGE / "features" / "activity" / "config_repository.py").read_text(encoding="utf-8")
    activity_config_event_service = (PACKAGE / "xiuxian" / "xiuxian_activity" / "config_event_service.py").read_text(encoding="utf-8")
    activity_web = (PACKAGE / "xiuxian" / "xiuxian_web" / "activity.py").read_text(encoding="utf-8")
    activity_config_tests = (ROOT / "tests" / "test_activity_config_application.py").read_text(encoding="utf-8")
    activity_config_event_tests = (ROOT / "tests" / "test_activity_config_event_service.py").read_text(encoding="utf-8")
    web_pages = (PACKAGE / "xiuxian" / "xiuxian_web" / "pages.py").read_text(encoding="utf-8")
    web_pages_access = (PACKAGE / "xiuxian" / "xiuxian_web" / "access.py").read_text(encoding="utf-8")
    web_pages_tests = (ROOT / "tests" / "test_web_auth.py").read_text(encoding="utf-8")
    updater_application = (PACKAGE / "features" / "updater" / "application.py").read_text(encoding="utf-8")
    updater_application_tests = (PACKAGE / "features" / "updater" / "tests" / "test_application.py").read_text(encoding="utf-8")
    updater_manager_tests = (PACKAGE / "features" / "updater" / "tests" / "test_manager_adapter.py").read_text(encoding="utf-8")
    updater_status = (PACKAGE / "xiuxian" / "xiuxian_status" / "__init__.py").read_text(encoding="utf-8")
    updater_web_core = (PACKAGE / "xiuxian" / "xiuxian_web" / "core.py").read_text(encoding="utf-8")
    updater_page_template = (PACKAGE / "xiuxian" / "xiuxian_web" / "templates" / "update.html").read_text(encoding="utf-8")
    updater_web_tests = (ROOT / "tests" / "test_updater_web.py").read_text(encoding="utf-8")
    plugin_backup_application = (PACKAGE / "features" / "plugin_backups" / "application.py").read_text(encoding="utf-8")
    plugin_backup_repository = (PACKAGE / "features" / "plugin_backups" / "repository.py").read_text(encoding="utf-8")
    plugin_backup_tests = (PACKAGE / "features" / "plugin_backups" / "tests" / "test_catalog.py").read_text(encoding="utf-8")
    plugin_backup_file_application = (PACKAGE / "features" / "plugin_backups" / "file_application.py").read_text(encoding="utf-8")
    plugin_backup_file_repository = (PACKAGE / "features" / "plugin_backups" / "file_repository.py").read_text(encoding="utf-8")
    plugin_backup_file_tests = (PACKAGE / "features" / "plugin_backups" / "tests" / "test_file_operations.py").read_text(encoding="utf-8")
    plugin_backup_restore_application = (PACKAGE / "features" / "plugin_backups" / "restore_application.py").read_text(encoding="utf-8")
    plugin_backup_restore_repository = (PACKAGE / "features" / "plugin_backups" / "restore_repository.py").read_text(encoding="utf-8")
    plugin_backup_restore_tests = (PACKAGE / "features" / "plugin_backups" / "tests" / "test_restore.py").read_text(encoding="utf-8")
    plugin_backup_cloud_application = (PACKAGE / "features" / "plugin_backups" / "cloud_application.py").read_text(encoding="utf-8")
    plugin_backup_cloud_repository = (PACKAGE / "features" / "plugin_backups" / "cloud_repository.py").read_text(encoding="utf-8")
    plugin_backup_cloud_tests = (PACKAGE / "features" / "plugin_backups" / "tests" / "test_cloud.py").read_text(encoding="utf-8")
    plugin_backup_creation_application = (PACKAGE / "features" / "plugin_backups" / "creation_application.py").read_text(encoding="utf-8")
    plugin_backup_creation_repository = (PACKAGE / "features" / "plugin_backups" / "creation_repository.py").read_text(encoding="utf-8")
    plugin_backup_creation_tests = (PACKAGE / "features" / "plugin_backups" / "tests" / "test_creation.py").read_text(encoding="utf-8")
    manual_backup_application = (PACKAGE / "features" / "manual_backups" / "application.py").read_text(encoding="utf-8")
    manual_backup_tests = (PACKAGE / "features" / "manual_backups" / "tests" / "test_application.py").read_text(encoding="utf-8")
    plugin_backup_manager = (PACKAGE / "xiuxian" / "xiuxian_utils" / "download_xiuxian_data.py").read_text(encoding="utf-8")
    backup_routes = (PACKAGE / "xiuxian" / "xiuxian_web" / "backups.py").read_text(encoding="utf-8")
    backup_page_template = (PACKAGE / "xiuxian" / "xiuxian_web" / "templates" / "backups.html").read_text(encoding="utf-8")
    database_backup_application = (PACKAGE / "features" / "database_backups" / "application.py").read_text(encoding="utf-8")
    database_backup_repository = (PACKAGE / "features" / "database_backups" / "repository.py").read_text(encoding="utf-8")
    database_backup_tests = "\n".join(
        (PACKAGE / "features" / "database_backups" / "tests" / test_name).read_text(encoding="utf-8")
        for test_name in ("test_application.py", "test_repository.py")
    )
    database_backup_adapter_tests = (PACKAGE / "features" / "updater" / "tests" / "test_manager_adapter.py").read_text(encoding="utf-8")
    config_backup_application = (PACKAGE / "features" / "config_backups" / "application.py").read_text(encoding="utf-8")
    config_backup_repository = (PACKAGE / "features" / "config_backups" / "repository.py").read_text(encoding="utf-8")
    plugin_backup_schemas = (PACKAGE / "features" / "plugin_backups" / "schemas.py").read_text(encoding="utf-8")
    database_backup_schemas = (PACKAGE / "features" / "database_backups" / "schemas.py").read_text(encoding="utf-8")
    config_backup_schemas = (PACKAGE / "features" / "config_backups" / "schemas.py").read_text(encoding="utf-8")
    config_backup_tests = "\n".join(
        (PACKAGE / "features" / "config_backups" / "tests" / test_name).read_text(encoding="utf-8")
        for test_name in ("test_application.py", "test_repository.py")
    )
    plugin_backup_web_tests = (ROOT / "tests" / "test_updater_web.py").read_text(encoding="utf-8")
    activity_help_handler = activity_commands[
        activity_commands.index("@activity_help_cmd.handle"):
        activity_commands.index("@activity_manage_cmd.handle")
    ]
    activity_manage_handler = activity_commands[
        activity_commands.index("@activity_manage_cmd.handle"):
        activity_commands.index("@activity_info_cmd.handle")
    ]
    activity_open_handler = activity_commands[
        activity_commands.index("@activity_open_cmd.handle"):
        activity_commands.index("@activity_close_cmd.handle")
    ]
    activity_close_handler = activity_commands[
        activity_commands.index("@activity_close_cmd.handle"):
        activity_commands.index("ACTIVITY_HELP =")
    ]
    activity_boss_settlement_repository = (PACKAGE / "features" / "activity" / "boss_settlement_repository.py").read_text(encoding="utf-8")
    activity_boss_feature_tests = "\n".join(
        (
            (ROOT / "tests" / "test_activity_read_model.py").read_text(encoding="utf-8"),
            (PACKAGE / "features" / "activity" / "tests" / "test_boss_settlement_repository.py").read_text(encoding="utf-8"),
            (PACKAGE / "features" / "activity" / "tests" / "test_activity_application.py").read_text(encoding="utf-8"),
        )
    )
    activity_sign_repository = (PACKAGE / "features" / "activity" / "sign_settlement_repository.py").read_text(encoding="utf-8")
    activity_collect_repository = (PACKAGE / "features" / "activity" / "collect_exchange_repository.py").read_text(encoding="utf-8")
    activity_collect_tests = (PACKAGE / "features" / "activity" / "tests" / "test_collect_exchange_repository.py").read_text(encoding="utf-8")
    activity_purchase_repository = (PACKAGE / "features" / "activity" / "point_shop_purchase_repository.py").read_text(encoding="utf-8")
    activity_purchase_tests = (PACKAGE / "features" / "activity" / "tests" / "test_point_shop_purchase_repository.py").read_text(encoding="utf-8")
    activity_state_migration_tests = (ROOT / "tests" / "test_activity_state_migration.py").read_text(encoding="utf-8")
    migrated_application = (PACKAGE / "features" / "_migrated_application.py").read_text(encoding="utf-8")
    activity_application_tests = (PACKAGE / "features" / "activity" / "tests" / "test_activity_application.py").read_text(encoding="utf-8")
    activity_sign_repository_tests = (PACKAGE / "features" / "activity" / "tests" / "test_sign_settlement_repository.py").read_text(encoding="utf-8")
    activity_cli = (PACKAGE / "cli.py").read_text(encoding="utf-8")
    activity_reward_application = (PACKAGE / "features" / "activity_reward" / "application.py").read_text(encoding="utf-8")
    activity_claim_repository = (PACKAGE / "features" / "activity_reward" / "claim_all_repository.py").read_text(encoding="utf-8")
    activity_task_claim_application = (PACKAGE / "features" / "activity_reward" / "task_claim_application.py").read_text(encoding="utf-8")
    activity_task_claim_repository = (PACKAGE / "features" / "activity_reward" / "task_claim_repository.py").read_text(encoding="utf-8")
    activity_pass_claim_application = (PACKAGE / "features" / "activity_reward" / "pass_claim_application.py").read_text(encoding="utf-8")
    activity_pass_claim_repository = (PACKAGE / "features" / "activity_reward" / "pass_claim_repository.py").read_text(encoding="utf-8")
    activity_boss_milestone_claim_application = (PACKAGE / "features" / "activity_reward" / "boss_milestone_claim_application.py").read_text(encoding="utf-8")
    activity_boss_milestone_claim_repository = (PACKAGE / "features" / "activity_reward" / "boss_milestone_claim_repository.py").read_text(encoding="utf-8")
    activity_boss_rank_claim_application = (PACKAGE / "features" / "activity_reward" / "boss_rank_claim_application.py").read_text(encoding="utf-8")
    activity_boss_rank_claim_repository = (PACKAGE / "features" / "activity_reward" / "boss_rank_claim_repository.py").read_text(encoding="utf-8")
    activity_reward_migrations = (PACKAGE / "features" / "activity_reward" / "migrations.py").read_text(encoding="utf-8")
    activity_state_migrations = (PACKAGE / "features" / "activity" / "migrations.py").read_text(encoding="utf-8")
    activity_storage = (PACKAGE / "xiuxian" / "xiuxian_activity" / "activity_storage.py").read_text(encoding="utf-8")
    activity_transaction_service = (PACKAGE / "xiuxian" / "xiuxian_activity" / "transaction_service.py").read_text(encoding="utf-8")
    activity_web_core = (PACKAGE / "xiuxian" / "xiuxian_web" / "core.py").read_text(encoding="utf-8")
    activity_web_database = (PACKAGE / "xiuxian" / "xiuxian_web" / "database.py").read_text(encoding="utf-8")
    activity_boss_transaction_service = (PACKAGE / "xiuxian" / "xiuxian_boss" / "transaction_service.py").read_text(encoding="utf-8")
    activity_boss_entry = (PACKAGE / "xiuxian" / "xiuxian_boss" / "__init__.py").read_text(encoding="utf-8")
    activity_claim_compatibility_source = (PACKAGE / "xiuxian" / "xiuxian_activity" / "transaction_service.py").read_text(encoding="utf-8")
    activity_task_claim_compatibility = activity_claim_compatibility_source[
        activity_claim_compatibility_source.index("class ActivityTaskClaimService:"):
        activity_claim_compatibility_source.index("class ActivityPassClaimService:")
    ]
    activity_pass_claim_compatibility = activity_claim_compatibility_source[
        activity_claim_compatibility_source.index("class ActivityPassClaimService:"):
        activity_claim_compatibility_source.index("class ActivityPointShopPurchaseResult:")
    ]
    boss_reward_claim_compatibility = activity_claim_compatibility_source[
        activity_claim_compatibility_source.index("class BossRewardClaimService:"):
        activity_claim_compatibility_source.index("class ActivityBossCoopSettlementService")
    ]
    boss_milestone_claim_compatibility = boss_reward_claim_compatibility[
        boss_reward_claim_compatibility.index("    def claim_milestones("):
        boss_reward_claim_compatibility.index("    def claim_rank(")
    ]
    boss_rank_claim_compatibility = boss_reward_claim_compatibility[
        boss_reward_claim_compatibility.index("    def claim_rank("):
    ]
    activity_boss_source = (PACKAGE / "xiuxian" / "xiuxian_activity" / "activity_boss.py").read_text(encoding="utf-8")
    activity_boss_claim_router = activity_boss_source[
        activity_boss_source.index("def claim_boss_rewards("):
    ]
    activity_boss_milestone_claim_entry = activity_boss_source[
        activity_boss_source.index("def claim_boss_milestone_reward("):
        activity_boss_source.index("def claim_boss_rank_reward(")
    ]
    activity_boss_rank_claim_entry = activity_boss_source[
        activity_boss_source.index("def claim_boss_rank_reward("):
        activity_boss_source.index("def claim_boss_rewards(")
    ]
    activity_claim_runners = (PACKAGE / "compatibility" / "legacy_activity_claim_steps.py").read_text(encoding="utf-8")
    activity_command_repository = (PACKAGE / "features" / "activity" / "repository.py").read_text(encoding="utf-8")
    dungeon_facade = (PACKAGE / "xiuxian" / "xiuxian_dungeon" / "__init__.py").read_text(encoding="utf-8")
    dungeon_team_manager = (PACKAGE / "xiuxian" / "xiuxian_dungeon" / "team_manager.py").read_text(encoding="utf-8")
    dungeon_invite_compatibility = (PACKAGE / "compatibility" / "legacy_dungeon_team_invite_mapping.py").read_text(encoding="utf-8")
    dungeon_transaction_shim = (PACKAGE / "xiuxian" / "xiuxian_dungeon" / "transaction_service.py").read_text(encoding="utf-8")
    dungeon_team_legacy_transactions = (PACKAGE / "compatibility" / "legacy_dungeon_team_transactions.py").read_text(encoding="utf-8")
    dungeon_team_presentation = (PACKAGE / "features" / "dungeon" / "team_presentation.py").read_text(encoding="utf-8")
    dungeon_reset_compatibility = (PACKAGE / "compatibility" / "legacy_dungeon_reset.py").read_text(encoding="utf-8")
    dungeon_session_compatibility = (PACKAGE / "compatibility" / "legacy_dungeon_session.py").read_text(encoding="utf-8")
    dungeon_purchase_compatibility = (PACKAGE / "compatibility" / "legacy_dungeon_purchase.py").read_text(encoding="utf-8")
    dungeon_explore_compatibility = (PACKAGE / "compatibility" / "legacy_dungeon_explore.py").read_text(encoding="utf-8")
    dungeon_reward_compatibility = (PACKAGE / "compatibility" / "legacy_dungeon_reward.py").read_text(encoding="utf-8")
    dungeon_compatibility_facade = (PACKAGE / "compatibility" / "dungeon.py").read_text(encoding="utf-8")
    dungeon_repository = (PACKAGE / "features" / "dungeon" / "repository.py").read_text(encoding="utf-8")
    dungeon_team_repository = (PACKAGE / "features" / "dungeon" / "team_repository.py").read_text(encoding="utf-8")
    dungeon_invite_expiry = (PACKAGE / "features" / "dungeon" / "invite_expiry.py").read_text(encoding="utf-8")
    dungeon_invite_expiry_tests = (ROOT / "tests" / "test_dungeon_invite_expiry_boundary.py").read_text(encoding="utf-8")
    dungeon_migrations = (PACKAGE / "features" / "dungeon" / "migrations.py").read_text(encoding="utf-8")
    dungeon_reset_repository = (PACKAGE / "features" / "dungeon" / "reset_repository.py").read_text(encoding="utf-8")
    dungeon_application = (PACKAGE / "features" / "dungeon" / "application.py").read_text(encoding="utf-8")
    dungeon_manager = (PACKAGE / "xiuxian" / "xiuxian_dungeon" / "dungeon_manager.py").read_text(encoding="utf-8")
    dungeon_progress_read = dungeon_manager[
        dungeon_manager.index("    def get_dungeon_progress("):
        dungeon_manager.index("    def get_player_status(")
    ]
    dungeon_reset_clock_tests = (ROOT / "tests" / "test_dungeon_reset_clock_boundary.py").read_text(encoding="utf-8")
    dungeon_player_fight = (PACKAGE / "xiuxian" / "xiuxian_utils" / "player_fight.py").read_text(encoding="utf-8")
    bank_facade = (PACKAGE / "xiuxian" / "xiuxian_bank" / "__init__.py").read_text(encoding="utf-8")
    bank_handler = bank_facade[bank_facade.index("async def bank_") : bank_facade.index("def savef")]
    bank_web_application = (PACKAGE / "features" / "bank" / "application.py").read_text(encoding="utf-8")
    bank_account_info_application = (PACKAGE / "features" / "bank" / "account_info_application.py").read_text(encoding="utf-8")
    bank_account_repository = (PACKAGE / "features" / "bank" / "account_repository.py").read_text(encoding="utf-8")
    bank_command_sources = {"handlers": bank_facade, **{
        name: (PACKAGE / "features" / "bank" / filename).read_text(encoding="utf-8")
        for name, filename in {
            "command": "command_application.py", "receipts": "command_receipt_repository.py",
            "replies": "command_replies.py", "deposit": "account_application.py",
            "withdrawal": "account_withdrawal_application.py", "upgrade": "account_upgrade_application.py",
            "interest": "account_interest_application.py",
        }.items()
    }}
    bank_command_owner = _bank_command_owner_status(bank_command_sources)
    bank_account_applications = "\n".join(
        bank_command_sources[name] for name in ("deposit", "withdrawal", "upgrade", "interest")
    )
    bank_upgrade_writer = bank_account_repository[
        bank_account_repository.index("    def save_upgrade(") : bank_account_repository.index("    def save_interest(")
    ]
    bank_migrations = (PACKAGE / "features" / "bank" / "migrations.py").read_text(encoding="utf-8")
    bank_import_application = (PACKAGE / "features" / "bank" / "account_import_application.py").read_text(encoding="utf-8")
    bank_legacy_account_repository = (PACKAGE / "features" / "bank" / "legacy_account_repository.py").read_text(encoding="utf-8")
    bank_legacy_account_storage = (PACKAGE / "compatibility" / "legacy_bank_account_storage.py").read_text(encoding="utf-8")
    bank_jobs = (PACKAGE / "features" / "bank" / "jobs.py").read_text(encoding="utf-8")
    map_facade = (PACKAGE / "xiuxian" / "xiuxian_map" / "__init__.py").read_text(encoding="utf-8")
    map_application = (PACKAGE / "features" / "map" / "application.py").read_text(encoding="utf-8")
    map_reward_resolver = (PACKAGE / "features" / "map" / "rewards.py").read_text(encoding="utf-8")
    map_static_data = (PACKAGE / "features" / "map" / "static_data.py").read_text(encoding="utf-8")
    map_battle_adapter = (PACKAGE / "compatibility" / "legacy_map_battle.py").read_text(encoding="utf-8")
    player_fight = (PACKAGE / "xiuxian" / "xiuxian_utils" / "player_fight.py").read_text(encoding="utf-8")
    map_combat_handler = map_facade[
        map_facade.index("async def _process_node_combat") : map_facade.index("def _get_explore_status")
    ]
    map_legacy_shim = (PACKAGE / "xiuxian" / "xiuxian_map" / "transaction_service.py").read_text(encoding="utf-8")
    map_compatibility = (PACKAGE / "compatibility" / "legacy_map_transactions.py").read_text(encoding="utf-8")
    map_repository = (PACKAGE / "features" / "map" / "repository.py").read_text(encoding="utf-8")
    map_random_repository = (PACKAGE / "features" / "map" / "random_target_repository.py").read_text(encoding="utf-8")
    map_random_query = (PACKAGE / "features" / "map" / "random_target_query.py").read_text(encoding="utf-8")
    map_display_repository = (PACKAGE / "features" / "map" / "nearby_display_repository.py").read_text(encoding="utf-8")
    map_display_query = (PACKAGE / "features" / "map" / "nearby_display_query.py").read_text(encoding="utf-8")
    map_display_handler = map_facade[map_facade.index("@nearby_users_cmd.handle") : map_facade.index("@dao_qc.handle")]
    map_named_target_query = map_repository.split("class MapNearbyPlayersSqlQueryRepository:", 1)[-1].split("    def list(", 1)[0]
    map_qc_handler = map_facade[map_facade.index("@dao_qc.handle") : map_facade.index("@dao_view.handle")]
    map_named_qc = map_qc_handler.split("    if target_name:", 1)[-1].split("    else:", 1)[0]
    map_record_handler = map_facade[map_facade.index("@dao_view.handle") : map_facade.index("@seed_shop.handle")]
    pet_application_source = (PACKAGE / "features" / "pet" / "application.py").read_text(encoding="utf-8")
    pet_repository_source = (PACKAGE / "features" / "pet" / "repository.py").read_text(encoding="utf-8")
    pet_migrations_source = (PACKAGE / "features" / "pet" / "migrations.py").read_text(encoding="utf-8")
    game_event_effects_source = (PACKAGE / "compatibility" / "game_event_effects.py").read_text(encoding="utf-8")
    game_event_statistics_source = (PACKAGE / "features" / "game_events" / "statistics.py").read_text(encoding="utf-8")
    game_event_migrations_source = (PACKAGE / "features" / "game_events" / "migrations.py").read_text(encoding="utf-8")
    combat_settlement_repository = (PACKAGE / "features" / "combat_settlement" / "repository.py").read_text(encoding="utf-8")
    sect_facade = (PACKAGE / "xiuxian" / "xiuxian_sect" / "__init__.py").read_text(encoding="utf-8")
    sect_member_utils = (PACKAGE / "xiuxian" / "xiuxian_sect" / "sect_member_utils.py").read_text(encoding="utf-8")
    sect_elixir_claim_handler = sect_facade[
        sect_facade.index("async def sect_elixir_get_") : sect_facade.index("@sect_buff_info.handle")
    ]
    sect_inactive_owner_handler = sect_facade[
        sect_facade.index("async def auto_handle_inactive_sect_owners") : sect_facade.index("@sect_help.handle")
    ]
    sect_list_handler = sect_facade[
        sect_facade.index("async def sect_list_") : sect_facade.index("@sect_users.handle")
    ]
    sect_materials_grant_handler = sect_facade[
        sect_facade.index("async def materialsupdate_") : sect_facade.index("# 重置用户宗门任务次数")
    ]
    sect_fairyland_upgrade_handler = sect_facade[
        sect_facade.index("async def sect_fairyland_upgrade_") : sect_facade.index(
            "@sect_fairyland_claim.handle", sect_facade.index("async def sect_fairyland_upgrade_")
        )
    ]
    sect_weekly_commands = (PACKAGE / "xiuxian" / "xiuxian_sect" / "sect_weekly_commands.py").read_text(encoding="utf-8")
    sect_weekly_manager = (PACKAGE / "xiuxian" / "xiuxian_sect" / "sect_weekly.py").read_text(encoding="utf-8")
    sect_weekly_progress_repository = (PACKAGE / "features" / "sect" / "weekly_progress_repository.py").read_text(encoding="utf-8")
    sect_fairyland_application = (PACKAGE / "features" / "sect_fairyland" / "application.py").read_text(encoding="utf-8")
    sect_fairyland_repository = (PACKAGE / "features" / "sect_fairyland" / "claim_repository.py").read_text(encoding="utf-8")
    sect_fairyland_legacy_state = (PACKAGE / "xiuxian" / "xiuxian_sect" / "sect_fairyland.py").read_text(encoding="utf-8")
    sect_fairyland_upgrade_repository = (PACKAGE / "features" / "sect" / "fairyland_repository.py").read_text(encoding="utf-8")
    sect_fairyland_compatibility = (PACKAGE / "features" / "sect_fairyland" / "repository.py").read_text(encoding="utf-8")
    sect_transaction_service = (PACKAGE / "xiuxian" / "xiuxian_sect" / "transaction_service.py").read_text(encoding="utf-8")
    sect_membership_legacy_service = (PACKAGE / "compatibility" / "legacy_sect_membership.py").read_text(encoding="utf-8")
    sect_membership_legacy_shim = (PACKAGE / "xiuxian" / "xiuxian_sect" / "membership_service.py").read_text(encoding="utf-8")
    sect_fairyland_legacy_service = (PACKAGE / "compatibility" / "legacy_sect_fairyland_claim.py").read_text(encoding="utf-8")
    sect_fairyland_legacy_shim = (PACKAGE / "xiuxian" / "xiuxian_sect" / "fairyland_claim_service.py").read_text(encoding="utf-8")
    sect_elixir_legacy_service = (PACKAGE / "compatibility" / "legacy_sect_elixir_claim.py").read_text(encoding="utf-8")
    sect_member_join_legacy_service = (PACKAGE / "compatibility" / "legacy_sect_member_join.py").read_text(encoding="utf-8")
    sect_shop_legacy_service = (PACKAGE / "compatibility" / "legacy_sect_shop_purchase.py").read_text(encoding="utf-8")
    sect_main_buff_legacy_service = (PACKAGE / "compatibility" / "legacy_sect_main_buff_learn.py").read_text(encoding="utf-8")
    sect_secondary_buff_legacy_service = (PACKAGE / "compatibility" / "legacy_sect_secondary_buff_learn.py").read_text(encoding="utf-8")
    sect_owner_inherit_legacy_service = (PACKAGE / "compatibility" / "legacy_sect_owner_inherit.py").read_text(encoding="utf-8")
    sect_close_mountain_legacy_service = (PACKAGE / "compatibility" / "legacy_sect_close_mountain.py").read_text(encoding="utf-8")
    sect_join_state_legacy_service = (PACKAGE / "compatibility" / "legacy_sect_join_state.py").read_text(encoding="utf-8")
    sect_disband_legacy_service = (PACKAGE / "compatibility" / "legacy_sect_disband.py").read_text(encoding="utf-8")
    sect_daily_maintenance_legacy_service = (PACKAGE / "compatibility" / "legacy_sect_daily_maintenance.py").read_text(encoding="utf-8")
    sect_weekly_claim_legacy_service = (PACKAGE / "compatibility" / "legacy_sect_weekly_reward_claim.py").read_text(encoding="utf-8")
    sect_fairyland_migrations = (PACKAGE / "features" / "sect_fairyland" / "migrations.py").read_text(encoding="utf-8")
    sect_application = (PACKAGE / "features" / "sect" / "application.py").read_text(encoding="utf-8")
    sect_feature_repository = (PACKAGE / "features" / "sect" / "repository.py").read_text(encoding="utf-8")
    sect_activity_repository = (PACKAGE / "features" / "sect" / "activity_repository.py").read_text(encoding="utf-8")
    sect_task_state_repository = (PACKAGE / "features" / "sect" / "task_state_repository.py").read_text(encoding="utf-8")
    sect_task_settlement_repository = (PACKAGE / "features" / "sect" / "task_settlement_repository.py").read_text(encoding="utf-8")
    sect_directory_repository = (PACKAGE / "features" / "sect" / "directory_repository.py").read_text(encoding="utf-8")
    sect_inactive_owner_repository = (PACKAGE / "features" / "sect" / "inactive_owner_repository.py").read_text(encoding="utf-8")
    sect_info_repository = (PACKAGE / "features" / "sect" / "sect_info_repository.py").read_text(encoding="utf-8")
    sect_member_repository = (PACKAGE / "features" / "sect" / "member_repository.py").read_text(encoding="utf-8")
    sect_disband_repository = (PACKAGE / "features" / "sect" / "disband_repository.py").read_text(encoding="utf-8")
    sect_scheduled_repository = (PACKAGE / "features" / "sect" / "scheduled_material_repository.py").read_text(encoding="utf-8")
    sect_weekly_repository = (PACKAGE / "features" / "sect" / "weekly_reward_repository.py").read_text(encoding="utf-8")
    sect_manual_disband_repository = (PACKAGE / "features" / "sect" / "manual_disband_repository.py").read_text(encoding="utf-8")
    sect_migrations = (PACKAGE / "features" / "sect" / "migrations.py").read_text(encoding="utf-8")
    entertainment_application = (PACKAGE / "features" / "entertainment" / "application.py").read_text(encoding="utf-8")
    entertainment_facade = (PACKAGE / "xiuxian" / "xiuxian_entertainment" / "mod" / "newapi_store.py").read_text(encoding="utf-8")
    entertainment_guess_application = (PACKAGE / "features" / "entertainment" / "guess_application.py").read_text(encoding="utf-8")
    entertainment_guess_repository = (PACKAGE / "features" / "entertainment" / "guess_repository.py").read_text(encoding="utf-8")
    entertainment_migrations = (PACKAGE / "features" / "entertainment" / "migrations.py").read_text(encoding="utf-8")
    entertainment_number = (PACKAGE / "xiuxian" / "xiuxian_entertainment" / "mod" / "guess_number.py").read_text(encoding="utf-8")
    entertainment_puzzle = (PACKAGE / "xiuxian" / "xiuxian_entertainment" / "mod" / "guess_number_puzzle.py").read_text(encoding="utf-8")
    partner_facade = (PACKAGE / "xiuxian" / "xiuxian_buff" / "partner.py").read_text(encoding="utf-8")
    partner_cultivation_application = (PACKAGE / "features" / "buff" / "partner_cultivation_application.py").read_text(encoding="utf-8")
    partner_cultivation_repository = (PACKAGE / "features" / "buff" / "partner_cultivation_repository.py").read_text(encoding="utf-8")
    buff_migrations = (PACKAGE / "features" / "buff" / "migrations.py").read_text(encoding="utf-8")
    pvp_repository = (PACKAGE / "features" / "buff" / "pvp_repository.py").read_text(encoding="utf-8")
    natal_facade = (PACKAGE / "xiuxian" / "xiuxian_natal_treasure" / "__init__.py").read_text(encoding="utf-8")
    world_events_facade = (PACKAGE / "xiuxian" / "xiuxian_world_events" / "__init__.py").read_text(encoding="utf-8")
    world_events_plugin = (PACKAGE / "plugin.py").read_text(encoding="utf-8")
    world_events_application = (PACKAGE / "features" / "world_events" / "application.py").read_text(encoding="utf-8")
    world_events_repository = (PACKAGE / "features" / "world_events" / "repository.py").read_text(encoding="utf-8")
    world_events_attack_application = (PACKAGE / "features" / "world_events" / "attack_application.py").read_text(encoding="utf-8")
    world_events_attack_repository = (PACKAGE / "features" / "world_events" / "attack_repository.py").read_text(encoding="utf-8")
    world_events_lifecycle_repository = (PACKAGE / "features" / "world_events" / "lifecycle_repository.py").read_text(encoding="utf-8")
    world_events_wave_repository = (PACKAGE / "features" / "world_events" / "wave_refresh_repository.py").read_text(encoding="utf-8")
    world_events_migrations = (PACKAGE / "features" / "world_events" / "migrations.py").read_text(encoding="utf-8")
    world_events_transaction = (PACKAGE / "xiuxian" / "xiuxian_world_events" / "transaction_service.py").read_text(encoding="utf-8")
    world_events_attack_compatibility = (PACKAGE / "compatibility" / "legacy_demon_attack_settlement.py").read_text(encoding="utf-8")
    world_events_lifecycle_compatibility = (PACKAGE / "compatibility" / "legacy_demon_event_lifecycle.py").read_text(encoding="utf-8")
    world_events_wave_compatibility = (PACKAGE / "compatibility" / "legacy_demon_wave_refresh.py").read_text(encoding="utf-8")
    world_events_spirit_compatibility = (PACKAGE / "compatibility" / "legacy_spirit_vein_lifecycle.py").read_text(encoding="utf-8")
    world_events_claim_compatibility = (PACKAGE / "compatibility" / "legacy_demon_claim.py").read_text(encoding="utf-8")
    rift_facade = (PACKAGE / "xiuxian" / "xiuxian_rift" / "__init__.py").read_text(encoding="utf-8")
    rift_event_handler = rift_facade[
        rift_facade.index("async def _roll_rift_event") : rift_facade.index(
            "async def _roll_rift_boss_event", rift_facade.index("async def _roll_rift_event")
        )
    ]
    rift_boss_handler = rift_facade[
        rift_facade.index("async def _roll_rift_boss_event") : rift_facade.index(
            "@complete_rift.handle", rift_facade.index("async def _roll_rift_boss_event")
        )
    ]
    rift_jsondata = (PACKAGE / "xiuxian" / "xiuxian_rift" / "jsondata.py").read_text(encoding="utf-8")
    rift_application = (PACKAGE / "features" / "rift" / "application.py").read_text(encoding="utf-8")
    rift_domain = (PACKAGE / "features" / "rift" / "domain.py").read_text(encoding="utf-8")
    rift_make = (PACKAGE / "xiuxian" / "xiuxian_rift" / "riftmake.py").read_text(encoding="utf-8")
    rift_player_fight = (PACKAGE / "xiuxian" / "xiuxian_utils" / "player_fight.py").read_text(encoding="utf-8")
    rift_attributes = (PACKAGE / "xiuxian" / "xiuxian_utils" / "xiuxian2_handle.py").read_text(encoding="utf-8")
    rift_cooldown_repository = (PACKAGE / "features" / "rift" / "cooldown_repository.py").read_text(encoding="utf-8")
    rift_generation_repository = (PACKAGE / "features" / "rift" / "generation_repository.py").read_text(encoding="utf-8")
    rift_key_event_repository = (PACKAGE / "features" / "rift" / "key_event_repository.py").read_text(encoding="utf-8")
    rift_settlement_repository = (PACKAGE / "features" / "rift" / "settlement_repository.py").read_text(encoding="utf-8")
    rift_entry_repository = (PACKAGE / "features" / "rift" / "entry_repository.py").read_text(encoding="utf-8")
    rift_termination_repository = (PACKAGE / "features" / "rift" / "termination_repository.py").read_text(encoding="utf-8")
    rift_speedup_repository = (PACKAGE / "features" / "rift" / "speedup_repository.py").read_text(encoding="utf-8")
    rift_migrations = (PACKAGE / "features" / "rift" / "migrations.py").read_text(encoding="utf-8")
    back_facade = (PACKAGE / "xiuxian" / "xiuxian_back" / "__init__.py").read_text(encoding="utf-8")
    back_accessory_facade = (PACKAGE / "xiuxian" / "xiuxian_back" / "accessory.py").read_text(encoding="utf-8")
    back_util_facade = (PACKAGE / "xiuxian" / "xiuxian_back" / "back_util.py").read_text(encoding="utf-8")
    back_application_source = (PACKAGE / "features" / "back" / "application.py").read_text(encoding="utf-8")
    daily_pill_reset_application = (PACKAGE / "features" / "back" / "daily_pill_usage_reset_application.py").read_text(encoding="utf-8")
    daily_pill_reset_repository = (PACKAGE / "features" / "back" / "daily_pill_usage_reset_repository.py").read_text(encoding="utf-8")
    beg_application = (PACKAGE / "features" / "beg" / "application.py").read_text(encoding="utf-8")
    beg_command_sources = {
        "facade": (PACKAGE / "xiuxian/xiuxian_beg/__init__.py").read_text(encoding="utf-8"),
        "command": (PACKAGE / "features/beg/command_application.py").read_text(encoding="utf-8"),
        "reads": (PACKAGE / "features/beg/command_repository.py").read_text(encoding="utf-8"),
        "application": beg_application,
        "replies": (PACKAGE / "features/beg/command_replies.py").read_text(encoding="utf-8"),
    }
    beg_daily_reset_repository = (PACKAGE / "features" / "beg" / "daily_reset_repository.py").read_text(encoding="utf-8")
    beg_daily_reset_tests = (PACKAGE / "features" / "beg" / "tests" / "test_daily_reset_repository.py").read_text(encoding="utf-8")
    past_life_events_facade = (PACKAGE / "xiuxian" / "xiuxian_past_life" / "past_life_events.py").read_text(encoding="utf-8")
    past_life_command_facade = (PACKAGE / "xiuxian" / "xiuxian_past_life" / "__init__.py").read_text(encoding="utf-8")
    dufang_facade = (PACKAGE / "xiuxian" / "xiuxian_dufang" / "__init__.py").read_text(encoding="utf-8")
    dufang_unseal_handler = dufang_facade[
        dufang_facade.index("async def unseal_(bot"):dufang_facade.index("# 尘封之物类型")
    ]
    dufang_application = (PACKAGE / "features" / "dufang" / "application.py").read_text(encoding="utf-8")
    dufang_repository = (PACKAGE / "features" / "dufang" / "repository.py").read_text(encoding="utf-8")
    dufang_bet_repository = (PACKAGE / "features" / "dufang" / "bet_repository.py").read_text(encoding="utf-8")
    dufang_payout_repository = (PACKAGE / "features" / "dufang" / "payout_repository.py").read_text(encoding="utf-8")
    dufang_player_stats_repository = (PACKAGE / "features" / "dufang" / "player_stats_repository.py").read_text(encoding="utf-8")
    dufang_sharing_preferences_repository = (PACKAGE / "features" / "dufang" / "sharing_preferences_repository.py").read_text(encoding="utf-8")
    dufang_share_repository = (PACKAGE / "features" / "dufang" / "share_repository.py").read_text(encoding="utf-8")
    dufang_migrations = (PACKAGE / "features" / "dufang" / "migrations.py").read_text(encoding="utf-8")
    dufang_storage_audit = (ROOT / "scripts" / "audit_dufang_storage.py").read_text(encoding="utf-8")
    backup_capacity_source = (PACKAGE / "infrastructure" / "database" / "backup_capacity.py").read_text(encoding="utf-8")
    backup_service_source = (PACKAGE / "infrastructure" / "database" / "backup.py").read_text(encoding="utf-8")
    backup_web_source = (PACKAGE / "adapters" / "web" / "blueprints" / "backups.py").read_text(encoding="utf-8")
    avatar_facade = (PACKAGE / "xiuxian" / "xiuxian_info" / "avatar.py").read_text(encoding="utf-8")
    avatar_utils = (PACKAGE / "xiuxian" / "xiuxian_utils" / "utils.py").read_text(encoding="utf-8")
    avatar_base_facade = (PACKAGE / "xiuxian" / "xiuxian_base" / "__init__.py").read_text(encoding="utf-8")
    avatar_repository = (PACKAGE / "features" / "info" / "avatar_repository.py").read_text(encoding="utf-8")
    avatar_migrations = (PACKAGE / "features" / "info" / "migrations.py").read_text(encoding="utf-8")
    legacy_migrated_source = (PACKAGE / "features" / "_legacy_migrated.py").read_text(encoding="utf-8")
    fusion_facade = (PACKAGE / "xiuxian" / "xiuxian_fusion" / "__init__.py").read_text(encoding="utf-8")
    fusion_repository = (PACKAGE / "features" / "fusion" / "repository.py").read_text(encoding="utf-8")
    fusion_settlement_repository = (PACKAGE / "features" / "fusion" / "settlement_repository.py").read_text(encoding="utf-8")
    fusion_migrations = (PACKAGE / "features" / "fusion" / "migrations.py").read_text(encoding="utf-8")
    fusion_compatibility = (PACKAGE / "xiuxian" / "xiuxian_fusion" / "fusion_service.py").read_text(encoding="utf-8")
    title_facade = (PACKAGE / "xiuxian" / "xiuxian_title" / "__init__.py").read_text(encoding="utf-8")
    base_facade = (PACKAGE / "xiuxian" / "xiuxian_base" / "__init__.py").read_text(encoding="utf-8")
    puppet_facade = (PACKAGE / "xiuxian" / "xiuxian_puppet" / "__init__.py").read_text(encoding="utf-8")
    puppet_application_source = (PACKAGE / "features" / "puppet" / "application.py").read_text(encoding="utf-8")
    puppet_status_repository_source = (PACKAGE / "features" / "puppet" / "status_repository.py").read_text(encoding="utf-8")
    puppet_migrations_source = (PACKAGE / "features" / "puppet" / "migrations.py").read_text(encoding="utf-8")
    puppet_status_tests = (PACKAGE / "features" / "puppet" / "tests" / "test_status_repository.py").read_text(encoding="utf-8")
    plugin_source = (PACKAGE / "plugin.py").read_text(encoding="utf-8")
    pet_facade = (PACKAGE / "xiuxian" / "xiuxian_pet" / "__init__.py").read_text(encoding="utf-8")
    trade_facade = (PACKAGE / "xiuxian" / "xiuxian_trade" / "__init__.py").read_text(encoding="utf-8")
    trade_application_source = (PACKAGE / "features" / "trade" / "application.py").read_text(encoding="utf-8")
    trade_manifest_source = (PACKAGE / "features" / "trade" / "manifest.py").read_text(encoding="utf-8")
    trade_web_source = (PACKAGE / "features" / "trade" / "web.py").read_text(encoding="utf-8")
    trade_web_test_source = (ROOT / "tests" / "test_trade_auction_web.py").read_text(encoding="utf-8")
    trade_xianshi_transactions = (PACKAGE / "features" / "trade" / "xianshi_listing_repository.py").read_text(encoding="utf-8")
    trade_plan_xianshi_transactions = (PACKAGE / "features" / "trade" / "xianshi_plan_listing_repository.py").read_text(encoding="utf-8")
    trade_xianshi_removal_transactions = (PACKAGE / "features" / "trade" / "xianshi_removal_repository.py").read_text(encoding="utf-8")
    trade_xianshi_purchase_transactions = (PACKAGE / "features" / "trade" / "xianshi_purchase_repository.py").read_text(encoding="utf-8")
    trade_xianshi_query_repository = (PACKAGE / "features" / "trade" / "xianshi_query_repository.py").read_text(encoding="utf-8")
    trade_xianshi_schema_adapter = (PACKAGE / "compatibility" / "legacy_xianshi_schema.py").read_text(encoding="utf-8")
    trade_feature_repository = (PACKAGE / "features" / "trade" / "repository.py").read_text(encoding="utf-8")
    legacy_trade_auction_compatibility = (PACKAGE / "compatibility" / "legacy_trade_auction_sessions.py").read_text(encoding="utf-8")
    trade_auction_transactions = (PACKAGE / "xiuxian" / "xiuxian_trade" / "transaction_service.py").read_text(encoding="utf-8")
    auction_settlement_source = (PACKAGE / "features" / "auction" / "settlement.py").read_text(encoding="utf-8")
    trade_legacy_guishi_compatibility = (PACKAGE / "compatibility" / "legacy_guishi_stone.py").read_text(encoding="utf-8")
    trade_deposit_handler = trade_facade[
        trade_facade.index("async def guishi_deposit_") : trade_facade.index(
            "@guishi_withdraw.handle", trade_facade.index("async def guishi_deposit_")
        )
    ]
    trade_withdraw_handler = trade_facade[
        trade_facade.index("async def guishi_withdraw_") : trade_facade.index(
            "@guishi_qiugou.handle", trade_facade.index("async def guishi_withdraw_")
        )
    ]
    trade_qiugou_handler = trade_facade[
        trade_facade.index("async def guishi_qiugou_") : trade_facade.index(
            "@guishi_cancel_qiugou.handle", trade_facade.index("async def guishi_qiugou_")
        )
    ]
    trade_baitan_handler = trade_facade[
        trade_facade.index("async def guishi_baitan_") : trade_facade.index(
            "@guishi_shoutan.handle", trade_facade.index("async def guishi_baitan_")
        )
    ]
    trade_cancel_qiugou_handler = trade_facade[
        trade_facade.index("async def guishi_cancel_qiugou_") : trade_facade.index(
            "@guishi_baitan.handle", trade_facade.index("async def guishi_cancel_qiugou_")
        )
    ]
    trade_cancel_baitan_handler = trade_facade[
        trade_facade.index("async def guishi_shoutan_") : trade_facade.index(
            "@guishi_take_item.handle", trade_facade.index("async def guishi_shoutan_")
        )
    ]
    trade_expired_job = trade_facade[
        trade_facade.index("async def clear_expired_baitan_orders_job") : trade_facade.index(
            "@auction_view.handle", trade_facade.index("async def clear_expired_baitan_orders_job")
        )
    ]
    trade_take_handler = trade_facade[
        trade_facade.index("async def guishi_take_item_(") : trade_facade.index(
            "@guishi_info.handle", trade_facade.index("async def guishi_take_item_(")
        )
    ]
    xianshi_listing_handler = trade_facade[
        trade_facade.index("async def xian_shop_add_(") : trade_facade.index(
            "@xianshi_auto_add.handle", trade_facade.index("async def xian_shop_add_(")
        )
    ]
    xianshi_auto_listing_handler = trade_facade[
        trade_facade.index("async def xianshi_auto_add_(") : trade_facade.index(
            "@xianshi_fast_add.handle", trade_facade.index("async def xianshi_auto_add_(")
        )
    ]
    xianshi_fast_listing_handler = trade_facade[
        trade_facade.index("async def xianshi_fast_add_(") : trade_facade.index(
            "@xiuxian_shop_view.handle", trade_facade.index("async def xianshi_fast_add_(")
        )
    ]
    xianshi_system_listing_handler = trade_facade[
        trade_facade.index("async def xian_shop_added_by_admin_(") : trade_facade.index(
            "@xian_shop_remove_by_admin.handle",
            trade_facade.index("async def xian_shop_added_by_admin_("),
        )
    ]
    xianshi_name_removal_handler = trade_facade[
        trade_facade.index("async def xian_shop_remove_(") : trade_facade.index(
            "@xian_buy.handle", trade_facade.index("async def xian_shop_remove_(")
        )
    ]
    xianshi_clear_handler = trade_facade[
        trade_facade.index("async def xian_shop_off_all_(") : trade_facade.index(
            "@xian_shop_added_by_admin.handle", trade_facade.index("async def xian_shop_off_all_(")
        )
    ]
    xianshi_admin_removal_handler = trade_facade[
        trade_facade.index("async def xian_shop_remove_by_admin_(") : trade_facade.index(
            "# --- 鬼市命令处理 ---", trade_facade.index("async def xian_shop_remove_by_admin_(")
        )
    ]
    auction_settlement = (PACKAGE / "features" / "auction" / "settlement.py").read_text(encoding="utf-8")
    auction_settlement_statistics = (PACKAGE / "features" / "auction" / "settlement_statistics.py").read_text(encoding="utf-8")
    auction_settlement_compat = (PACKAGE / "compatibility" / "auction_settlement_effects.py").read_text(encoding="utf-8")
    auction_bid = (PACKAGE / "features" / "auction" / "bid_repository.py").read_text(encoding="utf-8")
    auction_bid_application = (PACKAGE / "features" / "auction" / "application.py").read_text(encoding="utf-8")
    auction_bid_effects = (PACKAGE / "features" / "auction" / "bid_effects.py").read_text(encoding="utf-8")
    auction_bid_statistics = (PACKAGE / "features" / "auction" / "bid_statistics.py").read_text(encoding="utf-8")
    auction_compat_effects = (PACKAGE / "compatibility" / "auction_bid_effects.py").read_text(encoding="utf-8")
    web_app_source = (PACKAGE / "adapters" / "web" / "app.py").read_text(encoding="utf-8")
    cli_source = (PACKAGE / "cli.py").read_text(encoding="utf-8")
    auction_queue = (PACKAGE / "features" / "auction" / "queue_application.py").read_text(encoding="utf-8")
    auction_start = (PACKAGE / "features" / "auction" / "session_start_application.py").read_text(encoding="utf-8")
    auction_query = (PACKAGE / "features" / "auction" / "query_application.py").read_text(encoding="utf-8")
    auction_query_repository = (PACKAGE / "features" / "auction" / "query_repository.py").read_text(encoding="utf-8")
    auction_queue_handlers = trade_facade[
        trade_facade.index("async def auction_add_(") : trade_facade.index(
            "@my_auction.handle", trade_facade.index("async def auction_add_(")
        )
    ]
    boss_facade = (PACKAGE / "xiuxian" / "xiuxian_boss" / "__init__.py").read_text(encoding="utf-8")
    reward_service_source = (PACKAGE / "xiuxian" / "xiuxian_utils" / "reward_service.py").read_text(encoding="utf-8")
    boss_application = (PACKAGE / "features" / "boss" / "application.py").read_text(encoding="utf-8")
    boss_purchase_application = (PACKAGE / "features" / "boss" / "purchase_command_application.py").read_text(encoding="utf-8")
    boss_purchase_command_repository = (PACKAGE / "features" / "boss" / "purchase_command_repository.py").read_text(encoding="utf-8")
    boss_purchase_replies = (PACKAGE / "features" / "boss" / "purchase_command_replies.py").read_text(encoding="utf-8")
    boss_purchase_execute = boss_purchase_application[boss_purchase_application.index("    def execute("):]
    boss_purchase_repository = (PACKAGE / "features" / "boss" / "repository.py").read_text(encoding="utf-8")
    boss_world_repository = (PACKAGE / "features" / "boss" / "world_boss_repository.py").read_text(encoding="utf-8")
    boss_migrations = (PACKAGE / "features" / "boss" / "migrations.py").read_text(encoding="utf-8")
    boss_shop_handler = boss_facade[
        boss_facade.index("async def boss_integral_store_") : boss_facade.index(
            "@boss_integral_info.handle", boss_facade.index("async def boss_integral_store_")
        )
    ]
    boss_purchase_handler = boss_facade[
        boss_facade.index("async def boss_integral_use_") : boss_facade.index(
            "@boss_integral_rank.handle", boss_facade.index("async def boss_integral_use_")
        )
    ]
    boss_weekly_snapshot_repository = boss_purchase_repository[
        boss_purchase_repository.index(
            "    def weekly_purchases(", boss_purchase_repository.index("class BossPurchaseSqlRepository")
        ) : boss_purchase_repository.index("    @staticmethod\n    def _weekly")
    ]
    boss_purchase_write_repository = boss_purchase_repository[
        boss_purchase_repository.index("    def purchase(", boss_purchase_repository.index("class BossPurchaseSqlRepository")) : boss_purchase_repository.index(
            "    @staticmethod\n    def _record", boss_purchase_repository.index("class BossPurchaseSqlRepository")
        )
    ]
    buff_facade = (PACKAGE / "xiuxian" / "xiuxian_buff" / "__init__.py").read_text(encoding="utf-8")
    buff_closing_enter_handler = buff_facade[
        buff_facade.index("async def in_closing_") : buff_facade.index(
            "@out_closing.handle", buff_facade.index("async def in_closing_")
        )
    ]
    buff_closing_handler = buff_facade[
        buff_facade.index("async def out_closing_") : buff_facade.index(
            "@mind_state.handle", buff_facade.index("async def out_closing_")
        )
    ]
    buff_training_handler = buff_facade[
        buff_facade.index("async def up_exp_") : buff_facade.index("@stone_exp.handle")
    ]
    buff_application_source = (PACKAGE / "features" / "buff" / "application.py").read_text(encoding="utf-8")
    buff_training_start_repository = (PACKAGE / "features" / "buff" / "training_start_repository.py").read_text(encoding="utf-8")
    buff_training_complete_repository = (PACKAGE / "features" / "buff" / "training_complete_repository.py").read_text(encoding="utf-8")
    buff_closing_repository = (PACKAGE / "features" / "buff" / "closing_repository.py").read_text(encoding="utf-8")
    buff_closing_enter_repository = (PACKAGE / "features" / "buff" / "closing_enter_repository.py").read_text(encoding="utf-8")
    buff_closing_reward = (PACKAGE / "features" / "buff" / "closing_reward.py").read_text(encoding="utf-8")
    buff_closing_effects = (
        (PACKAGE / "features" / "buff" / "closing_effects_application.py").read_text(encoding="utf-8")
        + (PACKAGE / "features" / "buff" / "closing_log.py").read_text(encoding="utf-8")
    )
    buff_migrations = (PACKAGE / "features" / "buff" / "migrations.py").read_text(encoding="utf-8")
    buff_normalize_experience_handler = buff_facade[
        buff_facade.index("@del_exp_decimal.handle") : buff_facade.index("@daily_info.handle")
    ]
    impart_facade = (PACKAGE / "xiuxian" / "xiuxian_impart" / "__init__.py").read_text(encoding="utf-8")
    impart_catalog = (PACKAGE / "features" / "impart" / "catalog.py").read_text(encoding="utf-8")
    impart_draw_repository = (PACKAGE / "features" / "impart" / "draw_repository.py").read_text(encoding="utf-8")
    impart_legacy_utils = (PACKAGE / "xiuxian" / "xiuxian_impart" / "impart_uitls.py").read_text(encoding="utf-8")
    impart_prayer_repository = (PACKAGE / "features" / "impart" / "prayer_repository.py").read_text(encoding="utf-8")
    impart_migrations = (PACKAGE / "features" / "impart" / "migrations.py").read_text(encoding="utf-8")
    impart_prayer_handler = impart_facade[
        impart_facade.index("async def use_wishing_stone") : impart_facade.index(
            "async def use_love_sand", impart_facade.index("async def use_wishing_stone")
        )
    ]
    impart_paid_draw_handler = impart_facade[
        impart_facade.index("async def impart_draw2_") : impart_facade.index("async def use_wishing_stone")
    ]
    mixelixir_facade = (PACKAGE / "xiuxian" / "xiuxian_mixelixir" / "__init__.py").read_text(encoding="utf-8")
    mixelixir_application = (PACKAGE / "features" / "mixelixir" / "application.py").read_text(encoding="utf-8")
    mixelixir_daily_reset = (PACKAGE / "features" / "mixelixir" / "daily_reset_repository.py").read_text(encoding="utf-8")
    mixelixir_scheduler = (PACKAGE / "xiuxian" / "xiuxian_scheduler" / "__init__.py").read_text(encoding="utf-8")
    dongfu_facade = (PACKAGE / "xiuxian" / "xiuxian_dongfu" / "__init__.py").read_text(encoding="utf-8")
    dongfu_legacy_shim = (PACKAGE / "xiuxian" / "xiuxian_dongfu" / "transaction_service.py").read_text(encoding="utf-8")
    dongfu_compatibility = (PACKAGE / "compatibility" / "legacy_dongfu_transactions.py").read_text(encoding="utf-8")
    dongfu_migrations = (PACKAGE / "features" / "dongfu" / "migrations.py").read_text(encoding="utf-8")
    dongfu_status_repository = (PACKAGE / "features" / "dongfu" / "status_repository.py").read_text(encoding="utf-8")
    dongfu_nearby_target_repository = (PACKAGE / "features" / "dongfu" / "nearby_target_repository.py").read_text(encoding="utf-8")
    dongfu_random_target_repository = (PACKAGE / "features" / "dongfu" / "random_target_repository.py").read_text(encoding="utf-8")
    dongfu_random_target_query = (PACKAGE / "features" / "dongfu" / "random_target_query.py").read_text(encoding="utf-8")
    dongfu_expansion_repository = (PACKAGE / "features" / "dongfu" / "expansion_repository.py").read_text(encoding="utf-8")
    dongfu_plant_slots = (PACKAGE / "features" / "dongfu" / "plant_slots.py").read_text(encoding="utf-8")
    dongfu_application = (PACKAGE / "features" / "dongfu" / "application.py").read_text(encoding="utf-8")
    dongfu_repository = (PACKAGE / "features" / "dongfu" / "repository.py").read_text(encoding="utf-8")
    map_migrations = (PACKAGE / "features" / "map" / "migrations.py").read_text(encoding="utf-8")
    dongfu_operation_schema = (PACKAGE / "features" / "dongfu" / "operation_schema.py").read_text(encoding="utf-8")
    dongfu_operation_repositories = tuple(
        (PACKAGE / "features" / "dongfu" / f"{name}_repository.py").read_text(encoding="utf-8")
        for name in (
            "accelerate",
            "array",
            "expansion",
            "fertilize",
            "harvest",
            "patrol",
            "plant",
            "visit_reward",
        )
    )
    dongfu_visit_reward_repository = (PACKAGE / "features" / "dongfu" / "visit_reward_repository.py").read_text(encoding="utf-8")
    dongfu_infiltration_plan_repository = (PACKAGE / "features" / "dongfu" / "infiltration_plan_repository.py").read_text(encoding="utf-8")
    dongfu_operation_receipt_repository = (PACKAGE / "features" / "dongfu" / "operation_receipt_repository.py").read_text(encoding="utf-8")
    dongfu_settlement_helper = dongfu_facade[
        dongfu_facade.index("async def _settle_infiltration_plan") : dongfu_facade.index(
            "@infiltrate_dongfu.handle", dongfu_facade.index("async def _settle_infiltration_plan")
        )
    ]
    dongfu_infiltration_handler = dongfu_facade[
        dongfu_facade.index("@infiltrate_dongfu.handle") :
    ]
    impart_pk_facade = (PACKAGE / "xiuxian" / "xiuxian_impart_pk" / "__init__.py").read_text(encoding="utf-8")
    admin_facade = (PACKAGE / "xiuxian" / "xiuxian_admin" / "__init__.py").read_text(encoding="utf-8")
    admin_config_handlers = admin_facade[
        admin_facade.index("@set_xiuxian.handle") : admin_facade.index("@xiuxian_updata_level.handle")
    ]
    admin_welcome = (PACKAGE / "xiuxian" / "xiuxian_admin" / "group_welcome.py").read_text(encoding="utf-8")
    admin_config_application = (PACKAGE / "features" / "admin" / "config_application.py").read_text(encoding="utf-8")
    admin_config_repository = (PACKAGE / "features" / "admin" / "config_repository.py").read_text(encoding="utf-8")
    admin_config_compat = (PACKAGE / "xiuxian" / "xiuxian_config.py").read_text(encoding="utf-8")
    admin_config_tests = (ROOT / "tests" / "test_admin_config_owner.py").read_text(encoding="utf-8")
    admin_mutation_tests = (ROOT / "tests" / "test_admin_mutation_results.py").read_text(encoding="utf-8")
    xiangyuan_clear_tests = (ROOT / "tests" / "test_xiangyuan_clear_all.py").read_text(encoding="utf-8")
    admin_event_debug = (PACKAGE / "xiuxian" / "xiuxian_admin" / "event_debug.py").read_text(encoding="utf-8")
    admin_command_controls = (PACKAGE / "xiuxian" / "xiuxian_admin" / "command_controls.py").read_text(encoding="utf-8")
    admin_event_debug_tests = (ROOT / "tests" / "test_admin_event_debug_compat.py").read_text(encoding="utf-8")
    admin_output_tests = (ROOT / "tests" / "test_admin_output_commands_compat.py").read_text(encoding="utf-8")
    admin_all_apply_handler = output_function(admin_command_controls, "all_apply_cmd_")
    admin_blackhouse_handlers = {
        name: output_function(admin_facade, name)
        for name in ("blackhouse_", "unblackhouse_", "view_blackhouse_")
    }
    admin_blackhouse_repository = (PACKAGE / "features/admin/blackhouse_repository.py").read_text(encoding="utf-8")
    admin_blackhouse_compat = (PACKAGE / "xiuxian/blackhouse.py").read_text(encoding="utf-8")
    admin_blackhouse_router = (PACKAGE / "xiuxian/on_compat.py").read_text(encoding="utf-8")
    admin_blackhouse_status_tests = (ROOT / "tests/test_admin_blackhouse_status.py").read_text(encoding="utf-8")
    admin_blackhouse_routing_tests = (ROOT / "tests/test_admin_blackhouse_routing.py").read_text(encoding="utf-8")
    admin_blackhouse_migration_tests = (ROOT / "tests/test_admin_blackhouse_migration.py").read_text(encoding="utf-8")
    admin_blackhouse_repository_tests = (PACKAGE / "features/admin/tests/test_blackhouse_repository.py").read_text(encoding="utf-8")
    admin_command_application = (PACKAGE / "features/admin/command_control_application.py").read_text(encoding="utf-8")
    admin_command_repository = (PACKAGE / "features/admin/command_control_repository.py").read_text(encoding="utf-8")
    admin_command_compat = (PACKAGE / "xiuxian/command_disable.py").read_text(encoding="utf-8")
    admin_command_handlers = {
        name: output_function(admin_command_controls, name)
        for name in ("cmd_disable_", "cmd_enable_", "cmd_list_")
    }
    admin_command_tests = (ROOT / "tests/test_admin_command_control.py").read_text(encoding="utf-8")
    admin_command_repository_tests = (PACKAGE / "features/admin/tests/test_command_control_repository.py").read_text(encoding="utf-8")
    admin_broadcast_sources = {
        "application": (PACKAGE / "features/admin/broadcast_application.py").read_text(encoding="utf-8"),
        "repository": (PACKAGE / "features/admin/broadcast_repository.py").read_text(encoding="utf-8"),
        "history": (PACKAGE / "features/admin/broadcast_history_repository.py").read_text(encoding="utf-8"),
        "facade": (PACKAGE / "xiuxian/broadcast_manager.py").read_text(encoding="utf-8"),
        "handlers": admin_facade,
        "web": (PACKAGE / "xiuxian/xiuxian_web/messages.py").read_text(encoding="utf-8"),
        "events": (PACKAGE / "xiuxian/__init__.py").read_text(encoding="utf-8"),
        "entry_tests": (ROOT / "tests/test_admin_broadcast.py").read_text(encoding="utf-8"),
        "core_tests": (PACKAGE / "features/admin/tests/test_broadcast_application.py").read_text(encoding="utf-8"),
        "history_tests": (PACKAGE / "features/admin/tests/test_broadcast_history_repository.py").read_text(encoding="utf-8"),
    }
    admin_runtime_sources = {
        "handlers": admin_facade,
        **{name: (PACKAGE / path).read_text(encoding="utf-8") for name, path in {
            "rift": "xiuxian/xiuxian_rift/__init__.py",
            "rift_application": "features/rift/application.py",
            "catalog_facade": "xiuxian/xiuxian_utils/item_json.py",
            "catalog_application": "features/admin/item_catalog_application.py",
            "catalog_repository": "features/admin/item_catalog_repository.py",
            "utils": "xiuxian/xiuxian_utils/utils.py",
            "impersonation_application": "features/admin/impersonation_application.py",
            "impersonation_repository": "features/admin/impersonation_repository.py",
        }.items()},
    }
    admin_status_batch_handler = admin_facade[
        admin_facade.index("async def restate_") : admin_facade.index(
            "@set_xiuxian.handle", admin_facade.index("async def restate_")
        )
    ]
    admin_rename_handler = admin_facade[
        admin_facade.index("@admin_rename_cmd.handle") : admin_facade.index("# GM加灵石")
    ]
    admin_stone_batch_handler = admin_facade[
        admin_facade.index("@gm_command.handle") : admin_facade.index("# GM加思恋结晶")
    ]
    admin_item_destroy_handler = admin_facade[
        admin_facade.index("@hmll.handle") : admin_facade.index("@restate.handle")
    ]
    admin_item_grant_handler = admin_facade[
        admin_facade.index("@cz.handle") : admin_facade.index("@hmll.handle")
    ]
    admin_level_handler = admin_facade[
        admin_facade.index("async def zaohua_xiuxian_") : admin_facade.index("@gmm_command.handle")
    ]
    admin_realm_adaptation_handler = admin_facade[
        admin_facade.index("@xiuxian_updata_level.handle") : admin_facade.index("@clear_xiangyuan.handle")
    ]
    admin_novice_reset_handler = admin_facade[
        admin_facade.index("@xiuxian_novice.handle") : admin_facade.index("@create_new_rift.handle")
    ]
    admin_work_reset_handler = admin_facade[
        admin_facade.index("@do_work_cz.handle") : admin_facade.index("@training_reset.handle")
    ]
    admin_training_reset_handler = admin_facade[
        admin_facade.index("@training_reset.handle") : admin_facade.index("@tower_reset.handle")
    ]
    admin_root_handler = admin_facade[
        admin_facade.index("async def gmm_command_") : admin_facade.index("@cz.handle")
    ]
    admin_asset_application = (PACKAGE / "features" / "admin_asset" / "application.py").read_text(encoding="utf-8")
    legacy_admin_application = (PACKAGE / "features" / "admin" / "application.py").read_text(encoding="utf-8")
    admin_status_batch_repository = (PACKAGE / "features" / "admin" / "player_status_batch_repository.py").read_text(encoding="utf-8")
    admin_feature_migrations = (PACKAGE / "features" / "admin" / "migrations.py").read_text(encoding="utf-8")
    legacy_migrated = (PACKAGE / "features" / "_legacy_migrated.py").read_text(encoding="utf-8")
    admin_status_batch_repository_tests = (PACKAGE / "features" / "admin" / "tests" / "test_player_status_batch_repository.py").read_text(encoding="utf-8")
    admin_asset_stone_application = admin_asset_application[
        admin_asset_application.index("    def adjust_stone(") : admin_asset_application.index("    def adjust_impart_stone(")
    ]
    admin_stone_repository = (PACKAGE / "features" / "admin_asset" / "stone_repository.py").read_text(encoding="utf-8")
    admin_stone_batch_repository = (PACKAGE / "features" / "admin_asset" / "stone_batch_repository.py").read_text(encoding="utf-8")
    admin_exp_repository = (PACKAGE / "features" / "admin_asset" / "exp_repository.py").read_text(encoding="utf-8")
    admin_item_destroy_repository = (PACKAGE / "features" / "admin_asset" / "item_destroy_repository.py").read_text(encoding="utf-8")
    admin_item_repository = (PACKAGE / "features" / "admin_asset" / "item_repository.py").read_text(encoding="utf-8")
    admin_item_batch_repository = (PACKAGE / "features" / "admin_asset" / "item_batch_repository.py").read_text(encoding="utf-8")
    admin_level_repository = (PACKAGE / "features" / "admin_asset" / "level_repository.py").read_text(encoding="utf-8")
    admin_legacy_realm_adaptation_repository = (PACKAGE / "features" / "admin_asset" / "legacy_realm_adaptation_repository.py").read_text(encoding="utf-8")
    admin_novice_reset_repository = (PACKAGE / "features" / "admin_asset" / "novice_reset_repository.py").read_text(encoding="utf-8")
    admin_root_repository = (PACKAGE / "features" / "admin_asset" / "root_repository.py").read_text(encoding="utf-8")
    admin_impart_stone_repository = (PACKAGE / "features" / "admin_asset" / "impart_stone_repository.py").read_text(encoding="utf-8")
    admin_impart_stone_batch_repository = (PACKAGE / "features" / "admin_asset" / "impart_stone_batch_repository.py").read_text(encoding="utf-8")
    admin_accessory_repository = (PACKAGE / "features" / "admin_asset" / "accessory_repository.py").read_text(encoding="utf-8")
    admin_accessory_batch_repository = (PACKAGE / "features" / "admin_asset" / "accessory_batch_repository.py").read_text(encoding="utf-8")
    admin_asset_migrations = (PACKAGE / "features" / "admin_asset" / "migrations.py").read_text(encoding="utf-8")
    admin_asset_manifest = (PACKAGE / "features" / "admin_asset" / "manifest.py").read_text(encoding="utf-8")
    admin_accessory_adjustment = admin_asset_application[
        admin_asset_application.index("    def adjust_accessory(") : admin_asset_application.index("    def grant_item(")
    ]
    field_list_source = (PACKAGE / "xiuxian" / "xiuxian_utils" / "player_data_manager.py").read_text(encoding="utf-8")
    return {
        "interactive": {
            "static_commands_remain_message_only": (
                len(interactive_static_handlers) == 17
                and all(
                    "handle_send" in interactive_call_names(name)
                    and interactive_call_names(name) <= {"handle_send", "choice", "get_time_message", "items"}
                    for name in interactive_static_handlers
                )
            ),
            "effectful_commands_use_interactive_application": all(
                "check_user" in interactive_call_names(name)
                and "_run_interactive_action" in interactive_call_names(name)
                and interactive_delegates_to(name, action)
                for name, action in interactive_effect_handlers.items()
            ),
            "application_and_repository_own_reward_effects": all(
                token in interactive_application_source
                for token in (
                    "self.repository.settle_exp(",
                    "self.repository.settle_stone(",
                    "self.repository.claim_greeting(",
                    "self.ledger.begin(uow",
                    "self.ledger.finish(uow, outcome)",
                )
            ) and all(
                token in interactive_repository_source
                for token in (
                    "def settle_exp(",
                    "UPDATE user_xiuxian SET exp = ?",
                    "def settle_stone(",
                    "UPDATE user_xiuxian SET stone = ?",
                    "def claim_greeting(",
                    "interactive_greeting_claims",
                )
            ),
            "startup_migration_manifest_and_service_registered": (
                'ConfigSpec("interactive_enabled"' in interactive_manifest_source
                and 'commands_for("interactive")' in interactive_manifest_source
                and "def apply_interactive(" in interactive_migrations_source
                and 'Migration("interactive.001", "interactive_feature_migrations", apply_interactive)' in interactive_plugin_source
                and 'not context.settings.get("interactive_enabled", True)' in interactive_plugin_source
                and '"interactive": InteractiveApplication(str(context.database.path("game_db")))' in interactive_plugin_source
            ),
            "exp_application_replay_and_asset_effect_tested": (
                "def test_exp_reward_action_replays_and_commits_asset_once" in interactive_application_tests
            ),
            "legacy_fortune_suppression_has_migrated_matcher": (
                'daily = on_command(\n        "今日运势"' in interactive_command_adapter_source
                and "def _daily_handler(" in interactive_command_adapter_source
                and "handle_daily_fortune" in interactive_command_adapter_source
                and "今日运势 - 占卜每日运势" in interactive_help_source
            ),
            "status": "interactive_21_commands_17_message_compatibility_4_application_owned",
        },
        "field_list_cache": {
            "shared_entry_and_byte_budget": all(token in field_list_source for token in (
                "FIELD_LIST_CACHE_MAX_ENTRIES = 64",
                "FIELD_LIST_CACHE_MAX_BYTES = 8 * 1024 * 1024",
                "FIELD_LIST_CACHE_MAX_ENTRY_BYTES = 1024 * 1024",
                "self._field_list_cache_charged_bytes + charge > FIELD_LIST_CACHE_MAX_BYTES",
            )),
            "bounded_charge_before_and_after_copy": all(token in field_list_source for token in (
                "FIELD_LIST_CACHE_MAX_NODES = 8192",
                "FIELD_LIST_CACHE_MAX_DEPTH = 64",
                "_field_list_cache_charge(key, value)",
                "_field_list_cache_charge(key, cached)",
            )),
            "expiry_invalidation_and_lifecycle_release": all(token in field_list_source for token in (
                "def _expire_field_list_cache(",
                "not math.isfinite(ttl)",
                "self._drop_field_list_cache(key)",
                "def close(self):\n        with self._conn_lock:\n            self._clear_field_list_cache()",
                '"""恢复 player.db 后重建当前单例持有的连接。"""\n        with self._conn_lock:\n            self._clear_field_list_cache()',
            )),
            "status": "retained_cache_bounded_query_materialization_open",
        },
        "compensation": {
            "claim_schema_migration_owned": "def apply_compensation_reward_claim_schema" in compensation_migrations and "legacy.compensation.002" in compensation_legacy_migrated,
            "claim_request_path_has_no_ddl": "CREATE TABLE" not in compensation_repository and "ALTER TABLE" not in compensation_repository,
            "claim_schema_checked_read_only": "def _schema_ready" in compensation_repository and "read_only=True" in compensation_repository,
            "claim_delete_application_owned": "delete_reward_definition(" in compensation_common and "_reward_claim_service" not in compensation_common,
            "claim_delete_request_path_has_no_ddl": "def delete_claims(" in compensation_repository and "CREATE TABLE" not in compensation_repository and "ALTER TABLE" not in compensation_repository and '"schema_missing"' in compensation_repository,
            "claim_delete_is_atomic_with_definition": (
                "_compensation_application().delete_reward_definition(" in compensation_common
                and "_compensation_application().clear_reward_definitions(" in compensation_common
                and 'DELETE FROM reward_claims WHERE reward_type=? AND record_id=?' in compensation_reward_definition_repository
                and 'DELETE FROM reward_claim_counters WHERE reward_type=? AND record_id=?' in compensation_reward_definition_repository
            ),
            "claim_delete_admin_handlers_handle_failure": all(
                "if not result.succeeded:" in (PACKAGE / "xiuxian" / "xiuxian_compensation" / name).read_text(encoding="utf-8")
                and "_compensation_operation_id(event, \"delete\"" in (PACKAGE / "xiuxian" / "xiuxian_compensation" / name).read_text(encoding="utf-8")
                for name in ("gift_package.py", "redeem_code.py")
            ),
            "redeem_entry_handles_schema_missing": 'result.status == "schema_missing"' in compensation_redeem_code,
            "invitation_application_owned": "invitation_claim(" in compensation_invitation and "InvitationRewardClaimService" not in compensation_invitation,
            "invitation_request_path_has_no_ddl": "CREATE TABLE" not in compensation_invitation_repository and "ALTER TABLE" not in compensation_invitation_repository,
            "invitation_schema_migration_owned": "def apply_compensation_invitation_reward_schema" in compensation_migrations and "legacy.compensation.003" in compensation_legacy_migrated,
            "invitation_schema_checked": "def _schema_ready" in compensation_invitation_repository and "schema_missing" in compensation_invitation_repository and "legacy.compensation.invitation-json-v1" in compensation_invitation_repository,
            "invitation_snapshot_migration_owned": (
                "def apply_compensation_invitation_snapshot_migration" in compensation_migrations
                and "legacy.compensation.007" in compensation_legacy_migrated
                and "invitation_reward_migrations" in compensation_migrations
            ),
            "invitation_request_path_has_no_json": all(
                token not in compensation_invitation
                for token in (
                    "load_invitation_records",
                    "load_invitation_rewards",
                    "load_claimed_records",
                    "save_invitation_records",
                    "save_invitation_rewards",
                    "load_json_file",
                    "save_json_file",
                )
            ),
            "invitation_binding_application_owned": "invitation_bind(" in compensation_invitation and "add_invitation_record(inviter_id, user_id)" not in compensation_invitation,
            "invitation_binding_projection_owned": "def bind(" in compensation_invitation_repository and "invitation_bind(" in compensation_invitation,
            "invitation_definition_migration_owned": "def apply_compensation_invitation_definition_schema" in compensation_migrations and "legacy.compensation.004" in compensation_legacy_migrated,
            "invitation_definition_application_owned": "invitation_set_reward(" in compensation_invitation and "save_invitation_rewards(rewards)" not in compensation_invitation,
            "invitation_definition_schema_checked": "def _definition_schema_ready" in compensation_invitation_repository and '"status": "schema_missing"' in compensation_invitation_repository,
            "definition_schema_migration_owned": (
                "def apply_compensation_definition_schema" in compensation_migrations
                and "legacy.compensation.005" in compensation_legacy_migrated
            ),
            "definition_request_path_has_no_ddl": (
                "CREATE TABLE" not in compensation_definition_service
                and "ALTER TABLE" not in compensation_definition_service
                and "schema is missing" in compensation_definition_service
            ),
            "definition_legacy_migration_receipt_checked": (
                "compensation_legacy_migrations" in compensation_definition_service
                and "legacy-compensation-json-v1" in compensation_definition_service
            ),
            "reward_catalog_migration_owned": (
                "def apply_compensation_reward_catalog_schema" in compensation_migrations
                and "legacy.compensation.006" in compensation_legacy_migrated
            ),
            "reward_definition_runtime_sql_owned": (
                ".reward_definitions(config[\"type_key\"])" in compensation_common
                and ".reward_definition(" in compensation_common
                and ".reward_definition(" in compensation_redeem_code
                and ".upsert_reward_definition(" in compensation_common
                and ".delete_reward_definition(" in compensation_common
                and ".clear_reward_definitions(" in compensation_common
                and "gift and redeem definitions must be changed through CompensationApplication" in compensation_common
            ),
            "reward_definition_request_path_has_no_ddl": (
                "CREATE TABLE" not in compensation_reward_definition_repository
                and "ALTER TABLE" not in compensation_reward_definition_repository
                and "schema_missing" in compensation_reward_definition_repository
            ),
            "reward_runtime_does_not_write_json": (
                "save_json_file" not in compensation_common
                and "load_json_file" not in compensation_common
                and "save_claimed_data(config, claimed_data)" not in compensation_common
            ),
            "claim_precheck_uses_point_lookup": (
                "def has_claimed(user_id: str, item_id: str, config: Dict[str, Any])" in compensation_common
                and "return _compensation_application().has_claimed(" in compensation_common
                and "claimed_data = load_claimed_data(config)" not in compensation_common
            ),
            "item_catalog_construction_is_deferred": (
                "_item_catalog_instance = None" in compensation_common
                and "def _item_catalog(" in compensation_common
                and "items = Items()" not in compensation_common
            ),
            "redeem_claim_checks_definition_version": (
                "expected_definition_version=redeem_info.get(\"_definition_version\")" in compensation_redeem_code
            ),
            "reward_migration_reconciles_sql_claims": (
                "claim_count = int(" in compensation_migrations
                and "legacy_used_count - claim_count" in compensation_migrations
                and "DELETE FROM reward_claim_counters" in compensation_migrations
            ),
            "reward_web_counts_use_sql_aggregates": (
                "return get_claim_count(config, record_id)" in compensation_reward_center
                and "get_reward_used_count(" in compensation_reward_center
                and "load_claimed_data" not in compensation_reward_center
            ),
            "reward_web_definition_saves_use_sql": (
                "def api_save_reward_record" in compensation_reward_center
                and "upsert_reward_definition(" in compensation_reward_center
                and "expected_version" in compensation_reward_center
                and 'if kind == "compensation":' in compensation_reward_center
            ),
            "reward_web_delete_clear_report_sql_failures": (
                compensation_reward_center.count("if not result.succeeded:") >= 2
                and compensation_reward_center.count('request.headers.get("Idempotency-Key")') >= 2
            ),
            "reward_inventory_application_owned": "PlayerInventoryApplication" in compensation_common and "_inventory_application().grant_item(" in compensation_reward_writer,
            "reward_inventory_legacy_writer_disabled": ".send_back(" not in compensation_reward_writer,
            "status": "claim, invitation snapshots and reward definitions startup-migrated; requests fail closed without DDL or JSON fallback",
        },
        "stone_gift": {
            "default_legacy_handler_disabled": '"送灵石" if _legacy_stone_gift_enabled' in base,
            "nonebot_application_path": "_build_stone" in adapter and "application.read_limits" in adapter and "handle_stone_gift" in adapter,
            "web_application_path": "create_stone_gift_blueprint" in web and "application.transfer" in web,
            "old_service_removed": "class StoneGiftService" not in legacy_transaction and (PACKAGE / "compatibility" / "legacy_stone_gift.py").is_file(),
            "status": "cutover_with_compatibility_rollback_isolated",
        },
        "sign_in": {
            "default_legacy_handler_disabled": '"修仙签到" if _legacy_sign_in_enabled' in base,
            "nonebot_application_path": "_build_sign" in adapter and "application.read_limits" in adapter,
            "web_application_path": "create_sign_in_blueprint" in web and "application.claim" in web,
            "old_service_removed": "class SignInService" not in legacy_transaction and (PACKAGE / "compatibility" / "legacy_sign_in.py").is_file(),
            "lottery_service_isolated": "class LotterySettlementService" not in legacy_transaction and (PACKAGE / "compatibility" / "legacy_base_lottery.py").is_file(),
            "effects_application_owned": "SignInApplicationEffects" in sign_effects and "SignInApplicationEffects(" in plugin,
            "effects_outbox_reconcile_owned": '"sign_in.effects"' in plugin and "reconcile_outbox_event" in sign_application,
            "task_core_legacy": "SignInTaskEffects(record_task_progress)" in plugin,
            "lottery_core_default_legacy": "LotteryApplication(" not in plugin or "LotterySettlementService" in plugin,
            "lottery_compatibility_fallback": "XIUXIAN_SIGN_IN_LEGACY_LOTTERY" in plugin and "LotterySettlementService" in plugin,
            "lottery_scheduler_application_owned": "_lottery_application().snapshot(" in base and "lottery_settlement_service" not in base,
            "legacy_lottery_scheduler_disabled": "lottery_settlement_service" not in base,
            "daily_reset_application_owned": (
                '_run_job("每日修仙签到重置", _daily_sign_reset)' in mixelixir_scheduler
                and "_sql_message().sign_remake" not in mixelixir_scheduler
                and "SignInApplication(get_paths().game_db).reset_daily_flags(_scheduler_business_date())" in mixelixir_scheduler
                and "SignInDailyResetSqlRepository(" in sign_application
            ),
            "daily_reset_atomic_no_request_ddl": (
                "DatabaseUnitOfWork(self.database, immediate=True)" in sign_daily_reset_repository
                and "self.ledger.finish(uow, outcome)" in sign_daily_reset_repository
                and '"operation_ledger"' in sign_daily_reset_repository
                and '"operation_audit"' in sign_daily_reset_repository
                and '"is_sign"' in sign_daily_reset_repository
                and "CREATE TABLE" not in sign_daily_reset_repository
            ),
            "daily_reset_replay_rollback_and_missing_schema_covered": all(
                marker in sign_daily_reset_tests
                for marker in (
                    "same_day_replay_preserves_new_signs",
                    "daily_reset_conflict_and_missing_schema_fail_closed",
                    "rolls_back_flag_and_ledger_when_audit_write_fails",
                )
            ),
            "daily_reset_historical_date_ambiguity_documented": (
                "迁移前的 `is_sign=1` 没有业务日期回执" in sign_in_docs
                and "首次重置任务若延迟到用户已签到之后" in sign_in_docs
                and "scheduler timezone 由部署配置决定" in sign_in_docs
            ),
            "status": "cutover_with_compatibility_rollback_side_effects_retained",
        },
        "tasks": {
            "progress_application_owned": "TaskProgressApplication(get_paths().player_db)" in tasks_entry,
            "progress_repository_owned": "class TasksProgressRepository" in tasks_progress,
            "progress_request_path_has_no_ddl": "CREATE TABLE" not in tasks_progress and "ALTER TABLE" not in tasks_progress and "TaskProgressEventService(" not in tasks_entry,
            "status_read_application_owned": (
                "def read_states(" in tasks_claim_application
                and "return self.repository.read_states(user_id, periods)" in tasks_claim_application
            ),
            "status_commands_use_non_mutating_read": (
                "self.progress_application.read_states(" in tasks_entry
                and "self.progress_application.get_states(" not in tasks_entry
            ),
            "status_read_uses_read_only_uow": (
                "DatabaseUnitOfWork(self.database, read_only=True)" in tasks_read_states
                and "INSERT INTO" not in tasks_read_states
                and "UPDATE " not in tasks_read_states
            ),
            "status_read_missing_schema_fails_closed": (
                "except (FileNotFoundError, sqlite3.OperationalError) as exc:" in tasks_read_states
                and all(
                    marker in tasks_read_states
                    for marker in ("unable to open database", "no such table", "no such column")
                )
            ),
            "progress_migration_registered": 'Migration("tasks.001", "task_progress_schema", apply_task_progress)' in plugin and "def apply_task_progress(" in tasks_migrations,
            "claim_schema_migration_registered": 'Migration("tasks.002", "task_reward_claim_schema", apply_task_claim)' in plugin and "def apply_task_claim(" in tasks_migrations,
            "claim_recovery_migrations_registered": 'Migration("tasks.003", "task_reward_claim_recovery_schema", apply_task_claim_recovery)' in plugin and 'Migration("tasks.004", "task_reward_claim_player_schema", apply_task_claim_player)' in plugin,
            "claim_application_owned": "class TaskClaimApplication" in tasks_claim_application and "TaskClaimGameRepository" in tasks_claim_application and "TaskClaimPlayerRepository" in tasks_claim_application,
            "claim_default_entry_owned": "task_manager.claim_rewards(operation_id, user_id, cycle)" in tasks_command and "self.claim_application.claim_rewards(" in tasks_entry and "TaskRewardClaimService" not in tasks_entry,
            "claim_task_definition_compatibility": "def _claim_task_snapshots(" in tasks_entry and "self.items.get_data_by_item_id(item_id)" in tasks_entry,
            "claim_started_operation_recoverable": "def reconcile(" in tasks_claim_application and '"tasks.claim_rewards": context.services["task_claim"].reconcile' in plugin,
            "claim_request_path_has_no_ddl": "CREATE TABLE" not in tasks_claim and "ALTER TABLE" not in tasks_claim and "CREATE TABLE" not in tasks_claim_repository and "ALTER TABLE" not in tasks_claim_repository,
            "claim_request_path_avoids_attached_transaction": "ATTACH DATABASE" not in tasks_claim_application and "ATTACH DATABASE" not in tasks_claim_repository,
            "claim_schema_migrations_owned": "task_reward_claim_operations" in tasks_migrations and "sect_contribution_delta" in tasks_migrations,
            "reward_claim_legacy_boundary": "class TaskRewardClaimService" not in tasks_transaction and "class TaskRewardClaimService" in tasks_legacy_transactions and "from ...compatibility.legacy_task_transactions import" in tasks_transaction,
            "legacy_progress_implementation_retained": "class LegacyTaskProgressEventService" not in tasks_transaction and "class LegacyTaskProgressEventService" in tasks_legacy_transactions,
            "legacy_transaction_reexports_explicit": "from ...compatibility.legacy_task_transactions import" in tasks_transaction and "TaskProgressEventService = TasksProgressRepository" in tasks_transaction,
            "status": "daily_weekly_progress_and_claim_saga_cutover; task_definition_adapter_and_legacy_service_retained_for_compatibility",
        },
        "entertainment": {
            "account_delete_application_owned": "entertainment_application.delete_accounts(" in entertainment_facade and "_run_entertainment_write(" not in entertainment_facade[entertainment_facade.index("def delete_accounts("):entertainment_facade.index("def load_checkin_history(")],
            "account_bind_application_owned": "entertainment_application.bind_newapi_account(" in entertainment_facade,
            "auto_checkin_application_owned": "entertainment_application.toggle_auto_checkin(" in entertainment_facade,
            "checkin_history_application_owned": "entertainment_application.append_checkin_history(" in entertainment_facade and "entertainment_application.list_checkin_history(" in entertainment_facade,
            "newapi_store_has_no_runtime_json_owner": "json_store" not in entertainment_facade and "load_json_file" not in entertainment_facade and "save_json_file" not in entertainment_facade,
            "guess_sessions_application_owned": (
                "EntertainmentGuessSessionApplication" in entertainment_guess_application
                and "self.guess_sessions = EntertainmentGuessSessionApplication(" in entertainment_application
            ),
            "guess_sessions_repository_atomic": (
                "DatabaseUnitOfWork(self.database, immediate=True, timeout=0.5)" in entertainment_guess_repository
                and "session_token" in entertainment_guess_repository
                and "expires_at" in entertainment_guess_repository
            ),
            "guess_sessions_startup_migration_registered": (
                "def apply_entertainment_guess_sessions(" in entertainment_migrations
                and '("legacy.entertainment.004", apply_entertainment_guess_sessions)' in legacy_migrated
            ),
            "guess_handlers_use_feature_owner_off_event_loop": (
                "entertainment_application.guess_sessions" in entertainment_number
                and "entertainment_application.guess_sessions" in entertainment_puzzle
                and "guess_number_sessions" not in entertainment_number
                and "guess_puzzle_sessions" not in entertainment_puzzle
                and "run_blocking_io" in entertainment_number
                and "run_blocking_io" in entertainment_puzzle
            ),
            "status": "NewAPI and guess-session state are owned by Entertainment SQL repositories; legacy JSON is a one-time migration source",
        },
        "arena": {
            **_arena_owner_status(arena_owner_sources),
            "state_application_owned": "ArenaStateApplication" in arena_limit,
            "legacy_state_owner_disabled": "ArenaStateService" not in arena_limit,
            "legacy_transaction_service_isolated": all(f"class {name}" not in arena_transaction_service for name in ("ArenaStateService", "ArenaPurchaseService", "ArenaChallengePurchaseService", "ArenaChallengeSettlementService", "ArenaBattleSettlementService", "ArenaWeeklyRankReductionService", "ArenaSeasonRewardService")) and "from ...compatibility.legacy_arena_transactions import" in arena_transaction_service and all(f"class {name}" in arena_legacy_transaction_service for name in ("ArenaStateService", "ArenaPurchaseService", "ArenaChallengePurchaseService", "ArenaChallengeSettlementService", "ArenaBattleSettlementService", "ArenaWeeklyRankReductionService", "ArenaSeasonRewardService")),
            "legacy_transaction_imports_explicit": "legacy_arena_transactions" in arena and ".transaction_service import" not in arena and "legacy_arena_transactions" in arena_repository and "xiuxian_arena.transaction_service" not in arena_repository,
            "weekly_rank_application_owned": "arena_weekly_rank_application.reduce(" in arena,
            "legacy_scheduler_disabled": "ArenaWeeklyRankReductionService" not in arena and "_arena_weekly_rank_reduction_service" not in arena,
            "daily_reward_application_owned": "arena_season_reward_application.reset_daily()" in arena,
            "legacy_daily_reward_disabled": "ArenaSeasonRewardService" not in arena and "_arena_season_reward_service" not in arena,
            "status": "purchase_challenge_and_shared_opponent_owners_with_state_and_reward_cutovers_and_other_legacy_compatibility",
        },
        "tower": {
            "state_application_owned": "TowerStateApplication" in tower_limit,
            "legacy_state_owner_disabled": "TowerStateService" not in tower_limit,
            "global_reset_application_owned": (
                "state_application.reset_all_floors(" in tower_limit
                and "def reset_all_floors(" in tower_state_application
                and "await asyncio.to_thread(" in tower_reset_facade
            ),
            "global_reset_repository_atomic": (
                "DatabaseUnitOfWork(self.player_database, immediate=True)" in tower_state_repository
                and "UPDATE tower SET current_floor=0" in tower_state_repository
                and "INSERT INTO tower_state_operations" in tower_state_repository
            ),
            "global_reset_request_path_has_no_ddl": (
                "CREATE TABLE" not in tower_state_repository
                and "ALTER TABLE" not in tower_state_repository
                and "update_all_records" not in tower_limit
            ),
            "global_reset_replay_protected": (
                "tower_state_operations " in tower_state_repository
                and '"WHERE operation_id=?"' in tower_state_repository
                and '"duplicate"' in tower_state_repository
            ),
            "global_reset_schema_migration_owned": (
                "tower_state_operations" in tower_migrations
                and 'Migration("tower.004", "tower_state_operations", apply_tower_state)' in plugin
            ),
            "global_reset_scheduler_uses_stable_week_id": (
                'f"tower-reset:weekly:{period_key}"' in tower_reset_scheduler
                and "business_date = _scheduler_business_date()" in tower_reset_scheduler
            ),
            "global_reset_admin_uses_event_id": (
                'f"tower-reset:admin:{operator_id}:{message_id}"' in tower_reset_admin
                and "if not result.succeeded:" in tower_reset_admin
            ),
            "ranking_application_owned": (
                "tower_limit.ranking(\"current_floor\")" in tower_facade
                and "tower_limit.ranking(\"score\")" in tower_facade
                and "def ranking(" in tower_state_application
                and "def ranking(" in tower_state_repository
            ),
            "ranking_query_bounded_and_stable": (
                'field not in {"current_floor", "score"}' in tower_state_repository
                and "limit = min(50, max(0, int(limit)))" in tower_state_repository
                and 'ORDER BY value DESC,user_id ASC LIMIT ?' in tower_state_repository
            ),
            "ranking_legacy_full_scan_removed": (
                "get_all_field_data" not in tower_facade
                and "_player_data_manager" not in tower_limit
            ),
            "stamina_refund_application_owned": (
                "restore_player_stamina(" in tower_facade
                and "_sql_message().update_user_stamina(" not in tower_facade
                and "def restore(" in base_stamina_application
                and "def restore(" in base_stamina_repository
            ),
            "status": "state_and_global_floor_reset_cutover_with_legacy_service_removed_from_default_path",
        },
        "training": {
            "state_application_owned": "TrainingStateApplication" in training_limit,
            "legacy_state_owner_disabled": "TrainingStateService" not in training_limit,
            "event_application_owned": "TrainingEventSqlRepository" in training_repository and "run_event" in training_repository,
            "event_default_entry_owned": "training_application.run_event(" in training_facade and "TrainingApplication(get_paths().game_db, get_paths().player_db)" in training_facade,
            "event_repository_atomic": "AttachedDatabaseUnitOfWork" in training_event_repository and "player_data" in training_event_repository,
            "event_request_path_has_no_ddl": "CREATE TABLE" not in training_event_repository and "ALTER TABLE" not in training_event_repository,
            "event_plan_is_frozen_before_settlement": (
                "def freeze_plan(" in training_event_repository
                and "def apply_frozen(" in training_event_repository
                and "training_event_resolutions" in training_event_repository
                and "DELETE FROM training_event_resolutions" in training_event_repository
                and "self.repository.run_event(operation_id, user_id, create_plan)" in training_application
            ),
            "event_context_is_read_only": (
                "DatabaseUnitOfWork(self.game_database, read_only=True)" in training_event_context_repository
                and all(token not in training_event_context_repository for token in ("CREATE TABLE", "INSERT INTO", "UPDATE ", "DELETE FROM", "UserBuffDate", "XiuxianDateManage"))
                and "def read_event_context(" in training_repository
            ),
            "event_resolver_has_no_legacy_database_reads": (
                "from ...features.training.event_resolver import TrainingEvents" in training_events
                and "class TrainingEvents" in training_event_resolver
                and "XiuxianDateManage" not in training_event_resolver
                and "UserBuffDate" not in training_event_resolver
                and "get_back_msg" not in training_event_resolver
                and "get_top_users_by_level" not in training_event_resolver
            ),
            "event_migrations_registered": (
                'Migration("training.001", "training_event_operations", apply_training_event_operations)' in training_plugin
                and 'Migration("training.002", "training_event_player_schema", apply_training_event_player)' in training_plugin
                and 'Migration("training.005", "training_event_resolutions", apply_training_event_resolutions)' in training_plugin
                and '"training.005"' not in training_plugin[training_plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):training_plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")]
                and '"training.005"' not in training_plugin[training_plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):training_plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")]
                and "def apply_training_event_operations(" in training_migrations
                and "def apply_training_event_player(" in training_migrations
                and "def apply_training_event_resolutions(" in training_migrations
            ),
            "purchase_application_owned": "TrainingPurchaseSqlRepository" in training_repository and "purchase" in training_repository,
            "purchase_default_entry_owned": "training_application.execute(" in training_facade and '"purchase"' in training_facade[training_facade.index("def _run_training_action"):],
            "purchase_repository_atomic": "AttachedDatabaseUnitOfWork" in training_purchase_repository and "player_data" in training_purchase_repository,
            "purchase_request_path_has_no_ddl": "CREATE TABLE" not in training_purchase_repository and "ALTER TABLE" not in training_purchase_repository,
            "purchase_migrations_registered": (
                'Migration("training.003", "training_purchase_operations", apply_training_purchase_operations)' in plugin
                and "def apply_training_purchase_operations(" in training_migrations
            ),
            "reset_application_owned": "TrainingResetSqlRepository" in training_repository and "reset_limits" in training_application,
            "reset_default_entry_owned": "training_application.reset_limits(" in training_facade,
            "reset_repository_atomic": "AttachedDatabaseUnitOfWork" in training_reset_repository and "player_data" in training_reset_repository,
            "reset_request_path_has_no_ddl": "CREATE TABLE" not in training_reset_repository and "ALTER TABLE" not in training_reset_repository,
            "reset_clock_injected": "clock=self.clock" in training_repository and "self.clock.now()" in training_reset_repository,
            "reset_migrations_registered": (
                'Migration("training.004", "training_reset_operations", apply_training_reset_operations)' in plugin
                and "def apply_training_reset_operations(" in training_migrations
            ),
            "admin_reset_uses_resumable_feature_application": (
                "run_chunked_until_done(" in admin_training_reset_handler
                and "training_reset_limits(operation_id, operator_id)" in admin_training_reset_handler
                and "training_application.reset_limits(" in training_facade
                and "TrainingResetSqlRepository" in training_repository
            ),
            "leaderboard_is_bounded_and_cached": (
                "limit = max(1, min(int(limit), 50))" in training_leaderboard_repository
                and "ORDER BY CAST(COALESCE(t." in training_leaderboard_repository
                and "LIMIT ?" in training_leaderboard_repository
                and "cache_ttl: float = 45.0" in training_leaderboard_repository
                and "training_application.leaderboard(field, limit=50)" in training_facade
            ),
            "leaderboard_indexes_registered_on_player_db": (
                'Migration("training.006", "training_leaderboard_indexes", apply_training_leaderboard_indexes)' in training_plugin
                and '"training.006"' in training_plugin[training_plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):training_plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")]
                and '"training.006"' in training_plugin[training_plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):training_plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")]
                and "training_completed_rank_idx" in training_migrations
                and "training_points_rank_idx" in training_migrations
            ),
            "state_reads_use_training_application": "training_application.get_state(user_id)" in training_facade and "get_user_training_info(user_id)" not in training_facade,
            "item_catalog_is_lazy_and_shared": "_items_instance = None" in training_facade and "def _items(" in training_facade and "items=_items()" in training_facade and "items = Items()" not in training_facade,
            "purchase_reset_compatibility_retained": "training_purchase_service" in training_repository and "training_reset_service" in training_repository,
            "status": "event_resolution_freeze_bounded_leaderboard_state_purchase_and_reset_feature_owned",
        },
        "work": {
            "daily_refresh_application_owned": "work_daily_refresh_application.reset(" in work_facade,
            "legacy_daily_refresh_disabled": "_work_daily_refresh_reset_service" not in work_facade,
            "admin_global_reset_application_owned": (
                "asyncio.to_thread(" in admin_work_reset_handler
                and "work_admin_refresh_reset_application.reset_all," in admin_work_reset_handler
                and "_sql_message().reset_work_num(" not in admin_work_reset_handler
            ),
            "admin_global_reset_reuses_platform_ledger": (
                "class WorkAdminRefreshResetSqlRepository" in work_admin_reset_repository
                and "OperationLedger" in work_admin_reset_repository
                and "operation_audit" in work_admin_reset_repository
            ),
            "admin_global_reset_atomic_without_ddl_or_user_cache": (
                "DatabaseUnitOfWork(self.database, immediate=True)" in work_admin_reset_repository
                and "UPDATE user_xiuxian SET work_num=? WHERE work_num IS NOT ?" in work_admin_reset_repository
                and "CREATE TABLE" not in work_admin_reset_repository
                and "ALTER TABLE" not in work_admin_reset_repository
                and "SELECT user_id" not in work_admin_reset_repository
            ),
            "admin_global_reset_replay_conflict_and_rollback_covered": (
                "operation_conflict" in work_admin_reset_repository
                and "fail_work_reset_audit" in work_admin_reset_tests
                and "schema_missing" in work_admin_reset_tests
            ),
            "item_accelerate_application_owned": "work_item_use_application.accelerate(" in work_accelerate_handler,
            "legacy_item_accelerate_disabled": "_work_item_use_service().accelerate(" not in work_accelerate_handler,
            "capture_application_owned": "work_item_use_application.capture(" in work_capture_handler,
            "legacy_item_capture_disabled": "_work_item_use_service().capture(" not in work_capture_handler,
            "offer_projection_adapter_wired_to_each_application": (
                "legacy_work_offer_json = LegacyWorkOfferJsonAdapter()" in work_facade
                and "def project(self, user_id: str" in work_legacy_offer_adapter
                and "legacy_projection_writer=legacy_work_offer_json.project" in work_claim_projection_wiring
                and "legacy_projection_writer=legacy_work_offer_json.project" in work_refresh_projection_wiring
                and "legacy_projection_writer=legacy_work_offer_json.project" in work_status_projection_wiring
                and "legacy_projection_writer=legacy_work_offer_json.project" in work_item_use_projection_wiring
            ),
            "status_application_owns_offer_projection": (
                "self.repository.mark_offer_expired(" in work_status_application_source
                and "self._project(user_id, offer)" in work_status_application_source
            ),
            "refresh_application_owns_offer_projection": (
                'if result.status == "applied" and isinstance(result.offer, dict):' in work_refresh_application_source
                and "self._project_legacy(request.user_id, result.offer)" in work_refresh_application_source
            ),
            "claim_application_owns_offer_projection": (
                'if status == "applied" and self.legacy_projection_writer is not None:' in work_claim_application_source
                and "self.legacy_projection_writer(" in work_claim_application_source
            ),
            "capture_json_projection_application_owned": (
                "savef(" not in work_capture_handler
                and 'if result.status in {"applied", "duplicate"}' in work_item_use_application_source
                and "self._project_legacy(user_id, offer)" in work_item_use_application_source
            ),
            "offer_projection_applications_own_legacy_json": (
                "legacy_work_offer_json = LegacyWorkOfferJsonAdapter()" in work_facade
                and "legacy_projection_writer=legacy_work_offer_json.project" in work_claim_projection_wiring
                and "legacy_projection_writer=legacy_work_offer_json.project" in work_refresh_projection_wiring
                and "legacy_projection_writer=legacy_work_offer_json.project" in work_status_projection_wiring
                and "legacy_projection_writer=legacy_work_offer_json.project" in work_item_use_projection_wiring
                and "self._project(user_id, offer)" in work_status_application_source
                and "self._project_legacy(request.user_id, result.offer)" in work_refresh_application_source
                and "self.legacy_projection_writer(" in work_claim_application_source
                and "self._project_legacy(user_id, offer)" in work_item_use_application_source
                and "savef(" not in work_capture_handler
            ),
            "offer_generation_reuses_profile_snapshot": "workmake(level, exp, level," in work_handle_source,
            "offer_generation_has_no_legacy_handle_import": "xiuxian2_handle" not in work_handle_source and "xiuxian2_handle" not in workmake_source,
            "unused_item_cache_not_constructed": "items = Items()" not in work_handle_source and "from ..xiuxian_utils.item_json import Items" not in work_handle_source,
            "refresh_application_owned": "work_refresh_application.refresh(" in work_facade and "work_refresh_application.get_result(" in work_facade,
            "legacy_refresh_default_path_disabled": "WorkRefreshSettlementService" not in work_facade and "_work_refresh_service(" not in work_facade,
            "refresh_repository_has_no_request_ddl": "CREATE TABLE" not in work_refresh_repository and "ALTER TABLE" not in work_refresh_repository,
            "offer_projection_has_no_request_ddl": "CREATE TABLE" not in work_reward_source and "ALTER TABLE" not in work_reward_source,
            "refresh_migration_registered": (
                'Migration("work.005", "work_refresh_operations", apply_work_refresh_operations)' in plugin
                and "def apply_work_refresh_operations(" in work_migrations
            ),
            "legacy_refresh_isolated_with_compatibility_export": (
                "class WorkRefreshSettlementService" in work_legacy_refresh
                and "class WorkRefreshSettlementService" not in work_transaction_shim
                and "legacy_work_refresh import" in work_transaction_shim
            ),
            "abort_cleanup_application_owned": work_facade.count("work_abort_cleanup_application.cleanup(") == 3,
            "legacy_abort_cleanup_default_path_disabled": (
                "WorkAbortCleanupService" not in work_facade and "_work_abort_cleanup_service" not in work_facade
            ),
            "abort_cleanup_repository_has_no_request_ddl": "CREATE TABLE" not in work_abort_repository and "ALTER TABLE" not in work_abort_repository,
            "abort_cleanup_migration_registered": (
                'Migration("work.006", "work_abort_cleanup_operations", apply_work_abort_cleanup)' in plugin
                and "def apply_work_abort_cleanup(" in work_migrations
            ),
            "legacy_abort_cleanup_isolated_with_compatibility_export": (
                "class WorkAbortCleanupService" in work_abort_legacy
                and "class WorkAbortCleanupService" not in work_transaction_shim
                and "legacy_work_abort_cleanup import" in work_transaction_shim
            ),
            "claim_default_entry_owned": "work_claim_application.claim(" in work_facade,
            "claim_runtime_default_has_no_legacy_repository": "LegacyWorkClaimRepository" not in work_plugin_source,
            "claim_repository_has_no_request_ddl": "CREATE TABLE" not in work_claim_repository and "ALTER TABLE" not in work_claim_repository,
            "claim_migration_registered": (
                'Migration("work.007", "work_claim_operations", apply_work_claim_operations)' in plugin
                and "def apply_work_claim_operations(" in work_migrations
            ),
            "settlement_repository_has_no_request_ddl": "CREATE TABLE" not in work_settlement_repository and "ALTER TABLE" not in work_settlement_repository,
            "settlement_migration_registered": (
                'Migration("work.008", "work_settlement_operations", apply_work_settlement_operations)' in plugin
                and "def apply_work_settlement_operations(" in work_migrations
            ),
            "settlement_application_owned": "work_settlement_application.settle(" in work_facade,
            "settlement_transaction_owned": (
                "class WorkSettlementSqlRepository" in work_settlement_repository
                and "with DatabaseUnitOfWork(self.database, immediate=True)" in work_settlement_repository
                and "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL" in work_settlement_repository
            ),
            "legacy_settlement_isolated_with_compatibility_export": (
                "class WorkSettlementService" in work_legacy_settlement
                and "class WorkSettlementService" not in work_transaction_shim
                and "legacy_work_settlement import" in work_transaction_shim
            ),
            "legacy_item_use_isolated_with_compatibility_export": (
                "class WorkItemUseService" in work_legacy_item_use
                and "class WorkItemUseService" not in work_transaction_shim
                and "legacy_work_item_use import" in work_transaction_shim
            ),
            "legacy_daily_refresh_isolated_with_compatibility_export": (
                "class WorkDailyRefreshResetService" in work_legacy_daily_refresh
                and "class WorkDailyRefreshResetService" not in work_transaction_shim
                and "legacy_work_daily_refresh_reset import" in work_transaction_shim
            ),
            "item_use_migrations_registered": (
                'Migration("work.003", "work_item_use_operations", apply_work_item_use)' in plugin
                and 'Migration("work.004", "work_offer_snapshots", apply_work_offer_snapshots)' in plugin
            ),
            "status": "daily_refresh_accelerate_capture_offer_refresh_abort_cleanup_claim_settlement_item_use_and_daily_reset_compatibility_isolation",
        },
        "activity_reward": {
            "claim_all_application_owned": "activity_claim_all_application.run(" in activity_service,
            "legacy_claim_all_disabled": "_activity_claim_all_service().run(" not in activity_service,
            "claim_all_startup_schema_owned": (
                "CREATE TABLE" not in activity_claim_repository
                and "_assert_schema_ready(uow)" in activity_claim_repository
                and 'Migration("activity_reward.003", "activity_claim_all_legacy_receipts"' in plugin
            ),
            "claim_all_legacy_receipts_imported": (
                "apply_activity_claim_all_legacy_receipts" in plugin
                and "with DatabaseUnitOfWork(legacy_database, read_only=True)" in
                (PACKAGE / "features" / "activity_reward" / "migrations.py").read_text(encoding="utf-8")
            ),
            "claim_all_game_db_default": "activity_claim_all_application = ActivityClaimAllApplication(get_paths().game_db)" in activity_service,
            "claim_all_web_default_owned": (
                "ActivityClaimAllApplication(self.database).run(" in activity_reward_application
                and "repository=LegacyActivityRewardRepository" not in plugin
            ),
            "claim_all_command_default_owned": (
                "ActivityClaimAllApplication(self.database).run(" in activity_command_repository
                and "service import claim_activity_rewards" not in activity_command_repository
            ),
            "tasks_game_reward_application_owned": (
                "_activity_task_claim_application().claim(" in activity_service
                and "ActivityTaskClaimRepository" in activity_task_claim_repository
                and "ATTACH DATABASE" not in activity_task_claim_compatibility
            ),
            "tasks_started_operation_recoverable": (
                "def prepare_operation(" in activity_task_claim_repository
                and "self.repository.prepare_operation(" in activity_task_claim_application
                and '"activity_reward.tasks.claim": context.services["activity_task_claim"].reconcile' in plugin
            ),
            "tasks_startup_schema_and_receipt_backfill": (
                'Migration("activity_reward.004", "activity_task_reward_claims"' in plugin
                and 'Migration("activity_reward.005", "activity_task_reward_legacy_receipts"' in plugin
                and "apply_activity_task_claim_legacy_receipts" in activity_reward_migrations
            ),
            "tasks_legacy_state_projection_retryable": (
                "_finalize_legacy_state" in activity_task_claim_repository
                and "activity_task_reward_claim_reservations" in activity_task_claim_repository
            ),
            "task_claim_matcher_operation_id_owned": (
                '"claim_activity_tasks"' in activity_command_repository
                and 'str(kwargs.get("operation_id", ""))' in activity_command_repository[
                    activity_command_repository.index("    def _claim_tasks("):
                    activity_command_repository.index("    def _claim_pass(")
                ]
                and "_activity_task_claim_application().claim(" in activity_service
                and "operation_id or f\"activity-task:" in activity_service
            ),
            "pass_game_reward_application_owned": (
                "claim_application = _activity_pass_claim_application()" in activity_service
                and "claim_application.claim(" in activity_service
                and "ActivityPassClaimRepository" in activity_pass_claim_repository
                and "ATTACH DATABASE" not in activity_pass_claim_compatibility
            ),
            "pass_started_operation_recoverable": (
                "def prepare_operation(" in activity_pass_claim_repository
                and "self.repository.prepare_operation(" in activity_pass_claim_application
                and '"activity_reward.pass.claim": context.services["activity_pass_claim"].reconcile' in plugin
                and '"activity_reward.pass.claim": activity_pass_claim.reconcile' in activity_cli
                and "claim_application.resume_pending(operation_id, uid)" in activity_service
            ),
            "pass_startup_schema_and_receipt_backfill": (
                'Migration("activity_reward.006", "activity_pass_reward_claims"' in plugin
                and 'Migration("activity_reward.007", "activity_pass_reward_legacy_receipts"' in plugin
                and "apply_activity_pass_claim_legacy_receipts" in activity_reward_migrations
            ),
            "pass_legacy_state_projection_retryable": (
                "_finalize_legacy_state" in activity_pass_claim_repository
                and "activity_pass_reward_claim_reservations" in activity_pass_claim_repository
            ),
            "pass_compatibility_facade_isolated": (
                "ActivityPassClaimApplication" in activity_pass_claim_compatibility
                and "ATTACH DATABASE" not in activity_pass_claim_compatibility
                and "CREATE TABLE" not in activity_pass_claim_compatibility
            ),
            "pass_claim_all_child_operation_id_stable": (
                '"pass": lambda child_id: claim_activity_pass_rewards(uid, operation_id=child_id)' in activity_claim_runners
            ),
            "pass_claim_matcher_operation_id_owned": (
                '"claim_activity_pass_rewards"' in activity_command_repository
                and 'str(kwargs.get("operation_id", ""))' in activity_command_repository[
                    activity_command_repository.index("    def _claim_pass("):
                    activity_command_repository.index("    def _claim_sign(")
                ]
                and "claim_application.claim(" in activity_service
                and "operation_id or f\"activity-pass:" in activity_service
            ),
            "boss_milestone_game_reward_application_owned": (
                "application.claim(" in activity_boss_milestone_claim_entry
                and "ActivityBossMilestoneClaimRepository" in activity_boss_milestone_claim_repository
                and "ATTACH DATABASE" not in activity_boss_milestone_claim_entry
            ),
            "boss_milestone_started_operation_recoverable": (
                "def prepare_operation(" in activity_boss_milestone_claim_repository
                and "self.repository.prepare_operation(" in activity_boss_milestone_claim_application
                and '"activity_reward.boss_milestone.claim": context.services["activity_boss_milestone_claim"].reconcile' in plugin
                and '"activity_reward.boss_milestone.claim": activity_boss_milestone_claim.reconcile' in activity_cli
                and "application.resume_pending(operation_id, user_id)" in activity_boss_milestone_claim_entry
            ),
            "boss_milestone_startup_schema_and_receipt_backfill": (
                'Migration("activity_reward.008", "activity_boss_milestone_reward_claims"' in plugin
                and 'Migration("activity_reward.009", "activity_boss_milestone_legacy_receipts"' in plugin
                and "apply_activity_boss_milestone_legacy_receipts" in activity_reward_migrations
            ),
            "boss_milestone_legacy_state_projection_retryable": (
                "_finalize_legacy_state" in activity_boss_milestone_claim_repository
                and "activity_boss_milestone_claim_reservations" in activity_boss_milestone_claim_repository
            ),
            "boss_milestone_compatibility_facade_isolated": (
                "self._get_milestone_application().claim(" in boss_milestone_claim_compatibility
                and "ATTACH DATABASE" not in boss_milestone_claim_compatibility
                and "CREATE TABLE" not in boss_milestone_claim_compatibility
            ),
            "boss_milestone_claim_all_child_operation_id_stable": (
                '"boss_milestone": lambda child_id: claim_boss_milestone_reward(uid, operation_id=child_id)' in activity_claim_runners
            ),
            "boss_claim_matcher_operation_id_owned": (
                '"activity_boss.claim_boss_rewards"' in activity_command_repository
                and 'str(kwargs.get("operation_id", ""))' in activity_command_repository[
                    activity_command_repository.index("    def _claim_boss("):
                    activity_command_repository.index("    def _fight_boss(")
                ]
                and "operation_id: str | None = None" in activity_boss_claim_router
                and 'f"{operation_id}:milestone"' in activity_boss_claim_router
                and 'f"{operation_id}:rank"' in activity_boss_claim_router
            ),
            "boss_rank_game_reward_application_owned": (
                "application.claim(" in activity_boss_rank_claim_entry
                and "ActivityBossRankClaimRepository" in activity_boss_rank_claim_application
                and "ATTACH DATABASE" not in activity_boss_rank_claim_repository
            ),
            "boss_rank_started_operation_recoverable": (
                "def prepare_operation(" in activity_boss_rank_claim_repository
                and "self.repository.prepare_operation(" in activity_boss_rank_claim_application
                and '"activity_reward.boss_rank.claim": context.services["activity_boss_rank_claim"].reconcile' in plugin
                and '"activity_reward.boss_rank.claim": activity_boss_rank_claim.reconcile' in activity_cli
                and "application.resume_pending(operation_id, user_id)" in activity_boss_rank_claim_entry
            ),
            "boss_rank_startup_schema_and_receipt_backfill": (
                'Migration("activity_reward.010", "activity_boss_rank_reward_claims"' in plugin
                and 'Migration("activity_reward.011", "activity_boss_rank_legacy_receipts"' in plugin
                and "apply_activity_boss_rank_legacy_receipts" in activity_reward_migrations
            ),
            "boss_rank_legacy_state_projection_retryable": (
                "_finalize_legacy_state" in activity_boss_rank_claim_repository
                and "activity_boss_rank_claim_reservations" in activity_boss_rank_claim_repository
                and "status='granted'" in activity_boss_rank_claim_repository
            ),
            "boss_rank_compatibility_facade_isolated": (
                "self._get_rank_application().claim(" in boss_rank_claim_compatibility
                and "ATTACH DATABASE" not in boss_rank_claim_compatibility
                and "CREATE TABLE" not in boss_rank_claim_compatibility
            ),
            "boss_rank_claim_all_child_operation_id_stable": (
                '"boss_rank": lambda child_id: claim_boss_rank_reward(uid, operation_id=child_id)' in activity_claim_runners
            ),
            "state_migrations_registered_game_only": (
                'Migration("activity_state.001", "activity_state_schema", apply_activity_state_schema)' in plugin
                and 'Migration("activity_state.002", "activity_state_legacy_backfill", apply_activity_state_legacy)' in plugin
                and "def apply_activity_state_schema(" in activity_state_migrations
                and "def apply_activity_state_legacy(" in activity_state_migrations
            ),
            "state_legacy_backfill_read_only_bounded_conflict_checked": (
                "with DatabaseUnitOfWork(legacy_database, read_only=True)" in activity_state_migrations
                and "ORDER BY rowid LIMIT 200" in activity_state_migrations
                and "activity state migration conflict" in activity_state_migrations
                and "legacy activity schema incomplete" in activity_state_migrations
            ),
            "state_migration_disk_preflight": (
                "shutil.disk_usage(uow.database.parent).free" in activity_state_migrations
                and "source_size * 2 + 128 * 1024 * 1024" in activity_state_migrations
            ),
            "state_default_storage_is_game_db": (
                "DB_PATH = get_paths().game_db" in activity_storage
                and "LEGACY_DB_PATH = BASE_DIR / \"activity.db\"" in activity_storage
                and "CREATE TABLE" not in activity_storage[activity_storage.index("def init_db(conn=None):"):activity_storage.index("def resolve_daohao(")]
            ),
            "state_transaction_services_use_migrated_schema": all(
                table in activity_transaction_service
                for table in (
                    "activity_boss_settlement_operations",
                    "activity_sign_settlement_operations",
                    "activity_point_purchase_operations",
                    "activity_collect_exchange_operations",
                )
            ) and "activity_state.001 schema_missing" in activity_transaction_service,
            "state_default_reward_and_boss_paths_use_game_db": (
                "ActivityTaskClaimApplication(\n            paths.game_db, paths.game_db" in activity_service
                and "ActivityPassClaimApplication(\n            paths.game_db, paths.game_db" in activity_service
                and "ActivityBossMilestoneClaimApplication(\n            paths.game_db, paths.game_db" in activity_boss_source
                and "activity_database=get_paths().game_db" in activity_boss_entry
                and "activity_table_prefix = \"\"" in activity_boss_transaction_service
            ),
            "state_web_management_targets_game_db_and_keeps_config_separate": (
                "ACTIVITY_DB = DATABASE" in activity_web_core
                and "ACTIVITY_CONFIG_DB = get_paths().data / \"activity\" / \"activity.db\"" in activity_web_core
                and "_get_dynamic_tables(ACTIVITY_DB, \"activity_\"" in activity_web_core
                and "_get_dynamic_tables(ACTIVITY_CONFIG_DB, \"activity_config_\"" in activity_web_core
                and "get_dynamic_activity_config_tables()" in activity_web_database
            ),
            "state_legacy_projection_cutover_complete_but_source_retained": (
                '"activity_state.002"' in plugin
                and "legacy_database = uow.database.parent / \"activity\" / \"activity.db\"" in activity_state_migrations
                and "LEGACY_DB_PATH" in activity_storage
            ),
            "status": "claim_all_and_reward_ledgers_cut_over; activity_gameplay_state_migrated_to_game_db; legacy_activity_file_retained_for_config_events_and_backup",
        },
        "activity_read_model": {
            "task_and_pass_matchers_use_feature_application": (
                "activity_application.read_model.task_progress_text(" in activity_commands
                and "activity_application.read_model.task_catalog_text()" in activity_commands
                and "activity_application.read_model.pass_text(" in activity_commands
                and "build_activity_task_progress_text(" not in activity_commands
                and "build_activity_pass_text(" not in activity_commands
            ),
            "activity_application_composes_read_model": (
                "self.read_model = ActivityReadModelApplication(database)" in activity_application
            ),
            "read_model_repository_is_read_only_and_has_no_ddl": (
                "DatabaseUnitOfWork(self.database, read_only=True)" in activity_read_model_repository
                and "CREATE TABLE" not in activity_read_model_repository
                and "UPDATE " not in activity_read_model_repository
                and "INSERT INTO " not in activity_read_model_repository
            ),
            "read_model_rendering_matches_legacy_contract": (
                "test_read_model_application_preserves_task_and_pass_output" in activity_read_model_tests
            ),
            "sign_rank_matcher_uses_read_model": (
                "activity_application.read_model.sign_rank_text(10)" in activity_commands
                and "build_rank_text" not in activity_commands
                and "def sign_rank_text(" in activity_read_model_application
            ),
            "sign_rank_is_bounded_join_and_read_only": (
                "LEFT JOIN user_xiuxian" in activity_read_model_repository
                and "ORDER BY activity_user.sign_days DESC" in activity_read_model_repository
                and "LIMIT ?" in activity_read_model_repository
                and "test_sign_rank_uses_one_read_only_join_and_preserves_display" in activity_read_model_tests
            ),
            "collect_bag_matcher_uses_read_model": (
                "activity_application.read_model.collect_bag_text(" in activity_commands
                and "build_collect_bag_text(" not in activity_commands
                and "def collect_bag_text(" in activity_read_model_application
                and "def collect_state(" in activity_read_model_repository
                and "test_collect_bag_read_model_matches_legacy_text" in activity_read_model_tests
            ),
            "collect_bag_uses_bounded_read_only_snapshot": (
                "DatabaseUnitOfWork(self.database, read_only=True)" in activity_read_model_repository
                and "activity_collect_inventory" in activity_read_model_repository
                and "activity_collect_claim" in activity_read_model_repository
                and "activity_collect_pity_state" in activity_read_model_repository
                and "test_collect_bag_read_model_does_not_create_missing_database" in activity_read_model_tests
            ),
            "status": "activity_task_pass_sign_rank_and_collect_bag_read_models_owned_by_feature_application",
        },
        "activity_config_owner": {
            "activity_overview_reward_gameplay_matchers_use_feature_read_model": (
                "activity_application.read_model.activity_info_text(" in activity_commands
                and "activity_application.read_model.rewards_text()" in activity_commands
                and "activity_application.read_model.gameplay_text()" in activity_commands
                and "build_activity_info(" not in activity_commands
                and "build_activity_rewards_text(" not in activity_commands
                and "build_activity_gameplay_text(" not in activity_commands
            ),
            "activity_overview_uses_one_read_only_game_snapshot": (
                "def overview_snapshot(" in activity_read_model_repository
                and "DatabaseUnitOfWork(self.database, read_only=True)" in activity_read_model_repository
                and "def test_overview_uses_one_read_only_snapshot_without_writes" in activity_read_model_tests
                and "def test_overview_does_not_create_missing_database" in activity_read_model_tests
                and "def test_overview_rewards_and_gameplay_match_legacy_text" in activity_read_model_tests
            ),
            "config_toggle_matchers_use_separate_config_application": (
                activity_commands.count("activity_application.config.set_enabled(") == 2
                and "self.config = ActivityConfigApplication()" in activity_application
                and "ActivityConfigApplication" in activity_application
                and "service.set_enabled(" not in activity_commands
            ),
            "config_owner_uses_activity_event_store_and_repairs_projection_replay": (
                "get_paths().data / \"activity\" / \"activity.db\"" in activity_config_repository
                and "ActivityConfigEventService(self.database)" in activity_config_repository
                and "self.event_service.replace(" in activity_config_repository
                and "self._write_projection(result.config)" in activity_config_repository
                and "DatabaseUnitOfWork(self._database, read_only=True)" in activity_config_event_service
                and "def test_duplicate_replay_repairs_projection_after_commit_failure" in activity_config_tests
            ),
            "config_owner_target_replay_conflict_and_revision_contracts_tested": (
                "def test_enabled_targets_preserve_legacy_mapping_and_only_write_config_store" in activity_config_tests
                and "def test_conflict_and_stale_revision_do_not_write_projection" in activity_config_tests
                and "def test_replay_lookup_does_not_create_missing_database_or_schema" in activity_config_event_tests
            ),
            "status": "activity_overview_rewards_gameplay_and_config_toggles_owned_by_feature_applications",
        },
        "activity_web_config_owner": {
            "management_and_config_routes_use_feature_config_application": (
                "def _activity_config_application():" in activity_web
                and "ActivityApplication(get_paths().game_db)" in activity_web
                and "_activity_application_instance.config" in activity_web
                and activity_web.count("_activity_config_application().read()") == 2
                and "_activity_config_application().replace(" in activity_web
                and "save_activity_config" not in activity_web
                and "def read(self) -> ActivityConfigState:" in activity_config_application
                and "def replace(" in activity_config_application
            ),
            "configuration_http_contracts_are_exercised_through_flask_client": (
                "def test_web_config_and_management_page_read_through_application" in activity_config_event_tests
                and "def test_web_post_passes_operation_revision_and_operator" in activity_config_event_tests
                and "def test_web_config_post_requires_csrf_before_application" in activity_config_event_tests
                and "def test_read_is_non_mutating_and_replace_preserves_web_revision_contract" in activity_config_tests
            ),
            "static_template_routes_are_explicit_read_only_compatibility": (
                "ACTIVITY_TEMPLATE_DEFINITIONS.get(str(template_key))" in activity_web
                and "GAMEPLAY_TEMPLATE_DEFINITIONS.get(str(template_key))" in activity_web
                and "def test_static_template_routes_remain_read_only" in activity_config_event_tests
            ),
            "status": "activity_management_and_configuration_routes_use_the_config_application",
        },
        "activity_web_admin_data_owner": {
            "data_routes_use_feature_owned_admin_data_application": (
                "def _activity_admin_data_application():" in activity_web
                and "_activity_application_instance.admin_data" in activity_web
                and "self.admin_data = ActivityAdminDataApplication(database)" in activity_application
                and "_activity_admin_data_application().overview(" in activity_web
                and "_activity_admin_data_application().reset(" in activity_web
                and "_activity_admin_data_application().adjust(" in activity_web
                and all(
                    legacy not in activity_web
                    for legacy in (
                        "get_activity_data_overview(",
                        "reset_activity_data(",
                        "adjust_activity_points(",
                        "adjust_collect_word(",
                        "adjust_activity_pass_exp(",
                    )
                )
            ),
            "admin_data_uses_read_only_snapshot_and_atomic_existing_schema_writes": (
                "DatabaseUnitOfWork(self.database, read_only=True)" in activity_admin_data_repository
                and "DatabaseUnitOfWork(self.database, immediate=True)" in activity_admin_data_repository
                and "CREATE TABLE" not in activity_admin_data_repository
                and "activity_state.001 schema_missing" in activity_admin_data_repository
                and "activity_state.003 schema_missing" in activity_admin_data_repository
                and "def overview_snapshot(" in activity_admin_data_repository
            ),
            "admin_data_http_and_legacy_response_contracts_are_tested": (
                "def test_overview_matches_legacy_http_projection" in activity_admin_data_tests
                and "def test_overview_uses_one_read_only_snapshot_and_does_not_create_database" in activity_admin_data_tests
                and "def test_reset_is_atomic_and_preserves_each_scope" in activity_admin_data_tests
                and "def test_adjustments_preserve_clamping_and_validation" in activity_admin_data_tests
                and "def test_web_routes_use_application_and_keep_csrf_boundary" in activity_admin_data_tests
            ),
            "status": "activity_admin_overview_reset_and_adjust_use_one_feature_owned_game_db_boundary",
        },
        "web_pages_presentation_owner": {
            "page_handlers_remain_session_template_or_static_adapters": (
                "return render_template('home.html', admin_id=session['admin_id'])" in web_pages
                and "return render_template('login.html', error=\"无效的管理员 ID\"), 401" in web_pages
                and "session['_csrf_token'] = secrets.token_urlsafe(32)" in web_pages
                and "def logout():\n    session.clear()" in web_pages
                and "return render_template('update.html')" in web_pages
                and "return (\"\", 204)" in web_pages
                and "Disallow: /" in web_pages
            ),
            "page_permissions_and_global_csrf_remain_declared": (
                '"login": WebPermission.PUBLIC' in web_pages_access
                and '"home": WebPermission.READ' in web_pages_access
                and '"logout": WebPermission.READ' in web_pages_access
                and '"update": WebPermission.UPDATE' in web_pages_access
                and "def _validate_csrf_token():" in (PACKAGE / "xiuxian" / "xiuxian_web" / "core.py").read_text(encoding="utf-8")
            ),
            "browser_session_and_static_http_contracts_are_tested": (
                "def test_login_accepts_configured_superuser" in web_pages_tests
                and "def test_login_requires_csrf_token" in web_pages_tests
                and "def test_home_and_update_pages_require_admin_session" in web_pages_tests
                and "def test_public_static_page_routes_keep_fixed_responses" in web_pages_tests
                and "def test_logout_clears_admin_session" in web_pages_tests
            ),
            "status": "web_pages_session_templates_and_static_responses_remain_explicit_compatibility",
        },
        "updater_owner": {
            "status_commands_use_updater_application": (
                "UpdateManager, UpdateApplication" in updater_status
                and "UpdateApplication(update_manager).latest_releases" in updater_status
                and "UpdateApplication(update_manager).check_update" in updater_status
                and "lambda: UpdateApplication(update_manager).perform_update_with_backup(release_tag)" in updater_status
            ),
            "web_update_routes_use_updater_application": (
                "update_application = UpdateApplication(update_manager)" in updater_web_core
                and "update_application" in web_pages
                and "update_application.check_update()" in web_pages
                and "update_application.latest_releases(10)" in web_pages
                and "update_application.perform_update_with_backup(release_tag)" in web_pages
                and "update_manager.perform_update_with_backup(" not in web_pages
            ),
            "application_preflights_asset_and_orders_backups_before_update": (
                "prepare_release_asset(release_tag)" in updater_application
                and "UPDATE_ASSET_NAME" in updater_application
                and "self._provider.enhanced_backup_current_version" in updater_application
                and "self._provider.backup_db_files" in updater_application
                and "self._provider.backup_all_configs" in updater_application
                and "release_tag=release_tag" in updater_application
            ),
            "remote_release_metadata_is_rendered_as_text": (
                "changelog.textContent = text" in updater_page_template
                and "heading.append(document.createTextNode(`${release.name} `))" in updater_page_template
                and "button.addEventListener('click', () => performUpdate(release.tag_name))" in updater_page_template
                and "showChangelog('${data.latest_version}'" not in updater_page_template
                and "onclick=\"performUpdate('${release.tag_name}')\"" not in updater_page_template
            ),
            "updater_application_manager_and_http_contracts_are_tested": (
                "def test_backups_run_in_order_and_stop_at_first_failure" in updater_application_tests
                and "def test_update_passes_verified_asset_and_exact_tag_and_cleans_archive" in updater_application_tests
                and "def test_release_preflight_requires_requested_official_asset" in updater_manager_tests
                and "def test_failed_download_removes_temporary_directory" in updater_manager_tests
                and "def test_invalid_archive_does_not_write_version_or_create_target" in updater_manager_tests
                and "def test_update_routes_require_admin_and_keep_update_permission" in updater_web_tests
            ),
            "status": "updater_commands_and_web_api_use_one_validated_application_boundary",
        },
        "plugin_backup_catalog_owner": {
            "legacy_route_uses_catalog_application": (
                "backup_catalog_application.list_plugin_backups()" in web_pages
                and "update_manager.get_backups()" not in web_pages
            ),
            "catalog_preserves_list_contract_without_path_disclosure": (
                '"filename"' in plugin_backup_repository
                and '"version"' in plugin_backup_repository
                and '"created_at"' in plugin_backup_repository
                and '"size"' in plugin_backup_repository
                and '"path"' not in plugin_backup_repository
                and "entry.stat(follow_symlinks=False)" in plugin_backup_repository
                and "stat.S_ISREG(metadata.st_mode)" in plugin_backup_repository
            ),
            "catalog_is_read_only_and_handles_missing_entries": (
                "except FileNotFoundError" in plugin_backup_repository
                and "except OSError" in plugin_backup_repository
                and "mkdir" not in plugin_backup_repository
            ),
            "catalog_application_and_http_contracts_are_tested": (
                "def test_application_delegates_to_catalog_repository" in plugin_backup_tests
                and "def test_repository_skips_symlinks_and_missing_directory_without_creating_it" in plugin_backup_tests
                and "def test_get_backups_route_requires_admin_and_keeps_legacy_payload" in updater_web_tests
            ),
            "status": "plugin_backup_listing_has_a_read_only_feature_owner",
        },
        "plugin_backup_local_file_owner": {
            "legacy_file_routes_use_feature_application": (
                "plugin_backup_file_application.open_plugin_backup(filename)" in backup_routes
                and "plugin_backup_file_application.delete_plugin_backup(str(backup_filename))" in backup_routes
                and "plugin_backup_file_application.delete_plugin_backups(filenames)" in backup_routes
            ),
            "file_operations_share_catalog_filename_policy_and_reject_symlinks": (
                "is_plugin_backup_filename(filename)" in plugin_backup_file_repository
                and "is_plugin_backup_filename(entry.name)" in plugin_backup_repository
                and "path.lstat()" in plugin_backup_file_repository
                and "O_NOFOLLOW" in plugin_backup_file_repository
            ),
            "batch_delete_preserves_partial_result_contract": (
                "def delete_plugin_backups" in plugin_backup_file_application
                and "{\"filename\": filename, \"reason\": \"文件不存在\"}" in plugin_backup_file_application
                and "return deleted, failed" in plugin_backup_file_application
            ),
            "file_repository_and_http_contracts_are_tested": (
                "def test_repository_rejects_symlinks_without_following_or_deleting_target" in plugin_backup_file_tests
                and "def test_repository_does_not_create_missing_backup_directory" in plugin_backup_file_tests
                and "def test_application_batch_delete_reports_partial_results" in plugin_backup_file_tests
                and "def test_plugin_backup_file_routes_keep_auth_csrf_and_response_contracts" in plugin_backup_web_tests
                and "def test_plugin_backup_download_rejects_invalid_and_missing_archives" in plugin_backup_web_tests
            ),
            "status": "local_plugin_backup_list_download_and_delete_share_a_feature_file_boundary",
        },
        "plugin_backup_restore_owner": {
            "both_legacy_restore_routes_share_feature_application": (
                "plugin_backup_restore_application.restore_backup(filename)" in backup_routes
                and "plugin_backup_restore_application.restore_backup(backup_filename)" in backup_routes
                and "update_manager.restore_backup(" not in backup_routes
            ),
            "application_owns_restore_order_and_runtime_boundary": (
                "with self._repository.stage_backup(filename, database_names) as staged:" in plugin_backup_restore_application
                and "self._repository.merge_data(" in plugin_backup_restore_application
                and "self._repository.merge_plugin(" in plugin_backup_restore_application
                and "self._runtime.after_plugin_backup_restore(restored_databases)" in plugin_backup_restore_application
                and "self._repository.write_version(self._version_file" in plugin_backup_restore_application
            ),
            "repository_bounds_and_validates_zip_before_overlay": (
                _schema_constant_bound(plugin_backup_schemas, plugin_backup_restore_repository, "MAX_ARCHIVE_MEMBERS", 100_000)
                and "shutil.disk_usage(directory)" in plugin_backup_restore_repository
                and "_validate_disk_capacity" in plugin_backup_restore_repository
                and "O_NOFOLLOW" in plugin_backup_restore_repository
                and "_safe_member_name" in plugin_backup_restore_repository
                and "_validate_member_type" in plugin_backup_restore_repository
                and "_ensure_target_directory" in plugin_backup_restore_repository
                and "os.replace(temporary_path" in plugin_backup_restore_repository
                and "self._data_root" in plugin_backup_restore_repository
            ),
            "restore_behavior_and_failure_boundaries_are_tested": (
                "def test_restore_overlays_configured_roots_and_reloads_restored_databases" in plugin_backup_restore_tests
                and "def test_invalid_member_path_is_rejected_before_any_target_is_written" in plugin_backup_restore_tests
                and "def test_member_count_limit_is_checked_before_extracting" in plugin_backup_restore_tests
                and "def test_existing_symlink_in_target_tree_is_rejected" in plugin_backup_restore_tests
                and "def test_restore_failure_does_not_update_version" in plugin_backup_restore_tests
                and "def test_plugin_backup_restore_routes_keep_admin_csrf_and_local_contract" in plugin_backup_web_tests
                and "def test_cloud_restore_reuses_local_archive_and_keeps_error_contract" in plugin_backup_web_tests
                and "def test_cloud_restore_downloads_only_when_local_archive_is_absent" in plugin_backup_web_tests
                and "def test_cloud_restore_does_not_restore_after_download_failure" in plugin_backup_web_tests
            ),
            "status": "local_and_cloud_plugin_zip_restore_share_a_validated_feature_owner",
        },
        "plugin_backup_cloud_owner": {
            "legacy_cloud_routes_use_feature_application": (
                "plugin_backup_cloud_application.list_cloud_backups()" in backup_routes
                and "plugin_backup_cloud_application.sync_cloud_backup(" in backup_routes
                and "plugin_backup_cloud_application.sync_cloud_backups(" in backup_routes
                and "plugin_backup_cloud_application.delete_cloud_backups(" in backup_routes
                and "update_manager.list_webdav_backups()" not in backup_routes
                and "update_manager.download_from_webdav(" not in backup_routes
                and "update_manager.delete_webdav_backup(" not in backup_routes
            ),
            "cloud_restore_fetch_and_restore_share_feature_owners": (
                "plugin_backup_cloud_application.local_backup_exists(filename)" in backup_routes
                and "plugin_backup_cloud_application.sync_cloud_backup(" in backup_routes
                and "plugin_backup_restore_application.restore_backup(filename)" in backup_routes
            ),
            "application_bounds_batches_and_preserves_partial_results": (
                _schema_constant_bound(plugin_backup_schemas, plugin_backup_cloud_application, "MAX_CLOUD_BACKUP_BATCH", 100)
                and "def sync_cloud_backups(" in plugin_backup_cloud_application
                and "def delete_cloud_backups(" in plugin_backup_cloud_application
                and "def local_backup_exists(" in plugin_backup_cloud_application
                and "return synced, exists, failed" in plugin_backup_cloud_application
                and "return deleted, failed" in plugin_backup_cloud_application
            ),
            "repository_bounds_webdav_and_atomically_installs_archives": (
                _schema_constant_bound(plugin_backup_schemas, plugin_backup_cloud_repository, "MAX_CLOUD_LIST_BYTES", 2 * 1024 * 1024)
                and _schema_constant_bound(plugin_backup_schemas, plugin_backup_cloud_repository, "MAX_CLOUD_LIST_ENTRIES", 1_000)
                and _schema_constant_bound(plugin_backup_schemas, plugin_backup_cloud_repository, "MAX_PLUGIN_BACKUP_DOWNLOAD_BYTES", 4 * 1024 * 1024 * 1024)
                and "self._read_response(response, MAX_CLOUD_LIST_BYTES)" in plugin_backup_cloud_repository
                and "zipfile.is_zipfile(temporary_path)" in plugin_backup_cloud_repository
                and "os.replace(temporary_path, target_path)" in plugin_backup_cloud_repository
                and "os.link(temporary_path, target_path)" in plugin_backup_cloud_repository
            ),
            "manager_compatibility_methods_and_cloud_contracts_are_tested": (
                "return build_plugin_backup_cloud_application(self).list_cloud_backups()" in plugin_backup_manager
                and "def test_plugin_backup_cloud_compatibility_methods_delegate_to_feature_owner" in updater_manager_tests
                and "def test_list_cloud_backups_rejects_oversized_and_entity_xml" in plugin_backup_cloud_tests
                and "def test_download_writes_valid_zip_atomically_and_closes_response" in plugin_backup_cloud_tests
                and "def test_bad_download_preserves_existing_file" in plugin_backup_cloud_tests
                and "def test_batch_sync_and_delete_preserve_partial_results_and_bound_work" in plugin_backup_cloud_tests
                and "def test_cloud_plugin_backup_routes_keep_admin_csrf_and_batch_contracts" in plugin_backup_web_tests
            ),
            "status": "plugin_backup_cloud_listing_sync_delete_and_restore_fetch_share_a_bounded_owner",
        },
        "database_backup_owner": {
            "database_routes_use_feature_application": (
                "database_backup_application.create_backup()" in backup_routes
                and "database_backup_application.list_local_backups()" in backup_routes
                and "database_backup_application.restore_local_backup(" in backup_routes
                and "database_backup_application.list_cloud_backups()" in backup_routes
                and "database_backup_application.sync_cloud_backup(" in backup_routes
                and "database_backup_application.restore_cloud_backup(" in backup_routes
                and "database_backup_application.delete_local_backups(filenames)" in backup_routes
                and "database_backup_application.sync_cloud_backups(" in backup_routes
                and "database_backup_application.delete_cloud_backups(filenames)" in backup_routes
                and "update_manager.backup_db_files()" not in backup_routes
                and "update_manager.restore_db_files(" not in backup_routes
                and "update_manager.download_db_backup_from_webdav(" not in backup_routes
            ),
            "manager_methods_are_compatibility_forwarders": (
                "return self._database_backup_application().create_backup()" in plugin_backup_manager
                and "return self._database_backup_application().list_local_backups()" in plugin_backup_manager
                and "return self._database_backup_application().restore_local_backup(" in plugin_backup_manager
                and "return self._database_backup_application().list_cloud_backups()" in plugin_backup_manager
                and "return self._database_backup_application().sync_cloud_backup(" in plugin_backup_manager
                and "return self._database_backup_application().restore_cloud_backup(" in plugin_backup_manager
                and "return self._database_backup_application().delete_cloud_backup(filename)" in plugin_backup_manager
            ),
            "repository_bounds_zip_restore_and_cloud_io": (
                _schema_constant_bound(database_backup_schemas, database_backup_application, "MAX_DATABASE_BACKUP_BATCH", 100)
                and _schema_constant_bound(database_backup_schemas, database_backup_repository, "MAX_DATABASE_RESTORE_MEMBERS", 256)
                and _schema_constant_bound(database_backup_schemas, database_backup_repository, "MAX_DATABASE_RESTORE_BYTES", 16 * 1024 * 1024 * 1024)
                and "O_NOFOLLOW" in database_backup_repository
                and "database_backup_validate_sqlite" in database_backup_repository
                and "staged[database] = source_path" in database_backup_repository
                and "self._backup_directory: payload_size," in database_backup_repository
                and "zipfile.is_zipfile(temporary_path)" in database_backup_repository
                and "os.replace(temporary_path, target_path)" in database_backup_repository
                and "self._read_response(response, MAX_DATABASE_BACKUP_CLOUD_LIST_BYTES)" in database_backup_repository
                and "PartialDatabaseRestoreError" in database_backup_repository
            ),
            "restore_cloud_fallback_batch_bounds_and_behavior_tested": (
                "self._repository.download_cloud_backup(\n            filename, overwrite=True" in database_backup_application
                and "self._repository.local_backup_exists(filename)" in database_backup_application
                and "test_cloud_restore_keeps_local_fallback_after_download_failure" in database_backup_tests
                and "test_batch_delete_bounds_work_and_rejects_other_zip_files" in database_backup_tests
                and "test_failed_restore_reconnects_attempted_databases_once" in database_backup_tests
                and "test_restore_uses_snapshot_recovery_only_after_read_only_check_fails" in database_backup_tests
                and "assert runtime.snapshot_calls == 0" in database_backup_tests
                and "test_database_backup_compatibility_methods_delegate_to_feature_owner" in database_backup_adapter_tests
            ),
            "database_routes_keep_admin_csrf_and_legacy_contracts_tested": (
                "def test_database_backup_routes_share_feature_application_and_http_contract" in updater_web_tests
                and "missing_csrf.status_code, 403" in updater_web_tests
                and "anonymous.status_code, 401" in updater_web_tests
            ),
            "status": "database_zip_local_restore_and_webdav_operations_have_one_feature_owner",
        },
        "config_backup_owner": {
            "config_routes_use_feature_application": (
                "config_backup_application.backup_cloud_config()" in backup_routes
                and "config_backup_application.list_cloud_backups()" in backup_routes
                and "config_backup_application.sync_cloud_backup(" in backup_routes
                and "config_backup_application.restore_cloud_backup(" in backup_routes
                and "config_backup_application.export_config(" in backup_routes
                and "config_backup_application.import_config(" in backup_routes
                and "config_backup_application.create_local_backup(" in backup_routes
                and "config_backup_application.list_local_backups()" in backup_routes
                and "config_backup_application.restore_local_backup(" in backup_routes
                and "config_backup_application.delete_local_backup(" in backup_routes
            ),
            "manager_methods_are_compatibility_forwarders": (
                "return self._config_backup_application().create_cloud_backup(local_file_path)" in plugin_backup_manager
                and "return self._config_backup_application().list_cloud_backups()" in plugin_backup_manager
                and "return self._config_backup_application().sync_cloud_backup(" in plugin_backup_manager
                and "return self._config_backup_application().restore_cloud_backup(filename)" in plugin_backup_manager
                and "return self._config_backup_application().backup_all_configs()" in plugin_backup_manager
                and "return self._config_backup_application().restore_config_from_backup(backup_path)" in plugin_backup_manager
                and "def test_config_backup_compatibility_methods_delegate_to_feature_owner" in database_backup_adapter_tests
            ),
            "repository_bounds_json_and_cloud_io": (
                _schema_constant_bound(config_backup_schemas, config_backup_repository, "MAX_CONFIG_BACKUP_BYTES", 16 * 1024 * 1024)
                and _schema_constant_bound(config_backup_schemas, config_backup_repository, "MAX_CONFIG_CLOUD_LIST_BYTES", 2 * 1024 * 1024)
                and _schema_constant_bound(config_backup_schemas, config_backup_repository, "MAX_CONFIG_CLOUD_LIST_ENTRIES", 1_000)
                and "O_NOFOLLOW" in config_backup_repository
                and "os.replace(temporary_path, target)" in config_backup_repository
                and "os.link(temporary_path, target)" in config_backup_repository
            ),
            "import_restore_and_manual_cloud_behavior_are_tested": (
                "test_import_and_restore_routes_only_stage_values_without_writing_config" in config_backup_tests
                and "test_manual_cloud_backup_uploads_only_once_when_auto_cloud_is_enabled" in config_backup_tests
                and "test_cloud_restore_prefers_local_backup_and_only_fetches_when_missing" in config_backup_tests
                and "test_cloud_listing_rejects_entity_xml_and_closes_response" in config_backup_tests
                and "test_cloud_download_installs_json_atomically_and_preserves_existing_on_error" in config_backup_tests
            ),
            "routes_keep_admin_csrf_and_legacy_contracts_tested": (
                "def test_config_backup_routes_share_feature_application_and_http_contract" in plugin_backup_web_tests
                and "all(response.status_code == 403 for response in missing_csrf)" in plugin_backup_web_tests
                and "anonymous_list.status_code, 401" in plugin_backup_web_tests
            ),
            "status": "configuration_json_backup_import_restore_and_webdav_share_a_bounded_feature_owner",
        },
        "plugin_backup_creation_owner": {
            "manager_compatibility_entrypoint_delegates_to_feature_owner": (
                "return build_plugin_backup_creation_application(self).create_backup()" in plugin_backup_manager
                and "def test_plugin_backup_creation_compatibility_method_delegates_to_feature_owner" in updater_manager_tests
            ),
            "archive_creation_preserves_exclusions_and_installs_atomically": (
                "_SKIP_DIRECTORY_NAMES" in plugin_backup_creation_repository
                and "directories[:] = kept_directories" in plugin_backup_creation_repository
                and "_is_transient_data_file" in plugin_backup_creation_repository
                and "os.replace(temporary_path, target)" in plugin_backup_creation_repository
                and "def test_archive_keeps_legacy_paths_and_skips_transient_and_cache_files" in plugin_backup_creation_tests
                and "def test_archive_failure_preserves_existing_archive_and_removes_temporary_file" in plugin_backup_creation_tests
            ),
            "cloud_and_local_retention_failures_do_not_invalidate_local_archive": (
                "defer_cloud_cleanup" in plugin_backup_creation_application
                and "cleanup_local_backups" in plugin_backup_creation_application
                and "def test_creation_upload_and_cleanup_failures_do_not_fail_local_backup" in plugin_backup_creation_tests
                and "def test_cleanup_uses_backup_timestamp_and_never_follows_symlinks" in plugin_backup_creation_tests
            ),
            "status": "plugin_zip_creation_and_legacy_adapter_have_a_feature_owner",
        },
        "manual_backup_owner": {
            "legacy_route_uses_cross_type_application": (
                "manual_backup_application.create_backup()" in backup_routes
                and "update_manager.enhanced_backup_current_version()" not in backup_routes
                and "update_manager.backup_all_configs()" not in backup_routes
            ),
            "application_runs_both_types_and_cleans_shared_cloud_once": (
                manual_backup_application.index("self._plugin_backup.create_backup_with_details(")
                < manual_backup_application.index("self._config_backup.backup_all_configs_with_details(")
                and "if plugin.cloud_uploaded or config_cloud_uploaded" in manual_backup_application
                and "test_plugin_failure_still_runs_config_backup" in manual_backup_tests
                and "manual_backup_is_serial_and_cleans_shared_cloud_once" in manual_backup_tests
            ),
            "legacy_page_is_preserved_as_a_login_gated_template_compatibility_route": (
                "@app.route('/backups')" in backup_routes
                and "return render_template('backups.html')" in backup_routes
                and "数据库备份 / 恢复" in backup_page_template
                and "插件备份 / 恢复" in backup_page_template
                and "def test_backups_page_keeps_database_and_plugin_management_behind_login" in plugin_backup_web_tests
            ),
            "manual_route_auth_csrf_and_partial_result_contracts_are_tested": (
                "def test_manual_backup_route_keeps_auth_csrf_and_partial_result_contract" in plugin_backup_web_tests
                and "missing_csrf.status_code, 403" in plugin_backup_web_tests
                and "manual_backup_application" in backup_routes
            ),
            "status": "manual_plugin_and_config_backup_orchestration_is_feature_owned",
        },
        "activity_static_help": {
            "help_and_manage_remain_static_compatibility_handlers": (
                "send_help_message(" in activity_help_handler
                and "send_help_message(" in activity_manage_handler
                and "Cooldown(cd_time=0)" in activity_help_handler
                and "Cooldown(cd_time=0)" in activity_manage_handler
                and "activity_application." not in activity_help_handler
                and "activity_application." not in activity_manage_handler
            ),
            "status": "static_activity_help_and_manage_handlers_are_explicit_compatibility_paths",
        },
        "activity_boss": {
            "boss_status_matcher_uses_feature_read_model": (
                "activity_application.read_model.boss_status_text(" in activity_commands
                and "build_boss_status_text" not in activity_commands
                and "def boss_status_text(" in activity_read_model_application
                and "def boss_snapshot(" in activity_read_model_repository
            ),
            "boss_rank_matcher_uses_feature_read_model": (
                "activity_application.read_model.boss_rank_text(" in activity_commands
                and "build_boss_rank_text" not in activity_commands
                and "def boss_rank_text(" in activity_read_model_application
                and "def boss_rank(" in activity_read_model_repository
            ),
            "boss_read_repository_is_read_only_bounded_no_hp_init": (
                "DatabaseUnitOfWork(self.database, read_only=True)" in activity_read_model_repository
                and "activity_boss_state" in activity_read_model_repository
                and "LIMIT ?" in activity_read_model_repository
                and "CREATE TABLE" not in activity_read_model_repository
                and "UPDATE " not in activity_read_model_repository
                and "INSERT INTO " not in activity_read_model_repository
                and "test_boss_status_is_read_only_and_does_not_initialize_hp" in activity_boss_feature_tests
                and "test_boss_rank_uses_one_batched_display_name_lookup" in activity_boss_feature_tests
            ),
            "boss_attack_application_owned": (
                "ActivityBossSettlementSqlRepository(database)" in activity_command_repository
                and "_run_activity_action(" in activity_commands
                and "def _settle_boss(" in activity_command_repository
                and "from ...xiuxian.xiuxian_activity.activity_boss import fight_cooperative_boss" not in activity_command_repository
                and "from ...xiuxian.xiuxian_activity.activity_boss import use_item_on_boss" not in activity_command_repository
            ),
            "boss_settlement_atomic_replay_conflict_cas_and_rollback": (
                "DatabaseUnitOfWork(self.database, immediate=True)" in activity_boss_settlement_repository
                and "activity_boss_settlement_operations" in activity_boss_settlement_repository
                and "operation_payload_matches" in activity_boss_settlement_repository
                and "WHERE activity_key=? AND hp_left=? AND max_hp=?" in activity_boss_settlement_repository
                and "test_cooperative_settlement_is_atomic_and_replayable" in activity_boss_feature_tests
                and "test_item_settlement_rolls_back_inventory_and_boss_state_on_receipt_failure" in activity_boss_feature_tests
            ),
            "boss_settlement_uses_existing_state_schema": (
                "CREATE TABLE" not in activity_boss_settlement_repository
                and "activity_state.001 schema_missing" in activity_boss_settlement_repository
                and "activity_boss_settlement_operations" in activity_state_migrations
            ),
            "boss_operation_id_and_item_damage_stable": (
                "hashlib.sha256(operation_id.encode())" in activity_command_repository
                and "operation_id=operation_id" in activity_command_repository
            ),
            "boss_legacy_settlement_default_path_isolated": (
                "from ...xiuxian.xiuxian_activity.activity_boss import fight_cooperative_boss" not in activity_command_repository
                and "from ...xiuxian.xiuxian_activity.activity_boss import use_item_on_boss" not in activity_command_repository
                and "ActivityBossCoopSettlementService" in activity_transaction_service
            ),
            "status": "activity boss status/rank reads and cooperative/item settlement owned by feature repositories",
        },
        "activity_sign_in": {
            "default_matcher_uses_feature_owned_settlement_repository": (
                "settlement_repository=self.sign_settlement" in activity_command_repository
                and "ActivitySignSettlementSqlRepository(database)" in activity_command_repository
                and "ActivitySignSettlementSqlRepository" in activity_sign_repository
                and "DatabaseUnitOfWork(self.database, immediate=True)" in activity_sign_repository
            ),
            "settlement_uses_existing_startup_schema_and_atomic_receipt": (
                "activity_sign_settlement_operations" in activity_sign_repository
                and "activity_sign_log" in activity_sign_repository
                and "CREATE TABLE" not in activity_sign_repository
                and "activity_sign_settlement_operations" in activity_state_migrations
                and "test_settlement_and_receipt_are_atomic_and_replay_once" in activity_sign_repository_tests
            ),
            "sign_gameplay_event_replay_is_stable_and_retryable": (
                "event_id=f\"activity-sign:{user_id}:{sign_date}\"" in activity_service
                and "settlement_repository is not None" in activity_service
                and "test_sign_projection_failure_replays_with_a_stable_event_receipt" in activity_application_tests
            ),
            "status": "sign_settlement_owned_by_activity_feature_repository_with_replayable_event_projection",
        },
        "activity_collect_words": {
            "default_matchers_use_feature_read_and_settlement_repositories": (
                "activity_application.read_model.collect_bag_text(" in activity_commands
                and '"claim_collect_phrase",' in activity_commands
                and '_activity_operation_id(event, "collect-exchange", user_id)' in activity_commands
                and "ActivityCollectExchangeSqlRepository(database)" in activity_command_repository
                and "settlement_repository=self.collect_exchange" in activity_command_repository
                and "settlement_repository=None" in activity_service
                and "ActivityCollectExchangeSqlRepository" in activity_collect_repository
            ),
            "exchange_is_atomic_receipted_and_reuses_startup_schema": (
                "DatabaseUnitOfWork(self.database, immediate=True)" in activity_collect_repository
                and "activity_collect_exchange_operations" in activity_collect_repository
                and "CREATE TABLE" not in activity_collect_repository
                and "activity_collect_exchange_operations" in activity_state_migrations
                and "test_exchange_and_business_receipt_are_atomic_and_replay_once" in activity_collect_tests
                and "test_receipt_failure_rolls_back_tokens_and_assets" in activity_collect_tests
            ),
            "started_exchange_retries_only_with_business_receipt_boundary": (
                "retry_started_actions = frozenset({\"claim_collect_phrase\"})" in activity_command_repository
                and "retry_started_actions" in migrated_application
                and "def lookup_receipt(" in activity_collect_repository
                and "lookup_receipt(operation_id, uid)" in activity_service
                and "test_started_recovery_precedes_changed_activity_configuration" in activity_collect_tests
                and "test_started_retry_without_business_receipt_runs_current_request" in activity_collect_tests
                and "test_stale_started_ledger_resumes_against_existing_exchange_receipt" in activity_collect_tests
                and "test_backfilled_legacy_receipt_replays_without_duplicate_award" in activity_collect_tests
            ),
            "status": "collect_bag_and_exchange_owned_by_activity_feature_read_and_settlement_repositories",
        },
        "activity_points_shop": {
            "default_read_matchers_use_feature_read_model": (
                "activity_application.read_model.points_text(" in activity_commands
                and "activity_application.read_model.point_shop_text(" in activity_commands
                and "def points_text(" in activity_read_model_application
                and "def point_shop_text(" in activity_read_model_application
                and "def point_balances(" in activity_read_model_repository
                and "def point_shop_state(" in activity_read_model_repository
                and "test_point_and_shop_read_models_preserve_text_with_bounded_reads" in activity_read_model_tests
                and "test_point_shop_read_models_do_not_create_missing_database" in activity_read_model_tests
            ),
            "default_purchase_uses_feature_owned_repository": (
                "ActivityPointShopPurchaseSqlRepository(database)" in activity_command_repository
                and "settlement_repository=self.point_shop_purchase" in activity_command_repository
                and "settlement_repository=None" in activity_service
                and "settlement = settlement_repository or _point_shop_purchase_service()" in activity_service
                and "test_point_shop_action_uses_feature_repository" in activity_application_tests
            ),
            "purchase_assets_and_receipt_share_atomic_startup_schema": (
                "DatabaseUnitOfWork(self.database, immediate=True)" in activity_purchase_repository
                and all(
                    table in activity_purchase_repository
                    for table in (
                        "activity_point_balance",
                        "activity_point_purchase",
                        "activity_point_purchase_operations",
                        "user_xiuxian",
                        "back",
                    )
                )
                and "CREATE TABLE" not in activity_purchase_repository
                and "test_purchase_and_receipt_are_atomic_and_idempotent" in activity_purchase_tests
                and "test_receipt_write_failure_rolls_back_points_and_rewards" in activity_purchase_tests
            ),
            "historical_receipts_replay_without_started_retry": (
                "retry_started_actions = frozenset({\"claim_collect_phrase\"})" in activity_command_repository
                and "test_point_shop_started_operation_is_not_blindly_retried" in activity_application_tests
                and "test_backfilled_purchase_receipt_replays_without_granting_assets_twice" in activity_state_migration_tests
                and "apply_activity_state_legacy" in activity_state_migrations
                and 'Migration("activity_state.002"' in plugin
            ),
            "status": "activity_points_shop_reads_and_purchase_owned_by_feature_repositories",
        },
        "dungeon_team": {
            "team_commands_application_owned": all(
                f"dungeon_team_application.{action}(" in dungeon_facade
                for action in ("create", "invite", "join", "reject", "leave", "kick", "disband", "transfer")
            ),
            "legacy_team_service_factories_removed": all(
                name not in dungeon_facade
                for name in (
                    "DungeonTeamTransactionService",
                    "DungeonTeamExitService",
                    "_dungeon_team_transaction_service",
                    "_dungeon_team_exit_service",
                )
            ),
            "team_reads_application_owned": (
                "dungeon_team_application.team_id_for_user(" in dungeon_facade
                and "dungeon_team_application.team_info(" in dungeon_facade
            ),
            "legacy_team_read_helpers_disabled": (
                "get_user_team(" not in dungeon_facade
                and "get_team_info(" not in dungeon_facade
            ),
            "invite_expiry_application_owned": (
                "application.invite_by_id(invite_id)" in dungeon_team_manager
                and "application.expire(" in dungeon_team_manager
                and "service.invite_by_id(" not in dungeon_team_manager
            ),
            "invite_mapping_isolated": (
                "class PersistentTeamInviteMapping" not in dungeon_team_manager
                and "class PersistentTeamInviteMapping" in dungeon_invite_compatibility
                and "legacy_dungeon_team_invite_mapping import" in dungeon_team_manager
            ),
            "legacy_team_transaction_service_isolated": (
                "class DungeonTeamTransactionService" in dungeon_team_legacy_transactions
                and "class DungeonTeamExitService" in dungeon_team_legacy_transactions
                and "class DungeonTeamTransactionService" not in dungeon_transaction_shim
                and "class DungeonTeamExitService" not in dungeon_transaction_shim
            ),
            "legacy_team_transaction_imports_explicit": (
                "legacy_dungeon_team_transactions import" in dungeon_transaction_shim
                and "legacy_dungeon_team_transactions import" in dungeon_invite_compatibility
                and "xiuxian_dungeon.transaction_service import" not in dungeon_invite_compatibility
            ),
            "team_presentation_feature_owned": (
                "from ...features.dungeon.team_presentation import" in dungeon_facade
                and "class TeamViewResult" in dungeon_team_presentation
                and "def build_team_view_message" in dungeon_team_presentation
                and "class TeamViewResult" not in dungeon_transaction_shim
            ),
            "team_presentation_legacy_identity_preserved": (
                "from ...features.dungeon.team_presentation import" in dungeon_transaction_shim
                and "TeamInviteResponseResult" in dungeon_transaction_shim
            ),
            "legacy_dungeon_reset_service_isolated": (
                "class DungeonResetService" in dungeon_reset_compatibility
                and "class DungeonResetResult" in dungeon_reset_compatibility
                and "class DungeonResetService" not in dungeon_transaction_shim
            ),
            "legacy_dungeon_reset_construction_removed": (
                "DungeonResetService" not in dungeon_manager
                and "_legacy_reset_service" not in dungeon_manager
            ),
            "legacy_dungeon_session_service_isolated": (
                "class DungeonSessionService" in dungeon_session_compatibility
                and "class DungeonSessionService" not in dungeon_transaction_shim
                and "legacy_dungeon_session import DungeonSessionResult, DungeonSessionService" in dungeon_transaction_shim
            ),
            "legacy_dungeon_session_imports_explicit": (
                "from ...compatibility.legacy_dungeon_session import DungeonSessionService" in dungeon_repository
                and "from ...xiuxian.xiuxian_dungeon.transaction_service import DungeonSessionService" not in dungeon_repository
            ),
            "session_exit_application_owned": (
                "dungeon_application.session_operation(" in dungeon_facade
                and "dungeon_application.session_transition(" in dungeon_facade
                and "DungeonSessionResult" not in dungeon_facade
            ),
            "dungeon_purchase_application_owned": (
                "dungeon_application.purchase(" in dungeon_facade
                and "dungeon_purchase_service.purchase(" not in dungeon_facade
            ),
            "legacy_dungeon_purchase_service_isolated": (
                "class DungeonPurchaseService" in dungeon_purchase_compatibility
                and "class DungeonPurchaseResult" in dungeon_purchase_compatibility
                and "class DungeonPurchaseService" not in dungeon_transaction_shim
                and "class DungeonPurchaseResult" not in dungeon_transaction_shim
                and "legacy_dungeon_purchase import DungeonPurchaseResult, DungeonPurchaseService" in dungeon_transaction_shim
            ),
            "legacy_dungeon_purchase_imports_explicit": (
                "from ...compatibility.legacy_dungeon_purchase import DungeonPurchaseService" in dungeon_repository
                and "from ...xiuxian.xiuxian_dungeon.transaction_service import DungeonPurchaseService" not in dungeon_repository
                and "from .legacy_dungeon_purchase import DungeonPurchaseResult" in dungeon_compatibility_facade
            ),
            "legacy_dungeon_explore_service_isolated": (
                "class DungeonExploreOperationService" in dungeon_explore_compatibility
                and "class DungeonExploreOperationResult" in dungeon_explore_compatibility
                and "class DungeonExploreOperationService" not in dungeon_transaction_shim
                and "class DungeonExploreOperationResult" not in dungeon_transaction_shim
                and "legacy_dungeon_explore import DungeonExploreOperationResult, DungeonExploreOperationService" in dungeon_transaction_shim
            ),
            "legacy_dungeon_explore_imports_explicit": (
                "from ...compatibility.legacy_dungeon_explore import DungeonExploreOperationService" in dungeon_repository
                and "from ...xiuxian.xiuxian_dungeon.transaction_service import DungeonExploreOperationService" not in dungeon_repository
            ),
            "legacy_dungeon_reward_service_isolated": (
                "class DungeonRewardService" in dungeon_reward_compatibility
                and "class DungeonRewardResult" in dungeon_reward_compatibility
                and "class DungeonRewardService" not in dungeon_transaction_shim
                and "class DungeonRewardResult" not in dungeon_transaction_shim
                and "legacy_dungeon_reward import DungeonRewardResult, DungeonRewardService" in dungeon_transaction_shim
            ),
            "explore_settlement_application_owned": (
                all(
                    f"dungeon_application.{method}(" in dungeon_facade
                    for method in ("replay", "prepare_intent", "prepare_resolution", "settle", "resolve_rejection")
                )
                and "_dungeon_explore_operation_service" not in dungeon_facade
                and "DungeonExploreOperationResult" not in dungeon_facade
            ),
            "reset_application_owned": "self.dungeon_application = DungeonApplication(" in dungeon_manager and "self._reset_application().reset(" in dungeon_manager and "self.reset_service.reset(" not in dungeon_manager,
            "reset_clock_injected": (
                "get_paths().game_db, get_paths().player_db, clock=self.clock" in dungeon_manager
                and "DungeonResetSqlRepository(self.player_database, clock=self.clock)" in dungeon_application
                and "OperationLedger(clock=self.clock)" in dungeon_application
                and "clock=SystemClock()" not in dungeon_application
                and "date.today()" not in dungeon_reset_repository
            ),
            "reset_business_date_uses_scheduler_timezone": (
                'business_timezone=getattr(scheduler, "timezone", None)' in dungeon_facade
                and "self.clock.now().astimezone(self.business_timezone).date().isoformat()" in dungeon_manager
            ),
            "reset_crossday_business_date_frozen": dungeon_manager.count("business_date=current_date") == 2,
            "reset_progress_uses_published_snapshot": (
                '"date": str(global_state.get("date") or "")' in dungeon_progress_read
                and "self._published_template(global_state)" in dungeon_progress_read
                and "_get_current_date()" not in dungeon_progress_read
                and dungeon_progress_read.count("self._get_global_state()") == 1
            ),
            "reset_manual_crossday_replay_covered": (
                "previous = self._reset_operation(operation_id)" in dungeon_manager
                and "current_date = str(previous.business_date)" in dungeon_manager
                and "test_manual_crossday_replay_preserves_current_publication" in dungeon_reset_clock_tests
                and 'match="operation_conflict"' in dungeon_reset_clock_tests
            ),
            "team_schema_startup_migrated": (
                'Migration("dungeon.006", "dungeon_team_state_schema", apply_dungeon_team_schema)' in plugin
                and "def apply_dungeon_team_schema(" in dungeon_migrations
                and '"dungeon.006"' in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):]
            ),
            "team_members_projection_migrated": (
                'Migration("dungeon.009", "dungeon_team_members_index", apply_dungeon_team_members_index)' in plugin
                and "def apply_dungeon_team_members_index(" in dungeon_migrations
                and '"dungeon.009"' in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):]
                and '"dungeon.009"' in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")]
                and "CREATE TABLE IF NOT EXISTS dungeon_team_members" in dungeon_migrations
                and "json_each" in dungeon_migrations
            ),
            "team_member_lookup_bounded": (
                "FROM dungeon_team_members" in dungeon_team_repository
                and "WHERE member_id=? ORDER BY team_id LIMIT 1" in dungeon_team_repository
                and "SELECT * FROM teams" not in dungeon_team_repository
            ),
            "team_invite_expiry_startup_migrated": (
                'Migration("dungeon.010", "dungeon_team_invite_expiry", apply_dungeon_team_invite_expiry)' in plugin
                and '"dungeon.010"' in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):]
                and '"dungeon.010"' in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")]
                and "dungeon_team_invites_expiry_idx" in dungeon_migrations
                and "def _invite_expiry_schema_ready(" in dungeon_team_repository
            ),
            "team_invite_expiry_single_bounded_worker": (
                'id="dungeon_team_invite_expiry", max_instances=1, coalesce=True' in dungeon_facade
                and "await team_invite_expiry_worker.run()" in dungeon_facade
                and "create_task(expire_team_invite" not in dungeon_facade
                and "MAX_INVITE_EXPIRY_BATCH = 100" in dungeon_team_repository
                and "ORDER BY expires_at,invite_id LIMIT ?" in dungeon_team_repository
                and "limit=100" in dungeon_invite_expiry
                and "self._cursor = None if busy else batch.next_cursor" in dungeon_invite_expiry
            ),
            "team_invite_expiry_replay_retry_and_route_covered": (
                "test_not_expired_has_no_receipt_and_can_retry_at_deadline" in dungeon_invite_expiry_tests
                and "test_legacy_not_expired_receipt_is_retryable_without_deleting_row" in dungeon_invite_expiry_tests
                and "test_expiry_receipt_failure_rolls_back_state" in dungeon_invite_expiry_tests
                and "test_due_batch_and_restarted_worker_are_bounded" in dungeon_invite_expiry_tests
                and "test_invite_handler_freezes_origin_before_assign_bot" in dungeon_invite_expiry_tests
                and "test_runtime_notification_uses_exact_origin_bot_and_scene" in dungeon_invite_expiry_tests
            ),
            "team_invite_expiry_notifications_bounded_best_effort": (
                "notification_timeout: float = 2.0" in dungeon_invite_expiry
                and "notification_budget: float = 10.0" in dungeon_invite_expiry
                and "asyncio.wait_for(" in dungeon_invite_expiry
                and "test_notification_budget_expires_all_states_without_sending" in dungeon_invite_expiry_tests
            ),
            "team_invite_expiry_lock_wait_and_poison_batch_bounded": (
                "lock_timeout=0" in dungeon_invite_expiry
                and "timeout=0" in dungeon_team_repository
                and "await asyncio.sleep(0)" in dungeon_invite_expiry
                and "test_locked_database_does_not_block_event_loop_or_accumulate_workers" in dungeon_invite_expiry_tests
                and "test_full_poison_batch_does_not_starve_later_healthy_invite" in dungeon_invite_expiry_tests
            ),
            "explore_team_lookup_bounded": (
                "FROM player_data.dungeon_team_members" in dungeon_repository
                and "WHERE member_id=? ORDER BY team_id LIMIT 1" in dungeon_repository
                and 'uow.query_all("SELECT " + ",".join(selected) + " FROM player_data.teams")' not in dungeon_repository
            ),
            "team_user_lookup_game_owner": (
                "game_database=get_paths().game_db" in dungeon_facade
                and "game_database=get_paths().game_db" in dungeon_team_manager
                and "DatabaseUnitOfWork(self.game_database, read_only=True)" in dungeon_team_repository
            ),
            "team_membership_projection_sync": (
                "def _sync_membership(" in dungeon_team_repository
                and "INSERT INTO dungeon_team_members" in dungeon_team_repository
                and "DELETE FROM dungeon_team_members" in dungeon_team_repository
                and "UPDATE dungeon_team_members SET version=?" in dungeon_team_repository
            ),
            "team_reads_use_read_only_uow": (
                "DatabaseUnitOfWork(self.database, read_only=True)" in dungeon_team_repository
                and "def team_info(" in dungeon_team_repository
                and "def snapshot(" in dungeon_team_repository
                and "DatabaseUnitOfWork(self.database) as uow" not in dungeon_team_repository
            ),
            "team_request_path_has_no_ddl": all(
                token not in dungeon_team_repository
                for token in ("CREATE TABLE", "ALTER TABLE", "ensure_schema", "ensure_team_schema")
            ),
            "team_schema_missing_fails_closed": (
                "def _schema_ready(" in dungeon_team_repository
                and "def _database_exists(" in dungeon_team_repository
                and 'TeamMutationResult("schema_missing"' in dungeon_team_repository
                and 'TeamExitResult("schema_missing"' in dungeon_team_repository
            ),
            "explore_settlement_schema_startup_migrated": (
                'Migration("dungeon.007", "dungeon_explore_player_state_schema", apply_dungeon_explore_player_schema)' in plugin
                and "def apply_dungeon_explore_player_schema(" in dungeon_migrations
                and '"dungeon.007"' in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):]
            ),
            "explore_settlement_request_path_has_no_ddl": all(
                token not in dungeon_repository
                for token in ("CREATE TABLE", "ALTER TABLE", "ensure_schema", "ensure_explore_schema")
            ),
            "explore_settlement_missing_schema_retryable": (
                "def _schema_missing(" in dungeon_repository
                and '"status": "schema_missing"' in dungeon_repository
                and 'return self._schema_missing("prepared")' in dungeon_repository
            ),
            "dungeon_reset_request_path_has_no_ddl": all(
                token not in dungeon_reset_repository
                for token in ("CREATE TABLE", "ALTER TABLE", "ensure_schema")
            ),
            "dungeon_global_state_read_feature_owned": (
                "return self._reset_application().global_state()" in dungeon_manager
                and "_player_data_manager().get_fields(self.GLOBAL_USER_ID" not in dungeon_manager
            ),
            "explore_random_sources_operation_scoped": (
                "def _dungeon_explore_random_source(" in dungeon_facade
                and "DUNGEON_EXPLORE_RNG_VERSION" in dungeon_facade
                and 'random_source=encounter_rng' in dungeon_facade
                and 'random_source=battle_rng' in dungeon_facade
                and "def trigger_event(self, user_level, user_exp, *, random_source=None)" in dungeon_manager
                and "rng.choices(candidates, weights=weights, k=1)[0]" in dungeon_manager
                and "async def pve_fight(" in dungeon_player_fight
                and "_battle_random_source = ContextVar" in dungeon_player_fight
                and "generate_boss_buff(m, random_source=random_source)" in dungeon_player_fight
            ),
            "explore_resolution_intent_frozen_inputs": (
                'Migration("dungeon.008", "dungeon_explore_resolution_intent", apply_dungeon_explore_resolution_intent)' in plugin
                and "def apply_dungeon_explore_resolution_intent(" in dungeon_migrations
                and "def prepare_intent(" in dungeon_repository
                and "def prepare_resolution(" in dungeon_repository
                and '== "intent"' in dungeon_repository
                and '"intent_json"' in dungeon_repository
                and "player_data_by_id=frozen_battle_data" in dungeon_facade
                and "resolution_manager.current_dungeon" in dungeon_facade
                and "_frozen_explore_seed" in dungeon_facade
                and "inventory_snapshot=frozen_inventory" in dungeon_facade
            ),
            "session_schema_missing_fails_closed": (
                "def _columns(" in dungeon_repository
                and '"status": "schema_missing"' in dungeon_repository
                and "DatabaseUnitOfWork(self.player_database, read_only=True)" in dungeon_repository
                and "player_dungeon_status" in dungeon_repository
                and 'if result_status == "schema_missing":' in dungeon_facade
            ),
            "session_aba_schema_required": (
                "_SESSION_STATUS_FIELDS" in dungeon_repository
                and "def _session_status_ready(" in dungeon_repository
                and "not set(columns) <= expected.keys()" in dungeon_repository
            ),
            "status": "team_commands_and_reads_application_owned_with_bounded_member_projection_and_session_schema_boundary",
        },
        "bank": {
            **bank_command_owner,
            "v1_web_game_db_owned": (
                '"bank": BankApplication(' in plugin
                and "LegacyBankRepository" not in plugin
                and all(
                    token in bank_web_application
                    for token in (
                        "BankDepositApplication(self.game_database)",
                        "BankWithdrawalApplication(self.game_database)",
                        "BankUpgradeApplication(self.game_database)",
                        "BankInterestApplication(self.game_database)",
                    )
                )
                and "expected_saved_at=request.expected_saved_at" in bank_web_application
            ),
            "v1_web_started_operation_recoverable": (
                "previous.replay()" in bank_web_application
                and "Feature receipts make a retry safe" in bank_web_application
                and "raise ConflictError(\"操作正在处理中\")" not in bank_web_application
            ),
            "v1_web_snapshots_checked": all(
                token in bank_account_applications
                for token in (
                    "expected_saved_stone",
                    "expected_saved_at",
                    'str(account["bank_level"]) != str(bank_level)',
                )
            ),
            "request_time_legacy_bootstrap_removed": (
                "BankAccountBootstrapApplication" not in bank_web_application
                and "BankAccountBootstrapApplication" not in bank_facade
                and 'return "account_missing"' in bank_web_application
            ),
            "legacy_deposit_disabled": "bank_deposit_service.deposit(" not in bank_facade,
            "legacy_withdrawal_disabled": "bank_withdrawal_service.withdraw(" not in bank_facade,
            "legacy_upgrade_disabled": "bank_upgrade_service.upgrade(" not in bank_facade,
            "legacy_interest_disabled": "bank_interest_service.settle(" not in bank_facade,
            "legacy_account_reads_removed_from_matcher": (
                "_read_legacy_bankinfo" not in bank_handler
                and "_legacy_account_record_status" not in bank_handler
                and "get_legacy_info(" not in bank_handler
            ),
            "legacy_account_reader_is_explicit_read_only_compatibility": (
                "get_legacy_info(" in bank_account_info_application
                and "BankLegacyAccountReadRepository" in bank_account_info_application
                and "read_only=True" in bank_account_info_application
                and "SELECT * FROM \"bankinfo\" WHERE user_id=?" in bank_legacy_account_repository
                and "CREATE TABLE" not in bank_legacy_account_repository
            ),
            "legacy_account_request_fallbacks_removed": (
                "_legacy_account_record_status" not in bank_handler
                and "_read_legacy_bankinfo" not in bank_handler
            ),
            "first_upgrade_and_interest_bootstrap_owned": (
                "initial_account: Mapping[str, Any] | None = None" in bank_account_applications
                and "self.repository.create_account(" in bank_account_applications
                and "def create_account(" in bank_account_repository
            ),
            "upgrade_preserves_interest_clock": (
                "UPDATE bank_accounts SET bank_level=? WHERE user_id=? AND bank_level=?" in bank_upgrade_writer
                and "updated_at" not in bank_upgrade_writer
            ),
            "legacy_account_write_compatibility_isolated": (
                "PlayerDataManager" not in bank_facade
                and "legacy_bank_account_storage" in bank_facade
                and "def savef(" in bank_legacy_account_storage
                and "update_or_write_data(" in bank_legacy_account_storage
            ),
            "legacy_account_writer_has_no_production_callers": (
                not _has_production_bank_savef_import()
                and "return legacy_savef(user_id, data)" in bank_facade
            ),
            "legacy_account_reader_has_no_production_callers": not _has_production_call(
                "get_legacy_info",
                "legacy_record_status",
            ),
            "bank_interest_scheduler_absent": "JOBS = ()" in bank_jobs,
            "legacy_bank_rollback_not_default_composed": (
                "LegacyBankRepository" not in plugin
                and not _has_production_call(
                    "LegacyBankRepository",
                    excluding={PACKAGE / "features" / "bank" / "repository.py"},
                )
            ),
            "account_schema_migration_required": (
                "self.repository.assert_schema_ready(uow)" in bank_account_info_application
                and "def assert_schema_ready(" in bank_account_repository
                and "CREATE TABLE" not in bank_account_repository
                and 'Migration("bank.002", "bank_accounts", apply_bank_accounts)' in plugin
                and 'Migration("bank.003", "bank_legacy_account_backfill", apply_bank_legacy_accounts)' in plugin
                and all(table in bank_migrations for table in ("bank_accounts", "bank_account_operations"))
            ),
            "legacy_account_backfill_is_bounded_conflict_checked_and_space_preflighted": (
                '"bankinfo" WHERE rowid>? ORDER BY rowid LIMIT 200' in bank_migrations
                and "_assert_legacy_account_migration_space(" in bank_migrations
                and "bank_account_operations WHERE user_id=? LIMIT 1" in bank_migrations
                and "bank account migration conflict" in bank_migrations
            ),
            "legacy_json_sync_imports_only_missing_game_accounts": (
                "BankAccountImportApplication" in bank_import_application
                and "importer.import_if_missing(" in (PACKAGE / "xiuxian" / "xiuxian_buff" / "__init__.py").read_text(encoding="utf-8")
                and "self.repository.existing_account(uow, user_id) is not None" in bank_import_application
                and "self.repository.create_account(" in bank_import_application
                and "update_or_write_data(" not in (PACKAGE / "xiuxian" / "xiuxian_buff" / "__init__.py").read_text(encoding="utf-8").split("def _migrate_bank_data_sync", 1)[1].split("@migrate_bank_data.handle", 1)[0]
            ),
            "account_writes_require_startup_schema": (
                bank_account_applications.count("self.repository.assert_schema_ready(uow)") == 4
                and "CREATE TABLE" not in bank_account_applications
            ),
            "status": "command_orchestration_and_receipt_reads_feature_owned; existing_game_db_writers_and_explicit_rollback_retained",
        },
        "map": {
            "nearby_display_feature_owned": (
                "await map_application.nearby_display(" in map_display_handler
                and "exclude_user_id=uid, random_source=runtime_random" in map_display_handler
                and "async def nearby_display(" in map_application
                and "return await select_nearby_display(" in map_application
                and "MapNearbyDisplaySqlQueryRepository(self.player_database, self.game_database)" in map_application
            ),
            "nearby_display_unique_uid_seek_and_first_pair": (
                "class MapNearbyDisplaySqlQueryRepository(MapRandomTargetSqlQueryRepository)" in map_display_repository
                and "earlier_map.rowid<map.rowid" in map_display_repository
                and "earlier_map.user_id AS TEXT) COLLATE BINARY" in map_display_repository
                and "earlier_profile.rowid<profile.rowid" in map_display_repository
                and "CAST(map.user_id AS TEXT) COLLATE BINARY>?" in map_display_repository
                and "ORDER BY CAST(map.user_id AS TEXT) COLLATE BINARY ASC LIMIT ?" in map_display_repository
                and "expected_user_id=user_cursor" in map_display_query
                and "if expected_user_id is not None:" in map_random_repository
            ),
            "nearby_display_bounded_short_read_only": (
                "DISPLAY_PAGE_SIZE = 1" in map_display_repository
                and "read_only=True" in map_display_repository
                and "?mode=ro" in map_display_repository
                and "DISPLAY_LIMIT = 10" in map_display_query
                and "len(selected) < DISPLAY_LIMIT" in map_display_query
                and "CREATE TABLE" not in map_display_repository
                and "ALTER TABLE" not in map_display_repository
                and "await asyncio.sleep(0)" in map_display_query
                and "except MapCandidateReadError:\n        return []" in map_display_query
            ),
            "nearby_display_fair_sample_and_legacy_small_order": (
                "random_source.randint(1, eligible_count)" in map_display_query
                and "slot <= DISPLAY_LIMIT" in map_display_query
                and "random_source.sample(selected, len(selected))" in map_display_query
                and "selected.sort(key=lambda entry: entry[0])" in map_display_query
            ),
            "nearby_display_full_list_and_unbounded_seen_disabled": (
                "_get_all_in_same_node" not in map_facade
                and "map_application.nearby_players(" not in map_display_handler
                and "seen_ids" not in map_display_handler
                and "set()" not in map_display_query
            ),
            "random_nearby_target_feature_owned": (
                "await map_application.random_nearby_target(" in map_qc_handler
                and "exclude_user_id=uid, random_source=runtime_random" in map_qc_handler
                and "async def random_nearby_target(" in map_application
                and "return await select_random_nearby_target(" in map_application
                and "MapRandomTargetSqlQueryRepository(self.player_database, self.game_database)" in map_application
            ),
            "random_nearby_target_bounded_short_read_only": (
                "CANDIDATE_PAGE_SIZE = 256" in map_random_repository
                and "read_only=True" in map_random_repository
                and "?mode=ro" in map_random_repository
                and "SELECT map.rowid AS map_cursor,profile.rowid AS profile_cursor" in map_random_repository
                and "ORDER BY map.rowid ASC,profile.rowid ASC LIMIT ?" in map_random_repository
                and "AND map.rowid=? AND profile.rowid=? LIMIT 1" in map_random_repository
                and "CREATE TABLE" not in map_random_repository
                and "ALTER TABLE" not in map_random_repository
            ),
            "random_nearby_target_pair_weight_and_boundaries_preserved": (
                "MAX(rowid) FROM map_status" in map_random_repository
                and "MAX(rowid) FROM game_data.user_xiuxian" in map_random_repository
                and "map.rowid<=? AND profile.rowid<=?" in map_random_repository
                and "(map.rowid,profile.rowid)>(?,?)" in map_random_repository
                and "CAST(profile.user_id AS TEXT)=CAST(map.user_id AS TEXT)" in map_random_repository
                and "DISTINCT" not in map_random_repository
                and "after = None" in map_random_query
            ),
            "random_nearby_target_fair_cooperative_fail_closed": (
                "random_source.randint(1, eligible_count) == 1" in map_random_query
                and "CANDIDATE_YIELD_INTERVAL = 32" in map_random_query
                and "await asyncio.sleep(0)" in map_random_query
                and "except MapCandidateReadError:\n        return None" in map_random_query
                and "CancelledError" not in map_random_query
                and "del rows" in map_random_query
            ),
            "random_nearby_target_full_list_disabled": (
                "_get_all_in_same_node" not in map_qc_handler
                and "runtime_random.choice(" not in map_qc_handler
            ),
            "named_nearby_target_feature_owned": (
                "map_application.nearby_target(" in map_named_qc
                and "map_application.nearby_target(" in map_record_handler
                and "def nearby_target(" in map_application
                and ").find(" in map_application
            ),
            "named_nearby_target_single_row_read_only": (
                "read_only=True" in map_named_target_query
                and "?mode=ro" in map_named_target_query
                and "profile.rowid ASC LIMIT 1" in map_named_target_query
                and "query_all" not in map_named_target_query
                and "CREATE TABLE" not in map_named_target_query
                and "ALTER TABLE" not in map_named_target_query
            ),
            "named_nearby_self_and_empty_semantics_preserved": (
                "exclude_user_id=uid" in map_named_qc
                and "if not selection.has_candidates:" in map_named_qc
                and "exclude_user_id=None" in map_record_handler
                and "MapNearbyTargetResult(has_candidates=present is not None)" in map_named_target_query
            ),
            "named_nearby_full_list_disabled": (
                "_get_all_in_same_node" not in map_named_qc
                and "_get_all_in_same_node" not in map_record_handler
            ),
            "interactive_application_owned": "map_application.interactive_settlement(" in map_facade and "map_application.interactive_start(" in map_facade,
            "resource_application_owned": "map_application.resource_reward(" in map_facade,
            "combat_engine_injected": (
                "await map_application.combat_battle(" in map_combat_handler
                and "Boss_fight(" not in map_combat_handler
                and "combat_runner=build_legacy_map_battle_runner()" in map_facade
                and "async def combat_battle(" in map_application
                and all(
                    name in map_battle_adapter
                    for name in (
                        "player_data_provider=get_players_attributes",
                        "boss_attribute_provider=get_boss_attributes",
                        "boss_buff_provider=generate_boss_buff",
                        "boss_skill_provider=generate_boss_skill",
                        "boss_status_updater=update_data_boss_status",
                        "player_status_updater=update_all_user_status",
                    )
                )
                and "player_status_updater=None" in player_fight
            ),
            "reward_resolver_feature_owned": (
                "map_reward_resolver = MapRewardResolver(" in map_facade
                and all(
                    f"map_reward_resolver.{method}(" in map_facade
                    for method in (
                        "roll_rewards",
                        "roll_dongfu_material",
                        "roll_skill_equip_drop",
                        "roll_mission_reward",
                    )
                )
                and "class MapRewardResolver:" in map_reward_resolver
                and "def _roll_rewards(" not in map_facade
            ),
            "map_item_catalog_lazy": (
                "class _LazyMapItemCatalog:" in map_facade
                and "self._catalog = None" in map_facade
                and "self._catalog = Items()" in map_facade
                and "items = Items()" not in map_facade
                and "item_catalog=map_item_catalog" in map_facade
            ),
            "static_json_provider_owned": (
                "map_data_provider = MapStaticDataProvider(" in map_facade
                and "map_data_provider.load()" in map_facade
                and "class MapStaticDataProvider:" in map_static_data
                and "self.reader.read_object(self.path)" in map_static_data
            ),
            "legacy_interactive_disabled": "map_interactive_action_service.save_settlement(" not in map_facade and "map_interactive_action_service.start(" not in map_facade,
            "legacy_resource_disabled": "map_resource_reward_service.settle(" not in map_facade,
            "legacy_transactions_isolated": "transaction_service" not in map_facade and all(name not in map_facade for name in ("PlayerDataManager", "XiuxianDateManage", "_sql_message()", "_player_data_manager()")),
            "legacy_transaction_compatibility_explicit": (
                "legacy_map_transactions import *" in map_legacy_shim
                and "...compatibility.legacy_map_transactions" in map_repository
                and "...compatibility.legacy_map_transactions" in combat_settlement_repository
                and "class MapInteractiveActionService" in map_compatibility
            ),
            "mission_claim_effects_outbox_owned": (
                "self.outbox.append(" in map_repository
                and 'event_type="game_event.projection"' in map_repository
                and "map_mission_claim_operations" in map_migrations
                and "def configure_map_application(" in map_facade
                and "configure_map_application(context.services[\"map\"])" in plugin_source
            ),
            "mission_claim_effects_dispatched_on_replay": (
                "self.game_event_effects.dispatch(" in map_application
                and "map_application.mission_claim(" in map_facade
                and "event_meta=reward_meta" in map_facade
                and "safe_record_game_event(" not in map_facade[
                    map_facade.index("@map_mission_claim_cmd.handle") : map_facade.index(
                        "@map_mission_claim_cmd.handle"
                    ) + 4500
                ]
            ),
            "map_dtos_feature_owned": "features.map.schemas import" in map_facade and "class MapInteractiveActionResult" in (PACKAGE / "features" / "map" / "schemas.py").read_text(encoding="utf-8"),
            "status": "map_actions_combat_runner_and_reward_resolution_feature_owned_with_explicit_legacy_adapters",
        },
        "tianti": {
            "frozen_display_commands_registered": all(
                marker in tianti_facade
                for marker in (
                    'tianti_info = on_command("我的炼体", aliases={"炼体状态"}',
                    'tiqiao_info = on_command("我的体窍"',
                    'tianti_help = on_command("炼体帮助"',
                    'tianti_level_help = on_command("炼体境界"',
                )
            ),
            "frozen_dynamic_profile_commands_use_feature_reader": all(
                "tianti_training_application.read_profile(user_id)" in handler
                and "TiantiDataManager" not in handler
                and "get_user_tianti_info" not in handler
                for handler in (tianti_my_profile_handler, tianti_qiaoxue_profile_handler)
            ),
            "my_tianti_display_uses_feature_presentation": (
                "calculate_tianti_gain_rate(" in tianti_my_profile_handler
                and "_get_active_medicine_bath(data, now_t)" in tianti_my_profile_handler
                and "get_sect_fairyland_bonus(sect_fairyland_level)" in tianti_my_profile_handler
                and "_get_tianti_sect_fairyland_level(user_info)" in tianti_my_profile_handler
                and "sect_application.get_sect_info(sect_id)" in tianti_facade[
                    tianti_facade.index("def _get_tianti_sect_fairyland_level") : tianti_facade.index(
                        "def _get_active_medicine_bath"
                    )
                ]
                and "return get_active_medicine_bath(data, now_t)" in tianti_facade[
                    tianti_facade.index("def _get_active_medicine_bath") : tianti_facade.index("def _medicine_bath_slot")
                ]
                and all(
                    name in tianti_facade[: tianti_facade.index("@tianti_info.handle")]
                    for name in ("calculate_tianti_gain_rate", "get_active_medicine_bath", "get_sect_fairyland_bonus")
                )
                and all(
                    f"def {name}(" in tianti_presentation
                    for name in ("calculate_tianti_gain_rate", "get_active_medicine_bath", "get_sect_fairyland_bonus")
                )
            ),
            "my_qiaoxue_display_uses_feature_reader_and_static_catalog": (
                "tianti_training_application.read_profile(user_id)" in tianti_qiaoxue_profile_handler
                and "get_qiaoxue_pool()" in tianti_qiaoxue_profile_handler
                and "get_qiaoxue_map()" in tianti_qiaoxue_profile_handler
                and "TiantiDataManager" not in tianti_qiaoxue_profile_handler
                and "get_user_tianti_info" not in tianti_qiaoxue_profile_handler
            ),
            "my_qiaoxue_display_uses_feature_presentation": (
                "calc_qiaoxue_bonus(data)" in tianti_qiaoxue_profile_handler
                and "def calc_qiaoxue_bonus(" in tianti_presentation
            ),
            "frozen_static_commands_remain_message_only": (
                "await send_help_message(" in tianti_help_handler
                and tianti_static_handler_awaits["help"]
                and set(tianti_static_handler_awaits["help"]).issubset({"assign_bot", "send_help_message"})
                and set(tianti_static_handler_calls["help"]).issubset(
                    {"tianti_help.handle", "Cooldown", "_", "assign_bot", "msg.strip", "send_help_message"}
                )
                and "await handle_send(bot, event, msg)" in tianti_level_help_handler
                and tianti_static_handler_awaits["level_help"]
                and set(tianti_static_handler_awaits["level_help"]).issubset({"assign_bot", "handle_send"})
                and set(tianti_static_handler_calls["level_help"]).issubset(
                    {"tianti_level_help.handle", "Cooldown", "_", "assign_bot", "strip", "handle_send"}
                )
                and all(
                    token not in tianti_help_handler + tianti_level_help_handler
                    for token in (
                        "tianti_training_application.",
                        "tianti_settlement_application.",
                        "get_user_tianti_info(",
                        "TiantiDataManager",
                        "INSERT INTO",
                        "UPDATE ",
                        "DELETE FROM",
                    )
                )
            ),
            "settlement_command_application_owned": "tianti_settlement_application.settle(" in tianti_facade,
            "training_command_application_owned": all(
                f"tianti_training_application.{method}(" in tianti_facade
                for method in ("train", "apply_bath", "breakthrough", "open_qiaoxue", "read_profile")
            ),
            "default_facade_has_no_profile_manager": "TiantiDataManager" not in tianti_facade and "tianti_manager" not in tianti_facade,
            "stone_repository_has_no_legacy_manager_injection": "data_manager" not in tianti_training_stone_repository and "_manager" not in tianti_training_stone_repository,
            "profile_upserts_share_feature_writer": all(
                "upsert_tianti_profile" in source
                for source in (tianti_training_repository, tianti_settlement_repository, sect_fairyland_claim_repository)
            ) and "INSERT INTO" in tianti_training_writer,
            "settlement_default_repository_is_feature_owned": "TiantiSettlementSqlRepository(" in tianti_settlement_application and "LegacyTiantiSettlementRepository" not in tianti_settlement_application,
            "gain_display_rules_are_feature_owned": "from ...features.tianti_training.presentation import" in tianti_facade and "calc_tianti_gain_rate" not in tianti_facade,
            "settlement_and_display_share_rules": all(
                name in tianti_settlement_repository and name in tianti_presentation
                for name in ("calc_qiaoxue_bonus", "get_active_medicine_bath", "get_sect_fairyland_bonus", "parse_tianti_time")
            ) and "_parse_tianti_time" in tianti_training_repository and "_get_sect_fairyland_bonus" in tianti_training_repository,
            "profile_cap_query_feature_owned": "tianti_training_application.profile_cap(data)" in tianti_facade and "transaction_service import" not in tianti_facade and "def profile_cap(" in tianti_training_application,
            "sect_bonus_display_is_feature_owned": "from ...features.tianti_training.presentation import get_sect_fairyland_bonus" in sect_facade,
            "legacy_profile_write_through_is_named": "Legacy write-through getter" in tianti_data,
            "legacy_transaction_adapters_remain_explicit": "class LegacyTiantiTrainingRepository" in tianti_training_repository and "class LegacyTiantiSettlementRepository" in tianti_settlement_repository,
            "legacy_training_adapter_is_per_operation": "def _service(self, operation: str)" in tianti_training_repository and "def _services(" not in tianti_training_repository,
            "status": "feature_profile_writer_and_gain_rules_shared; legacy_training_adapter_constructs_only_requested_service",
        },
        "sect": {
            "membership_application_owned": all(f"sect_application.{name}(" in sect_facade for name in ("join", "leave", "kick", "change_position")),
            "membership_service_isolated": all(f"class {name}" not in sect_transaction_service for name in ("SectOwnerTransfer", "SectFairylandUpgrade", "SectElixirRoomUpgrade", "SectBuffSearch", "SectPracticeUpgrade", "SectScheduledMaterialGrant", "SectElixirRoomMaintenance", "SectDonation", "SectTaskSettlement", "SectTaskClaim", "SectCreation", "SectNameRefresh", "SectRename", "SectMemberRemoval", "SectPositionChange", "SectMembershipService")) and "from ...compatibility.legacy_sect_membership import" in sect_transaction_service and all(f"class {name}" in sect_membership_legacy_service for name in ("SectOwnerTransfer", "SectFairylandUpgrade", "SectElixirRoomUpgrade", "SectBuffSearch", "SectPracticeUpgrade", "SectScheduledMaterialGrant", "SectElixirRoomMaintenance", "SectDonation", "SectTaskSettlement", "SectTaskClaim", "SectCreation", "SectNameRefresh", "SectRename", "SectMemberRemoval", "SectPositionChange", "SectMembershipService")),
            "membership_compatibility_import_isolated": "SectMembershipService" in sect_transaction_service and "from ...compatibility.legacy_sect_membership import SectMembershipService" in sect_membership_legacy_shim,
            "economy_application_owned": all(f"sect_application.{name}(" in sect_facade for name in ("rename", "donate", "purchase")),
            "daily_maintenance_application_owned": "sect_application.reset_daily_maintenance(" in sect_facade,
            "daily_maintenance_service_isolated": all(name not in sect_transaction_service for name in ("class SectMaintenanceOutcome", "class SectDailyResetResult", "class SectDailyResetMaintenanceService")) and "from ...compatibility.legacy_sect_daily_maintenance import" in sect_transaction_service and all(name in sect_daily_maintenance_legacy_service for name in ("class SectMaintenanceOutcome", "class SectDailyResetResult", "class SectDailyResetMaintenanceService")),
            "daily_maintenance_rollback_import_isolated": "from ...compatibility.legacy_sect_daily_maintenance import" in sect_transaction_service and "SectDailyResetMaintenanceService" in sect_transaction_service and "SectDailyResetMaintenanceService" in sect_daily_maintenance_legacy_service,
            "close_mountain_application_owned": sect_facade.count("sect_application.close_mountain(") >= 2,
            "close_mountain_service_isolated": "class SectCloseMountainService" not in sect_transaction_service and "from ...compatibility.legacy_sect_close_mountain import" in sect_transaction_service and "class SectCloseMountainService" in sect_close_mountain_legacy_service,
            "owner_inherit_application_owned": "sect_application.inherit_owner(" in sect_facade,
            "owner_inherit_service_isolated": "class SectOwnerInheritService" not in sect_transaction_service and "from ...compatibility.legacy_sect_owner_inherit import" in sect_transaction_service and "class SectOwnerInheritService" in sect_owner_inherit_legacy_service,
            "join_state_application_owned": "sect_application.open_join(" in sect_facade and "sect_application.close_join(" in sect_facade,
            "join_state_services_isolated": all(name not in sect_transaction_service for name in ("class SectOpenJoinService", "class SectCloseJoinService")) and "from ...compatibility.legacy_sect_join_state import" in sect_transaction_service and all(name in sect_join_state_legacy_service for name in ("class SectOpenJoinService", "class SectCloseJoinService")),
            "disband_application_owned": sect_facade.count("sect_application.disband_inactive(") >= 3,
            "disband_services_isolated": all(name not in sect_transaction_service for name in ("class SectDisbandService", "class SectDisbandResult", "class SectInactiveDisbandResult")) and "from ...compatibility.legacy_sect_disband import" in sect_transaction_service and all(name in sect_disband_legacy_service for name in ("class SectDisbandService", "class SectDisbandResult", "class SectInactiveDisbandResult")),
            "disband_rollback_import_isolated": "from ...compatibility.legacy_sect_disband import" in sect_transaction_service and "SectDisbandService" in sect_transaction_service and "SectDisbandService" in sect_disband_legacy_service,
            "disband_confirmation_application_owned": "sect_application.disband(" in sect_facade[sect_facade.index("async def sect_disband2_confirm"):sect_facade.index("@sect_power_top.handle")] and "_sect_disband_service().disband(" not in sect_facade,
            "disband_confirmation_repository_owned": "class SectManualDisbandSqlRepository" in sect_manual_disband_repository and "SectManualDisbandSqlRepository" in sect_application,
            "disband_confirmation_request_path_has_no_ddl": "CREATE TABLE" not in sect_manual_disband_repository and "schema_missing" in sect_manual_disband_repository,
            "disband_confirmation_migration_registered": all(token in plugin for token in ("sect.013", "apply_sect_manual_disband")) and "sect_disband_operations" in sect_migrations,
            "disband_confirmation_migration_game_only": '"sect.013"' not in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")] and '"sect.013"' not in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")],
            "owner_transfer_application_owned": "sect_application.transfer_owner(" in sect_facade,
            "scheduled_grant_application_owned": "sect_application.grant_scheduled_materials(" in sect_facade,
            "scheduled_grant_target_query_application_owned": "sect_application.list_scheduled_material_targets()" in sect_materials_grant_handler and "_sql_message().get_all_sects_id_scale()" not in sect_materials_grant_handler,
            "scheduled_grant_repository_owned": "SectScheduledMaterialSqlRepository" in sect_application and "def list_targets(" in sect_scheduled_repository,
            "scheduled_grant_request_path_has_no_ddl": "CREATE TABLE" not in sect_scheduled_repository and "schema_missing" in sect_scheduled_repository,
            "scheduled_grant_success_result_owned": '"granted"' in sect_application[sect_application.index("class SectMutationResult"):sect_application.index("class SectApplication")],
            "scheduled_grant_migration_registered": all(token in plugin for token in ("sect.015", "apply_sect_scheduled_materials")) and "sect_scheduled_material_grants" in sect_migrations,
            "scheduled_grant_migration_game_only": '"sect.015"' not in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")] and '"sect.015"' not in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")],
            "sect_directory_application_owned": "sect_application.list_sects_with_member_count()" in sect_inactive_owner_handler and "sect_application.list_sects_with_member_count()" in sect_list_handler and "get_all_sects_with_member_count" not in sect_inactive_owner_handler + sect_list_handler,
            "sect_directory_repository_owned": "SectDirectorySqlRepository" in sect_application and "def list_with_member_count(" in sect_directory_repository,
            "sect_directory_query_read_only": "read_only=True" in sect_directory_repository and "CREATE TABLE" not in sect_directory_repository,
            "sect_active_names_repository_owned": "def list_active_sect_names(" in sect_directory_repository and "WHERE sect_owner IS NOT NULL" in sect_directory_repository,
            "sect_active_names_application_owned": "sect_application.list_active_sect_names()" in sect_member_utils and "_sql_message().get_all_sects()" not in sect_member_utils,
            "sect_member_utils_no_legacy_manager": "XiuxianDateManage" not in sect_member_utils and "sect_task_state_manager" not in sect_member_utils and "_sql_message(" not in sect_member_utils,
            "sect_info_application_owned": "_sql_message().get_sect_info(" not in sect_facade and "_sql_message().get_sect_info_by_id(" not in sect_facade and sect_facade.count("sect_application.get_sect_info(") >= 20,
            "sect_info_repository_owned": "SectInfoSqlRepository" in sect_application and "def get_by_id(" in sect_info_repository,
            "sect_info_repository_read_only": "read_only=True" in sect_info_repository and "normalize_sect_row(row)" in sect_info_repository,
            "sect_id_by_name_repository_owned": "def get_id_by_name(" in sect_info_repository and "SELECT sect_id FROM sects WHERE sect_name=?" in sect_info_repository and "read_only=True" in sect_info_repository,
            "sect_id_by_name_application_owned": "def get_sect_id_by_name(" in sect_application and "sect_application.get_sect_id_by_name(sect_input)" in sect_facade and "_sql_message().get_sect_name(" not in sect_facade,
            "inactive_owner_sect_state_application_owned": "sect_application.get_inactive_owner_sect_state(sect_id)" in sect_inactive_owner_handler and "_sql_message().get_sect_info(sect_id)" not in sect_inactive_owner_handler,
            "inactive_owner_sect_state_repository_owned": "SectInactiveOwnerSqlRepository" in sect_application and "def get_sect_state(" in sect_inactive_owner_repository,
            "inactive_owner_sect_state_read_only": "read_only=True" in sect_inactive_owner_repository and "CREATE TABLE" not in sect_inactive_owner_repository,
            "inactive_owner_profile_application_owned": "sect_application.get_inactive_owner_user_profile(owner_id)" in sect_inactive_owner_handler and "_sql_message().get_user_info_with_id(owner_id)" not in sect_inactive_owner_handler,
            "inactive_owner_profile_repository_owned": "def get_owner_profile(" in sect_inactive_owner_repository and "ORDER BY rowid ASC LIMIT 1" in sect_inactive_owner_repository,
            "inactive_owner_reads_fully_feature_owned": "_sql_message()" not in sect_inactive_owner_handler,
            "sect_member_list_application_owned": "_sql_message().get_all_users_by_sect_id(" not in sect_facade and sect_facade.count("sect_application.list_sect_members(") >= 6,
            "sect_member_list_repository_owned": "SectMemberSqlRepository" in sect_application and "def list_by_sect_id(" in sect_member_repository and "normalize_user_row(row)" in sect_member_repository,
            "sect_member_list_read_only": "read_only=True" in sect_member_repository and "CREATE TABLE" not in sect_member_repository,
            "sect_member_utils_application_injected": "sect_app=sect_application" in sect_facade and "if sect_app is None:" in sect_member_utils and "raise ValueError(\"sect_app is required\")" in sect_member_utils,
            "sect_member_utils_info_reads_feature_owned": sect_member_utils.count("sect_application.get_sect_info(") >= 4,
            "sect_member_utils_join_count_feature_owned": "sect_application.list_sect_members(sect_id)" in sect_member_utils,
            "sect_user_profile_repository_owned": "def get_user_profile(" in sect_member_repository and "ORDER BY rowid ASC LIMIT 1" in sect_member_repository and "normalize_user_row(row)" in sect_member_repository,
            "sect_user_profile_facade_owned": sect_facade.count("sect_application.get_user_profile(") >= 6 and "_sql_message().get_user_info_with_id(" not in sect_facade,
            "sect_user_profile_helper_default_owned": "sect_application.get_user_profile(user_id)" in sect_member_utils and "_sql_message().get_user_info_with_id(user_id)" not in sect_member_utils,
            "sect_user_name_profile_repository_owned": "def get_user_profile_by_name(" in sect_member_repository and "WHERE user_name=?" in sect_member_repository and "ORDER BY rowid ASC LIMIT 1" in sect_member_repository and "normalize_user_row(row)" in sect_member_repository,
            "sect_user_name_profile_facade_owned": sect_facade.count("sect_application.get_user_profile_by_name(") == 2 and "_sql_message().get_user_info_with_name(" not in sect_facade,
            "sect_weekly_progress_profile_application_owned": "_sect_application().get_user_profile(str(user_id))" in sect_weekly_manager and "self._sql_message().get_user_info_with_id(" not in sect_weekly_manager,
            "sect_weekly_progress_application_owned": all(
                f"def {name}(" in sect_application
                for name in ("ensure_weekly_goals", "list_weekly_goal_rows", "record_weekly_progress", "weekly_rank")
            ) and all(
                f"_sect_application().{name}(" in sect_weekly_manager
                for name in ("ensure_weekly_goals", "list_weekly_goal_rows", "record_weekly_progress", "weekly_rank")
            ),
            "sect_weekly_progress_repository_owned": "class SectWeeklyProgressSqlRepository" in sect_weekly_progress_repository and "DatabaseUnitOfWork" in sect_weekly_progress_repository and "def record_progress(" in sect_weekly_progress_repository,
            "sect_weekly_progress_request_path_has_no_ddl": all(
                token not in source
                for source in (sect_weekly_manager, sect_weekly_progress_repository)
                for token in ("CREATE TABLE", "ALTER TABLE")
            ) and "_assert_schema_ready" in sect_weekly_progress_repository,
            "sect_weekly_progress_manager_no_legacy_writes": "self._sql_message().lock" not in sect_weekly_manager and "self._sql_message().conn" not in sect_weekly_manager,
            "sect_weekly_progress_manager_no_legacy_manager": "XiuxianDateManage" not in sect_weekly_manager and "def _sql_message(" not in sect_weekly_manager,
            "fairyland_upgrade_application_owned": "sect_application.upgrade_fairyland(" in sect_facade,
            "fairyland_upgrade_repository_owned": "class SectFairylandSqlRepository" in sect_fairyland_upgrade_repository and "SectFairylandSqlRepository" in sect_application,
            "fairyland_upgrade_request_path_has_no_ddl": all(token not in sect_fairyland_upgrade_repository for token in ("CREATE TABLE", "ALTER TABLE")) and "schema_missing" in sect_fairyland_upgrade_repository,
            "fairyland_upgrade_migration_registered": all(token in plugin for token in ("sect.014", "apply_sect_fairyland_upgrade")) and "sect_fairyland_operations" in sect_migrations,
            "fairyland_upgrade_migration_game_only": '"sect.014"' not in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")] and '"sect.014"' not in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")],
            "fairyland_upgrade_success_result_owned": '"upgraded"' in sect_application[sect_application.index("class SectMutationResult"):sect_application.index("class SectApplication")],
            "fairyland_upgrade_duplicate_handled_before_effects": sect_fairyland_upgrade_handler.index('if result.status == "duplicate"') < sect_fairyland_upgrade_handler.index("safe_log_economy_change("),
            "fairyland_claim_status_application_owned": "sect_fairyland_application.get_last_claim_day(" in sect_facade and "_get_fairyland_last_claim(" not in sect_facade,
            "fairyland_claim_status_repository_owned": "def get_last_claim_day(" in sect_fairyland_repository and "sect_fairyland_claim_days" in sect_fairyland_repository,
            "fairyland_claim_status_read_only": "DatabaseUnitOfWork(self.player_database, read_only=True)" in sect_fairyland_repository,
            "fairyland_claim_status_no_player_manager": "PlayerDataManager" not in sect_fairyland_legacy_state and "def _get_fairyland_last_claim(" not in sect_fairyland_legacy_state,
            "sect_default_repository_has_no_legacy_fallback": "class SectRenameSqlRepository:" in sect_feature_repository and "class SectRenameSqlRepository(LegacySectRepository)" not in sect_feature_repository and "self._service(" not in sect_feature_repository[sect_feature_repository.index("class SectRenameSqlRepository:"):],
            "sect_task_claim_application_owned": "sect_application.claim_task(" in sect_member_utils and "sect_application.refresh_task(" in sect_member_utils and "sect_membership_service" not in sect_facade + sect_member_utils,
            "sect_task_claim_repository_owned": all(f"def {name}(" in sect_task_state_repository for name in ("claim_task", "refresh_task")) and "sect_task_claim_operations" in sect_task_state_repository,
            "sect_task_claim_request_path_has_no_ddl": "CREATE TABLE" not in sect_task_state_repository and "_assert_claim_schema_ready" in sect_task_state_repository,
            "sect_task_claim_migration_registered": all(token in plugin for token in ("sect.017", "apply_sect_task_claim_operations")) and "def apply_sect_task_claim_operations(" in sect_migrations,
            "sect_task_claim_migration_game_only": '"sect.017"' not in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")] and '"sect.017"' not in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")],
            "task_settlement_repository_owned": "class SectTaskSettlementSqlRepository" in sect_task_settlement_repository and "SectTaskSettlementSqlRepository" in sect_application,
            "task_settlement_request_path_has_no_ddl": "CREATE TABLE" not in sect_task_settlement_repository and "_schema_ready" in sect_task_settlement_repository and "schema_missing" in sect_task_settlement_repository,
            "task_settlement_clock_injected": "SectTaskSettlementSqlRepository(self.database, clock=self.clock)" in sect_application and "self.clock.now()" in sect_task_settlement_repository,
            "task_settlement_migration_registered": all(token in plugin for token in ("sect.018", "apply_sect_task_settlement_operations")) and "sect_task_settlement_operations" in sect_migrations,
            "task_settlement_migration_game_only": '"sect.018"' not in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")] and '"sect.018"' not in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")],
            "elixir_room_upgrade_application_owned": "sect_application.upgrade_elixir_room(" in sect_facade,
            "activity_timestamp_application_owned": "sect_application.update_last_check_info_time(user_id)" in sect_elixir_claim_handler and "_sql_message().update_last_check_info_time(" not in sect_elixir_claim_handler,
            "activity_timestamp_repository_owned": "SectActivitySqlRepository" in sect_application and "class SectActivitySqlRepository" in sect_activity_repository,
            "activity_timestamp_clock_injected": "self.clock.now()" in sect_application and "UPDATE user_cd SET last_check_info_time=? WHERE user_id=?" in sect_activity_repository,
            "activity_timestamp_legacy_format_preserved": "astimezone().replace(tzinfo=None)" in sect_application and "isoformat(sep=\" \")" in sect_application,
            "activity_timestamp_order_preserved": sect_elixir_claim_handler.index("sect_application.update_last_check_info_time(user_id)") < sect_elixir_claim_handler.index("if sect_id:"),
            "activity_timestamp_read_application_owned": sect_inactive_owner_handler.count("sect_application.get_last_check_info_time(") == 2 and "_sql_message().get_last_check_info_time(" not in sect_inactive_owner_handler,
            "activity_timestamp_read_repository_owned": "def get_last_check_info_time(" in sect_activity_repository,
            "activity_timestamp_legacy_timezone_safe": "def _elapsed_days(" in sect_disband_repository and "occurred_at.astimezone()" in sect_disband_repository,
            "buff_search_application_owned": sect_facade.count("sect_application.apply_buff_search(") >= 2,
            "practice_application_owned": sect_facade.count("sect_application.upgrade_practice(") >= 3,
            "task_settlement_application_owned": sect_facade.count("sect_application.settle_task(") >= 2,
            "creation_application_owned": sect_facade.count("sect_application.create_sect(") >= 2,
            "name_refresh_application_owned": "sect_application.charge_name_refresh(" in sect_facade,
            "legacy_membership_disabled": all(token not in sect_facade for token in ("sect_membership_service.join", "sect_membership_service.leave_sect", "sect_membership_service.kick_member", "sect_membership_service.change_position")),
            "legacy_membership_getter_removed": "_sect_membership_service" not in sect_facade and "SectMembershipService" not in sect_facade,
            "legacy_transaction_service_import_removed": "transaction_service" not in sect_facade,
            "stale_legacy_service_getters_removed": all(token not in sect_facade for token in (
                "_sect_close_mountain_service", "_sect_owner_inherit_service", "_sect_open_join_service",
                "_sect_close_join_service", "_sect_daily_reset_maintenance_service", "SectCloseMountainService",
                "SectOwnerInheritService", "SectOpenJoinService", "SectCloseJoinService",
                "SectDailyResetMaintenanceService",
            )),
            "weekly_claim_application_owned": "_sect_weekly_application().claim_weekly(" in sect_weekly_commands and "_legacy_sect_weekly_reward_service().claim(" not in sect_weekly_commands,
            "weekly_status_sect_info_application_owned": "_sect_weekly_application().get_sect_info(sect_id)" in sect_weekly_commands and "_sql_message().get_sect_info(sect_id)" not in sect_weekly_commands,
            "weekly_claim_repository_owned": "class SectWeeklyRewardSqlRepository" in sect_weekly_repository and "SectWeeklyRewardSqlRepository" in sect_application,
            "weekly_claim_request_path_has_no_ddl": all(token not in sect_weekly_repository for token in ("CREATE TABLE", "ALTER TABLE")) and all(token not in sect_weekly_manager for token in ("CREATE TABLE", "ALTER TABLE")) and "schema_missing" in sect_weekly_repository,
            "weekly_claim_migrations_registered": all(token in plugin for token in ("sect.011", "sect.012", "apply_sect_weekly", "apply_sect_weekly_player")) and "sect_weekly_reward_operations" in sect_migrations,
            "weekly_claim_player_migration_routed": '"sect.012"' in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")] and '"sect.012"' in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")],
            "weekly_claim_default_has_no_legacy_service": "_legacy_sect_weekly_reward_service" not in sect_weekly_commands and "SectWeeklyRewardClaimService" not in sect_weekly_commands and "XiuxianDateManage" not in sect_weekly_commands,
            "weekly_claim_legacy_service_isolated": all(name not in sect_transaction_service for name in ("class SectWeeklyRewardClaimResult", "class SectWeeklyRewardClaimService")) and "from ...compatibility.legacy_sect_weekly_reward_claim import" in sect_transaction_service and all(name in sect_weekly_claim_legacy_service for name in ("class SectWeeklyRewardClaimResult", "class SectWeeklyRewardClaimService")),
            "weekly_claim_rollback_import_isolated": "from ...compatibility.legacy_sect_weekly_reward_claim import" in sect_transaction_service and "SectWeeklyRewardClaimService" in sect_transaction_service and "SectWeeklyRewardClaimService" in sect_weekly_claim_legacy_service,
            "fairyland_claim_application_owned": "sect_fairyland_application.claim(" in sect_facade and "repository=LegacySectFairylandRepository" not in sect_facade,
            "fairyland_claim_repository_owned": "class SectFairylandSqlRepository" in sect_fairyland_repository and "SectFairylandSqlRepository" in sect_fairyland_application,
            "fairyland_claim_request_path_has_no_ddl": all(token not in sect_fairyland_repository for token in ("CREATE TABLE", "ALTER TABLE")) and "schema_missing" in sect_fairyland_repository,
            "fairyland_claim_migration_registered": all(token in plugin for token in ("sect_fairyland.002", "apply_sect_fairyland_player")) and "sect_fairyland_claim_days" in sect_fairyland_migrations,
            "fairyland_claim_player_migration_routed": '"sect_fairyland.002"' in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")] and '"sect_fairyland.002"' in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")],
            "fairyland_claim_legacy_fallback_retained": "LegacySectFairylandRepository" in sect_fairyland_compatibility,
            "fairyland_claim_service_isolated": "class FairylandClaimService" not in sect_transaction_service and "from ...compatibility.legacy_sect_fairyland_claim import" in sect_transaction_service and "class FairylandClaimService" in sect_fairyland_legacy_service,
            "fairyland_claim_rollback_import_isolated": "from ...compatibility.legacy_sect_fairyland_claim import FairylandClaimService" in sect_fairyland_compatibility and "FairylandClaimService(self.player_database)" in sect_fairyland_compatibility and "from ...compatibility.legacy_sect_fairyland_claim import FairylandClaimService" in sect_fairyland_legacy_shim,
            "elixir_claim_application_owned": "sect_application.claim_elixir(" in sect_elixir_claim_handler and "sect_elixir_claim_service.claim(" not in sect_elixir_claim_handler,
            "elixir_claim_service_isolated": "class SectElixirClaimService" not in sect_transaction_service and "from ...compatibility.legacy_sect_elixir_claim import" in sect_transaction_service and "class SectElixirClaimService" in sect_elixir_legacy_service,
            "elixir_claim_rollback_import_isolated": "from ...compatibility.legacy_sect_elixir_claim import SectElixirClaimService" in sect_feature_repository and "SectElixirClaimService" in sect_feature_repository,
            "member_join_service_isolated": "class SectMemberJoinService" not in sect_transaction_service and "from ...compatibility.legacy_sect_member_join import" in sect_transaction_service and "class SectMemberJoinService" in sect_member_join_legacy_service,
            "member_join_rollback_import_isolated": "from ...compatibility.legacy_sect_member_join import SectMemberJoinService" in sect_feature_repository,
            "shop_purchase_application_owned": "sect_application.purchase(" in sect_facade,
            "shop_purchase_service_isolated": "class SectShopPurchaseService" not in sect_transaction_service and "from ...compatibility.legacy_sect_shop_purchase import" in sect_transaction_service and "class SectShopPurchaseService" in sect_shop_legacy_service,
            "shop_purchase_rollback_import_isolated": "from ...compatibility.legacy_sect_shop_purchase import SectShopPurchaseService" in sect_feature_repository,
            "main_buff_learn_application_owned": "sect_application.learn_main(" in sect_facade,
            "main_buff_learn_service_isolated": "class SectMainBuffLearnService" not in sect_transaction_service and "from ...compatibility.legacy_sect_main_buff_learn import" in sect_transaction_service and "class SectMainBuffLearnService" in sect_main_buff_legacy_service,
            "main_buff_learn_rollback_import_isolated": "from ...compatibility.legacy_sect_main_buff_learn import SectMainBuffLearnService" in sect_feature_repository,
            "secondary_buff_learn_application_owned": "sect_application.learn_secondary(" in sect_facade,
            "secondary_buff_learn_service_isolated": "class SectSecBuffLearnService" not in sect_transaction_service and "from ...compatibility.legacy_sect_secondary_buff_learn import" in sect_transaction_service and "class SectSecBuffLearnService" in sect_secondary_buff_legacy_service,
            "secondary_buff_learn_rollback_import_isolated": "from ...compatibility.legacy_sect_secondary_buff_learn import SectSecBuffLearnService" in sect_feature_repository,
            "status": "weekly_progress_and_claim_application_owned; fairyland_claim_application_owned; disconnected_facade_legacy_getters_removed",
        },
        "natal_treasure": {
            "awaken_application_owned": "natal_treasure_application.awaken(" in natal_facade,
            "effect_upgrade_application_owned": "natal_treasure_application.upgrade(" in natal_facade,
            "legacy_awaken_disabled": "_natal_awaken_service().awaken(" not in natal_facade,
            "status": "awaken_effect_upgrade_cutover_with_other_natal_mutations_compatibility",
        },
        "world_events": {
            "claim_application_owned": "demon_claim_application.claim(" in world_events_facade,
            "claim_repository_owned": "WorldEventClaimSqlRepository" in world_events_application and "WorldEventClaimSqlRepository" in world_events_plugin and "LegacyWorldEventClaimRepository(" not in world_events_plugin,
            "claim_request_path_has_no_ddl": all(token not in world_events_repository for token in ("CREATE TABLE", "ALTER TABLE")),
            "claim_game_migration_registered": 'Migration("world_events.003"' in world_events_plugin and "demon_claim_operations" in world_events_migrations,
            "claim_game_migration_routed": '"world_events.003"' not in world_events_plugin[world_events_plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):world_events_plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")] and '"world_events.003"' not in world_events_plugin[world_events_plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):world_events_plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")],
            "legacy_claim_service_isolated": "class DemonClaimService" not in world_events_transaction and "compatibility.legacy_demon_claim import" in world_events_transaction and "DemonClaimService" in world_events_transaction and "class DemonClaimService" in world_events_claim_compatibility,
            "attack_application_owned": "demon_attack_application.settle(" in world_events_facade and "demon_attack_application.get_result(" in world_events_facade and "DemonAttackSettlementSqlRepository" in world_events_attack_application and "class DemonAttackSettlementSqlRepository" in world_events_attack_repository,
            "attack_legacy_settlement_disconnected": "_demon_attack_settlement_service" not in world_events_facade and "DemonAttackSettlementService" not in world_events_facade,
            "legacy_attack_settlement_isolated": "class DemonAttackSettlementService" not in world_events_transaction and "compatibility.legacy_demon_attack_settlement import" in world_events_transaction and "DemonAttackSettlementService" in world_events_transaction and "class DemonAttackSettlementService" in world_events_attack_compatibility,
            "attack_request_path_has_no_ddl": all(token not in world_events_attack_repository for token in ("CREATE TABLE", "ALTER TABLE")),
            "attack_player_migration_registered": 'Migration("world_events.002"' in plugin and "demon_attack_settlement_operations" in world_events_migrations,
            "attack_player_migration_routed": '"world_events.002"' in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")] and '"world_events.002"' in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")],
            "lifecycle_application_owned": "demon_event_lifecycle_application.replay(" in world_events_facade and "demon_event_lifecycle_application.transition(" in world_events_facade and "class DemonEventLifecycleApplication" in world_events_application and "class DemonEventLifecycleSqlRepository" in world_events_lifecycle_repository,
            "lifecycle_default_has_no_legacy_service": "DemonEventLifecycleService" not in world_events_facade and "_demon_event_lifecycle_service" not in world_events_facade,
            "legacy_lifecycle_service_isolated": "class DemonEventLifecycleService" not in world_events_transaction and "compatibility.legacy_demon_event_lifecycle import" in world_events_transaction and "DemonEventLifecycleService" in world_events_transaction and "class DemonEventLifecycleService" in world_events_lifecycle_compatibility,
            "lifecycle_request_path_has_no_ddl": all(token not in world_events_lifecycle_repository for token in ("CREATE TABLE", "ALTER TABLE")),
            "lifecycle_player_migration_registered": 'Migration("world_events.004"' in plugin and "demon_event_lifecycle_operations" in world_events_migrations,
            "lifecycle_player_migration_routed": '"world_events.004"' in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")] and '"world_events.004"' in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")],
            "wave_refresh_application_owned": "demon_wave_refresh_application.replay(" in world_events_facade and "demon_wave_refresh_application.refresh(" in world_events_facade and "class DemonWaveRefreshApplication" in world_events_application and "class DemonWaveRefreshSqlRepository" in world_events_wave_repository,
            "wave_refresh_default_has_no_legacy_service": "DemonWaveRefreshService" not in world_events_facade and "_demon_wave_refresh_service" not in world_events_facade,
            "legacy_wave_refresh_service_isolated": "class DemonWaveRefreshService" not in world_events_transaction and "compatibility.legacy_demon_wave_refresh import" in world_events_transaction and "DemonWaveRefreshService" in world_events_transaction and "class DemonWaveRefreshService" in world_events_wave_compatibility,
            "wave_refresh_request_path_has_no_ddl": all(token not in world_events_wave_repository for token in ("CREATE TABLE", "ALTER TABLE")),
            "wave_refresh_player_migration_registered": 'Migration("world_events.005"' in plugin and "demon_wave_refresh_operations" in world_events_migrations,
            "wave_refresh_player_migration_routed": '"world_events.005"' in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")] and '"world_events.005"' in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")],
            "spirit_vein_application_owned": "spirit_vein_lifecycle_application.replay(" in world_events_facade and "spirit_vein_lifecycle_application.transition(" in world_events_facade and "class SpiritVeinLifecycleApplication" in world_events_application and "class SpiritVeinLifecycleSqlRepository" in world_events_lifecycle_repository,
            "spirit_vein_default_has_no_legacy_service": "SpiritVeinLifecycleService" not in world_events_facade and "_spirit_vein_lifecycle_service" not in world_events_facade,
            "legacy_spirit_vein_service_isolated": "class SpiritVeinLifecycleService" not in world_events_transaction and "from ...compatibility.legacy_spirit_vein_lifecycle import" in world_events_transaction and "class SpiritVeinLifecycleService" in world_events_spirit_compatibility,
            "spirit_vein_request_path_has_no_ddl": all(token not in world_events_lifecycle_repository for token in ("CREATE TABLE", "ALTER TABLE")),
            "spirit_vein_player_migration_registered": 'Migration("world_events.006"' in plugin and "spirit_vein_lifecycle_operations" in world_events_migrations,
            "spirit_vein_player_migration_routed": '"world_events.006"' in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")] and '"world_events.006"' in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")],
            "legacy_claim_disabled": "_demon_claim_service" not in world_events_facade and "DemonClaimService" not in world_events_facade,
            "status": "demon_attack_claim_event_lifecycle_wave_refresh_and_spirit_vein_feature_owned",
        },
        "rift": {
            "entry_application_owned": "rift_application.enter(" in rift_facade,
            "speedup_application_owned": "rift_application.speedup(" in rift_facade and "rift_application.execute_legacy_call(" not in rift_facade,
            "settlement_application_owned": "rift_application.settle(" in rift_facade,
            "key_event_application_owned": "rift_application.event_settle(" in rift_facade,
            "demon_token_application_owned": "rift_application.replay_demon_token_battle(" in rift_facade and "rift_application.settle_demon_token_battle(" in rift_facade,
            "legacy_demon_token_disabled": "_rift_demon_token_battle_settlement_service" not in rift_facade,
            "demon_token_migrations_registered": all(token in plugin for token in ("rift.002", "rift.003", "apply_rift_demon_token_operations", "apply_rift_demon_token_player_schema")) and "rift_demon_token_battle_operations" in rift_migrations,
            "speedup_default_repository_owned": "repository=repository" in rift_application and "if self.repository is None" in rift_application and "RiftSpeedupSqlRepository(self.database).apply" in rift_application,
            "speedup_migrations_registered": "rift.004" in plugin and "apply_rift_speedup_operations" in plugin and "rift_speedup_operations" in rift_migrations,
            "speedup_request_path_has_no_ddl": "CREATE TABLE" not in rift_speedup_repository and "ALTER TABLE" not in rift_speedup_repository and "schema_missing" in rift_speedup_repository,
            "legacy_speedup_getter_disabled": "RiftSpeedupService" not in rift_facade and "_rift_speedup_service" not in rift_facade,
            "generation_application_owned": all(token in rift_facade for token in ("rift_application.generate(", "rift_application.current_world(", "rift_application.bootstrap_world(")),
            "legacy_generation_disabled": "_rift_entry_service()" not in rift_facade and "RiftEntryService" not in rift_facade,
            "generation_migrations_registered": "rift.005" in plugin and "apply_rift_world_generation" in plugin and "rift_world_state" in rift_migrations and "rift_generation_operations" in rift_migrations,
            "generation_request_path_has_no_ddl": "CREATE TABLE" not in rift_generation_repository and "ALTER TABLE" not in rift_generation_repository and "schema_missing" in rift_generation_repository,
            "termination_application_owned": all(token in rift_facade for token in ("rift_application.terminate(", "rift_application.replay_termination(")),
            "legacy_termination_disabled": "_rift_termination_service()" not in rift_facade and "RiftTerminationService" not in rift_facade,
            "termination_migrations_registered": "rift.006" in plugin and "apply_rift_termination_operations" in plugin and "rift_termination_operations" in rift_migrations,
            "termination_request_path_has_no_ddl": "CREATE TABLE" not in rift_termination_repository and "ALTER TABLE" not in rift_termination_repository and "schema_missing" in rift_termination_repository,
            "key_event_default_repository_owned": "RiftKeyEventSqlRepository" in rift_application and "self.key_event_repository.settle" in rift_application,
            "legacy_key_event_disabled": "_rift_key_event_settlement_service()" not in rift_facade and "RiftKeyEventSettlementService" not in rift_facade,
            "key_event_migrations_registered": "rift.007" in plugin and "apply_rift_key_event_operations" in plugin and "rift_key_event_operations" in rift_migrations,
            "key_event_request_path_has_no_ddl": "CREATE TABLE" not in rift_key_event_repository and "ALTER TABLE" not in rift_key_event_repository,
            "settlement_default_repository_owned": "RiftSettlementSqlRepository" in rift_application and "self.settlement_repository.settle" in rift_application,
            "legacy_settlement_disabled": "_rift_settlement_service()" not in rift_facade and "RiftSettlementService" not in rift_facade,
            "settlement_migrations_registered": "rift.008" in plugin and "apply_rift_settlement_operations" in plugin and "rift_settlement_operations" in rift_migrations,
            "settlement_request_path_has_no_ddl": "CREATE TABLE" not in rift_settlement_repository and "ALTER TABLE" not in rift_settlement_repository,
            "entry_default_repository_owned": "RiftEntrySqlRepository" in rift_application and "self.entry_repository.enter" in rift_application,
            "entry_migrations_registered": "rift.009" in plugin and "apply_rift_entry_schema" in plugin and all(token in rift_migrations for token in ("rift_entries", "rift_entry_counts", "rift_entry_operations")),
            "entry_request_path_has_no_ddl": "CREATE TABLE" not in rift_entry_repository and "ALTER TABLE" not in rift_entry_repository,
            "legacy_entry_disabled": "_rift_entry_service().enter(" not in rift_facade,
            "entry_read_projection_repository_owned": "RiftEntrySqlRepository" in rift_jsondata and "read_entry" in rift_jsondata,
            "entry_read_projection_has_no_ddl": "CREATE TABLE" not in rift_jsondata and "ALTER TABLE" not in rift_jsondata,
            "legacy_entry_read_disabled": "RiftEntryService" not in rift_jsondata,
            "cooldown_read_projection_repository_owned": "RiftCooldownSqlRepository" in rift_application and "self.cooldown_repository.read" in rift_application,
            "cooldown_read_request_path_has_no_ddl": "CREATE TABLE" not in rift_cooldown_repository and "ALTER TABLE" not in rift_cooldown_repository,
            "legacy_cooldown_read_disabled": "get_user_cd(" not in rift_facade and "XiuxianDateManage" not in rift_facade,
            "damage_event_application_owned": "rift_application.roll_damage_event(" in rift_event_handler,
            "damage_event_resolver_owned": "class RiftDamageEventResolver" in rift_domain and "RiftDamageEventResolver" in rift_application,
            "damage_event_is_persistence_free": "update_exp" not in rift_domain and "update_ls" not in rift_domain,
            "legacy_damage_event_disabled": "get_dxsj_info(" not in rift_event_handler,
            "boss_battle_application_owned": "rift_application.roll_boss_battle(" in rift_event_handler and "rift_application.roll_boss_battle(" in rift_boss_handler,
            "boss_battle_resolver_owned": "class RiftBossBattleResolver" in rift_domain and "RiftBossBattleResolver" in rift_application,
            "boss_battle_is_persistence_free": "update_exp" not in rift_domain and "update_ls" not in rift_domain and "battle_mode=0" in rift_application,
            "legacy_boss_battle_disabled": all("get_boss_battle_info(" not in handler for handler in (rift_event_handler, rift_boss_handler)),
            "boss_battle_asset_provider_wired": "player_asset_provider=get_rift_battle_player_assets" in rift_facade and "player_asset_provider=get_rift_battle_player_assets" in rift_make,
            "boss_battle_asset_snapshot_boundary": "class RiftBossBattleAssetProvider" in rift_domain and "runner_kwargs[\"player_data\"] = player_data" in rift_domain and "player1_data = player_data if" in rift_player_fight,
            "boss_battle_engine_provider_wired": all(
                token in rift_make and token in rift_facade
                for token in (
                    "boss_attribute_provider=get_boss_attributes",
                    "boss_buff_provider=generate_boss_buff",
                    "boss_skill_provider=get_rift_battle_boss_skill_provider",
                    "boss_status_updater=ignore_rift_battle_boss_status_update",
                )
            ) and all(
                token in rift_domain and token in rift_player_fight
                for token in (
                    "boss_attribute_provider",
                    "boss_buff_provider",
                    "boss_skill_provider",
                    "boss_status_updater",
                )
            ) and all(
                token in rift_domain for token in (
                    'runner_kwargs["boss_attribute_provider"] = self.boss_attribute_provider',
                    'runner_kwargs["boss_buff_provider"] = self.boss_buff_provider',
                    'runner_kwargs["boss_skill_provider"] = self.boss_skill_provider',
                    'runner_kwargs["boss_status_updater"] = self.boss_status_updater',
                )
            ),
            "boss_battle_buff_random_source_wired": 'runner_kwargs["boss_buff_random_source"] = random_source' in rift_domain and "boss_buff_random_source=None" in rift_player_fight and "random_source=boss_buff_random_source" in rift_player_fight and "rng = random_source or random" in rift_player_fight,
            "boss_battle_status_writeback_isolated": "def ignore_rift_battle_boss_status_update" in rift_make and "return None" in rift_make[rift_make.index("def ignore_rift_battle_boss_status_update"):rift_make.index("async def get_boss_battle_info", rift_make.index("def ignore_rift_battle_boss_status_update"))] and "boss_status_updater=ignore_rift_battle_boss_status_update" in rift_make,
            "boss_battle_skill_provider_read_only": "def get_rift_battle_boss_skill_data" in rift_make and "skill_path.open" in rift_make and "skill_data_cache" not in rift_make[rift_make.index("def get_rift_battle_boss_skill_data"):rift_make.index("async def get_boss_battle_info", rift_make.index("def get_rift_battle_boss_skill_data"))],
            "boss_battle_legacy_asset_provider_explicit": "def get_rift_battle_player_assets" in rift_make and "get_players_attributes(" in rift_make and "item_provider=get_rift_battle_item_data" in rift_make,
            "boss_battle_item_provider_wired": "item_provider=get_rift_battle_item_data" in rift_make and "item_data = item_provider(item_id)" in rift_player_fight,
            "boss_battle_item_provider_read_only": "def get_rift_battle_item_data" in rift_make and "item_path.open" in rift_make and "ITEMS_CACHE" not in rift_make[rift_make.index("def get_rift_battle_item_data"):rift_make.index("def get_rift_battle_boss_skill_data", rift_make.index("def get_rift_battle_item_data"))],
            "boss_battle_items_lazy": all("\nitems = Items()\n" not in source and "_items_instance" in source for source in (rift_make, rift_player_fight, rift_attributes)),
            "boss_battle_pet_provider_wired": "pet_provider=get_user_pet_for_battle" in rift_make and "buffs[\"宠物\"] = pet_provider(user_id)" in rift_player_fight,
            "boss_battle_attribute_provider_wired": "attribute_provider=get_rift_battle_final_attributes" in rift_make and "final_attr = attribute_provider(user_id, ratio=ratio, include_current=True)" in rift_player_fight,
            "boss_battle_natal_provider_wired": "natal_provider=get_rift_battle_natal_data" in rift_make and "natal_data = natal_provider(user_id)" in rift_player_fight,
            "boss_battle_natal_provider_read_only": "def get_rift_battle_natal_data" in rift_make and "DatabaseUnitOfWork(database, read_only=True)" in rift_make and "CREATE TABLE" not in rift_make[rift_make.index("def get_rift_battle_natal_data"):rift_make.index("async def get_boss_battle_info", rift_make.index("def get_rift_battle_natal_data"))],
            "boss_battle_impart_provider_wired": "impart_provider=get_rift_battle_impart_data" in rift_make and "impart_provider=None" in rift_attributes and "impart = impart_provider(user_id) or {}" in rift_attributes,
            "boss_battle_impart_provider_read_only": "def get_rift_battle_impart_data" in rift_make and "DatabaseUnitOfWork(database, read_only=True)" in rift_make and "CREATE TABLE" not in rift_make[rift_make.index("def get_rift_battle_impart_data"):rift_make.index("def get_rift_battle_final_attributes", rift_make.index("def get_rift_battle_impart_data"))],
            "boss_battle_buff_info_provider_wired": "buff_info_provider=get_rift_battle_buff_info" in rift_make and "buff_info_provider=None" in rift_attributes and "buff_info = buff_info_provider(user_id) or {}" in rift_attributes,
            "boss_battle_buff_info_provider_read_only": "def get_rift_battle_buff_info" in rift_make and "DatabaseUnitOfWork(database, read_only=True)" in rift_make and "CREATE TABLE" not in rift_make[rift_make.index("def get_rift_battle_buff_info"):rift_make.index("async def get_boss_battle_info", rift_make.index("def get_rift_battle_buff_info"))],
            "boss_battle_accessory_provider_wired": "accessory_provider=get_rift_battle_accessory_data" in rift_make and "accessory_provider=None" in rift_attributes and "calc_accessory_effects(user_id, accessory_provider=accessory_provider)" in rift_attributes,
            "boss_battle_accessory_provider_read_only": "def get_rift_battle_accessory_data" in rift_make and "DatabaseUnitOfWork(database, read_only=True)" in rift_make and "CREATE TABLE" not in rift_make[rift_make.index("def get_rift_battle_accessory_data"):rift_make.index("async def get_boss_battle_info", rift_make.index("def get_rift_battle_accessory_data"))],
            "boss_battle_tianti_provider_wired": "tianti_provider=get_rift_battle_tianti_data" in rift_make and "tianti_provider=None" in rift_attributes and "_tdata = tianti_provider(user_id) or {}" in rift_attributes,
            "boss_battle_tianti_provider_read_only": "def get_rift_battle_tianti_data" in rift_make and "DatabaseUnitOfWork(database, read_only=True)" in rift_make and "CREATE TABLE" not in rift_make[rift_make.index("def get_rift_battle_tianti_data"):rift_make.index("async def get_boss_battle_info", rift_make.index("def get_rift_battle_tianti_data"))],
            "boss_battle_base_provider_wired": "base_provider=get_rift_battle_base_attributes" in rift_make and "base_provider=None" in rift_attributes and "base_provider = base_provider or get_base_attributes" in rift_attributes,
            "boss_battle_base_provider_read_only": "def get_rift_battle_base_attributes" in rift_make and "DatabaseUnitOfWork(database, read_only=True)" in rift_make and "CREATE TABLE" not in rift_make[rift_make.index("def get_rift_battle_base_attributes"):rift_make.index("async def get_boss_battle_info", rift_make.index("def get_rift_battle_base_attributes"))],
            "treasure_application_owned": "rift_application.roll_treasure(" in rift_event_handler,
            "treasure_resolver_owned": "class RiftTreasureResolver" in rift_domain and "RiftTreasureResolver" in rift_application,
            "treasure_is_persistence_free": "update_exp" not in rift_domain and "update_ls" not in rift_domain and "update_ls" not in rift_application,
            "legacy_treasure_disabled": "get_treasure_info(" not in rift_event_handler,
            "status": "world_generation_termination_key_event_settlement_entry_speedup_demon_token_damage_event_boss_battle_asset_boundary_engine_provider_boundary_skill_provider_boundary_buff_random_source_boundary_status_writeback_isolation_item_provider_boundary_lazy_items_boundary_treasure_cutover_with_natal_impart_buff_info_accessory_tianti_and_base_provider_boundaries_and_remaining_rift_compatibility",
        },
        "beg": {
            **_beg_command_owner_status(beg_command_sources),
            "daily_reset_application_owned": (
                '_run_job("仙途奇缘重置", _daily_beg_reset)' in mixelixir_scheduler
                and "_sql_message().beg_remake" not in mixelixir_scheduler
                and "BegApplication(get_paths().game_db).reset_daily_claim_flag(_scheduler_business_date())" in mixelixir_scheduler
                and "BegDailyResetSqlRepository(self.database, ledger=self.ledger).reset(" in beg_application
            ),
            "daily_reset_atomic_no_request_ddl": (
                "DatabaseUnitOfWork(self.database, immediate=True)" in beg_daily_reset_repository
                and "self.ledger.finish(uow, outcome)" in beg_daily_reset_repository
                and '"operation_ledger"' in beg_daily_reset_repository
                and '"operation_audit"' in beg_daily_reset_repository
                and '"is_beg"' in beg_daily_reset_repository
                and "CREATE TABLE" not in beg_daily_reset_repository
            ),
            "daily_reset_replay_rollback_and_missing_schema_covered": all(
                marker in beg_daily_reset_tests
                for marker in (
                    "same_day_replay_preserves_new_claims",
                    "daily_reset_conflict_and_missing_schema_fail_closed",
                    "rolls_back_flag_and_ledger_when_audit_write_fails",
                )
            ),
            "status": "three_commands_feature_owned_with_receipt_first_reads_and_existing_atomic_claims_and_daily_reset",
        },
        "back": {
            "daily_pill_usage_reset_application_owned": (
                "_run_job(\"每日丹药使用次数重置\", _daily_pill_usage_reset)" in mixelixir_scheduler
                and "_sql_message().day_num_reset" not in mixelixir_scheduler
                and "class DailyPillUsageResetApplication" in daily_pill_reset_application
            ),
            "daily_pill_usage_reset_atomic_no_request_ddl": (
                "DatabaseUnitOfWork(self.database, immediate=True)" in daily_pill_reset_repository
                and "self.ledger.finish(uow, outcome)" in daily_pill_reset_repository
                and "CREATE TABLE" not in daily_pill_reset_repository
            ),
            "cultivation_item_application_owned": "back_application.cultivation_item(" in back_facade and "_cultivation_item_application().apply(" in back_util_facade,
            "legacy_cultivation_item_disabled": "_cultivation_item_service().apply(" not in back_facade and "_cultivation_item_service().apply(" not in back_util_facade,
            "skill_learning_application_owned": "back_application.learn_skill(" in back_facade,
            "legacy_skill_learning_disabled": "_skill_learning_service().learn(" not in back_facade,
            "lottery_talisman_application_owned": "back_application.lottery_talisman(" in back_facade,
            "legacy_lottery_talisman_disabled": "_lottery_talisman_service().apply(" not in back_facade,
            "stone_reward_application_owned": back_facade.count("back_application.stone_reward(") >= 2,
            "legacy_stone_reward_disabled": "_stone_reward_service().apply(" not in back_facade,
            "three_cultivation_pill_application_owned": "back_application.three_cultivation_pill(" in back_facade,
            "legacy_three_cultivation_pill_disabled": "_three_cultivation_pill_service().apply(" not in back_facade,
            "breakthrough_rate_item_application_owned": "_breakthrough_rate_item_application().apply(" in back_util_facade,
            "legacy_breakthrough_rate_item_disabled": "_breakthrough_rate_item_service().apply(" not in back_util_facade,
            "recovery_item_application_owned": "_recovery_item_application().apply(" in back_util_facade,
            "legacy_recovery_item_disabled": "_recovery_item_service().apply(" not in back_util_facade,
            "permanent_atk_item_application_owned": "_permanent_atk_item_application().apply(" in back_util_facade,
            "legacy_permanent_atk_item_disabled": "_permanent_atk_item_service().apply(" not in back_util_facade,
            "alchemy_application_owned": back_facade.count("back_application.alchemy(") >= 3,
            "legacy_alchemy_disabled": "_alchemy_service().apply(" not in back_facade,
            "unbind_application_owned": "back_application.unbind(" in back_facade,
            "legacy_unbind_disabled": "_unbind_item_service().apply(" not in back_facade,
            "repair_application_owned": "back_application.repair(" in back_facade,
            "equipment_unequip_application_owned": "back_application.change_equipment(" in back_facade,
            "equipment_equip_application_owned": back_facade.count("back_application.change_equipment(") >= 2,
            "pet_egg_application_owned": "back_application.use_pet_eggs(" in back_facade,
            "generic_item_use_application_owned": "self.item_use_application.apply(" in back_application_source,
            "legacy_generic_item_use_default_disabled": (
                "if self._explicit_repository is None:\n            if \"item_id\"" in back_application_source
            ),
            "package_application_owned": (
                "package_reward_application.open_package(" in back_facade
                and "back_application.open_package(" not in back_facade
            ),
            "accessory_package_application_owned": "back_application.accessory_package(" in back_facade,
            "accessory_affix_application_owned": (
                back_accessory_facade.count("_affix_application().set_locks") >= 2
                and "_affix_application().replay" in back_accessory_facade
            ),
            "legacy_accessory_affix_disabled": "_accessory_transaction_service().set_affix_locks" not in back_accessory_facade,
            "accessory_decompose_application_owned": (
                "_decompose_application().decompose" in back_accessory_facade
                and "_decompose_application().replay" in back_accessory_facade
            ),
            "legacy_accessory_decompose_disabled": "_accessory_transaction_service().decompose" not in back_accessory_facade,
            "accessory_batch_decompose_application_owned": "_decompose_application().batch_decompose" in back_accessory_facade,
            "legacy_accessory_batch_decompose_disabled": "_accessory_transaction_service().batch_decompose" not in back_accessory_facade,
            "accessory_wash_application_owned": "_wash_application().wash" in back_accessory_facade and "_wash_application().replay" in back_accessory_facade,
            "legacy_accessory_wash_disabled": "_accessory_transaction_service().wash" not in back_accessory_facade,
            "accessory_upgrade_application_owned": "_upgrade_application().upgrade" in back_accessory_facade and "_upgrade_application().replay" in back_accessory_facade,
            "legacy_accessory_upgrade_disabled": "_accessory_transaction_service().upgrade" not in back_accessory_facade,
            "accessory_preset_application_owned": "_preset_application().save" in back_accessory_facade and "_preset_application().replay" in back_accessory_facade,
            "legacy_accessory_preset_disabled": "_accessory_transaction_service().save_preset" not in back_accessory_facade,
            "accessory_quick_equip_application_owned": "_quick_equip_application().equip" in back_accessory_facade and "_quick_equip_application().replay" in back_accessory_facade,
            "legacy_accessory_quick_equip_disabled": "_accessory_transaction_service().quick_equip_preset" not in back_accessory_facade,
            "legacy_repair_disabled": "_backpack_repair_service().run(" not in back_facade,
            "status": "cultivation_item_skill_learning_lottery_talisman_stone_reward_three_cultivation_pill_alchemy_unbind_repair_equipment_equip_unequip_pet_egg_package_accessory_package_affix_lock_unlock_decompose_batch_decompose_wash_upgrade_preset_quick_equip_generic_item_use_cutover_with_other_back_compatibility",
        },
        "past_life": {
            "final_settlement_application_owned": "_past_life_application.final_settle(" in past_life_events_facade,
            "choice_application_owned": "_past_life_application.choice(" in past_life_events_facade,
            "start_application_owned": "_past_life_application.start(" in past_life_events_facade,
            "item_catalog_lazy": (
                "_items_instance = None" in past_life_events_facade
                and "def _items(" in past_life_events_facade
                and "items = Items()" not in past_life_events_facade
                and "_items().get_random_id_list_by_rank_and_item_type(" in past_life_events_facade
            ),
            "reset_one_application_owned": "past_life_application.reset_one(" in past_life_command_facade,
            "legacy_reset_one_disabled": "_past_life_reset_service().reset_one(" not in past_life_command_facade,
            "reset_all_application_owned": (
                "past_life_application.reset_all_create(" in past_life_command_facade
                and "past_life_application.reset_all_batch(" in past_life_command_facade
                and "past_life_application.reset_all_pending(" in past_life_command_facade
            ),
            "legacy_reset_all_disabled": (
                "_past_life_reset_service().create_all(" not in past_life_command_facade
                and "_past_life_reset_service().run_batch(" not in past_life_command_facade
                and "_past_life_reset_service().find_pending_all(" not in past_life_command_facade
            ),
            "status": "reset_one_and_reset_all_cutover_with_legacy_service_retained_for_compatibility",
        },
        "avatar_identity": {
            "migration_is_player_db_only": (
                '"info.avatar.001"' in plugin_source[
                    plugin_source.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):
                    plugin_source.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"info.avatar.001"' in plugin_source[
                    plugin_source.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):
                    plugin_source.index("_TRADE_DATABASE_MIGRATION_VERSIONS")
                ]
                and 'Migration("info.avatar.001"' in plugin_source
                and "def apply_avatar_identity_player(" in avatar_migrations
            ),
            "initialization_migration_is_player_db_only": (
                '"info.avatar.002"' in plugin_source[
                    plugin_source.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):
                    plugin_source.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"info.avatar.002"' in plugin_source[
                    plugin_source.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):
                    plugin_source.index("_TRADE_DATABASE_MIGRATION_VERSIONS")
                ]
                and 'Migration("info.avatar.002"' in plugin_source
                and "def apply_avatar_initialization_player(" in avatar_migrations
            ),
            "legacy_avatar_rows_preserved_and_prebuilt": (
                "CREATE TABLE IF NOT EXISTS avatar (" in avatar_migrations
                and "ALTER TABLE avatar ADD COLUMN" in avatar_migrations
                and "avatar_operation_receipts" in avatar_migrations
            ),
            "active_id_writes_are_player_owned_and_atomic": (
                "_player_avatar_application().toggle_active(" in avatar_facade
                and "_player_avatar_application().restore_active(" in avatar_facade
                and "_player_data_manager().update_or_write_data(main_id, \"avatar\", \"active_id\"" not in avatar_facade
                and "DatabaseUnitOfWork(self.database, immediate=True)" in avatar_repository
                and "UPDATE avatar SET active_id=?" in avatar_repository
                and "avatar_operation_receipts" in avatar_repository
            ),
            "active_id_reads_use_application_and_preserve_priority": (
                _avatar_identity_priority(avatar_utils)
                and "user_id = get_active_user_id(real_user_id)" in avatar_base_facade
            ),
            "initialization_is_player_application_owned_and_recoverable": (
                "_player_avatar_application().initialize(" in avatar_facade
                and "_player_avatar_application().get_avatar_info(" in avatar_facade
                and "_player_data_manager().update_or_write_data(" not in avatar_facade
                and "_run_info_action(" not in avatar_facade
                and "avatar_initialization_plans" in avatar_repository
                and "def _complete_initialization(" in avatar_repository
            ),
            "request_path_has_no_ddl": "CREATE TABLE" not in avatar_repository and "ALTER TABLE" not in avatar_repository,
            "status": "active_avatar_restore_toggle_owned_by_player_database_with_receipts",
        },
        "dufang": {
            "share_application_owned": "dufang_application.share_settle(" in dufang_facade,
            "legacy_share_disabled": (
                "DufangShareSettlementService" not in dufang_repository
                and "_dufang_share_service" not in dufang_facade
            ),
            "share_resume_reachable": "dufang_application.resume_share(" in dufang_facade,
            "share_repository_owned": "DufangShareSqlRepository" in dufang_repository,
            "share_legacy_settlement_disabled": (
                "DufangShareSettlementService" not in dufang_repository
                and "_dufang_share_service" not in dufang_facade
            ),
            "share_request_path_has_no_ddl": (
                "CREATE TABLE" not in dufang_share_repository
                and "ALTER TABLE" not in dufang_share_repository
            ),
            "share_ledger_identity_stable": 'identity = {"user_id": user_id}' in dufang_application,
            "share_player_receipt_recovery_owned": (
                "dufang_share_player_receipts" in dufang_share_repository
                and "dufang_share_player_receipts" in dufang_migrations
            ),
            "share_migrations_registered_and_routed": (
                "legacy.dufang.002" in legacy_migrated_source
                and "legacy.dufang.003" in legacy_migrated_source
                and '"legacy.dufang.003"' in plugin_source
            ),
            "bet_payout_request_path_has_no_ddl": all(
                token not in source
                for source in (dufang_bet_repository, dufang_payout_repository)
                for token in ("CREATE TABLE", "ALTER TABLE")
            ),
            "bet_payout_missing_schema_fails_closed": (
                all("if not self.game_database.is_file()" in source or "if not Path(self.game_database).is_file()" in source
                    for source in (dufang_bet_repository, dufang_payout_repository))
                and "if not self.player_database.is_file()" in dufang_player_stats_repository
                and "if not self.repository.player_projection_ready()" in dufang_application
            ),
            "bet_payout_migration_registered": (
                "legacy.dufang.004" in legacy_migrated_source
                and "def apply_dufang_bet_payout(" in dufang_migrations
            ),
            "bet_resolution_migrations_registered": (
                "legacy.dufang.005" in legacy_migrated_source
                and "legacy.dufang.006" in legacy_migrated_source
                and "def apply_dufang_resolution(" in dufang_migrations
                and "def apply_dufang_player_receipts(" in dufang_migrations
            ),
            "bet_resolution_replay_uses_frozen_plan": (
                "dufang_application.resolution(operation_id)" in dufang_unseal_handler
                and "dufang_application.plan_for_bet(" in dufang_unseal_handler
                and "dufang_bet_resolutions" in dufang_bet_repository
                and "resolution=resolution" in dufang_unseal_handler
                and "runtime_random." not in dufang_unseal_handler
            ),
            "bet_payout_player_stats_use_outbox_receipts": (
                "append_player_outbox(" in dufang_bet_repository
                and "append_player_outbox(" in dufang_payout_repository
                and "dufang_player_operation_receipts" in dufang_player_stats_repository
                and "dufang_player_operation_receipts" in dufang_migrations
            ),
            "bet_payout_started_ledger_recovers": (
                'existing.status != "started"' in dufang_application
                and "self.ledger.begin(uow, operation_id, ledger_action, ledger_request)" in dufang_application
                and "resolution=resolution" in dufang_unseal_handler
            ),
            "bet_payout_pending_reconciliation_is_bounded": (
                "limit = max(1, min(int(limit), 5))" in dufang_application
                and "WHERE b.status='pending'" in dufang_bet_repository
                and "LIMIT ?" in dufang_bet_repository
                and "dufang_application.reconcile_pending(" in dufang_unseal_handler
            ),
            "storage_audit_is_read_only_and_bounded": (
                "mode=ro" in dufang_storage_audit
                and "PRAGMA query_only=ON" in dufang_storage_audit
                and "PRAGMA page_count" in dufang_storage_audit
                and "PRAGMA freelist_count" in dufang_storage_audit
                and "minimum_sqlite_backup_estimate_bytes" in dufang_storage_audit
                and "MAX_BACKUP_SCAN_ENTRIES" in dufang_storage_audit
                and "existing_backup_inventory" in dufang_storage_audit
                and "dufang_player_operation_receipts" in dufang_storage_audit
                and "dufang_bet_resolutions" in dufang_storage_audit
                and "archive_ready\": False" in dufang_storage_audit
                and "VACUUM" not in dufang_storage_audit
                and "wal_checkpoint" not in dufang_storage_audit
            ),
            "backup_capacity_preflight_is_shared_and_fail_closed": (
                "preflight_capacity(" in backup_service_source
                and "MINIMUM_BACKUP_RESERVE_BYTES = 64 * 1024 * 1024" in backup_capacity_source
                and "BACKUP_RESERVE_PERCENT = 10" in backup_capacity_source
                and "backup_reserve_bytes" in dufang_storage_audit
                and "restore_plan" in backup_service_source
                and "except BackupCapacityError as exc" in backup_web_source
                and '"insufficient_storage"' in backup_capacity_source
            ),
            "payout_result_is_read_only": "DatabaseUnitOfWork(self.game_database, read_only=True)" in dufang_payout_repository,
            "sharing_preference_commands_use_application": (
                "dufang_application.set_sharing_enabled(" in dufang_facade
                and "dufang_application.sharing_enabled(" in dufang_facade
                and "dufang_application.sharing_user_ids(" in dufang_facade
                and "_player_data_manager().get_field_data(\"global\", \"unseal_sharing\"" not in dufang_facade
                and "_player_data_manager().update_or_write_data(\"global\", \"unseal_sharing\"" not in dufang_facade
            ),
            "sharing_preferences_repository_is_schema_owned": (
                "DufangSharingPreferencesSqlRepository" in dufang_repository
                and "CREATE TABLE" not in dufang_sharing_preferences_repository
                and "ALTER TABLE" not in dufang_sharing_preferences_repository
            ),
            "sharing_preferences_migration_registered_and_player_db_only": (
                "def apply_dufang_sharing_preferences(" in dufang_migrations
                and "legacy.dufang.007" in legacy_migrated_source
                and "legacy.dufang.007" in plugin_source
                and "legacy.dufang.007" in plugin_source[plugin_source.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):]
            ),
            "legacy_stats_import_application_owned_and_idempotent": (
                "dufang_application.import_legacy_player_stats(" in dufang_facade
                and "def import_legacy_player_stats(" in dufang_application
                and "def import_legacy_snapshot(" in dufang_player_stats_repository
                and "INSERT OR IGNORE INTO unseal_data" in dufang_player_stats_repository
            ),
            "legacy_sync_command_keeps_file_io_off_event_loop": (
                "await asyncio.to_thread(\n        _migrate_unseal_data_sync" in dufang_facade
                and "dufang_application.import_sharing_users(" in dufang_facade
                and "dufang_application.import_legacy_player_stats(" in dufang_facade
            ),
            "status": "sharing_preferences_and_legacy_import_application_owned_with_share_bet_recovery_and_storage_capacity_gates",
        },
        "fusion": {
            "single_application_owned": "fusion_application.apply(" in fusion_facade,
            "legacy_single_disabled": "_fusion_service().apply(" not in fusion_facade,
            "batch_application_owned": "fusion_application.apply_batch(" in fusion_facade,
            "legacy_batch_disabled": "_fusion_service().apply_batch(" not in fusion_facade,
            "item_catalog_lazy": (
                "_items_instance = None" in fusion_facade
                and "def _items(" in fusion_facade
                and "items = Items()" not in fusion_facade
                and "_items().get_data_by_item_name(" in fusion_facade
            ),
            "legacy_service_factory_removed": "def _fusion_service(" not in fusion_facade,
            "repository_transaction_owned": (
                "from .settlement_repository import FusionService" in fusion_repository
                and "xiuxian_fusion.fusion_service" not in fusion_repository
            ),
            "request_path_has_no_ddl": "CREATE TABLE" not in fusion_settlement_repository and "ALTER TABLE" not in fusion_settlement_repository,
            "operation_schema_migration_owned": all(
                table in fusion_migrations for table in ("fusion_operations", "fusion_batch_operations")
            ) and "fusion.002" in plugin and "apply_fusion_operations" in plugin,
            "legacy_import_identity_preserved": (
                "FusionService as _FeatureFusionService" in fusion_compatibility
                and "class FusionService(_FeatureFusionService)" in fusion_compatibility
            ),
            "status": "single_batch_fusion_cutover",
        },
        "title": {
            "equip_replay_application_owned": "title_application.get_result(" in title_facade,
            "legacy_equip_replay_disabled": "# 先回放：成功后 equipped 变化会挡住同事件幂等。\n    prior = _title_transaction_service().get_result(" not in title_facade,
            "legacy_unequip_replay_disabled": "operation_id = _title_operation_id(event, \"unequip\", str(user_id))\n    prior = _title_transaction_service().get_result(" not in title_facade,
            "unlock_batch_application_owned": "title_application.execute(" in (PACKAGE / "xiuxian" / "xiuxian_title" / "title_data.py").read_text(encoding="utf-8"),
            "legacy_unlock_batch_disabled": "_title_transaction_service().unlock_batch(" not in (PACKAGE / "xiuxian" / "xiuxian_title" / "title_data.py").read_text(encoding="utf-8"),
            "title_state_read_application_owned": (
                "title_application.get_state(" in title_state_readers
                and "def get_state(" in title_application_source
                and "def get_state(" in title_repository_state_reader
            ),
            "title_state_read_legacy_disabled": "xiuxian2_handle" not in title_state_readers and "player_data_manager" not in title_state_readers,
            "title_state_read_has_no_ddl": "CREATE TABLE" not in title_repository_state_reader and "ALTER TABLE" not in title_repository_state_reader,
            "title_state_and_replay_reads_use_read_only_uow": (
                "DatabaseUnitOfWork(self.database, read_only=True)" in title_application_source
                and title_application_source.count("DatabaseUnitOfWork(self.database, read_only=True)") >= 2
            ),
            "title_condition_read_application_owned": (
                "title_eligibility_application.read_snapshot(" in title_condition_readers
                and "class TitleEligibilityApplication" in title_eligibility_source
                and "class TitleEligibilitySqlRepository" in title_eligibility_source
                and "title_eligibility_application = TitleEligibilityApplication(" in title_handler_facade_source
            ),
            "title_condition_reads_batch_one_snapshot_per_projection": (
                title_condition_readers.count(
                    "snapshot = _read_condition_snapshot(str(user_id), all_conditions)"
                ) == 2
                and "_condition_progress(conditions_by_title[str(title_id)], snapshot)" in title_condition_readers
                and "_condition_matches(conditions, snapshot)" in title_condition_readers
            ),
            "title_condition_reads_are_read_only_and_legacy_manager_free": (
                "DatabaseUnitOfWork(self.game_database, read_only=True)" in title_eligibility_source
                and "DatabaseUnitOfWork(self.player_database, read_only=True)" in title_eligibility_source
                and "CREATE TABLE" not in title_eligibility_source
                and "ALTER TABLE" not in title_eligibility_source
                and all(token not in title_condition_readers for token in (
                    "get_statistics_data", "player_data_manager", "sql_message"
                ))
            ),
            "title_check_commands_reuse_condition_snapshot": (
                "snapshot = get_title_condition_snapshot(user_id)" in title_handler_facade_source
                and "user_id, snapshot=snapshot, unlocked_title_ids=expected" in title_handler_facade_source
                and title_handler_facade_source.count(
                    "user_id, snapshot=snapshot, unlocked_title_ids=unlocked"
                ) == 2
            ),
            "mentor_title_grant_application_owned": (
                "TitleApplication" in partner_facade
                and "_mentor_title_application().grant(" in partner_facade
            ),
            "mentor_title_state_read_application_owned": "_mentor_title_application().get_state(" in partner_facade,
            "legacy_mentor_title_writer_disabled": "update_or_write_data(" not in partner_facade,
            "status": "equip_unequip_unlock_and_mentor_grant_cutover_with_shared_condition_snapshot",
        },
        "info": {
            "dynamic_attribute_application_owned": (
                "class PlayerAttributeApplication" in info_attribute_application
                and "get_player_attributes(" in info_utils
                and "configure_player_attribute_application" in plugin
                and "player_attributes" in plugin
                and "get_final_attributes" not in info_user_handler
                and "get_final_attributes" not in buff_handler
                and "get_final_attributes" not in player_fight
                and "get_final_attributes" not in json_config
                and "get_user_real_info(" not in buff_handler
                and "get_player_attributes(" in buff_handler
            ),
            "dynamic_attribute_compatibility_explicit": (
                "legacy_player_attributes" in info_attribute_application
                and "legacy_get_final_attributes" in info_attribute_compatibility
                and "CREATE TABLE" not in info_attribute_compatibility
                and "ALTER TABLE" not in info_attribute_compatibility
            ),
            "dynamic_attribute_provider_ports_preserved": (
                "**providers: Any" in info_attribute_application
                and "ratio=ratio" in info_attribute_application
                and "include_current=include_current" in info_attribute_application
            ),
            "status": "dynamic_attribute_read_boundary_owned_with_legacy_formula_adapter",
        },
        "player_state": {
            "battle_vital_write_application_owned": (
                "_player_state().update_vitals(" in player_fight
                and "def update_all_user_status(" in player_fight
            ),
            "battle_vital_legacy_writer_disabled": all(
                token not in player_fight
                for token in ("_sql_message", "XiuxianDateManage", "update_user_hp_mp(")
            ),
            "battle_vital_repository_is_bounded_and_no_ddl": (
                "ORDER BY rowid ASC LIMIT 1" in player_state_repository
                and "UPDATE user_xiuxian SET hp=?,mp=?" in player_state_repository
                and "CREATE TABLE" not in player_state_repository
                and "ALTER TABLE" not in player_state_repository
            ),
            "battle_vital_schema_failure_is_observable": (
                'logger.warning("战斗后的玩家气血状态未更新: {}", result.status)' in player_fight
                and "fallback=" not in player_fight
            ),
            "status": "battle_vital_write_application_owned_without_legacy_fallback",
        },
        "base": {
            "reward_service_legacy_connection_lazy": (
                "self._sql_message_instance = None" in reward_service_source
                and "self.sql_message = XiuxianDateManage()" not in reward_service_source
                and "def _sql_message(self)" in reward_service_source
            ),
            "reward_service_item_catalog_lazy": (
                "self._items_instance = None" in reward_service_source
                and "self.items = Items()" not in reward_service_source
                and "reward_items = reward.get(\"items\", []) or []" in reward_service_source
                and "def _items(self)" in reward_service_source
            ),
            "wishing_stone_inventory_application_owned": (
                "PlayerInventoryApplication" in wishing_stone_writer
                and "require_full=True" in wishing_stone_writer
            ),
            "wishing_stone_legacy_writer_disabled": "sql_message.send_back(" not in wishing_stone_writer,
            "refund_stamina_application_owned": (
                all("restore_player_stamina(" in source for source in (tower_facade, breakthrough_facade, activity_boss_entry))
                and all("_sql_message().update_user_stamina(" not in source for source in (tower_facade, breakthrough_facade, activity_boss_entry))
                and "def restore(" in base_stamina_application
                and "def restore(" in base_stamina_repository
            ),
            "cooldown_stamina_application_owned": (
                "consume_player_stamina(" in layout_source
                and "_sql_message().update_user_stamina(" not in layout_source
                and "class PlayerStaminaApplication" in base_stamina_application
            ),
            "cooldown_stamina_request_path_has_no_ddl": (
                "CREATE TABLE" not in base_stamina_repository
                and "ALTER TABLE" not in base_stamina_repository
            ),
            "cooldown_stamina_runtime_wired": (
                '"player_stamina": PlayerStaminaApplication' in plugin
                and "configure_player_stamina_application" in plugin
            ),
            "stamina_recovery_application_owned": (
                "recover_player_stamina(" in layout_source
                and "def recover(" in base_stamina_application
                and "def recover(" in base_stamina_repository
                and "update_all_users_stamina(" not in layout_source
                and "XiuxianDateManage" not in layout_source
            ),
            "stamina_recovery_request_path_has_no_ddl": (
                "CREATE TABLE" not in base_stamina_repository
                and "ALTER TABLE" not in base_stamina_repository
                and "ORDER BY rowid LIMIT ?" in base_stamina_repository
                and "batch_size" in base_stamina_repository
            ),
            "rename_application_owned": "base_application.rename(" in base_facade,
            "legacy_rename_disabled": "_player_rename_service().rename_user(" not in base_facade and "_player_rename_service().rename_root(" not in base_facade,
            "rename_replay_read_application_owned": "base_application.get_rename_result(" in base_facade and "def get_rename_result(" in base_application_source and "_player_rename_service" not in base_facade,
            "rename_request_path_has_no_ddl": "CREATE TABLE" not in base_rename_repository and "ALTER TABLE" not in base_rename_repository,
            "rename_startup_migration_registered": 'Migration("base.002", "player_rename_operations", apply_base_player_rename_operations)' in plugin and "def apply_base_player_rename_operations(" in base_migrations,
            "rename_migration_game_only": (
                "base.002" not in plugin[
                    plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")
                ]
                and "base.002" not in plugin[
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")
                ]
                and "base.002" not in plugin[
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("def migrations_for_database")
                ]
            ),
            "rename_service_isolated": "class PlayerRenameService" not in legacy_transaction and (PACKAGE / "compatibility" / "legacy_base_player_rename.py").is_file(),
            "stone_contest_service_isolated": "class StoneContestService" not in base_transaction and "class StoneContestService" in stone_contest_compatibility and "stone_contest_operations" in stone_contest_compatibility,
            "stone_contest_default_application_owned": (
                "BaseStoneContestSqlRepository" in base_application_source
                and "self._stone_contest_repository.transfer(" in base_application_source
                and "StoneContestService" not in base_application_source
            ),
            "stone_contest_web_route_application_owned": (
                "@router.post(\"/api/v1/base/stone_contest\")" in (PACKAGE / "adapters" / "web" / "blueprints" / "base.py").read_text(encoding="utf-8")
                and "application.stone_contest(" in (PACKAGE / "adapters" / "web" / "blueprints" / "base.py").read_text(encoding="utf-8")
            ),
            "stone_contest_request_path_has_no_ddl": (
                "CREATE TABLE" not in base_contest_repository
                and "ALTER TABLE" not in base_contest_repository
            ),
            "stone_contest_startup_migration_registered": (
                'Migration("base.003", "stone_contest_operations", apply_base_stone_contest_operations)' in plugin
                and "def apply_base_stone_contest_operations(" in base_migrations
            ),
            "stone_theft_default_application_owned": (
                "base_application.get_stone_theft_result(" in base
                and "base_application.settle_stone_theft(" in base
                and "_stone_contest_service(" not in base
                and "StoneContestService" not in base
            ),
            "stone_theft_request_path_has_no_ddl": (
                "CREATE TABLE" not in base_theft_repository
                and "ALTER TABLE" not in base_theft_repository
            ),
            "stone_theft_startup_migration_registered": (
                'Migration("base.003", "stone_contest_operations", apply_base_stone_contest_operations)' in plugin
                and "def apply_base_stone_contest_operations(" in base_migrations
            ),
            "stone_theft_migration_game_only": (
                all(
                    "base.003" not in plugin[start:end]
                    for start, end in (
                        (plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"), plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")),
                        (plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"), plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")),
                        (plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS"), plugin.index("def migrations_for_database")),
                    )
                )
            ),
            "stone_robbery_default_application_owned": (
                "base_application.get_stone_robbery_result(" in base
                and "base_application.settle_stone_robbery(" in base
                and "_stone_robbery_service(" not in base
                and "StoneRobberySettlementService" not in base
            ),
            "stone_robbery_request_path_has_no_ddl": (
                "CREATE TABLE" not in base_robbery_repository
                and "ALTER TABLE" not in base_robbery_repository
            ),
            "stone_robbery_startup_migrations_registered": (
                'Migration("base.004", "stone_robbery_operations", apply_base_stone_robbery_operations)' in plugin
                and 'Migration("base.005", "stone_robbery_player_statistics", apply_base_stone_robbery_player_statistics)' in plugin
                and "def apply_base_stone_robbery_operations(" in base_migrations
                and "def apply_base_stone_robbery_player_statistics(" in base_migrations
            ),
            "stone_robbery_migrations_routed_game_and_player": (
                "base.004" not in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")]
                and "base.004" not in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")]
                and "base.005" in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")]
                and "base.005" in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")]
            ),
            "stone_robbery_service_isolated": "class StoneRobberySettlementService" not in base_transaction and "class StoneRobberySettlementService" in stone_robbery_compatibility and "stone_robbery_operations" in stone_robbery_compatibility,
            "root_reroll_default_application_owned": (
                "base_application.reroll_root(" in base_root_reroll_handler
                and "base_application.get_root_reroll_result(" in base_root_reroll_handler
                and "ramaker(" not in base_root_reroll_handler
                and base_root_reroll_handler.index("get_root_reroll_result(")
                < base_root_reroll_handler.index("if user_info['stone'] < XiuConfig().remake")
            ),
            "root_reroll_repository_has_no_request_ddl": (
                "CREATE TABLE" not in base_root_reroll_repository
                and "ALTER TABLE" not in base_root_reroll_repository
                and "immediate=True" in base_root_reroll_repository
            ),
            "root_reroll_startup_migration_registered": (
                'Migration("base.008", "player_root_reroll_operations", apply_base_root_reroll_operations)' in plugin
                and "def apply_base_root_reroll_operations(" in base_migrations
            ),
            "root_reroll_migration_game_only": (
                "base.008" not in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")]
                and "base.008" not in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")]
            ),
            "xiangyuan_service_isolated": "class XiangyuanSettlementService" not in base_transaction and "class XiangyuanSettlementService" in xiangyuan_compatibility and "xiangyuan_create_operations" in xiangyuan_compatibility and "xiangyuan_claim_operations" in xiangyuan_compatibility,
            "xiangyuan_default_application_owned": (
                "XiangyuanApplication" in xiangyuan_facade
                and "_xiangyuan_settlement_service().create(" in xiangyuan_facade
                and "XiangyuanSettlementService" not in xiangyuan_facade
                and "stone_limit" not in xiangyuan_facade
            ),
            "xiangyuan_request_path_has_no_ddl": (
                "CREATE TABLE" not in base_xiangyuan_repository
                and "ALTER TABLE" not in base_xiangyuan_repository
            ),
            "xiangyuan_startup_migrations_registered": (
                'Migration("base.006", "xiangyuan_projection", apply_base_xiangyuan)' in plugin
                and 'Migration("base.007", "xiangyuan_player_limits", apply_base_xiangyuan_player)' in plugin
                and "def apply_base_xiangyuan(" in base_migrations
                and "def apply_base_xiangyuan_player(" in base_migrations
            ),
            "xiangyuan_migrations_routed_game_and_player": (
                "base.006" not in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")]
                and "base.006" not in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")]
                and "base.007" in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")]
                and "base.007" in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")]
            ),
            "xiangyuan_application_repository_owned": (
                "class XiangyuanApplication" in base_xiangyuan_application
                and "class XiangyuanSqlRepository" in base_xiangyuan_repository
            ),
            "breakthrough_service_isolated": "class BreakthroughService" not in base_transaction and "class BreakthroughService" in breakthrough_compatibility and all(token in breakthrough_compatibility for token in ("direct_breakthrough_operations", "continuous_breakthrough_operations", "tribulation_breakthrough_operations", "continuous_tribulation_operations")),
            "direct_breakthrough_handler_feature_owned": (
                "application.resolve_direct_breakthrough(" in breakthrough_facade[
                    breakthrough_facade.index("@level_up_zj.handle"):
                    breakthrough_facade.index("@level_up_lx.handle")
                ]
                and "_breakthrough_service().apply_failure(" not in breakthrough_facade[
                    breakthrough_facade.index("@level_up_zj.handle"):
                    breakthrough_facade.index("@level_up_lx.handle")
                ]
                and "_breakthrough_service().apply_success(" not in breakthrough_facade[
                    breakthrough_facade.index("@level_up_zj.handle"):
                    breakthrough_facade.index("@level_up_lx.handle")
                ]
                and "BaseDirectBreakthroughSqlRepository" in base_direct_breakthrough_repository
            ),
            "direct_breakthrough_startup_migration_registered": (
                'Migration("base.009", "direct_breakthrough_operations", apply_base_direct_breakthrough_operations)' in plugin
                and "def apply_base_direct_breakthrough_operations(" in base_migrations
                and 'migration_version="base.012"' in base_manifest
                and 'configure_direct_breakthrough_application(context.services["base"])' in plugin_source
            ),
            "direct_breakthrough_request_path_has_no_ddl": (
                "CREATE TABLE" not in base_direct_breakthrough_repository
                and "ALTER TABLE" not in base_direct_breakthrough_repository
                and "schema_missing" in base_direct_breakthrough_repository
            ),
            "direct_breakthrough_effects_replay_owned": (
                "effects_event_id" in base_direct_breakthrough_repository
                and "domain_outbox" in base_direct_breakthrough_repository
                and "plan_factory(uow" in base_direct_breakthrough_repository
                and "application.direct_breakthrough_replay(operation_id, user_id)" in breakthrough_facade
                and "record_level_up_result(" not in breakthrough_facade[
                    breakthrough_facade.index("@level_up_zj.handle"):breakthrough_facade.index("@level_up_lx.handle")
                ]
                and "trigger_breakthrough_relation_rewards(" not in breakthrough_facade[
                    breakthrough_facade.index("@level_up_zj.handle"):breakthrough_facade.index("@level_up_lx.handle")
                ]
                and "self.statistics.record(" in base_direct_breakthrough_effects
                and "direct_breakthrough_relation_receipts" in base_direct_breakthrough_relations
                and "self._finalize(event_id, payload)" in base_direct_breakthrough_relations
                and "ATTACH" not in base_direct_breakthrough_relations
            ),
            "direct_breakthrough_effects_runtime_and_cli_owned": (
                '"base.direct_breakthrough.effects": context.services["base"].reconcile_direct_breakthrough_event' in plugin
                and '"base.direct_breakthrough.effects": base.reconcile_direct_breakthrough_event' in cli_source
                and "application.resume_pending_direct_breakthroughs(limit=5)" in breakthrough_facade
                and "ORDER BY attempts,created_at,event_id LIMIT ?" in base_application_source
            ),
            "direct_breakthrough_effects_migrations_routed": (
                'Migration("base.010", "direct_breakthrough_plans", apply_base_direct_breakthrough_plans)' in plugin
                and 'Migration("base.011", "direct_breakthrough_player", apply_base_direct_breakthrough_player)' in plugin
                and '"base.011"' in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")]
                and '"base.011"' in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")]
                and '"base.010"' not in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")]
            ),
            "ordinary_tribulation_service_isolated": "class OrdinaryTribulationService" not in base_transaction and "class OrdinaryTribulationService" in ordinary_tribulation_compatibility and "ordinary_tribulation_operations" in ordinary_tribulation_compatibility,
            "destiny_tribulation_service_isolated": "class DestinyTribulationService" not in base_transaction and "class DestinyTribulationService" in destiny_tribulation_compatibility and "destiny_tribulation_operations" in destiny_tribulation_compatibility,
            "heart_devil_tribulation_service_isolated": "class HeartDevilTribulationService" not in base_transaction and "class HeartDevilTribulationService" in heart_devil_tribulation_compatibility and "heart_devil_tribulation_operations" in heart_devil_tribulation_compatibility,
            "pill_fusion_service_isolated": "class PillFusionService" not in base_transaction and "class PillFusionService" in pill_fusion_compatibility and "pill_fusion_operations" in pill_fusion_compatibility,
            "tribulation_state_migration_service_isolated": "class TribulationStateMigrationService" not in base_transaction and "class TribulationStateMigrationService" in tribulation_state_migration_compatibility and "tribulation_state_migration_operations" in tribulation_state_migration_compatibility,
            "status": "direct_breakthrough_core_and_effects_owned_with_other_base_compatibility_remaining",
        },
        "puppet": {
            "purchase_application_owned": (
                "def purchase(" in puppet_application_source
                and "PuppetPurchaseSqlRepository" in puppet_application_source
                and ".purchase(request.operation_id" in puppet_application_source
            ),
            "upgrade_application_owned": (
                "def upgrade(" in puppet_application_source
                and "PuppetPurchaseSqlRepository" in puppet_application_source
                and ".upgrade(\n                request.operation_id" in puppet_application_source
            ),
            "harvest_application_owned": "puppet_application.harvest(" in puppet_facade,
            "legacy_harvest_disabled": "_puppet_harvest_service().harvest(" not in puppet_facade,
            "default_application_has_no_legacy_repository": (
                "LegacyPuppetRepository" not in puppet_facade
                and "LegacyPuppetRepository" not in plugin_source
            ),
            "status_application_owned": (
                "puppet_application.set_enabled(" in puppet_facade
                and "puppet_application.get_status(" in puppet_facade
            ),
            "legacy_status_io_disabled": all(
                token not in puppet_facade
                for token in (
                    "_sql_message().set_puppet_status(",
                    "_sql_message().check_puppet_status(",
                    "_sql_message().get_all_enabled_puppets(",
                )
            ),
            "scheduler_enabled_users_bounded": (
                "puppet_application.enabled_user_high_watermark()" in puppet_facade
                and "puppet_application.list_enabled_users(" in puppet_facade
                and "limit=200" in puppet_facade
                and "get_all_enabled_puppets()" not in puppet_facade
            ),
            "status_schema_startup_migration_registered": (
                'Migration("puppet.002", "puppet_status_column", apply_puppet_status)' in plugin_source
                and "def apply_puppet_status(" in puppet_migrations_source
                and "ALTER TABLE user_xiuxian ADD COLUMN puppet_status" in puppet_migrations_source
            ),
            "status_repository_has_no_request_ddl_and_uses_ledger": (
                "OperationLedger" in puppet_status_repository_source
                and "operation_audit" in puppet_status_repository_source
                and "DatabaseUnitOfWork(self.database, immediate=True)" in puppet_status_repository_source
                and "CREATE TABLE" not in puppet_status_repository_source
                and "ALTER TABLE" not in puppet_status_repository_source
                and "LIMIT ?" in puppet_status_repository_source
            ),
            "status_replay_conflict_and_rollback_covered": (
                "operation_conflict" in puppet_status_repository_source
                and "fail_puppet_status_audit" in puppet_status_tests
                and "apply_puppet_status(uow)" in puppet_status_tests
            ),
            "status": "purchase_upgrade_status_and_harvest_default_feature_owned_with_bounded_scheduler_reads",
        },
        "pet": {
            "active_switch_application_owned": "pet_application.switch(" in pet_facade,
            "legacy_active_switch_disabled": "_pet_active_switch_service().switch(" not in pet_facade,
            "skill_replace_application_owned": "pet_application.skill_replace(" in pet_facade,
            "legacy_skill_replace_disabled": "_pet_skill_replace_service().replace(" not in pet_facade,
            "pet_transaction_compatibility_isolated": (
                "class PetTravelClaimService" not in (PACKAGE / "xiuxian" / "xiuxian_pet" / "transaction_service.py").read_text(encoding="utf-8")
                and "class PetTravelClaimService" in (PACKAGE / "compatibility" / "legacy_pet_transactions.py").read_text(encoding="utf-8")
                and "transaction_service" not in pet_facade
            ),
            "status": "feature_owned_pet_actions_with_legacy_transaction_services_isolated",
        },
        "game_event_claims": {
            "pet_travel_claim_effects_outbox_owned": (
                "self.outbox.append(" in pet_repository_source
                and 'event_type="game_event.projection"' in pet_repository_source
                and "def apply_pet_travel_claim(" in pet_migrations_source
                and 'Migration("pet.004"' in plugin_source
            ),
            "pet_travel_claim_effects_dispatched_on_replay": (
                "self.game_event_effects.dispatch(" in pet_application_source
                and "pet_application.claim_travel(" in pet_facade
                and "update_statistics_value(user_id, \"宠物游历次数\")" not in pet_facade
                and "configure_pet_application(context.services[\"pet\"])" in plugin_source
            ),
            "claim_projection_receipts_and_migrations_owned": (
                "PRIMARY KEY(event_id,event_key)" in game_event_migrations_source
                and "INSERT INTO game_event_statistics_events" in game_event_statistics_source
                and "record_task_progress_event_strict(" in (PACKAGE / "xiuxian" / "xiuxian_utils" / "game_events.py").read_text(encoding="utf-8")
                and "activity_event_operations" in (PACKAGE / "features" / "activity" / "migrations.py").read_text(encoding="utf-8")
            ),
            "claim_projection_runtime_and_cli_reconcile_owned": (
                '"game_event.projection": game_event_effects.on_outbox_event' in plugin_source
                and '"game_event.projection": game_event_effects.on_outbox_event' in cli_source
            ),
            "claim_operation_recovery_runtime_and_cli_owned": (
                '"map.mission_claim": context.services["map"].reconcile_mission_claim_operation' in plugin_source
                and '"pet.travel_claim": context.services["pet"].reconcile_travel_claim_operation' in plugin_source
                and '"map.mission_claim": map_application.reconcile_mission_claim_operation' in cli_source
                and '"pet.travel_claim": pet_application.reconcile_travel_claim_operation' in cli_source
                and "def reconcile_mission_claim_operation(" in map_application
                and "def reconcile_travel_claim_operation(" in pet_application_source
            ),
            "claim_projection_migrations_routed": (
                'Migration("game_events.001"' in plugin_source
                and '"game_events.001"' in plugin_source
                and '"pet.004"' in plugin_source
            ),
            "status": "map_and_pet_claim_effects_outbox_idempotent_and_reconcilable",
        },
        "trade": {
            "xianshi_listing_application_owned": "trade_application.xianshi_list_items(" in xianshi_listing_handler and "class XianshiListingSqlRepository" in trade_xianshi_transactions,
            "xianshi_auto_listing_application_owned": "trade_application.xianshi_list_plan(" in xianshi_auto_listing_handler and "class XianshiPlanListingSqlRepository" in trade_plan_xianshi_transactions,
            "legacy_xianshi_auto_listing_disabled": "xianshi_repository.add_xianshi_plan_items(" not in xianshi_auto_listing_handler,
            "xianshi_fast_listing_application_owned": "trade_application.xianshi_list_items(" in xianshi_fast_listing_handler and "class XianshiListingSqlRepository" in trade_xianshi_transactions,
            "legacy_xianshi_fast_listing_disabled": "xianshi_repository.add_xianshi_items(" not in xianshi_fast_listing_handler,
            "xianshi_system_listing_application_owned": "trade_application.xianshi_list_system_item(" in xianshi_system_listing_handler and "def list_system_item(" in trade_xianshi_transactions,
            "legacy_xianshi_system_listing_disabled": "xianshi_repository.add_xianshi_item(" not in xianshi_system_listing_handler,
            "xianshi_name_removal_application_owned": "trade_application.xianshi_remove_by_name(" in xianshi_name_removal_handler and "class XianshiRemovalSqlRepository" in trade_xianshi_removal_transactions,
            "legacy_xianshi_name_removal_disabled": "xianshi_repository.remove_xianshi_by_name(" not in xianshi_name_removal_handler,
            "xianshi_admin_removal_application_owned": "trade_application.xianshi_remove_listing(" in xianshi_admin_removal_handler,
            "legacy_xianshi_admin_removal_disabled": "xianshi_repository.remove_xianshi_listing(" not in xianshi_admin_removal_handler,
            "xianshi_clear_application_owned": "trade_application.xianshi_clear_all(" in xianshi_clear_handler,
            "legacy_xianshi_clear_disabled": "xianshi_repository.clear_all_xianshi_listings(" not in xianshi_clear_handler,
            "purchase_application_owned": "trade_application.purchase(" in trade_facade,
            "legacy_purchase_disabled": "_xianshi_purchase_service().purchase(" not in trade_facade,
            "purchase_default_repository_direct": "self.xianshi_purchase_repository.purchase(" in trade_application_source and "xiuxian.xiuxian_trade.repository" not in trade_application_source,
            "purchase_compatibility_repository_direct": "XianshiPurchaseSqlRepository" in trade_feature_repository and ".purchase(" in trade_feature_repository and "XianshiPurchaseService" not in trade_feature_repository,
            "purchase_repository_feature_owned": "class XianshiPurchaseSqlRepository" in trade_xianshi_purchase_transactions and "DatabaseUnitOfWork(self.database, immediate=True)" in trade_xianshi_purchase_transactions,
            "xianshi_query_application_owned": "trade_application.xianshi_get_items(" in trade_facade and "XianshiQuerySqlRepository" in trade_application_source,
            "xianshi_query_repository_read_only": "DatabaseUnitOfWork(self.database, read_only=True)" in trade_xianshi_query_repository and "CREATE TABLE" not in trade_xianshi_query_repository,
            "legacy_xianshi_query_disabled": "xianshi_repository.get_xianshi_items(" not in trade_facade,
            "legacy_trade_repository_runtime_disabled": "from .repository import TradeRepository" not in trade_facade and "xianshi_repository = TradeRepository(" not in trade_facade,
            "legacy_xianshi_schema_adapter_explicit": "LegacyXianshiSchemaAdapter" in trade_facade and "from ..xiuxian.xiuxian_trade.repository import TradeRepository" in trade_xianshi_schema_adapter,
            "auction_compatibility_binding_feature_owned": "bind_auction_repository(_auction_bid_repository, _auction_session_service)" in trade_facade,
            "guishi_compatibility_repository_direct": "GuishiDepositSqlRepository(" in trade_feature_repository and "GuishiWithdrawSqlRepository(" in trade_feature_repository and "GuishiStoneService" not in trade_feature_repository,
            "guishi_legacy_service_isolated": "class GuishiStoneService" not in trade_auction_transactions and "class LegacyGuishiStoneService" in trade_legacy_guishi_compatibility,
            "guishi_legacy_getter_removed": "_guishi_stone_service" not in trade_facade and "GuishiStoneService" not in trade_facade,
            "guishi_deposit_application_owned": "trade_application.guishi_deposit(" in trade_deposit_handler,
            "legacy_guishi_deposit_disabled": "_guishi_stone_service().deposit(" not in trade_deposit_handler,
            "guishi_withdraw_application_owned": "trade_application.guishi_withdraw(" in trade_withdraw_handler,
            "legacy_guishi_withdraw_disabled": "_guishi_stone_service().withdraw(" not in trade_withdraw_handler,
            "guishi_qiugou_application_owned": "trade_application.guishi_qiugou(" in trade_qiugou_handler,
            "legacy_guishi_qiugou_disabled": "xianshi_repository.create_guishi_qiugou_order(" not in trade_qiugou_handler,
            "guishi_baitan_application_owned": "trade_application.guishi_baitan(" in trade_baitan_handler,
            "legacy_guishi_baitan_disabled": "xianshi_repository.create_guishi_baitan_order(" not in trade_baitan_handler,
            "guishi_cancel_qiugou_application_owned": "trade_application.guishi_cancel_qiugou(" in trade_cancel_qiugou_handler,
            "legacy_guishi_cancel_qiugou_disabled": "xianshi_repository.clear_guishi_qiugou_order(" not in trade_cancel_qiugou_handler,
            "guishi_cancel_baitan_application_owned": "trade_application.guishi_cancel_baitan(" in trade_cancel_baitan_handler,
            "legacy_guishi_cancel_baitan_disabled": "xianshi_repository.clear_expired_guishi_order(" not in trade_cancel_baitan_handler,
            "guishi_matching_application_owned": "trade_application.guishi_match(" in trade_facade and "xianshi_repository.match_guishi_orders(" not in trade_facade[trade_facade.index("async def process_guishi_transactions"):trade_facade.index("@scheduler.scheduled_job", trade_facade.index("async def process_guishi_transactions"))],
            "legacy_guishi_matching_disabled": "xianshi_repository.match_guishi_orders(" not in trade_facade[trade_facade.index("async def process_guishi_transactions"):trade_facade.index("@scheduler.scheduled_job", trade_facade.index("async def process_guishi_transactions"))],
            "guishi_expired_cleanup_application_owned": "trade_application.guishi_clear_expired_baitan(" in trade_expired_job,
            "legacy_guishi_expired_cleanup_disabled": "xianshi_repository.clear_expired_guishi_order(" not in trade_expired_job,
            "guishi_take_application_owned": "trade_application.guishi_take_stored_item(" in trade_take_handler,
            "legacy_guishi_take_disabled": "xianshi_repository.take_guishi_stored_item(" not in trade_take_handler,
            "status": "xianshi_query_purchase_listing_removal_legacy_schema_and_guishi_stone_cutover_with_other_trade_compatibility",
        },
        "auction": {
            "bid_application_owned": "AuctionBidSqlRepository" in auction_bid and "bid_application.place_bid(" in trade_auction_transactions and "auction_bid_application=_auction_bid_application" in trade_facade,
            "bid_effects_owned": "AuctionBidEffects" in auction_bid_application and "self.effects.on_bid(" in auction_bid_application and "self.outbox.append(" in auction_bid_application and "auction_bid_statistics_events" in auction_bid_statistics and "class LegacyAuctionBidEffects" in auction_compat_effects and "log_auction_bid_once" in auction_compat_effects and "LegacyAuctionBidEffects(get_paths().player_db)" in trade_facade and "LegacyAuctionBidEffects(str(context.database.path(\"player_db\")))" in web_app_source and "if bid_application is None and not bid_replayed:" in trade_auction_transactions,
            "bid_outbox_reconcile_owned": '"auction.bid.effects": context.services["auction"].reconcile_outbox_event' in plugin and 'handlers=getattr(context, "outbox_handlers", None)' in (PACKAGE / "adapters" / "web" / "blueprints" / "database.py").read_text(encoding="utf-8"),
            "bid_started_operation_recoverable": 'elif existing.status != "started"' in auction_bid_application and 'self.repository.place_auction_bid(' in auction_bid_application,
            "queue_application_owned": "AuctionQueueSqlRepository" in auction_queue,
            "queue_display_queries_application_owned": "get_player_items(" in auction_queue and "count_player_items(" in auction_queue and "read_only=True" in (PACKAGE / "features" / "auction" / "queue_repository.py").read_text(encoding="utf-8") and "_trade_manager().get_player_auction_items(" not in trade_facade,
            "legacy_queue_disabled": "_auction_queue_service().enqueue(" not in auction_queue_handlers and "_auction_queue_service().dequeue(" not in auction_queue_handlers,
            "session_start_application_owned": "AuctionSessionStartSqlRepository" in auction_start and "auction_session_start_application=_auction_session_start_application" in trade_facade and "start_application.start(" in trade_auction_transactions,
            "session_start_uses_settlement_application": "settlement_application.settle_active(" in trade_auction_transactions and "auction_settlement_application=_auction_settlement_application" in trade_facade,
            "settlement_application_owned": "AuctionSettlementSqlRepository" in auction_settlement,
            "settlement_effects_owned": "AuctionSettlementEffects" in auction_settlement and "self.effects.on_settlement(" in auction_settlement and "self.outbox.append(" in auction_settlement and "recover_started_operation" in auction_settlement and "auction_settlement_statistics_events" in auction_settlement_statistics and "safe_record_game_event" in auction_settlement_compat,
            "settlement_outbox_reconcile_owned": '"auction.settlement.effects": context.services["auction_settlement"].reconcile_outbox_event' in plugin and '"auction.settlement.effects": settlement.reconcile_outbox_event' in cli_source and 'outbox_handlers.setdefault("auction.settlement.effects", settlement.reconcile_outbox_event)' in web_app_source,
            "settlement_projection_ids_idempotent": "log_auction_event_once" in auction_settlement_compat and "skip_statistics\": True" in auction_settlement_compat and "season_rank_event_receipts" in (PACKAGE / "xiuxian" / "xiuxian_utils" / "season_rank_service.py").read_text(encoding="utf-8") and "event_id" in (PACKAGE / "xiuxian" / "xiuxian_utils" / "economy_log.py").read_text(encoding="utf-8"),
            "settlement_started_operation_recoverable": "recover_started_operation = True" in auction_settlement and 'status != "duplicate" or recover_started_operation' in auction_settlement,
            "settlement_compat_effects_disabled": "if settlement_application is not None:\n        logger.info(\"拍卖已结束，结算及副作用事件已提交！\")\n        return auction_results" in trade_auction_transactions,
            "legacy_settlement_disabled": "repository=LegacyAuctionSettlementRepository(" not in plugin,
            "trade_web_actions_application_owned": all(
                f"self.auction_{application}" in trade_application_source
                for application in ("queue", "session_start", "settlement")
            ) and all(
                f"self.auction_{application}." in trade_application_source
                for application in ("queue", "session_start", "settlement")
            ),
            "trade_web_finish_uses_settlement_ledger_directly": "if action == \"session_finish\" and self.repository is None:" in trade_application_source and "return self.auction_settlement.settle_active(" in trade_application_source,
            "trade_web_session_routes_admin": '"session_start": "admin"' in trade_web_source and '"session_finish": "admin"' in trade_web_source and '"admin" if action in {"session_start", "session_finish"}' in trade_manifest_source,
            "trade_web_real_route_replay_and_effects_covered": all(
                token in trade_web_test_source
                for token in (
                    "/api/v1/trade/enqueue",
                    "/api/v1/trade/dequeue",
                    "/api/v1/trade/session_start",
                    "/api/v1/trade/session_finish",
                    "finish_replay",
                    "self.effects.events",
                )
            ),
            "legacy_trade_auction_session_isolated": "AuctionSessionService" not in trade_feature_repository and "LegacyTradeAuctionSessionAdapter" in trade_feature_repository and "AuctionSessionService" in legacy_trade_auction_compatibility,
            "legacy_trade_auction_session_feature_owned": "AuctionSessionStartSqlRepository" in legacy_trade_auction_compatibility and "AuctionSettlementSqlRepository" in legacy_trade_auction_compatibility and "transaction_service" not in legacy_trade_auction_compatibility and "class AuctionSessionService" not in trade_auction_transactions,
            "legacy_settlement_adapter_feature_owned": "AuctionSettlementSqlRepository" in auction_settlement_source and "_auction_dependencies" not in auction_settlement_source and "transaction_service" not in auction_settlement_source,
            "display_queries_application_owned": "AuctionQuerySqlRepository" in auction_query and "AuctionQuerySqlRepository" in auction_query_repository and all(
                token in trade_facade for token in (
                    "_auction_query_application().get_current_auction(auction_id)",
                    "_auction_query_application().get_auction_history(auction_id)",
                    "_auction_query_application().get_current_auction()",
                )
            ) and "_auction_query_application().count_auction_history()" in trade_facade and "xianshi_repository.get_current_auction(auction_id)" not in trade_facade and "xianshi_repository.get_auction_history(auction_id)" not in trade_facade,
            "display_query_repository_does_not_own_ddl": "CREATE TABLE" not in auction_query_repository and "ensure_schema" not in auction_query_repository and "read_only=True" in auction_query_repository and "read_only: bool = False" in (PACKAGE / "infrastructure" / "database" / "uow.py").read_text(encoding="utf-8"),
            "scheduler_query_application_owned": "_auction_query_application().count_current_auctions()" in trade_facade and "xianshi_repository.get_current_auction()" not in trade_facade,
            "status": "bid_and_settlement_effects_owned_with_outbox_reconcile; trade_web_auction_actions_application_owned; display_queue_and_scheduler_queries_application_owned; explicit_auction_rollback_adapters_feature_owned",
        },
        "boss": {
            "reward_integral_application_owned": (
                "BossIntegralApplication" in reward_service_source
                and "self.boss_integral_application.grant_integral(" in reward_service_source
            ),
            "reward_integral_legacy_writer_disabled": "player_data_manager.update_or_write_data" not in reward_service_source,
            "manual_spawn_application_owned": "boss_application.spawn(" in boss_facade,
            "daily_limit_application_owned": "boss_application.reset_daily_limit(" in boss_facade,
            "manual_spawn_repository_owned": "WorldBossManualSpawnSqlRepository" in boss_application and "transaction_service" not in boss_application,
            "daily_limit_repository_owned": "WorldBossDailyLimitResetSqlRepository" in boss_application and "transaction_service" not in boss_application,
            "world_boss_request_path_has_no_ddl": "CREATE TABLE" not in boss_world_repository and "ALTER TABLE" not in boss_world_repository,
            "world_boss_player_migration_registered": "boss.004" in plugin and "apply_boss_player_schema" in boss_migrations,
            "full_refresh_application_owned": all(token in boss_application for token in ("def full_refresh_snapshot(", "def full_refresh_result(", "def full_refresh(")),
            "full_refresh_repository_owned": "WorldBossFullRefreshSqlRepository" in boss_application and "WorldBossFullRefreshResult" in boss_world_repository,
            "full_refresh_migration_registered": "boss.005" in plugin and "apply_boss_full_refresh_player_schema" in boss_migrations,
            "full_refresh_request_path_has_no_ddl": "CREATE TABLE" not in boss_world_repository and "ALTER TABLE" not in boss_world_repository,
            "full_refresh_replay_cas_and_rollback": all(token in boss_world_repository for token in ("operation_conflict", "session_changed", "CURRENT_TIMESTAMP", "world_boss_full_refresh_operations")),
            "full_refresh_legacy_service_not_default": "WorldBossFullRefreshService" not in boss_facade,
            "battle_application_owned": "boss_application.settle_compat(" in boss_facade,
            "legacy_battle_settlement_disabled": "_world_boss_battle_settlement_service().settle(" not in boss_facade,
            "player_state_composition_owner": (
                "PlayerStateApplication(get_paths().game_db)" in boss_facade
                and "def configure_player_state_application(" in boss_facade
                and "configure_boss_player_state_application(context.services[\"player_state\"])" in plugin
            ),
            "player_state_default_fails_closed_without_legacy_fallback": (
                "def _legacy_initialize_player_state(" in boss_facade
                and "fallback=" not in boss_facade[:boss_facade.index("def _legacy_initialize_player_state(")]
                and "initialize_if_empty(user_id)" in boss_facade
            ),
            "punishment_application_owned": "boss_application.punish(" in boss_facade and "boss_application.punishment_snapshot(" in boss_facade,
            "item_catalog_lazy": (
                "_items_instance = None" in boss_facade
                and "def _items(" in boss_facade
                and "items = Items()" not in boss_facade
                and "_items().get_data_by_item_id(" in boss_facade
            ),
            "weekly_purchase_snapshot_application_owned": (
                "def weekly_purchases(" in boss_application
                and "self._repository().weekly_purchases(" in boss_application
                and "def weekly_purchases(" in boss_weekly_snapshot_repository
            ),
            "weekly_purchase_snapshot_read_only_and_no_ddl": (
                "DatabaseUnitOfWork(self.player_database, read_only=True)" in boss_weekly_snapshot_repository
                and "boss_weekly_purchases" in boss_weekly_snapshot_repository
                and '"weekly_purchases" in boss_columns' in boss_weekly_snapshot_repository
                and "CREATE TABLE" not in boss_weekly_snapshot_repository
                and "ALTER TABLE" not in boss_weekly_snapshot_repository
            ),
            "weekly_purchase_handlers_use_feature_snapshot": (
                "boss_application.weekly_purchases(user_id)" in boss_shop_handler
                and "self.application.weekly_purchases(" in boss_purchase_application
                and "boss_purchase_command_application.execute(" in boss_purchase_handler
                and "boss_limit.get_weekly_purchases(" not in boss_shop_handler + boss_purchase_handler
                and "boss_limit._load_data(" not in boss_purchase_handler
            ),
            "weekly_purchase_snapshot_and_write_share_schema_selection": (
                '"weekly_purchases" in boss_columns' in boss_purchase_write_repository
                and 'weekly_table = "boss_weekly_purchases"' in boss_purchase_write_repository
                and "INSERT INTO player_data.boss_weekly_purchases" in boss_purchase_write_repository
                and "ON CONFLICT(user_id) DO UPDATE SET weekly_purchases=excluded.weekly_purchases" in boss_purchase_write_repository
            ),
            "weekly_purchase_legacy_row_initialized_only_on_success": (
                'boss = boss or {"weekly": "{}"}' in boss_purchase_write_repository
                and "INSERT INTO player_data.boss(user_id,weekly_purchases)" in boss_purchase_write_repository
            ),
            "purchase_command_application_owned": (
                "class BossPurchaseCommandApplication" in boss_purchase_application
                and "self.repository.receipt(" in boss_purchase_application
                and "self.application.purchase(" in boss_purchase_application
                and "requested_quantity=quantity" in boss_purchase_application
            ),
            "purchase_command_receipt_first_and_read_only": (
                boss_purchase_execute.index("self.repository.receipt(")
                < boss_purchase_execute.index("self.repository.profile(")
                < boss_purchase_execute.index("self._config(")
                and "read_only=True" in boss_purchase_command_repository
                and "CREATE TABLE" not in boss_purchase_command_repository
                and "ALTER TABLE" not in boss_purchase_command_repository
                and "operation_pending" in boss_purchase_command_repository
                and "receipt_invalid" in boss_purchase_command_repository
            ),
            "purchase_command_facade_uses_one_owner": (
                "boss_purchase_command_application.execute(" in boss_purchase_handler
                and "boss_application.purchase(" not in boss_purchase_handler
                and "boss_application.weekly_purchases(" not in boss_purchase_handler
                and "boss_integral_application.get_integral(" not in boss_purchase_handler
                and "_items()" not in boss_purchase_handler
            ),
            "purchase_command_replies_fail_closed": (
                "status not in {\"applied\", \"duplicate\"}" in boss_purchase_replies
                and "receipt_invalid" in boss_purchase_replies
                and "operation_pending" in boss_purchase_replies
            ),
            "purchase_command_ingress_covered": (
                "test_real_handler_clips_first_quantity" in (ROOT / "tests" / "test_boss_purchase_command_ingress.py").read_text(encoding="utf-8")
                and "test_sql_failure_rolls_back_assets" in (ROOT / "tests" / "test_boss_purchase_command_ingress.py").read_text(encoding="utf-8")
            ),
            "legacy_manual_spawn_disabled": "_spawn_world_boss(" not in boss_facade or "boss_application.spawn(" in boss_facade,
            "status": "purchase_command_manual_spawn_daily_limit_full_refresh_punishment_and_weekly_purchase_snapshot_cutover_with_other_boss_compatibility",
        },
        "buff": {
            "player_experience_normalization_owned": (
                "player_economy_application.normalize_experience(" in buff_normalize_experience_handler
                and "_sql_message().del_exp_decimal(" not in buff_normalize_experience_handler
            ),
            "blessed_open_application_owned": "buff_application.open(" in buff_facade,
            "legacy_blessed_open_disabled": "_blessed_spot_service().open(" not in buff_facade,
            "blessed_rename_application_owned": "buff_application.rename(" in buff_facade,
            "legacy_blessed_rename_disabled": "_blessed_spot_service().rename(" not in buff_facade,
            "blessed_upgrade_application_owned": "buff_application.upgrade_field(" in buff_facade,
            "stone_training_application_owned": "buff_application.stone_training(" in buff_facade,
            "training_lifecycle_application_owned": (
                "buff_application.training_start(" in buff_training_handler
                and "buff_application.training_complete(" in buff_training_handler
                and "_normal_training_lifecycle_service" not in buff_training_handler
                and "if not start_result.ok" in buff_training_handler
                and "if not result.ok" in buff_training_handler
                and "result.data" in buff_training_handler
            ),
            "training_lifecycle_request_path_has_no_ddl": all(
                token not in buff_training_start_repository + buff_training_complete_repository
                for token in ("CREATE TABLE", "ALTER TABLE")
            ),
            "training_lifecycle_startup_migrations_registered": (
                'Migration("buff.010", "normal_training_operations", apply_normal_training_game)' in plugin
                and 'Migration("buff.011", "normal_training_player_statistics", apply_normal_training_player)' in plugin
                and "def apply_normal_training_game(" in buff_migrations
                and "def apply_normal_training_player(" in buff_migrations
            ),
            "training_lifecycle_stats_and_weekly_task_owned": (
                "_increment_statistics(" in buff_training_complete_repository
                and "_increment_weekly_task(" in buff_training_complete_repository
                and '"weekly_out_closing"' in buff_training_complete_repository
                and "self.ledger.finish(uow, outcome)" in buff_application_source
                and "def _training_lifecycle_execute(" in buff_application_source
            ),
            "closing_enter_handler_application_owned": (
                "buff_application.closing_enter(" in buff_closing_enter_handler
                and "operation_id=_closing_enter_operation_id(event, user_id)" in buff_closing_enter_handler
                and "started_at=runtime_clock.now().strftime" in buff_closing_enter_handler
                and "_sql_message().in_closing(" not in buff_closing_enter_handler
                and "check_user_type(" not in buff_closing_enter_handler
                and "update_statistics_value(" not in buff_closing_enter_handler
            ),
            "closing_enter_operation_identity_event_stable": (
                'return f"buff-closing-enter:{event_id}:{user_id}"' in buff_facade
                and 'return f"buff-closing-enter:{user_id}:{runtime_ids.new_id()}"' in buff_facade
            ),
            "closing_enter_application_owns_attached_uow_and_ledger": (
                "def closing_enter(" in buff_application_source
                and "ClosingEnterSqlRepository(" in buff_application_source
                and "AttachedDatabaseUnitOfWork(" in buff_application_source
                and 'action = "buff.closing_enter"' in buff_application_source
                and "self.ledger.begin(uow, operation_id, action, payload)" in buff_application_source
                and "self.ledger.finish(uow, outcome)" in buff_application_source
                and "repository.enter_in_uow(" in buff_application_source
            ),
            "closing_enter_repository_cas_and_statistics_owned": (
                "class ClosingEnterSqlRepository" in buff_closing_enter_repository
                and "UPDATE user_cd SET type=1" in buff_closing_enter_repository
                and "COALESCE(type,0)=0" in buff_closing_enter_repository
                and 'INSERT INTO player_data.statistics(user_id,"闭关次数")' in buff_closing_enter_repository
                and "ON CONFLICT(user_id) DO UPDATE" in buff_closing_enter_repository
                and "INSERT INTO closing_enter_operations" in buff_closing_enter_repository
            ),
            "closing_enter_rejection_and_replay_statuses_explicit": (
                all(
                    f'ClosingEnterResult("{status}")' in buff_closing_enter_repository
                    for status in (
                        "schema_missing", "user_missing", "ineligible", "busy", "state_changed",
                    )
                )
                and "operation_conflict" in buff_application_source
                and "previous.replay()" in buff_application_source
                and 'result_status == "ineligible"' in buff_closing_enter_handler
                and 'result_status == "duplicate" or result.replayed' in buff_closing_enter_handler
            ),
            "closing_enter_request_path_has_no_ddl": all(
                token not in buff_closing_enter_repository
                for token in ("CREATE TABLE", "ALTER TABLE")
            ),
            "closing_enter_migrations_registered_and_routed": (
                'Migration("buff.012", "closing_enter_operations", apply_closing_enter_game)' in plugin
                and 'Migration("buff.013", "closing_enter_player_statistics", apply_closing_enter_player)' in plugin
                and "def apply_closing_enter_game(" in buff_migrations
                and "def apply_closing_enter_player(" in buff_migrations
                and '"buff.013"' in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")]
                and '"buff.013"' in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")]
            ),
            "closing_reward_handler_application_owned": (
                'out_closing = on_command("出关", aliases={"灵石出关"}' in buff_facade
                and "buff_application.calculate_closing_reward(" in buff_closing_handler
                and "buff_application.closing_settle(" in buff_closing_handler
                and "_closing_settlement_service().settle(" not in buff_closing_handler
            ),
            "closing_reward_snapshot_calculation_owned": (
                "class ClosingRewardSnapshot" in buff_closing_reward
                and "class ClosingRewardCalculator" in buff_closing_reward
                and "def calculate(" in buff_closing_reward
                and "def calculate_closing_reward(" in buff_application_source
            ),
            "closing_reward_replay_before_snapshot_reads": (
                buff_closing_handler.index("buff_application.closing_replay(")
                < buff_closing_handler.index("_sql_message().get_user_info_with_id(")
            ),
            "closing_reward_operation_identity_event_stable": (
                'return f"buff-closing-settle:{event_id}:{user_id}"' in buff_facade
                and 'return f"buff-closing-settle:{user_id}:{runtime_ids.new_id()}"' in buff_facade
            ),
            "closing_reward_stone_alias_uses_same_application_path": (
                'str(event.message) == "灵石出关"' in buff_closing_handler
                and "stone_exit=" in buff_closing_handler
                and "buff_application.calculate_closing_reward(" in buff_closing_handler
            ),
            "closing_reward_scope_backlog_and_virtual_world_excluded": all(
                token in (
                    (ROOT / "docs" / "features" / "buff.md").read_text(encoding="utf-8")
                    + (ROOT / "docs" / "full_refactor_progress.md").read_text(encoding="utf-8")
                    + (ROOT / "docs" / "refactor_slice_execution_protocol.md").read_text(encoding="utf-8")
                )
                for token in (
                    "scope_id=phase3-player-lifecycle-v2",
                    "buff-closing-settle:",
                    "虚神界出关",
                    "跨事务恢复",
                )
            ),
            "closing_reward_phase2_membership_unchanged": (
                "Phase 2 `496`" in (ROOT / "docs" / "refactor_slice_execution_protocol.md").read_text(encoding="utf-8")
                and "7787a74a3e0c15c220a5f257921693b14706ef23b224bac9f3dad55a1ff51fac"
                in (ROOT / "docs" / "refactor_slice_execution_protocol.md").read_text(encoding="utf-8")
            ),
            "closing_settlement_application_owned": "buff_application.closing_settle(" in buff_facade,
            "closing_settlement_replay_application_owned": (
                "buff_application.closing_replay(" in buff_facade
                and "_closing_settlement_service().get_result(" not in buff_facade
            ),
            "closing_settlement_request_path_has_no_ddl": (
                "CREATE TABLE" not in buff_closing_repository
                and "self.outbox.append(" in buff_closing_repository
            ),
            "closing_effects_runtime_and_cli_reconcile_owned": (
                '"buff.closing.effects": context.services["buff"].reconcile_outbox_event' in plugin
                and '"buff.closing.effects": buff.reconcile_outbox_event' in activity_cli
                and "def reconcile_outbox_event(" in buff_application_source
            ),
            "closing_effects_application_feature_owned": (
                "class ClosingEffectsApplication" in buff_closing_effects
                and "self.statistics.record(" in buff_closing_effects
                and "self._task_progress(" in buff_closing_effects
                and "self._activity_event(" in buff_closing_effects
            ),
            "closing_effects_default_runtime_feature_owned": (
                "from .features.buff.closing_effects_application import ClosingEffectsApplication" in plugin
                and "ClosingEffectsApplication(context.database.path(\"player_db\"))" in plugin
                and "from .features.buff.closing_effects_application import ClosingEffectsApplication" in cli_source
                and "ClosingEffectsApplication(player_db)" in cli_source
            ),
            "closing_effect_projections_have_stable_receipts": (
                "class ClosingStatisticsRepository" in (PACKAGE / "features" / "buff" / "closing_statistics.py").read_text(encoding="utf-8")
                and '"event_id": str(event_id)' in buff_closing_effects
                and "record_task_progress_event_strict(" in buff_closing_effects
                and "event_id=f\"{event_id}:activity:out_closing\"" in buff_closing_effects
                and "activity_event_operations" in activity_storage
            ),
            "closing_effects_legacy_receipts_remain_explicit": (
                "if not event_id" in buff_application_source
                and "if not event_id:" in buff_application_source
                and "Historical receipts predate the outbox" in buff_application_source
            ),
            "closing_effects_scope_backlog_and_virtual_world_excluded": all(
                token in (
                    (ROOT / "docs" / "features" / "buff.md").read_text(encoding="utf-8")
                    + (ROOT / "docs" / "full_refactor_progress.md").read_text(encoding="utf-8")
                    + (ROOT / "docs" / "refactor_slice_execution_protocol.md").read_text(encoding="utf-8")
                )
                for token in (
                    "scope_id=phase3-player-lifecycle-v3",
                    "command:buff:出关:effects-reconcile",
                    "虚神界出关",
                    "game CAS",
                    "跨库恢复",
                )
            ),
            "closing_effects_phase2_membership_unchanged": (
                "Phase 2 `496`" in (ROOT / "docs" / "refactor_slice_execution_protocol.md").read_text(encoding="utf-8")
                and "7787a74a3e0c15c220a5f257921693b14706ef23b224bac9f3dad55a1ff51fac"
                in (ROOT / "docs" / "refactor_slice_execution_protocol.md").read_text(encoding="utf-8")
            ),
            "closing_effects_migrations_routed": (
                '"buff.008"' in plugin
                and '"buff.009"' in plugin
                and '"activity_state.003"' in plugin
                and "def apply_closing_settlement_game(" in buff_migrations
                and "def apply_closing_effects_player(" in buff_migrations
                and "def apply_activity_event_receipts(" in activity_state_migrations
            ),
            "pvp_application_owned": "buff_application.pvp_settle(" in buff_facade,
            "pvp_repository_owned": "class NormalPvpSqlRepository" in pvp_repository,
            "pvp_request_path_has_no_ddl": "CREATE TABLE" not in pvp_repository,
            "pvp_default_legacy_service_disconnected": (
                "NormalPvpSettlementService" not in buff_facade
                and "buff_application.pvp_replay(" in buff_facade
                and "calculate_battle(" in buff_facade
            ),
            "pvp_migrations_game_and_player": (
                '"buff.006"' in plugin
                and '"buff.007"' in plugin
                and "def apply_normal_pvp_operations(" in buff_migrations
                and "def apply_normal_pvp_player_statistics(" in buff_migrations
            ),
            "partner_token_application_owned": (
                "_partner_token_application().apply(" in partner_facade
                and "PartnerTokenUseApplication" in partner_facade
            ),
            "legacy_partner_token_disabled": "_partner_token_service().apply(" not in partner_facade,
            "legacy_blessed_upgrade_disabled": "_blessed_spot_service().upgrade_field(" not in buff_facade,
            "status": "player_exp_normalization_blessed_spot_open_rename_upgrade_stone_training_lifecycle_closing_settlement_pvp_partner_token_partner_cultivation_cutover_with_other_buff_compatibility",
        },
        "partner_cultivation": {
            "identity_reads_application_owned": (
                "get_user_profile(" in partner_facade
                and "_sql_message().get_user_real_info(" not in partner_facade
            ),
            "application_owned": (
                "_partner_cultivation_application().apply(" in partner_facade
                and "class PartnerCultivationApplication" in partner_cultivation_application
            ),
            "repository_owned": "class PartnerCultivationSqlRepository" in partner_cultivation_repository,
            "legacy_default_disabled": "PartnerCultivationService" not in partner_facade,
            "usage_settled_atomically": (
                "expected_used_count_1=limt_1" in partner_facade
                and "used_count=used_count+?" in partner_cultivation_repository
                and "two_exp_cd.add_user(" not in partner_facade
            ),
            "migration_owned": (
                '"buff.004"' in plugin
                and '"buff.005"' in plugin
                and "def apply_partner_cultivation_operations(" in buff_migrations
                and "def apply_partner_cultivation_player_schema(" in buff_migrations
            ),
            "request_path_has_no_ddl": "CREATE TABLE" not in partner_cultivation_repository,
            "status": "cultivation_settlement_cutover_with_profile_identity_reads_and_legacy_transaction_service_retained_as_compatibility_reference",
        },
        "impart": {
            "love_sand_application_owned": "impart_application.love_sand(" in impart_facade,
            "card_compose_application_owned": "impart_application.compose(" in impart_facade,
            "card_disassemble_application_owned": "impart_application.disassemble(" in impart_facade,
            "prayer_application_owned": "impart_application.prayer_settle(" in impart_facade,
            "paid_draw_application_owned": "impart_application.draw(" in impart_facade,
            "crystal_draw_application_owned": "impart_application.crystal_draw(" in impart_facade,
            "paid_draw_pity_resets_after_a_hit": (
                "probability_wish = int(current_wish)" in impart_paid_draw_handler
                and '_rank_success({"wish": probability_wish})' in impart_paid_draw_handler
                and "probability_wish = 0" in impart_paid_draw_handler
            ),
            "draw_card_classification_is_transactional": (
                "existing_card_counts = self._card_counts(uow" in impart_draw_repository
                and '"传承新卡": len(new_cards)' in impart_draw_repository
                and '"传承重复卡": duplicate_count' in impart_draw_repository
            ),
            "draw_original_request_is_replay_checked": (
                "requested_pulls: int | None = None" in impart_draw_repository
                and "prior_requested != int(requested_pulls)" in impart_draw_repository
            ),
            "draw_request_paths_have_no_ddl": "CREATE TABLE" not in impart_draw_repository,
            "card_catalog_does_not_import_legacy_package": (
                "ast.literal_eval(statement.value)" in impart_catalog
                and '"xiuxian_impart" / "impart_all.py"' in impart_catalog
                and "from ...xiuxian.xiuxian_impart.impart_all" not in impart_catalog
            ),
            "draw_schema_migrations_are_database_routed": (
                'Migration("impart.006", "impart_draw_operations"' in plugin
                and 'Migration("impart.007", "impart_crystal_and_card_operations"' in plugin
                and 'Migration("impart.008", "impart_draw_player_statistics"' in plugin
                and '"impart.007"' in plugin[plugin.index("_IMPART_DATABASE_MIGRATION_VERSIONS"):]
                and '"impart.008"' in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):]
            ),
            "legacy_impart_data_initialization_is_lazy": (
                "def _impart_manager()" in impart_legacy_utils
                and "def _impart_data_manager()" in impart_legacy_utils
                and "xiuxian_impart = XIUXIAN_IMPART_BUFF()" not in impart_legacy_utils
                and "impart_data_json = IMPART_DATA()" not in impart_legacy_utils
            ),
            "prayer_stats_transaction_owned": (
                "impart_database=get_paths().impart_db" in impart_facade
                and "player_database=get_paths().player_db" in impart_prayer_handler
                and 'update_statistics_value(user_id, "祈愿石使用"' not in impart_prayer_handler
                and 'invalidate_player_data_cache("statistics"' in impart_prayer_handler
                and all(f'"{field}"' in impart_migrations for field in ("祈愿石使用", "传承新卡", "传承重复卡"))
            ),
            "prayer_request_path_has_no_ddl": "CREATE TABLE" not in impart_prayer_repository,
            "prayer_schema_migrations_owned": (
                'Migration("impart.002", "impart_prayer_operations"' in plugin
                and 'Migration("impart.003", "impart_prayer_player_statistics"' in plugin
                and "def apply_impart_prayer_operations(" in impart_migrations
                and "def apply_impart_prayer_player_statistics(" in impart_migrations
            ),
            "status": "read_draw_prayer_love_sand_card_mutations_and_startup_schema_feature_owned_with_remaining_legacy_impart_consumers",
        },
        "mixelixir": {
            "harvest_level_application_owned": "mixelixir_application.harvest_level_upgrade(" in mixelixir_facade,
            "legacy_harvest_level_disabled": "_mixelixir_harvest_level_upgrade_service().upgrade(" not in mixelixir_facade,
            "daily_reset_application_owned": (
                "def reset_daily_count(" in mixelixir_application
                and '_run_job("每日炼丹次数重置", _mixelixir_daily_count_reset)' in mixelixir_scheduler
                and "_sql_message().mixelixir_num_reset" not in mixelixir_scheduler
            ),
            "daily_reset_atomic_and_no_request_ddl": (
                "DatabaseUnitOfWork(self.database, immediate=True)" in mixelixir_daily_reset
                and "self.ledger.finish(uow, outcome)" in mixelixir_daily_reset
                and "CREATE TABLE" not in mixelixir_daily_reset
            ),
            "runtime_default_has_no_legacy_repository": "LegacyMixelixirRepository" not in plugin_source,
            "status": "harvest_level_upgrade_and_daily_reset_feature_owned_with_explicit_compatibility",
        },
        "dongfu": {
            "expansion_application_owned": "dongfu_application.expand(" in dongfu_facade,
            "expansion_slots_persisted_with_assets": (
                "SET plot_count=?,plant_slots=?,planting=?,plant_seed_id=?,plant_start=?,plant_finish=?" in dongfu_expansion_repository
                and "dongfu_expansion_operations" in dongfu_expansion_repository
                and "uow.savepoint(\"dongfu_expansion\")" in dongfu_expansion_repository
                and "normalize_plant_slots(" in dongfu_expansion_repository
                and "seed_names.get(seed_id" in dongfu_plant_slots
                and "seed_names={seed_id: conf[\"name\"] for seed_id, conf in SEED_CONFIG.items()}" in dongfu_facade
            ),
            "expansion_handler_has_no_legacy_writeback": (
                "@dongfu_expand.handle" in dongfu_facade
                and "_save_dongfu" not in dongfu_facade[
                    dongfu_facade.index("@dongfu_expand.handle") : dongfu_facade.index(
                        "@visit_friend.handle", dongfu_facade.index("@dongfu_expand.handle")
                    )
                ]
            ),
            "plant_application_owned": "dongfu_application.plant(" in dongfu_facade,
            "harvest_application_owned": "dongfu_application.harvest(" in dongfu_facade,
            "harvest_snapshot_application_owned": (
                "dongfu_application.prepare_harvest_snapshot(" in dongfu_facade
                and "def prepare_harvest_snapshot(" in dongfu_application
                and "def prepare_harvest_snapshot(" in dongfu_repository
                and "def prepare_snapshot(" in dongfu_operation_repositories[4]
                and "harvest_settlement=?" in dongfu_operation_repositories[4]
                and "operation_id = str(snapshot.get(\"operation_id\") or operation_id)" in dongfu_facade
            ),
            "harvest_effects_outbox_atomic": (
                "game_event.projection" in dongfu_operation_repositories[4]
                and "self.outbox.append(" in dongfu_operation_repositories[4]
                and "effects_event_id" in dongfu_application
                and "configure_dongfu_application(context.services[\"dongfu\"])" in plugin
                and "safe_record_game_event" not in dongfu_facade
            ),
            "visit_reward_payload_is_stable_and_replay_preserves_gain": (
                "_receipt(old['payload'],old['gain'])" in dongfu_visit_reward_repository
                and "INSERT INTO dongfu_visit_reward_operations(operation_id,payload,gain)" in dongfu_visit_reward_repository
                and "number_to(result.gain)" in dongfu_facade
                and "dongfu.005" in plugin
            ),
            "infiltration_plan_freezes_branch_and_operation_id": (
                "dongfu_application.infiltration_plan(" in dongfu_facade
                and "dongfu_application.prepare_infiltration_plan(" in dongfu_facade
                and "DongfuInfiltrationPlanSqlRepository" in dongfu_repository
                and "dongfu-infiltrate:{my_uid}:" in dongfu_infiltration_handler
                and 'operation_id = f"dongfu-infiltrate-success:' not in dongfu_infiltration_handler
                and 'operation_id = f"dongfu-infiltrate-failure:' not in dongfu_infiltration_handler
                and '"settlement": "failure" if failure else "success"' in dongfu_infiltration_handler
                and "_decode" in dongfu_infiltration_plan_repository
            ),
            "static_map_and_visit_profile_reads_are_feature_owned": (
                "MapStaticDataProvider(JsonDocumentReader(), MAP_FILE)" in dongfu_facade
                and "player_profile_application.get_user_profile_by_name(tname)" in dongfu_facade
                and "PlayerDataManager" not in dongfu_facade
                and "XiuxianDateManage" not in dongfu_facade
            ),
            "harvest_snapshot_handler_has_no_legacy_writeback": (
                "@dongfu_harvest.handle" in dongfu_facade
                and "_save_dongfu" not in dongfu_facade[
                    dongfu_facade.index("@dongfu_harvest.handle") : dongfu_facade.index(
                        "@dongfu_geomancy.handle", dongfu_facade.index("@dongfu_harvest.handle")
                    )
                ]
            ),
            "fertilize_application_owned": "dongfu_application.fertilize(" in dongfu_facade,
            "accelerate_application_owned": "dongfu_application.accelerate(" in dongfu_facade,
            "patrol_application_owned": "dongfu_application.patrol(" in dongfu_facade,
            "array_upgrade_application_owned": "dongfu_application.array_upgrade(" in dongfu_facade,
            "inventory_facade_has_no_direct_mutator": all(
                token not in dongfu_facade
                for token in ("def _consume_item(", "_sql_message().goods_num(", "_sql_message().update_back_j(")
            ),
            "infiltrate_success_application_owned": "dongfu_application.infiltrate_success(" in dongfu_settlement_helper,
            "legacy_infiltrate_success_disabled": "_dongfu_infiltrate_success_service().settle(" not in dongfu_settlement_helper and "_run_dongfu_action(" not in dongfu_settlement_helper,
            "infiltrate_failure_application_owned": "dongfu_application.infiltrate_failure(" in dongfu_settlement_helper,
            "legacy_infiltrate_failure_disabled": "_dongfu_infiltrate_failure_service().settle(" not in dongfu_settlement_helper and "_run_dongfu_action(" not in dongfu_settlement_helper,
            "legacy_infiltration_receipts_replayed_before_checks": (
                "dongfu_application.operation_receipt(legacy_action, legacy_operation_id)" in dongfu_infiltration_handler
                and "DongfuOperationReceiptSqlQueryRepository" in dongfu_operation_receipt_repository
            ),
            "legacy_transactions_isolated": (
                "transaction_service" not in dongfu_facade
                and all(
                    f"def {name}(" not in dongfu_facade
                    for name in (
                        "_dongfu_expansion_service",
                        "_dongfu_plant_service",
                        "_dongfu_accelerate_service",
                        "_dongfu_patrol_service",
                        "_dongfu_array_upgrade_service",
                        "_dongfu_visit_reward_service",
                        "_dongfu_infiltrate_failure_service",
                        "_dongfu_infiltrate_success_service",
                        "_dongfu_harvest_settlement_service",
                        "_dongfu_fertilize_service",
                    )
                )
                and "def _run_dongfu_action" not in dongfu_facade
            ),
            "legacy_transaction_compatibility_preserved": (
                "legacy_dongfu_transactions import *" in dongfu_legacy_shim
                and "class DongfuPlantService" in dongfu_compatibility
                and "class InfiltrateFailureService" in dongfu_compatibility
            ),
            "operation_request_paths_have_no_ddl": all(
                "CREATE TABLE" not in source
                and "operation_schema_ready" in source
                and "operation_databases_ready" in source
                for source in dongfu_operation_repositories
            ),
            "operation_schema_startup_migration_owned": (
                'Migration("dongfu.004", "dongfu_action_operations", apply_dongfu_operations)' in plugin
                and "def apply_dongfu_operations(" in dongfu_migrations
                and 'Migration("dongfu.005", "dongfu_event_replay_plans", apply_dongfu_event_replay)' in plugin
                and "def apply_dongfu_event_replay(" in dongfu_migrations
                and all(table in dongfu_operation_schema for table in (
                    "dongfu_accelerate_operations",
                    "dongfu_array_upgrade_operations",
                    "dongfu_expansion_operations",
                    "dongfu_fertilize_operations",
                    "dongfu_harvest_operations",
                    "dongfu_patrol_operations",
                    "dongfu_plant_operations",
                    "dongfu_visit_reward_operations",
                    "dongfu_infiltration_operations",
                ))
            ),
            "status_read_application_owned": "dongfu_application.status(" in dongfu_facade
            and "DongfuStatusSqlQueryRepository" in dongfu_status_repository,
            "named_target_feature_owned": (
                "dongfu_application.nearby_target(my_uid, tname)" in dongfu_facade
                and "def nearby_target(" in dongfu_application
                and "DongfuNearbyTargetSqlQueryRepository" in dongfu_repository
            ),
            "named_target_single_row_read_only": (
                "read_only=True" in dongfu_nearby_target_repository
                and "?mode=ro" in dongfu_nearby_target_repository
                and "ORDER BY nearby.rowid ASC LIMIT 1" in dongfu_nearby_target_repository
                and "query_all" not in dongfu_nearby_target_repository
                and "CREATE TABLE" not in dongfu_nearby_target_repository
                and "ALTER TABLE" not in dongfu_nearby_target_repository
            ),
            "legacy_named_target_list_disabled": (
                "def _get_same_node_users(" not in dongfu_facade
                and "nearby_users =" not in dongfu_facade
            ),
            "random_target_feature_owned": (
                "await dongfu_application.random_target(" in dongfu_facade
                and "await _get_random_dongfu_target(my_uid)" in dongfu_facade
                and "async def random_target(" in dongfu_application
                and "DongfuRandomTargetSqlQueryRepository" in dongfu_repository
            ),
            "random_target_bounded_short_read_only": (
                "CANDIDATE_PAGE_SIZE = 256" in dongfu_random_target_repository
                and "rowid>? AND rowid<=?" in dongfu_random_target_repository
                and "ORDER BY rowid ASC LIMIT ?" in dongfu_random_target_repository
                and "read_only=True" in dongfu_random_target_repository
                and "?mode=ro" in dongfu_random_target_repository
                and "PRAGMA table_info(dongfu_status)" in dongfu_random_target_repository
                and "CREATE TABLE" not in dongfu_random_target_repository
                and "ALTER TABLE" not in dongfu_random_target_repository
            ),
            "random_target_fair_cooperative_fail_closed": (
                "random_source.randint(1, eligible_count) == 1" in dongfu_random_target_query
                and "CANDIDATE_YIELD_INTERVAL = 32" in dongfu_random_target_query
                and "await asyncio.sleep(0)" in dongfu_random_target_query
                and "except DongfuCandidateReadError:\n        return None" in dongfu_random_target_query
            ),
            "legacy_random_target_full_list_disabled": all(
                token not in dongfu_facade[
                    dongfu_facade.index("async def _get_random_dongfu_target") :
                    dongfu_facade.index("@dongfu_help.handle")
                ]
                for token in ("list_users_by_fields", "get_fields", "get_user_info_with_id", "candidates", "choice(")
            ),
            "status_read_has_no_legacy_writeback": (
                "def _get_dongfu" in dongfu_facade
                and "_player_data_manager().get_fields" not in dongfu_facade[
                    dongfu_facade.index("def _get_dongfu") : dongfu_facade.index("def _has_dongfu")
                ]
                and "update_or_write_data" not in dongfu_facade[
                    dongfu_facade.index("def _get_dongfu") : dongfu_facade.index("def _has_dongfu")
                ]
            ),
            "infiltration_eligibility_has_no_legacy_writeback": all(
                "_save_dongfu" not in dongfu_facade[dongfu_facade.index(start) : dongfu_facade.index(end)]
                for start, end in (
                    ("def _can_infiltrate", "def _can_intrude"),
                    ("def _can_intrude", "async def _get_random_dongfu_target"),
                )
            ),
            "status_display_has_no_legacy_writeback": (
                "@my_dongfu.handle" in dongfu_facade
                and "_save_dongfu" not in dongfu_facade[
                    dongfu_facade.index("@my_dongfu.handle") : dongfu_facade.index("@dongfu_plant.handle")
                ]
            ),
            "status_schema_startup_migration_owned": (
                'Migration("map.017", "map_dongfu_status_schema", apply_map_dongfu_status_schema)' in plugin
                and "def apply_map_dongfu_status_schema(" in map_migrations
                and '"map.017"' in plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS") : plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")]
                and '"map.017"' in plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS") : plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")]
            ),
            "status": "dongfu_all_commands_classified_owned_action_transactions_replay_safe_outbox_effects_and_reads_feature_owned",
        },
        "impart_pk": {
            "project_join_application_owned": "impart_pk_application.project_join(" in impart_pk_facade,
            "training_replay_application_owned": "impart_pk_application.training_settle(" in impart_pk_facade,
            "legacy_training_replay_disabled": "_impart_training_settlement_service().get_result(" not in impart_pk_facade,
            "closing_enter_replay_application_owned": "impart_pk_application.closing_enter(" in impart_pk_facade,
            "legacy_closing_enter_replay_disabled": "_impart_closing_enter_service().get_result(" not in impart_pk_facade,
            "closing_settlement_replay_application_owned": "impart_pk_application.closing_settle(" in impart_pk_facade,
            "legacy_closing_settlement_replay_disabled": "_impart_closing_settlement_service().get_result(" not in impart_pk_facade,
            "explore_replay_application_owned": "impart_pk_application.explore_settle(" in impart_pk_facade,
            "legacy_explore_replay_disabled": "_impart_explore_settlement_service().get_result(" not in impart_pk_facade,
            "battle_replay_application_owned": "impart_pk_application.battle_settle(" in impart_pk_facade,
            "legacy_battle_replay_disabled": "_impart_battle_batch_service().get_result(" not in impart_pk_facade,
            "status": "training_closing_enter_settlement_explore_battle_replay_cutover_with_other_impart_pk_compatibility",
        },
        "admin_config_owner": {
            "six_commands_share_feature_application": (
                admin_config_handlers.count("admin_config_application.set_switch(") == 6
                and admin_config_handlers.count("admin_config_application.set_group_enabled(") == 2
                and admin_welcome.count("admin_config_application.set_group_welcome(") == 2
                and "JsonConfig" not in admin_config_handlers
                and "AdminConfigRepository(get_paths().data / \"config.json\")" in admin_config_application
            ),
            "canonical_switch_keys_and_legacy_readers_share_repository": (
                all(f'set_switch("{key}",' in admin_config_handlers for key in ("private", "root_selection", "sect_name"))
                and "class JsonConfig(AdminConfigRepository):" in admin_config_compat
                and "self.update(change)" in admin_config_compat
                and 'open(self.config_jsonpath, "w"' not in admin_config_compat
            ),
            "serialized_atomic_writes_and_path_aware_cache": (
                "_LOCK = RLock()" in admin_config_repository
                and "atomic_write(" in admin_config_repository
                and "stat.st_ino, stat.st_mtime_ns, stat.st_size" in admin_config_repository
                and "copy.deepcopy(data)" in admin_config_repository
                and "if data != before:" in admin_config_repository
            ),
            "behavior_failures_concurrency_and_handler_guards_covered": (
                "def test_real_switch_handlers_disable_reenable_and_report_duplicates" in admin_config_tests
                and "def test_real_group_and_welcome_handlers_preserve_permission_guards" in admin_config_tests
                and "def test_failed_replace_and_corrupt_input_do_not_publish_or_destroy_config" in admin_config_tests
                and "def test_repeated_switch_does_not_write_and_parallel_instances_do_not_lose_groups" in admin_config_tests
            ),
            "status": "six_admin_config_commands_feature_owned_with_shared_legacy_json_compatibility",
        },
        "admin_existing_mutation_results": {
            "asset_helpers_merge_result_status_once": (
                "SimpleNamespace(**dict(outcome.data or {}), status=" not in admin_facade
                and admin_facade.count('"status": outcome.status, "succeeded": outcome.ok') == 4
                and "def test_real_asset_helpers_call_default_application_for_missing_schema" in admin_mutation_tests
                and "def test_real_asset_helpers_accept_status_in_application_payload" in admin_mutation_tests
            ),
            "rename_and_batch_failure_replies_are_not_success": (
                'response = f"道号修改未完成：{message}"' in admin_rename_handler
                and "def test_real_rename_handler_never_prefixes_failure_as_success" in admin_mutation_tests
                and "def test_real_batch_completion_callbacks_reject_non_success" in admin_mutation_tests
            ),
            "world_boss_failure_does_not_log_completion": (
                'logger.error(f"世界BOSS额度重置未完成：{result.status}")' in activity_boss_entry
                and "def test_real_boss_reset_loop_logs_failure_instead_of_completion" in admin_mutation_tests
            ),
            "xiangyuan_refund_rollback_and_missing_target_are_covered": (
                'raise ValueError("user_missing")' in base_xiangyuan_repository
                and "def test_clear_all_rolls_back_every_refund_when_any_inventory_is_full" in xiangyuan_clear_tests
                and "def test_clear_all_preserves_orphaned_gifts_instead_of_claiming_a_refund" in xiangyuan_clear_tests
                and "def test_real_clear_all_facade_reports_rolled_back_failure" in admin_mutation_tests
            ),
            "status": "existing_feature_mutations_retained_with_executable_adapter_and_refund_contracts",
        },
        "admin_output_compatibility": {
            "nine_output_commands_keep_permission_boundaries": (
                all(command_permission(admin_facade, command) == "SUPERUSER" for command in ("修仙手册", "按钮测试", "艾特测试", "广播帮助"))
                and all(command_permission(admin_event_debug, command) == "SUPERUSER" for command in ("消息信息", "取链接", "取raw", "取reply"))
                and command_permission(admin_command_controls, "全量申请") == ""
            ),
            "event_debug_reads_current_event_without_history_lookup": (
                all(token in admin_event_debug for token in ("_event_to_dict(event)", "_extract_urls_from_any(event)", "_extract_reply_raw_payload(event)"))
                and not any(token in admin_event_debug for token in ("get_msg(", "get_history", "message_db", "history_messages"))
            ),
            "output_commands_use_shared_delivery_ports": (
                all(token in output_function(admin_event_debug, "_send_blocks") for token in ("delivery_service.reply", "handle_send"))
                and all("_send_blocks(" in output_function(admin_event_debug, name) for name in (
                    "parse_event_cmd_", "fetch_link_cmd_", "fetch_raw_cmd_", "fetch_reply_cmd_",
                ))
                and all("send_help_message(" in output_function(admin_facade, name) for name in (
                    "super_help_", "broadcast_help_cmd_",
                ))
                and all(token in output_function(admin_facade, name) for name in (
                    "at_test_cmd_", "keyboard_test_cmd_",
                ) for token in ("delivery_service.reply", "handle_send"))
                and "delivery_service.reply" in admin_all_apply_handler
            ),
            "all_apply_fallback_preserves_authorization_url": (
                "url_msg = f" in admin_all_apply_handler
                and "授权链接：{target_url}" in admin_all_apply_handler
                and "MessageSegment.markdown(bot, url_msg)" in admin_all_apply_handler
                and "handle_send(bot, event, url_msg)" in admin_all_apply_handler
            ),
            "output_compatibility_has_behavioral_coverage": (
                all(
                    token in admin_event_debug_tests
                    for token in (
                        "test_link_handler_extracts_and_deduplicates_current_event_without_fetching",
                        "test_raw_handler_serializes_current_event_and_truncates_large_output",
                        "test_reply_handler_prefers_raw_reply_and_supports_adapter_reference_shapes",
                    )
                )
                and all(
                    token in admin_output_tests
                    for token in (
                        "test_output_command_permission_boundaries",
                        "test_manual_uses_real_pagination_and_shared_help_delivery",
                        "test_all_apply_markdown_fallback_retains_authorization_url",
                    )
                )
            ),
            "status": "admin_output_commands_keep_legacy_permissions_event_only_debug_reads_shared_delivery_and_url_fallback",
        },
        "admin_blackhouse_owner": {
            "commands_and_router_share_feature_state": (
                all("admin_application.set_blackhouse_status(" in admin_blackhouse_handlers[name]
                    and "admin_application.blackhouse_snapshot(" in admin_blackhouse_handlers[name]
                    for name in ("blackhouse_", "unblackhouse_"))
                and "admin_application.list_blackhoused_users(" in admin_blackhouse_handlers["view_blackhouse_"]
                and "blackhouse_repository or AdminBlackhouseSqlRepository(database)" in legacy_admin_application
                and "self.blackhouse_repository.set_banned(" in legacy_admin_application
                and "AdminBlackhouseStatusService" not in legacy_admin_application
                and "AdminApplication(get_paths().game_db)" in admin_blackhouse_compat
                and "_application().is_user_blackhoused(uid)" in admin_blackhouse_compat
            ),
            "runtime_has_no_json_owner_or_request_ddl": (
                all(token not in admin_blackhouse_compat for token in ("_BANNED", "load_json_file", "save_json_file", "bootstrap_from_user_xiuxian"))
                and all(token not in admin_blackhouse_repository for token in ("CREATE TABLE", "ALTER TABLE", "blackhouse.json"))
                and "global_ban_user" not in admin_facade
                and "global_unban_user" not in admin_facade
                and "bootstrap_from_user_xiuxian" not in admin_blackhouse_router
            ),
            "startup_import_is_game_only_and_required_before_use": (
                '("legacy.admin.007", apply_admin_blackhouse)' in legacy_migrated
                and all('"legacy.admin.007"' not in section for section in (
                    plugin[plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS") : plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")],
                    plugin[plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS") : plugin.index("def migrations_for_database")],
                ))
                and "legacy_json_and_is_ban_v1" in admin_blackhouse_repository
                and "admin_blackhouse_imports" in admin_blackhouse_repository
                and "def test_lifecycle_imports_blackhouse_once_into_game_database" in admin_blackhouse_migration_tests
                and "def test_migration_merges_json_and_sql_once_without_rewriting_json" in admin_blackhouse_repository_tests
            ),
            "membership_projection_and_receipt_are_atomic_and_replay_safe": (
                "DatabaseUnitOfWork(self.database, immediate=True)" in admin_blackhouse_repository
                and all(token in admin_blackhouse_repository for token in (
                    "INSERT INTO admin_blackhouse_users", "UPDATE user_xiuxian SET is_ban=",
                    "INSERT INTO admin_blackhouse_status_operations", '"operation_conflict"',
                ))
                and "def test_receipt_failure_rolls_back_roster_and_player_projection" in admin_blackhouse_status_tests
                and "def test_parallel_repository_instances_record_one_effect" in admin_blackhouse_status_tests
                and "def test_old_receipt_replay_never_reverses_a_later_unban" in admin_blackhouse_status_tests
            ),
            "real_handlers_routing_and_failures_have_behavioral_coverage": (
                all(command_permission(admin_facade, name) == "SUPERUSER" for name in ("小黑屋", "解除小黑屋", "查看小黑屋"))
                and all("elif not result.succeeded" in admin_blackhouse_handlers[name] for name in ("blackhouse_", "unblackhouse_"))
                and all(token in admin_blackhouse_routing_tests for token in (
                    "def test_real_handlers_and_router_share_registered_and_unregistered_state",
                    "def test_failed_write_results_are_never_reported_as_success",
                    "def test_replayed_ban_after_unban_neither_rebans_nor_claims_current_ban",
                    "def test_storage_failure_blocks_only_routed_nonadmin_matchers",
                    "def test_admin_and_unrouted_candidates_skip_lookup_and_remain_available",
                ))
            ),
            "status": "blackhouse_commands_and_router_share_atomic_sql_membership_with_startup_legacy_import",
        },
        "admin_command_control_owner": {
            "commands_and_compatibility_consumers_share_feature_owner": (
                "AdminCommandControlRepository(" in admin_command_application
                and 'get_paths().data / "command_disable.json"' in admin_command_application
                and "return AdminCommandControlApplication()" in admin_command_compat
                and all(f"self.repository.{method}(" in admin_command_application
                        and f"_application().{method}(" in admin_command_compat
                        for method in ("apply_disable_targets", "set_command_disabled", "is_command_disabled",
                                       "sync_command_registry", "rebuild_alias_index", "collect_command_list_rows"))
                and all(token not in admin_command_compat for token in (
                    "_COMMAND_ENTRIES", "_ALIAS_TO_PRIMARY", "load_json_file", "save_json_file",
                ))
            ),
            "atomic_file_owner_preserves_state_and_noop_avoids_writes": (
                "atomic_write(self.path," in admin_command_repository
                and "if document != before:" in admin_command_repository
                and all(token in admin_command_repository_tests for token in (
                    "test_identical_sync_and_toggles_do_not_write",
                    "test_failed_atomic_replace_keeps_disk_cache_and_registry_unchanged",
                    "test_wrapped_unknown_data_and_dormant_commands_survive_sync",
                    "test_multiple_instances_serialize_mutations_without_lost_updates",
                ))
            ),
            "handler_flags_do_not_rebuild_registry_and_failures_are_reported": (
                "rebuild_on_compat_index" not in admin_command_controls
                and "load_command_disable_memory" not in admin_blackhouse_router
                and all("except Exception as exc:" in handler and "type(exc).__name__" in handler
                        for handler in admin_command_handlers.values())
                and all("apply_disable_targets(" in admin_command_handlers[name]
                        for name in ("cmd_disable_", "cmd_enable_"))
                and "format_command_list_page(" in admin_command_handlers["cmd_list_"]
                and "test_real_write_failure_preserves_file_cache_and_route_state" in admin_command_tests
            ),
            "routing_aliases_exemptions_and_bad_files_have_behavioral_coverage": (
                all(command_permission(admin_command_controls, name) == "SUPERUSER"
                    for name in ("指令禁用", "指令解禁", "指令列表"))
                and "command control lookup failed closed" in admin_blackhouse_router
                and all(token in admin_command_tests for token in (
                    "test_real_handlers_deduplicate_primary_alias_and_module_and_apply_to_router_immediately",
                    "test_real_handler_cannot_disable_admin_by_name_alias_or_module",
                    "test_web_compatible_single_setter_cannot_disable_admin",
                    "test_corrupt_file_is_not_replaced_and_router_fails_closed_except_admin_and_unrouted",
                    "test_real_list_handler_filters_disabled_rows_and_clamps_page",
                    "test_real_list_handler_handles_overlong_numeric_page",
                ))
            ),
            "status": "command_flags_registry_and_aliases_share_atomic_json_owner_with_failure_safe_runtime_view",
        },
        "admin_broadcast_owner": _admin_broadcast_owner_status(admin_broadcast_sources),
        "admin_runtime_owner": _admin_runtime_owner_status(admin_runtime_sources),
        "admin_qqid_owner": _admin_qqid_owner_status({
            name: (PACKAGE / path).read_text(encoding="utf-8") for name, path in {
                "handlers": "xiuxian/xiuxian_admin/__init__.py",
                "compatibility": "xiuxian/xiuxian_utils/id_migration.py",
                "application": "features/admin/qqid_application.py",
                "candidate": "features/admin/qqid_candidate_repository.py",
                "batch": "features/admin/qqid_batch_repository.py",
                "migrations": "features/admin/migrations.py",
                "registry": "features/_legacy_migrated.py",
            }.items()
        }),
        "admin": {
            "stone_default_application_owned": (
                "AdminStoneSqlRepository" in admin_asset_application
                and "self.repository or AdminStoneSqlRepository(self.database)" in admin_asset_stone_application
                and '"applied", "duplicate"' in admin_asset_stone_application
            ),
            "stone_legacy_repository_not_default_composed": "LegacyAdminStoneRepository" not in plugin,
            "stone_request_path_has_no_ddl": (
                "CREATE TABLE" not in admin_stone_repository
                and "admin_stone_adjustment_operations" in admin_asset_migrations
                and "economy_log" in admin_asset_migrations
            ),
            "stone_replay_payload_and_audit_atomic": (
                "[operator_id, user_id, requested_delta]" in admin_stone_repository
                and '"INSERT INTO economy_log' in admin_stone_repository
                and '"INSERT INTO admin_stone_adjustment_operations(' in admin_stone_repository
                and '"operation_conflict"' in admin_stone_repository
            ),
            "stone_startup_migration_registered": (
                'Migration("admin_asset.002", "admin_stone_adjustment_operations", apply_admin_stone_adjustment)' in plugin
                and 'migration_version="admin_asset.013"' in admin_asset_manifest
            ),
            "stone_started_operation_recoverable": (
                "The repository receipt can recover a commit whose" in admin_asset_stone_application
                and 'raise ConflictError("操作正在处理中")' not in admin_asset_stone_application
            ),
            "global_stone_batch_application_owned": (
                "admin_asset_application.adjust_stone_batch(" in admin_stone_batch_handler
                and "AdminStoneBatchSqlRepository" in admin_asset_application
            ),
            "global_stone_batch_legacy_update_removed": (
                "update_ls_all(" not in admin_stone_batch_handler
                and "find_running_stone_batch(" in admin_stone_batch_handler
            ),
            "global_stone_batch_async_and_resumable": (
                "spawn_admin_job(" in admin_stone_batch_handler
                and "run_chunked_until_done" in admin_stone_batch_handler
                and "def find_running(" in admin_stone_batch_repository
                and "status='pending'" in admin_stone_batch_repository
            ),
            "global_stone_batch_duplicate_request_guarded": (
                '"in_progress"' in admin_stone_batch_repository
                and "admin_stone_batch_single_running_idx" in admin_asset_migrations
                and '"in_progress":' in admin_stone_batch_handler
            ),
            "global_stone_batch_request_path_has_no_ddl": (
                "CREATE TABLE" not in admin_stone_batch_repository
                and "admin_stone_batch_progress" in admin_asset_migrations
            ),
            "global_stone_batch_disk_preflight": (
                "shutil.disk_usage" in admin_stone_batch_repository
                and "bytes_per_target" in admin_stone_batch_repository
            ),
            "global_stone_batch_startup_migration_registered": (
                'Migration("admin_asset.003", "admin_stone_batch_adjustment_operations", apply_admin_stone_batch)' in plugin
                and 'migration_version="admin_asset.013"' in admin_asset_manifest
            ),
            "exp_request_path_has_no_ddl": "CREATE TABLE" not in admin_exp_repository,
            "exp_startup_migration_registered": (
                'Migration("admin_asset.004", "admin_exp_adjustment_operations", apply_admin_exp_adjustment)' in plugin
                and 'migration_version="admin_asset.013"' in admin_asset_manifest
                and "def apply_admin_exp_adjustment(" in admin_asset_migrations
            ),
            "item_destroy_request_path_has_no_ddl": (
                "CREATE TABLE" not in admin_item_destroy_repository
                and "ALTER TABLE" not in admin_item_destroy_repository
                and "_schema_ready" in admin_item_destroy_repository
            ),
            "item_destroy_startup_migration_registered": (
                'Migration("admin_asset.005", "admin_item_destroy_operations", apply_admin_item_destroy)' in plugin
                and 'migration_version="admin_asset.013"' in admin_asset_manifest
                and "def apply_admin_item_destroy(" in admin_asset_migrations
            ),
            "item_destroy_missing_schema_reported": (
                'result.status == "schema_missing"' in admin_item_destroy_handler
            ),
            "item_destroy_migration_game_only": (
                '"admin_asset.005"' not in plugin[
                    plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"admin_asset.005"' not in plugin[
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"admin_asset.005"' not in plugin[
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("def migrations_for_database")
                ]
            ),
            "item_grant_request_path_has_no_ddl": (
                "CREATE TABLE" not in admin_item_repository
                and "ALTER TABLE" not in admin_item_repository
                and "_schema_ready" in admin_item_repository
            ),
            "item_grant_startup_migration_registered": (
                'Migration("admin_asset.006", "admin_item_grant_operations", apply_admin_item_grant)' in plugin
                and 'migration_version="admin_asset.013"' in admin_asset_manifest
                and "def apply_admin_item_grant(" in admin_asset_migrations
            ),
            "item_grant_schema_missing_reported": (
                'result.status == "schema_missing"' in admin_item_grant_handler
                and '"schema_missing": "管理员物品服务尚未就绪，请检查启动迁移。"' in admin_asset_application
            ),
            "item_grant_migration_game_only": (
                '"admin_asset.006"' not in plugin[
                    plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"admin_asset.006"' not in plugin[
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"admin_asset.006"' not in plugin[
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("def migrations_for_database")
                ]
            ),
            "realm_change_request_paths_have_no_ddl": (
                "CREATE TABLE" not in admin_level_repository
                and "CREATE TABLE" not in admin_root_repository
                and "_schema_ready" in admin_level_repository
                and "_schema_ready" in admin_root_repository
            ),
            "realm_change_startup_migration_registered": (
                'Migration("admin_asset.007", "admin_level_root_change_operations", apply_admin_realm_changes)' in plugin
                and 'migration_version="admin_asset.013"' in admin_asset_manifest
                and "def apply_admin_realm_changes(" in admin_asset_migrations
            ),
            "realm_change_schema_missing_reported": (
                'result.status == "schema_missing"' in admin_level_handler
                and 'result.status == "schema_missing"' in admin_root_handler
            ),
            "realm_change_migration_game_only": (
                '"admin_asset.007"' not in plugin[
                    plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"admin_asset.007"' not in plugin[
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"admin_asset.007"' not in plugin[
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("def migrations_for_database")
                ]
            ),
            "realm_change_legacy_getters_removed": (
                "_admin_level_change_service" not in admin_facade
                and "_admin_root_change_service" not in admin_facade
                and "AdminRootChangeSqlRepository.root_values(" in admin_root_handler
            ),
            "impart_stone_request_path_has_no_ddl": (
                "CREATE TABLE" not in admin_impart_stone_repository
                and "ALTER TABLE" not in admin_impart_stone_repository
            ),
            "impart_stone_balance_owner_is_impart_database": (
                "impart_data.xiuxian_impart" in admin_impart_stone_repository
                and "user_xiuxian WHERE user_id" in admin_impart_stone_repository
                and "CREATE TABLE" not in admin_impart_stone_repository
                and "impart_data.statistics" not in admin_impart_stone_repository
            ),
            "impart_stone_single_snapshot_feature_owned": (
                "admin_asset_application.snapshot_impart_stone(" in admin_facade
                and "admin_asset_application.adjust_impart_stone(" in admin_facade
                and "_admin_impart_stone_adjustment_service().snapshot(user_id)" not in admin_facade
            ),
            "impart_stone_startup_migration_registered": (
                'Migration("admin_asset.008", "admin_impart_stone_operations", apply_admin_impart_stone_operations)' in plugin
                and 'migration_version="admin_asset.013"' in admin_asset_manifest
                and "def apply_admin_impart_stone_operations(" in admin_asset_migrations
            ),
            "impart_stone_schema_missing_reported": (
                'snapshot.status == "schema_missing"' in admin_facade
                and 'result.status == "schema_missing"' in admin_facade
            ),
            "impart_stone_migration_game_only": (
                '"admin_asset.008"' not in plugin[
                    plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"admin_asset.008"' not in plugin[
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"admin_asset.008"' not in plugin[
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("def migrations_for_database")
                ]
            ),
            "accessory_single_repository_owned": (
                "AdminAccessorySqlRepository" in admin_accessory_adjustment
                and "AdminAccessoryAdjustmentService" not in admin_accessory_adjustment
                and "quality=quality" in admin_facade
                and "create_accessory=lambda: create_accessory_instance(item_id, quality)" in admin_facade
            ),
            "accessory_single_request_path_has_no_ddl": (
                "CREATE TABLE" not in admin_accessory_repository
                and "ALTER TABLE" not in admin_accessory_repository
                and "_schema_ready" in admin_accessory_repository
            ),
            "accessory_single_startup_migration_registered": (
                'Migration("admin_asset.009", "admin_accessory_operations", apply_admin_accessory_operations)' in plugin
                and 'migration_version="admin_asset.013"' in admin_asset_manifest
                and "def apply_admin_accessory_operations(" in admin_asset_migrations
                and "apply_attached_player_accessory_operations" in plugin
            ),
            "accessory_single_schema_missing_reported": (
                admin_facade.count('result.status == "schema_missing"') >= 4
                and "snapshot.status != \"ok\"" in admin_accessory_adjustment
            ),
            "accessory_single_migration_game_only": (
                '"admin_asset.009"' not in plugin[
                    plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"admin_asset.009"' not in plugin[
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"admin_asset.009"' not in plugin[
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("def migrations_for_database")
                ]
            ),
            "accessory_batch_application_owned": (
                "AdminAccessoryBatchSqlRepository" in admin_asset_application
                and "admin_asset_application.grant_accessory_batch(" in admin_facade
                and "admin_asset_application.destroy_accessory_batch(" in admin_facade
                and "_admin_accessory_batch_adjustment_service" not in admin_facade
            ),
            "accessory_batch_request_path_has_no_ddl": (
                "CREATE TABLE" not in admin_accessory_batch_repository
                and "status='pending'" in admin_accessory_batch_repository
            ),
            "accessory_batch_disk_preflight_and_bounded_targets": (
                "shutil.disk_usage" in admin_accessory_batch_repository
                and "bytes_per_target" in admin_accessory_batch_repository
                and "bytes_per_accessory" in admin_accessory_batch_repository
                and "target_insert_chunk_size" in admin_accessory_batch_repository
            ),
            "accessory_batch_startup_migration_registered": (
                'Migration("admin_asset.010", "admin_accessory_batch_operations", apply_admin_accessory_batch)' in plugin
                and 'migration_version="admin_asset.013"' in admin_asset_manifest
                and "def apply_admin_accessory_batch(" in admin_asset_migrations
            ),
            "accessory_batch_migration_game_only": (
                '"admin_asset.010"' not in plugin[
                    plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"admin_asset.010"' not in plugin[
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"admin_asset.010"' not in plugin[
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("def migrations_for_database")
                ]
            ),
            "impart_stone_batch_request_path_has_no_ddl": (
                "CREATE TABLE" not in admin_impart_stone_batch_repository
                and "ALTER TABLE" not in admin_impart_stone_batch_repository
                and "admin_impart_stone_batch_targets" in admin_impart_stone_batch_repository
            ),
            "impart_stone_batch_disk_preflight_and_bounded_roster": (
                "shutil.disk_usage" in admin_impart_stone_batch_repository
                and "bytes_per_target" in admin_impart_stone_batch_repository
                and "target_insert_chunk_size" in admin_impart_stone_batch_repository
                and "max_chunk_size" in admin_impart_stone_batch_repository
                and "INSERT INTO admin_impart_stone_batch_targets" in admin_impart_stone_batch_repository
                and "payload_prefix_chars" in admin_impart_stone_batch_repository
                and "_iter_legacy_users" in admin_impart_stone_batch_repository
                and "max_legacy_payload_chars" in admin_impart_stone_batch_repository
            ),
            "impart_stone_batch_startup_migration_registered": (
                'Migration("admin_asset.011", "admin_impart_stone_batch_operations", apply_admin_impart_stone_batch)' in plugin
                and 'migration_version="admin_asset.013"' in admin_asset_manifest
                and "def apply_admin_impart_stone_batch(" in admin_asset_migrations
            ),
            "impart_stone_batch_migration_game_only": (
                '"admin_asset.011"' not in plugin[
                    plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"admin_asset.011"' not in plugin[
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"admin_asset.011"' not in plugin[
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("def migrations_for_database")
                ]
            ),
            "item_batch_request_path_has_no_ddl": (
                "CREATE TABLE" not in admin_item_batch_repository
                and "ALTER TABLE" not in admin_item_batch_repository
                and "admin_item_batch_targets" in admin_item_batch_repository
            ),
            "item_batch_disk_preflight_and_bounded_roster": (
                "shutil.disk_usage" in admin_item_batch_repository
                and "bytes_per_target" in admin_item_batch_repository
                and "max_chunk_size" in admin_item_batch_repository
                and "target_insert_chunk_size" in admin_item_batch_repository
                and "_iter_legacy_users" in admin_item_batch_repository
                and "max_legacy_payload_chars" in admin_item_batch_repository
                and "_import_legacy_progress" in admin_item_batch_repository
                and "query_all" not in admin_item_batch_repository[
                    admin_item_batch_repository.index("    def _import_legacy_progress("):
                    admin_item_batch_repository.index("    def _begin(")
                ]
            ),
            "item_batch_startup_migration_registered": (
                'Migration("admin_asset.012", "admin_item_batch_operations", apply_admin_item_batch)' in plugin
                and 'migration_version="admin_asset.013"' in admin_asset_manifest
                and "def apply_admin_item_batch(" in admin_asset_migrations
            ),
            "item_batch_migration_game_only": (
                '"admin_asset.012"' not in plugin[
                    plugin.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS"):
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"admin_asset.012"' not in plugin[
                    plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")
                ]
                and '"admin_asset.012"' not in plugin[
                    plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS"):
                    plugin.index("def migrations_for_database")
                ]
            ),
            "item_destroy_application_owned": "admin_asset_application.destroy_item(" in admin_facade,
            "rename_application_owned": (
                "BaseApplication" in admin_facade
                and "admin_base_application.rename(" in admin_rename_handler
                and "_sql_message().update_user_name(" not in admin_rename_handler
                and 'Migration("base.002", "player_rename_operations", apply_base_player_rename_operations)' in plugin
            ),
            "exp_adjust_application_owned": "admin_asset_application.adjust_exp(" in admin_facade,
            "level_change_application_owned": "admin_asset_application.change_level(" in admin_facade,
            "legacy_realm_adaptation_application_owned": (
                "admin_asset_application.adapt_legacy_realms(" in admin_realm_adaptation_handler
                and "_sql_message().updata_level(" not in admin_realm_adaptation_handler
                and "get_all_user_id()" not in admin_realm_adaptation_handler
            ),
            "legacy_realm_adaptation_repository_atomic": (
                "class AdminLegacyRealmAdaptationSqlRepository" in admin_legacy_realm_adaptation_repository
                and "DatabaseUnitOfWork" in admin_legacy_realm_adaptation_repository
                and "immediate=True" in admin_legacy_realm_adaptation_repository
                and "admin_level_change_operations" in admin_legacy_realm_adaptation_repository
            ),
            "legacy_realm_adaptation_request_path_has_no_ddl": (
                "CREATE TABLE" not in admin_legacy_realm_adaptation_repository
                and "ALTER TABLE" not in admin_legacy_realm_adaptation_repository
                and "_schema_ready" in admin_legacy_realm_adaptation_repository
            ),
            "legacy_realm_adaptation_replay_and_rollback_covered": (
                "operation_conflict" in admin_legacy_realm_adaptation_repository
                and "result_json" in admin_legacy_realm_adaptation_repository
                and "fail_realm_adaptation_receipt" in (PACKAGE / "features" / "admin_asset" / "tests" / "test_legacy_realm_adaptation_repository.py").read_text(encoding="utf-8")
            ),
            "novice_reset_application_owned": (
                "admin_asset_application.reset_novice_gifts(" in admin_novice_reset_handler
                and "_sql_message().novice_remake(" not in admin_novice_reset_handler
            ),
            "novice_reset_repository_uses_existing_ledger": (
                "class AdminNoviceResetSqlRepository" in admin_novice_reset_repository
                and "OperationLedger" in admin_novice_reset_repository
                and "operation_audit" in admin_novice_reset_repository
            ),
            "novice_reset_request_path_has_no_ddl": (
                "CREATE TABLE" not in admin_novice_reset_repository
                and "ALTER TABLE" not in admin_novice_reset_repository
                and "schema_missing" in admin_novice_reset_repository
            ),
            "novice_reset_replay_and_rollback_covered": (
                "operation_conflict" in admin_novice_reset_repository
                and "fail_novice_reset_audit" in (PACKAGE / "features" / "admin_asset" / "tests" / "test_novice_reset_repository.py").read_text(encoding="utf-8")
            ),
            "root_change_application_owned": "admin_asset_application.change_root(" in admin_facade,
            "impart_stone_application_owned": "admin_asset_application.adjust_impart_stone(" in admin_facade,
            "accessory_application_owned": "admin_asset_application.adjust_accessory(" in admin_facade,
            "player_status_batch_application_owned": (
                "AdminPlayerStatusBatchResetSqlRepository" in legacy_admin_application
                and "self.player_status_batch_repository.reset(" in legacy_admin_application
                and "AdminPlayerStatusBatchResetService" not in legacy_admin_application
            ),
            "player_status_batch_default_entry_bounded": (
                "admin_application.find_player_status_batch(" in admin_status_batch_handler
                and "admin_application.reset_player_status_batch(" in admin_status_batch_handler
                and "get_all_user_id()" not in admin_status_batch_handler
                and "tuple(all_users" not in admin_status_batch_handler
                and "MAX_CHUNK_SIZE = 100" in admin_status_batch_repository
                and "ORDER BY t.ordinal LIMIT ?" in admin_status_batch_repository
                and "def _has_target_capacity(" in admin_status_batch_repository
                and "shutil.disk_usage" in admin_status_batch_repository
            ),
            "player_status_batch_startup_migration_and_capacity": (
                '("legacy.admin.002", apply_admin_player_status_batch_reset)' in legacy_migrated
                and "def apply_admin_player_status_batch_reset(" in admin_feature_migrations
                and "json_each(" in admin_feature_migrations
                and "shutil.disk_usage" in admin_feature_migrations
                and "MemAvailable:" in admin_feature_migrations
            ),
            "player_status_batch_recovery_and_resource_tests": (
                "test_legacy_batches_backfill_targets_and_keep_progress" in admin_status_batch_repository_tests
                and "test_progress_failure_replays_stable_child_receipt" in admin_status_batch_repository_tests
                and "test_legacy_forced_child_receipt_remains_replayable" in admin_status_batch_repository_tests
                and "test_new_batch_freezes_targets_and_caps_each_chunk_at_100" in admin_status_batch_repository_tests
                and "test_new_batch_checks_disk_before_freezing_targets" in admin_status_batch_repository_tests
                and "test_migration_capacity_and_database_routing" in admin_status_batch_repository_tests
            ),
            "item_batch_application_owned": (
                "AdminItemBatchSqlRepository" in admin_asset_application
                and "admin_asset_application.find_running_item_batch(" in admin_item_grant_handler
                and "admin_asset_application.adjust_item_batch(" in admin_item_grant_handler
                and "admin_asset_application.find_running_item_batch(" in admin_item_destroy_handler
                and "admin_asset_application.adjust_item_batch(" in admin_item_destroy_handler
                and "def grant_item_batch(" not in legacy_admin_application
                and "AdminItemBatchGrantService" not in admin_facade
                and "_admin_item_batch_grant_service" not in admin_facade
                and "get_all_user_id()" not in admin_item_grant_handler[
                    admin_item_grant_handler.index(
                        "operation_id = admin_asset_application.find_running_item_batch("
                    ):]
                and "get_all_user_id()" not in admin_item_destroy_handler[
                    admin_item_destroy_handler.index(
                        "operation_id = admin_asset_application.find_running_item_batch("
                    ):]
            ),
            "impart_stone_batch_application_owned": (
                "AdminImpartStoneBatchSqlRepository" in admin_asset_application
                and "admin_asset_application.find_running_impart_stone_batch(" in admin_facade
                and "admin_asset_application.adjust_impart_stone_batch(" in admin_facade
                and "_admin_impart_stone_batch_adjustment_service" not in admin_facade
                and "def adjust_impart_stone_batch(" not in legacy_admin_application
                and "get_all_user_id()" not in admin_facade[
                    admin_facade.index("async def ccll_command_"):
                    admin_facade.index("@adjust_exp_command.handle")
                ]
            ),
            "blackhouse_application_owned": "admin_application.set_blackhouse_status(" in admin_facade,
            "player_status_application_owned": "admin_application.reset_player_status(" in admin_facade,
            "status": "player_name_stone_accessory_impart_stone_and_item_single_and_batch_feature_owned_with_other_admin_compatibility",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    slices = _slice_status()
    phase2_scope = load_phase2_scope_report()
    blockers = list(phase2_scope["integrity_errors"])
    if phase2_scope["blocked_count"]:
        blockers.append(f"{phase2_scope['blocked_count']} frozen default legacy paths remain blocked")
    report = {
        "schema": 1,
        "scope": "full_refactor_phase2",
        "counts": _counts(),
        "slices": slices,
        "phase2_scope": phase2_scope,
        "phase2_complete": phase2_scope["ready"],
        "exit_ready": phase2_scope["ready"],
        "exit_blockers": blockers,
        "p7_release_gate": {
            "status": "independent; evaluated by scripts/refactor_completion_audit.py with real release evidence",
            "included_in_phase2_complete": False,
        },
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=None if args.json else 2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
