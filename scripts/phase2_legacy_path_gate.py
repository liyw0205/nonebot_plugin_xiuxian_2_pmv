"""Evaluate the frozen Phase 2 default-runtime legacy-path scope."""

from __future__ import annotations

import argparse
import ast
from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "nonebot_plugin_xiuxian_2"
SCOPE_PATH = ROOT / "docs" / "refactor_phase2_legacy_paths.json"
ITEMS_PATH = ROOT / "docs" / "refactor_phase2_legacy_path_items.json"
VALID_STATUSES = frozenset({"已迁移", "允许保留的兼容路径", "不可达", "受阻"})
_COMMAND_PACKAGE_NAMES = {"illusion": "xiuxian_Illusion", "interactive": "xiuxian_Interactive"}
_PLUGIN_MODULE_EXCLUSIONS = frozenset(
    {
        "infrastructure",
        "messaging",
        "qq_compat",
        "xiuxian_adapter",
        "xiuxian_utils",
        "adapter_compat",
        "adapter_message_actions",
        "adapter_message_records",
        "adapter_message_sender",
        "broadcast_manager",
        "command_disable",
        "on_compat",
        "runtime",
        "xiuxian_config",
    }
)
_COMMAND_INDEX_CACHE_VERSION = "legacy-command-index-v3"


@lru_cache(maxsize=None)
def _read_source(path: Path) -> str | None:
    """Read a source file once per gate invocation."""
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return None


def _canonical_projection(inventory: dict[str, Any], fields: list[str]) -> bytes:
    projection = {field: inventory.get(field) for field in fields}
    return json.dumps(
        projection,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _canonical_items(items: list[dict[str, Any]]) -> bytes:
    return json.dumps(items, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _membership_digest(items: list[dict[str, Any]]) -> str:
    membership = [
        {
            key: item.get(key)
            for key in ("id", "kind", "entry", "source", "legacy_command_exclusion")
        }
        for item in items
    ]
    return hashlib.sha256(_canonical_items(membership)).hexdigest()


@lru_cache(maxsize=1)
def _legacy_manifest_job_ids() -> frozenset[str] | None:
    path = PACKAGE / "compatibility" / "legacy_manifest.py"
    source = _read_source(path)
    if source is None:
        return None
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return None
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "_JOB_IDS" for target in node.targets):
            continue
        try:
            value = ast.literal_eval(node.value)
        except (ValueError, TypeError, SyntaxError):
            return None
        if isinstance(value, (tuple, list, set)) and all(isinstance(job_id, str) for job_id in value):
            return frozenset(value)
    return None


@lru_cache(maxsize=None)
def _legacy_command_declarations(feature: str) -> dict[str, tuple[str, ...]]:
    declarations: dict[str, list[str]] = {}
    for (record_feature, name), records in _default_legacy_command_inventory().items():
        if record_feature != feature:
            continue
        for record in records:
            aliases = record.get("aliases") or []
            aliases_note = f" aliases={','.join(sorted(str(alias) for alias in aliases))}" if aliases else ""
            location = f"{record['file']}:{record['line']}{aliases_note}"
            if location not in declarations.setdefault(name, []):
                declarations[name].append(location)
    return {name: tuple(locations) for name, locations in declarations.items()}


@lru_cache(maxsize=None)
def _parse_ast(path: Path) -> ast.Module | None:
    """Parse each source file once during a gate invocation.

    The static gate revisits legacy modules for command declarations, route
    validation and suppression checks.  The source tree is stable for the
    duration of one invocation, so an in-process AST cache removes redundant
    parser/compiler work without persistent invalidation.
    """
    source = _read_source(path)
    if source is None:
        return None
    try:
        return ast.parse(source, filename=str(path))
    except SyntaxError:
        return None


@lru_cache(maxsize=1)
def _plugin_module_exclusions_from_source() -> frozenset[str] | None:
    path = PACKAGE / "__init__.py"
    tree = _parse_ast(path)
    if tree is None:
        return None
    values: dict[str, frozenset[str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id in {"_INTERNAL_PACKAGES", "_NON_PLUGIN_MODULES"}
            for target in node.targets
        ):
            continue
        try:
            value = ast.literal_eval(node.value)
        except (ValueError, TypeError, SyntaxError):
            continue
        if isinstance(value, (set, list, tuple)):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in {"_INTERNAL_PACKAGES", "_NON_PLUGIN_MODULES"}:
                    values[target.id] = frozenset(str(item) for item in value)
    if set(values) != {"_INTERNAL_PACKAGES", "_NON_PLUGIN_MODULES"}:
        return None
    return values["_INTERNAL_PACKAGES"] | values["_NON_PLUGIN_MODULES"]


def _command_index_cache_path() -> Path:
    root_key = hashlib.sha256(str(ROOT).encode("utf-8")).hexdigest()[:16]
    return Path(tempfile.gettempdir()) / f"xiuxian-phase2-command-index-{root_key}.json"


def _command_index_fingerprint(candidates: list[tuple[Path, str]]) -> str:
    digest = hashlib.sha256(_COMMAND_INDEX_CACHE_VERSION.encode("ascii"))
    for path, source in candidates:
        digest.update(path.relative_to(ROOT).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(source.encode("utf-8"))
        digest.update(b"\0")
    suppression_source = _read_source(PACKAGE / "xiuxian" / "on_compat.py") or ""
    digest.update(suppression_source.encode("utf-8"))
    loader_source = _read_source(PACKAGE / "__init__.py") or ""
    digest.update(loader_source.encode("utf-8"))
    return digest.hexdigest()


def _load_command_index_cache(fingerprint: str) -> dict[tuple[str, str], tuple[dict[str, Any], ...]] | None:
    if os.environ.get("XIUXIAN_PHASE2_DISABLE_CACHE"):
        return None
    try:
        payload = json.loads(_command_index_cache_path().read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("version") != _COMMAND_INDEX_CACHE_VERSION or payload.get("fingerprint") != fingerprint:
        return None
    result: dict[tuple[str, str], tuple[dict[str, Any], ...]] = {}
    for entry in payload.get("entries", []):
        if not isinstance(entry, dict):
            return None
        key = (str(entry.get("feature", "")), str(entry.get("name", "")))
        records = entry.get("records", [])
        if not isinstance(records, list) or not all(isinstance(record, dict) for record in records):
            return None
        result[key] = tuple(records)
    return result


def _save_command_index_cache(
    fingerprint: str,
    result: dict[tuple[str, str], tuple[dict[str, Any], ...]],
) -> None:
    if os.environ.get("XIUXIAN_PHASE2_DISABLE_CACHE"):
        return
    payload = {
        "version": _COMMAND_INDEX_CACHE_VERSION,
        "fingerprint": fingerprint,
        "entries": [
            {"feature": feature, "name": name, "records": list(records)}
            for (feature, name), records in sorted(result.items())
        ],
    }
    path = _command_index_cache_path()
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        os.replace(temporary, path)
    except OSError:
        try:
            temporary.unlink()
        except OSError:
            pass


@lru_cache(maxsize=1)
def _default_legacy_command_inventory() -> dict[tuple[str, str], tuple[dict[str, Any], ...]]:
    """Index default legacy commands without compiling unrelated modules.

    The production loader imports each legacy plugin package below ``xiuxian``.
    Command and handler declarations are source-local, so walking only files
    containing a registration/decorator token is equivalent to the previous
    import-closure result for the default tree while avoiding AST compilation
    of hundreds of helper modules that cannot contribute a command record.
    """
    xiuxian_root = PACKAGE / "xiuxian"
    candidates: list[tuple[Path, str]] = []
    for path in sorted(xiuxian_root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        relative_parts = path.relative_to(xiuxian_root).parts
        if relative_parts and relative_parts[0] in _PLUGIN_MODULE_EXCLUSIONS:
            continue
        source = _read_source(path)
        if source is None:
            continue
        # A handler in a helper file cannot bind a matcher declared in a
        # different module: the source-local index below only joins names
        # within one parsed file.  Skip helper-only files accordingly.
        if "on_command" not in source:
            continue
        candidates.append((path, source))

    fingerprint = _command_index_fingerprint(candidates)
    cached = _load_command_index_cache(fingerprint)
    if cached is not None:
        return cached

    result: dict[tuple[str, str], list[dict[str, Any]]] = {}
    feature_aliases = {package: feature for feature, package in _COMMAND_PACKAGE_NAMES.items()}
    for path, source in candidates:
        tree = _parse_ast(path)
        if tree is None:
            continue
        relative_parts = path.relative_to(xiuxian_root).parts
        module_root = relative_parts[0] if relative_parts else path.stem
        feature = feature_aliases.get(module_root, module_root.removeprefix("xiuxian_"))
        matchers: list[tuple[str, ast.Call, tuple[str, ...], tuple[str, ...], bool]] = []
        handlers: dict[str, list[dict[str, Any]]] = {}
        # Carry the direct parent and import-time context in one explicit
        # traversal.  A stack avoids the allocation and popleft overhead of
        # ``ast.walk`` plus a second parent map on these large legacy files.
        pending = [(tree, None, True)]
        while pending:
            node, parent, import_time = pending.pop()
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for decorator in node.decorator_list:
                    target = decorator.func if isinstance(decorator, ast.Call) else decorator
                    if (
                        isinstance(target, ast.Attribute)
                        and target.attr in {"handle", "got"}
                        and isinstance(target.value, ast.Name)
                    ):
                        handlers.setdefault(target.value.id, []).append(
                            {"name": node.name, "line": node.lineno}
                        )
                # Function bodies cannot register a matcher during module
                # import.  Their decorators above are the only source-bound
                # handler evidence needed by this index.
                continue
            if isinstance(node, (ast.ClassDef, ast.Lambda)):
                # The legacy loader does not treat class/lambda bodies as
                # import-time matcher declarations.
                continue

            if (
                isinstance(node, ast.Call)
                and import_time
                and isinstance(node.func, ast.Name)
                and node.func.id == "on_command"
                and node.args
            ):
                try:
                    name = ast.literal_eval(node.args[0])
                except (ValueError, TypeError, SyntaxError):
                    name = None
                if isinstance(name, str) and name.strip():
                    aliases: tuple[str, ...] = ()
                    for keyword in node.keywords:
                        if keyword.arg != "aliases":
                            continue
                        try:
                            value = ast.literal_eval(keyword.value)
                        except (ValueError, TypeError, SyntaxError):
                            continue
                        if isinstance(value, (set, list, tuple)):
                            aliases = tuple(sorted(str(alias) for alias in value if str(alias).strip()))
                    matcher_names: tuple[str, ...] = ()
                    if isinstance(parent, ast.Assign) and parent.value is node:
                        matcher_names = tuple(target.id for target in parent.targets if isinstance(target, ast.Name))
                    elif isinstance(parent, ast.AnnAssign) and parent.value is node and isinstance(parent.target, ast.Name):
                        matcher_names = (parent.target.id,)
                    suppressed = any(
                        key.casefold() in _suppressed_legacy_command_keys()
                        for key in (name, *aliases)
                    )
                    matchers.append((name, node, aliases, matcher_names, suppressed))

            pending.extend((child, node, import_time) for child in ast.iter_child_nodes(node))

        for name, call, aliases, matcher_names, suppressed in matchers:
            bindings = [
                handler
                for matcher_name in matcher_names
                for handler in handlers.get(matcher_name, [])
            ]
            record = {
                "feature": feature,
                "name": name,
                "aliases": list(aliases),
                "file": path.relative_to(ROOT).as_posix(),
                "line": call.lineno,
                "matcher_names": list(matcher_names),
                "handlers": bindings,
                "suppressed": suppressed,
            }
            result.setdefault((feature, name), []).append(record)
    indexed = {key: tuple(records) for key, records in result.items()}
    _save_command_index_cache(fingerprint, indexed)
    return indexed


def _command_path_call_graph(records: tuple[dict[str, Any], ...]) -> tuple[list[str], list[str]]:
    call_graph = [
        "initialized NoneBot -> _load_legacy_plugins_if_initialized -> load_all_plugins imports the legacy plugin package",
        "legacy on_command registration -> xiuxian/on_compat.py:on_command -> _nb_on_command -> _register_route",
        "XiuxianOnCompatProvider selects the indexed command matcher -> NoneBot dispatches its handle() handler",
    ]
    evidence = [
        "nonebot_plugin_xiuxian_2/__init__.py:89-121",
        "nonebot_plugin_xiuxian_2/xiuxian/on_compat.py:819-849",
        "nonebot_plugin_xiuxian_2/xiuxian/on_compat.py:486-537,749-773",
    ]
    for record in records:
        file = str(record["file"])
        line = int(record["line"])
        matcher_names = record.get("matcher_names") or []
        aliases = record.get("aliases") or []
        alias_note = f" aliases={','.join(aliases)}" if aliases else ""
        matcher_note = f" {','.join(matcher_names)}" if matcher_names else ""
        call_graph.append(
            f"{file}:{line}{matcher_note} = on_command({record['name']!r}){alias_note} -> compatibility matcher"
        )
        evidence.append(f"{file}:{line}")
        for handler in record.get("handlers", []):
            call_graph.append(
                f"{file}:{handler['line']} {handler['name']} -> legacy downstream effect not closed in this frozen item"
            )
            evidence.append(f"{file}:{handler['line']}")
    return call_graph, list(dict.fromkeys(evidence))


def _refreshed_command_graph(
    existing_graph: list[str], records: tuple[dict[str, Any], ...]
) -> tuple[list[str], list[str]]:
    call_graph, evidence = _command_path_call_graph(records)
    preserved_edges: list[str] = []
    for edge in existing_graph:
        if (
            "on_command(" in edge
            or "matcher -> on_compat.on_command" in edge
            or edge.startswith("default NoneBot startup")
            or "legacy downstream effect not closed in this frozen item" in edge
            or "reviewed downstream call-graph edges" in edge
        ):
            continue
        for record in records:
            source_file = str(record["file"])
            for handler in record.get("handlers", []):
                handler_name = str(handler["name"])
                marker = f" {handler_name} ->"
                if edge.startswith(f"{source_file}:") and marker in edge:
                    edge = f"{source_file}:{handler['line']} {edge[edge.index(marker) + 1:]}"
                elif edge.startswith(f"{handler_name} "):
                    edge = f"{source_file}:{handler['line']} {edge}"
                elif f".{handler_name} " in edge and "->" in edge.split(f".{handler_name} ", 1)[1]:
                    fragment = edge.index(f".{handler_name} ") + 1
                    edge = f"{source_file}:{handler['line']} {edge[fragment:]}"
        preserved_edges.append(edge)

    bound_handler_names = {
        str(handler["name"])
        for record in records
        for handler in record.get("handlers", [])
        if any(
            (
                edge.startswith(f"{handler['name']} ")
                or f" {handler['name']} ->" in edge
                or f".{handler['name']} ->" in edge
                or edge.startswith(f"{record['file']}:{handler['line']} {handler['name']} ")
            )
            and "legacy downstream effect not closed in this frozen item" not in edge
            for edge in preserved_edges
        )
    }
    call_graph = [
        edge
        for edge in call_graph
        if not (
            "legacy downstream effect not closed in this frozen item" in edge
            and any(f" {handler_name} ->" in edge for handler_name in bound_handler_names)
        )
    ]
    return list(dict.fromkeys([*call_graph, *preserved_edges])), evidence


@lru_cache(maxsize=1)
def _suppressed_legacy_command_keys() -> frozenset[str]:
    path = PACKAGE / "xiuxian" / "on_compat.py"
    tree = _parse_ast(path)
    if tree is None:
        return frozenset()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if not any(isinstance(target, ast.Name) and target.id == "_MIGRATED_COMMANDS" for target in targets):
            continue
        value = node.value
        if not isinstance(value, ast.Call) or not isinstance(value.func, ast.Name) or value.func.id != "frozenset" or not value.args:
            continue
        try:
            names = ast.literal_eval(value.args[0])
        except (ValueError, TypeError, SyntaxError):
            continue
        if isinstance(names, (set, list, tuple)):
            return frozenset(str(name).casefold() for name in names)
    return frozenset()


@lru_cache(maxsize=None)
def _legacy_route_locations(relative: str) -> dict[tuple[str, str], str]:
    """Index all Flask route decorators in one file once per gate run."""
    path = ROOT / relative
    if not path.is_file():
        return {}
    tree = _parse_ast(path)
    if tree is None:
        return {}
    locations: dict[tuple[str, str], str] = {}
    pending = list(tree.body)
    while pending:
        node = pending.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for decorator in node.decorator_list:
                if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
                    continue
                if decorator.func.attr != "route" or not decorator.args:
                    continue
                try:
                    declared_path = ast.literal_eval(decorator.args[0])
                except (ValueError, TypeError, SyntaxError):
                    continue
                if isinstance(declared_path, str):
                    locations[(node.name, declared_path)] = f"{relative}:{node.lineno}"
            # Function bodies cannot contain a module-level Flask route.
            continue
        pending.extend(ast.iter_child_nodes(node))
    return locations


def _legacy_route_location(item: dict[str, Any]) -> str | None:
    relative = str(item.get("file", ""))
    function_name = str(item.get("function", ""))
    route_path = str(item.get("path", ""))
    return _legacy_route_locations(relative).get((function_name, route_path))


def _inventory_entry_key(field: str, item: Any) -> str:
    if field == "commands":
        return f"{item.get('feature', '')}:{item.get('name', '')}"
    if field == "legacy_jobs":
        return str(item)
    if field == "legacy_routes":
        return "|".join(
            (
                ",".join(sorted(str(method) for method in item.get("methods", []))),
                str(item.get("path", "")),
                str(item.get("file", "")),
                str(item.get("function", "")),
            )
        )
    return json.dumps(item, ensure_ascii=False, sort_keys=True)


def evaluate_phase2_scope(
    scope: dict[str, Any],
    inventory: dict[str, Any],
    *,
    frozen_items: list[dict[str, Any]] | None = None,
    frozen_scope_id: str | None = None,
    frozen_source_projection: dict[str, Any] | None = None,
    default_legacy_commands: dict[tuple[str, str], tuple[dict[str, Any], ...]] | None = None,
    include_items: bool = False,
) -> dict[str, Any]:
    """Compute readiness from the frozen entries; inventory drift never adds paths."""
    snapshot = scope.get("source_snapshot", {})
    fields = snapshot.get("fields", [])
    projection_digest = (
        hashlib.sha256(_canonical_projection(frozen_source_projection, fields)).hexdigest()
        if frozen_source_projection is not None
        else None
    )
    provenance_valid = projection_digest == snapshot.get("sha256")
    inventory_unchanged = frozen_source_projection == {
        field: inventory.get(field) for field in fields
    }
    items = [dict(item) for item in frozen_items or []]
    expected_membership = scope.get("frozen_membership_sha256")
    membership_valid = bool(items) and _membership_digest(items) == expected_membership
    integrity_errors: list[str] = []
    if frozen_items is None:
        integrity_errors.append("Frozen entry file is missing; scope cannot be evaluated.")
    if frozen_scope_id is not None and frozen_scope_id != scope.get("scope_id"):
        integrity_errors.append("Frozen entry file scope_id does not match the scope manifest.")
    if not membership_valid:
        integrity_errors.append("Frozen entry membership hash is invalid.")
    if not provenance_valid:
        integrity_errors.append("Frozen source provenance hash is invalid.")
    if _plugin_module_exclusions_from_source() != _PLUGIN_MODULE_EXCLUSIONS:
        integrity_errors.append("Default plugin import scanner exclusions differ from the production startup loader.")

    frozen_by_id = {str(item.get("id", "")): item for item in items}
    explicit_fields = ("kind", "entry", "status", "reason", "call_graph", "evidence")
    explicit_ids: set[str] = set()
    for summary in scope.get("explicit_paths", []):
        item_id = str(summary.get("id", ""))
        if not item_id or item_id in explicit_ids:
            integrity_errors.append(f"Explicit path summary has a missing or duplicate id: {item_id!r}")
            continue
        explicit_ids.add(item_id)
        frozen = frozen_by_id.get(item_id)
        if frozen is None:
            integrity_errors.append(f"Explicit path summary is missing from frozen entries: {item_id}")
        elif any(summary.get(field) != frozen.get(field) for field in explicit_fields):
            integrity_errors.append(f"Explicit path summary differs from frozen entry: {item_id}")

    for item in items:
        exclusion = item.get("legacy_command_exclusion")
        source = item.get("source") or {}
        if exclusion:
            feature, name = str(exclusion["feature"]), str(exclusion["name"])
            if _legacy_command_declarations(feature).get(name):
                integrity_errors.append(f"Excluded legacy command became registered: {feature}:{name}")
        elif item.get("kind") == "suppressed-command" and item.get("status") == "不可达":
            if str(item.get("entry", "")).casefold() not in _suppressed_legacy_command_keys():
                integrity_errors.append(f"Suppressed legacy command is no longer suppressed: {item.get('entry')}")
        elif item.get("kind") == "commands":
            feature, name = str(source.get("feature", "")), str(source.get("name", ""))
            records = (default_legacy_commands or {}).get((feature, name), ())
            if not records:
                integrity_errors.append(f"Frozen legacy command is not in the AST-derived default startup closure: {feature}:{name}")
            elif all(record.get("suppressed") for record in records):
                integrity_errors.append(f"Frozen default legacy command is suppressed: {feature}:{name}")
            elif not any(record.get("handlers") for record in records):
                integrity_errors.append(f"Frozen default legacy command has no bound handler: {feature}:{name}")
            else:
                evidence = {str(value) for value in item.get("evidence", [])}
                for record in records:
                    declaration = f"{record['file']}:{record['line']}"
                    declaration_in_graph = any(
                        str(edge).startswith(f"{declaration} ") and "= on_command(" in str(edge)
                        for edge in item.get("call_graph", [])
                    )
                    if declaration not in evidence or not declaration_in_graph:
                        integrity_errors.append(
                            f"Frozen command evidence/call graph omits declaration {declaration}: {feature}:{name}"
                        )
                    for handler in record.get("handlers", []):
                        handler_location = f"{record['file']}:{handler['line']}"
                        handler_name = str(handler["name"])
                        handler_edges = [
                            str(edge)
                            for edge in item.get("call_graph", [])
                            if str(edge).startswith(f"{handler_location} {handler_name} ")
                            and "->" in str(edge)
                        ]
                        if handler_location not in evidence or not handler_edges:
                            integrity_errors.append(
                                f"Frozen command evidence/call graph omits source-bound handler {handler_name} at "
                                f"{handler_location}: {feature}:{name}"
                            )
                    if any("unresolved handler target" in str(edge) for edge in item.get("call_graph", [])):
                        integrity_errors.append(
                            f"Frozen command call graph still has an unresolved handler: {feature}:{name}"
                        )
                    if item.get("status") in {"已迁移", "允许保留的兼容路径"} and any(
                        "legacy downstream effect not closed in this frozen item" in str(edge)
                        for edge in item.get("call_graph", [])
                    ):
                        integrity_errors.append(
                            f"Closed command status retains an unresolved downstream edge: {feature}:{name}"
                        )
                    if item.get("status") in {"已迁移", "允许保留的兼容路径"} and not any(
                        "legacy downstream effect not closed in this frozen item" not in edge
                        and "reviewed downstream call-graph edges" not in edge
                        for edge in handler_edges
                    ):
                        integrity_errors.append(
                            f"Closed command has no source-bound downstream edge: {feature}:{name}"
                        )
        elif item.get("kind") == "legacy_routes":
            location = _legacy_route_location(source)
            if not location:
                integrity_errors.append(f"Legacy Web route registration disappeared: {item.get('id')}")
            elif item.get("status") in {"已迁移", "允许保留的兼容路径"}:
                function_name = str(source.get("function", ""))
                evidence = {str(value) for value in item.get("evidence", [])}
                handler_prefix = f"{location} {function_name} ->"
                handler_edges = [
                    str(edge)
                    for edge in item.get("call_graph", [])
                    if str(edge).startswith(handler_prefix)
                ]
                if location not in evidence or not handler_edges:
                    integrity_errors.append(
                        f"Closed legacy Web route has no source-bound handler edge: {item.get('id')}"
                    )
                if any(
                    "downstream state effect not closed" in edge
                    or "unknown downstream" in edge
                    for edge in handler_edges
                ):
                    integrity_errors.append(
                        f"Closed legacy Web route retains an unresolved downstream edge: {item.get('id')}"
                    )
        elif item.get("kind") == "legacy_jobs":
            job_id = str(source.get("job_id", ""))
            manifest_job_ids = _legacy_manifest_job_ids()
            if manifest_job_ids is None:
                integrity_errors.append("Legacy scheduler manifest job IDs could not be verified.")
            elif job_id not in manifest_job_ids:
                integrity_errors.append(f"Frozen scheduler job is not registered by the default manifest: {job_id}")
            elif job_id not in inventory.get("legacy_jobs", []):
                integrity_errors.append(f"Frozen scheduler job is no longer declared in source inventory: {job_id}")
            graph = "\n".join(str(edge) for edge in item.get("call_graph", []))
            if "JobExecutor.run_sync" not in graph or "legacy_job_handler" not in graph:
                integrity_errors.append(f"Frozen scheduler job call graph omits the default manual execution path: {job_id}")

    seen_ids: set[str] = set()
    for item in items:
        item_id = str(item.get("id", ""))
        if not item_id:
            integrity_errors.append("A scoped path has no stable id.")
        elif item_id in seen_ids:
            integrity_errors.append(f"Duplicate scoped path id: {item_id}")
        seen_ids.add(item_id)
        if item.get("status") not in VALID_STATUSES:
            integrity_errors.append(f"Invalid status for {item_id}: {item.get('status')!r}")
        if not item.get("call_graph") or not item.get("evidence"):
            integrity_errors.append(f"Missing call-graph evidence for {item_id}")
        if item.get("source_declaration_found") is False:
            integrity_errors.append(f"Missing source registration for {item_id}")

    status_counts = {status: 0 for status in sorted(VALID_STATUSES)}
    for item in items:
        status = item.get("status")
        if status in status_counts:
            status_counts[status] += 1
    blocked_count = status_counts["受阻"]
    ready = membership_valid and provenance_valid and not integrity_errors and blocked_count == 0 and bool(items)

    family_counts = {
        str(family["id"]): sum(
            item.get("kind") == family.get("source_field") for item in items
        )
        for family in scope.get("families", [])
    }
    baseline_keys = {
        field: {
            _inventory_entry_key(field, baseline)
            for baseline in (frozen_source_projection or {}).get(field, [])
        }
        for field in fields
    }
    source_inventory_added = {
        field: [
            item
            for item in inventory.get(field, [])
            if _inventory_entry_key(field, item) not in baseline_keys[field]
        ]
        for field in fields
    }
    backlog = list(scope.get("backlog", []))
    for field, entries in source_inventory_added.items():
        for entry in entries:
            entry_key = _inventory_entry_key(field, entry)
            backlog.append(
                {
                    "id": f"inventory-drift:{field}:{entry_key}",
                    "source_field": field,
                    "entry": entry,
                    "reason": "Discovered after the Phase 2 scope freeze; review in backlog before any explicit scope-version change.",
                }
            )
    frozen_command_keys = {
        (str((item.get("source") or {}).get("feature", "")), str((item.get("source") or {}).get("name", "")))
        for item in items
        if item.get("kind") == "commands"
    }
    default_command_backlog = []
    for (feature, name), records in sorted((default_legacy_commands or {}).items()):
        if (feature, name) in frozen_command_keys or all(record.get("suppressed") for record in records):
            continue
        call_graph, evidence = _command_path_call_graph(records)
        default_command_backlog.append(
            {
                "id": f"default-legacy-command:{feature}:{name}",
                "source_field": "default_legacy_commands",
                "entry": {"feature": feature, "name": name},
                "call_graph": call_graph,
                "evidence": evidence,
                "reason": (
                    "The production startup import graph registers this non-suppressed legacy command, but it is absent "
                    "from the frozen scope membership. Keep it in backlog; do not add it to Phase 2 completion until an "
                    "explicitly reviewed scope version adopts it."
                ),
            }
        )
    backlog.extend(default_command_backlog)
    result: dict[str, Any] = {
        "scope_id": scope.get("scope_id"),
        "ready": ready,
        "frozen_membership_valid": membership_valid,
        "source_inventory_unchanged": inventory_unchanged,
        "source_inventory_added": source_inventory_added,
        "source_inventory_drift_notice": None if inventory_unchanged else (
            "Current refactor inventory differs from the frozen provenance snapshot. Review the diff into backlog; "
            "the frozen Phase 2 item set was not expanded."
        ),
        "backlog": backlog,
        "default_legacy_command_discovery": "static_ast_startup_import_closure_not_live_runtime_observation",
        "default_legacy_command_count": sum(
            not all(record.get("suppressed") for record in records)
            for records in (default_legacy_commands or {}).values()
        ),
        "default_legacy_command_backlog_count": len(default_command_backlog),
        "path_count": len(items),
        "status_counts": status_counts,
        "family_counts": family_counts,
        "blocked_count": blocked_count,
        "integrity_errors": integrity_errors,
        "p7_gate": scope.get("p7_gate", {"status": "independent"}),
    }
    if include_items:
        result["items"] = items
    return result


def load_phase2_scope_report(*, include_items: bool = False) -> dict[str, Any]:
    scope = json.loads(SCOPE_PATH.read_text(encoding="utf-8"))
    inventory_path = ROOT / str(scope["source_snapshot"]["path"])
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    items_path = ROOT / str(scope.get("frozen_entries_file", ITEMS_PATH.relative_to(ROOT)))
    if not items_path.is_file():
        return evaluate_phase2_scope(scope, inventory, include_items=include_items)
    frozen = json.loads(items_path.read_text(encoding="utf-8"))
    return evaluate_phase2_scope(
        scope,
        inventory,
        frozen_items=frozen.get("items"),
        frozen_scope_id=frozen.get("scope_id"),
        frozen_source_projection=frozen.get("source_projection"),
        default_legacy_commands=_default_legacy_command_inventory(),
        include_items=include_items,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="print machine-readable JSON")
    parser.add_argument("--items", action="store_true", help="include every frozen entry")
    parser.add_argument("--check", action="store_true", help="return nonzero when Phase 2 scope is incomplete")
    parser.add_argument("--membership-hash", action="store_true", help="print the frozen file membership hash for a reviewed scope edit")
    parser.add_argument(
        "--refresh-command-evidence",
        action="store_true",
        help="refresh generic frozen command call graphs from the default startup import graph without changing membership",
    )
    parser.add_argument(
        "--refresh-job-evidence",
        action="store_true",
        help="refresh frozen scheduler call graphs with separate manual and APScheduler paths",
    )
    parser.add_argument(
        "--refresh-command-bindings",
        action="store_true",
        help="refresh frozen command declaration/handler source locations without changing membership",
    )
    parser.add_argument(
        "--feature",
        help="limit --refresh-command-bindings to one frozen feature",
    )
    args = parser.parse_args(argv)
    if args.feature and not args.refresh_command_bindings:
        parser.error("--feature requires --refresh-command-bindings")
    if args.membership_hash:
        scope = json.loads(SCOPE_PATH.read_text(encoding="utf-8"))
        items_path = ROOT / str(scope.get("frozen_entries_file", ITEMS_PATH.relative_to(ROOT)))
        frozen = json.loads(items_path.read_text(encoding="utf-8"))
        print(_membership_digest(frozen["items"]))
        return 0
    if args.refresh_command_evidence:
        scope = json.loads(SCOPE_PATH.read_text(encoding="utf-8"))
        items_path = ROOT / str(scope.get("frozen_entries_file", ITEMS_PATH.relative_to(ROOT)))
        frozen = json.loads(items_path.read_text(encoding="utf-8"))
        before_membership = _membership_digest(frozen["items"])
        command_index = _default_legacy_command_inventory()
        refreshed = 0
        for item in frozen["items"]:
            if item.get("kind") != "commands" or item.get("status") != "受阻":
                continue
            if not any("unresolved handler target" in str(edge) for edge in item.get("call_graph", [])):
                continue
            source = item.get("source") or {}
            key = (str(source.get("feature", "")), str(source.get("name", "")))
            records = command_index.get(key, ())
            if not records or all(record.get("suppressed") for record in records):
                print(f"cannot resolve default command route for {item.get('id')}", file=sys.stderr)
                return 1
            item["call_graph"], item["evidence"] = _command_path_call_graph(records)
            item["reason"] = (
                "The command is default-reachable and its legacy handler binding is recorded below. This item remains "
                "blocked until the handler's downstream state/effect ownership is reviewed; adjacent discoveries remain "
                "in backlog rather than expanding the frozen scope."
            )
            item["unknown_edge"] = "legacy handler -> downstream state/effect owner"
            refreshed += 1
        if _membership_digest(frozen["items"]) != before_membership:
            print("refusing command evidence refresh because frozen membership changed", file=sys.stderr)
            return 1
        items_path.write_text(json.dumps(frozen, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"refreshed_command_call_graphs={refreshed} membership_sha256={before_membership}")
        return 0
    if args.refresh_job_evidence:
        scope = json.loads(SCOPE_PATH.read_text(encoding="utf-8"))
        items_path = ROOT / str(scope.get("frozen_entries_file", ITEMS_PATH.relative_to(ROOT)))
        frozen = json.loads(items_path.read_text(encoding="utf-8"))
        before_membership = _membership_digest(frozen["items"])
        for item in frozen["items"]:
            if item.get("kind") != "legacy_jobs":
                continue
            job_id = str((item.get("source") or {}).get("job_id", ""))
            item["call_graph"] = [
                "production NoneBot startup -> plugin.install_driver_hooks -> ensure_jobs -> legacy scheduler manifest JobSpec registration with handler=legacy_job_handler(job.id)",
                f"admin POST /api/v1/scheduler/{job_id}/run -> scheduler blueprint run_job -> JobExecutor.run_sync -> JobExecutor.run -> registered legacy_job_handler({job_id})",
                f"legacy_job_handler({job_id}) -> _TARGETS/_ALIASES or xiuxian_scheduler fallback -> importlib.import_module -> target(*args, **kwargs)",
                "separate automatic path when this job has a legacy scheduled_job declaration: plugin startup imports its declaring module -> DeferredScheduler captures the original decorated function -> ensure_jobs.activate_scheduler_bridge -> APScheduler.add_job(original function); this path does not call legacy_job_handler",
            ]
            item["evidence"] = [
                "nonebot_plugin_xiuxian_2/__init__.py:89-121",
                "nonebot_plugin_xiuxian_2/plugin.py:1211-1244",
                "nonebot_plugin_xiuxian_2/compatibility/legacy_manifest.py:18-68",
                "nonebot_plugin_xiuxian_2/adapters/web/app.py:466-473",
                "nonebot_plugin_xiuxian_2/adapters/web/blueprints/scheduler.py:16-32",
                "nonebot_plugin_xiuxian_2/infrastructure/scheduler/runner.py:22-94",
                "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/web_runtime.py:9-18",
                "nonebot_plugin_xiuxian_2/compatibility/legacy_jobs.py:42-75",
                "nonebot_plugin_xiuxian_2/compatibility/scheduler.py:48-91,122-128",
            ]
        if _membership_digest(frozen["items"]) != before_membership:
            print("refusing scheduler evidence refresh because frozen membership changed", file=sys.stderr)
            return 1
        items_path.write_text(json.dumps(frozen, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"refreshed_scheduler_call_graphs={sum(item.get('kind') == 'legacy_jobs' for item in frozen['items'])} membership_sha256={before_membership}")
        return 0
    if args.refresh_command_bindings:
        scope = json.loads(SCOPE_PATH.read_text(encoding="utf-8"))
        items_path = ROOT / str(scope.get("frozen_entries_file", ITEMS_PATH.relative_to(ROOT)))
        frozen = json.loads(items_path.read_text(encoding="utf-8"))
        before_membership = _membership_digest(frozen["items"])
        command_index = _default_legacy_command_inventory()
        refreshed = 0
        for item in frozen["items"]:
            if item.get("kind") != "commands":
                continue
            source = item.get("source") or {}
            if args.feature and str(source.get("feature", "")) != args.feature:
                continue
            key = (str(source.get("feature", "")), str(source.get("name", "")))
            records = command_index.get(key, ())
            if not records:
                print(f"cannot resolve default command route for {item.get('id')}", file=sys.stderr)
                return 1
            call_graph, evidence = _refreshed_command_graph(
                [str(edge) for edge in item.get("call_graph", [])], records
            )
            record_evidence = evidence[3:]
            stale_binding_locations = set()
            for edge in item.get("call_graph", []):
                edge = str(edge)
                is_declaration = "= on_command(" in edge
                is_handler = any(
                    f" {handler['name']} ->" in edge
                    for record in records
                    for handler in record.get("handlers", [])
                )
                if not (is_declaration or is_handler):
                    continue
                location = edge.split(" ", 1)[0]
                if ":" in location and location.rsplit(":", 1)[-1].isdigit():
                    stale_binding_locations.add(location)
            item["call_graph"] = call_graph
            retained_evidence = [
                value
                for value in item.get("evidence", [])
                if str(value) not in stale_binding_locations
            ]
            item["evidence"] = list(dict.fromkeys([*retained_evidence, *record_evidence]))
            refreshed += 1
        if _membership_digest(frozen["items"]) != before_membership:
            print("refusing command binding refresh because frozen membership changed", file=sys.stderr)
            return 1
        items_path.write_text(json.dumps(frozen, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        feature_label = f" feature={args.feature}" if args.feature else ""
        print(f"refreshed_command_bindings={refreshed}{feature_label} membership_sha256={before_membership}")
        return 0
    report = load_phase2_scope_report(include_items=args.items)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    else:
        print(
            f"{report['scope_id']}: ready={report['ready']} paths={report['path_count']} "
            f"blocked={report['blocked_count']} frozen_membership_valid={report['frozen_membership_valid']} "
            f"static_ast_default_command_candidates={report.get('default_legacy_command_count', 0)} "
            f"backlog_static_ast_commands={report.get('default_legacy_command_backlog_count', 0)}"
        )
        for error in report["integrity_errors"]:
            print(f"ERROR: {error}", file=sys.stderr)
        if report["source_inventory_drift_notice"]:
            print(f"BACKLOG REVIEW: {report['source_inventory_drift_notice']}", file=sys.stderr)
    return 1 if args.check and not report["ready"] else 0


__all__ = [
    "VALID_STATUSES",
    "evaluate_phase2_scope",
    "load_phase2_scope_report",
]


if __name__ == "__main__":
    raise SystemExit(main())
