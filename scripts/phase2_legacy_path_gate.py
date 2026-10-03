"""Evaluate the frozen Phase 2 default-runtime legacy-path scope."""

from __future__ import annotations

import argparse
import ast
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "nonebot_plugin_xiuxian_2"
SCOPE_PATH = ROOT / "docs" / "refactor_phase2_legacy_paths.json"
ITEMS_PATH = ROOT / "docs" / "refactor_phase2_legacy_path_items.json"
VALID_STATUSES = frozenset({"已迁移", "允许保留的兼容路径", "不可达", "受阻"})
_COMMAND_PACKAGE_NAMES = {"illusion": "xiuxian_Illusion", "interactive": "xiuxian_Interactive"}


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
    membership = [{key: item.get(key) for key in ("id", "kind", "entry")} for item in items]
    return hashlib.sha256(_canonical_items(membership)).hexdigest()


@lru_cache(maxsize=None)
def _legacy_command_declarations(feature: str) -> dict[str, tuple[str, ...]]:
    package_name = _COMMAND_PACKAGE_NAMES.get(feature, f"xiuxian_{feature}")
    package_root = PACKAGE / "xiuxian" / package_name
    declarations: dict[str, list[str]] = {}
    if not package_root.is_dir():
        return {}
    for path in sorted(package_root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name) or node.func.id != "on_command":
                continue
            if not node.args:
                continue
            try:
                name = ast.literal_eval(node.args[0])
            except (ValueError, TypeError, SyntaxError):
                continue
            if not isinstance(name, str) or not name.strip():
                continue
            aliases: tuple[str, ...] = ()
            for keyword in node.keywords:
                if keyword.arg != "aliases":
                    continue
                try:
                    value = ast.literal_eval(keyword.value)
                except (ValueError, TypeError, SyntaxError):
                    continue
                if isinstance(value, (set, list, tuple)):
                    aliases = tuple(str(alias) for alias in value)
            if name not in declarations:
                declarations[name] = []
            aliases_note = f" aliases={','.join(sorted(aliases))}" if aliases else ""
            location = f"{path.relative_to(ROOT).as_posix()}:{node.lineno}{aliases_note}"
            if location not in declarations[name]:
                declarations[name].append(location)
    return {name: tuple(locations) for name, locations in declarations.items()}


@lru_cache(maxsize=1)
def _suppressed_legacy_command_keys() -> frozenset[str]:
    path = PACKAGE / "xiuxian" / "on_compat.py"
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError):
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


def _legacy_route_location(item: dict[str, Any]) -> str | None:
    relative = str(item.get("file", ""))
    path = ROOT / relative
    function_name = str(item.get("function", ""))
    route_path = str(item.get("path", ""))
    if not path.is_file():
        return None
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError):
        return None
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or node.name != function_name:
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
                continue
            if decorator.func.attr != "route" or not decorator.args:
                continue
            try:
                declared_path = ast.literal_eval(decorator.args[0])
            except (ValueError, TypeError, SyntaxError):
                continue
            if declared_path == route_path:
                return f"{relative}:{node.lineno}"
    return None


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
        elif item.get("kind") == "commands" and item.get("status") == "受阻":
            feature, name = str(source.get("feature", "")), str(source.get("name", ""))
            if not _legacy_command_declarations(feature).get(name):
                integrity_errors.append(f"Blocked legacy command registration disappeared: {feature}:{name}")
        elif item.get("kind") == "legacy_routes" and item.get("status") == "受阻":
            if not _legacy_route_location(source):
                integrity_errors.append(f"Blocked legacy Web route registration disappeared: {item.get('id')}")
        elif item.get("kind") == "legacy_jobs" and item.get("status") == "允许保留的兼容路径":
            if str(source.get("job_id", "")) not in inventory.get("legacy_jobs", []):
                integrity_errors.append(f"Allowed compatibility job is no longer declared: {source.get('job_id')}")

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
    result: dict[str, Any] = {
        "scope_id": scope.get("scope_id"),
        "ready": ready,
        "frozen_membership_valid": membership_valid,
        "source_inventory_unchanged": inventory_unchanged,
        "source_inventory_added": {
            field: [
                item
                for item in inventory.get(field, [])
                if _inventory_entry_key(field, item)
                not in {
                    _inventory_entry_key(field, baseline)
                    for baseline in (frozen_source_projection or {}).get(field, [])
                }
            ]
            for field in fields
        },
        "source_inventory_drift_notice": None if inventory_unchanged else (
            "Current refactor inventory differs from the frozen provenance snapshot. Review the diff into backlog; "
            "the frozen Phase 2 item set was not expanded."
        ),
        "backlog": scope.get("backlog", []),
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
        include_items=include_items,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="print machine-readable JSON")
    parser.add_argument("--items", action="store_true", help="include every frozen entry")
    parser.add_argument("--check", action="store_true", help="return nonzero when Phase 2 scope is incomplete")
    parser.add_argument("--membership-hash", action="store_true", help="print the frozen file membership hash for a reviewed scope edit")
    args = parser.parse_args(argv)
    if args.membership_hash:
        scope = json.loads(SCOPE_PATH.read_text(encoding="utf-8"))
        items_path = ROOT / str(scope.get("frozen_entries_file", ITEMS_PATH.relative_to(ROOT)))
        frozen = json.loads(items_path.read_text(encoding="utf-8"))
        print(_membership_digest(frozen["items"]))
        return 0
    report = load_phase2_scope_report(include_items=args.items)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    else:
        print(
            f"{report['scope_id']}: ready={report['ready']} paths={report['path_count']} "
            f"blocked={report['blocked_count']} frozen_membership_valid={report['frozen_membership_valid']}"
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
