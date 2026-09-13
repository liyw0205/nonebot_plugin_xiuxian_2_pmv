"""Export the static command, route, job, data and schema inventory.

The inventory is deliberately derived from source and manifests instead of
runtime databases.  It is therefore safe to regenerate in a clean checkout
and is useful before a migration or release review.
"""

from __future__ import annotations

import argparse
import ast
from contextlib import redirect_stdout
import json
import re
import sys
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "nonebot_plugin_xiuxian_2"
OUTPUT = ROOT / "docs" / "refactor_inventory.json"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _literal(node: ast.AST, default: Any = None) -> Any:
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError, SyntaxError):
        return default


def _iter_python_files(root: Path) -> Iterable[Path]:
    return sorted(
        path
        for path in root.rglob("*.py")
        if "__pycache__" not in path.parts and "vendor" not in path.parts
    )


def _legacy_routes() -> list[dict[str, Any]]:
    routes: list[dict[str, Any]] = []
    web_root = PACKAGE / "xiuxian" / "xiuxian_web"
    for path in _iter_python_files(web_root):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for decorator in node.decorator_list:
                if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
                    continue
                if decorator.func.attr != "route" or not decorator.args:
                    continue
                rule = _literal(decorator.args[0])
                if not isinstance(rule, str):
                    continue
                methods: Any = ("GET",)
                for keyword in decorator.keywords:
                    if keyword.arg == "methods":
                        value = _literal(keyword.value)
                        if isinstance(value, (list, tuple, set)):
                            methods = tuple(sorted(str(item).upper() for item in value))
                routes.append(
                    {
                        "file": str(path.relative_to(ROOT)),
                        "function": node.name,
                        "path": rule,
                        "methods": list(methods),
                    }
                )
    return sorted(routes, key=lambda item: (item["path"], item["function"], item["file"]))


def _legacy_jobs() -> list[str]:
    ids: set[str] = set()
    for path in _iter_python_files(PACKAGE / "xiuxian"):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in {"scheduled_job", "add_job"}:
                continue
            for keyword in node.keywords:
                if keyword.arg == "id":
                    value = _literal(keyword.value)
                    if isinstance(value, str):
                        ids.add(value)
    return sorted(ids)


_TABLE_RE = re.compile(
    r"\b(?:CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?|FROM|JOIN|INTO|UPDATE)\s+[`\"']?([A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)


def _database_tables() -> list[str]:
    tables: set[str] = set()
    for path in _iter_python_files(PACKAGE):
        source = path.read_text(encoding="utf-8")
        tables.update(match.group(1).lower() for match in _TABLE_RE.finditer(source))
    return sorted(tables)


def _json_files() -> list[str]:
    roots = (PACKAGE, ROOT / "data")
    files: set[str] = set()
    for root in roots:
        if not root.is_dir():
            continue
        for path in root.rglob("*.json"):
            if not path.is_file():
                continue
            # Compatibility hit counters and similar runtime state are
            # derived artifacts, not deployable JSON assets.  Including them
            # would make the checked-in inventory change after every test or
            # process start.
            if path.name in {"compatibility_hits.json"}:
                continue
            files.add(str(path.relative_to(ROOT)))
    return sorted(files)


def _registry_export() -> dict[str, Any]:
    from nonebot_plugin_xiuxian_2.plugin import build_registry

    return build_registry().export()


def build_inventory() -> dict[str, Any]:
    registry = _registry_export()
    features = registry.get("features", [])
    commands = [
        {
            "feature": feature.get("key"),
            "name": command.get("name"),
            "aliases": sorted(command.get("aliases", [])),
            "permission": command.get("permission"),
        }
        for feature in features
        for command in feature.get("commands", [])
    ]
    routes = [
        {
            "feature": feature.get("key"),
            "path": route.get("path"),
            "methods": sorted(route.get("methods", [])),
            "permission": route.get("permission"),
        }
        for feature in features
        for route in feature.get("routes", [])
    ]
    jobs = [
        {
            "feature": feature.get("key"),
            "id": job.get("id"),
            "owner": job.get("owner"),
            "schedule": job.get("schedule"),
        }
        for feature in features
        for job in feature.get("jobs", [])
    ]
    return {
        "schema": 1,
        "source": "scripts/export_refactor_inventory.py",
        "features": sorted(str(feature.get("key")) for feature in features),
        "commands": sorted(commands, key=lambda item: (str(item["name"]), str(item["feature"]))),
        "routes": sorted(routes, key=lambda item: (str(item["path"]), str(item["feature"]))),
        "jobs": sorted(jobs, key=lambda item: (str(item["id"]), str(item["feature"]))),
        "legacy_routes": _legacy_routes(),
        "legacy_jobs": _legacy_jobs(),
        "database_tables": _database_tables(),
        "json_files": _json_files(),
        "database_files": [
            "data/xiuxian/player.db",
            "data/xiuxian/xiuxian.db",
            "data/xiuxian/trade.db",
            "data/xiuxian/xiuxian_impart.db",
            "data/xiuxian/message.db",
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="export-refactor-inventory")
    parser.add_argument("--output", default=str(OUTPUT))
    parser.add_argument("--check", action="store_true", help="fail when the checked-in inventory is stale")
    args = parser.parse_args(argv)
    with redirect_stdout(sys.stderr):
        payload = json.dumps(build_inventory(), ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    output = Path(args.output).expanduser().resolve()
    if args.check:
        if not output.is_file() or output.read_text(encoding="utf-8") != payload:
            print(f"inventory is stale: {output}", file=sys.stderr)
            return 1
        return 0
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(payload, encoding="utf-8")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
