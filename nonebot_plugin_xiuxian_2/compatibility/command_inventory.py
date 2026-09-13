"""Read the historical command inventory without importing legacy modules.

The old packages still own matcher registration during the compatibility
release.  Parsing their source here lets the new manifest declare the same
surface without triggering NoneBot or opening a database at registry-build
time.  This module is transitional and can be removed together with the last
legacy package.
"""

from __future__ import annotations

import ast
from pathlib import Path

from ..bootstrap.registry import CommandSpec


_ROOT = Path(__file__).resolve().parents[1] / "xiuxian"
_PACKAGE_NAMES = {
    "illusion": "xiuxian_Illusion",
    "interactive": "xiuxian_Interactive",
}

# New vertical slices own these command names already.  Leaving them out of
# the legacy manifest avoids declaring two owners during the release cycle.
_MIGRATED_NAMES = {
    "活动领取",
    "活动一键领取",
    "领取活动奖励",
    "今日运势",
    "运势",
    "炼体结算",
    "炼体收获",
    "灵石炼体",
    "炼体药浴",
    "药浴",
    "炼体突破",
    "冲窍",
    "神秘力量",
    "管理员灵石",
}


def legacy_package_root(feature: str) -> Path:
    """Return a legacy package path without importing the package."""
    package_name = _PACKAGE_NAMES.get(str(feature), f"xiuxian_{feature}")
    return _ROOT / package_name


def _literal(value: ast.AST, default):
    try:
        return ast.literal_eval(value)
    except (ValueError, TypeError, SyntaxError):
        return default


def legacy_command_candidates(
    feature: str,
    *,
    include_migrated: bool = False,
) -> tuple[tuple[str, tuple[str, ...], str], ...]:
    """Parse historical ``on_command`` declarations for guardrails/tests."""
    root = legacy_package_root(feature)
    if not root.is_dir():
        return ()
    candidates: list[tuple[str, tuple[str, ...], str]] = []
    for path in sorted(root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
                continue
            if node.func.id != "on_command" or not node.args:
                continue
            name = _literal(node.args[0], "")
            if not isinstance(name, str) or not name.strip() or (not include_migrated and name in _MIGRATED_NAMES):
                continue
            aliases: tuple[str, ...] = ()
            permission = "user"
            for keyword in node.keywords:
                if keyword.arg == "aliases":
                    raw_aliases = _literal(keyword.value, ())
                    if isinstance(raw_aliases, (set, list, tuple)):
                        aliases = tuple(sorted(str(alias) for alias in raw_aliases if str(alias).strip()))
                elif keyword.arg == "permission":
                    text = ast.unparse(keyword.value)
                    permission = "superuser" if "SUPERUSER" in text else text
            candidates.append((name, aliases, permission))
    return tuple(candidates)


def commands_for(feature: str) -> tuple[CommandSpec, ...]:
    candidates = legacy_command_candidates(feature)

    # A few historical mini-games reuse one command as another handler's
    # alias.  Keep the first declaration and remove duplicate aliases so the
    # registry has one unambiguous owner.
    result: list[CommandSpec] = []
    claimed: set[str] = set()
    for name, aliases, permission in sorted(set(candidates)):
        normalized = name.casefold()
        if normalized in claimed:
            continue
        local_aliases: set[str] = set()
        clean_aliases_list: list[str] = []
        for alias in aliases:
            alias_key = alias.casefold()
            if alias_key in claimed or alias_key == normalized or alias_key in local_aliases:
                continue
            local_aliases.add(alias_key)
            clean_aliases_list.append(alias)
        clean_aliases = tuple(clean_aliases_list)
        result.append(CommandSpec(name, aliases=clean_aliases, permission=permission))
        claimed.add(normalized)
        claimed.update(alias.casefold() for alias in clean_aliases)
    return tuple(result)


def legacy_command_names(feature: str) -> frozenset[str]:
    """Return primary command names found in the legacy AST."""
    return frozenset(name for name, _, _ in legacy_command_candidates(feature))


def legacy_migrated_command_names(feature: str) -> frozenset[str]:
    """Return old names/aliases now owned by a new vertical slice."""
    names: set[str] = set()
    for name, aliases, _ in legacy_command_candidates(feature, include_migrated=True):
        if name in _MIGRATED_NAMES:
            names.add(name)
            names.update(aliases)
    return frozenset(names)


__all__ = [
    "commands_for",
    "legacy_command_candidates",
    "legacy_command_names",
    "legacy_migrated_command_names",
    "legacy_package_root",
]
