"""Shared assertions for feature vertical-slice contract modules.

A slice declares its durable bounds once in ``schemas.py`` and every other module
imports them.  These helpers prove the implementing modules no longer carry their
own copy, which is the failure mode a decorative ``schemas.py`` would hide.
"""

from __future__ import annotations

import ast
from pathlib import Path
from types import ModuleType


def module_level_assignments(module: ModuleType) -> set[str]:
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"), filename=str(module.__file__))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names.update(target.id for target in node.targets if isinstance(target, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return names


def assert_contract_is_single_source(
    test, contract: ModuleType, bindings: tuple[tuple[ModuleType, tuple[str, ...]], ...]
) -> None:
    for consumer, names in bindings:
        assigned = module_level_assignments(consumer)
        for name in names:
            test.assertNotIn(
                name,
                assigned,
                f"{consumer.__name__} still defines {name}; the contract has two homes",
            )
            test.assertEqual(
                getattr(contract, name),
                getattr(consumer, name),
                f"{consumer.__name__} does not use the {name} declared in {contract.__name__}",
            )


def assert_no_autonomous_surface(
    test, *, commands, routes, jobs, migrations, legacy_routes=()
) -> None:
    test.assertEqual(tuple(commands), ())
    test.assertEqual(tuple(routes), ())
    test.assertEqual(tuple(jobs), ())
    test.assertEqual(tuple(migrations), ())
    for method, path, _target in tuple(legacy_routes):
        test.assertIn(method, {"GET", "POST", "PUT", "DELETE"})
        test.assertTrue(path.startswith("/"), path)
