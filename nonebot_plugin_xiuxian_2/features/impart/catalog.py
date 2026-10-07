"""Feature-owned access to the immutable Impart card catalog."""

from __future__ import annotations

import ast
from functools import lru_cache
from pathlib import Path
from typing import Any


@lru_cache(maxsize=1)
def _catalog_data() -> dict[str, dict[str, Any]]:
    source = Path(__file__).resolve().parents[2] / "xiuxian" / "xiuxian_impart" / "impart_all.py"
    module = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
    for statement in module.body:
        if not isinstance(statement, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "impart_all" for target in statement.targets):
            continue
        value = ast.literal_eval(statement.value)
        if not isinstance(value, dict):
            break
        return {
            str(name): dict(definition)
            for name, definition in value.items()
        }
    raise RuntimeError("legacy impart card catalog is not a dictionary literal")


def card_definitions() -> dict[str, dict[str, Any]]:
    """Return a detached catalog suitable for repository calculations."""

    return {name: dict(definition) for name, definition in _catalog_data().items()}


def card_names() -> tuple[str, ...]:
    return tuple(_catalog_data())


__all__ = ["card_definitions", "card_names"]
