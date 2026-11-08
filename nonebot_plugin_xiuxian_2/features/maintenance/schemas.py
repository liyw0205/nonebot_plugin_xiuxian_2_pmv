"""Task table and limits for the startup receipt-compaction pass.

The pass rewrites historical snapshot columns in place.  Everything it touches is
declared here so a reviewer can see the full blast radius in one place: one
column per table, no deletes, and replay columns (``response_json`` and the
past-life/world-boss ``result_json``) are absent by construction.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .receipt_compaction import (
    convert_pet_hatch,
    convert_world_boss,
    past_life_converter,
)

STATE_VERSION = 1
DEFAULT_BATCH_SIZE = 200
DEFAULT_BUDGET_SECONDS = 20.0
STATE_RELATIVE_PATH = "maintenance/receipt_compaction.json"


@dataclass(frozen=True)
class TaskSpec:
    """One receipt table to compact.

    ``database`` selects the owning SQLite file (``game`` or ``player``);
    ``column`` is the single snapshot column that may be rewritten.
    """

    key: str
    database: str
    table: str
    column: str
    convert: Callable[[dict[str, Any]], dict[str, Any] | None]


TASKS: tuple[TaskSpec, ...] = (
    TaskSpec("pet_hatch", "game", "pet_hatch_operations", "result_json", convert_pet_hatch),
    TaskSpec("past_life_choice", "game", "past_life_choice_operations", "payload",
             past_life_converter("advance")),
    TaskSpec("past_life_final", "game", "past_life_final_operations", "payload",
             past_life_converter("final")),
    TaskSpec("past_life_start", "game", "past_life_start_operations", "payload",
             past_life_converter("start")),
    TaskSpec("world_boss_refresh", "player", "world_boss_full_refresh_operations", "payload",
             convert_world_boss),
)

__all__ = [
    "TaskSpec",
    "TASKS",
    "STATE_VERSION",
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_BUDGET_SECONDS",
    "STATE_RELATIVE_PATH",
]
