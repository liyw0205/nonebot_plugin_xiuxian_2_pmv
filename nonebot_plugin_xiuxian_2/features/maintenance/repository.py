"""Bounded, resumable batch execution for receipt compaction.

Each batch runs in one immediate transaction and updates only the rows it read,
guarded by ``rowid`` and the previous column value, so a concurrent writer's row
is skipped instead of overwritten.  The watermark advances only after the batch
commits; because conversion is idempotent, a crash between commit and the state
write re-scans exactly one batch.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.filesystem.atomic import atomic_write
from .schemas import STATE_VERSION, TaskSpec


class ReceiptCompactionRepository:
    """State file + one-batch-at-a-time execution."""

    def __init__(self, state_path: str | Path) -> None:
        self.state_path = Path(state_path)

    # ---------------------------------------------------------------- state
    def load_state(self) -> dict[str, Any]:
        try:
            document = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError):
            return {"version": STATE_VERSION, "tasks": {}, "vacuum_done": False}
        if not isinstance(document, dict):
            return {"version": STATE_VERSION, "tasks": {}, "vacuum_done": False}
        tasks = document.get("tasks")
        document["tasks"] = tasks if isinstance(tasks, dict) else {}
        document.setdefault("version", STATE_VERSION)
        document.setdefault("vacuum_done", False)
        return document

    def save_state(self, state: dict[str, Any]) -> None:
        atomic_write(
            self.state_path,
            json.dumps(state, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8"),
        )

    # ---------------------------------------------------------------- batch
    def run_batch(
        self,
        database: str | Path,
        spec: TaskSpec,
        watermark: int,
        batch_size: int,
    ) -> tuple[int, int, int, int, bool]:
        """Return ``(converted, skipped, failed, new_watermark, done)``.

        Missing table/column counts as done: the pass never creates schema.
        """

        converted = skipped = failed = 0
        last = watermark
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            present = uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
                (spec.table,),
            )
            if present is None:
                return 0, 0, 0, watermark, True
            columns = {str(row["name"]) for row in uow.query_all('PRAGMA table_info("%s")' % spec.table)}
            if spec.column not in columns:
                return 0, 0, 0, watermark, True
            has_user = "user_id" in columns
            select = (
                'SELECT rowid AS __rowid__,"%s" AS value,user_id FROM "%s" '
                "WHERE rowid>? ORDER BY rowid LIMIT ?" % (spec.column, spec.table)
                if has_user
                else 'SELECT rowid AS __rowid__,"%s" AS value FROM "%s" '
                "WHERE rowid>? ORDER BY rowid LIMIT ?" % (spec.column, spec.table)
            )
            rows = []
            for row in uow.query_all(select, (watermark, batch_size)):
                item = dict(row)
                item[spec.column] = item.pop("value", None)
                rows.append(item)
            if not rows:
                return 0, 0, 0, watermark, True
            for row in rows:
                last = int(row["__rowid__"])
                try:
                    change = spec.convert(row)
                except Exception:
                    failed += 1
                    continue
                if not change:
                    skipped += 1
                    continue
                assignments = ",".join('"%s"=?' % name for name in change)
                params = list(change.values()) + [row["__rowid__"], row[spec.column]]
                result = uow.execute(
                    'UPDATE "%s" SET %s WHERE rowid=? AND "%s" IS ?'
                    % (spec.table, assignments, spec.column),
                    params,
                )
                if getattr(result, "rowcount", 0) == 1:
                    converted += 1
                else:
                    # The row changed under us; a later pass retries it.
                    skipped += 1
            done = len(rows) < batch_size
        return converted, skipped, failed, last, done


__all__ = ["ReceiptCompactionRepository"]
