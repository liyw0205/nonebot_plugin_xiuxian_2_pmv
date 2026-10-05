from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class PlayerStaminaSqlRepository:
    """Atomically consume stamina from the existing player projection."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        columns = {
            str(row["name"])
            for row in uow.query_all('PRAGMA table_info("user_xiuxian")')
        }
        return {"user_id", "user_stamina"}.issubset(columns)

    @staticmethod
    def _result(status: str, stamina: int | None = None) -> dict[str, Any]:
        return {
            "status": status,
            "stamina": None if stamina is None else int(stamina),
        }

    @staticmethod
    def _operation_schema_ready(uow: DatabaseUnitOfWork) -> bool:
        rows = uow.query_all('PRAGMA table_info("player_stamina_operations")')
        columns = {str(row["name"]) for row in rows}
        return (
            {"operation_id", "payload", "stamina_after"}.issubset(columns)
            and any(
                str(row["name"]) == "operation_id" and int(row["pk"] or 0) == 1
                for row in rows
            )
        )

    def consume(
        self,
        user_id: str,
        amount: int,
        *,
        expected_stamina: int | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        user_id = str(user_id).strip()
        amount = int(amount)
        operation_id = str(operation_id or "").strip()
        if not user_id or amount < 0:
            return self._result("invalid")
        if amount == 0:
            return self._result("applied", expected_stamina)
        if not self.database.is_file():
            return self._result("schema_missing")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return self._result("schema_missing")
            payload = json.dumps([user_id, amount], separators=(",", ":"))
            if operation_id:
                if not self._operation_schema_ready(uow):
                    return self._result("schema_missing")
                previous = uow.query_one(
                    "SELECT payload,stamina_after FROM player_stamina_operations WHERE operation_id=?",
                    (operation_id,),
                )
                if previous is not None:
                    if str(previous["payload"]) != payload:
                        return self._result("operation_conflict")
                    return self._result("duplicate", int(previous["stamina_after"]))
            row = uow.query_one(
                "SELECT rowid AS _rowid,COALESCE(user_stamina,0) AS user_stamina "
                "FROM user_xiuxian WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                (user_id,),
            )
            if row is None:
                return self._result("user_missing")
            current = int(row["user_stamina"] or 0)
            if expected_stamina is not None and current != int(expected_stamina):
                return self._result("state_changed", current)
            if current < amount:
                return self._result("stamina_insufficient", current)
            changed = uow.execute(
                "UPDATE user_xiuxian SET user_stamina=? WHERE rowid=? "
                "AND user_id=? AND COALESCE(user_stamina,0)=? "
                "AND COALESCE(user_stamina,0)>=?",
                (current - amount, row["_rowid"], user_id, current, amount),
            )
            if changed.rowcount != 1:
                return self._result("state_changed", current)
            remaining = current - amount
            if operation_id:
                uow.execute(
                    "INSERT INTO player_stamina_operations(operation_id,payload,stamina_after) "
                    "VALUES(?,?,?)",
                    (operation_id, payload, remaining),
                )
            return self._result("applied", remaining)

    def recover(
        self,
        max_stamina: int,
        points: int,
        *,
        batch_size: int = 1000,
    ) -> dict[str, Any]:
        max_stamina = int(max_stamina)
        points = int(points)
        batch_size = max(1, int(batch_size))
        if max_stamina < 0 or points < 0:
            return {"status": "invalid", "updated": 0}
        if points == 0:
            return {"status": "applied", "updated": 0}
        if not self.database.is_file():
            return {"status": "schema_missing", "updated": 0}

        total = 0
        last_rowid = 0
        while True:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                if not self._schema_ready(uow):
                    return {"status": "schema_missing", "updated": total}
                rows = uow.query_all(
                    "SELECT rowid AS _rowid FROM user_xiuxian "
                    "WHERE rowid>? AND COALESCE(user_stamina,0)<? "
                    "ORDER BY rowid LIMIT ?",
                    (last_rowid, max_stamina, batch_size),
                )
                if not rows:
                    break
                first_rowid = int(rows[0]["_rowid"])
                last_rowid = int(rows[-1]["_rowid"])
                changed = uow.execute(
                    "UPDATE user_xiuxian SET user_stamina=MIN(COALESCE(user_stamina,0)+?,?) "
                    "WHERE rowid BETWEEN ? AND ? "
                    "AND COALESCE(user_stamina,0)<?",
                    (points, max_stamina, first_rowid, last_rowid, max_stamina),
                )
                updated = max(int(changed.rowcount), 0)
            total += updated
        return {"status": "applied", "updated": total}

    def restore(
        self,
        user_id: str,
        points: int,
        max_stamina: int,
    ) -> dict[str, Any]:
        """Restore one user's stamina without creating a schema or user row."""
        user_id = str(user_id).strip()
        points, max_stamina = int(points), int(max_stamina)
        if not user_id or points < 0 or max_stamina < 0:
            return self._result("invalid")
        if points == 0:
            return self._result("applied")
        if not self.database.is_file():
            return self._result("schema_missing")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return self._result("schema_missing")
            row = uow.query_one(
                "SELECT rowid AS _rowid,COALESCE(user_stamina,0) AS user_stamina "
                "FROM user_xiuxian WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                (user_id,),
            )
            if row is None:
                return self._result("user_missing")
            current = int(row["user_stamina"] or 0)
            restored = min(current + points, max_stamina)
            changed = uow.execute(
                "UPDATE user_xiuxian SET user_stamina=? WHERE rowid=? "
                "AND user_id=? AND COALESCE(user_stamina,0)=?",
                (restored, row["_rowid"], user_id, current),
            )
            if changed.rowcount != 1:
                return self._result("state_changed", current)
            return self._result("applied", restored)


__all__ = ["PlayerStaminaSqlRepository"]
