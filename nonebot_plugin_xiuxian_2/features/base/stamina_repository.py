from __future__ import annotations

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

    def consume(
        self,
        user_id: str,
        amount: int,
        *,
        expected_stamina: int | None = None,
    ) -> dict[str, Any]:
        user_id = str(user_id).strip()
        amount = int(amount)
        if not user_id or amount < 0:
            return self._result("invalid")
        if amount == 0:
            return self._result("applied", expected_stamina)
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
            return self._result("applied", current - amount)


__all__ = ["PlayerStaminaSqlRepository"]
