from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any


class LegacyBankOperationReceiptRepository:
    """Read old bank replay receipts without creating or changing schema."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    def _read(self, table: str, operation_id: str, columns: tuple[str, ...]) -> dict[str, Any] | None:
        operation_id = str(operation_id).strip()
        if not operation_id or not self.database.is_file():
            return None
        uri = f"{self.database.resolve().as_uri()}?mode=ro"
        with closing(sqlite3.connect(uri, uri=True)) as connection:
            connection.row_factory = sqlite3.Row
            exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            if exists is None:
                return None
            available = {
                str(row["name"])
                for row in connection.execute(f'PRAGMA table_info("{table}")')
            }
            if not set(columns).issubset(available):
                return None
            selected = ",".join(columns)
            row = connection.execute(
                f'SELECT {selected} FROM "{table}" WHERE operation_id=?', (operation_id,)
            ).fetchone()
            return None if row is None else dict(row)

    def get_upgrade_result(self, operation_id: str) -> dict[str, Any] | None:
        row = self._read(
            "bank_upgrade_operations",
            operation_id,
            ("operation_id", "cost", "wallet_stone", "bank_level"),
        )
        if row is None:
            return None
        return {
            "status": "duplicate",
            "cost": int(row["cost"]),
            "wallet_stone": int(row["wallet_stone"]),
            "bank_level": str(row["bank_level"]),
        }

    def get_interest_result(self, operation_id: str) -> dict[str, Any] | None:
        row = self._read(
            "bank_interest_operations",
            operation_id,
            ("operation_id", "interest", "wallet_stone", "saved_at"),
        )
        if row is None:
            return None
        return {
            "status": "duplicate",
            "interest": int(row["interest"]),
            "wallet_stone": int(row["wallet_stone"]),
            "saved_at": str(row["saved_at"]),
        }


__all__ = ["LegacyBankOperationReceiptRepository"]
