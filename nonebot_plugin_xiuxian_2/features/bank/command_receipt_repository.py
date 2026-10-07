from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class BankCommandReceiptRepository:
    """Read successful committed receipts and validate the original command identity."""

    _LEGACY = {
        "deposit": ("bank_deposit_operations", ("deposited", "interest", "wallet_stone", "saved_stone", "saved_at")),
        "withdrawal": ("bank_withdrawal_operations", ("withdrawn", "interest", "wallet_stone", "saved_stone", "saved_at")),
        "upgrade": ("bank_upgrade_operations", ("cost", "wallet_stone", "bank_level")),
        "interest": ("bank_interest_operations", ("interest", "wallet_stone", "saved_at")),
    }

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _row(uow, table, operation_id, required):
        if uow.query_one("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)) is None:
            return None
        columns = {str(row["name"]) for row in uow.query_all(f'PRAGMA table_info("{table}")')}
        if not set(required).union({"operation_id", "payload"}).issubset(columns):
            raise ValueError("receipt schema incomplete")
        return uow.query_one(f'SELECT * FROM "{table}" WHERE operation_id=?', (operation_id,))

    @staticmethod
    def _number(value):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("invalid receipt number")
        return value

    def _unified(self, row):
        payload = json.loads(row["payload"])
        if not isinstance(payload, list):
            raise ValueError("invalid receipt payload")
        user = str(row["user_id"])
        result = {
            "status": "duplicate", "wallet_stone": self._number(row["wallet_after"]),
            "saved_stone": self._number(row["saved_after"]), "interest": self._number(row["interest"]),
            "saved_at": str(row["created_at"]),
        }
        amount = None
        if len(payload) == 5 and payload[0] == user and type(payload[1]) is int:
            action, amount = "deposit", self._number(payload[1])
            if amount <= 0 or row["deposited"] != amount or payload[2] != result["interest"]:
                raise ValueError("deposit receipt mismatch")
            self._number(payload[3])
            result.update(deposited=amount, bank_level=str(payload[4]))
        elif len(payload) == 5 and payload[:2] == ["withdraw", user]:
            action, amount = "withdrawal", self._number(payload[2])
            if amount <= 0 or row["deposited"] != -amount or payload[3] != result["interest"]:
                raise ValueError("withdrawal receipt mismatch")
            result.update(withdrawn=amount, bank_level=str(payload[4]))
        elif len(payload) == 4 and payload[:2] == ["upgrade", user]:
            action = "upgrade"
            if row["deposited"] != 0 or result["interest"] != 0:
                raise ValueError("upgrade receipt mismatch")
            result.update(cost=self._number(payload[3]), bank_level=str(payload[2]))
        elif len(payload) == 4 and payload[:2] == ["interest", user]:
            action = "interest"
            if row["deposited"] != 0 or payload[2] != result["interest"]:
                raise ValueError("interest receipt mismatch")
            result["bank_level"] = str(payload[3])
        else:
            raise ValueError("unrecognized receipt payload")
        return user, action, amount, result

    def _legacy(self, action, row):
        payload = json.loads(row["payload"])
        expected_length = 2 if action in {"deposit", "withdrawal"} else 3 if action == "upgrade" else 1
        if not isinstance(payload, list) or len(payload) != expected_length or not isinstance(payload[0], str):
            raise ValueError("invalid legacy receipt identity")
        result = {"status": "duplicate"}
        for key in self._LEGACY[action][1]:
            result[key] = str(row[key]) if key in {"saved_at", "bank_level"} else self._number(row[key])
        amount = None
        if action in {"deposit", "withdrawal"}:
            amount = self._number(payload[1])
            field = "deposited" if action == "deposit" else "withdrawn"
            if amount <= 0 or result[field] != amount:
                raise ValueError("legacy receipt amount mismatch")
        elif action == "upgrade":
            if str(payload[1]) != result["bank_level"] or self._number(payload[2]) != result["cost"]:
                raise ValueError("legacy upgrade receipt mismatch")
        return payload[0], action, amount, result

    def find(self, *, operation_id: str, user_id: str, action: str, amount: int | None = None) -> dict | None:
        if not self.database.is_file():
            return {"action": action, "status": "schema_missing"}
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                row = self._row(uow, "bank_account_operations", operation_id, {
                    "user_id", "deposited", "interest", "wallet_after", "saved_after", "created_at",
                })
                if row is not None:
                    receipt = self._unified(row)
                else:
                    receipts = []
                    for kind, (table, columns) in self._LEGACY.items():
                        previous = self._row(uow, table, operation_id, columns)
                        if previous is not None:
                            receipts.append(self._legacy(kind, previous))
                    if not receipts:
                        return None
                    if len(receipts) != 1:
                        return {"action": action, "status": "operation_conflict"}
                    receipt = receipts[0]
        except (ValueError, TypeError, KeyError):
            return {"action": action, "status": "receipt_invalid"}
        previous_user, previous_action, previous_amount, result = receipt
        if (previous_user, previous_action, previous_amount) != (user_id, action, amount):
            return {"action": action, "status": "operation_conflict"}
        return {"action": action, "operation_id": operation_id, **result}


__all__ = ["BankCommandReceiptRepository"]
