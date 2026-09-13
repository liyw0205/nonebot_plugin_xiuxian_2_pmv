from __future__ import annotations

import json
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .domain import TitleTransactionResult


def _decode_titles(value: Any) -> tuple[str, ...]:
    try:
        decoded = json.loads(str(value or "[]"))
    except (TypeError, ValueError):
        decoded = []
    if not isinstance(decoded, list):
        return ()
    return tuple(sorted({str(item) for item in decoded if str(item)}))


def _payload(parts: Any) -> str:
    return json.dumps(list(parts), ensure_ascii=True, separators=(",", ":"))


class TitleRepository:
    """Owns the historical title projection in ``player_db``."""

    def ensure_schema(self, uow: DatabaseUnitOfWork) -> None:
        uow.execute("CREATE TABLE IF NOT EXISTS title(user_id TEXT PRIMARY KEY)")
        columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(title)").fetchall()}
        if "unlocked" not in columns:
            uow.execute("ALTER TABLE title ADD COLUMN unlocked TEXT")
        if "equipped" not in columns:
            uow.execute("ALTER TABLE title ADD COLUMN equipped TEXT")
        uow.execute(
            "CREATE TABLE IF NOT EXISTS title_transaction_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_status TEXT NOT NULL,"
            "title_id TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def get_result(self, uow: DatabaseUnitOfWork, operation_id: str) -> TitleTransactionResult | None:
        self.ensure_schema(uow)
        row = uow.query_one(
            "SELECT result_status,title_id FROM title_transaction_operations WHERE operation_id = ?",
            (str(operation_id).strip(),),
        )
        if row is None:
            return None
        return TitleTransactionResult("duplicate", str(row["title_id"] or ""))

    def _previous(self, uow: DatabaseUnitOfWork, operation_id: str, payload: str) -> TitleTransactionResult | None:
        row = uow.query_one(
            "SELECT payload,result_status,title_id FROM title_transaction_operations WHERE operation_id = ?",
            (operation_id,),
        )
        if row is None:
            return None
        if str(row["payload"]) != payload:
            return TitleTransactionResult("operation_conflict")
        return TitleTransactionResult("duplicate", str(row["title_id"] or ""))

    @staticmethod
    def _record(uow: DatabaseUnitOfWork, operation_id: str, payload: str, status: str, title_id: str) -> None:
        uow.execute(
            "INSERT INTO title_transaction_operations(operation_id,payload,result_status,title_id) VALUES (?, ?, ?, ?)",
            (operation_id, payload, status, title_id),
        )

    def equip(
        self,
        uow: DatabaseUnitOfWork,
        *,
        operation_id: str,
        user_id: str,
        expected_unlocked: Any,
        expected_equipped: Any,
        title_id: str,
    ) -> TitleTransactionResult:
        self.ensure_schema(uow)
        operation_id, user_id, title_id = str(operation_id).strip(), str(user_id), str(title_id).strip()
        unlocked = tuple(sorted({str(item) for item in expected_unlocked}))
        expected_equipped = str(expected_equipped or "")
        if not operation_id or not user_id or not title_id:
            raise ValueError("operation, user and title are required")
        payload = _payload(["equip", user_id, title_id])
        previous = self._previous(uow, operation_id, payload)
        if previous is not None:
            return previous
        row = uow.query_one("SELECT unlocked,equipped FROM title WHERE user_id = ?", (user_id,))
        actual_unlocked = _decode_titles(row["unlocked"]) if row else ()
        actual_equipped = str(row["equipped"] or "") if row else ""
        if actual_unlocked != unlocked or actual_equipped != expected_equipped:
            return TitleTransactionResult("state_changed")
        if title_id not in actual_unlocked:
            return TitleTransactionResult("title_locked")
        status = "already_equipped" if actual_equipped == title_id else "applied"
        if status == "applied":
            changed = uow.execute(
                "UPDATE title SET equipped = ? WHERE user_id = ? AND CAST(COALESCE(equipped,'') AS TEXT) = ?",
                (title_id, user_id, expected_equipped),
            )
            if changed.rowcount != 1:
                return TitleTransactionResult("state_changed")
        self._record(uow, operation_id, payload, status, title_id)
        return TitleTransactionResult(status, title_id)

    def unequip(
        self,
        uow: DatabaseUnitOfWork,
        *,
        operation_id: str,
        user_id: str,
        expected_equipped: Any,
    ) -> TitleTransactionResult:
        self.ensure_schema(uow)
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_equipped = str(expected_equipped or "")
        if not operation_id or not user_id:
            raise ValueError("operation and user are required")
        payload = _payload(["unequip", user_id])
        previous = self._previous(uow, operation_id, payload)
        if previous is not None:
            return previous
        row = uow.query_one("SELECT equipped FROM title WHERE user_id = ?", (user_id,))
        actual = str(row["equipped"] or "") if row else ""
        if actual != expected_equipped:
            return TitleTransactionResult("state_changed")
        if not actual:
            return TitleTransactionResult("not_equipped")
        changed = uow.execute(
            "UPDATE title SET equipped = '' WHERE user_id = ? AND CAST(COALESCE(equipped,'') AS TEXT) = ?",
            (user_id, expected_equipped),
        )
        if changed.rowcount != 1:
            return TitleTransactionResult("state_changed")
        self._record(uow, operation_id, payload, "applied", expected_equipped)
        return TitleTransactionResult("applied", expected_equipped)

    def grant(
        self,
        uow: DatabaseUnitOfWork,
        *,
        operation_id: str,
        user_id: str,
        expected_unlocked: Any,
        title_id: str,
    ) -> TitleTransactionResult:
        self.ensure_schema(uow)
        operation_id, user_id, title_id = str(operation_id).strip(), str(user_id), str(title_id).strip()
        unlocked = tuple(sorted({str(item) for item in expected_unlocked}))
        if not operation_id or not user_id or not title_id:
            raise ValueError("operation, user and title are required")
        payload = _payload(["grant", user_id, title_id])
        previous = self._previous(uow, operation_id, payload)
        if previous is not None:
            return previous
        row = uow.query_one("SELECT unlocked FROM title WHERE user_id = ?", (user_id,))
        actual = _decode_titles(row["unlocked"]) if row else ()
        if actual != unlocked:
            return TitleTransactionResult("state_changed")
        if title_id in actual:
            return TitleTransactionResult("already_unlocked", title_id)
        updated = tuple(sorted((*actual, title_id)))
        value = json.dumps(updated, ensure_ascii=False)
        if row is None:
            uow.execute("INSERT INTO title(user_id,unlocked,equipped) VALUES (?, ?, '')", (user_id, value))
        else:
            changed = uow.execute(
                "UPDATE title SET unlocked = ? WHERE user_id = ? AND COALESCE(unlocked,'') = ?",
                (value, user_id, str(row["unlocked"] or "")),
            )
            if changed.rowcount != 1:
                return TitleTransactionResult("state_changed")
        self._record(uow, operation_id, payload, "applied", title_id)
        return TitleTransactionResult("applied", title_id)

    def unlock_batch(
        self,
        uow: DatabaseUnitOfWork,
        *,
        operation_id: str,
        user_id: str,
        expected_unlocked: Any,
        title_ids: Any,
    ) -> TitleTransactionResult:
        self.ensure_schema(uow)
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected = tuple(sorted({str(item) for item in expected_unlocked}))
        additions = tuple(sorted({str(item) for item in title_ids if str(item)} - set(expected)))
        if not operation_id or not user_id:
            raise ValueError("operation and user are required")
        payload = _payload(["unlock_batch", user_id, additions])
        previous = self._previous(uow, operation_id, payload)
        if previous is not None:
            return previous
        row = uow.query_one("SELECT unlocked FROM title WHERE user_id = ?", (user_id,))
        actual = _decode_titles(row["unlocked"]) if row else ()
        if actual != expected:
            return TitleTransactionResult("state_changed")
        result_ids = tuple(sorted(set(actual) | set(additions)))
        if additions:
            value = json.dumps(result_ids, ensure_ascii=False)
            if row is None:
                uow.execute("INSERT INTO title(user_id,unlocked,equipped) VALUES (?, ?, '')", (user_id, value))
            else:
                uow.execute("UPDATE title SET unlocked = ? WHERE user_id = ?", (value, user_id))
        joined = ",".join(additions)
        self._record(uow, operation_id, payload, "applied", joined)
        return TitleTransactionResult("applied", joined)


__all__ = ["TitleRepository"]
