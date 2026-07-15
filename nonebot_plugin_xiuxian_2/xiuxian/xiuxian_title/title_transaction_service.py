from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


def _decode_titles(value) -> tuple[str, ...]:
    try:
        decoded = json.loads(str(value or "[]"))
    except (TypeError, ValueError):
        decoded = []
    if not isinstance(decoded, list):
        return ()
    return tuple(sorted({str(item) for item in decoded if str(item)}))


def _payload(parts) -> str:
    return json.dumps(list(parts), ensure_ascii=True, separators=(",", ":"))


@dataclass(frozen=True)
class TitleTransactionResult:
    status: str
    title_id: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate", "already_equipped"}


class TitleTransactionService:
    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS title(user_id TEXT PRIMARY KEY)")
        columns = set(conn.column_names("title"))
        if "unlocked" not in columns:
            conn.execute("ALTER TABLE title ADD COLUMN unlocked TEXT")
        if "equipped" not in columns:
            conn.execute("ALTER TABLE title ADD COLUMN equipped TEXT")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS title_transaction_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_status TEXT NOT NULL,"
            "title_id TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def get_result(self, operation_id: str) -> TitleTransactionResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_schema(conn)
            previous = conn.execute(
                "SELECT result_status,title_id FROM title_transaction_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return TitleTransactionResult("duplicate", str(previous[1] or ""))

    def equip(self, operation_id, user_id, expected_unlocked, expected_equipped, title_id):
        operation_id, user_id, title_id = str(operation_id).strip(), str(user_id), str(title_id).strip()
        unlocked = tuple(sorted({str(item) for item in expected_unlocked}))
        expected_equipped = str(expected_equipped or "")
        if not operation_id or not user_id or not title_id:
            raise ValueError("operation, user and title are required")
        payload = _payload(["equip", user_id, title_id])
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_status,title_id FROM title_transaction_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return TitleTransactionResult("operation_conflict")
                    return TitleTransactionResult("duplicate", str(previous[2]))
                row = conn.execute(
                    "SELECT unlocked,equipped FROM title WHERE user_id=%s", (user_id,),
                ).fetchone()
                actual_unlocked = _decode_titles(row[0]) if row else ()
                actual_equipped = str(row[1] or "") if row else ""
                if actual_unlocked != unlocked or actual_equipped != expected_equipped:
                    conn.rollback()
                    return TitleTransactionResult("state_changed")
                if title_id not in actual_unlocked:
                    conn.rollback()
                    return TitleTransactionResult("title_locked")
                status = "already_equipped" if actual_equipped == title_id else "applied"
                if status == "applied":
                    changed = conn.execute(
                        "UPDATE title SET equipped=%s WHERE user_id=%s "
                        "AND CAST(COALESCE(equipped,'') AS TEXT)=%s",
                        (title_id, user_id, expected_equipped),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return TitleTransactionResult("state_changed")
                conn.execute(
                    "INSERT INTO title_transaction_operations(operation_id,payload,result_status,title_id) "
                    "VALUES(%s,%s,%s,%s)",
                    (operation_id, payload, status, title_id),
                )
                conn.commit()
                return TitleTransactionResult(status, title_id)
            except Exception:
                conn.rollback()
                raise

    def unequip(self, operation_id, user_id, expected_equipped):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_equipped = str(expected_equipped or "")
        if not operation_id or not user_id:
            raise ValueError("operation and user are required")
        payload = _payload(["unequip", user_id])
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_status,title_id FROM title_transaction_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return TitleTransactionResult("operation_conflict")
                    return TitleTransactionResult("duplicate", str(previous[2]))
                row = conn.execute("SELECT equipped FROM title WHERE user_id=%s", (user_id,)).fetchone()
                actual = str(row[0] or "") if row else ""
                if actual != expected_equipped:
                    conn.rollback()
                    return TitleTransactionResult("state_changed")
                if not actual:
                    conn.rollback()
                    return TitleTransactionResult("not_equipped")
                changed = conn.execute(
                    "UPDATE title SET equipped='' WHERE user_id=%s "
                    "AND CAST(COALESCE(equipped,'') AS TEXT)=%s",
                    (user_id, expected_equipped),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return TitleTransactionResult("state_changed")
                conn.execute(
                    "INSERT INTO title_transaction_operations(operation_id,payload,result_status,title_id) "
                    "VALUES(%s,%s,%s,%s)",
                    (operation_id, payload, "applied", expected_equipped),
                )
                conn.commit()
                return TitleTransactionResult("applied", expected_equipped)
            except Exception:
                conn.rollback()
                raise

    def grant(self, operation_id, user_id, expected_unlocked, title_id):
        operation_id, user_id, title_id = str(operation_id).strip(), str(user_id), str(title_id).strip()
        unlocked = tuple(sorted({str(item) for item in expected_unlocked}))
        if not operation_id or not user_id or not title_id:
            raise ValueError("operation, user and title are required")
        payload = _payload(["grant", user_id, title_id])
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_status,title_id FROM title_transaction_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return TitleTransactionResult("operation_conflict")
                    return TitleTransactionResult("duplicate", str(previous[2]))
                row = conn.execute("SELECT unlocked FROM title WHERE user_id=%s", (user_id,)).fetchone()
                actual = _decode_titles(row[0]) if row else ()
                if actual != unlocked:
                    conn.rollback()
                    return TitleTransactionResult("state_changed")
                if title_id in actual:
                    conn.rollback()
                    return TitleTransactionResult("already_unlocked", title_id)
                updated = tuple(sorted((*actual, title_id)))
                value = json.dumps(updated, ensure_ascii=False)
                if row is None:
                    conn.execute(
                        "INSERT INTO title(user_id,unlocked,equipped) VALUES(%s,%s,'')",
                        (user_id, value),
                    )
                else:
                    changed = conn.execute(
                        "UPDATE title SET unlocked=%s WHERE user_id=%s AND COALESCE(unlocked,'')=%s",
                        (value, user_id, str(row[0] or "")),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return TitleTransactionResult("state_changed")
                conn.execute(
                    "INSERT INTO title_transaction_operations(operation_id,payload,result_status,title_id) "
                    "VALUES(%s,%s,'applied',%s)",
                    (operation_id, payload, title_id),
                )
                conn.commit()
                return TitleTransactionResult("applied", title_id)
            except Exception:
                conn.rollback()
                raise

    def unlock_batch(self, operation_id, user_id, expected_unlocked, title_ids):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected = tuple(sorted({str(item) for item in expected_unlocked}))
        additions = tuple(sorted({str(item) for item in title_ids if str(item)} - set(expected)))
        if not operation_id or not user_id:
            raise ValueError("operation and user are required")
        payload = _payload(["unlock_batch", user_id, additions])
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_status,title_id FROM title_transaction_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return TitleTransactionResult("operation_conflict")
                    return TitleTransactionResult("duplicate", str(previous[2]))
                row = conn.execute("SELECT unlocked FROM title WHERE user_id=%s", (user_id,)).fetchone()
                actual = _decode_titles(row[0]) if row else ()
                if actual != expected:
                    conn.rollback()
                    return TitleTransactionResult("state_changed")
                result_ids = tuple(sorted(set(actual) | set(additions)))
                value = json.dumps(result_ids, ensure_ascii=False)
                if additions:
                    if row is None:
                        conn.execute(
                            "INSERT INTO title(user_id,unlocked,equipped) VALUES(%s,%s,'')",
                            (user_id, value),
                        )
                    else:
                        conn.execute(
                            "UPDATE title SET unlocked=%s WHERE user_id=%s",
                            (value, user_id),
                        )
                joined = ",".join(additions)
                conn.execute(
                    "INSERT INTO title_transaction_operations(operation_id,payload,result_status,title_id) "
                    "VALUES(%s,%s,'applied',%s)",
                    (operation_id, payload, joined),
                )
                conn.commit()
                return TitleTransactionResult("applied", joined)
            except Exception:
                conn.rollback()
                raise


__all__ = ["TitleTransactionResult", "TitleTransactionService"]
