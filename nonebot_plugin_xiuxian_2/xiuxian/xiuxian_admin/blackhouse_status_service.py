from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class AdminBlackhouseStatusResult:
    status: str
    action: str = ""
    previous_banned: bool = False
    final_banned: bool = False
    changed: bool = False

    @property
    def succeeded(self) -> bool:
        return self.status in {"changed", "unchanged", "duplicate"}


class AdminBlackhouseStatusService:
    """Atomically change one player's blackhouse status."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_blackhouse_status_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def snapshot(self, user_id) -> bool | None:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user id is required")
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            row = conn.execute(
                "SELECT COALESCE(is_ban,0) FROM user_xiuxian WHERE user_id=%s",
                (user_id,),
            ).fetchone()
            return bool(int(row[0])) if row else None

    def set_banned(
        self,
        operation_id,
        operator_id,
        user_id,
        expected_banned,
        banned,
    ) -> AdminBlackhouseStatusResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        banned = bool(banned)
        action = "ban" if banned else "unban"
        if not operation_id or not operator_id or not user_id:
            raise ValueError("operation, operator and user are required")
        normalized_expected = (
            None if expected_banned is None else bool(expected_banned)
        )
        payload = json.dumps(
            [operator_id, user_id, action],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json "
                    "FROM admin_blackhouse_status_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return AdminBlackhouseStatusResult("operation_conflict")
                    saved = json.loads(str(previous[1]))
                    return AdminBlackhouseStatusResult(
                        "duplicate",
                        str(saved["action"]),
                        bool(saved["previous_banned"]),
                        bool(saved["final_banned"]),
                        bool(saved["changed"]),
                    )

                row = conn.execute(
                    "SELECT COALESCE(is_ban,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return AdminBlackhouseStatusResult("user_missing", action=action)
                actual_banned = bool(int(row[0]))
                if actual_banned != normalized_expected:
                    conn.rollback()
                    return AdminBlackhouseStatusResult(
                        "state_changed",
                        action,
                        actual_banned,
                        actual_banned,
                    )

                changed = actual_banned != banned
                if changed:
                    updated = conn.execute(
                        "UPDATE user_xiuxian SET is_ban=%s "
                        "WHERE user_id=%s AND COALESCE(is_ban,0)=%s",
                        (int(banned), user_id, int(actual_banned)),
                    )
                    if updated.rowcount != 1:
                        conn.rollback()
                        return AdminBlackhouseStatusResult("state_changed", action=action)

                result_json = json.dumps(
                    {
                        "action": action,
                        "previous_banned": actual_banned,
                        "final_banned": banned,
                        "changed": changed,
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                conn.execute(
                    "INSERT INTO admin_blackhouse_status_operations("
                    "operation_id,payload,result_json) VALUES(%s,%s,%s)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return AdminBlackhouseStatusResult(
                    "changed" if changed else "unchanged",
                    action,
                    actual_banned,
                    banned,
                    changed,
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["AdminBlackhouseStatusResult", "AdminBlackhouseStatusService"]
