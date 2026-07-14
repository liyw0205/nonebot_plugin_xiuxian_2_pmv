from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class TrainingResetResult:
    status: str
    task_status: str = ""
    reset_date: str = ""
    total: int = 0
    completed: int = 0
    changed: int = 0
    skipped: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class TrainingResetService:
    """Reset a frozen player set in resumable cross-database chunks."""

    _STATE_FIELDS = (
        "progress",
        "last_time",
        "points",
        "completed",
        "max_progress",
        "last_event",
        "weekly_purchases",
    )

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _normalize_date(value) -> str:
        if value is None:
            value = date.today()
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return value.isoformat()
        return date.fromisoformat(str(value).strip()).isoformat()

    @staticmethod
    def _payload(operator_id: str) -> str:
        return json.dumps(
            {"action": "training_reset", "operator_id": operator_id},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_training_reset_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,reset_date TEXT NOT NULL,"
            "total INTEGER NOT NULL,completed INTEGER NOT NULL DEFAULT 0,"
            "changed INTEGER NOT NULL DEFAULT 0,skipped INTEGER NOT NULL DEFAULT 0,"
            "status TEXT NOT NULL DEFAULT 'running',created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_training_reset_targets("
            "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,"
            "status TEXT NOT NULL DEFAULT 'pending',previous_state TEXT,updated_at TEXT NOT NULL,"
            "PRIMARY KEY(operation_id,user_id))"
        )

    @staticmethod
    def _result(conn, operation_id: str, status: str) -> TrainingResetResult:
        row = conn.execute(
            "SELECT status,reset_date,total,completed,changed,skipped "
            "FROM admin_training_reset_operations WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        if row is None:
            return TrainingResetResult(status)
        return TrainingResetResult(
            status=status,
            task_status=str(row[0]),
            reset_date=str(row[1]),
            total=int(row[2]),
            completed=int(row[3]),
            changed=int(row[4]),
            skipped=int(row[5]),
        )

    @classmethod
    def _snapshot(cls, row) -> str:
        return json.dumps(
            dict(zip(cls._STATE_FIELDS, row)),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )

    @staticmethod
    def _state_changed(row, weekly_purchases: dict[str, str]) -> bool:
        try:
            current_weekly = json.loads(str(row[6])) if row[6] else {}
        except (TypeError, ValueError):
            current_weekly = row[6]
        return (
            int(row[0] or 0) != 0
            or row[1] is not None
            or int(row[2] or 0) != 0
            or int(row[3] or 0) != 0
            or int(row[4] or 0) != 0
            or str(row[5] or "") != ""
            or current_weekly != weekly_purchases
        )

    def reset(
        self,
        operation_id,
        operator_id,
        *,
        chunk_size=500,
        reset_date=None,
        updated_at=None,
    ) -> TrainingResetResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        chunk_size = max(1, int(chunk_size))
        reset_date = self._normalize_date(reset_date)
        updated_at = str(updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if not operation_id or not operator_id:
            raise ValueError("operation and operator are required")
        payload = self._payload(operator_id)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute(
                    "ATTACH DATABASE %s AS player_data",
                    (str(self._player_database),),
                )
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                operation = conn.execute(
                    "SELECT payload,reset_date,status FROM admin_training_reset_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if operation is None:
                    user_ids = tuple(
                        str(row[0])
                        for row in conn.execute(
                            "SELECT DISTINCT user_id FROM user_xiuxian ORDER BY user_id"
                        ).fetchall()
                    )
                    task_status = "completed" if not user_ids else "running"
                    conn.execute(
                        "INSERT INTO admin_training_reset_operations("
                        "operation_id,payload,reset_date,total,status,created_at,updated_at) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s)",
                        (
                            operation_id,
                            payload,
                            reset_date,
                            len(user_ids),
                            task_status,
                            updated_at,
                            updated_at,
                        ),
                    )
                    conn.executemany(
                        "INSERT INTO admin_training_reset_targets("
                        "operation_id,user_id,updated_at) VALUES(%s,%s,%s)",
                        (
                            (operation_id, user_id, updated_at)
                            for user_id in user_ids
                        ),
                    )
                    conn.commit()
                    if not user_ids:
                        return self._result(conn, operation_id, "applied")
                else:
                    if str(operation[0]) != payload:
                        result = self._result(conn, operation_id, "operation_conflict")
                        conn.rollback()
                        return result
                    reset_date = str(operation[1])
                    if str(operation[2]) == "completed":
                        result = self._result(conn, operation_id, "duplicate")
                        conn.rollback()
                        return result
                    conn.commit()

                conn.execute("BEGIN IMMEDIATE")
                pending = conn.execute(
                    "SELECT user_id FROM admin_training_reset_targets "
                    "WHERE operation_id=%s AND status='pending' ORDER BY user_id LIMIT %s",
                    (operation_id, chunk_size),
                ).fetchall()
                changed = 0
                skipped = 0
                weekly_purchases = {"_last_reset": reset_date}
                weekly_json = json.dumps(
                    weekly_purchases,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                for pending_row in pending:
                    user_id = str(pending_row[0])
                    user_exists = conn.execute(
                        "SELECT 1 FROM user_xiuxian WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()
                    training = None
                    if user_exists is not None:
                        training = conn.execute(
                            "SELECT progress,last_time,points,completed,max_progress,last_event,"
                            "weekly_purchases FROM player_data.training WHERE user_id=%s",
                            (user_id,),
                        ).fetchone()
                    if training is None:
                        skipped += 1
                        conn.execute(
                            "UPDATE admin_training_reset_targets SET status='skipped',updated_at=%s "
                            "WHERE operation_id=%s AND user_id=%s AND status='pending'",
                            (updated_at, operation_id, user_id),
                        )
                        continue

                    previous_state = self._snapshot(training)
                    changed += int(self._state_changed(training, weekly_purchases))
                    updated = conn.execute(
                        "UPDATE player_data.training SET progress=0,last_time=NULL,points=0,"
                        "completed=0,max_progress=0,last_event='',weekly_purchases=%s "
                        "WHERE user_id=%s",
                        (weekly_json, user_id),
                    )
                    if updated.rowcount != 1:
                        raise db_backend.IntegrityError(
                            "training reset target changed"
                        )
                    target_updated = conn.execute(
                        "UPDATE admin_training_reset_targets SET status='applied',"
                        "previous_state=%s,updated_at=%s WHERE operation_id=%s AND user_id=%s "
                        "AND status='pending'",
                        (previous_state, updated_at, operation_id, user_id),
                    )
                    if target_updated.rowcount != 1:
                        raise db_backend.IntegrityError(
                            "training reset progress changed"
                        )

                progress = conn.execute(
                    "SELECT COUNT(*),"
                    "COALESCE(SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),0) "
                    "FROM admin_training_reset_targets WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                completed = int(progress[0]) - int(progress[1])
                task_status = "completed" if int(progress[1]) == 0 else "running"
                conn.execute(
                    "UPDATE admin_training_reset_operations SET completed=%s,"
                    "changed=changed+%s,skipped=skipped+%s,status=%s,updated_at=%s "
                    "WHERE operation_id=%s",
                    (
                        completed,
                        changed,
                        skipped,
                        task_status,
                        updated_at,
                        operation_id,
                    ),
                )
                result = self._result(conn, operation_id, "applied")
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["TrainingResetResult", "TrainingResetService"]
