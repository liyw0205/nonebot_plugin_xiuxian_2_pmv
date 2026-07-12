from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class StoneContestResult:
    status: str
    payer_id: str
    receiver_id: str
    requested_amount: int
    transferred_amount: int
    payer_balance: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"transferred", "duplicate"}


class StoneContestService:
    """Transfer contested stones between two players atomically and idempotently."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _result(status, payer_id, receiver_id, requested_amount,
                transferred_amount=0, payer_balance=0):
        return StoneContestResult(
            status, payer_id, receiver_id, int(requested_amount),
            int(transferred_amount), int(payer_balance),
        )

    def transfer(self, operation_id, payer_id, receiver_id, requested_amount):
        operation_id = str(operation_id).strip()
        payer_id = str(payer_id)
        receiver_id = str(receiver_id)
        requested_amount = int(requested_amount)
        if not operation_id or requested_amount <= 0 or payer_id == receiver_id:
            raise ValueError("valid operation, distinct players and positive amount are required")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS stone_contest_operations ("
                    "operation_id TEXT PRIMARY KEY, payer_id TEXT NOT NULL, receiver_id TEXT NOT NULL, "
                    "requested_amount INTEGER NOT NULL, transferred_amount INTEGER NOT NULL, "
                    "payer_balance INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payer_id, receiver_id, requested_amount, transferred_amount, payer_balance "
                    "FROM stone_contest_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if (
                        str(previous[0]) != payer_id
                        or str(previous[1]) != receiver_id
                        or int(previous[2]) != requested_amount
                    ):
                        return self._result("state_changed", payer_id, receiver_id, requested_amount)
                    return self._result(
                        "duplicate", payer_id, receiver_id, requested_amount,
                        previous[3], previous[4],
                    )

                payer = conn.execute(
                    "SELECT COALESCE(stone, 0) FROM user_xiuxian WHERE user_id=%s", (payer_id,),
                ).fetchone()
                receiver = conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (receiver_id,),
                ).fetchone()
                if payer is None or receiver is None:
                    conn.rollback()
                    return self._result("user_missing", payer_id, receiver_id, requested_amount)
                payer_balance = max(0, int(payer[0]))
                transferred = min(requested_amount, payer_balance)
                if transferred <= 0:
                    conn.rollback()
                    return self._result("payer_empty", payer_id, receiver_id, requested_amount)

                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone>=%s",
                    (transferred, payer_id, transferred),
                )
                credited = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                    (transferred, receiver_id),
                )
                if charged.rowcount != 1 or credited.rowcount != 1:
                    conn.rollback()
                    return self._result("state_changed", payer_id, receiver_id, requested_amount)
                new_balance = payer_balance - transferred
                conn.execute(
                    "INSERT INTO stone_contest_operations "
                    "(operation_id, payer_id, receiver_id, requested_amount, transferred_amount, payer_balance) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (operation_id, payer_id, receiver_id, requested_amount, transferred, new_balance),
                )
                conn.commit()
                return self._result(
                    "transferred", payer_id, receiver_id, requested_amount, transferred, new_balance
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["StoneContestResult", "StoneContestService"]
