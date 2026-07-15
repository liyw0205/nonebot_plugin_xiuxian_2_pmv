from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class StoneGiftResult:
    status: str
    sender_id: str
    recipient_id: str
    gross_amount: int
    net_amount: int
    fee_amount: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"transferred", "duplicate"}


class StoneGiftService:
    """Transfer stones with a fee atomically and idempotently."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stone_gift_operations (
                operation_id TEXT PRIMARY KEY,
                sender_id TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                gross_amount INTEGER NOT NULL,
                net_amount INTEGER NOT NULL,
                fee_amount INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def transfer(
        self,
        operation_id,
        sender_id,
        recipient_id,
        gross_amount,
        *,
        fee_rate=0.1,
    ) -> StoneGiftResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        sender_id = str(sender_id)
        recipient_id = str(recipient_id)
        gross_amount = int(gross_amount)
        fee_rate = float(fee_rate)
        if gross_amount <= 0:
            raise ValueError("gross_amount must be positive")
        if sender_id == recipient_id:
            raise ValueError("sender and recipient must differ")
        if not 0 <= fee_rate < 1:
            raise ValueError("fee_rate must be in [0, 1)")
        fee_amount = int(gross_amount * fee_rate)
        net_amount = gross_amount - fee_amount

        def result(status: str, gross=gross_amount, net=net_amount, fee=fee_amount):
            return StoneGiftResult(
                status,
                sender_id,
                recipient_id,
                int(gross),
                int(net),
                int(fee),
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT sender_id, recipient_id, gross_amount, net_amount, fee_amount "
                    "FROM stone_gift_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    previous_sender, previous_recipient, gross, net, fee = previous
                    if (
                        str(previous_sender) != sender_id
                        or str(previous_recipient) != recipient_id
                    ):
                        return result("state_changed")
                    return result("duplicate", gross, net, fee)

                recipient = conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s",
                    (recipient_id,),
                ).fetchone()
                if recipient is None:
                    conn.rollback()
                    return result("recipient_missing")
                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s "
                    "WHERE user_id=%s AND COALESCE(stone, 0)>=%s",
                    (gross_amount, sender_id, gross_amount),
                )
                if charged.rowcount != 1:
                    conn.rollback()
                    return result("stone_insufficient")
                credited = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                    (net_amount, recipient_id),
                )
                if credited.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO stone_gift_operations "
                    "(operation_id, sender_id, recipient_id, gross_amount, "
                    "net_amount, fee_amount) VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        sender_id,
                        recipient_id,
                        gross_amount,
                        net_amount,
                        fee_amount,
                    ),
                )
                conn.commit()
                return result("transferred")
            except Exception:
                conn.rollback()
                raise

    def get_result(self, operation_id: str) -> StoneGiftResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("stone_gift_operations",),
            ).fetchone()
            if exists is None:
                return None
            previous = conn.execute(
                "SELECT sender_id, recipient_id, gross_amount, net_amount, fee_amount "
                "FROM stone_gift_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return StoneGiftResult(
                "duplicate", str(previous[0]), str(previous[1]), int(previous[2]), int(previous[3]), int(previous[4])
            )

    def get_operation(
        self, operation_id, sender_id, recipient_id
    ) -> StoneGiftResult | None:
        operation_id = str(operation_id).strip()
        sender_id = str(sender_id)
        recipient_id = str(recipient_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("stone_gift_operations",),
            ).fetchone()
            if exists is None:
                return None
            previous = conn.execute(
                "SELECT sender_id, recipient_id, gross_amount, net_amount, fee_amount "
                "FROM stone_gift_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            previous_sender, previous_recipient, gross, net, fee = previous
            if str(previous_sender) != sender_id or str(previous_recipient) != recipient_id:
                return None
            return StoneGiftResult(
                "duplicate", sender_id, recipient_id, int(gross), int(net), int(fee)
            )


__all__ = ["StoneGiftResult", "StoneGiftService"]
