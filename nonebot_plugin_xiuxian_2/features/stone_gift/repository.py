from __future__ import annotations

from ...core.numeric import as_int_like
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .domain import StoneGiftRecord
from .schema import (
    LIMITS_COLUMNS,
    OPERATIONS_COLUMNS,
    SCHEMA_MISSING,
    table_ready,
)


class TransferStatus(str):
    def __new__(cls, status, reason="", subject=""):
        value = super().__new__(cls, status)
        value.reason, value.subject = reason, subject
        return value


class _TransferRejected(Exception):
    def __init__(self, status):
        self.status = status


class StoneGiftRepository:
    """Owns the game database projection for spirit-stone transfers."""

    def find_user(self, uow: DatabaseUnitOfWork, identifier: str) -> dict[str, Any] | None:
        """Resolve a command target by stable ID first, then display name."""
        value = str(identifier).strip()
        if not value:
            return None
        row = uow.query_one(
            "SELECT user_id, user_name, level FROM user_xiuxian "
            "WHERE CAST(user_id AS TEXT) = ? OR user_name = ? LIMIT 1",
            (value, value),
        )
        return dict(row) if row is not None else None

    def limits(self, uow: DatabaseUnitOfWork, transfer_date: str, user_ids: tuple[str, ...]) -> dict[str, dict[str, int]]:
        result = {str(user_id): {"sent": 0, "received": 0} for user_id in user_ids}
        if not result:
            return result
        if not table_ready(uow, "stone_gift_limits", LIMITS_COLUMNS):
            return result
        placeholders = ",".join("?" for _ in result)
        rows = uow.query_all(
            f"SELECT user_id, sent_amount, received_amount FROM stone_gift_limits WHERE limit_date = ? AND user_id IN ({placeholders})",
            (transfer_date, *result),
        )
        for row in rows:
            result[str(row["user_id"])] = {"sent": as_int_like(row["sent_amount"] or 0), "received": as_int_like(row["received_amount"] or 0)}
        return result

    @staticmethod
    def _record(row: dict[str, Any] | None) -> StoneGiftRecord | None:
        if row is None:
            return None
        return StoneGiftRecord(
            str(row["operation_id"]),
            str(row["sender_id"]),
            str(row["recipient_id"]),
            as_int_like(row["gross_amount"]),
            as_int_like(row["net_amount"]),
            as_int_like(row["fee_amount"]),
        )

    def operation(self, uow: DatabaseUnitOfWork, operation_id: str) -> StoneGiftRecord | None:
        if not table_ready(uow, "stone_gift_operations", OPERATIONS_COLUMNS):
            return None
        row = uow.query_one(
            "SELECT operation_id, sender_id, recipient_id, gross_amount, net_amount, fee_amount "
            "FROM stone_gift_operations WHERE operation_id = ?",
            (operation_id,),
        )
        return self._record(row)

    def snapshot(self, uow: DatabaseUnitOfWork, user_ids: tuple[str, ...]) -> dict[str, dict[str, int]]:
        unique_ids = tuple(dict.fromkeys(str(value) for value in user_ids))
        result = {user_id: {"rows": 0, "stone": 0} for user_id in unique_ids}
        if not unique_ids:
            return result
        placeholders = ",".join("?" for _ in unique_ids)
        rows = uow.query_all(
            f"SELECT user_id,stone FROM user_xiuxian WHERE user_id IN ({placeholders})",
            unique_ids,
        )
        for row in rows:
            account = result[str(row["user_id"])]
            account["rows"] += 1
            account["stone"] += as_int_like(row["stone"])
        return result

    def transfer(
        self, uow: DatabaseUnitOfWork, record: StoneGiftRecord, **policy: Any,
    ) -> str:
        try:
            with uow.savepoint("stone_gift_complete"):
                status = self._transfer(uow, record, **policy)
                if status != "transferred":
                    raise _TransferRejected(status)
                return status
        except _TransferRejected as exc:
            return exc.status

    def _transfer(
        self,
        uow: DatabaseUnitOfWork,
        record: StoneGiftRecord,
        *,
        transfer_date: str | None = None,
        send_limit: int | None = None,
        receive_limit: int | None = None,
        send_used: int | None = None,
        receive_used: int | None = None,
    ) -> str:
        baseline_sent = 0
        baseline_received = 0
        sender_limit_exists = False
        recipient_limit_exists = False
        if not table_ready(uow, "stone_gift_operations", OPERATIONS_COLUMNS):
            return SCHEMA_MISSING
        if transfer_date is not None:
            if not table_ready(uow, "stone_gift_limits", LIMITS_COLUMNS):
                return SCHEMA_MISSING
            rows = uow.query_all(
                "SELECT user_id, sent_amount, received_amount FROM stone_gift_limits "
                "WHERE limit_date = ? AND user_id IN (?, ?)",
                (transfer_date, record.sender_id, record.recipient_id),
            )
            totals = {str(row["user_id"]): row for row in rows}
            sender_limit_exists = record.sender_id in totals
            recipient_limit_exists = record.recipient_id in totals
            # Legacy player.db stored today's counters without a date.  Keep
            # them in memory until the transfer succeeds so a rejection does
            # not itself mutate the new projection.
            baseline_sent = as_int_like(totals.get(record.sender_id, {}).get("sent_amount", send_used or 0))
            baseline_received = as_int_like(totals.get(record.recipient_id, {}).get("received_amount", receive_used or 0))
            sent = baseline_sent
            received = baseline_received
            if send_limit is not None and sent + record.gross_amount > send_limit:
                return "send_limit_reached"
            if receive_limit is not None and received + record.net_amount > receive_limit:
                return "receive_limit_reached"
        recipient = uow.query_one("SELECT 1 AS present FROM user_xiuxian WHERE user_id = ?", (record.recipient_id,))
        if recipient is None:
            return "recipient_missing"
        accounts = self.snapshot(uow, (record.sender_id, record.recipient_id))
        for uid, subject in ((record.sender_id, "sender"), (record.recipient_id, "recipient")):
            if accounts[uid]["rows"] != 1:
                return TransferStatus("state_changed", "account_rows_inconsistent", subject)
        sender_stone = accounts[record.sender_id]["stone"]
        recipient_stone = accounts[record.recipient_id]["stone"]
        if sender_stone < record.gross_amount:
            return TransferStatus("stone_insufficient", "stone_insufficient", "sender")
        expected = {record.sender_id: sender_stone - record.gross_amount,
                    record.recipient_id: recipient_stone + record.net_amount}
        try:
            with uow.savepoint("stone_gift_assets"):
                charged = uow.execute(
                    "UPDATE user_xiuxian SET stone = ? "
                    "WHERE user_id = ? AND xiuxian_int_cmp(COALESCE(stone, 0),?)=0",
                    (expected[record.sender_id], record.sender_id, sender_stone),
                )
                if charged.rowcount != 1:
                    sender = uow.query_all("SELECT stone FROM user_xiuxian WHERE user_id=?", (record.sender_id,))
                    reason = "sender_missing" if not sender else "account_rows_inconsistent" if len(sender) != 1 else "stone_insufficient" if as_int_like(sender[0]["stone"] or 0) < record.gross_amount else "stone_write_rejected"
                    raise _TransferRejected(TransferStatus("stone_insufficient", reason, "sender"))
                credited = uow.execute(
                    "UPDATE user_xiuxian SET stone = ? WHERE user_id = ? AND xiuxian_int_cmp(COALESCE(stone,0),?)=0",
                    (expected[record.recipient_id], record.recipient_id, recipient_stone),
                )
                if credited.rowcount != 1:
                    raise _TransferRejected(TransferStatus("state_changed", "stone_write_rejected", "recipient"))
                saved = self.snapshot(uow, (record.sender_id, record.recipient_id))
                for uid, subject in ((record.sender_id, "sender"), (record.recipient_id, "recipient")):
                    if saved[uid]["stone"] != expected[uid]:
                        raise _TransferRejected(TransferStatus("state_changed", "stone_write_rejected", subject))
        except _TransferRejected as exc:
            return exc.status
        if transfer_date is not None:
            sent_initial = baseline_sent if not sender_limit_exists else 0
            received_initial = baseline_received if not recipient_limit_exists else 0
            sent_write = uow.execute(
                "INSERT INTO stone_gift_limits(limit_date, user_id, sent_amount, received_amount) "
                "VALUES (?, ?, ?, 0) ON CONFLICT(limit_date, user_id) DO UPDATE SET "
                "sent_amount = sent_amount + excluded.sent_amount, updated_at = CURRENT_TIMESTAMP",
                (transfer_date, record.sender_id, sent_initial + record.gross_amount),
            )
            if sent_write.rowcount != 1:
                return TransferStatus("state_changed", "gift_send_limit_write_rejected", "sender")
            received_write = uow.execute(
                "INSERT INTO stone_gift_limits(limit_date, user_id, sent_amount, received_amount) "
                "VALUES (?, ?, 0, ?) ON CONFLICT(limit_date, user_id) DO UPDATE SET "
                "received_amount = received_amount + excluded.received_amount, updated_at = CURRENT_TIMESTAMP",
                (transfer_date, record.recipient_id, received_initial + record.net_amount),
            )
            if received_write.rowcount != 1:
                return TransferStatus("state_changed", "gift_receive_limit_write_rejected", "recipient")
        receipt_write = uow.execute(
            "INSERT INTO stone_gift_operations(operation_id, sender_id, recipient_id, gross_amount, net_amount, fee_amount) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                record.operation_id,
                record.sender_id,
                record.recipient_id,
                record.gross_amount,
                record.net_amount,
                record.fee_amount,
            ),
        )
        if receipt_write.rowcount != 1:
            return TransferStatus("state_changed", "gift_receipt_write_rejected", "sender")
        return "transferred"
