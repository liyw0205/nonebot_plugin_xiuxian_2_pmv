from __future__ import annotations

from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .domain import StoneGiftRecord


class StoneGiftRepository:
    """Owns the game database projection for spirit-stone transfers."""

    def ensure_schema(self, uow: DatabaseUnitOfWork) -> None:
        uow.execute(
            """
            CREATE TABLE IF NOT EXISTS stone_gift_operations (
                operation_id TEXT PRIMARY KEY,
                sender_id TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                gross_amount INTEGER NOT NULL,
                net_amount INTEGER NOT NULL,
                fee_amount INTEGER NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def ensure_limit_schema(self, uow: DatabaseUnitOfWork) -> None:
        uow.execute(
            """
            CREATE TABLE IF NOT EXISTS stone_gift_limits (
                limit_date TEXT NOT NULL,
                user_id TEXT NOT NULL,
                sent_amount INTEGER NOT NULL DEFAULT 0,
                received_amount INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(limit_date, user_id)
            )
            """
        )

    def find_user(self, uow: DatabaseUnitOfWork, identifier: str) -> dict[str, Any] | None:
        """Resolve a command target by stable ID first, then display name."""
        value = str(identifier).strip()
        if not value:
            return None
        row = uow.query_one(
            "SELECT user_id, user_name FROM user_xiuxian "
            "WHERE CAST(user_id AS TEXT) = ? OR user_name = ? LIMIT 1",
            (value, value),
        )
        return dict(row) if row is not None else None

    def limits(self, uow: DatabaseUnitOfWork, transfer_date: str, user_ids: tuple[str, ...]) -> dict[str, dict[str, int]]:
        self.ensure_limit_schema(uow)
        result = {str(user_id): {"sent": 0, "received": 0} for user_id in user_ids}
        if not result:
            return result
        placeholders = ",".join("?" for _ in result)
        rows = uow.query_all(
            f"SELECT user_id, sent_amount, received_amount FROM stone_gift_limits WHERE limit_date = ? AND user_id IN ({placeholders})",
            (transfer_date, *result),
        )
        for row in rows:
            result[str(row["user_id"])] = {"sent": int(row["sent_amount"] or 0), "received": int(row["received_amount"] or 0)}
        return result

    @staticmethod
    def _record(row: dict[str, Any] | None) -> StoneGiftRecord | None:
        if row is None:
            return None
        return StoneGiftRecord(
            str(row["operation_id"]),
            str(row["sender_id"]),
            str(row["recipient_id"]),
            int(row["gross_amount"]),
            int(row["net_amount"]),
            int(row["fee_amount"]),
        )

    def operation(self, uow: DatabaseUnitOfWork, operation_id: str) -> StoneGiftRecord | None:
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
            f"SELECT user_id, COUNT(*) AS rows, "
            f"CAST(COALESCE(SUM(CAST(COALESCE(stone, 0) AS REAL)), 0) AS INTEGER) AS stone "
            f"FROM user_xiuxian WHERE user_id IN ({placeholders}) GROUP BY user_id",
            unique_ids,
        )
        for row in rows:
            result[str(row["user_id"])] = {"rows": int(row["rows"]), "stone": int(row["stone"] or 0)}
        return result

    def transfer(
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
        if transfer_date is not None:
            self.ensure_limit_schema(uow)
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
            baseline_sent = int(totals.get(record.sender_id, {}).get("sent_amount", send_used or 0))
            baseline_received = int(totals.get(record.recipient_id, {}).get("received_amount", receive_used or 0))
            sent = baseline_sent
            received = baseline_received
            if send_limit is not None and sent + record.gross_amount > send_limit:
                return "send_limit_reached"
            if receive_limit is not None and received + record.net_amount > receive_limit:
                return "receive_limit_reached"
        recipient = uow.query_one("SELECT 1 AS present FROM user_xiuxian WHERE user_id = ?", (record.recipient_id,))
        if recipient is None:
            return "recipient_missing"
        charged = uow.execute(
            "UPDATE user_xiuxian SET stone = CAST(COALESCE(stone, 0) AS REAL) - CAST(? AS REAL) "
            "WHERE user_id = ? AND CAST(COALESCE(stone, 0) AS REAL) >= CAST(? AS REAL)",
            (record.gross_amount, record.sender_id, record.gross_amount),
        )
        if charged.rowcount != 1:
            return "stone_insufficient"
        credited = uow.execute(
            "UPDATE user_xiuxian SET stone = CAST(COALESCE(stone, 0) AS REAL) + CAST(? AS REAL) WHERE user_id = ?",
            (record.net_amount, record.recipient_id),
        )
        if credited.rowcount != 1:
            return "state_changed"
        if transfer_date is not None:
            sent_initial = baseline_sent if not sender_limit_exists else 0
            received_initial = baseline_received if not recipient_limit_exists else 0
            uow.execute(
                "INSERT INTO stone_gift_limits(limit_date, user_id, sent_amount, received_amount) "
                "VALUES (?, ?, ?, 0) ON CONFLICT(limit_date, user_id) DO UPDATE SET "
                "sent_amount = sent_amount + excluded.sent_amount, updated_at = CURRENT_TIMESTAMP",
                (transfer_date, record.sender_id, sent_initial + record.gross_amount),
            )
            uow.execute(
                "INSERT INTO stone_gift_limits(limit_date, user_id, sent_amount, received_amount) "
                "VALUES (?, ?, 0, ?) ON CONFLICT(limit_date, user_id) DO UPDATE SET "
                "received_amount = received_amount + excluded.received_amount, updated_at = CURRENT_TIMESTAMP",
                (transfer_date, record.recipient_id, received_initial + record.net_amount),
            )
        uow.execute(
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
        return "transferred"
