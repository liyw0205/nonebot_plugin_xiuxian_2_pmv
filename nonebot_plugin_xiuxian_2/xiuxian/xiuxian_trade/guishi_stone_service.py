from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class GuishiStoneResult:
    status: str
    operation_type: str
    user_id: str
    amount: int
    fee: int = 0
    actual_amount: int = 0
    stored_balance: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"completed", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "completed"


class GuishiStoneService:
    """Move stones between a player and the Guishi account atomically."""

    def __init__(
        self,
        game_database: str | Path,
        trade_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._trade_database = Path(trade_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS guishi_stone_operations (
                operation_id TEXT PRIMARY KEY,
                operation_type TEXT NOT NULL,
                user_id TEXT NOT NULL,
                amount INTEGER NOT NULL,
                fee INTEGER NOT NULL,
                actual_amount INTEGER NOT NULL,
                stored_balance INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS guishi_trade.guishi_info (
                user_id TEXT PRIMARY KEY,
                stored_stone INTEGER DEFAULT 0,
                items TEXT DEFAULT '{}'
            )
            """
        )

    @staticmethod
    def _fee_for_balance(stored_balance: int, amount: int) -> int:
        fee_rate = 0.2
        if stored_balance > 10_000_000_000:
            excess = stored_balance - 10_000_000_000
            fee_rate += (excess // 10_000_000_000) * 0.05
        return int(amount * min(fee_rate, 0.8))

    @staticmethod
    def _result_from_row(status: str, row) -> GuishiStoneResult:
        operation_type, user_id, amount, fee, actual_amount, balance = row
        return GuishiStoneResult(
            status,
            str(operation_type),
            str(user_id),
            int(amount),
            int(fee),
            int(actual_amount),
            int(balance),
        )

    def _execute(self, operation_id, operation_type, user_id, amount) -> GuishiStoneResult:
        operation_id = str(operation_id).strip()
        operation_type = str(operation_type)
        user_id = str(user_id)
        amount = int(amount)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if operation_type not in {"deposit", "withdraw"}:
            raise ValueError("unsupported operation_type")
        if amount <= 0:
            raise ValueError("amount must be positive")

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS guishi_trade", (str(self._trade_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT operation_type, user_id, amount, fee, actual_amount, "
                    "stored_balance FROM guishi_stone_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != operation_type or str(previous[1]) != user_id:
                        return GuishiStoneResult(
                            "state_changed", operation_type, user_id, amount
                        )
                    return self._result_from_row("duplicate", previous)

                player = conn.execute(
                    "SELECT COALESCE(stone, 0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if player is None:
                    conn.rollback()
                    return GuishiStoneResult("player_missing", operation_type, user_id, amount)

                stored = conn.execute(
                    "SELECT COALESCE(stored_stone, 0) FROM guishi_trade.guishi_info "
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                stored_balance = int(stored[0]) if stored is not None else 0

                if operation_type == "deposit":
                    charged = conn.execute(
                        "UPDATE user_xiuxian SET stone=stone-%s "
                        "WHERE user_id=%s AND COALESCE(stone, 0)>=%s",
                        (amount, user_id, amount),
                    )
                    if charged.rowcount != 1:
                        conn.rollback()
                        return GuishiStoneResult(
                            "stone_insufficient", operation_type, user_id, amount,
                            stored_balance=stored_balance,
                        )
                    new_balance = stored_balance + amount
                    fee = 0
                    actual_amount = amount
                else:
                    if stored_balance < amount:
                        conn.rollback()
                        return GuishiStoneResult(
                            "stored_insufficient", operation_type, user_id, amount,
                            stored_balance=stored_balance,
                        )
                    fee = self._fee_for_balance(stored_balance, amount)
                    actual_amount = amount - fee
                    new_balance = stored_balance - amount
                    conn.execute(
                        "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                        (actual_amount, user_id),
                    )

                conn.execute(
                    """
                    INSERT INTO guishi_trade.guishi_info (user_id, stored_stone, items)
                    VALUES (%s, %s, '{}')
                    ON CONFLICT (user_id) DO UPDATE SET stored_stone=EXCLUDED.stored_stone
                    """,
                    (user_id, new_balance),
                )
                conn.execute(
                    "INSERT INTO guishi_stone_operations "
                    "(operation_id, operation_type, user_id, amount, fee, "
                    "actual_amount, stored_balance) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        operation_type,
                        user_id,
                        amount,
                        fee,
                        actual_amount,
                        new_balance,
                    ),
                )
                conn.commit()
                return GuishiStoneResult(
                    "completed",
                    operation_type,
                    user_id,
                    amount,
                    fee,
                    actual_amount,
                    new_balance,
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE guishi_trade")

    def deposit(self, operation_id, user_id, amount) -> GuishiStoneResult:
        return self._execute(operation_id, "deposit", user_id, amount)

    def withdraw(self, operation_id, user_id, amount) -> GuishiStoneResult:
        return self._execute(operation_id, "withdraw", user_id, amount)


__all__ = ["GuishiStoneResult", "GuishiStoneService"]
