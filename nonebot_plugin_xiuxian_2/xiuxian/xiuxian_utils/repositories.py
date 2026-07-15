from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any


class UserRepository:
    """Read access for the main user table."""

    def __init__(
        self,
        read_query: Callable[..., Any],
        connection: Callable[[], AbstractContextManager[Any]],
        build_real_user: Callable[[Any, Any], Any],
    ) -> None:
        self._read_query = read_query
        self._connection = connection
        self._build_real_user = build_real_user

    def get_by_id(self, user_id: str):
        from .numeric_bind import normalize_user_row

        return normalize_user_row(
            self._read_query(
                "SELECT * FROM user_xiuxian WHERE user_id=%s",
                (user_id,),
                one=True,
                dict_row=True,
            )
        )

    def get_by_name(self, user_name: str):
        from .numeric_bind import normalize_user_row

        return normalize_user_row(
            self._read_query(
                "SELECT * FROM user_xiuxian WHERE user_name=%s",
                (user_name,),
                one=True,
                dict_row=True,
            )
        )

    def get_with_attributes(self, user_id: str):
        from .numeric_bind import normalize_user_row

        with self._connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT * FROM user_xiuxian WHERE user_id=%s",
                (user_id,),
            )
            result = cur.fetchone()
            if result is None:
                return None
            return normalize_user_row(self._build_real_user(result, cur.description))


class EconomyRepository:
    """Atomic stone and experience mutations for the main user table."""

    def __init__(
        self,
        connection: Callable[[], AbstractContextManager[Any]],
        normalize_amount: Callable[[Any], int],
        log_change: Callable[..., Any],
    ) -> None:
        self._connection = connection
        self._normalize_amount = normalize_amount
        self._log_change = log_change

    def update_stones(self, user_id, amount, operation, log_context=None) -> None:
        amount = abs(int(amount))
        current_stones = None
        stone_delta = 0

        with self._connection() as conn:
            cur = conn.cursor()
            if log_context:
                cur.execute(
                    "SELECT stone FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                )
                row = cur.fetchone()
                if row:
                    current_stones = int(row[0] or 0)

            if operation == 1:
                cur.execute(
                    "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                    (amount, user_id),
                )
                if cur.rowcount > 0:
                    stone_delta = amount
            elif operation == 2:
                cur.execute(
                    "UPDATE user_xiuxian SET stone=GREATEST(stone-%s, 0) WHERE user_id=%s",
                    (amount, user_id),
                )
                if cur.rowcount > 0:
                    stone_delta = -min(
                        current_stones if current_stones is not None else amount,
                        amount,
                    )
            conn.commit()

        if log_context and stone_delta:
            self._log_change(
                log_context,
                user_id=user_id,
                default_action="stone_add" if operation == 1 else "stone_cost",
                stone_delta=stone_delta,
                detail={
                    "asset": "stone",
                    "method": "update_ls",
                    "key": operation,
                    "requested_amount": amount,
                },
            )

    def try_update_stones(self, user_id, amount, operation, log_context=None) -> bool:
        amount = abs(int(amount))
        if amount <= 0:
            return True

        with self._connection() as conn:
            cur = conn.cursor()
            if operation == 1:
                cur.execute(
                    "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                    (amount, user_id),
                )
            elif operation == 2:
                cur.execute(
                    """
                    UPDATE user_xiuxian
                    SET stone=stone-%s
                    WHERE user_id=%s AND COALESCE(stone, 0) >= %s
                    """,
                    (amount, user_id, amount),
                )
            else:
                return False
            success = cur.rowcount > 0
            conn.commit()

        if log_context and success:
            self._log_change(
                log_context,
                user_id=user_id,
                default_action="stone_add" if operation == 1 else "stone_cost",
                stone_delta=amount if operation == 1 else -amount,
                detail={
                    "asset": "stone",
                    "method": "try_update_ls",
                    "key": operation,
                    "requested_amount": amount,
                },
            )
        return success

    def transfer_stones(
        self,
        operation_id: str,
        sender_id: str,
        recipient_id: str,
        amount: int,
    ) -> bool:
        """Atomically transfer stones; repeated operation IDs are no-ops."""
        amount = abs(int(amount))
        if amount <= 0 or str(sender_id) == str(recipient_id):
            return False

        with self._connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS economy_operations (
                    operation_id TEXT PRIMARY KEY,
                    operation_type TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(
                "SELECT operation_id FROM economy_operations WHERE operation_id=%s",
                (operation_id,),
            )
            if cur.fetchone() is not None:
                return False
            cur.execute(
                """
                UPDATE user_xiuxian SET stone=stone-%s
                WHERE user_id=%s AND COALESCE(stone, 0) >= %s
                """,
                (amount, sender_id, amount),
            )
            if cur.rowcount != 1:
                return False
            cur.execute(
                "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                (amount, recipient_id),
            )
            if cur.rowcount != 1:
                raise ValueError("recipient does not exist")
            cur.execute(
                "INSERT INTO economy_operations (operation_id, operation_type) VALUES (%s, %s)",
                (operation_id, "stone_transfer"),
            )
            conn.commit()
        return True

    def add_experience(self, user_id, amount) -> None:
        self._update_experience(user_id, amount, subtract=False)

    def subtract_experience(self, user_id, amount) -> None:
        self._update_experience(user_id, amount, subtract=True)

    def _update_experience(self, user_id, amount, *, subtract: bool) -> None:
        amount = self._normalize_amount(amount)
        with self._connection() as conn:
            cur = conn.cursor()
            if subtract:
                cur.execute(
                    "UPDATE user_xiuxian SET exp=GREATEST(exp-%s, 0) WHERE user_id=%s",
                    (amount, user_id),
                )
            else:
                cur.execute(
                    "UPDATE user_xiuxian SET exp=exp+%s WHERE user_id=%s",
                    (amount, user_id),
                )
            conn.commit()
