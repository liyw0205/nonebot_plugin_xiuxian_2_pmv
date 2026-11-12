from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any


def _numeric_bind():
    """Load numeric helpers both as a package and in source-file tests."""
    try:
        from . import numeric_bind

        return numeric_bind
    except ImportError:
        import importlib.util
        from pathlib import Path

        path = Path(__file__).with_name("numeric_bind.py")
        spec = importlib.util.spec_from_file_location("_xiuxian_numeric_bind", path)
        if spec is None or spec.loader is None:
            raise ImportError(f"无法加载数值绑定模块: {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


def _economy_operations_bind():
    """Load the transfer receipt schema both as a package and in source-file tests."""
    try:
        from ...infrastructure.database import economy_operations

        return economy_operations
    except ImportError:
        import importlib.util
        from pathlib import Path

        path = (
            Path(__file__).resolve().parents[2]
            / "infrastructure"
            / "database"
            / "economy_operations.py"
        )
        spec = importlib.util.spec_from_file_location(
            "_xiuxian_economy_operations", path
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"无法加载经济回执 schema 模块: {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


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
        normalize_user_row = _numeric_bind().normalize_user_row

        # 若历史脏数据出现重复 user_id，固定取最早一行（id 最小），避免行为抖动
        return normalize_user_row(
            self._read_query(
                "SELECT * FROM user_xiuxian WHERE user_id=%s ORDER BY rowid ASC LIMIT 1",
                (user_id,),
                one=True,
                dict_row=True,
            )
        )

    def get_by_name(self, user_name: str):
        normalize_user_row = _numeric_bind().normalize_user_row

        return normalize_user_row(
            self._read_query(
                "SELECT * FROM user_xiuxian WHERE user_name=%s ORDER BY rowid ASC LIMIT 1",
                (user_name,),
                one=True,
                dict_row=True,
            )
        )

    def get_with_attributes(self, user_id: str):
        normalize_user_row = _numeric_bind().normalize_user_row

        with self._connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT * FROM user_xiuxian WHERE user_id=%s ORDER BY rowid ASC LIMIT 1",
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
        numeric_bind = _numeric_bind()
        exact = numeric_bind._core_numeric.require_exact_integer
        amount_int = abs(exact(amount, field="stone_amount", subject=str(user_id)))
        stone_delta = 0
        if operation not in (1, 2):
            return
        with self._connection() as conn:
            cur = conn.cursor()
            if not conn.in_transaction:
                cur.execute("BEGIN IMMEDIATE")
            cur.execute("SELECT rowid,stone FROM user_xiuxian WHERE user_id=%s", (user_id,))
            rows = [(row[0], exact(row[1], field="stone", subject=str(user_id))) for row in cur.fetchall()]
            for rowid, before in rows:
                after = before + amount_int if operation == 1 else max(before - amount_int, 0)
                self._write_stones(cur, rowid, user_id, before, after)
                stone_delta += after - before
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
                    "requested_amount": amount_int,
                },
            )

    def try_update_stones(self, user_id, amount, operation, log_context=None) -> bool:
        numeric_bind = _numeric_bind()
        exact = numeric_bind._core_numeric.require_exact_integer
        amount_int = abs(exact(amount, field="stone_amount", subject=str(user_id)))
        if amount_int <= 0:
            return True
        if operation not in (1, 2):
            return False
        with self._connection() as conn:
            cur = conn.cursor()
            if not conn.in_transaction:
                cur.execute("BEGIN IMMEDIATE")
            cur.execute("SELECT rowid,stone FROM user_xiuxian WHERE user_id=%s", (user_id,))
            rows = [(row[0], exact(row[1], field="stone", subject=str(user_id))) for row in cur.fetchall()]
            if not rows or (operation == 2 and any(before < amount_int for _, before in rows)):
                return False
            for rowid, before in rows:
                self._write_stones(cur, rowid, user_id, before, before + amount_int if operation == 1 else before - amount_int)
            success = True
            conn.commit()

        if log_context and success:
            self._log_change(
                log_context,
                user_id=user_id,
                default_action="stone_add" if operation == 1 else "stone_cost",
                stone_delta=amount_int if operation == 1 else -amount_int,
                detail={
                    "asset": "stone",
                    "method": "try_update_ls",
                    "key": operation,
                    "requested_amount": amount_int,
                },
            )
        return success

    @staticmethod
    def _write_stones(cur, rowid, user_id, before, after):
        numeric = _numeric_bind()._core_numeric
        cur.execute("UPDATE user_xiuxian SET stone=xiuxian_int_store(%s,'stone','actor') WHERE rowid=%s AND user_id=%s "
                    "AND xiuxian_int_cmp(COALESCE(stone,0),%s,1,'stone','actor')=0",
                    (numeric.bind_sqlite_param(after, exact=True), rowid, user_id,
                     numeric.bind_sqlite_param(before, exact=True)))
        if cur.rowcount != 1:
            from nonebot_plugin_xiuxian_2.core.errors import DomainError
            raise DomainError("stone_write_rejected", "stone snapshot write rejected",
                              {"reason": "stone_write_rejected", "subject": str(user_id)})
        cur.execute("SELECT stone FROM user_xiuxian WHERE rowid=%s", (rowid,))
        row = cur.fetchone()
        if row is None or numeric.require_exact_integer(row[0], field="stone", subject=str(user_id)) != after:
            from nonebot_plugin_xiuxian_2.core.errors import DomainError
            raise DomainError("stone_write_rejected", "stone persisted balance differs",
                              {"reason": "stone_write_rejected", "subject": str(user_id)})

    def transfer_stones(
        self,
        operation_id: str,
        sender_id: str,
        recipient_id: str,
        amount: int,
    ) -> bool:
        """Atomically transfer stones; repeated operation IDs are no-ops."""
        numeric_bind = _numeric_bind()
        exact = numeric_bind._core_numeric.require_exact_integer
        amount_int = abs(exact(amount, field="stone_amount", subject=str(sender_id)))
        if amount_int <= 0 or str(sender_id) == str(recipient_id):
            return False

        schema = _economy_operations_bind()
        with self._connection() as conn:
            if not schema.operations_ready(conn):
                return False
            cur = conn.cursor()
            if not conn.in_transaction:
                cur.execute("BEGIN IMMEDIATE")
            cur.execute(
                "SELECT operation_id FROM economy_operations WHERE operation_id=%s",
                (operation_id,),
            )
            if cur.fetchone() is not None:
                return False
            accounts = {}
            for uid in (sender_id, recipient_id):
                cur.execute("SELECT rowid,stone FROM user_xiuxian WHERE user_id=%s", (uid,))
                rows = cur.fetchall()
                if len(rows) != 1:
                    if uid == recipient_id and not rows:
                        raise ValueError("recipient does not exist")
                    return False
                accounts[uid] = (rows[0][0], exact(rows[0][1], field="stone", subject=str(uid)))
            if accounts[sender_id][1] < amount_int:
                return False
            for uid, delta in ((sender_id, -amount_int), (recipient_id, amount_int)):
                rowid, before = accounts[uid]
                self._write_stones(cur, rowid, uid, before, before + delta)
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
                    "UPDATE user_xiuxian SET exp=MAX(CAST(COALESCE(exp,0) AS REAL)-CAST(%s AS REAL), 0) WHERE user_id=%s",
                    (amount, user_id),
                )
            else:
                cur.execute(
                    "UPDATE user_xiuxian SET exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s",
                    (amount, user_id),
                )
            conn.commit()
