from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork
from .guishi_stone_rules import OP_AMOUNT_CAP, STORED_CAP, withdrawal_fee


@dataclass(frozen=True)
class GuishiWithdrawResult:
    status: str
    user_id: str
    amount: int = 0
    fee: int = 0
    actual_amount: int = 0
    stored_balance: int = 0

    @property
    def operation_type(self) -> str:
        return "withdraw"

    @property
    def succeeded(self) -> bool:
        return self.status in {"completed", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "completed"


class GuishiWithdrawSqlRepository:
    """Atomically withdraw Guishi stones into the player's wallet."""

    STORED_CAP = STORED_CAP
    OP_AMOUNT_CAP = OP_AMOUNT_CAP

    def __init__(self, game_database: str | Path, trade_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.trade_database = str(trade_database)

    @staticmethod
    def _payload(user_id: str, amount: int) -> str:
        return json.dumps(
            {"amount": amount, "operation_type": "withdraw", "user_id": user_id},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _legacy_operation(uow: DatabaseUnitOfWork, operation_id: str):
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE type='table' AND name='guishi_stone_operations'"
        )
        if table is None:
            return None
        return uow.query_one(
            "SELECT operation_type,user_id,amount,fee,actual_amount,stored_balance "
            "FROM guishi_stone_operations WHERE operation_id=?",
            (operation_id,),
        )

    def withdraw(
        self,
        *,
        operation_id: str,
        user_id: str,
        amount: int,
        withdrawal_open: bool,
    ) -> GuishiWithdrawResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        try:
            amount = int(amount)
        except (TypeError, ValueError) as exc:
            raise ValueError("amount must be an integer") from exc
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        if amount <= 0:
            raise ValueError("amount must be positive")
        if amount > self.OP_AMOUNT_CAP:
            return GuishiWithdrawResult("amount_capped", user_id, amount)

        payload = self._payload(user_id, amount)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.trade_database, "guishi_trade")
            previous = uow.query_one(
                "SELECT payload,amount,fee,actual_amount,stored_balance "
                "FROM trade_guishi_withdraw_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return GuishiWithdrawResult("operation_conflict", user_id, amount)
                return GuishiWithdrawResult(
                    "duplicate",
                    user_id,
                    int(previous["amount"]),
                    int(previous["fee"]),
                    int(previous["actual_amount"]),
                    int(previous["stored_balance"]),
                )

            legacy = self._legacy_operation(uow, operation_id)
            if legacy is not None:
                if (
                    str(legacy["operation_type"]) != "withdraw"
                    or str(legacy["user_id"]) != user_id
                    or int(legacy["amount"]) != amount
                ):
                    return GuishiWithdrawResult("operation_conflict", user_id, amount)
                return GuishiWithdrawResult(
                    "duplicate",
                    user_id,
                    int(legacy["amount"]),
                    int(legacy["fee"]),
                    int(legacy["actual_amount"]),
                    int(legacy["stored_balance"]),
                )

            if not withdrawal_open:
                return GuishiWithdrawResult("weekend_closed", user_id, amount)

            player = uow.query_one(
                "SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (user_id,)
            )
            if player is None:
                return GuishiWithdrawResult("user_missing", user_id, amount)
            stored = uow.query_one(
                "SELECT COALESCE(stored_stone,0) AS stored_stone "
                "FROM guishi_trade.guishi_info WHERE user_id=?",
                (user_id,),
            )
            stored_balance = int(stored["stored_stone"]) if stored is not None else 0
            effective_balance = min(stored_balance, self.STORED_CAP)
            if effective_balance < amount:
                return GuishiWithdrawResult(
                    "stored_insufficient", user_id, amount, stored_balance=effective_balance
                )

            fee = withdrawal_fee(effective_balance, amount)
            actual_amount = amount - fee
            credited = uow.execute(
                "UPDATE user_xiuxian "
                "SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(? AS REAL) "
                "WHERE user_id=?",
                (actual_amount, user_id),
            )
            if credited.rowcount != 1:
                return GuishiWithdrawResult(
                    "state_changed", user_id, amount, fee, actual_amount, effective_balance
                )
            new_balance = effective_balance - amount
            uow.execute(
                "INSERT INTO guishi_trade.guishi_info(user_id,stored_stone,items) "
                "VALUES(?,?, '{}') ON CONFLICT(user_id) DO UPDATE SET "
                "stored_stone=excluded.stored_stone",
                (user_id, new_balance),
            )
            uow.execute(
                "INSERT INTO trade_guishi_withdraw_operations("
                "operation_id,payload,user_id,amount,fee,actual_amount,stored_balance) "
                "VALUES(?,?,?,?,?,?,?)",
                (operation_id, payload, user_id, amount, fee, actual_amount, new_balance),
            )
            return GuishiWithdrawResult(
                "completed", user_id, amount, fee, actual_amount, new_balance
            )


__all__ = ["GuishiWithdrawResult", "GuishiWithdrawSqlRepository"]
