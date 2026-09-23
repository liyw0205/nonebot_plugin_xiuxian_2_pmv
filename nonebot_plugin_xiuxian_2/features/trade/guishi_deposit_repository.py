from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class GuishiDepositResult:
    status: str
    user_id: str
    amount: int = 0
    stored_balance: int = 0

    @property
    def operation_type(self) -> str:
        return "deposit"

    @property
    def fee(self) -> int:
        return 0

    @property
    def actual_amount(self) -> int:
        return self.amount

    @property
    def succeeded(self) -> bool:
        return self.status in {"completed", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "completed"


class GuishiDepositSqlRepository:
    """Atomically move player stones into the legacy Guishi account projection."""

    STORED_CAP = 1_000_000_000
    OP_AMOUNT_CAP = 1_000_000_000

    def __init__(self, game_database: str | Path, trade_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.trade_database = str(trade_database)

    @staticmethod
    def _payload(user_id: str, amount: int) -> str:
        return json.dumps(
            {"amount": amount, "operation_type": "deposit", "user_id": user_id},
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
            "SELECT operation_type,user_id,amount,stored_balance "
            "FROM guishi_stone_operations WHERE operation_id=?",
            (operation_id,),
        )

    def deposit(
        self, *, operation_id: str, user_id: str, amount: int
    ) -> GuishiDepositResult:
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
            return GuishiDepositResult("amount_capped", user_id, amount)

        payload = self._payload(user_id, amount)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.trade_database, "guishi_trade")
            previous = uow.query_one(
                "SELECT payload,amount,stored_balance "
                "FROM trade_guishi_deposit_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return GuishiDepositResult("operation_conflict", user_id, amount)
                return GuishiDepositResult(
                    "duplicate",
                    user_id,
                    int(previous["amount"]),
                    int(previous["stored_balance"]),
                )

            legacy = self._legacy_operation(uow, operation_id)
            if legacy is not None:
                if (
                    str(legacy["operation_type"]) != "deposit"
                    or str(legacy["user_id"]) != user_id
                    or int(legacy["amount"]) != amount
                ):
                    return GuishiDepositResult("operation_conflict", user_id, amount)
                return GuishiDepositResult(
                    "duplicate",
                    user_id,
                    int(legacy["amount"]),
                    int(legacy["stored_balance"]),
                )

            player = uow.query_one(
                "SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (user_id,)
            )
            if player is None:
                return GuishiDepositResult("user_missing", user_id, amount)
            stored = uow.query_one(
                "SELECT COALESCE(stored_stone,0) AS stored_stone "
                "FROM guishi_trade.guishi_info WHERE user_id=?",
                (user_id,),
            )
            stored_balance = int(stored["stored_stone"]) if stored is not None else 0
            # A rejected deposit must not persist a normalization that the old
            # transaction would have rolled back with the rejected request.
            stored_balance = min(stored_balance, self.STORED_CAP)
            if stored_balance + amount > self.STORED_CAP:
                return GuishiDepositResult(
                    "stored_cap_exceeded", user_id, amount, stored_balance
                )

            charged = uow.execute(
                "UPDATE user_xiuxian "
                "SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(? AS REAL) "
                "WHERE user_id=? AND COALESCE(stone,0)>=?",
                (amount, user_id, amount),
            )
            if charged.rowcount != 1:
                return GuishiDepositResult(
                    "stone_insufficient", user_id, amount, stored_balance
                )
            new_balance = stored_balance + amount
            uow.execute(
                "INSERT INTO guishi_trade.guishi_info(user_id,stored_stone,items) "
                "VALUES(?,?, '{}') ON CONFLICT(user_id) DO UPDATE SET "
                "stored_stone=excluded.stored_stone",
                (user_id, new_balance),
            )
            uow.execute(
                "INSERT INTO trade_guishi_deposit_operations("
                "operation_id,payload,user_id,amount,stored_balance) VALUES(?,?,?,?,?)",
                (operation_id, payload, user_id, amount, new_balance),
            )
            return GuishiDepositResult("completed", user_id, amount, new_balance)


__all__ = ["GuishiDepositResult", "GuishiDepositSqlRepository"]
