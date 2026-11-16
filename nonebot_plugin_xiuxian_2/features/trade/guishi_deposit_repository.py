from __future__ import annotations

from ...core.numeric import as_int_like, bind_sqlite_param, require_exact_integer
from ...core.errors import DomainError
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
    reason: str = ""
    subject: str = ""

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
                    as_int_like(previous["stored_balance"]),
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
                    as_int_like(legacy["stored_balance"]),
                )

            player = uow.query_one(
                "SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,)
            )
            if player is None:
                return GuishiDepositResult("user_missing", user_id, amount)
            wallet = require_exact_integer(player["stone"], field="stone", subject=user_id)
            stored = uow.query_one(
                "SELECT COALESCE(stored_stone,0) AS stored_stone "
                "FROM guishi_trade.guishi_info WHERE user_id=?",
                (user_id,),
            )
            stored_balance = require_exact_integer(stored["stored_stone"], field="stored_stone", subject=user_id) if stored is not None else 0
            # A rejected deposit must not persist a normalization that the old
            # transaction would have rolled back with the rejected request.
            stored_balance = min(stored_balance, self.STORED_CAP)
            if stored_balance + amount > self.STORED_CAP:
                return GuishiDepositResult(
                    "stored_cap_exceeded", user_id, amount, stored_balance
                )

            if wallet < amount:
                return GuishiDepositResult("stone_insufficient", user_id, amount, stored_balance)
            new_balance = stored_balance + amount
            try:
                with uow.savepoint("deposit_transfer"):
                    charged = uow.execute(
                        "UPDATE user_xiuxian "
                        "SET stone=xiuxian_int_store(?,'stone','actor') "
                        "WHERE user_id=? AND xiuxian_int_cmp(COALESCE(stone,0),?,1,'stone','actor')=0",
                        (bind_sqlite_param(wallet - amount, exact=True), user_id, bind_sqlite_param(wallet, exact=True)),
                    )
                    if charged.rowcount != 1:
                        raise DomainError("stone_write_rejected", "Guishi deposit wallet write rejected")
                    balance = uow.execute(
                        "INSERT INTO guishi_trade.guishi_info(user_id,stored_stone,items) "
                        "VALUES(?,xiuxian_int_store(?,'stored_stone','actor'),'{}') ON CONFLICT(user_id) DO UPDATE SET "
                        "stored_stone=excluded.stored_stone",
                        (user_id, new_balance),
                    )
                    if balance.rowcount != 1:
                        raise DomainError("guishi_balance_write_rejected", "Guishi deposit balance write rejected")
                    receipt = uow.execute(
                        "INSERT INTO trade_guishi_deposit_operations("
                        "operation_id,payload,user_id,amount,stored_balance) VALUES(?,?,?,xiuxian_int_store(?,'stone','actor'),?)",
                        (operation_id, payload, user_id, amount, new_balance),
                    )
                    saved_receipt = uow.query_one(
                        "SELECT payload,user_id,amount,stored_balance FROM trade_guishi_deposit_operations WHERE operation_id=?",
                        (operation_id,),
                    )
                    if receipt.rowcount != 1 or saved_receipt != dict(payload=payload, user_id=user_id, amount=amount, stored_balance=new_balance):
                        raise DomainError("guishi_receipt_write_rejected", "Guishi deposit receipt did not persist exactly")
                    # Validate final projections after all writes, including receipt triggers.
                    saved = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,))
                    if saved is None or require_exact_integer(saved["stone"], field="stone", subject=user_id) != wallet - amount:
                        raise DomainError("stone_write_rejected", "Guishi deposit wallet did not persist exactly")
                    saved = uow.query_one("SELECT stored_stone FROM guishi_trade.guishi_info WHERE user_id=?", (user_id,))
                    if saved is None or require_exact_integer(saved["stored_stone"], field="stored_stone", subject=user_id) != new_balance:
                        raise DomainError("guishi_balance_write_rejected", "Guishi deposit balance did not persist exactly")
            except DomainError as exc:
                return GuishiDepositResult(
                    "state_changed", user_id, amount, stored_balance,
                    reason=exc.details.get("reason") or exc.code,
                    subject=exc.details.get("subject") or user_id,
                )
            return GuishiDepositResult("completed", user_id, amount, new_balance)


__all__ = ["GuishiDepositResult", "GuishiDepositSqlRepository"]
