from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class _StateChanged(Exception):
    pass


@dataclass(frozen=True)
class BaseStoneContestResult:
    status: str
    payer_id: str
    receiver_id: str
    requested_amount: int
    transferred_amount: int = 0
    payer_balance: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"transferred", "duplicate"}

    def to_dict(self) -> dict[str, object]:
        return asdict(self) | {"succeeded": self.succeeded}


class BaseStoneContestSqlRepository:
    """Own ordinary spirit-stone transfers after startup migration."""

    OPERATION_COLUMNS = {
        "operation_id", "payer_id", "receiver_id", "requested_amount",
        "transferred_amount", "payer_balance", "operation_type",
    }
    PLAYER_COLUMNS = {"user_id", "stone"}

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA table_info("{table}")')
        }

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        return (
            cls.OPERATION_COLUMNS.issubset(cls._columns(uow, "stone_contest_operations"))
            and cls.PLAYER_COLUMNS.issubset(cls._columns(uow, "user_xiuxian"))
        )

    @staticmethod
    def _result(
        status: str,
        payer_id: str,
        receiver_id: str,
        requested_amount: int,
        transferred_amount: int = 0,
        payer_balance: int = 0,
    ) -> BaseStoneContestResult:
        return BaseStoneContestResult(
            status,
            payer_id,
            receiver_id,
            int(requested_amount),
            int(transferred_amount),
            int(payer_balance),
        )

    @staticmethod
    def _from_row(row, status: str = "duplicate") -> BaseStoneContestResult:
        return BaseStoneContestResult(
            status=status,
            payer_id=str(row["payer_id"]),
            receiver_id=str(row["receiver_id"]),
            requested_amount=int(row["requested_amount"]),
            transferred_amount=int(row["transferred_amount"]),
            payer_balance=int(row["payer_balance"]),
        )

    def get_result(
        self,
        operation_id: str,
        payer_id: str,
        receiver_id: str,
        requested_amount: int | None = None,
    ) -> BaseStoneContestResult | None:
        operation_id = str(operation_id).strip()
        payer_id, receiver_id = str(payer_id), str(receiver_id)
        if not operation_id or not self.database.is_file():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return None
            row = uow.query_one(
                "SELECT operation_type,payer_id,receiver_id,requested_amount,"
                "transferred_amount,payer_balance FROM stone_contest_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
        if row is None:
            return None
        if (
            str(row["operation_type"]) != "transfer"
            or str(row["payer_id"]) != payer_id
            or str(row["receiver_id"]) != receiver_id
            or (requested_amount is not None and int(row["requested_amount"]) != int(requested_amount))
        ):
            return self._result("state_changed", payer_id, receiver_id, requested_amount or 0)
        return self._from_row(row)

    def transfer(
        self,
        operation_id: str,
        payer_id: str,
        receiver_id: str,
        requested_amount: int,
    ) -> BaseStoneContestResult:
        operation_id = str(operation_id).strip()
        payer_id, receiver_id = str(payer_id), str(receiver_id)
        requested_amount = int(requested_amount)
        if not operation_id or not payer_id or not receiver_id or payer_id == receiver_id or requested_amount <= 0:
            raise ValueError("valid operation, distinct players and positive amount are required")
        if not self.database.is_file():
            return self._result("schema_missing", payer_id, receiver_id, requested_amount)

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return self._result("schema_missing", payer_id, receiver_id, requested_amount)
            previous = uow.query_one(
                "SELECT operation_type,payer_id,receiver_id,requested_amount,"
                "transferred_amount,payer_balance FROM stone_contest_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if (
                    str(previous["operation_type"]) != "transfer"
                    or str(previous["payer_id"]) != payer_id
                    or str(previous["receiver_id"]) != receiver_id
                    or int(previous["requested_amount"]) != requested_amount
                ):
                    return self._result("state_changed", payer_id, receiver_id, requested_amount)
                return self._from_row(previous)

            rows = uow.query_all(
                "SELECT user_id,COALESCE(stone,0) AS stone FROM user_xiuxian "
                "WHERE user_id IN (?,?)",
                (payer_id, receiver_id),
            )
            users = {str(row["user_id"]): max(0, int(row["stone"])) for row in rows}
            if payer_id not in users or receiver_id not in users:
                return self._result("user_missing", payer_id, receiver_id, requested_amount)
            payer_balance = users[payer_id]
            transferred = min(requested_amount, payer_balance)
            if transferred <= 0:
                return self._result("payer_empty", payer_id, receiver_id, requested_amount, payer_balance=payer_balance)
            receiver_balance = users[receiver_id]

            try:
                with uow.savepoint("stone_contest_assets"):
                    charged = uow.execute(
                        "UPDATE user_xiuxian SET stone=stone-? WHERE user_id=? "
                        "AND COALESCE(stone,0)=? AND COALESCE(stone,0)>=?",
                        (transferred, payer_id, payer_balance, transferred),
                    )
                    credited = uow.execute(
                        "UPDATE user_xiuxian SET stone=stone+? WHERE user_id=? "
                        "AND COALESCE(stone,0)=?",
                        (transferred, receiver_id, receiver_balance),
                    )
                    if charged.rowcount != 1 or credited.rowcount != 1:
                        raise _StateChanged
            except _StateChanged:
                return self._result("state_changed", payer_id, receiver_id, requested_amount, payer_balance=payer_balance)

            new_balance = payer_balance - transferred
            uow.execute(
                "INSERT INTO stone_contest_operations "
                "(operation_id,payer_id,receiver_id,requested_amount,transferred_amount,payer_balance) "
                "VALUES(?,?,?,?,?,?)",
                (operation_id, payer_id, receiver_id, requested_amount, transferred, new_balance),
            )
            return self._result(
                "transferred", payer_id, receiver_id, requested_amount, transferred, new_balance
            )


__all__ = ["BaseStoneContestResult", "BaseStoneContestSqlRepository"]
