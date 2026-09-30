from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class _StateChanged(Exception):
    pass


@dataclass(frozen=True)
class BaseStoneTheftResult:
    status: str
    thief_id: str = ""
    victim_id: str = ""
    outcome: str = ""
    payer_id: str = ""
    receiver_id: str = ""
    requested_amount: int = 0
    transferred_amount: int = 0
    payer_balance: int = 0
    stamina_cost: int = 0
    thief_stamina: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate"}


class BaseStoneTheftSqlRepository:
    OPERATION_COLUMNS = {
        "operation_id", "payer_id", "receiver_id", "requested_amount",
        "transferred_amount", "payer_balance", "operation_type", "thief_id",
        "victim_id", "outcome", "penalty_amount", "stamina_cost", "thief_stamina",
    }

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
            and {"user_id", "stone", "user_stamina"}.issubset(
                cls._columns(uow, "user_xiuxian")
            )
        )

    @staticmethod
    def _result_from_row(row, status: str = "duplicate") -> BaseStoneTheftResult:
        return BaseStoneTheftResult(
            status=status,
            thief_id=str(row["thief_id"]),
            victim_id=str(row["victim_id"]),
            outcome=str(row["outcome"]),
            payer_id=str(row["payer_id"]),
            receiver_id=str(row["receiver_id"]),
            requested_amount=int(row["requested_amount"]),
            transferred_amount=int(row["transferred_amount"]),
            payer_balance=int(row["payer_balance"]),
            stamina_cost=int(row["stamina_cost"]),
            thief_stamina=int(row["thief_stamina"]),
        )

    def get_result(
        self, operation_id: str, thief_id: str, victim_id: str
    ) -> BaseStoneTheftResult | None:
        operation_id = str(operation_id).strip()
        thief_id, victim_id = str(thief_id), str(victim_id)
        if not operation_id or not self.database.is_file():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return None
            row = uow.query_one(
                "SELECT operation_type,thief_id,victim_id,outcome,payer_id,receiver_id,"
                "requested_amount,transferred_amount,payer_balance,stamina_cost,thief_stamina "
                "FROM stone_contest_operations WHERE operation_id=?",
                (operation_id,),
            )
        if row is None:
            return None
        if (
            str(row["operation_type"]) != "theft"
            or str(row["thief_id"]) != thief_id
            or str(row["victim_id"]) != victim_id
        ):
            return BaseStoneTheftResult("operation_conflict")
        return self._result_from_row(row)

    def settle(
        self,
        operation_id: str,
        thief_id: str,
        victim_id: str,
        *,
        outcome: str,
        requested_amount: int,
        penalty_amount: int,
        stamina_cost: int = 10,
    ) -> BaseStoneTheftResult:
        operation_id = str(operation_id).strip()
        thief_id, victim_id, outcome = str(thief_id), str(victim_id), str(outcome)
        requested_amount = int(requested_amount)
        penalty_amount = int(penalty_amount)
        stamina_cost = int(stamina_cost)
        if (
            not operation_id
            or thief_id == victim_id
            or outcome not in {"success", "failure"}
            or requested_amount <= 0
            or penalty_amount <= 0
            or stamina_cost < 0
            or (outcome == "failure" and requested_amount != penalty_amount)
        ):
            raise ValueError("valid theft participants, outcome and costs are required")

        def result(status: str, *, thief_stamina: int = 0) -> BaseStoneTheftResult:
            return BaseStoneTheftResult(
                status=status,
                thief_id=thief_id,
                victim_id=victim_id,
                outcome=outcome,
                requested_amount=requested_amount,
                stamina_cost=stamina_cost,
                thief_stamina=int(thief_stamina),
            )

        if not self.database.is_file():
            return result("schema_missing")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return result("schema_missing")
            previous = uow.query_one(
                "SELECT operation_type,thief_id,victim_id,outcome,payer_id,receiver_id,"
                "requested_amount,transferred_amount,payer_balance,stamina_cost,thief_stamina "
                "FROM stone_contest_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if (
                    str(previous["operation_type"]) != "theft"
                    or str(previous["thief_id"]) != thief_id
                    or str(previous["victim_id"]) != victim_id
                ):
                    return result("operation_conflict")
                return self._result_from_row(previous)

            rows = uow.query_all(
                "SELECT user_id,COALESCE(stone,0) AS stone,"
                "COALESCE(user_stamina,0) AS user_stamina FROM user_xiuxian "
                "WHERE user_id IN (?,?)",
                (thief_id, victim_id),
            )
            users = {
                str(row["user_id"]): (
                    max(0, int(row["stone"])),
                    max(0, int(row["user_stamina"])),
                )
                for row in rows
            }
            if thief_id not in users or victim_id not in users:
                return result("user_missing")

            thief_stone, thief_stamina = users[thief_id]
            victim_stone, _ = users[victim_id]
            if thief_stamina < stamina_cost:
                return result("stamina_insufficient", thief_stamina=thief_stamina)
            if thief_stone < penalty_amount:
                return result("stone_insufficient", thief_stamina=thief_stamina)
            if victim_stone <= 0:
                return result("payer_empty", thief_stamina=thief_stamina)

            payer_id, receiver_id = (
                (victim_id, thief_id) if outcome == "success" else (thief_id, victim_id)
            )
            payer_stone = users[payer_id][0]
            receiver_stone = users[receiver_id][0]
            transferred = min(requested_amount, payer_stone)
            if transferred <= 0:
                return result("payer_empty", thief_stamina=thief_stamina)

            try:
                with uow.savepoint("stone_theft_assets"):
                    charged = uow.execute(
                        "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-? "
                        "WHERE user_id=? AND COALESCE(stone,0)=? AND stone>=?",
                        (transferred, payer_id, payer_stone, transferred),
                    )
                    credited = uow.execute(
                        "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+? "
                        "WHERE user_id=? AND COALESCE(stone,0)=?",
                        (transferred, receiver_id, receiver_stone),
                    )
                    stamina = uow.execute(
                        "UPDATE user_xiuxian SET user_stamina=user_stamina-? "
                        "WHERE user_id=? AND COALESCE(user_stamina,0)=? AND user_stamina>=?",
                        (stamina_cost, thief_id, thief_stamina, stamina_cost),
                    )
                    if charged.rowcount != 1 or credited.rowcount != 1 or stamina.rowcount != 1:
                        raise _StateChanged
            except _StateChanged:
                return result("state_changed", thief_stamina=thief_stamina)

            payer_balance = payer_stone - transferred
            final_stamina = thief_stamina - stamina_cost
            uow.execute(
                "INSERT INTO stone_contest_operations ("
                "operation_id,payer_id,receiver_id,requested_amount,transferred_amount,"
                "payer_balance,operation_type,thief_id,victim_id,outcome,penalty_amount,"
                "stamina_cost,thief_stamina) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    operation_id, payer_id, receiver_id, requested_amount, transferred,
                    payer_balance, "theft", thief_id, victim_id, outcome, penalty_amount,
                    stamina_cost, final_stamina,
                ),
            )
            return BaseStoneTheftResult(
                status="settled",
                thief_id=thief_id,
                victim_id=victim_id,
                outcome=outcome,
                payer_id=payer_id,
                receiver_id=receiver_id,
                requested_amount=requested_amount,
                transferred_amount=transferred,
                payer_balance=payer_balance,
                stamina_cost=stamina_cost,
                thief_stamina=final_stamina,
            )


__all__ = ["BaseStoneTheftResult", "BaseStoneTheftSqlRepository"]
