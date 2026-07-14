from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class StoneContestResult:
    status: str
    payer_id: str
    receiver_id: str
    requested_amount: int
    transferred_amount: int
    payer_balance: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"transferred", "duplicate"}


@dataclass(frozen=True)
class StoneTheftResult:
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


class StoneContestService:
    """Transfer contested stones between two players atomically and idempotently."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _result(status, payer_id, receiver_id, requested_amount,
                transferred_amount=0, payer_balance=0):
        return StoneContestResult(
            status, payer_id, receiver_id, int(requested_amount),
            int(transferred_amount), int(payer_balance),
        )

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS stone_contest_operations ("
            "operation_id TEXT PRIMARY KEY, payer_id TEXT NOT NULL, receiver_id TEXT NOT NULL, "
            "requested_amount INTEGER NOT NULL, transferred_amount INTEGER NOT NULL, "
            "payer_balance INTEGER NOT NULL, operation_type TEXT NOT NULL DEFAULT 'transfer', "
            "thief_id TEXT NOT NULL DEFAULT '', victim_id TEXT NOT NULL DEFAULT '', "
            "outcome TEXT NOT NULL DEFAULT '', penalty_amount INTEGER NOT NULL DEFAULT 0, "
            "stamina_cost INTEGER NOT NULL DEFAULT 0, thief_stamina INTEGER NOT NULL DEFAULT 0, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        for name, definition in (
            ("operation_type", "TEXT NOT NULL DEFAULT 'transfer'"),
            ("thief_id", "TEXT NOT NULL DEFAULT ''"),
            ("victim_id", "TEXT NOT NULL DEFAULT ''"),
            ("outcome", "TEXT NOT NULL DEFAULT ''"),
            ("penalty_amount", "INTEGER NOT NULL DEFAULT 0"),
            ("stamina_cost", "INTEGER NOT NULL DEFAULT 0"),
            ("thief_stamina", "INTEGER NOT NULL DEFAULT 0"),
        ):
            if not conn.column_exists("stone_contest_operations", name):
                conn.execute(
                    f"ALTER TABLE stone_contest_operations ADD COLUMN {name} {definition}"
                )

    @staticmethod
    def _theft_from_row(row, status="duplicate") -> StoneTheftResult:
        return StoneTheftResult(
            status=status,
            thief_id=str(row[1]),
            victim_id=str(row[2]),
            outcome=str(row[3]),
            payer_id=str(row[4]),
            receiver_id=str(row[5]),
            requested_amount=int(row[6]),
            transferred_amount=int(row[7]),
            payer_balance=int(row[8]),
            stamina_cost=int(row[9]),
            thief_stamina=int(row[10]),
        )

    def replay_theft(self, operation_id, thief_id, victim_id):
        operation_id = str(operation_id).strip()
        thief_id, victim_id = str(thief_id), str(victim_id)
        if not operation_id:
            raise ValueError("valid theft operation is required")
        with closing(db_backend.connect(self._database)) as conn:
            if not conn.table_exists("stone_contest_operations"):
                return None
            if not conn.column_exists("stone_contest_operations", "operation_type"):
                previous = conn.execute(
                    "SELECT 1 FROM stone_contest_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                return StoneTheftResult("operation_conflict") if previous else None
            previous = conn.execute(
                "SELECT operation_type,thief_id,victim_id,outcome,payer_id,receiver_id,"
                "requested_amount,transferred_amount,payer_balance,stamina_cost,thief_stamina "
                "FROM stone_contest_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            if (
                str(previous[0]) != "theft"
                or str(previous[1]) != thief_id
                or str(previous[2]) != victim_id
            ):
                return StoneTheftResult("operation_conflict")
            return self._theft_from_row(previous)

    def transfer(self, operation_id, payer_id, receiver_id, requested_amount):
        operation_id = str(operation_id).strip()
        payer_id = str(payer_id)
        receiver_id = str(receiver_id)
        requested_amount = int(requested_amount)
        if not operation_id or requested_amount <= 0 or payer_id == receiver_id:
            raise ValueError("valid operation, distinct players and positive amount are required")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payer_id, receiver_id, requested_amount, transferred_amount, payer_balance, "
                    "operation_type "
                    "FROM stone_contest_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if (
                        str(previous[5]) != "transfer"
                        or str(previous[0]) != payer_id
                        or str(previous[1]) != receiver_id
                        or int(previous[2]) != requested_amount
                    ):
                        return self._result("state_changed", payer_id, receiver_id, requested_amount)
                    return self._result(
                        "duplicate", payer_id, receiver_id, requested_amount,
                        previous[3], previous[4],
                    )

                payer = conn.execute(
                    "SELECT COALESCE(stone, 0) FROM user_xiuxian WHERE user_id=%s", (payer_id,),
                ).fetchone()
                receiver = conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (receiver_id,),
                ).fetchone()
                if payer is None or receiver is None:
                    conn.rollback()
                    return self._result("user_missing", payer_id, receiver_id, requested_amount)
                payer_balance = max(0, int(payer[0]))
                transferred = min(requested_amount, payer_balance)
                if transferred <= 0:
                    conn.rollback()
                    return self._result("payer_empty", payer_id, receiver_id, requested_amount)

                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone>=%s",
                    (transferred, payer_id, transferred),
                )
                credited = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                    (transferred, receiver_id),
                )
                if charged.rowcount != 1 or credited.rowcount != 1:
                    conn.rollback()
                    return self._result("state_changed", payer_id, receiver_id, requested_amount)
                new_balance = payer_balance - transferred
                conn.execute(
                    "INSERT INTO stone_contest_operations "
                    "(operation_id, payer_id, receiver_id, requested_amount, transferred_amount, payer_balance) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (operation_id, payer_id, receiver_id, requested_amount, transferred, new_balance),
                )
                conn.commit()
                return self._result(
                    "transferred", payer_id, receiver_id, requested_amount, transferred, new_balance
                )
            except Exception:
                conn.rollback()
                raise

    def settle_theft(
        self,
        operation_id,
        thief_id,
        victim_id,
        *,
        outcome,
        requested_amount,
        penalty_amount,
        stamina_cost=10,
    ):
        operation_id = str(operation_id).strip()
        thief_id, victim_id = str(thief_id), str(victim_id)
        outcome = str(outcome)
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

        def current_result(status, *, thief_stamina=0):
            return StoneTheftResult(
                status=status,
                thief_id=thief_id,
                victim_id=victim_id,
                outcome=outcome,
                requested_amount=requested_amount,
                stamina_cost=stamina_cost,
                thief_stamina=int(thief_stamina),
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT operation_type,thief_id,victim_id,outcome,payer_id,receiver_id,"
                    "requested_amount,transferred_amount,payer_balance,stamina_cost,thief_stamina "
                    "FROM stone_contest_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if (
                        str(previous[0]) != "theft"
                        or str(previous[1]) != thief_id
                        or str(previous[2]) != victim_id
                    ):
                        return current_result("operation_conflict")
                    return self._theft_from_row(previous)

                rows = conn.execute(
                    "SELECT user_id,COALESCE(stone,0),COALESCE(user_stamina,0) "
                    "FROM user_xiuxian WHERE user_id IN (%s,%s)",
                    (thief_id, victim_id),
                ).fetchall()
                users = {
                    str(row[0]): (max(0, int(row[1])), max(0, int(row[2])))
                    for row in rows
                }
                if thief_id not in users or victim_id not in users:
                    conn.rollback()
                    return current_result("user_missing")

                thief_stone, thief_stamina = users[thief_id]
                victim_stone, _ = users[victim_id]
                if thief_stamina < stamina_cost:
                    conn.rollback()
                    return current_result(
                        "stamina_insufficient", thief_stamina=thief_stamina
                    )
                if thief_stone < penalty_amount:
                    conn.rollback()
                    return current_result(
                        "stone_insufficient", thief_stamina=thief_stamina
                    )
                if victim_stone <= 0:
                    conn.rollback()
                    return current_result("payer_empty", thief_stamina=thief_stamina)

                if outcome == "success":
                    payer_id, receiver_id = victim_id, thief_id
                else:
                    payer_id, receiver_id = thief_id, victim_id
                payer_stone = users[payer_id][0]
                receiver_stone = users[receiver_id][0]
                transferred = min(requested_amount, payer_stone)
                if transferred <= 0:
                    conn.rollback()
                    return current_result("payer_empty", thief_stamina=thief_stamina)

                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s "
                    "WHERE user_id=%s AND COALESCE(stone,0)=%s AND stone>=%s",
                    (transferred, payer_id, payer_stone, transferred),
                )
                credited = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone+%s "
                    "WHERE user_id=%s AND COALESCE(stone,0)=%s",
                    (transferred, receiver_id, receiver_stone),
                )
                stamina = conn.execute(
                    "UPDATE user_xiuxian SET user_stamina=user_stamina-%s "
                    "WHERE user_id=%s AND COALESCE(user_stamina,0)=%s AND user_stamina>=%s",
                    (stamina_cost, thief_id, thief_stamina, stamina_cost),
                )
                if charged.rowcount != 1 or credited.rowcount != 1 or stamina.rowcount != 1:
                    conn.rollback()
                    return current_result("state_changed", thief_stamina=thief_stamina)

                payer_balance = payer_stone - transferred
                final_stamina = thief_stamina - stamina_cost
                conn.execute(
                    "INSERT INTO stone_contest_operations ("
                    "operation_id,payer_id,receiver_id,requested_amount,transferred_amount,"
                    "payer_balance,operation_type,thief_id,victim_id,outcome,penalty_amount,"
                    "stamina_cost,thief_stamina) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        operation_id, payer_id, receiver_id, requested_amount, transferred,
                        payer_balance, "theft", thief_id, victim_id, outcome, penalty_amount,
                        stamina_cost, final_stamina,
                    ),
                )
                conn.commit()
                return StoneTheftResult(
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
            except Exception:
                conn.rollback()
                raise


__all__ = ["StoneContestResult", "StoneContestService", "StoneTheftResult"]
