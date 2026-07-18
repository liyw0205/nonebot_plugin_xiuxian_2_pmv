from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
import random
from typing import Callable
from ..xiuxian_utils import db_backend
from ..xiuxian_buff.relation_transaction_utils import increment_stat
from datetime import date, datetime
from datetime import datetime

@dataclass(frozen=True)
class SignInResult:
    status: str
    user_id: str
    stone: int = 0

    @property
    def applied(self) -> bool:
        return self.status == "signed"

    @property
    def succeeded(self) -> bool:
        return self.status in {"signed", "duplicate"}

class SignInService:
    def __init__(
        self,
        database: str | Path,
        lock: RLock | None = None,
        randint: Callable[[int, int], int] | None = None,
    ) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()
        self._randint = randint or random.randint

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sign_in_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                stone INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def get_result(self, operation_id: str) -> SignInResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_operations(conn)
            previous = conn.execute(
                "SELECT user_id, stone FROM sign_in_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return SignInResult("duplicate", str(previous[0]), int(previous[1]))

    def sign(self, operation_id, user_id, stone_lower: int, stone_upper: int) -> SignInResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        stone_lower = int(stone_lower)
        stone_upper = int(stone_upper)
        if stone_lower > stone_upper:
            raise ValueError("stone_lower must not exceed stone_upper")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT stone FROM sign_in_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SignInResult("duplicate", user_id, int(previous[0]))

                # 同一 openid 可能有多行；只要还有未签到行就允许签到
                users = conn.execute(
                    "SELECT CAST(COALESCE(is_sign,0) AS INTEGER) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchall()
                if not users:
                    conn.rollback()
                    return SignInResult("user_missing", user_id)
                if all(int(row[0] or 0) == 1 for row in users):
                    conn.rollback()
                    return SignInResult("already_signed", user_id)

                stone = int(self._randint(stone_lower, stone_upper))
                # 重复 user_id 会更新多行：rowcount>=1 即成功，避免误报「贪心」
                updated = conn.execute(
                    "UPDATE user_xiuxian SET is_sign=1, stone=CAST(COALESCE(stone,0) AS INTEGER)+%s "
                    "WHERE user_id=%s AND CAST(COALESCE(is_sign,0) AS INTEGER)=0",
                    (stone, user_id),
                )
                if updated.rowcount < 1:
                    conn.rollback()
                    return SignInResult("already_signed", user_id)
                conn.execute(
                    "INSERT INTO sign_in_operations (operation_id, user_id, stone) "
                    "VALUES (%s, %s, %s)",
                    (operation_id, user_id, stone),
                )
                conn.commit()
                return SignInResult("signed", user_id, stone)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class PlayerRenameResult:
    status: str
    user_id: str
    rename_type: str
    new_name: str
    previous_name: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"renamed", "duplicate"}

class PlayerRenameService:
    """Charge a rename cost and update the player record atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS player_rename_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                rename_type TEXT NOT NULL,
                new_name TEXT NOT NULL,
                previous_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _consume_item(conn, user_id: str, item_id: int) -> bool:
        columns = set(conn.column_names("back"))
        updates = ["goods_num=goods_num-1"]
        if "bind_num" in columns:
            updates.append(
                "bind_num=CASE WHEN goods_num-1=0 THEN 0 "
                "WHEN COALESCE(bind_num, 0)>0 THEN COALESCE(bind_num, 0)-1 "
                "ELSE MIN(COALESCE(bind_num, 0), goods_num-1) END"
            )
        changed = conn.execute(
            f"UPDATE back SET {', '.join(updates)} "
            "WHERE user_id=%s AND goods_id=%s AND goods_num>0",
            (user_id, int(item_id)),
        )
        return changed.rowcount == 1

    def get_result(self, operation_id: str) -> PlayerRenameResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_operations(conn)
            previous = conn.execute(
                "SELECT user_id, rename_type, new_name, previous_name "
                "FROM player_rename_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return PlayerRenameResult(
                "duplicate", str(previous[0]), str(previous[1]), str(previous[2]), str(previous[3] or "")
            )

    def _rename(
        self,
        operation_id,
        user_id,
        rename_type,
        new_name,
        *,
        target_column,
        item_id=None,
        stone_cost=0,
        require_unique=False,
    ) -> PlayerRenameResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        rename_type = str(rename_type)
        new_name = str(new_name).strip()
        stone_cost = int(stone_cost)
        if not new_name:
            raise ValueError("new_name must not be empty")
        if stone_cost < 0:
            raise ValueError("stone_cost must not be negative")
        if item_id is not None and stone_cost:
            raise ValueError("rename can charge either an item or stones")

        def result(status: str, previous_name="", result_name=new_name) -> PlayerRenameResult:
            return PlayerRenameResult(
                status, user_id, rename_type, str(result_name), str(previous_name or "")
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT previous_name, new_name FROM player_rename_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous[0], previous[1])

                player = conn.execute(
                    f"SELECT {target_column}, stone FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if player is None:
                    conn.rollback()
                    return result("user_missing")
                previous_name = str(player[0] or "")
                if previous_name == new_name:
                    conn.rollback()
                    return result("unchanged", previous_name)
                if require_unique:
                    conflict = conn.execute(
                        "SELECT 1 FROM user_xiuxian WHERE user_name=%s AND user_id<>%s",
                        (new_name, user_id),
                    ).fetchone()
                    if conflict is not None:
                        conn.rollback()
                        return result("name_conflict", previous_name)

                if item_id is not None:
                    if not self._consume_item(conn, user_id, int(item_id)):
                        conn.rollback()
                        return result("item_missing", previous_name)
                elif stone_cost:
                    charged = conn.execute(
                        "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(%s AS REAL) "
                        "WHERE user_id=%s AND stone>=%s",
                        (stone_cost, user_id, stone_cost),
                    )
                    if charged.rowcount != 1:
                        conn.rollback()
                        return result("stone_insufficient", previous_name)

                renamed = conn.execute(
                    f"UPDATE user_xiuxian SET {target_column}=%s WHERE user_id=%s",
                    (new_name, user_id),
                )
                if renamed.rowcount != 1:
                    conn.rollback()
                    return result("state_changed", previous_name)
                conn.execute(
                    "INSERT INTO player_rename_operations "
                    "(operation_id, user_id, rename_type, new_name, previous_name) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, rename_type, new_name, previous_name),
                )
                conn.commit()
                return result("renamed", previous_name)
            except Exception:
                conn.rollback()
                raise

    def rename_user(self, operation_id, user_id, new_name, *, item_id=None, stone_cost=0):
        return self._rename(
            operation_id,
            user_id,
            "user_name",
            new_name,
            target_column="user_name",
            item_id=item_id,
            stone_cost=stone_cost,
            require_unique=True,
        )

    def rename_root(self, operation_id, user_id, new_name, *, item_id):
        return self._rename(
            operation_id,
            user_id,
            "root",
            new_name,
            target_column="root",
            item_id=item_id,
        )

@dataclass(frozen=True)
class StoneGiftResult:
    status: str
    sender_id: str
    recipient_id: str
    gross_amount: int
    net_amount: int
    fee_amount: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"transferred", "duplicate"}

class StoneGiftService:
    """Transfer stones with a fee atomically and idempotently."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stone_gift_operations (
                operation_id TEXT PRIMARY KEY,
                sender_id TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                gross_amount INTEGER NOT NULL,
                net_amount INTEGER NOT NULL,
                fee_amount INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def transfer(
        self,
        operation_id,
        sender_id,
        recipient_id,
        gross_amount,
        *,
        fee_rate=0.1,
    ) -> StoneGiftResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        sender_id = str(sender_id)
        recipient_id = str(recipient_id)
        gross_amount = int(gross_amount)
        fee_rate = float(fee_rate)
        if gross_amount <= 0:
            raise ValueError("gross_amount must be positive")
        if sender_id == recipient_id:
            raise ValueError("sender and recipient must differ")
        if not 0 <= fee_rate < 1:
            raise ValueError("fee_rate must be in [0, 1)")
        fee_amount = int(gross_amount * fee_rate)
        net_amount = gross_amount - fee_amount

        def result(status: str, gross=gross_amount, net=net_amount, fee=fee_amount):
            return StoneGiftResult(
                status,
                sender_id,
                recipient_id,
                int(gross),
                int(net),
                int(fee),
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT sender_id, recipient_id, gross_amount, net_amount, fee_amount "
                    "FROM stone_gift_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    previous_sender, previous_recipient, gross, net, fee = previous
                    if (
                        str(previous_sender) != sender_id
                        or str(previous_recipient) != recipient_id
                    ):
                        return result("state_changed")
                    return result("duplicate", gross, net, fee)

                recipient = conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s",
                    (recipient_id,),
                ).fetchone()
                if recipient is None:
                    conn.rollback()
                    return result("recipient_missing")
                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(%s AS REAL) "
                    "WHERE user_id=%s AND COALESCE(stone, 0)>=%s",
                    (gross_amount, sender_id, gross_amount),
                )
                if charged.rowcount != 1:
                    conn.rollback()
                    return result("stone_insufficient")
                credited = conn.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s",
                    (net_amount, recipient_id),
                )
                if credited.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO stone_gift_operations "
                    "(operation_id, sender_id, recipient_id, gross_amount, "
                    "net_amount, fee_amount) VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        sender_id,
                        recipient_id,
                        gross_amount,
                        net_amount,
                        fee_amount,
                    ),
                )
                conn.commit()
                return result("transferred")
            except Exception:
                conn.rollback()
                raise

    def get_result(self, operation_id: str) -> StoneGiftResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("stone_gift_operations",),
            ).fetchone()
            if exists is None:
                return None
            previous = conn.execute(
                "SELECT sender_id, recipient_id, gross_amount, net_amount, fee_amount "
                "FROM stone_gift_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return StoneGiftResult(
                "duplicate", str(previous[0]), str(previous[1]), int(previous[2]), int(previous[3]), int(previous[4])
            )

    def get_operation(
        self, operation_id, sender_id, recipient_id
    ) -> StoneGiftResult | None:
        operation_id = str(operation_id).strip()
        sender_id = str(sender_id)
        recipient_id = str(recipient_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("stone_gift_operations",),
            ).fetchone()
            if exists is None:
                return None
            previous = conn.execute(
                "SELECT sender_id, recipient_id, gross_amount, net_amount, fee_amount "
                "FROM stone_gift_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            previous_sender, previous_recipient, gross, net, fee = previous
            if str(previous_sender) != sender_id or str(previous_recipient) != recipient_id:
                return None
            return StoneGiftResult(
                "duplicate", sender_id, recipient_id, int(gross), int(net), int(fee)
            )

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
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(%s AS REAL) WHERE user_id=%s AND stone>=%s",
                    (transferred, payer_id, transferred),
                )
                credited = conn.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s",
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
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(%s AS REAL) "
                    "WHERE user_id=%s AND COALESCE(stone,0)=%s AND stone>=%s",
                    (transferred, payer_id, payer_stone, transferred),
                )
                credited = conn.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) "
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

@dataclass(frozen=True)
class StoneRobberyResult:
    status: str
    robber_id: str = ""
    victim_id: str = ""
    winner_id: str = ""
    battle_messages: list = field(default_factory=list)
    robber_hp: int = 0
    robber_mp: int = 0
    victim_hp: int = 0
    victim_mp: int = 0
    requested_amount: int = 0
    transferred_amount: int = 0
    loser_balance: int = 0
    stamina_cost: int = 0
    robber_stamina: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class StoneRobberySettlementService:
    """Commit robbery combat, stamina, stones and statistics atomically."""

    _SNAPSHOT_FIELDS = ("hp", "mp", "user_stamina", "exp", "stone")

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS stone_robbery_operations("
            "operation_id TEXT PRIMARY KEY,robber_id TEXT NOT NULL,victim_id TEXT NOT NULL,"
            "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @classmethod
    def _snapshot(cls, value) -> tuple[int | None, ...]:
        data = dict(value)
        return tuple(
            None if field in {"hp", "mp"} and data[field] is None
            else int(data[field] or 0)
            for field in cls._SNAPSHOT_FIELDS
        )

    @staticmethod
    def _saved_result(value, status="duplicate") -> StoneRobberyResult:
        return StoneRobberyResult(status=status, **json.loads(str(value)))

    def replay(self, operation_id, robber_id, victim_id):
        operation_id = str(operation_id).strip()
        robber_id, victim_id = str(robber_id), str(victim_id)
        if not operation_id:
            raise ValueError("valid robbery operation is required")
        with closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("stone_robbery_operations"):
                return None
            previous = conn.execute(
                "SELECT robber_id,victim_id,result_json FROM stone_robbery_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            if str(previous[0]) != robber_id or str(previous[1]) != victim_id:
                return StoneRobberyResult("operation_conflict")
            return self._saved_result(previous[2])

    def settle(
        self,
        operation_id,
        robber_id,
        victim_id,
        *,
        expected_robber,
        expected_victim,
        robber_final,
        victim_final,
        winner_id,
        battle_messages,
        stamina_cost=15,
    ) -> StoneRobberyResult:
        operation_id = str(operation_id).strip()
        robber_id, victim_id = str(robber_id), str(victim_id)
        winner_id = str(winner_id)
        robber_snapshot = self._snapshot(expected_robber)
        victim_snapshot = self._snapshot(expected_victim)
        robber_final = (max(1, int(robber_final[0])), max(0, int(robber_final[1])))
        victim_final = (max(1, int(victim_final[0])), max(0, int(victim_final[1])))
        battle_messages = list(battle_messages or [])
        stamina_cost = int(stamina_cost)
        if (
            not operation_id
            or robber_id == victim_id
            or winner_id not in {robber_id, victim_id}
            or stamina_cost < 0
        ):
            raise ValueError("valid robbery participants, winner and stamina are required")

        def current_result(status):
            return StoneRobberyResult(
                status=status,
                robber_id=robber_id,
                victim_id=victim_id,
                winner_id=winner_id,
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT robber_id,victim_id,result_json FROM stone_robbery_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != robber_id or str(previous[1]) != victim_id:
                        return current_result("operation_conflict")
                    return self._saved_result(previous[2])

                rows = conn.execute(
                    "SELECT user_id,hp,mp,"
                    "COALESCE(user_stamina,0),COALESCE(exp,0),COALESCE(stone,0) "
                    "FROM user_xiuxian WHERE user_id IN (%s,%s)",
                    (robber_id, victim_id),
                ).fetchall()
                users = {
                    str(row[0]): tuple(
                        None if index < 2 and item is None else int(item or 0)
                        for index, item in enumerate(row[1:])
                    )
                    for row in rows
                }
                if robber_id not in users or victim_id not in users:
                    conn.rollback()
                    return current_result("user_missing")
                if users[robber_id] != robber_snapshot or users[victim_id] != victim_snapshot:
                    conn.rollback()
                    return current_result("state_changed")
                robber_hp = (
                    robber_snapshot[3] // 2
                    if robber_snapshot[0] is None else robber_snapshot[0]
                )
                victim_hp = (
                    victim_snapshot[3] // 2
                    if victim_snapshot[0] is None else victim_snapshot[0]
                )
                if robber_hp <= robber_snapshot[3] / 10:
                    conn.rollback()
                    return current_result("robber_injured")
                if victim_hp <= victim_snapshot[3] / 10:
                    conn.rollback()
                    return current_result("victim_injured")
                if robber_snapshot[2] < stamina_cost:
                    conn.rollback()
                    return current_result("stamina_insufficient")

                loser_id = victim_id if winner_id == robber_id else robber_id
                loser_snapshot = users[loser_id]
                requested_amount = min(int(min(loser_snapshot[4], 1000000) * 0.1), 1000000)
                transferred_amount = min(requested_amount, loser_snapshot[4])
                robber_stone = robber_snapshot[4]
                victim_stone = victim_snapshot[4]
                if winner_id == robber_id:
                    robber_stone += transferred_amount
                    victim_stone -= transferred_amount
                else:
                    robber_stone -= transferred_amount
                    victim_stone += transferred_amount

                robber_changed = conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s,user_stamina=%s,stone=%s "
                    "WHERE user_id=%s AND hp IS %s AND mp IS %s "
                    "AND COALESCE(user_stamina,0)=%s AND COALESCE(exp,0)=%s "
                    "AND COALESCE(stone,0)=%s",
                    (
                        robber_final[0], robber_final[1], robber_snapshot[2] - stamina_cost,
                        robber_stone, robber_id, *robber_snapshot,
                    ),
                )
                victim_changed = conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s,stone=%s "
                    "WHERE user_id=%s AND hp IS %s AND mp IS %s "
                    "AND COALESCE(user_stamina,0)=%s AND COALESCE(exp,0)=%s "
                    "AND COALESCE(stone,0)=%s",
                    (
                        victim_final[0], victim_final[1], victim_stone,
                        victim_id, *victim_snapshot,
                    ),
                )
                if robber_changed.rowcount != 1 or victim_changed.rowcount != 1:
                    conn.rollback()
                    return current_result("state_changed")

                increment_stat(conn, winner_id, "抢灵石成功", 1)
                increment_stat(conn, loser_id, "抢灵石失败", 1)
                loser_balance = victim_stone if loser_id == victim_id else robber_stone
                saved = {
                    "robber_id": robber_id,
                    "victim_id": victim_id,
                    "winner_id": winner_id,
                    "battle_messages": battle_messages,
                    "robber_hp": robber_final[0],
                    "robber_mp": robber_final[1],
                    "victim_hp": victim_final[0],
                    "victim_mp": victim_final[1],
                    "requested_amount": requested_amount,
                    "transferred_amount": transferred_amount,
                    "loser_balance": loser_balance,
                    "stamina_cost": stamina_cost,
                    "robber_stamina": robber_snapshot[2] - stamina_cost,
                }
                conn.execute(
                    "INSERT INTO stone_robbery_operations "
                    "(operation_id,robber_id,victim_id,result_json) VALUES (%s,%s,%s,%s)",
                    (
                        operation_id,
                        robber_id,
                        victim_id,
                        json.dumps(saved, ensure_ascii=False, separators=(",", ":")),
                    ),
                )
                conn.commit()
                return StoneRobberyResult("applied", **saved)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

@dataclass(frozen=True)
class LotteryWinner:
    user_id: str
    user_name: str
    won_at: str
    amount: int
    lottery_number: int
    prize_tier: str

@dataclass(frozen=True)
class LotteryPoolSnapshot:
    business_date: str
    pool: int = 0
    participants: int = 0
    last_winner: LotteryWinner | None = None

@dataclass(frozen=True)
class LotterySettlementResult:
    status: str
    operation_id: str
    user_id: str = ""
    user_name: str = ""
    business_date: str = ""
    lottery_number: int = 0
    prize_tier: str = "none"
    prize: int = 0
    deposit: int = 0
    pool_before: int = 0
    pool_after: int = 0
    participants: int = 0
    wallet_stone: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate", "already_participated"}

class LotterySettlementService:
    """Settle one lottery participation entirely in the game database."""

    _MIGRATION_KEY = "legacy-lottery-pool-json-v1"
    _OPERATION_COLUMNS = (
        "operation_id,user_id,user_name,business_date,deposit_amount,lottery_number,"
        "prize_tier,prize_amount,pool_before,pool_after,participant_count,wallet_stone"
    )

    def __init__(
        self,
        database: str | Path,
        legacy_path: str | Path | None = None,
        *,
        lock: RLock | None = None,
        randint: Callable[[int, int], int] | None = None,
    ) -> None:
        self._database = Path(database)
        self._legacy_path = None if legacy_path is None else Path(legacy_path)
        self._lock = lock or RLock()
        self._randint = randint or random.randint

    @staticmethod
    def _normalize_date(value) -> str:
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return value.isoformat()
        return date.fromisoformat(str(value).strip()).isoformat()

    @staticmethod
    def _normalize_time(value=None) -> str:
        if value is None:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return str(value).strip()

    @staticmethod
    def _nonnegative_int(value, default=0) -> int:
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return int(default)

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS lottery_pool_state("
            "state_id INTEGER PRIMARY KEY,pool_amount INTEGER NOT NULL DEFAULT 0,"
            "updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS lottery_settlement_operations("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,user_name TEXT NOT NULL,"
            "business_date TEXT NOT NULL,deposit_amount INTEGER NOT NULL,"
            "lottery_number INTEGER NOT NULL,prize_tier TEXT NOT NULL,"
            "prize_amount INTEGER NOT NULL,pool_before INTEGER NOT NULL,"
            "pool_after INTEGER NOT NULL,participant_count INTEGER NOT NULL,"
            "wallet_stone INTEGER NOT NULL,created_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS lottery_participants("
            "business_date TEXT NOT NULL,user_id TEXT NOT NULL,operation_id TEXT NOT NULL UNIQUE,"
            "participated_at TEXT NOT NULL,PRIMARY KEY(business_date,user_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS lottery_winner_history("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,operation_id TEXT NOT NULL UNIQUE,"
            "user_id TEXT NOT NULL,user_name TEXT NOT NULL,prize_tier TEXT NOT NULL,"
            "lottery_number INTEGER NOT NULL,prize_amount INTEGER NOT NULL,won_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS lottery_legacy_migrations("
            "migration_key TEXT PRIMARY KEY,source_path TEXT NOT NULL,payload TEXT NOT NULL,"
            "migrated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS economy_log("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
            "source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,"
            "exp_delta INTEGER NOT NULL DEFAULT 0,sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
            "sect_scale_delta INTEGER NOT NULL DEFAULT 0,sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
            "item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',"
            "trace_id TEXT,created_at TEXT NOT NULL)"
        )
        if not conn.column_exists("economy_log", "trace_id"):
            conn.execute("ALTER TABLE economy_log ADD COLUMN trace_id TEXT")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lottery_participants_date "
            "ON lottery_participants(business_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lottery_winners_time "
            "ON lottery_winner_history(won_at)"
        )

    def _read_legacy_data(self) -> dict:
        if self._legacy_path is None or not self._legacy_path.is_file():
            return {}
        try:
            with self._legacy_path.open("r", encoding="utf-8") as file:
                data = json.load(file)
        except (OSError, ValueError, TypeError):
            return {}
        return data if isinstance(data, dict) else {}

    def _migrate_legacy(self, conn, business_date: str, migrated_at: str) -> None:
        migrated = conn.execute(
            "SELECT 1 FROM lottery_legacy_migrations WHERE migration_key=%s",
            (self._MIGRATION_KEY,),
        ).fetchone()
        if migrated is not None:
            return

        legacy = self._read_legacy_data()
        payload = json.dumps(
            legacy, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        pool = self._nonnegative_int(legacy.get("pool"))
        conn.execute(
            "UPDATE lottery_pool_state SET pool_amount=%s,updated_at=%s WHERE state_id=1",
            (pool, migrated_at),
        )

        participant_ids = legacy.get("participants", [])
        if isinstance(participant_ids, (list, tuple, set)):
            for user_id in dict.fromkeys(str(value).strip() for value in participant_ids):
                if not user_id:
                    continue
                conn.execute(
                    "INSERT INTO lottery_participants("
                    "business_date,user_id,operation_id,participated_at) VALUES(%s,%s,%s,%s) "
                    "ON CONFLICT(business_date,user_id) DO NOTHING",
                    (
                        business_date,
                        user_id,
                        f"legacy-json:{business_date}:{user_id}",
                        migrated_at,
                    ),
                )

        winner = legacy.get("last_winner")
        if isinstance(winner, dict) and str(winner.get("name", "")).strip():
            conn.execute(
                "INSERT INTO lottery_winner_history("
                "operation_id,user_id,user_name,prize_tier,lottery_number,prize_amount,won_at) "
                "VALUES(%s,%s,%s,'legacy',%s,%s,%s) "
                "ON CONFLICT(operation_id) DO NOTHING",
                (
                    "legacy-json:last-winner",
                    str(winner.get("user_id", "")).strip(),
                    str(winner.get("name", "")).strip(),
                    self._nonnegative_int(winner.get("lottery_number")),
                    self._nonnegative_int(winner.get("amount")),
                    str(winner.get("time", migrated_at)).strip() or migrated_at,
                ),
            )

        conn.execute(
            "INSERT INTO lottery_legacy_migrations("
            "migration_key,source_path,payload,migrated_at) VALUES(%s,%s,%s,%s)",
            (
                self._MIGRATION_KEY,
                "" if self._legacy_path is None else str(self._legacy_path),
                payload,
                migrated_at,
            ),
        )

    def _prepare(self, conn, business_date: str, occurred_at: str) -> None:
        self._ensure_schema(conn)
        conn.execute(
            "INSERT INTO lottery_pool_state(state_id,pool_amount,updated_at) "
            "VALUES(1,0,%s) ON CONFLICT(state_id) DO NOTHING",
            (occurred_at,),
        )
        self._migrate_legacy(conn, business_date, occurred_at)

    @classmethod
    def _operation_result(cls, row, status: str) -> LotterySettlementResult:
        return LotterySettlementResult(
            status=status,
            operation_id=str(row[0]),
            user_id=str(row[1]),
            user_name=str(row[2]),
            business_date=str(row[3]),
            deposit=int(row[4]),
            lottery_number=int(row[5]),
            prize_tier=str(row[6]),
            prize=int(row[7]),
            pool_before=int(row[8]),
            pool_after=int(row[9]),
            participants=int(row[10]),
            wallet_stone=int(row[11]),
        )

    @classmethod
    def _get_operation(cls, conn, operation_id: str):
        return conn.execute(
            f"SELECT {cls._OPERATION_COLUMNS} FROM lottery_settlement_operations "
            "WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()

    @staticmethod
    def _tier_for_number(lottery_number: int) -> str:
        if lottery_number in {6, 66, 666, 6666, 66666}:
            return "grand"
        six_count = str(lottery_number).count("6")
        if six_count == 3:
            return "first"
        if six_count == 2:
            return "second"
        if six_count == 1:
            return "third"
        return "none"

    @staticmethod
    def _prize_for_tier(pool: int, prize_tier: str) -> int:
        if prize_tier == "grand":
            return pool
        if prize_tier == "first":
            return pool // 10
        if prize_tier == "second":
            return pool // 100
        if prize_tier == "third":
            return pool // 1000
        return 0

    def settle(
        self,
        operation_id,
        user_id,
        user_name,
        business_date,
        *,
        deposit=1_000_000,
        occurred_at=None,
    ) -> LotterySettlementResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        user_name = str(user_name)
        business_date = self._normalize_date(business_date)
        deposit = int(deposit)
        occurred_at = self._normalize_time(occurred_at)
        if not operation_id or not user_id or deposit <= 0 or not occurred_at:
            raise ValueError("operation, user, positive deposit and occurrence time are required")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._prepare(conn, business_date, occurred_at)
                previous = self._get_operation(conn, operation_id)
                if previous is not None:
                    if (
                        str(previous[1]) != user_id
                        or int(previous[4]) != deposit
                    ):
                        conn.commit()
                        return LotterySettlementResult("operation_conflict", operation_id)
                    result = self._operation_result(previous, "duplicate")
                    conn.commit()
                    return result

                user = conn.execute(
                    "SELECT COALESCE(user_name,''),COALESCE(stone,0) "
                    "FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.commit()
                    return LotterySettlementResult(
                        "user_missing", operation_id, user_id=user_id,
                        user_name=user_name, business_date=business_date,
                    )

                participant = conn.execute(
                    "SELECT operation_id FROM lottery_participants "
                    "WHERE business_date=%s AND user_id=%s",
                    (business_date, user_id),
                ).fetchone()
                if participant is not None:
                    prior_operation = self._get_operation(conn, str(participant[0]))
                    if prior_operation is not None:
                        result = self._operation_result(
                            prior_operation, "already_participated"
                        )
                    else:
                        pool_row = conn.execute(
                            "SELECT pool_amount FROM lottery_pool_state WHERE state_id=1"
                        ).fetchone()
                        participant_count = int(
                            conn.execute(
                                "SELECT COUNT(*) FROM lottery_participants "
                                "WHERE business_date=%s",
                                (business_date,),
                            ).fetchone()[0]
                        )
                        result = LotterySettlementResult(
                            "already_participated",
                            operation_id,
                            user_id=user_id,
                            user_name=str(user[0]) or user_name,
                            business_date=business_date,
                            pool_before=int(pool_row[0]),
                            pool_after=int(pool_row[0]),
                            participants=participant_count,
                        )
                    conn.commit()
                    return result

                pool_row = conn.execute(
                    "SELECT pool_amount FROM lottery_pool_state WHERE state_id=1"
                ).fetchone()
                pool_before = int(pool_row[0])
                lottery_number = int(self._randint(1, 100000))
                if not 1 <= lottery_number <= 100000:
                    raise ValueError("lottery number must be between 1 and 100000")
                prize_tier = self._tier_for_number(lottery_number)
                funded_pool = pool_before + deposit
                prize = self._prize_for_tier(funded_pool, prize_tier)
                pool_after = funded_pool - prize
                previous_stone = int(user[1])
                wallet_stone = previous_stone + prize
                frozen_name = str(user[0]) or user_name

                conn.execute(
                    "INSERT INTO lottery_participants("
                    "business_date,user_id,operation_id,participated_at) VALUES(%s,%s,%s,%s)",
                    (business_date, user_id, operation_id, occurred_at),
                )
                participants = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM lottery_participants WHERE business_date=%s",
                        (business_date,),
                    ).fetchone()[0]
                )
                conn.execute(
                    "UPDATE lottery_pool_state SET pool_amount=%s,updated_at=%s WHERE state_id=1",
                    (pool_after, occurred_at),
                )

                if prize > 0:
                    updated = conn.execute(
                        "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s",
                        (prize, user_id),
                    )
                    if updated.rowcount != 1:
                        raise db_backend.IntegrityError("lottery winner state changed")
                    conn.execute(
                        "INSERT INTO lottery_winner_history("
                        "operation_id,user_id,user_name,prize_tier,lottery_number,"
                        "prize_amount,won_at) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                        (
                            operation_id,
                            user_id,
                            frozen_name,
                            prize_tier,
                            lottery_number,
                            prize,
                            occurred_at,
                        ),
                    )
                    detail = json.dumps(
                        {
                            "business_date": business_date,
                            "deposit": deposit,
                            "final_stone": wallet_stone,
                            "lottery_number": lottery_number,
                            "pool_after": pool_after,
                            "pool_before": pool_before,
                            "previous_stone": previous_stone,
                            "prize_tier": prize_tier,
                        },
                        ensure_ascii=True,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    conn.execute(
                        "INSERT INTO economy_log("
                        "user_id,source,action,stone_delta,item_delta,detail,trace_id,created_at) "
                        "VALUES(%s,'lottery','lottery_prize',%s,'[]',%s,%s,%s)",
                        (user_id, prize, detail, operation_id, occurred_at),
                    )

                conn.execute(
                    "INSERT INTO lottery_settlement_operations("
                    "operation_id,user_id,user_name,business_date,deposit_amount,lottery_number,"
                    "prize_tier,prize_amount,pool_before,pool_after,participant_count,"
                    "wallet_stone,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        operation_id,
                        user_id,
                        frozen_name,
                        business_date,
                        deposit,
                        lottery_number,
                        prize_tier,
                        prize,
                        pool_before,
                        pool_after,
                        participants,
                        wallet_stone,
                        occurred_at,
                    ),
                )
                row = self._get_operation(conn, operation_id)
                result = self._operation_result(row, "settled")
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def get_snapshot(self, business_date, *, occurred_at=None) -> LotteryPoolSnapshot:
        business_date = self._normalize_date(business_date)
        occurred_at = self._normalize_time(occurred_at)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._prepare(conn, business_date, occurred_at)
                pool = int(
                    conn.execute(
                        "SELECT pool_amount FROM lottery_pool_state WHERE state_id=1"
                    ).fetchone()[0]
                )
                participants = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM lottery_participants WHERE business_date=%s",
                        (business_date,),
                    ).fetchone()[0]
                )
                winner_row = conn.execute(
                    "SELECT user_id,user_name,won_at,prize_amount,lottery_number,prize_tier "
                    "FROM lottery_winner_history ORDER BY id DESC LIMIT 1"
                ).fetchone()
                winner = None
                if winner_row is not None:
                    winner = LotteryWinner(
                        str(winner_row[0]),
                        str(winner_row[1]),
                        str(winner_row[2]),
                        int(winner_row[3]),
                        int(winner_row[4]),
                        str(winner_row[5]),
                    )
                conn.commit()
                return LotteryPoolSnapshot(
                    business_date, pool, participants, winner
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class XiangyuanCreateResult:
    status: str
    gift_id: int = 0
    send_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

@dataclass(frozen=True)
class XiangyuanClaimResult:
    status: str
    gift_id: int = 0
    giver_name: str = ""
    stone: int = 0
    items: tuple[tuple[int, str, int], ...] = ()
    received: int = 0
    receiver_count: int = 0
    remaining_stone: int = 0
    receive_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class XiangyuanSettlementService:
    """Store xiangyuan pools in the database and settle both sides atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS xiangyuan_groups ("
            "group_id TEXT PRIMARY KEY,next_gift_id INTEGER NOT NULL DEFAULT 1,"
            "legacy_imported INTEGER NOT NULL DEFAULT 0)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS xiangyuan_gifts ("
            "group_id TEXT NOT NULL,gift_id INTEGER NOT NULL,giver_id TEXT NOT NULL,"
            "giver_name TEXT NOT NULL,stone_amount INTEGER NOT NULL,remaining_stone INTEGER NOT NULL,"
            "receiver_count INTEGER NOT NULL,received INTEGER NOT NULL DEFAULT 0,create_time TEXT NOT NULL,"
            "PRIMARY KEY(group_id,gift_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS xiangyuan_gift_items ("
            "group_id TEXT NOT NULL,gift_id INTEGER NOT NULL,goods_id INTEGER NOT NULL,"
            "goods_name TEXT NOT NULL,goods_type TEXT NOT NULL,quantity INTEGER NOT NULL,"
            "PRIMARY KEY(group_id,gift_id,goods_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS xiangyuan_receivers ("
            "group_id TEXT NOT NULL,gift_id INTEGER NOT NULL,user_id TEXT NOT NULL,"
            "stone INTEGER NOT NULL,items TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY(group_id,gift_id,user_id))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS xiangyuan_create_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,gift_id INTEGER NOT NULL,"
            "send_count INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS xiangyuan_claim_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.xiangyuan_limit ("
            "user_id TEXT PRIMARY KEY,send_count INTEGER DEFAULT 0,receive_count INTEGER DEFAULT 0,"
            "last_reset_date TEXT DEFAULT '')"
        )
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA player_data.table_info(xiangyuan_limit)").fetchall()
        }
        for name, sql_type, default in (
            ("send_count", "INTEGER", "0"),
            ("receive_count", "INTEGER", "0"),
            ("last_reset_date", "TEXT", "''"),
        ):
            if name not in columns:
                conn.execute(
                    f"ALTER TABLE player_data.xiangyuan_limit ADD COLUMN "
                    f"{db_backend.quote_ident(name)} {sql_type} DEFAULT {default}"
                )

    @staticmethod
    def _normalize_items(items) -> tuple[tuple[int, str, str, int], ...]:
        merged: dict[int, list] = {}
        for item in items or ():
            goods_id = int(item["goods_id"])
            quantity = int(item["quantity"])
            if quantity <= 0:
                continue
            metadata = [str(item["name"]), str(item["type"])]
            if goods_id in merged and merged[goods_id][:2] != metadata:
                raise ValueError("conflicting xiangyuan item metadata")
            merged.setdefault(goods_id, metadata + [0])[2] += quantity
        return tuple((goods_id, *values) for goods_id, values in sorted(merged.items()))

    @classmethod
    def _import_legacy(cls, conn, group_id: str, legacy_data) -> None:
        row = conn.execute(
            "SELECT legacy_imported FROM xiangyuan_groups WHERE group_id=%s", (group_id,)
        ).fetchone()
        if row is not None and int(row[0]) == 1:
            return
        data = legacy_data if isinstance(legacy_data, dict) else {}
        gifts = data.get("gifts", {}) if isinstance(data.get("gifts", {}), dict) else {}
        largest_id = 0
        for raw_id, gift in gifts.items():
            if not isinstance(gift, dict):
                continue
            gift_id = int(gift.get("id", raw_id))
            largest_id = max(largest_id, gift_id)
            conn.execute(
                "INSERT OR IGNORE INTO xiangyuan_gifts (group_id,gift_id,giver_id,giver_name,"
                "stone_amount,remaining_stone,receiver_count,received,create_time) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    group_id, gift_id, str(gift.get("giver_id", "")), str(gift.get("giver_name", "")),
                    int(gift.get("stone_amount", 0)), int(gift.get("remaining_stone", 0)),
                    int(gift.get("receiver_count", 0)), int(gift.get("received", 0)),
                    str(gift.get("create_time", "")),
                ),
            )
            for goods_id, name, item_type, quantity in cls._normalize_items(gift.get("items", ())):
                conn.execute(
                    "INSERT OR IGNORE INTO xiangyuan_gift_items "
                    "(group_id,gift_id,goods_id,goods_name,goods_type,quantity) VALUES (%s,%s,%s,%s,%s,%s)",
                    (group_id, gift_id, goods_id, name, item_type, quantity),
                )
            for receiver in gift.get("receivers", ()):
                conn.execute(
                    "INSERT OR IGNORE INTO xiangyuan_receivers "
                    "(group_id,gift_id,user_id,stone,items) VALUES (%s,%s,%s,%s,%s)",
                    (group_id, gift_id, str(receiver), 0, "[]"),
                )
        next_id = max(int(data.get("last_id", 1) or 1), largest_id + 1)
        conn.execute(
            "INSERT INTO xiangyuan_groups (group_id,next_gift_id,legacy_imported) VALUES (%s,%s,1) "
            "ON CONFLICT(group_id) DO UPDATE SET next_gift_id=MAX(xiangyuan_groups.next_gift_id,EXCLUDED.next_gift_id),legacy_imported=1",
            (group_id, next_id),
        )

    def create(
        self, operation_id, group_id, giver_id, giver_name, stone, items,
        receiver_count, send_limit, *, legacy_data=None,
    ) -> XiangyuanCreateResult:
        operation_id, group_id, giver_id = str(operation_id).strip(), str(group_id), str(giver_id)
        giver_name = str(giver_name)
        stone, receiver_count, send_limit = map(int, (stone, receiver_count, send_limit))
        item_rows = self._normalize_items(items)
        if not operation_id or stone < 0 or receiver_count <= 0 or send_limit <= 0 or (stone == 0 and not item_rows):
            raise ValueError("valid xiangyuan gift is required")
        payload = json.dumps(
            [group_id, giver_id, giver_name, stone, item_rows, receiver_count, send_limit],
            ensure_ascii=True, separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                self._import_legacy(conn, group_id, legacy_data)
                previous = conn.execute(
                    "SELECT payload,gift_id,send_count FROM xiangyuan_create_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    status = "duplicate" if str(previous[0]) == payload else "operation_conflict"
                    return XiangyuanCreateResult(status, int(previous[1]), int(previous[2]))
                user = conn.execute(
                    "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (giver_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return XiangyuanCreateResult("user_missing")
                conn.execute(
                    "INSERT OR IGNORE INTO player_data.xiangyuan_limit "
                    "(user_id,send_count,receive_count,last_reset_date) VALUES (%s,0,0,'')", (giver_id,)
                )
                count = int(conn.execute(
                    "SELECT COALESCE(send_count,0) FROM player_data.xiangyuan_limit WHERE user_id=%s",
                    (giver_id,),
                ).fetchone()[0])
                if count >= send_limit:
                    conn.rollback()
                    return XiangyuanCreateResult("limit_reached", send_count=count)
                if int(user[0]) < stone:
                    conn.rollback()
                    return XiangyuanCreateResult("stone_insufficient", send_count=count)
                back_columns = {
                    str(row[1]) for row in conn.execute("PRAGMA table_info(back)").fetchall()
                }
                state_sql = "COALESCE(state,0)" if "state" in back_columns else "0"
                bind_sql = "COALESCE(bind_num,0)" if "bind_num" in back_columns else "0"
                for goods_id, _, _, quantity in item_rows:
                    row = conn.execute(
                        f"SELECT COALESCE(goods_num,0)-{bind_sql}-{state_sql} FROM back "
                        "WHERE user_id=%s AND goods_id=%s", (giver_id, goods_id),
                    ).fetchone()
                    if row is None or int(row[0]) < quantity:
                        conn.rollback()
                        return XiangyuanCreateResult("item_insufficient", send_count=count)
                gift_id = int(conn.execute(
                    "SELECT next_gift_id FROM xiangyuan_groups WHERE group_id=%s", (group_id,)
                ).fetchone()[0])
                conn.execute("UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(%s AS REAL) WHERE user_id=%s", (stone, giver_id))
                for goods_id, _, _, quantity in item_rows:
                    conn.execute(
                        "UPDATE back SET goods_num=goods_num-%s,update_time=%s WHERE user_id=%s AND goods_id=%s",
                        (quantity, datetime.now(), giver_id, goods_id),
                    )
                conn.execute(
                    "INSERT INTO xiangyuan_gifts (group_id,gift_id,giver_id,giver_name,stone_amount,"
                    "remaining_stone,receiver_count,received,create_time) VALUES (%s,%s,%s,%s,%s,%s,%s,0,%s)",
                    (group_id, gift_id, giver_id, giver_name, stone, stone, receiver_count, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                )
                for goods_id, name, item_type, quantity in item_rows:
                    conn.execute(
                        "INSERT INTO xiangyuan_gift_items "
                        "(group_id,gift_id,goods_id,goods_name,goods_type,quantity) VALUES (%s,%s,%s,%s,%s,%s)",
                        (group_id, gift_id, goods_id, name, item_type, quantity),
                    )
                conn.execute("UPDATE xiangyuan_groups SET next_gift_id=%s WHERE group_id=%s", (gift_id + 1, group_id))
                conn.execute(
                    "UPDATE player_data.xiangyuan_limit SET send_count=%s WHERE user_id=%s", (count + 1, giver_id)
                )
                conn.execute(
                    "INSERT INTO xiangyuan_create_operations (operation_id,payload,gift_id,send_count) VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, gift_id, count + 1),
                )
                conn.commit()
                return XiangyuanCreateResult("applied", gift_id, count + 1)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

    @staticmethod
    def _claim_result_from_json(status: str, raw: str) -> XiangyuanClaimResult:
        value = json.loads(raw)
        return XiangyuanClaimResult(status, int(value[0]), str(value[1]), int(value[2]), tuple(tuple(item) for item in value[3]), int(value[4]), int(value[5]), int(value[6]), int(value[7]))

    def claim(
        self, operation_id, group_id, gift_id, user_id, stone_reward, item_ids,
        receive_limit, max_goods_num, *, legacy_data=None,
    ) -> XiangyuanClaimResult:
        operation_id, group_id, user_id = str(operation_id).strip(), str(group_id), str(user_id)
        gift_id, stone_reward, receive_limit, max_goods_num = map(int, (gift_id, stone_reward, receive_limit, max_goods_num))
        item_ids = tuple(sorted({int(value) for value in item_ids}))
        if not operation_id or gift_id <= 0 or stone_reward < 0 or receive_limit <= 0 or max_goods_num <= 0:
            raise ValueError("valid xiangyuan claim is required")
        payload = json.dumps([group_id, gift_id, user_id, stone_reward, item_ids, receive_limit, max_goods_num], separators=(",", ":"))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                self._import_legacy(conn, group_id, legacy_data)
                previous = conn.execute(
                    "SELECT payload,result FROM xiangyuan_claim_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return XiangyuanClaimResult("operation_conflict")
                    return self._claim_result_from_json("duplicate", str(previous[1]))
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return XiangyuanClaimResult("user_missing")
                conn.execute(
                    "INSERT OR IGNORE INTO player_data.xiangyuan_limit "
                    "(user_id,send_count,receive_count,last_reset_date) VALUES (%s,0,0,'')", (user_id,)
                )
                receive_count = int(conn.execute(
                    "SELECT COALESCE(receive_count,0) FROM player_data.xiangyuan_limit WHERE user_id=%s", (user_id,)
                ).fetchone()[0])
                if receive_count >= receive_limit:
                    conn.rollback()
                    return XiangyuanClaimResult("limit_reached", receive_count=receive_count)
                gift = conn.execute(
                    "SELECT giver_name,remaining_stone,receiver_count,received FROM xiangyuan_gifts "
                    "WHERE group_id=%s AND gift_id=%s", (group_id, gift_id),
                ).fetchone()
                if gift is None or int(gift[3]) >= int(gift[2]):
                    conn.rollback()
                    return XiangyuanClaimResult("unavailable", gift_id=gift_id)
                if conn.execute(
                    "SELECT 1 FROM xiangyuan_receivers WHERE group_id=%s AND gift_id=%s AND user_id=%s",
                    (group_id, gift_id, user_id),
                ).fetchone():
                    conn.rollback()
                    return XiangyuanClaimResult("already_received", gift_id=gift_id)
                remaining_stone = int(gift[1])
                is_last = int(gift[3]) + 1 >= int(gift[2])
                if stone_reward > remaining_stone or (is_last and stone_reward != remaining_stone):
                    conn.rollback()
                    return XiangyuanClaimResult("state_changed", gift_id=gift_id)
                available_items = {}
                for row in conn.execute(
                    "SELECT goods_id,goods_name,goods_type,quantity FROM xiangyuan_gift_items "
                    "WHERE group_id=%s AND gift_id=%s AND quantity>0", (group_id, gift_id),
                ).fetchall():
                    available_items[int(row[0])] = (str(row[1]), str(row[2]), int(row[3]))
                if any(item_id not in available_items for item_id in item_ids):
                    conn.rollback()
                    return XiangyuanClaimResult("state_changed", gift_id=gift_id)
                awarded = tuple((item_id, available_items[item_id][0], 1) for item_id in item_ids)
                for item_id, _, amount in awarded:
                    current = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                    ).fetchone()
                    if (int(current[0]) if current else 0) + amount > max_goods_num:
                        conn.rollback()
                        return XiangyuanClaimResult("inventory_full", gift_id=gift_id)
                conn.execute("UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s", (stone_reward, user_id))
                back_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(back)").fetchall()}
                now = datetime.now()
                for item_id, name, amount in awarded:
                    item_type = available_items[item_id][1]
                    if "bind_num" in back_columns:
                        conn.execute(
                            "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                            "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+1,"
                            "bind_num=COALESCE(back.bind_num,0)+1,update_time=EXCLUDED.update_time",
                            (user_id, item_id, name, item_type, amount, now, now, amount),
                        )
                    else:
                        conn.execute(
                            "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                            "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+1,update_time=EXCLUDED.update_time",
                            (user_id, item_id, name, item_type, amount, now, now),
                        )
                    conn.execute(
                        "UPDATE xiangyuan_gift_items SET quantity=quantity-1 WHERE group_id=%s AND gift_id=%s AND goods_id=%s",
                        (group_id, gift_id, item_id),
                    )
                received = int(gift[3]) + 1
                remaining_stone -= stone_reward
                conn.execute(
                    "UPDATE xiangyuan_gifts SET received=%s,remaining_stone=%s WHERE group_id=%s AND gift_id=%s",
                    (received, remaining_stone, group_id, gift_id),
                )
                conn.execute(
                    "INSERT INTO xiangyuan_receivers (group_id,gift_id,user_id,stone,items) VALUES (%s,%s,%s,%s,%s)",
                    (group_id, gift_id, user_id, stone_reward, json.dumps(awarded, ensure_ascii=True)),
                )
                conn.execute(
                    "UPDATE player_data.xiangyuan_limit SET receive_count=%s WHERE user_id=%s", (receive_count + 1, user_id)
                )
                result_data = [gift_id, str(gift[0]), stone_reward, awarded, received, int(gift[2]), remaining_stone, receive_count + 1]
                conn.execute(
                    "INSERT INTO xiangyuan_claim_operations (operation_id,payload,result) VALUES (%s,%s,%s)",
                    (operation_id, payload, json.dumps(result_data, ensure_ascii=True)),
                )
                conn.commit()
                return self._claim_result_from_json("applied", json.dumps(result_data))
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

    def get_group(self, group_id, *, legacy_data=None) -> dict:
        group_id = str(group_id)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                self._import_legacy(conn, group_id, legacy_data)
                group = conn.execute("SELECT next_gift_id FROM xiangyuan_groups WHERE group_id=%s", (group_id,)).fetchone()
                gifts = {}
                for row in conn.execute(
                    "SELECT gift_id,giver_id,giver_name,stone_amount,remaining_stone,receiver_count,received,create_time "
                    "FROM xiangyuan_gifts WHERE group_id=%s ORDER BY gift_id", (group_id,),
                ).fetchall():
                    gift_id = int(row[0])
                    item_rows = conn.execute(
                        "SELECT goods_id,goods_name,goods_type,quantity FROM xiangyuan_gift_items "
                        "WHERE group_id=%s AND gift_id=%s ORDER BY goods_id", (group_id, gift_id),
                    ).fetchall()
                    receivers = conn.execute(
                        "SELECT user_id FROM xiangyuan_receivers WHERE group_id=%s AND gift_id=%s ORDER BY created_at,user_id",
                        (group_id, gift_id),
                    ).fetchall()
                    gifts[str(gift_id)] = {
                        "id": gift_id, "giver_id": str(row[1]), "giver_name": str(row[2]),
                        "stone_amount": int(row[3]), "remaining_stone": int(row[4]),
                        "items": [{"goods_id": int(item[0]), "name": str(item[1]), "type": str(item[2]), "quantity": int(item[3])} for item in item_rows],
                        "receiver_count": int(row[5]), "received": int(row[6]),
                        "receivers": [str(receiver[0]) for receiver in receivers], "create_time": str(row[7]),
                    }
                conn.commit()
                return {"gifts": gifts, "last_id": int(group[0]) if group else 1}
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

    def clear_all(self, max_goods_num: int) -> tuple[int, int, int, int]:
        """Refund every active database pool and clear its authoritative state."""
        max_goods_num = int(max_goods_num)
        if max_goods_num <= 0:
            raise ValueError("max_goods_num must be positive")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                if not conn.table_exists("xiangyuan_gifts"):
                    conn.rollback()
                    return 0, 0, 0, 0
                rows = conn.execute(
                    "SELECT group_id,gift_id,giver_id,remaining_stone FROM xiangyuan_gifts"
                ).fetchall()
                if not rows:
                    conn.rollback()
                    return 0, 0, 0, 0
                groups = {str(row[0]) for row in rows}
                refund_stone = 0
                refund_items = 0
                back_columns = {
                    str(row[1]) for row in conn.execute("PRAGMA table_info(back)").fetchall()
                }
                now = datetime.now()
                for group_id, gift_id, giver_id, remaining_stone in rows:
                    remaining_stone = int(remaining_stone)
                    if remaining_stone > 0:
                        conn.execute(
                            "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s",
                            (remaining_stone, str(giver_id)),
                        )
                        refund_stone += remaining_stone
                    for item in conn.execute(
                        "SELECT goods_id,goods_name,goods_type,quantity FROM xiangyuan_gift_items "
                        "WHERE group_id=%s AND gift_id=%s AND quantity>0",
                        (str(group_id), int(gift_id)),
                    ).fetchall():
                        goods_id, name, item_type, quantity = int(item[0]), str(item[1]), str(item[2]), int(item[3])
                        current = conn.execute(
                            "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s",
                            (str(giver_id), goods_id),
                        ).fetchone()
                        if (int(current[0]) if current else 0) + quantity > max_goods_num:
                            conn.rollback()
                            raise ValueError("inventory_full")
                        if "bind_num" in back_columns:
                            conn.execute(
                                "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                                "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+EXCLUDED.goods_num,"
                                "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,update_time=EXCLUDED.update_time",
                                (str(giver_id), goods_id, name, item_type, quantity, now, now, quantity),
                            )
                        else:
                            conn.execute(
                                "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time) "
                                "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                                "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+EXCLUDED.goods_num,update_time=EXCLUDED.update_time",
                                (str(giver_id), goods_id, name, item_type, quantity, now, now),
                            )
                        refund_items += quantity
                conn.execute("DELETE FROM xiangyuan_receivers")
                conn.execute("DELETE FROM xiangyuan_gift_items")
                conn.execute("DELETE FROM xiangyuan_gifts")
                conn.execute("UPDATE xiangyuan_groups SET next_gift_id=1")
                conn.commit()
                return len(groups), len(rows), refund_stone, refund_items
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class DirectBreakthroughResult:
    status: str
    user_id: str
    outcome: str
    from_level: str = ""
    to_level: str = ""
    exp_loss: int = 0

    @property
    def applied(self) -> bool:
        return self.status == "applied"

@dataclass(frozen=True)
class ContinuousBreakthroughResult:
    status: str
    user_id: str
    from_level: str
    to_level: str
    attempts: int
    fail_count: int
    exp_loss: int

    @property
    def applied(self) -> bool:
        return self.status == "applied"

class BreakthroughService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS direct_breakthrough_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                outcome TEXT NOT NULL,
                from_level TEXT NOT NULL,
                to_level TEXT NOT NULL,
                exp_loss INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_tribulation_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tribulation_breakthrough_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                outcome TEXT NOT NULL,
                from_level TEXT NOT NULL,
                to_level TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                item_count INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_continuous_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS continuous_breakthrough_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                from_level TEXT NOT NULL,
                to_level TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                fail_count INTEGER NOT NULL,
                exp_loss INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_continuous_tribulation_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS continuous_tribulation_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                from_level TEXT NOT NULL,
                to_level TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                fail_count INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                item_count INTEGER NOT NULL,
                exp_gain INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply_continuous_tribulation(
        self,
        operation_id,
        user_id,
        expected_level,
        expected_exp,
        expected_hp,
        expected_mp,
        expected_rate,
        final_level,
        final_exp,
        final_rate,
        attempts,
        fail_count,
        item_id,
        item_count,
        exp_gain,
        *,
        root_rate=0.0,
        level_spend=0.0,
        occurred_at: datetime | None = None,
    ) -> ContinuousBreakthroughResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        expected_level = str(expected_level)
        final_level = str(final_level)
        expected_exp = int(expected_exp)
        expected_hp = int(expected_hp)
        expected_mp = int(expected_mp)
        expected_rate = int(expected_rate)
        final_exp = max(int(final_exp), 0)
        final_rate = max(int(final_rate), 0)
        attempts = max(int(attempts), 0)
        fail_count = max(int(fail_count), 0)
        item_id = int(item_id)
        item_count = max(int(item_count), 0)
        exp_gain = max(int(exp_gain), 0)
        occurred_at = occurred_at or datetime.now()

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_continuous_tribulation_operations(conn)
                previous = conn.execute(
                    "SELECT from_level, to_level, attempts, fail_count, exp_gain "
                    "FROM continuous_tribulation_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return ContinuousBreakthroughResult(
                        "duplicate", user_id, str(previous[0]), str(previous[1]),
                        int(previous[2]), int(previous[3]), -int(previous[4]),
                    )

                user = conn.execute(
                    "SELECT level, exp, hp, mp, level_up_rate "
                    "FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return ContinuousBreakthroughResult(
                        "user_missing", user_id, expected_level, final_level,
                        attempts, fail_count, -exp_gain,
                    )
                if (
                    str(user[0]) != expected_level
                    or int(user[1] or 0) != expected_exp
                    or int(user[2] or 0) != expected_hp
                    or int(user[3] or 0) != expected_mp
                    or int(user[4] or 0) != expected_rate
                ):
                    conn.rollback()
                    return ContinuousBreakthroughResult(
                        "state_changed", user_id, str(user[0]), str(user[0]),
                        attempts, fail_count, -exp_gain,
                    )

                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item_count <= 0 or item is None or int(item[0] or 0) < item_count:
                    conn.rollback()
                    return ContinuousBreakthroughResult(
                        "item_missing", user_id, expected_level, final_level,
                        attempts, fail_count, -exp_gain,
                    )

                if final_level != expected_level:
                    conn.execute(
                        "UPDATE user_xiuxian SET level=%s, exp=%s, "
                        "power=ROUND(%s*%s*%s, 0), level_up_cd=%s, "
                        "level_up_rate=0, hp=%s/2, mp=%s, atk=%s/10 "
                        "WHERE user_id=%s",
                        (
                            final_level, final_exp, final_exp, float(root_rate),
                            float(level_spend), occurred_at, final_exp, final_exp,
                            final_exp, user_id,
                        ),
                    )
                else:
                    conn.execute(
                        "UPDATE user_xiuxian SET exp=%s, level_up_rate=%s, "
                        "level_up_cd=%s WHERE user_id=%s",
                        (final_exp, final_rate, occurred_at, user_id),
                    )

                consumed = conn.execute(
                    "UPDATE back SET update_time=%s, action_time=%s, "
                    "day_num=CASE WHEN goods_type='丹药' "
                    "THEN COALESCE(day_num, 0)+%s ELSE COALESCE(day_num, 0) END, "
                    "all_num=CASE WHEN goods_type='丹药' "
                    "THEN COALESCE(all_num, 0)+%s ELSE COALESCE(all_num, 0) END, "
                    "goods_num=goods_num-%s, "
                    "bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s) "
                    "WHERE user_id=%s AND goods_id=%s "
                    "AND COALESCE(goods_num, 0)>=%s",
                    (
                        occurred_at, occurred_at, item_count, item_count,
                        item_count, item_count, user_id, item_id, item_count,
                    ),
                )
                if consumed.rowcount != 1:
                    raise db_backend.IntegrityError(
                        "continuous tribulation items changed concurrently"
                    )
                conn.execute(
                    "INSERT INTO continuous_tribulation_operations "
                    "(operation_id, user_id, from_level, to_level, attempts, "
                    "fail_count, item_id, item_count, exp_gain) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id, user_id, expected_level, final_level,
                        attempts, fail_count, item_id, item_count, exp_gain,
                    ),
                )
                conn.commit()
                return ContinuousBreakthroughResult(
                    "applied", user_id, expected_level, final_level,
                    attempts, fail_count, -exp_gain,
                )
            except Exception:
                conn.rollback()
                raise

    def apply_continuous(
        self,
        operation_id,
        user_id,
        expected_level,
        expected_exp,
        expected_hp,
        expected_mp,
        expected_rate,
        final_level,
        final_exp,
        final_hp,
        final_mp,
        final_rate,
        attempts,
        fail_count,
        exp_loss,
        *,
        root_rate=0.0,
        level_spend=0.0,
        occurred_at: datetime | None = None,
    ) -> ContinuousBreakthroughResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        expected_level = str(expected_level)
        final_level = str(final_level)
        expected_exp = int(expected_exp)
        expected_hp = int(expected_hp)
        expected_mp = int(expected_mp)
        expected_rate = int(expected_rate)
        final_exp = max(int(final_exp), 0)
        final_hp = max(int(final_hp), 1)
        final_mp = max(int(final_mp), 1)
        final_rate = max(int(final_rate), 0)
        attempts = max(int(attempts), 0)
        fail_count = max(int(fail_count), 0)
        exp_loss = max(int(exp_loss), 0)
        occurred_at = occurred_at or datetime.now()

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_continuous_operations(conn)
                previous = conn.execute(
                    "SELECT from_level, to_level, attempts, fail_count, exp_loss "
                    "FROM continuous_breakthrough_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return ContinuousBreakthroughResult(
                        "duplicate",
                        user_id,
                        str(previous[0]),
                        str(previous[1]),
                        int(previous[2]),
                        int(previous[3]),
                        int(previous[4]),
                    )

                user = conn.execute(
                    "SELECT level, exp, hp, mp, level_up_rate "
                    "FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return ContinuousBreakthroughResult(
                        "user_missing", user_id, expected_level, final_level,
                        attempts, fail_count, exp_loss,
                    )
                if (
                    str(user[0]) != expected_level
                    or int(user[1] or 0) != expected_exp
                    or int(user[2] or 0) != expected_hp
                    or int(user[3] or 0) != expected_mp
                    or int(user[4] or 0) != expected_rate
                ):
                    conn.rollback()
                    return ContinuousBreakthroughResult(
                        "state_changed", user_id, str(user[0]), str(user[0]),
                        attempts, fail_count, exp_loss,
                    )

                if final_level != expected_level:
                    conn.execute(
                        "UPDATE user_xiuxian SET level=%s, exp=%s, "
                        "power=ROUND(%s*%s*%s, 0), level_up_cd=%s, "
                        "level_up_rate=0, hp=%s/2, mp=%s, atk=%s/10 "
                        "WHERE user_id=%s",
                        (
                            final_level,
                            final_exp,
                            final_exp,
                            float(root_rate),
                            float(level_spend),
                            occurred_at,
                            final_exp,
                            final_exp,
                            final_exp,
                            user_id,
                        ),
                    )
                else:
                    conn.execute(
                        "UPDATE user_xiuxian SET exp=%s, hp=%s, mp=%s, "
                        "level_up_rate=%s, level_up_cd=%s WHERE user_id=%s",
                        (
                            final_exp,
                            final_hp,
                            final_mp,
                            final_rate,
                            occurred_at,
                            user_id,
                        ),
                    )
                conn.execute(
                    "INSERT INTO continuous_breakthrough_operations "
                    "(operation_id, user_id, from_level, to_level, attempts, "
                    "fail_count, exp_loss) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        user_id,
                        expected_level,
                        final_level,
                        attempts,
                        fail_count,
                        exp_loss,
                    ),
                )
                conn.commit()
                return ContinuousBreakthroughResult(
                    "applied", user_id, expected_level, final_level,
                    attempts, fail_count, exp_loss,
                )
            except Exception:
                conn.rollback()
                raise

    def apply_failure(
        self,
        operation_id,
        user_id,
        expected_level,
        expected_exp,
        expected_hp,
        expected_mp,
        expected_rate,
        exp_loss,
        new_hp,
        new_mp,
        new_rate,
        *,
        occurred_at: datetime | None = None,
    ) -> DirectBreakthroughResult:
        return self._apply(
            operation_id,
            user_id,
            "failure",
            expected_level,
            expected_level,
            expected_exp,
            expected_hp,
            expected_mp,
            expected_rate,
            exp_loss=max(int(exp_loss), 0),
            new_hp=new_hp,
            new_mp=new_mp,
            new_rate=new_rate,
            occurred_at=occurred_at,
        )

    def apply_success(
        self,
        operation_id,
        user_id,
        expected_level,
        target_level,
        expected_exp,
        expected_hp,
        expected_mp,
        expected_rate,
        root_rate,
        level_spend,
        *,
        occurred_at: datetime | None = None,
    ) -> DirectBreakthroughResult:
        return self._apply(
            operation_id,
            user_id,
            "success",
            expected_level,
            target_level,
            expected_exp,
            expected_hp,
            expected_mp,
            expected_rate,
            root_rate=float(root_rate),
            level_spend=float(level_spend),
            occurred_at=occurred_at,
        )

    def apply_tribulation_failure(
        self,
        operation_id,
        user_id,
        expected_level,
        expected_exp,
        expected_hp,
        expected_mp,
        expected_rate,
        new_rate,
        item_id,
        exp_gain=0,
        *,
        occurred_at: datetime | None = None,
    ) -> DirectBreakthroughResult:
        return self._apply_tribulation(
            operation_id,
            user_id,
            "failure",
            expected_level,
            expected_level,
            expected_exp,
            expected_hp,
            expected_mp,
            expected_rate,
            item_id,
            new_rate=new_rate,
            exp_gain=exp_gain,
            occurred_at=occurred_at,
        )

    def apply_tribulation_success(
        self,
        operation_id,
        user_id,
        expected_level,
        target_level,
        expected_exp,
        expected_hp,
        expected_mp,
        expected_rate,
        root_rate,
        level_spend,
        item_id,
        exp_gain=0,
        *,
        occurred_at: datetime | None = None,
    ) -> DirectBreakthroughResult:
        return self._apply_tribulation(
            operation_id,
            user_id,
            "success",
            expected_level,
            target_level,
            expected_exp,
            expected_hp,
            expected_mp,
            expected_rate,
            item_id,
            root_rate=root_rate,
            level_spend=level_spend,
            exp_gain=exp_gain,
            occurred_at=occurred_at,
        )

    def _apply_tribulation(
        self,
        operation_id,
        user_id,
        outcome,
        expected_level,
        target_level,
        expected_exp,
        expected_hp,
        expected_mp,
        expected_rate,
        item_id,
        *,
        new_rate=0,
        root_rate=0.0,
        level_spend=0.0,
        exp_gain=0,
        occurred_at=None,
    ) -> DirectBreakthroughResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        expected_level = str(expected_level)
        target_level = str(target_level)
        expected_exp = int(expected_exp)
        expected_hp = int(expected_hp)
        expected_mp = int(expected_mp)
        expected_rate = int(expected_rate)
        item_id = int(item_id)
        exp_gain = max(int(exp_gain), 0)
        occurred_at = occurred_at or datetime.now()

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_tribulation_operations(conn)
                previous = conn.execute(
                    "SELECT outcome, from_level, to_level FROM "
                    "tribulation_breakthrough_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return DirectBreakthroughResult(
                        "duplicate",
                        user_id,
                        str(previous[0]),
                        str(previous[1]),
                        str(previous[2]),
                    )

                user = conn.execute(
                    "SELECT level, exp, hp, mp, level_up_rate "
                    "FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return DirectBreakthroughResult("user_missing", user_id, outcome)
                if (
                    str(user[0]) != expected_level
                    or int(user[1] or 0) != expected_exp
                    or int(user[2] or 0) != expected_hp
                    or int(user[3] or 0) != expected_mp
                    or int(user[4] or 0) != expected_rate
                ):
                    conn.rollback()
                    return DirectBreakthroughResult(
                        "state_changed", user_id, outcome, str(user[0]), str(user[0])
                    )

                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None or int(item[0] or 0) < 1:
                    conn.rollback()
                    return DirectBreakthroughResult(
                        "item_missing", user_id, outcome, expected_level, target_level
                    )

                if outcome == "failure":
                    conn.execute(
                        "UPDATE user_xiuxian SET exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL), level_up_rate=%s, level_up_cd=%s "
                        "WHERE user_id=%s",
                        (exp_gain, int(new_rate), occurred_at, user_id),
                    )
                elif outcome == "success":
                    conn.execute(
                        "UPDATE user_xiuxian SET level=%s, exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL), "
                        "power=ROUND((exp+%s)*%s*%s, 0), level_up_cd=%s, "
                        "level_up_rate=0, hp=(exp+%s)/2, mp=exp+%s, atk=(exp+%s)/10 "
                        "WHERE user_id=%s",
                        (
                            target_level,
                            exp_gain,
                            exp_gain,
                            float(root_rate),
                            float(level_spend),
                            occurred_at,
                            exp_gain,
                            exp_gain,
                            exp_gain,
                            user_id,
                        ),
                    )
                else:
                    raise ValueError(f"unsupported breakthrough outcome: {outcome}")

                consumed = conn.execute(
                    "UPDATE back SET update_time=%s, action_time=%s, "
                    "day_num=CASE WHEN goods_type='丹药' THEN COALESCE(day_num, 0)+1 "
                    "ELSE COALESCE(day_num, 0) END, "
                    "all_num=CASE WHEN goods_type='丹药' THEN COALESCE(all_num, 0)+1 "
                    "ELSE COALESCE(all_num, 0) END, goods_num=goods_num-1, "
                    "bind_num=MIN(COALESCE(bind_num, 0), goods_num-1) "
                    "WHERE user_id=%s AND goods_id=%s AND COALESCE(goods_num, 0)>=1",
                    (occurred_at, occurred_at, user_id, item_id),
                )
                if consumed.rowcount != 1:
                    raise db_backend.IntegrityError("tribulation item changed concurrently")
                conn.execute(
                    "INSERT INTO tribulation_breakthrough_operations "
                    "(operation_id, user_id, outcome, from_level, to_level, "
                    "item_id, item_count) VALUES (%s, %s, %s, %s, %s, %s, 1)",
                    (
                        operation_id,
                        user_id,
                        outcome,
                        expected_level,
                        target_level,
                        item_id,
                    ),
                )
                conn.commit()
                return DirectBreakthroughResult(
                    "applied", user_id, outcome, expected_level, target_level
                )
            except Exception:
                conn.rollback()
                raise

    def _apply(
        self,
        operation_id,
        user_id,
        outcome,
        expected_level,
        target_level,
        expected_exp,
        expected_hp,
        expected_mp,
        expected_rate,
        *,
        exp_loss=0,
        new_hp=0,
        new_mp=0,
        new_rate=0,
        root_rate=0.0,
        level_spend=0.0,
        occurred_at=None,
    ) -> DirectBreakthroughResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        expected_level = str(expected_level)
        target_level = str(target_level)
        expected_exp = int(expected_exp)
        expected_hp = int(expected_hp)
        expected_mp = int(expected_mp)
        expected_rate = int(expected_rate)
        occurred_at = occurred_at or datetime.now()

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT outcome, from_level, to_level, exp_loss "
                    "FROM direct_breakthrough_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return DirectBreakthroughResult(
                        "duplicate",
                        user_id,
                        str(previous[0]),
                        str(previous[1]),
                        str(previous[2]),
                        int(previous[3]),
                    )

                user = conn.execute(
                    "SELECT level, exp, hp, mp, level_up_rate "
                    "FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return DirectBreakthroughResult("user_missing", user_id, outcome)
                if (
                    str(user[0]) != expected_level
                    or int(user[1] or 0) != expected_exp
                    or int(user[2] or 0) != expected_hp
                    or int(user[3] or 0) != expected_mp
                    or int(user[4] or 0) != expected_rate
                ):
                    conn.rollback()
                    return DirectBreakthroughResult(
                        "state_changed", user_id, outcome, str(user[0]), str(user[0])
                    )

                if outcome == "failure":
                    conn.execute(
                        "UPDATE user_xiuxian SET exp=MAX(exp-%s, 0), hp=%s, mp=%s, "
                        "level_up_rate=%s, level_up_cd=%s WHERE user_id=%s",
                        (
                            int(exp_loss),
                            int(new_hp),
                            int(new_mp),
                            int(new_rate),
                            occurred_at,
                            user_id,
                        ),
                    )
                elif outcome == "success":
                    conn.execute(
                        "UPDATE user_xiuxian SET level=%s, power=ROUND(exp*%s*%s, 0), "
                        "level_up_cd=%s, level_up_rate=0, hp=exp/2, mp=exp, atk=exp/10 "
                        "WHERE user_id=%s",
                        (
                            target_level,
                            root_rate,
                            level_spend,
                            occurred_at,
                            user_id,
                        ),
                    )
                else:
                    raise ValueError(f"unsupported breakthrough outcome: {outcome}")

                conn.execute(
                    "INSERT INTO direct_breakthrough_operations "
                    "(operation_id, user_id, outcome, from_level, to_level, exp_loss) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        user_id,
                        outcome,
                        expected_level,
                        target_level,
                        int(exp_loss),
                    ),
                )
                conn.commit()
                return DirectBreakthroughResult(
                    "applied",
                    user_id,
                    outcome,
                    expected_level,
                    target_level,
                    int(exp_loss),
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class OrdinaryTribulationResult:
    status: str
    successful: bool = False
    rate: int = 0
    item_used: bool = False
    user_id: str = ""
    target_level: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class OrdinaryTribulationService:
    """Commit one resolved ordinary tribulation without partial state."""

    def __init__(self, game_database, player_database, lock=None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _saved_result(payload, successful, rate, item_used, *, status="duplicate"):
        data = json.loads(str(payload))
        return OrdinaryTribulationResult(
            status,
            bool(successful),
            int(rate),
            bool(item_used),
            str(data[0]),
            str(data[4]),
        )

    def replay(self, operation_id, user_id):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("ordinary_tribulation_operations"):
                return None
            previous = conn.execute(
                "SELECT payload,successful,rate,item_used "
                "FROM ordinary_tribulation_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            saved = self._saved_result(*previous)
            if saved.user_id != user_id:
                return OrdinaryTribulationResult("operation_conflict")
            return saved

    def settle(
        self, operation_id, user_id, *, expected_level, expected_exp,
        expected_rate, target_level, successful, new_rate, occurred_at,
        power=0, consume_destiny_pill=False,
    ) -> OrdinaryTribulationResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_level, target_level = str(expected_level), str(target_level)
        expected_exp, expected_rate, new_rate, power = map(
            int, (expected_exp, expected_rate, new_rate, power)
        )
        successful, consume_destiny_pill = bool(successful), bool(consume_destiny_pill)
        occurred_at = str(occurred_at)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        payload = json.dumps(
            [user_id, expected_level, expected_exp, expected_rate, target_level,
             successful, new_rate, occurred_at, power, consume_destiny_pill],
            ensure_ascii=True, separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS ordinary_tribulation_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,successful INTEGER NOT NULL,"
                    "rate INTEGER NOT NULL,item_used INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,successful,rate,item_used FROM ordinary_tribulation_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    saved = self._saved_result(*previous)
                    if saved.user_id != user_id:
                        return OrdinaryTribulationResult("operation_conflict", False, 0, False)
                    return saved
                user = conn.execute(
                    "SELECT level,exp FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                state = conn.execute(
                    "SELECT current_rate FROM user_tribulation WHERE user_id=%s", (user_id,)
                ).fetchone()
                # 无渡劫状态行时，与 get_user_tribulation_info 默认值一致（base=30）
                actual_rate = int(state[0]) if state else 30
                if user is None or str(user[0]) != expected_level or int(user[1] or 0) != expected_exp or actual_rate != expected_rate:
                    conn.rollback()
                    return OrdinaryTribulationResult(
                        "state_changed", False, actual_rate, False, user_id, target_level
                    )
                if consume_destiny_pill:
                    changed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-1,bind_num=MIN(COALESCE(bind_num,0),goods_num-1),"
                        "day_num=COALESCE(day_num,0)+1,all_num=COALESCE(all_num,0)+1,update_time=%s,action_time=%s "
                        "WHERE user_id=%s AND goods_id=1996 AND goods_num>0",
                        (occurred_at, occurred_at, user_id),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return OrdinaryTribulationResult(
                            "item_missing", False, actual_rate, False, user_id, target_level
                        )
                if successful:
                    changed = conn.execute(
                        "UPDATE user_xiuxian SET level=%s,power=%s WHERE user_id=%s AND level=%s AND exp=%s",
                        (target_level, power, user_id, expected_level, expected_exp),
                    )
                    conn.execute("DELETE FROM user_tribulation WHERE user_id=%s", (user_id,))
                else:
                    # 失败：必须能写入/更新渡劫状态。无行时 INSERT，有行时 UPDATE。
                    # 旧逻辑只 UPDATE，首次渡劫失败（无 user_tribulation 行）会 rowcount=0 → 误报 state_changed
                    if state is None:
                        changed = conn.execute(
                            "INSERT INTO user_tribulation "
                            "(user_id, current_rate, heart_devil_count, last_time, next_level) "
                            "VALUES (%s, %s, 0, %s, %s)",
                            (user_id, new_rate, occurred_at, target_level),
                        )
                    else:
                        changed = conn.execute(
                            "UPDATE user_tribulation SET current_rate=%s,last_time=%s,next_level=%s "
                            "WHERE user_id=%s AND current_rate=%s",
                            (new_rate, occurred_at, target_level, user_id, expected_rate),
                        )
                if changed.rowcount != 1:
                    conn.rollback()
                    return OrdinaryTribulationResult(
                        "state_changed", False, actual_rate, False, user_id, target_level
                    )
                increment_stat(conn, user_id, "渡劫次数", 1)
                increment_stat(conn, user_id, "渡劫成功" if successful else "渡劫失败", 1)
                if consume_destiny_pill:
                    increment_stat(conn, user_id, "天命丹消耗", 1)
                conn.execute(
                    "INSERT INTO ordinary_tribulation_operations(operation_id,payload,successful,rate,item_used) VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, int(successful), new_rate, int(consume_destiny_pill)),
                )
                conn.commit()
                return OrdinaryTribulationResult(
                    "applied",
                    successful,
                    new_rate,
                    consume_destiny_pill,
                    user_id,
                    target_level,
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

@dataclass(frozen=True)
class DestinyTribulationResult:
    status: str
    target_level: str = ""
    user_id: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class DestinyTribulationService:
    """Consume the destiny pill and promote the player in one transaction."""

    def __init__(self, game_database, player_database, lock=None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _saved_result(payload, target_level, *, status="duplicate"):
        data = json.loads(str(payload))
        return DestinyTribulationResult(status, str(target_level), str(data[0]))

    def replay(self, operation_id, user_id):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("destiny_tribulation_operations"):
                return None
            previous = conn.execute(
                "SELECT payload,target_level FROM destiny_tribulation_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            saved = self._saved_result(*previous)
            if saved.user_id != user_id:
                return DestinyTribulationResult("operation_conflict")
            return saved

    def settle(self, operation_id, user_id, *, expected_level, expected_exp, target_level, power, occurred_at):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_level, target_level = str(expected_level), str(target_level)
        expected_exp, power, occurred_at = int(expected_exp), int(power), str(occurred_at)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        payload = json.dumps([user_id, expected_level, expected_exp, target_level, power, occurred_at], separators=(",", ":"))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS destiny_tribulation_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,target_level TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,target_level FROM destiny_tribulation_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous:
                    conn.rollback()
                    saved = self._saved_result(*previous)
                    if saved.user_id != user_id:
                        return DestinyTribulationResult("operation_conflict")
                    return saved
                user = conn.execute("SELECT level,exp FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None or str(user[0]) != expected_level or int(user[1] or 0) != expected_exp:
                    conn.rollback(); return DestinyTribulationResult("state_changed", target_level, user_id)
                consumed = conn.execute(
                    "UPDATE back SET goods_num=goods_num-1,bind_num=MIN(COALESCE(bind_num,0),goods_num-1),"
                    "day_num=COALESCE(day_num,0)+1,all_num=COALESCE(all_num,0)+1,update_time=%s,action_time=%s "
                    "WHERE user_id=%s AND goods_id=1997 AND goods_num>0", (occurred_at, occurred_at, user_id),
                )
                if consumed.rowcount != 1:
                    conn.rollback(); return DestinyTribulationResult("item_missing", target_level, user_id)
                changed = conn.execute(
                    "UPDATE user_xiuxian SET level=%s,power=%s WHERE user_id=%s AND level=%s AND exp=%s",
                    (target_level, power, user_id, expected_level, expected_exp),
                )
                if changed.rowcount != 1:
                    conn.rollback(); return DestinyTribulationResult("state_changed", target_level, user_id)
                conn.execute("DELETE FROM user_tribulation WHERE user_id=%s", (user_id,))
                increment_stat(conn, user_id, "渡劫次数", 1)
                increment_stat(conn, user_id, "渡劫成功", 1)
                increment_stat(conn, user_id, "天命渡劫丹消耗", 1)
                conn.execute("INSERT INTO destiny_tribulation_operations(operation_id,payload,target_level) VALUES(%s,%s,%s)", (operation_id, payload, target_level))
                conn.commit(); return DestinyTribulationResult("applied", target_level, user_id)
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass

@dataclass(frozen=True)
class HeartDevilTribulationResult:
    status: str
    successful: bool = False
    rate: int = 0
    heart_devil_count: int = 0
    item_used: bool = False
    user_id: str = ""
    devil_name: str = ""
    message: str = ""
    battle_messages: list = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class HeartDevilTribulationService:
    """Commit one already-resolved heart-devil encounter atomically."""

    def __init__(self, game_database, player_database, lock=None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _saved_result(
        payload, successful, rate, heart_devil_count, item_used, *, status="duplicate"
    ):
        data = json.loads(str(payload))
        return HeartDevilTribulationResult(
            status=status,
            successful=bool(successful),
            rate=int(rate),
            heart_devil_count=int(heart_devil_count),
            item_used=bool(item_used),
            user_id=str(data[0]),
            devil_name=str(data[6] or ""),
            message=str(data[8] or "") if len(data) > 8 else "",
            battle_messages=list(data[9] or []) if len(data) > 9 else [],
        )

    def replay(self, operation_id, user_id):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("heart_devil_tribulation_operations"):
                return None
            previous = conn.execute(
                "SELECT payload,successful,rate,heart_devil_count,item_used "
                "FROM heart_devil_tribulation_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            saved = self._saved_result(*previous)
            if saved.user_id != user_id:
                return HeartDevilTribulationResult("operation_conflict")
            return saved

    def settle(
        self, operation_id, user_id, *, expected_rate, expected_count,
        successful, new_rate, occurred_at, devil_name="", consume_destiny_pill=False,
        message="", battle_messages=None,
    ) -> HeartDevilTribulationResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_rate, expected_count, new_rate = map(int, (expected_rate, expected_count, new_rate))
        successful, consume_destiny_pill = bool(successful), bool(consume_destiny_pill)
        occurred_at, devil_name, message = (
            str(occurred_at), str(devil_name), str(message)
        )
        battle_messages = list(battle_messages or [])
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        payload = json.dumps(
            [
                user_id, expected_rate, expected_count, successful, new_rate,
                occurred_at, devil_name, consume_destiny_pill, message,
                battle_messages,
            ],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS heart_devil_tribulation_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,successful INTEGER NOT NULL,rate INTEGER NOT NULL,"
                    "heart_devil_count INTEGER NOT NULL,item_used INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,successful,rate,heart_devil_count,item_used FROM heart_devil_tribulation_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous:
                    conn.rollback()
                    saved = self._saved_result(*previous)
                    if saved.user_id != user_id:
                        return HeartDevilTribulationResult("operation_conflict", False, 0, 0, False)
                    return saved
                state = conn.execute("SELECT current_rate,heart_devil_count FROM user_tribulation WHERE user_id=%s", (user_id,)).fetchone()
                actual_rate, actual_count = (int(state[0]), int(state[1])) if state else (30, 0)
                if actual_rate != expected_rate or actual_count != expected_count:
                    conn.rollback(); return HeartDevilTribulationResult("state_changed", False, actual_rate, actual_count, False)
                if consume_destiny_pill:
                    consumed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-1,bind_num=MIN(COALESCE(bind_num,0),goods_num-1),"
                        "day_num=COALESCE(day_num,0)+1,all_num=COALESCE(all_num,0)+1,update_time=%s,action_time=%s "
                        "WHERE user_id=%s AND goods_id=1996 AND goods_num>0", (occurred_at, occurred_at, user_id),
                    )
                    if consumed.rowcount != 1:
                        conn.rollback(); return HeartDevilTribulationResult("item_missing", False, actual_rate, actual_count, False)
                new_count = expected_count + 1
                changed = conn.execute(
                    "UPDATE user_tribulation SET current_rate=%s,heart_devil_count=%s,last_time=%s "
                    "WHERE user_id=%s AND current_rate=%s AND heart_devil_count=%s",
                    (new_rate, new_count, occurred_at, user_id, expected_rate, expected_count),
                )
                if changed.rowcount == 0 and state is None:
                    conn.execute(
                        "INSERT INTO user_tribulation(user_id,current_rate,heart_devil_count,last_time) VALUES(%s,%s,%s,%s)",
                        (user_id, new_rate, new_count, occurred_at),
                    )
                elif changed.rowcount != 1:
                    conn.rollback(); return HeartDevilTribulationResult("state_changed", False, actual_rate, actual_count, False)
                increment_stat(conn, user_id, "心魔劫次数", 1)
                increment_stat(conn, user_id, "心魔劫成功" if successful else "心魔劫失败", 1)
                if consume_destiny_pill:
                    increment_stat(conn, user_id, "天命丹消耗", 1)
                conn.execute(
                    "INSERT INTO heart_devil_tribulation_operations(operation_id,payload,successful,rate,heart_devil_count,item_used) VALUES(%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, int(successful), new_rate, new_count, int(consume_destiny_pill)),
                )
                conn.commit()
                return HeartDevilTribulationResult(
                    "applied", successful, new_rate, new_count,
                    consume_destiny_pill, user_id, devil_name, message,
                    battle_messages,
                )
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass

@dataclass(frozen=True)
class PillFusionResult:
    status: str
    user_id: str
    source_item_id: int
    source_quantity: int
    target_item_id: int
    target_quantity: int
    successful: bool

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class PillFusionService:
    """Consume fusion materials and grant a pre-rolled result atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pill_fusion_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                source_item_id INTEGER NOT NULL,
                source_quantity INTEGER NOT NULL,
                target_item_id INTEGER NOT NULL,
                target_quantity INTEGER NOT NULL,
                successful INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply(
        self,
        operation_id,
        user_id,
        source_item_id,
        source_quantity,
        target_item_id,
        target_name,
        target_type,
        *,
        successful,
        target_quantity=1,
        max_goods_num,
    ) -> PillFusionResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        source_item_id = int(source_item_id)
        source_quantity = int(source_quantity)
        target_item_id = int(target_item_id)
        target_name = str(target_name)
        target_type = str(target_type)
        successful = bool(successful)
        target_quantity = int(target_quantity) if successful else 0
        max_goods_num = int(max_goods_num)
        if source_quantity <= 0 or max_goods_num <= 0:
            raise ValueError("source_quantity and max_goods_num must be positive")
        if successful and target_quantity <= 0:
            raise ValueError("target_quantity must be positive after successful fusion")
        if source_item_id == target_item_id:
            raise ValueError("source and target items must differ")

        def result(status: str, success=successful, reward_quantity=target_quantity):
            return PillFusionResult(
                status,
                user_id,
                source_item_id,
                source_quantity,
                target_item_id,
                int(reward_quantity),
                bool(success),
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT target_quantity, successful FROM pill_fusion_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", bool(previous[1]), previous[0])

                source = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, source_item_id),
                ).fetchone()
                if source is None or int(source[0] or 0) < source_quantity:
                    conn.rollback()
                    return result("item_insufficient")

                columns = set(conn.column_names("back"))
                updates = ["goods_num=goods_num-%s"]
                params: list[object] = [source_quantity]
                if "bind_num" in columns:
                    updates.append(
                        "bind_num=CASE WHEN goods_num-%s=0 THEN 0 "
                        "WHEN COALESCE(bind_num, 0)>=%s "
                        "THEN COALESCE(bind_num, 0)-%s "
                        "ELSE MIN(COALESCE(bind_num, 0), goods_num-%s) END"
                    )
                    params.extend((source_quantity,) * 4)
                consumed = conn.execute(
                    f"UPDATE back SET {', '.join(updates)} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, source_item_id, source_quantity),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")

                if successful:
                    conn.execute(
                        "INSERT INTO back (user_id, goods_id, goods_name, goods_type, "
                        "goods_num, bind_num) VALUES (%s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                        "goods_num=MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s), "
                        "bind_num=MIN(COALESCE(back.bind_num, 0)+EXCLUDED.goods_num, "
                        "MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s))",
                        (
                            user_id, target_item_id, target_name, target_type,
                            target_quantity, target_quantity, max_goods_num, max_goods_num,
                        ),
                    )
                conn.execute(
                    "INSERT INTO pill_fusion_operations "
                    "(operation_id, user_id, source_item_id, source_quantity, "
                    "target_item_id, target_quantity, successful) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id, user_id, source_item_id, source_quantity,
                        target_item_id, target_quantity, int(successful),
                    ),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class TribulationStateMigrationResult:
    status: str
    state: dict = field(default_factory=dict)

    @property
    def database_ready(self) -> bool:
        return self.status in {
            "applied",
            "duplicate",
            "database_authoritative",
        }

class TribulationStateMigrationService:
    """Import one legacy tribulation state without overwriting database state."""

    _STATE_FIELDS = (
        "current_rate",
        "heart_devil_count",
        "last_time",
        "next_level",
    )

    def __init__(self, game_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._lock = lock or RLock()

    @staticmethod
    def normalize(data, *, base_rate=30) -> dict:
        data = dict(data or {})
        try:
            current_rate = int(data.get("current_rate", base_rate))
        except (TypeError, ValueError):
            current_rate = int(base_rate)
        try:
            heart_devil_count = int(data.get("heart_devil_count", 0))
        except (TypeError, ValueError):
            heart_devil_count = 0
        return {
            "current_rate": current_rate,
            "heart_devil_count": heart_devil_count,
            "last_time": data.get("last_time") or None,
            "next_level": data.get("next_level") or None,
        }

    @classmethod
    def _row_state(cls, row, *, base_rate=30) -> dict:
        return cls.normalize(
            {
                "current_rate": row[0],
                "heart_devil_count": row[1],
                "last_time": row[2],
                "next_level": row[3],
            },
            base_rate=base_rate,
        )

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tribulation_state_migration_operations("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL UNIQUE,"
            "payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def migrate(self, operation_id, user_id, legacy_data, *, base_rate=30):
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id must not be empty")
        state = self.normalize(legacy_data, base_rate=base_rate)
        payload = json.dumps(
            state,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT user_id,payload FROM tribulation_state_migration_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    status = (
                        "duplicate"
                        if str(previous[0]) == user_id and str(previous[1]) == payload
                        else "operation_conflict"
                    )
                    saved = json.loads(str(previous[1]))
                    return TribulationStateMigrationResult(status, saved)

                current = conn.execute(
                    "SELECT current_rate,heart_devil_count,last_time,next_level "
                    "FROM user_tribulation WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if current is not None:
                    conn.rollback()
                    return TribulationStateMigrationResult(
                        "database_authoritative",
                        self._row_state(current, base_rate=base_rate),
                    )

                conn.execute(
                    "INSERT INTO user_tribulation("
                    "user_id,current_rate,heart_devil_count,last_time,next_level"
                    ") VALUES(%s,%s,%s,%s,%s)",
                    (
                        user_id,
                        state["current_rate"],
                        state["heart_devil_count"],
                        state["last_time"],
                        state["next_level"],
                    ),
                )
                conn.execute(
                    "INSERT INTO tribulation_state_migration_operations("
                    "operation_id,user_id,payload) VALUES(%s,%s,%s)",
                    (operation_id, user_id, payload),
                )
                conn.commit()
                return TribulationStateMigrationResult("applied", state)
            except Exception:
                conn.rollback()
                raise

__all__ = [
    "SignInResult",
    "SignInService",
    "PlayerRenameResult",
    "PlayerRenameService",
    "StoneGiftResult",
    "StoneGiftService",
    "StoneContestResult",
    "StoneTheftResult",
    "StoneContestService",
    "StoneRobberyResult",
    "StoneRobberySettlementService",
    "LotteryWinner",
    "LotteryPoolSnapshot",
    "LotterySettlementResult",
    "LotterySettlementService",
    "XiangyuanCreateResult",
    "XiangyuanClaimResult",
    "XiangyuanSettlementService",
    "DirectBreakthroughResult",
    "ContinuousBreakthroughResult",
    "BreakthroughService",
    "OrdinaryTribulationResult",
    "OrdinaryTribulationService",
    "DestinyTribulationResult",
    "DestinyTribulationService",
    "HeartDevilTribulationResult",
    "HeartDevilTribulationService",
    "PillFusionResult",
    "PillFusionService",
    "TribulationStateMigrationResult",
    "TribulationStateMigrationService",
]
