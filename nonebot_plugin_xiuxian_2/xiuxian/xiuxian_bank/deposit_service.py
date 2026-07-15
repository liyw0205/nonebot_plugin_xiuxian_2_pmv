from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class BankDepositResult:
    status: str
    deposited: int = 0
    interest: int = 0
    wallet_stone: int = 0
    saved_stone: int = 0
    saved_at: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class BankDepositService:
    """Settle principal, interest and bank account state in one transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id, amount) -> str:
        # Request identity only — balances/interest/settled_at are concurrency checks or outcomes.
        return json.dumps([str(user_id), int(amount)], ensure_ascii=True, separators=(",", ":"))

    def get_result(self, operation_id: str) -> BankDepositResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS bank_deposit_operations ("
                "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, deposited INTEGER NOT NULL, "
                "interest INTEGER NOT NULL, wallet_stone INTEGER NOT NULL, saved_stone INTEGER NOT NULL, "
                "saved_at TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload, deposited, interest, wallet_stone, saved_stone, saved_at "
                "FROM bank_deposit_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return BankDepositResult("duplicate", *map(int, previous[1:5]), str(previous[5]))

    def deposit(
        self,
        operation_id,
        user_id,
        amount,
        expected_saved_stone,
        expected_saved_at,
        bank_level,
        interest,
        settled_at,
        save_limit,
    ) -> BankDepositResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        amount = int(amount)
        expected_saved_stone = int(expected_saved_stone)
        expected_saved_at = str(expected_saved_at)
        bank_level = str(bank_level)
        interest = int(interest)
        settled_at = str(settled_at)
        save_limit = int(save_limit)
        if not operation_id or amount <= 0 or interest < 0 or save_limit < 0 or not settled_at:
            raise ValueError("valid operation, amount, interest, limit and settlement time are required")

        payload = self._payload(user_id, amount)

        def result(status, deposited=0, wallet_stone=0, saved_stone=expected_saved_stone, interest_out=0, saved_at=settled_at):
            return BankDepositResult(
                status,
                deposited,
                interest_out if deposited else 0,
                wallet_stone,
                saved_stone,
                saved_at,
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS bank_deposit_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, deposited INTEGER NOT NULL, "
                    "interest INTEGER NOT NULL, wallet_stone INTEGER NOT NULL, saved_stone INTEGER NOT NULL, "
                    "saved_at TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, deposited, interest, wallet_stone, saved_stone, saved_at "
                    "FROM bank_deposit_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return BankDepositResult("duplicate", *map(int, previous[1:5]), str(previous[5]))

                user = conn.execute(
                    "SELECT COALESCE(stone, 0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                if int(user[0]) < amount:
                    conn.rollback()
                    return result("stone_insufficient", wallet_stone=int(user[0]))
                if expected_saved_stone + amount > save_limit:
                    conn.rollback()
                    return result("limit_exceeded", wallet_stone=int(user[0]))

                table = conn.execute(
                    "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s", ("bankinfo",)
                ).fetchone()
                if table is None:
                    conn.execute(
                        "CREATE TABLE player_data.bankinfo (user_id TEXT PRIMARY KEY, savestone INTEGER, savetime TEXT, banklevel TEXT)"
                    )
                columns = {
                    str(column[1]) for column in conn.execute("PRAGMA player_data.table_info(bankinfo)").fetchall()
                }
                for field, data_type in (("savestone", "INTEGER"), ("savetime", "TEXT"), ("banklevel", "TEXT")):
                    if field not in columns:
                        conn.execute(
                            f"ALTER TABLE player_data.bankinfo ADD COLUMN {db_backend.quote_ident(field)} {data_type}"
                        )
                account = conn.execute(
                    "SELECT COALESCE(savestone, 0), savetime, banklevel FROM player_data.bankinfo WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if account is None:
                    conn.execute(
                        "INSERT INTO player_data.bankinfo (user_id, savestone, savetime, banklevel) VALUES (%s, %s, %s, %s)",
                        (user_id, expected_saved_stone, expected_saved_at, bank_level),
                    )
                    account = (expected_saved_stone, expected_saved_at, bank_level)
                if (int(account[0]), str(account[1]), str(account[2])) != (
                    expected_saved_stone, expected_saved_at, bank_level
                ):
                    conn.rollback()
                    return result("state_changed", wallet_stone=int(user[0]))

                wallet_stone = int(user[0]) - amount + interest
                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s+%s WHERE user_id=%s AND stone>=%s",
                    (amount, interest, user_id, amount),
                )
                saved_stone = expected_saved_stone + amount
                updated = conn.execute(
                    "UPDATE player_data.bankinfo SET savestone=%s, savetime=%s WHERE user_id=%s "
                    "AND COALESCE(savestone, 0)=%s AND savetime=%s AND banklevel=%s",
                    (saved_stone, settled_at, user_id, expected_saved_stone, expected_saved_at, bank_level),
                )
                if charged.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed", wallet_stone=int(user[0]))
                conn.execute(
                    "INSERT INTO bank_deposit_operations VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (operation_id, payload, amount, interest, wallet_stone, saved_stone, settled_at),
                )
                conn.commit()
                return BankDepositResult("applied", amount, interest, wallet_stone, saved_stone, settled_at)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["BankDepositResult", "BankDepositService"]
