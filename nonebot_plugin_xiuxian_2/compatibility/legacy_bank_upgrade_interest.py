from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian.xiuxian_utils import db_backend


@dataclass(frozen=True)
class BankUpgradeResult:
    status: str
    cost: int = 0
    wallet_stone: int = 0
    bank_level: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class BankUpgradeService:
    """Compatibility-only attached transaction for pre-migration bank rows."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id, next_level, cost) -> str:
        return json.dumps([str(user_id), str(next_level), int(cost)], ensure_ascii=True, separators=(",", ":"))

    def get_result(self, operation_id: str) -> BankUpgradeResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS bank_upgrade_operations ("
                "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, cost INTEGER NOT NULL, "
                "wallet_stone INTEGER NOT NULL, bank_level TEXT NOT NULL, "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload, cost, wallet_stone, bank_level FROM bank_upgrade_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return BankUpgradeResult("duplicate", int(previous[1]), int(previous[2]), str(previous[3]))

    def upgrade(self, operation_id, user_id, expected_level, next_level, cost) -> BankUpgradeResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_level = str(expected_level)
        next_level = str(next_level)
        cost = int(cost)
        if not operation_id or cost < 0 or expected_level == next_level:
            raise ValueError("valid operation, non-negative cost and distinct levels are required")
        payload = self._payload(user_id, next_level, cost)

        def result(status, wallet_stone=0, bank_level=expected_level):
            return BankUpgradeResult(status, cost if status in {"applied", "duplicate"} else 0, wallet_stone, bank_level)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS bank_upgrade_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, cost INTEGER NOT NULL, "
                    "wallet_stone INTEGER NOT NULL, bank_level TEXT NOT NULL, "
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, cost, wallet_stone, bank_level FROM bank_upgrade_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return BankUpgradeResult("duplicate", int(previous[1]), int(previous[2]), str(previous[3]))

                user = conn.execute(
                    "SELECT COALESCE(stone, 0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                if int(user[0]) < cost:
                    conn.rollback()
                    return result("stone_insufficient", wallet_stone=int(user[0]))
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
                    "SELECT banklevel FROM player_data.bankinfo WHERE user_id=%s", (user_id,)
                ).fetchone()
                if account is None:
                    conn.execute(
                        "INSERT INTO player_data.bankinfo (user_id, savestone, savetime, banklevel) VALUES (%s, %s, %s, %s)",
                        (user_id, 0, "", expected_level),
                    )
                    account = (expected_level,)
                if str(account[0]) != expected_level:
                    conn.rollback()
                    return result("state_changed", wallet_stone=int(user[0]))

                wallet_stone = int(user[0]) - cost
                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(%s AS REAL) WHERE user_id=%s AND stone>=%s",
                    (cost, user_id, cost),
                )
                updated = conn.execute(
                    "UPDATE player_data.bankinfo SET banklevel=%s WHERE user_id=%s AND banklevel=%s",
                    (next_level, user_id, expected_level),
                )
                if charged.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed", wallet_stone=int(user[0]))
                conn.execute(
                    "INSERT INTO bank_upgrade_operations VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (operation_id, payload, cost, wallet_stone, next_level),
                )
                conn.commit()
                return BankUpgradeResult("applied", cost, wallet_stone, next_level)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


@dataclass(frozen=True)
class BankInterestResult:
    status: str
    interest: int = 0
    wallet_stone: int = 0
    saved_at: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class BankInterestService:
    """Compatibility-only attached transaction for pre-migration bank rows."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id) -> str:
        return json.dumps([str(user_id)], ensure_ascii=True, separators=(",", ":"))

    def get_result(self, operation_id: str) -> BankInterestResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS bank_interest_operations ("
                "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, interest INTEGER NOT NULL, "
                "wallet_stone INTEGER NOT NULL, saved_at TEXT NOT NULL, "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload, interest, wallet_stone, saved_at FROM bank_interest_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return BankInterestResult("duplicate", int(previous[1]), int(previous[2]), str(previous[3]))

    def settle(
        self,
        operation_id,
        user_id,
        expected_saved_stone,
        expected_saved_at,
        bank_level,
        interest,
        settled_at,
    ) -> BankInterestResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_saved_stone = int(expected_saved_stone)
        expected_saved_at = str(expected_saved_at)
        bank_level = str(bank_level)
        interest = int(interest)
        settled_at = str(settled_at)
        if not operation_id or expected_saved_stone < 0 or interest < 0 or not settled_at:
            raise ValueError("valid operation, account state, interest and settlement time are required")
        payload = self._payload(user_id)

        def result(status, wallet_stone=0):
            return BankInterestResult(status, interest if status in {"applied", "duplicate"} else 0, wallet_stone, settled_at)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS bank_interest_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, interest INTEGER NOT NULL, "
                    "wallet_stone INTEGER NOT NULL, saved_at TEXT NOT NULL, "
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, interest, wallet_stone, saved_at FROM bank_interest_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return BankInterestResult("duplicate", int(previous[1]), int(previous[2]), str(previous[3]))

                user = conn.execute(
                    "SELECT COALESCE(stone, 0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
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
                if (int(account[0]), str(account[1] or ""), str(account[2])) != (
                    expected_saved_stone, expected_saved_at, bank_level
                ):
                    conn.rollback()
                    return result("state_changed", wallet_stone=int(user[0]))

                wallet_stone = int(user[0]) + interest
                credited = conn.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s", (interest, user_id)
                )
                updated = conn.execute(
                    "UPDATE player_data.bankinfo SET savetime=%s WHERE user_id=%s "
                    "AND COALESCE(savestone, 0)=%s AND COALESCE(savetime, '')=%s AND banklevel=%s",
                    (settled_at, user_id, expected_saved_stone, expected_saved_at, bank_level),
                )
                if credited.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed", wallet_stone=int(user[0]))
                conn.execute(
                    "INSERT INTO bank_interest_operations VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (operation_id, payload, interest, wallet_stone, settled_at),
                )
                conn.commit()
                return BankInterestResult("applied", interest, wallet_stone, settled_at)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["BankUpgradeResult", "BankUpgradeService", "BankInterestResult", "BankInterestService"]
