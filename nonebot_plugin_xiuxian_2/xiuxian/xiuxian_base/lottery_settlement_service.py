from __future__ import annotations

import json
import random
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock
from typing import Callable

from ..xiuxian_utils import db_backend


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
                        "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
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


__all__ = [
    "LotteryPoolSnapshot",
    "LotterySettlementResult",
    "LotterySettlementService",
    "LotteryWinner",
]
