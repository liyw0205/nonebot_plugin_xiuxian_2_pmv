from __future__ import annotations

from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .lottery import LotterySettlement
from .lottery_snapshot import LotterySnapshot, LotteryWinner


class LotteryRepository:
    """Persistence port implementation for the new lottery use case."""

    columns = (
        "operation_id,user_id,user_name,business_date,deposit_amount,lottery_number,"
        "prize_tier,prize_amount,pool_before,pool_after,participant_count,wallet_stone"
    )

    @staticmethod
    def ensure_schema(uow: DatabaseUnitOfWork) -> None:
        uow.execute("CREATE TABLE IF NOT EXISTS lottery_pool_state(state_id INTEGER PRIMARY KEY,pool_amount INTEGER NOT NULL DEFAULT 0,updated_at TEXT NOT NULL)")
        uow.execute("CREATE TABLE IF NOT EXISTS lottery_settlement_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,user_name TEXT NOT NULL,business_date TEXT NOT NULL,deposit_amount INTEGER NOT NULL,lottery_number INTEGER NOT NULL,prize_tier TEXT NOT NULL,prize_amount INTEGER NOT NULL,pool_before INTEGER NOT NULL,pool_after INTEGER NOT NULL,participant_count INTEGER NOT NULL,wallet_stone INTEGER NOT NULL,created_at TEXT NOT NULL)")
        uow.execute("CREATE TABLE IF NOT EXISTS lottery_participants(business_date TEXT NOT NULL,user_id TEXT NOT NULL,operation_id TEXT NOT NULL UNIQUE,participated_at TEXT NOT NULL,PRIMARY KEY(business_date,user_id))")
        uow.execute("CREATE TABLE IF NOT EXISTS lottery_winner_history(id INTEGER PRIMARY KEY AUTOINCREMENT,operation_id TEXT NOT NULL UNIQUE,user_id TEXT NOT NULL,user_name TEXT NOT NULL,prize_tier TEXT NOT NULL,lottery_number INTEGER NOT NULL,prize_amount INTEGER NOT NULL,won_at TEXT NOT NULL)")
        uow.execute("CREATE TABLE IF NOT EXISTS lottery_legacy_migrations(migration_key TEXT PRIMARY KEY,source_path TEXT NOT NULL,payload TEXT NOT NULL,migrated_at TEXT NOT NULL)")
        uow.execute("CREATE TABLE IF NOT EXISTS economy_log(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',trace_id TEXT,created_at TEXT NOT NULL)")
        uow.execute("INSERT INTO lottery_pool_state(state_id,pool_amount,updated_at) VALUES(1,0,'') ON CONFLICT(state_id) DO NOTHING")

    @staticmethod
    def schema_exists(uow: DatabaseUnitOfWork) -> bool:
        row = uow.query_one("SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='lottery_pool_state'")
        return row is not None

    def operation(self, uow: DatabaseUnitOfWork, operation_id: str) -> LotterySettlement | None:
        if not self.schema_exists(uow):
            return None
        row = uow.query_one(f"SELECT {self.columns} FROM lottery_settlement_operations WHERE operation_id=?", (operation_id,))
        return self._result(row, "duplicate") if row else None

    def participant(self, uow: DatabaseUnitOfWork, business_date: str, user_id: str) -> str | None:
        row = uow.query_one("SELECT operation_id FROM lottery_participants WHERE business_date=? AND user_id=?", (business_date, user_id))
        return str(row["operation_id"]) if row else None

    def pool(self, uow: DatabaseUnitOfWork) -> int:
        row = uow.query_one("SELECT pool_amount FROM lottery_pool_state WHERE state_id=1")
        return int(row["pool_amount"] if row else 0)

    def participant_count(self, uow: DatabaseUnitOfWork, business_date: str) -> int:
        row = uow.query_one("SELECT COUNT(*) AS count FROM lottery_participants WHERE business_date=?", (business_date,))
        return int(row["count"] if row else 0)

    def snapshot(self, uow: DatabaseUnitOfWork, business_date: str) -> LotterySnapshot:
        pool = self.pool(uow)
        participants = self.participant_count(uow, business_date)
        row = uow.query_one("SELECT user_id,user_name,won_at,prize_amount,lottery_number,prize_tier FROM lottery_winner_history ORDER BY id DESC LIMIT 1")
        winner = None if row is None else LotteryWinner(str(row["user_id"]), str(row["user_name"]), str(row["won_at"]), int(row["prize_amount"]), int(row["lottery_number"]), str(row["prize_tier"]))
        return LotterySnapshot(business_date, pool, participants, winner)

    def _result(self, row: Any, status: str) -> LotterySettlement:
        return LotterySettlement(status=status, operation_id=str(row["operation_id"]), user_id=str(row["user_id"]), user_name=str(row["user_name"]), business_date=str(row["business_date"]), lottery_number=int(row["lottery_number"]), prize_tier=str(row["prize_tier"]), prize=int(row["prize_amount"]), deposit=int(row["deposit_amount"]), pool_before=int(row["pool_before"]), pool_after=int(row["pool_after"]), participants=int(row["participant_count"]), wallet_stone=int(row["wallet_stone"]))

    def insert(self, uow: DatabaseUnitOfWork, result: LotterySettlement, occurred_at: str) -> None:
        uow.execute("INSERT INTO lottery_participants(business_date,user_id,operation_id,participated_at) VALUES(?,?,?,?)", (result.business_date, result.user_id, result.operation_id, occurred_at))
        uow.execute("UPDATE lottery_pool_state SET pool_amount=?,updated_at=? WHERE state_id=1", (result.pool_after, occurred_at))
        if result.prize > 0:
            uow.execute("INSERT INTO lottery_winner_history(operation_id,user_id,user_name,prize_tier,lottery_number,prize_amount,won_at) VALUES(?,?,?,?,?,?,?)", (result.operation_id, result.user_id, result.user_name, result.prize_tier, result.lottery_number, result.prize, occurred_at))
            uow.execute("INSERT INTO economy_log(user_id,source,action,stone_delta,item_delta,detail,trace_id,created_at) VALUES(?,?,?,?,?,?,?,?)", (result.user_id, "lottery", "lottery_prize", result.prize, "[]", "{}", result.operation_id, occurred_at))
        uow.execute("INSERT INTO lottery_settlement_operations(operation_id,user_id,user_name,business_date,deposit_amount,lottery_number,prize_tier,prize_amount,pool_before,pool_after,participant_count,wallet_stone,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", (result.operation_id,result.user_id,result.user_name,result.business_date,result.deposit,result.lottery_number,result.prize_tier,result.prize,result.pool_before,result.pool_after,result.participants,result.wallet_stone,occurred_at))


__all__ = ["LotteryRepository"]
