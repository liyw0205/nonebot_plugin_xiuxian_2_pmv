from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class DufangPayoutResult:
    status: str
    wallet_stone: int = 0
    gain: int = 0
    loss: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class DufangPayoutSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database, self.player_database = str(game_database), str(player_database)

    def get_result(self, operation_id: str) -> DufangPayoutResult | None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS dufang_payout_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,wallet_stone INTEGER NOT NULL,gain INTEGER NOT NULL,loss INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            row = uow.query_one("SELECT wallet_stone,gain,loss FROM dufang_payout_operations WHERE operation_id=?", (str(operation_id).strip(),))
            return None if row is None else DufangPayoutResult("duplicate", int(row["wallet_stone"]), int(row["gain"]), int(row["loss"]))

    def settle(self, operation_id: str, bet_id: str, user_id: str, outcome: str, gain: int, requested_loss: int, settled_at: str) -> DufangPayoutResult:
        operation_id, bet_id, user_id, outcome = str(operation_id).strip(), str(bet_id).strip(), str(user_id), str(outcome)
        gain, requested_loss = int(gain), int(requested_loss)
        if not operation_id or not bet_id or outcome not in {"win", "loss"} or gain < 0 or requested_loss < 0:
            raise ValueError("valid payout request is required")
        payload = json.dumps([bet_id, user_id], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.execute("ATTACH DATABASE ? AS player_data", (self.player_database,))
            uow.execute("CREATE TABLE IF NOT EXISTS dufang_payout_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,wallet_stone INTEGER NOT NULL,gain INTEGER NOT NULL,loss INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            previous = uow.query_one("SELECT payload,wallet_stone,gain,loss FROM dufang_payout_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return DufangPayoutResult("state_changed")
                return DufangPayoutResult("duplicate", int(previous["wallet_stone"]), int(previous["gain"]), int(previous["loss"]))
            bet = uow.query_one("SELECT user_id,status FROM dufang_bets WHERE bet_id=?", (bet_id,))
            if bet is None or str(bet["user_id"]) != user_id or str(bet["status"]) != "pending":
                return DufangPayoutResult("state_changed")
            user = uow.query_one("SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None:
                return DufangPayoutResult("user_missing")
            actual_gain = gain if outcome == "win" else 0
            actual_loss = min(requested_loss, int(user["stone"])) if outcome == "loss" else 0
            wallet = int(user["stone"]) + actual_gain - actual_loss
            uow.execute("UPDATE user_xiuxian SET stone=? WHERE user_id=?", (wallet, user_id))
            field, amount = ("profit", actual_gain) if outcome == "win" else ("loss", actual_loss)
            uow.execute(f"UPDATE player_data.unseal_data SET {field}=COALESCE({field},0)+?,last_update=? WHERE user_id=?", (amount, str(settled_at), user_id))
            if uow.execute("UPDATE dufang_bets SET status=?,settled_at=? WHERE bet_id=? AND status='pending'", (outcome, str(settled_at), bet_id)).rowcount != 1:
                return DufangPayoutResult("state_changed")
            uow.execute("INSERT INTO dufang_payout_operations VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)", (operation_id, payload, wallet, actual_gain, actual_loss))
            return DufangPayoutResult("applied", wallet, actual_gain, actual_loss)


__all__ = ["DufangPayoutSqlRepository", "DufangPayoutResult"]
