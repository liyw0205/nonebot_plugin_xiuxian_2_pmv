from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class PuppetPurchaseResult:
    status: str
    user_id: str
    action: str = "purchase"
    previous_level: int = 0
    current_level: int = 0
    stone_cost: int = 0

    @property
    def applied(self) -> bool:
        return self.status in {"purchased", "upgraded"}


class PuppetPurchaseSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.lock = lock or RLock()

    def _ensure_operations(self, uow: DatabaseUnitOfWork) -> None:
        uow.execute(
            "CREATE TABLE IF NOT EXISTS puppet_operations("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,action TEXT NOT NULL,"
            "previous_level INTEGER NOT NULL,current_level INTEGER NOT NULL,"
            "stone_cost INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def _duplicate(self, previous, user_id: str) -> PuppetPurchaseResult:
        return PuppetPurchaseResult(
            "duplicate", user_id, str(previous["action"]),
            int(previous["previous_level"]), int(previous["current_level"]),
            int(previous["stone_cost"]),
        )

    def purchase(self, operation_id: str, user_id: str, stone_cost: int) -> PuppetPurchaseResult:
        operation_id, user_id, stone_cost = str(operation_id).strip(), str(user_id), int(stone_cost)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with self.lock, DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.execute("ATTACH DATABASE ? AS player_data", (self.player_database,))
            self._ensure_operations(uow)
            previous = uow.query_one("SELECT action,previous_level,current_level,stone_cost FROM puppet_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                return self._duplicate(previous, user_id)
            user = uow.query_one("SELECT stone,blessed_spot_flag FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None:
                return PuppetPurchaseResult("user_missing", user_id)
            if int(user["blessed_spot_flag"] or 0) == 0:
                return PuppetPurchaseResult("blessed_spot_missing", user_id)
            player = uow.query_one('SELECT "灵田傀儡" FROM player_data.mix_elixir_info WHERE user_id=?', (user_id,))
            if player is None:
                return PuppetPurchaseResult("player_info_missing", user_id)
            previous_level = int(player["灵田傀儡"] or 0)
            if previous_level > 0:
                return PuppetPurchaseResult("already_owned", user_id, previous_level=previous_level, current_level=previous_level)
            if int(user["stone"] or 0) < stone_cost:
                return PuppetPurchaseResult("stone_insufficient", user_id, stone_cost=stone_cost)
            if uow.execute("UPDATE user_xiuxian SET stone=stone-? WHERE user_id=? AND stone>=?", (stone_cost, user_id, stone_cost)).rowcount != 1:
                return PuppetPurchaseResult("stone_changed", user_id, stone_cost=stone_cost)
            if uow.execute('UPDATE player_data.mix_elixir_info SET "灵田傀儡"=1 WHERE user_id=? AND CAST(COALESCE("灵田傀儡",0) AS INTEGER)=0', (user_id,)).rowcount != 1:
                return PuppetPurchaseResult("puppet_level_changed", user_id, stone_cost=stone_cost)
            uow.execute("INSERT INTO puppet_operations(operation_id,user_id,action,previous_level,current_level,stone_cost) VALUES(?,?, 'purchase',0,1,?)", (operation_id, user_id, stone_cost))
            return PuppetPurchaseResult("purchased", user_id, previous_level=0, current_level=1, stone_cost=stone_cost)

    def upgrade(self, operation_id: str, user_id: str, upgrade_costs: dict[int, int], *, max_level: int) -> PuppetPurchaseResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        with self.lock, DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.execute("ATTACH DATABASE ? AS player_data", (self.player_database,))
            self._ensure_operations(uow)
            previous = uow.query_one("SELECT action,previous_level,current_level,stone_cost FROM puppet_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                return self._duplicate(previous, user_id)
            user = uow.query_one("SELECT stone,blessed_spot_flag FROM user_xiuxian WHERE user_id=?", (user_id,))
            player = uow.query_one('SELECT "灵田傀儡" FROM player_data.mix_elixir_info WHERE user_id=?', (user_id,))
            if user is None or player is None:
                return PuppetPurchaseResult("user_missing", user_id, action="upgrade")
            if int(user["blessed_spot_flag"] or 0) == 0:
                return PuppetPurchaseResult("blessed_spot_missing", user_id, action="upgrade")
            previous_level = int(player["灵田傀儡"] or 0)
            if previous_level <= 0:
                return PuppetPurchaseResult("puppet_missing", user_id, action="upgrade")
            if previous_level >= int(max_level):
                return PuppetPurchaseResult("max_level", user_id, action="upgrade", previous_level=previous_level, current_level=previous_level)
            cost = int(upgrade_costs.get(previous_level, -1))
            if cost < 0:
                return PuppetPurchaseResult("invalid_puppet_level", user_id, action="upgrade", previous_level=previous_level)
            if int(user["stone"] or 0) < cost:
                return PuppetPurchaseResult("stone_insufficient", user_id, action="upgrade", previous_level=previous_level, stone_cost=cost)
            if uow.execute("UPDATE user_xiuxian SET stone=stone-? WHERE user_id=? AND stone>=?", (cost, user_id, cost)).rowcount != 1:
                return PuppetPurchaseResult("stone_changed", user_id, action="upgrade", previous_level=previous_level, stone_cost=cost)
            current_level = previous_level + 1
            if uow.execute('UPDATE player_data.mix_elixir_info SET "灵田傀儡"=? WHERE user_id=? AND CAST(COALESCE("灵田傀儡",0) AS INTEGER)=?', (current_level, user_id, previous_level)).rowcount != 1:
                return PuppetPurchaseResult("puppet_level_changed", user_id, action="upgrade", previous_level=previous_level, stone_cost=cost)
            uow.execute("INSERT INTO puppet_operations(operation_id,user_id,action,previous_level,current_level,stone_cost) VALUES(?,?, 'upgrade',?,?,?)", (operation_id, user_id, previous_level, current_level, cost))
            return PuppetPurchaseResult("upgraded", user_id, action="upgrade", previous_level=previous_level, current_level=current_level, stone_cost=cost)


__all__ = ["PuppetPurchaseResult", "PuppetPurchaseSqlRepository"]
