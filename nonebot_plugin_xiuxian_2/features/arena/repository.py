from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

import json

from ...infrastructure.database import DatabaseUnitOfWork


class ArenaRepository(Protocol):
    def purchase(self, *args: Any, **kwargs: Any) -> Any: ...
    def purchase_challenges(self, *args: Any, **kwargs: Any) -> Any: ...
    def settle(self, *args: Any, **kwargs: Any) -> Any: ...


class LegacyArenaRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def _services(self):
        from ...xiuxian.xiuxian_arena.transaction_service import (
            ArenaPurchaseService,
            ArenaChallengePurchaseService,
            ArenaChallengeSettlementService,
        )

        return ArenaPurchaseService(self.game_database, self.player_database), ArenaChallengePurchaseService(self.game_database, self.player_database), ArenaChallengeSettlementService(self.game_database, self.player_database)

    def purchase(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[0].purchase(*args, **kwargs)

    def purchase_challenges(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[1].purchase(*args, **kwargs)

    def settle(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[2].settle(*args, **kwargs)


class ArenaChallengePurchaseSqlRepository(LegacyArenaRepository):
    def purchase_challenges(self, operation_id, user_id, amount, unit_cost, daily_limit, expected_stone, expected_bought, expected_extra, expected_last_buy_date, today=None) -> dict[str, Any]:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        amount, unit_cost, daily_limit, expected_stone, expected_bought, expected_extra = map(int, (amount, unit_cost, daily_limit, expected_stone, expected_bought, expected_extra))
        if not operation_id or amount <= 0 or min(unit_cost, daily_limit, expected_stone, expected_bought, expected_extra) < 0:
            raise ValueError("valid arena challenge purchase is required")
        today = today or __import__("datetime").date.today()
        payload = json.dumps([user_id, amount, unit_cost, daily_limit], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            old = uow.query_one("SELECT payload,amount,cost,stone,bought,extra FROM arena_challenge_purchase_operations WHERE operation_id=?", (operation_id,))
            if old:
                if str(old["payload"]) != payload:
                    return self._result("state_changed", 0, 0, expected_stone, expected_bought, expected_extra)
                return self._result("duplicate", int(old["amount"]), int(old["cost"]), int(old["stone"]), int(old["bought"]), int(old["extra"]))
            user = uow.query_one("SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            arena = uow.query_one("SELECT COALESCE(daily_challenge_buys,0) AS bought,COALESCE(daily_extra_challenges,0) AS extra,last_buy_date FROM player_data.arena WHERE user_id=?", (user_id,))
            if user is None or arena is None:
                return self._record(uow, operation_id, payload, self._result("state_changed", 0, 0, expected_stone, expected_bought, expected_extra))
            current_date = str(arena["last_buy_date"] or "")
            today_str = today.isoformat() if hasattr(today, "isoformat") else str(today)
            bought = int(arena["bought"]); extra = int(arena["extra"])
            if current_date != today_str:
                bought, extra = 0, 0
                expected_bought, expected_extra, expected_last_buy_date = 0, 0, today_str
            if (int(user["stone"]), bought, extra, current_date if current_date == today_str else today_str) != (expected_stone, expected_bought, expected_extra, str(expected_last_buy_date)):
                return self._record(uow, operation_id, payload, self._result("state_changed", 0, 0, expected_stone, expected_bought, expected_extra))
            real_amount = min(amount, max(0, daily_limit - expected_bought)); cost = real_amount * unit_cost
            if real_amount == 0:
                return self._record(uow, operation_id, payload, self._result("limit_reached", 0, 0, expected_stone, expected_bought, expected_extra))
            if expected_stone < cost:
                return self._record(uow, operation_id, payload, self._result("stone_insufficient", 0, 0, expected_stone, expected_bought, expected_extra))
            new_stone, new_bought, new_extra = expected_stone - cost, expected_bought + real_amount, expected_extra + real_amount
            uow.execute("UPDATE user_xiuxian SET stone=? WHERE user_id=? AND COALESCE(stone,0)=?", (new_stone, user_id, expected_stone))
            uow.execute("UPDATE player_data.arena SET daily_challenge_buys=?,daily_extra_challenges=?,last_buy_date=? WHERE user_id=?", (new_bought, new_extra, today_str, user_id))
            return self._record(uow, operation_id, payload, self._result("applied", real_amount, cost, new_stone, new_bought, new_extra))

    @staticmethod
    def _result(status, amount, cost, stone, bought, extra):
        return {"status": status, "amount": amount, "cost": cost, "stone": stone, "bought": bought, "extra": extra}

    @staticmethod
    def _record(uow, operation_id, payload, result):
        uow.execute("INSERT INTO arena_challenge_purchase_operations(operation_id,payload,amount,cost,stone,bought,extra) VALUES(?,?,?,?,?,?,?)", (operation_id, payload, result["amount"], result["cost"], result["stone"], result["bought"], result["extra"]))
        return result


__all__ = ["ArenaChallengePurchaseSqlRepository", "ArenaRepository", "LegacyArenaRepository"]
