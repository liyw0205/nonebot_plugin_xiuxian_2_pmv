from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

import json

from ...infrastructure.database import DatabaseUnitOfWork


class ArenaRepository(Protocol):
    def purchase(self, *args: Any, **kwargs: Any) -> Any: ...
    def purchase_challenges(self, *args: Any, **kwargs: Any) -> Any: ...
    def settle(self, *args: Any, **kwargs: Any) -> Any: ...
    def use_challenge_ticket(self, *args: Any, **kwargs: Any) -> Any: ...


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

    def use_challenge_ticket(self, *args: Any, **kwargs: Any) -> Any:
        from ...xiuxian.xiuxian_arena.transaction_service import ArenaChallengeTicketService
        return ArenaChallengeTicketService(self.game_database, self.player_database).use(*args, **kwargs)


class ArenaChallengePurchaseSqlRepository(LegacyArenaRepository):
    def use_challenge_ticket(self, operation_id, user_id, item_id, requested_count, expected_item_count, expected_challenges_used, expected_extra_challenges, challenge_cap) -> dict[str, Any]:
        operation_id,user_id=str(operation_id).strip(),str(user_id); item_id,requested_count,expected_item_count,expected_challenges_used,expected_extra_challenges,challenge_cap=map(int,(item_id,requested_count,expected_item_count,expected_challenges_used,expected_extra_challenges,challenge_cap));payload=json.dumps([user_id,item_id,requested_count,challenge_cap],ensure_ascii=True,separators=(",",":"))
        if not operation_id or requested_count<=0 or min(expected_item_count,expected_challenges_used,expected_extra_challenges,challenge_cap)<0: raise ValueError("valid arena ticket operation is required")
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,"player_data")
            old=uow.query_one("SELECT payload,used_tickets,item_remaining,challenges_used,challenges_remaining,challenge_cap FROM arena_challenge_ticket_operations WHERE operation_id=?",(operation_id,))
            if old:
                if str(old["payload"])!=payload:return self._ticket_result("operation_conflict",expected_item_count,expected_challenges_used,challenge_cap)
                return self._ticket_result("duplicate",int(old["item_remaining"]),int(old["challenges_used"]),int(old["challenge_cap"]),int(old["used_tickets"]))
            arena=uow.query_one("SELECT COALESCE(daily_challenges_used,0) AS used,COALESCE(daily_extra_challenges,0) AS extra FROM player_data.arena WHERE user_id=?",(user_id,));item=uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num,COALESCE(bind_num,0) AS bind_num FROM back WHERE user_id=? AND goods_id=?",(user_id,item_id))
            if arena is None or (int(arena["used"]),int(arena["extra"]))!=(expected_challenges_used,expected_extra_challenges):return self._record_ticket(uow,operation_id,payload,self._ticket_result("state_changed",expected_item_count,expected_challenges_used,challenge_cap))
            if item is None or int(item["goods_num"])<=0:return self._record_ticket(uow,operation_id,payload,self._ticket_result("item_missing",expected_item_count,expected_challenges_used,challenge_cap))
            if int(item["goods_num"])!=expected_item_count:return self._record_ticket(uow,operation_id,payload,self._ticket_result("state_changed",expected_item_count,expected_challenges_used,challenge_cap))
            if expected_challenges_used<=0:return self._record_ticket(uow,operation_id,payload,self._ticket_result("no_challenges_used",expected_item_count,expected_challenges_used,challenge_cap))
            used=min(requested_count,expected_item_count,expected_challenges_used);remaining=expected_item_count-used;new_used=expected_challenges_used-used;new_remaining=max(0,challenge_cap-new_used);bound=min(max(0,int(item["bind_num"])-used),remaining)
            uow.execute("UPDATE back SET goods_num=?,bind_num=? WHERE user_id=? AND goods_id=? AND COALESCE(goods_num,0)=?",(remaining,bound,user_id,item_id,expected_item_count));uow.execute("UPDATE player_data.arena SET daily_challenges_used=? WHERE user_id=? AND CAST(COALESCE(daily_challenges_used,0) AS INTEGER)=? AND CAST(COALESCE(daily_extra_challenges,0) AS INTEGER)=?",(new_used,user_id,expected_challenges_used,expected_extra_challenges));return self._record_ticket(uow,operation_id,payload,self._ticket_result("applied",remaining,new_used,challenge_cap,used))

    @staticmethod
    def _ticket_result(status,item_remaining,challenges_used,challenge_cap,used_tickets=0): return {"status":status,"used_tickets":used_tickets,"item_remaining":item_remaining,"challenges_used":challenges_used,"challenges_remaining":max(0,challenge_cap-challenges_used),"challenge_cap":challenge_cap}
    @staticmethod
    def _record_ticket(uow,operation_id,payload,result):
        uow.execute("INSERT INTO arena_challenge_ticket_operations(operation_id,payload,used_tickets,item_remaining,challenges_used,challenges_remaining,challenge_cap) VALUES(?,?,?,?,?,?,?)",(operation_id,payload,result["used_tickets"],result["item_remaining"],result["challenges_used"],result["challenges_remaining"],result["challenge_cap"]));return result
    def purchase(self, operation_id, user_id, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, expected_honor, expected_weekly_purchases, max_goods_num, bind_flag=1, today=None) -> dict[str, Any]:
        operation_id,user_id,item_name,item_type=str(operation_id).strip(),str(user_id),str(item_name),str(item_type);item_id,quantity,unit_cost,weekly_limit,expected_honor,max_goods_num=map(int,(item_id,quantity,unit_cost,weekly_limit,expected_honor,max_goods_num));payload=json.dumps([user_id,item_id,item_name,item_type,quantity,unit_cost,weekly_limit,max_goods_num,int(bind_flag)],ensure_ascii=True,sort_keys=True);today=today or __import__("datetime").date.today();today_key=today.isoformat() if hasattr(today,"isoformat") else str(today)
        if not operation_id or quantity<=0 or min(item_id,unit_cost,weekly_limit,expected_honor,max_goods_num)<0: raise ValueError("valid arena purchase is required")
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,"player_data")
            old=uow.query_one("SELECT payload,quantity,cost,honor_points,purchased,inventory FROM arena_purchase_operations WHERE operation_id=?",(operation_id,))
            if old:
                if str(old["payload"])!=payload:return self._purchase_result("state_changed",quantity,0,expected_honor,0,0)
                return self._purchase_result("duplicate",int(old["quantity"]),int(old["cost"]),int(old["honor_points"]),int(old["purchased"]),int(old["inventory"]))
            user=uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?",(user_id,));arena=uow.query_one("SELECT COALESCE(honor_points,0) AS honor,COALESCE(weekly_purchases,'{}') AS weekly FROM player_data.arena WHERE user_id=?",(user_id,))
            if user is None or arena is None:return self._record_purchase(uow,operation_id,payload,self._purchase_result("user_missing",quantity,0,expected_honor,0,0))
            try:weekly=json.loads(str(arena["weekly"] or "{}"))
            except (TypeError,ValueError):weekly={}
            if not isinstance(weekly,dict) or str(weekly.get("_last_reset", ""))!=today_key:weekly={"_last_reset":today_key}
            if int(arena["honor"])!=expected_honor or weekly.get(str(item_id),0)!=expected_weekly_purchases.get(str(item_id),0):return self._record_purchase(uow,operation_id,payload,self._purchase_result("state_changed",quantity,0,expected_honor,0,0))
            purchased=int(weekly.get(str(item_id),0) or 0)
            if purchased+quantity>weekly_limit:return self._record_purchase(uow,operation_id,payload,self._purchase_result("limit_reached",quantity,0,expected_honor,purchased,0))
            cost=quantity*unit_cost
            if expected_honor<cost:return self._record_purchase(uow,operation_id,payload,self._purchase_result("honor_insufficient",quantity,0,expected_honor,purchased,0))
            item=uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",(user_id,item_id));inventory=int(item["goods_num"]) if item else 0
            if inventory+quantity>max_goods_num:return self._record_purchase(uow,operation_id,payload,self._purchase_result("inventory_full",quantity,0,expected_honor,purchased,inventory))
            honor,purchased,inventory=expected_honor-cost,purchased+quantity,inventory+quantity;weekly[str(item_id)]=purchased
            uow.execute("UPDATE player_data.arena SET honor_points=?,weekly_purchases=? WHERE user_id=? AND COALESCE(honor_points,0)=?",(honor,json.dumps(weekly,ensure_ascii=True),user_id,expected_honor));now=today_key;uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,goods_name=excluded.goods_name,goods_type=excluded.goods_type,update_time=excluded.update_time",(user_id,item_id,item_name,item_type,quantity,now,now,quantity if int(bind_flag) else 0));return self._record_purchase(uow,operation_id,payload,self._purchase_result("applied",quantity,cost,honor,purchased,inventory))

    @staticmethod
    def _purchase_result(status, quantity, cost, honor_points, purchased, inventory):
        return {"status":status,"quantity":quantity,"cost":cost,"honor_points":honor_points,"purchased":purchased,"inventory":inventory}

    @staticmethod
    def _record_purchase(uow, operation_id, payload, result):
        uow.execute("INSERT INTO arena_purchase_operations(operation_id,payload,quantity,cost,honor_points,purchased,inventory) VALUES(?,?,?,?,?,?,?)",(operation_id,payload,result["quantity"],result["cost"],result["honor_points"],result["purchased"],result["inventory"]));return result
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
