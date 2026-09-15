from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

import json
from datetime import date, datetime

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.clock import SystemClock


class TowerRepository(Protocol):
    def purchase(self, operation_id: str, user_id: str, item_id: int, item_name: str, item_type: str, quantity: int, unit_cost: int, weekly_limit: int, expected_score: int, expected_weekly_purchases: Mapping[str, Any], max_goods_num: int, bind_flag: int = 1, today: Any = None) -> Any: ...
    def settle(self, operation_id: str, user_id: str, expected_tower: Mapping[str, Any], floor: int, score: int, stone: int, exp: int, items: Sequence[Mapping[str, Any]], max_goods_num: int, *, expected_player: Mapping[str, Any] | None = None, final_hp: int | None = None, final_mp: int | None = None, stamina_cost: int = 0, challenge_succeeded: bool = True) -> Any: ...
    def settlement_result(self, operation_id: str) -> Any: ...


class LegacyTowerRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def _services(self):
        from ...xiuxian.xiuxian_tower.transaction_service import TowerPurchaseService, TowerSettlementService

        return TowerPurchaseService(self.game_database, self.player_database), TowerSettlementService(self.game_database, self.player_database)

    def purchase(self, *args, **kwargs):
        return self._services()[0].purchase(*args, **kwargs)

    def settle(self, *args, **kwargs):
        return self._services()[1].settle(*args, **kwargs)

    def settlement_result(self, operation_id: str) -> Any:
        return self._services()[1].get_result(operation_id)


class TowerPurchaseSqlRepository(LegacyTowerRepository):
    def __init__(self, game_database: str | Path, player_database: str | Path, *, clock=None) -> None:
        super().__init__(game_database, player_database)
        self.clock = clock or SystemClock()

    def settlement_result(self, operation_id: str) -> dict[str, Any] | None:
        with DatabaseUnitOfWork(self.game_database) as uow:
            row = uow.query_one("SELECT payload,result_json FROM tower_settlement_operations WHERE operation_id=?", (str(operation_id),))
        if row is None:
            return None
        try:
            result = json.loads(str(row["result_json"] or "{}"))
        except (TypeError, ValueError):
            return {"status": "state_changed"}
        if not isinstance(result, dict):
            return {"status": "state_changed"}
        result["status"] = "duplicate"
        return result

    def settle(self, operation_id, user_id, expected_tower, floor, score, stone, exp, items, max_goods_num, *, expected_player=None, final_hp=None, final_mp=None, stamina_cost=0, challenge_succeeded=True):
        operation_id,user_id=str(operation_id).strip(),str(user_id);floor,score,stone,exp,max_goods_num,stamina_cost=map(int,(floor,score,stone,exp,max_goods_num,stamina_cost));expected={k:int(dict(expected_tower)[k]) for k in ("current_floor","max_floor","score")};player=None if expected_player is None else {k:int(dict(expected_player)[k]) for k in ("hp","mp","user_stamina")};rewards=tuple((int(i["id"]),str(i["name"]),str(i["type"]),int(i["amount"])) for i in items if int(i.get("amount",0))>0);payload=json.dumps([user_id,floor,stamina_cost],ensure_ascii=True,separators=(",",":"));result_json=json.dumps({"score":score,"stone":stone,"exp":exp,"floor":floor,"stamina_cost":stamina_cost,"challenge_succeeded":bool(challenge_succeeded),"rewards":[list(r) for r in rewards]},ensure_ascii=True,separators=(",",":"))
        if not operation_id or floor<=0 or min(score,stone,exp,max_goods_num,*expected.values())<0:raise ValueError("valid tower settlement required")
        def result(status):return {"status":status,"score":score if status in {"applied","duplicate"} else 0,"stone":stone if status in {"applied","duplicate"} else 0,"exp":exp if status in {"applied","duplicate"} else 0,"floor":floor if status in {"applied","duplicate"} else 0,"challenge_succeeded":bool(challenge_succeeded) if status in {"applied","duplicate"} else True,"rewards":rewards if status in {"applied","duplicate"} else (),"stamina_cost":stamina_cost if status in {"applied","duplicate"} else 0}
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,"player_data");old=uow.query_one("SELECT payload,result_json FROM tower_settlement_operations WHERE operation_id=?",(operation_id,))
            if old:
                if str(old["payload"])!=payload:return result("state_changed")
                saved=json.loads(str(old["result_json"]));saved["status"]="duplicate";return saved
            user=uow.query_one("SELECT hp,mp,user_stamina FROM user_xiuxian WHERE user_id=?",(user_id,));tower=uow.query_one("SELECT current_floor,max_floor,score FROM player_data.tower WHERE user_id=?",(user_id,))
            if user is None:return self._record_settlement(uow,operation_id,payload,result("user_missing"))
            if player is not None and tuple(map(int,user.values()))!=tuple(player.values()):return self._record_settlement(uow,operation_id,payload,result("state_changed"))
            if player is not None and player["user_stamina"]<stamina_cost:return self._record_settlement(uow,operation_id,payload,result("stamina_insufficient"))
            if tower is None or tuple(map(int,tower.values()))!=tuple(expected.values()):return self._record_settlement(uow,operation_id,payload,result("state_changed"))
            for item_id,_,_,amount in rewards:
                row=uow.query_one("SELECT COALESCE(goods_num,0) AS n FROM back WHERE user_id=? AND goods_id=?",(user_id,item_id))
                if (int(row["n"]) if row else 0)+amount>max_goods_num:return self._record_settlement(uow,operation_id,payload,result("inventory_full"))
            if challenge_succeeded:uow.execute("UPDATE player_data.tower SET current_floor=?,max_floor=?,score=? WHERE user_id=?",(floor,max(expected["max_floor"],floor),expected["score"]+score,user_id))
            if player is None:uow.execute("UPDATE user_xiuxian SET stone=COALESCE(stone,0)+?,exp=COALESCE(exp,0)+? WHERE user_id=?",(stone,exp,user_id))
            else:uow.execute("UPDATE user_xiuxian SET stone=COALESCE(stone,0)+?,exp=COALESCE(exp,0)+?,hp=?,mp=?,user_stamina=user_stamina-? WHERE user_id=?",(stone,exp,max(1,int(final_hp)),max(1,int(final_mp)),stamina_cost,user_id))
            stamp=self._date(self.clock.now()).isoformat()
            for item_id,name,item_type,amount in rewards:uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time",(user_id,item_id,name,item_type,amount,stamp,stamp,amount))
            return self._record_settlement(uow,operation_id,payload,result("applied"))

    @staticmethod
    def _record_settlement(uow,operation_id,payload,result):
        uow.execute("INSERT INTO tower_settlement_operations(operation_id,payload,result_json) VALUES(?,?,?)",(operation_id,payload,json.dumps(result,ensure_ascii=True,default=list,separators=(",",":"))));return result
    @staticmethod
    def _date(value: Any) -> date:
        if isinstance(value, datetime): return value.date()
        if isinstance(value, date): return value
        return date.fromisoformat(str(value))

    @classmethod
    def _weekly(cls, value: Any, today: Any) -> dict[str, int | str]:
        today = cls._date(today)
        try: data = json.loads(value) if isinstance(value, str) else dict(value or {})
        except (TypeError, ValueError): data = {}
        try: reset = date.fromisoformat(str(data.get("_last_reset", "")))
        except (TypeError, ValueError): reset = None
        if reset is None or reset.isocalendar()[:2] != today.isocalendar()[:2]: return {"_last_reset": today.isoformat()}
        result: dict[str, int | str] = {"_last_reset": reset.isoformat()}
        for key, amount in data.items():
            if str(key) == "_last_reset": continue
            try: amount = int(amount)
            except (TypeError, ValueError): continue
            if amount >= 0: result[str(key)] = amount
        return result

    def purchase(self, operation_id, user_id, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, expected_score, expected_weekly_purchases, max_goods_num, bind_flag=1, today=None):
        operation_id,user_id,item_name,item_type=str(operation_id).strip(),str(user_id),str(item_name),str(item_type);item_id,quantity,unit_cost,weekly_limit,expected_score,max_goods_num=map(int,(item_id,quantity,unit_cost,weekly_limit,expected_score,max_goods_num));bind_flag=1 if int(bind_flag)==1 else 0
        if today is None: today=dict(expected_weekly_purchases or {}).get("_last_reset",self.clock.now().date().isoformat())
        weekly=self._weekly(expected_weekly_purchases,today);payload=json.dumps([user_id,item_id,quantity,unit_cost,weekly_limit,max_goods_num,bind_flag],ensure_ascii=True,separators=(",",":"))
        if not operation_id or quantity<=0 or min(item_id,unit_cost,weekly_limit,expected_score,max_goods_num)<0:raise ValueError("valid tower purchase required")
        def result(status,score=expected_score,purchased=0,inventory=0):
            ok=status in {"applied","duplicate"};return {"status":status,"quantity":quantity if ok else 0,"cost":quantity*unit_cost if ok else 0,"score":int(score),"purchased":int(purchased),"inventory":int(inventory)}
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,"player_data")
            old=uow.query_one("SELECT payload,quantity,cost,score,purchased,inventory FROM tower_purchase_operations WHERE operation_id=?",(operation_id,))
            if old:
                if str(old["payload"])!=payload:return result("state_changed")
                return {"status":"duplicate","quantity":int(old["quantity"]),"cost":int(old["cost"]),"score":int(old["score"]),"purchased":int(old["purchased"]),"inventory":int(old["inventory"])}
            if uow.query_one("SELECT 1 AS ok FROM user_xiuxian WHERE user_id=?",(user_id,)) is None:return self._record(uow,operation_id,payload,result("user_missing"))
            tower=uow.query_one("SELECT COALESCE(score,0) AS score,COALESCE(weekly_purchases,'{}') AS weekly FROM player_data.tower WHERE user_id=?",(user_id,))
            if tower is None:return self._record(uow,operation_id,payload,result("state_changed"))
            current_weekly=self._weekly(tower["weekly"],today)
            if int(tower["score"])!=expected_score or current_weekly!=weekly:return self._record(uow,operation_id,payload,result("state_changed"))
            purchased=int(weekly.get(str(item_id),0) or 0)
            if purchased+quantity>weekly_limit:return self._record(uow,operation_id,payload,result("limit_reached",purchased=purchased))
            cost=quantity*unit_cost
            if expected_score<cost:return self._record(uow,operation_id,payload,result("score_insufficient",purchased=purchased))
            item=uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",(user_id,item_id));inventory=int(item["goods_num"]) if item else 0
            if inventory+quantity>max_goods_num:return self._record(uow,operation_id,payload,result("inventory_full",purchased=purchased,inventory=inventory))
            new_score,new_purchased,new_inventory=expected_score-cost,purchased+quantity,inventory+quantity;weekly[str(item_id)]=new_purchased
            updated=uow.execute("UPDATE player_data.tower SET score=?,weekly_purchases=? WHERE user_id=? AND COALESCE(score,0)=?",(new_score,json.dumps(weekly,ensure_ascii=True),user_id,expected_score))
            if updated.rowcount!=1:return self._record(uow,operation_id,payload,result("state_changed"))
            stamp=self._date(today).isoformat();uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time",(user_id,item_id,item_name,item_type,quantity,stamp,stamp,quantity if bind_flag else 0));return self._record(uow,operation_id,payload,result("applied",new_score,new_purchased,new_inventory))

    @staticmethod
    def _record(uow, operation_id, payload, result):
        uow.execute("INSERT INTO tower_purchase_operations(operation_id,payload,quantity,cost,score,purchased,inventory) VALUES(?,?,?,?,?,?,?)",(operation_id,payload,result["quantity"],result["cost"],result["score"],result["purchased"],result["inventory"]));return result


__all__ = ["LegacyTowerRepository", "TowerPurchaseSqlRepository", "TowerRepository"]
