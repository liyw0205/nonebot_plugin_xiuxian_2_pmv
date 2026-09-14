from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

import json
from datetime import date, datetime

from ...infrastructure.database import DatabaseUnitOfWork


class BossRepository(Protocol):
    def purchase(self, *args: Any, **kwargs: Any) -> Any: ...
    def settle(self, *args: Any, **kwargs: Any) -> Any: ...


class LegacyBossRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path, activity_database: str | Path | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.activity_database = str(activity_database) if activity_database else None

    def _services(self):
        from ...xiuxian.xiuxian_boss.transaction_service import BossPurchaseService, WorldBossBattleSettlementService

        return (
            BossPurchaseService(self.game_database, self.player_database),
            WorldBossBattleSettlementService(self.game_database, self.player_database, self.activity_database),
        )

    def purchase(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[0].purchase(*args, **kwargs)

    def settle(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[1].settle(*args, **kwargs)


class BossPurchaseSqlRepository(LegacyBossRepository):
    @staticmethod
    def _weekly(value, today):
        if isinstance(today, datetime): today=today.date()
        if not isinstance(today, date): today=date.fromisoformat(str(today))
        try:data=json.loads(value) if isinstance(value,str) else dict(value or {})
        except (TypeError,ValueError):data={}
        try:reset=date.fromisoformat(str(data.get("_last_reset","")))
        except (TypeError,ValueError):reset=None
        if reset is None or reset.isocalendar()[:2]!=today.isocalendar()[:2]:return {"_last_reset":today.isoformat()}
        return {str(k):v for k,v in data.items()}

    def purchase(self,operation_id,user_id,item_id,item_name,item_type,quantity,unit_cost,weekly_limit,expected_integral,expected_weekly_purchases,max_goods_num,today=None):
        operation_id,user_id,item_name,item_type=str(operation_id).strip(),str(user_id),str(item_name),str(item_type);item_id,quantity,unit_cost,weekly_limit,expected_integral,max_goods_num=map(int,(item_id,quantity,unit_cost,weekly_limit,expected_integral,max_goods_num));today=today or date.today();weekly=self._weekly(expected_weekly_purchases,today);payload=json.dumps([user_id,item_id,item_name,item_type,quantity,unit_cost,weekly_limit,max_goods_num],ensure_ascii=True,sort_keys=True,separators=(",",":"))
        if not operation_id or quantity<=0 or min(item_id,unit_cost,weekly_limit,expected_integral,max_goods_num)<0:raise ValueError("valid boss purchase required")
        def result(status,integral=expected_integral,purchased=0,inventory=0):
            ok=status in {"applied","duplicate"};return {"status":status,"quantity":quantity if ok else 0,"cost":quantity*unit_cost if ok else 0,"integral":int(integral),"purchased":int(purchased),"inventory":int(inventory)}
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,"player_data");old=uow.query_one("SELECT payload,quantity,cost,integral,purchased,inventory FROM boss_purchase_operations WHERE operation_id=?",(operation_id,))
            if old:
                if str(old["payload"])!=payload:return result("state_changed")
                return {"status":"duplicate","quantity":int(old["quantity"]),"cost":int(old["cost"]),"integral":int(old["integral"]),"purchased":int(old["purchased"]),"inventory":int(old["inventory"])}
            if uow.query_one("SELECT 1 AS ok FROM user_xiuxian WHERE user_id=?",(user_id,)) is None:return self._record(uow,operation_id,payload,result("user_missing"))
            integral=uow.query_one("SELECT COALESCE(integral,0) AS integral FROM player_data.boss_limit WHERE user_id=?",(user_id,));boss=uow.query_one("SELECT COALESCE(weekly_purchases,'{}') AS weekly FROM player_data.boss WHERE user_id=?",(user_id,))
            if integral is None or boss is None:return self._record(uow,operation_id,payload,result("state_changed"))
            current=self._weekly(boss["weekly"],today)
            if int(integral["integral"])!=expected_integral or current!=weekly:return self._record(uow,operation_id,payload,result("state_changed"))
            purchased=int(weekly.get(str(item_id),0) or 0)
            if purchased+quantity>weekly_limit:return self._record(uow,operation_id,payload,result("limit_reached",purchased=purchased))
            cost=quantity*unit_cost
            if expected_integral<cost:return self._record(uow,operation_id,payload,result("integral_insufficient",purchased=purchased))
            item=uow.query_one("SELECT COALESCE(goods_num,0) AS n FROM back WHERE user_id=? AND goods_id=?",(user_id,item_id));inventory=int(item["n"]) if item else 0
            if inventory+quantity>max_goods_num:return self._record(uow,operation_id,payload,result("inventory_full",purchased=purchased,inventory=inventory))
            new_integral,new_purchased,new_inventory=expected_integral-cost,purchased+quantity,inventory+quantity;weekly[str(item_id)]=new_purchased;uow.execute("UPDATE player_data.boss_limit SET integral=? WHERE user_id=? AND COALESCE(integral,0)=?",(new_integral,user_id,expected_integral));uow.execute("UPDATE player_data.boss SET weekly_purchases=? WHERE user_id=?",(json.dumps(weekly,ensure_ascii=True),user_id));stamp=(today.date() if isinstance(today,datetime) else today).isoformat();uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.goods_num,update_time=excluded.update_time",(user_id,item_id,item_name,item_type,quantity,stamp,stamp,quantity));return self._record(uow,operation_id,payload,result("applied",new_integral,new_purchased,new_inventory))

    @staticmethod
    def _record(uow,operation_id,payload,result):
        uow.execute("INSERT INTO boss_purchase_operations(operation_id,payload,quantity,cost,integral,purchased,inventory) VALUES(?,?,?,?,?,?,?)",(operation_id,payload,result["quantity"],result["cost"],result["integral"],result["purchased"],result["inventory"]));return result


__all__ = ["BossPurchaseSqlRepository", "BossRepository", "LegacyBossRepository"]
