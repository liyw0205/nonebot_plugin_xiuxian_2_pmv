from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

import json

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.clock import SystemClock


class DungeonRepository(Protocol):
    def purchase(self, *args: Any, **kwargs: Any) -> Any: ...
    def operation_result(self, *args: Any, **kwargs: Any) -> Any: ...
    def replay(self, *args: Any, **kwargs: Any) -> Any: ...
    def prepare(self, *args: Any, **kwargs: Any) -> Any: ...
    def settle(self, *args: Any, **kwargs: Any) -> Any: ...
    def resolve_rejection(self, *args: Any, **kwargs: Any) -> Any: ...


class LegacyDungeonRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def _purchase_service(self):
        from ...xiuxian.xiuxian_dungeon.transaction_service import DungeonPurchaseService

        return DungeonPurchaseService(self.game_database)

    def _explore_service(self):
        from ...xiuxian.xiuxian_dungeon.transaction_service import DungeonExploreOperationService

        return DungeonExploreOperationService(self.game_database, self.player_database)

    def purchase(self, *args: Any, **kwargs: Any) -> Any:
        return self._purchase_service().purchase(*args, **kwargs)

    def operation_result(self, *args: Any, **kwargs: Any) -> Any:
        return self._purchase_service().operation_result(*args, **kwargs)

    def replay(self, *args: Any, **kwargs: Any) -> Any:
        return self._explore_service().replay(*args, **kwargs)

    def prepare(self, *args: Any, **kwargs: Any) -> Any:
        return self._explore_service().prepare(*args, **kwargs)

    def settle(self, *args: Any, **kwargs: Any) -> Any:
        return self._explore_service().settle(*args, **kwargs)

    def resolve_rejection(self, *args: Any, **kwargs: Any) -> Any:
        return self._explore_service().resolve_rejection(*args, **kwargs)


class DungeonPurchaseSqlRepository(LegacyDungeonRepository):
    REJECTIONS = {"stone_insufficient": "灵石不足，无法兑换。", "inventory_full": "背包中该物品数量已达上限。", "state_changed": "兑换未结算：数据刚被其他操作改动，请重试。", "user_missing": "未找到道友数据，兑换失败。"}

    def __init__(self, game_database: str | Path, player_database: str | Path, *, clock: Any = None) -> None:
        super().__init__(game_database, player_database)
        self.clock = clock or SystemClock()

    @staticmethod
    def _payload(user_id: str, item_id: int, quantity: int, bind_flag: int) -> str:
        return json.dumps([user_id, item_id, quantity, bind_flag], ensure_ascii=True, separators=(",", ":"))

    def operation_result(self, operation_id: str, user_id: str, item_id: int, quantity: int, bind_flag: int = 1) -> dict[str, Any] | None:
        payload = self._payload(str(user_id), int(item_id), int(quantity), int(bind_flag))
        with DatabaseUnitOfWork(self.game_database) as uow:
            row = uow.query_one("SELECT payload,result_status,quantity,cost,stone,inventory,response FROM dungeon_purchase_operations WHERE operation_id=?", (str(operation_id),))
        if row is None:
            return None
        if str(row["payload"]) != payload:
            return self._result("state_changed", int(quantity), 0, 0, 0, "")
        status = "duplicate" if str(row["result_status"]) == "applied" else str(row["result_status"])
        return {"status": status, "quantity": int(row["quantity"]), "cost": int(row["cost"]), "stone": int(row["stone"]), "inventory": int(row["inventory"]), "response": str(row["response"])}

    def purchase(self, operation_id: str, user_id: str, item_id: int, item_name: str, item_type: str, quantity: int, unit_cost: int, expected_stone: int, max_goods: int, bind_flag: int = 1) -> dict[str, Any]:
        operation_id,user_id,item_name,item_type=str(operation_id).strip(),str(user_id),str(item_name),str(item_type);item_id,quantity,unit_cost,expected_stone,max_goods,bind_flag=map(int,(item_id,quantity,unit_cost,expected_stone,max_goods,bind_flag));payload=self._payload(user_id,item_id,quantity,bind_flag);cost=quantity*unit_cost
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            old=uow.query_one("SELECT payload,result_status,quantity,cost,stone,inventory,response FROM dungeon_purchase_operations WHERE operation_id=?",(operation_id,))
            if old:
                if str(old["payload"])!=payload:return self._result("state_changed",quantity,cost,0,0,item_name)
                status="duplicate" if str(old["result_status"])=="applied" else str(old["result_status"]);return {"status":status,"quantity":int(old["quantity"]),"cost":int(old["cost"]),"stone":int(old["stone"]),"inventory":int(old["inventory"]),"response":str(old["response"])}
            user=uow.query_one("SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?",(user_id,))
            if user is None:return self._record(uow,operation_id,payload,self._result("user_missing",quantity,cost,0,0,item_name))
            stone=int(user["stone"])
            if stone!=expected_stone:return self._record(uow,operation_id,payload,self._result("state_changed",quantity,cost,stone,0,item_name))
            if stone<cost:return self._record(uow,operation_id,payload,self._result("stone_insufficient",quantity,cost,stone,0,item_name))
            item=uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num,COALESCE(bind_num,0) AS bind_num FROM back WHERE user_id=? AND goods_id=?",(user_id,item_id));inventory,bound=(int(item["goods_num"]),int(item["bind_num"])) if item else (0,0)
            if inventory<0 or bound<0 or bound>inventory:return self._record(uow,operation_id,payload,self._result("state_changed",quantity,cost,stone,inventory,item_name))
            if inventory+quantity>max_goods:return self._record(uow,operation_id,payload,self._result("inventory_full",quantity,cost,stone,inventory,item_name))
            new_stone,new_inventory=stone-cost,inventory+quantity;now=self.clock.now().isoformat();uow.execute("UPDATE user_xiuxian SET stone=? WHERE user_id=? AND COALESCE(stone,0)=?",(new_stone,user_id,expected_stone));uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time",(user_id,item_id,item_name,item_type,quantity,now,now,quantity if bind_flag else 0));return self._record(uow,operation_id,payload,self._result("applied",quantity,cost,new_stone,new_inventory,item_name))

    def _result(self,status:str,quantity:int,cost:int,stone:int,inventory:int,item_name:str)->dict[str,Any]:
        response=f"成功兑换{item_name}×{quantity}，消耗{cost}灵石。" if status in {"applied","duplicate"} else self.REJECTIONS.get(status,"兑换失败。")
        return {"status":status,"quantity":quantity,"cost":cost,"stone":stone,"inventory":inventory,"response":response}

    @staticmethod
    def _record(uow:DatabaseUnitOfWork,operation_id:str,payload:str,result:dict[str,Any])->dict[str,Any]:
        uow.execute("INSERT INTO dungeon_purchase_operations(operation_id,payload,result_status,quantity,cost,stone,inventory,response) VALUES(?,?,?,?,?,?,?,?)",(operation_id,payload,result["status"],result["quantity"],result["cost"],result["stone"],result["inventory"],result["response"]));return result


__all__ = ["DungeonPurchaseSqlRepository", "DungeonRepository", "LegacyDungeonRepository"]
