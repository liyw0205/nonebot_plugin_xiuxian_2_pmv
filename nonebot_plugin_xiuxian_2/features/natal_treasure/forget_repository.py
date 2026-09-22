from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class NatalForgetSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database=str(game_database); self.player_database=str(player_database)
    @staticmethod
    def _result(status: str, slot: int=0, effect_type: int=0, effect_level: int=0, scripture_change: int=0) -> dict[str,Any]:
        return {"status":status,"slot":int(slot),"effect_type":int(effect_type),"effect_level":int(effect_level),"scripture_change":int(scripture_change)}
    def forget(self, operation_id: str, user_id: str, effect_type: int, scripture_id: int, scripture_name: str, scripture_type: str, scripture_cost: int, max_slots: int, max_goods_num: int) -> dict[str,Any]:
        operation_id,user_id=str(operation_id).strip(),str(user_id); effect_type,scripture_id,scripture_cost,max_slots,max_goods_num=map(int,(effect_type,scripture_id,scripture_cost,max_slots,max_goods_num))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database,"player_data")
            uow.execute("CREATE TABLE IF NOT EXISTS natal_forget_operations(operation_id TEXT PRIMARY KEY,user_id TEXT,effect_type INTEGER,scripture_id INTEGER,scripture_cost INTEGER,max_slots INTEGER,slot INTEGER,effect_level INTEGER,scripture_change INTEGER)")
            old=uow.query_one("SELECT user_id,slot,effect_type,effect_level,scripture_change FROM natal_forget_operations WHERE operation_id=?",(operation_id,))
            if old is not None:
                return self._result("duplicate",old["slot"],old["effect_type"],old["effect_level"],old["scripture_change"]) if str(old["user_id"])==user_id else self._result("state_changed")
            rows=[]
            for slot in range(1,max_slots+1):
                row=uow.query_one(f"SELECT effect{slot}_type AS effect_type,effect{slot}_level AS effect_level FROM player_data.natal_treasure WHERE user_id=?",(user_id,))
                if row and int(row["effect_type"] or 0)==effect_type: rows.append((slot,int(row["effect_level"] or 0)))
            if not rows: return self._result("effect_missing")
            if sum(1 for slot in range(1,max_slots+1) if (uow.query_one(f"SELECT effect{slot}_type AS effect_type FROM player_data.natal_treasure WHERE user_id=?",(user_id,)) and int(uow.query_one(f"SELECT effect{slot}_type AS effect_type FROM player_data.natal_treasure WHERE user_id=?",(user_id,))["effect_type"] or 0)))<=1: return self._result("last_effect")
            slot,level=rows[0]; change=level-1-scripture_cost; item=uow.query_one("SELECT goods_num FROM back WHERE user_id=? AND goods_id=?",(user_id,scripture_id)); qty=int(item["goods_num"] or 0) if item else 0
            if change<0 and qty < -change: return self._result("item_insufficient",slot,effect_type,level,change)
            if change>0 and qty+change>max_goods_num: return self._result("inventory_full",slot,effect_type,level,change)
            if change<0:
                if uow.execute("UPDATE back SET goods_num=goods_num-? WHERE user_id=? AND goods_id=? AND goods_num>=?",(-change,user_id,scripture_id,-change)).rowcount!=1:return self._result("state_changed")
            elif change>0:
                if uow.execute("UPDATE back SET goods_num=goods_num+? WHERE user_id=? AND goods_id=?",(change,user_id,scripture_id)).rowcount!=1:return self._result("state_changed")
            if uow.execute(f"UPDATE player_data.natal_treasure SET effect{slot}_type=0,effect{slot}_base_value=0,effect{slot}_level=0 WHERE user_id=?",(user_id,)).rowcount!=1:return self._result("state_changed")
            uow.execute("INSERT INTO natal_forget_operations VALUES(?,?,?,?,?,?,?,?,?)",(operation_id,user_id,effect_type,scripture_id,scripture_cost,max_slots,slot,level,change))
            return self._result("forgotten",slot,effect_type,level,change)
