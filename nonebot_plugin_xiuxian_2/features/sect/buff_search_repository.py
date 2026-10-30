from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Iterable
from ...infrastructure.database import DatabaseUnitOfWork

# 真实 sects 列名（权威来源 xiuxian_config.py sql_sects）。历史上这里写成了两个
# 并不存在的列名（`sect_` 前缀）：SELECT 会把未解析标识符当字符串字面量、
# UPDATE 直接抛 OperationalError。这里以真实列 mainbuff/secbuff 为准。
_BUFF_COLUMNS = {"main": "mainbuff", "secondary": "secbuff"}


class SectBuffSearchSqlRepository:
    def __init__(self, database: str | Path) -> None: self.database=str(database)
    def apply(self, operation_id, actor_id, sect_id, buff_type, previous_value, new_value, stone_cost, materials_cost, *, owner_position: int = 0) -> dict[str, Any]:
        operation_id,actor_id,buff_type=str(operation_id).strip(),str(actor_id),str(buff_type); sect_id,stone_cost,materials_cost=int(sect_id),max(int(stone_cost),0),max(int(materials_cost),0); previous_value=str(previous_value); new_value=str(new_value); owner_position=int(owner_position)
        if not operation_id: raise ValueError('operation_id must not be empty')
        column=_BUFF_COLUMNS.get(buff_type)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute('CREATE TABLE IF NOT EXISTS sect_buff_search_operations(operation_id TEXT PRIMARY KEY,actor_id TEXT NOT NULL,sect_id INTEGER NOT NULL,buff_type TEXT NOT NULL,previous_value TEXT NOT NULL,new_value TEXT NOT NULL,stone_cost INTEGER NOT NULL,materials_cost INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
            old=uow.query_one('SELECT buff_type,previous_value,new_value,stone_cost,materials_cost FROM sect_buff_search_operations WHERE operation_id=?',(operation_id,))
            if old:return {'status':'duplicate','actor_id':actor_id,'sect_id':sect_id,'buff_type':str(old['buff_type']),'previous_value':str(old['previous_value']),'new_value':str(old['new_value']),'stone_cost':int(old['stone_cost']),'materials_cost':int(old['materials_cost'])}
            user=uow.query_one('SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id=?',(actor_id,)); sect=uow.query_one('SELECT sect_owner,COALESCE(sect_used_stone,0) AS stones,COALESCE(sect_materials,0) AS materials FROM sects WHERE sect_id=?',(sect_id,))
            base={'actor_id':actor_id,'sect_id':sect_id,'buff_type':buff_type,'previous_value':previous_value,'new_value':new_value,'stone_cost':stone_cost,'materials_cost':materials_cost}
            if user is None:return {'status':'actor_missing',**base}
            if sect is None:return {'status':'sect_missing',**base}
            if int(user['sect_id'] or 0)!=sect_id:return {'status':'membership_changed',**base}
            if str(sect['sect_owner'])!=actor_id or int(user['sect_position'] or 0)!=owner_position:return {'status':'not_owner',**base}
            if int(sect['stones'])<stone_cost:return {'status':'stone_insufficient',**base}
            if int(sect['materials'])<materials_cost:return {'status':'materials_insufficient',**base}
            if column is None: return {'status':'invalid_buff_type',**base}
            changed=uow.execute(f'UPDATE sects SET {column}=?,sect_used_stone=sect_used_stone-?,sect_materials=sect_materials-? WHERE sect_id=? AND sect_owner=? AND COALESCE(sect_used_stone,0)>=? AND COALESCE(sect_materials,0)>=?',(new_value,stone_cost,materials_cost,sect_id,actor_id,stone_cost,materials_cost))
            if changed.rowcount!=1: raise RuntimeError('buff search state changed concurrently')
            uow.execute('INSERT INTO sect_buff_search_operations(operation_id,actor_id,sect_id,buff_type,previous_value,new_value,stone_cost,materials_cost) VALUES(?,?,?,?,?,?,?,?)',(operation_id,actor_id,sect_id,buff_type,previous_value,new_value,stone_cost,materials_cost))
            return {'status':'applied',**base}
