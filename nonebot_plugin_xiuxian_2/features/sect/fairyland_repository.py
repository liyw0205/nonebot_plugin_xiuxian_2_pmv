from __future__ import annotations
from pathlib import Path
from typing import Any
from ...infrastructure.database import DatabaseUnitOfWork

class SectFairylandSqlRepository:
    def __init__(self, database: str | Path) -> None: self.database=str(database)
    def upgrade(self, operation_id, actor_id, sect_id, expected_level, next_level, stone_cost, materials_cost, *, owner_position=0) -> dict[str, Any]:
        operation_id,actor_id=str(operation_id).strip(),str(actor_id); sect_id,expected_level,next_level,stone_cost,materials_cost=int(sect_id),int(expected_level),int(next_level),max(int(stone_cost),0),max(int(materials_cost),0)
        if not operation_id: raise ValueError('operation_id must not be empty')
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute('CREATE TABLE IF NOT EXISTS sect_fairyland_operations(operation_id TEXT PRIMARY KEY,actor_id TEXT NOT NULL,sect_id INTEGER NOT NULL,from_level INTEGER NOT NULL,to_level INTEGER NOT NULL,stone_cost INTEGER NOT NULL,materials_cost INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
            old=uow.query_one('SELECT from_level,to_level,stone_cost,materials_cost FROM sect_fairyland_operations WHERE operation_id=?',(operation_id,))
            if old:return {'status':'duplicate','actor_id':actor_id,'sect_id':sect_id,'from_level':int(old['from_level']),'to_level':int(old['to_level']),'stone_cost':int(old['stone_cost']),'materials_cost':int(old['materials_cost'])}
            user=uow.query_one('SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id=?',(actor_id,)); sect=uow.query_one('SELECT sect_owner,COALESCE(sect_fairyland,0) AS level,COALESCE(sect_used_stone,0) AS stones,COALESCE(sect_materials,0) AS materials FROM sects WHERE sect_id=?',(sect_id,))
            if user is None:return {'status':'actor_missing','actor_id':actor_id,'sect_id':sect_id}
            if sect is None:return {'status':'sect_missing','actor_id':actor_id,'sect_id':sect_id}
            base={'actor_id':actor_id,'sect_id':sect_id,'from_level':int(sect['level']),'to_level':next_level,'stone_cost':stone_cost,'materials_cost':materials_cost}
            if int(user['sect_id'] or 0)!=sect_id:return {'status':'membership_changed',**base}
            if str(sect['sect_owner'])!=actor_id or int(user['sect_position'] or 0)!=int(owner_position):return {'status':'not_owner',**base}
            if int(sect['level'])!=expected_level:return {'status':'level_changed',**base}
            if int(sect['stones'])<stone_cost:return {'status':'stone_insufficient',**base}
            if int(sect['materials'])<materials_cost:return {'status':'materials_insufficient',**base}
            changed=uow.execute('UPDATE sects SET sect_fairyland=?,sect_used_stone=sect_used_stone-?,sect_materials=sect_materials-? WHERE sect_id=? AND sect_owner=? AND COALESCE(sect_fairyland,0)=? AND COALESCE(sect_used_stone,0)>=? AND COALESCE(sect_materials,0)>=?',(next_level,stone_cost,materials_cost,sect_id,actor_id,expected_level,stone_cost,materials_cost))
            if changed.rowcount!=1: raise RuntimeError('fairyland state changed concurrently')
            uow.execute('INSERT INTO sect_fairyland_operations(operation_id,actor_id,sect_id,from_level,to_level,stone_cost,materials_cost) VALUES(?,?,?,?,?,?,?)',(operation_id,actor_id,sect_id,expected_level,next_level,stone_cost,materials_cost))
            return {'status':'upgraded',**base}
