from __future__ import annotations
from pathlib import Path
from typing import Any
from ...infrastructure.database import DatabaseUnitOfWork

class SectCreationSqlRepository:
    def __init__(self, database: str | Path) -> None: self.database=str(database)
    def create(self, operation_id, user_id, sect_name, stone_cost, owner_position) -> dict[str, Any]:
        operation_id,user_id,sect_name=str(operation_id).strip(),str(user_id),str(sect_name).strip(); stone_cost,owner_position=int(stone_cost),int(owner_position)
        if not operation_id: raise ValueError('operation_id must not be empty')
        base={'user_id':user_id,'sect_name':sect_name,'stone_cost':stone_cost}
        if not sect_name:return {'status':'invalid_name',**base}
        if stone_cost<0:return {'status':'invalid_cost',**base}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute('CREATE TABLE IF NOT EXISTS sect_creation_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,sect_id INTEGER NOT NULL,sect_name TEXT NOT NULL,stone_cost INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
            old=uow.query_one('SELECT sect_id,sect_name,stone_cost FROM sect_creation_operations WHERE operation_id=?',(operation_id,))
            if old:return {'status':'duplicate','user_id':user_id,'sect_name':str(old['sect_name']),'stone_cost':int(old['stone_cost']),'sect_id':int(old['sect_id'])}
            user=uow.query_one('SELECT sect_id,stone FROM user_xiuxian WHERE user_id=?',(user_id,))
            if user is None:return {'status':'user_missing',**base}
            if user['sect_id'] is not None:return {'status':'already_member',**base}
            if int(user['stone'] or 0)<stone_cost:return {'status':'stone_insufficient',**base}
            if uow.query_one('SELECT sect_id FROM sects WHERE sect_name=?',(sect_name,)):return {'status':'name_exists',**base}
            uow.execute('INSERT INTO sects(sect_name,sect_owner,sect_scale,sect_used_stone,join_open,closed,combat_power) VALUES(?,?,0,0,1,0,0)',(sect_name,user_id)); created=uow.query_one('SELECT sect_id FROM sects WHERE sect_owner=? AND sect_name=?',(user_id,sect_name))
            if created is None: raise RuntimeError('created sect could not be read back')
            sect_id=int(created['sect_id']); changed=uow.execute('UPDATE user_xiuxian SET sect_id=?,sect_position=?,stone=stone-? WHERE user_id=? AND sect_id IS NULL AND stone>=?',(sect_id,owner_position,stone_cost,user_id,stone_cost))
            if changed.rowcount!=1: return {'status':'user_changed',**base}
            uow.execute('INSERT INTO sect_creation_operations(operation_id,user_id,sect_id,sect_name,stone_cost) VALUES(?,?,?,?,?)',(operation_id,user_id,sect_id,sect_name,stone_cost))
            return {'status':'created',**base,'sect_id':sect_id}
