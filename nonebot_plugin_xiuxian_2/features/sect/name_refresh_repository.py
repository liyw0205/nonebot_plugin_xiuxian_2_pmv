from __future__ import annotations
from pathlib import Path
from typing import Any
from ...infrastructure.database import DatabaseUnitOfWork

class SectNameRefreshSqlRepository:
    def __init__(self, database: str | Path) -> None: self.database=str(database)
    def charge(self, operation_id, user_id, stone_cost) -> dict[str, Any]:
        operation_id,user_id=str(operation_id).strip(),str(user_id); stone_cost=int(stone_cost)
        if not operation_id: raise ValueError('operation_id must not be empty')
        base={'user_id':user_id,'stone_cost':stone_cost}
        if stone_cost<0:return {'status':'invalid_cost',**base}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute('CREATE TABLE IF NOT EXISTS sect_name_refresh_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,stone_cost INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
            old=uow.query_one('SELECT stone_cost FROM sect_name_refresh_operations WHERE operation_id=?',(operation_id,))
            if old:return {'status':'duplicate','user_id':user_id,'stone_cost':int(old['stone_cost'])}
            user=uow.query_one('SELECT sect_id,stone FROM user_xiuxian WHERE user_id=?',(user_id,))
            if user is None:return {'status':'user_missing',**base}
            if user['sect_id'] is not None:return {'status':'already_member',**base}
            changed=uow.execute('UPDATE user_xiuxian SET stone=stone-? WHERE user_id=? AND sect_id IS NULL AND stone>=?',(stone_cost,user_id,stone_cost))
            if changed.rowcount!=1:return {'status':'stone_insufficient',**base}
            uow.execute('INSERT INTO sect_name_refresh_operations(operation_id,user_id,stone_cost) VALUES(?,?,?)',(operation_id,user_id,stone_cost))
            return {'status':'charged',**base}
