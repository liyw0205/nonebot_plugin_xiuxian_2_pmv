from __future__ import annotations
import json
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

class SectOwnerTransferSqlRepository:
    def __init__(self, database: str | Path) -> None: self.database=str(database)
    def transfer(self, operation_id, actor_id, target_id, *, owner_position=0, former_owner_position=None):
        operation_id,actor_id,target_id=str(operation_id).strip(),str(actor_id),str(target_id); former_owner_position=int(owner_position)+1 if former_owner_position is None else int(former_owner_position)
        if not operation_id: raise ValueError('operation_id must not be empty')
        if actor_id==target_id: return {'status':'self_transfer','actor_id':actor_id,'target_id':target_id}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS sect_operations(operation_id TEXT PRIMARY KEY,operation_type TEXT NOT NULL,actor_id TEXT NOT NULL,target_id TEXT NOT NULL,sect_id INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            old=uow.query_one('SELECT sect_id FROM sect_operations WHERE operation_id=?',(operation_id,))
            if old: return {'status':'duplicate','actor_id':actor_id,'target_id':target_id,'sect_id':int(old['sect_id'])}
            actor=uow.query_one('SELECT sect_id,sect_position,user_name FROM user_xiuxian WHERE user_id=?',(actor_id,)); target=uow.query_one('SELECT sect_id,sect_position,user_name FROM user_xiuxian WHERE user_id=?',(target_id,))
            if actor is None:return {'status':'actor_missing','actor_id':actor_id,'target_id':target_id}
            if target is None:return {'status':'target_missing','actor_id':actor_id,'target_id':target_id}
            if actor['sect_id'] is None:return {'status':'actor_without_sect','actor_id':actor_id,'target_id':target_id}
            sect_id=int(actor['sect_id']); sect=uow.query_one('SELECT sect_owner,sect_name FROM sects WHERE sect_id=?',(sect_id,))
            base={'actor_id':actor_id,'target_id':target_id,'sect_id':sect_id,'actor_name':str(actor['user_name'] or ''),'target_name':str(target['user_name'] or ''),'sect_name':str(sect['sect_name'] or '') if sect else ''}
            if sect is None:return {'status':'sect_missing',**base}
            if str(sect['sect_owner'])!=actor_id or int(actor['sect_position'] or 0)!=int(owner_position):return {'status':'not_owner',**base}
            if target['sect_id']!=actor['sect_id']:return {'status':'target_not_member',**base}
            if int(target['sect_position'] or 0)==int(owner_position):return {'status':'target_already_owner',**base}
            a=uow.execute('UPDATE user_xiuxian SET sect_position=? WHERE user_id=? AND sect_id=? AND sect_position=?',(former_owner_position,actor_id,sect_id,owner_position)); t=uow.execute('UPDATE user_xiuxian SET sect_position=? WHERE user_id=? AND sect_id=?',(owner_position,target_id,sect_id)); s=uow.execute('UPDATE sects SET sect_owner=? WHERE sect_id=? AND sect_owner=?',(target_id,sect_id,actor_id))
            if a.rowcount!=1 or t.rowcount!=1 or s.rowcount!=1: raise RuntimeError('sect membership changed concurrently')
            uow.execute('INSERT INTO sect_operations(operation_id,operation_type,actor_id,target_id,sect_id) VALUES(?,?,?,?,?)',(operation_id,'transfer_owner',actor_id,target_id,sect_id))
            return {'status':'transferred',**base}
