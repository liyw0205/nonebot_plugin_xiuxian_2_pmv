from __future__ import annotations
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork
from .schema import OWNER_TRANSFER_COLUMNS, SCHEMA_MISSING, table_ready


class _OwnerTransferRejected(Exception):
    def __init__(self, reason: str, subject: str) -> None:
        self.reason = reason
        self.subject = subject


class SectOwnerTransferSqlRepository:
    def __init__(self, database: str | Path) -> None: self.database=str(database)
    def transfer(self, operation_id, actor_id, target_id, *, owner_position=0, former_owner_position=None):
        operation_id,actor_id,target_id=str(operation_id).strip(),str(actor_id),str(target_id); former_owner_position=int(owner_position)+1 if former_owner_position is None else int(former_owner_position)
        if not operation_id: raise ValueError('operation_id must not be empty')
        if actor_id==target_id: return {'status':'self_transfer','actor_id':actor_id,'target_id':target_id}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not table_ready(uow, "sect_operations", OWNER_TRANSFER_COLUMNS):
                return {"status": SCHEMA_MISSING}
            old=uow.query_one('SELECT operation_type,actor_id,target_id,sect_id FROM sect_operations WHERE operation_id=?',(operation_id,))
            if old:
                if (str(old['operation_type']), str(old['actor_id']), str(old['target_id'])) != ('transfer_owner', actor_id, target_id):
                    return {'status':'operation_conflict','actor_id':actor_id,'target_id':target_id,'sect_id':old['sect_id'],'reason':'operation_conflict','subject':'actor'}
                sect_id=int(old['sect_id'])
                actor=uow.query_one('SELECT user_name FROM user_xiuxian WHERE user_id=?',(actor_id,))
                target=uow.query_one('SELECT user_name FROM user_xiuxian WHERE user_id=?',(target_id,))
                sect=uow.query_one('SELECT sect_name FROM sects WHERE sect_id=?',(sect_id,))
                return {'status':'duplicate','actor_id':actor_id,'target_id':target_id,'sect_id':sect_id,
                        'actor_name':str(actor['user_name'] or '') if actor else '',
                        'target_name':str(target['user_name'] or '') if target else '',
                        'sect_name':str(sect['sect_name'] or '') if sect else ''}
            actor=uow.query_one('SELECT sect_id,sect_position,user_name FROM user_xiuxian WHERE user_id=?',(actor_id,)); target=uow.query_one('SELECT sect_id,sect_position,user_name FROM user_xiuxian WHERE user_id=?',(target_id,))
            if actor is None:return {'status':'actor_missing','actor_id':actor_id,'target_id':target_id}
            if target is None:return {'status':'target_missing','actor_id':actor_id,'target_id':target_id}
            if actor['sect_id'] is None:return {'status':'actor_without_sect','actor_id':actor_id,'target_id':target_id}
            sect_id=int(actor['sect_id']); sect=uow.query_one('SELECT sect_owner,sect_name FROM sects WHERE sect_id=?',(sect_id,))
            base={'actor_id':actor_id,'target_id':target_id,'sect_id':sect_id,'actor_name':str(actor['user_name'] or ''),'target_name':str(target['user_name'] or ''),'sect_name':str(sect['sect_name'] or '') if sect else ''}
            if sect is None:return {'status':'sect_missing',**base}
            if str(sect['sect_owner'])!=actor_id or int(actor['sect_position'] or 0)!=int(owner_position):return {'status':'not_owner','reason':'sect_owner_changed' if str(sect['sect_owner'])!=actor_id else 'sect_position_changed',**base}
            if target['sect_id']!=actor['sect_id']:return {'status':'target_not_member',**base}
            if int(target['sect_position'] or 0)==int(owner_position):return {'status':'target_already_owner',**base}
            try:
                with uow.savepoint('sect_owner_transfer'):
                    a=uow.execute('UPDATE user_xiuxian SET sect_position=? WHERE user_id=? AND sect_id=? AND sect_position=?',(former_owner_position,actor_id,sect_id,owner_position))
                    saved_actor=uow.query_one('SELECT sect_position FROM user_xiuxian WHERE user_id=? AND sect_id=?',(actor_id,sect_id))
                    if a.rowcount!=1 or saved_actor is None or int(saved_actor['sect_position'])!=former_owner_position:
                        raise _OwnerTransferRejected('sect_owner_transfer_actor_write_rejected','actor')
                    t=uow.execute('UPDATE user_xiuxian SET sect_position=? WHERE user_id=? AND sect_id=? AND sect_position<>?',(owner_position,target_id,sect_id,owner_position))
                    saved_target=uow.query_one('SELECT sect_position FROM user_xiuxian WHERE user_id=? AND sect_id=?',(target_id,sect_id))
                    if t.rowcount!=1 or saved_target is None or int(saved_target['sect_position'])!=owner_position:
                        raise _OwnerTransferRejected('sect_owner_transfer_target_write_rejected','target')
                    s=uow.execute('UPDATE sects SET sect_owner=? WHERE sect_id=? AND sect_owner=?',(target_id,sect_id,actor_id))
                    saved_sect=uow.query_one('SELECT sect_owner FROM sects WHERE sect_id=?',(sect_id,))
                    if s.rowcount!=1 or saved_sect is None or str(saved_sect['sect_owner'])!=target_id:
                        raise _OwnerTransferRejected('sect_owner_transfer_sect_write_rejected','sect')
                    receipt=uow.execute('INSERT INTO sect_operations(operation_id,operation_type,actor_id,target_id,sect_id) VALUES(?,?,?,?,?)',(operation_id,'transfer_owner',actor_id,target_id,sect_id))
                    if receipt.rowcount!=1:
                        raise _OwnerTransferRejected('sect_owner_transfer_receipt_write_rejected',operation_id)
                    saved_receipt=uow.query_one('SELECT operation_type,actor_id,target_id,sect_id FROM sect_operations WHERE operation_id=?',(operation_id,))
                    expected=('transfer_owner',actor_id,target_id,sect_id)
                    actual=None if saved_receipt is None else (str(saved_receipt['operation_type']),str(saved_receipt['actor_id']),str(saved_receipt['target_id']),int(saved_receipt['sect_id']))
                    if actual != expected:
                        raise _OwnerTransferRejected('sect_owner_transfer_receipt_write_rejected',operation_id)
            except _OwnerTransferRejected as exc:
                return {'status':'state_changed','actor_id':actor_id,'target_id':target_id,'sect_id':sect_id,'reason':exc.reason,'subject':exc.subject,**base}
            return {'status':'transferred',**base}
