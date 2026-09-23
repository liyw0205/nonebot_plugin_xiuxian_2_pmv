from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from ...infrastructure.database import DatabaseUnitOfWork

class SectDisbandSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def _ensure(self, uow: DatabaseUnitOfWork) -> None:
        uow.execute("CREATE TABLE IF NOT EXISTS sect_inactive_disband_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,sect_id INTEGER NOT NULL,sect_name TEXT NOT NULL,reason TEXT NOT NULL,member_count INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")

    @staticmethod
    def _time(value: Any) -> datetime | None:
        if isinstance(value, datetime): return value
        try: return datetime.fromisoformat(str(value or '').replace('Z','+00:00'))
        except ValueError: return None

    def disband_inactive(self, operation_id: str, sect_id: int, reason: str, *, expected_sect_name: str, expected_owner_id: str | None, expected_closed: bool, expected_member_ids: Iterable[str], expected_active_candidate_ids: Iterable[str], checked_at: Any, inactivity_days: int) -> dict[str, Any]:
        operation_id, sect_id, reason = str(operation_id).strip(), int(sect_id), str(reason).strip()
        checked = self._time(checked_at)
        members = tuple(sorted(str(value) for value in expected_member_ids))
        candidates = tuple(sorted(str(value) for value in expected_active_candidate_ids))
        expected_owner_id = None if expected_owner_id in (None, '') else str(expected_owner_id)
        if not operation_id or sect_id <= 0 or reason not in {'empty','no_active_successor','inactive_sole_owner'} or checked is None or int(inactivity_days) <= 0:
            raise ValueError('invalid inactive sect disband request')
        payload = json.dumps({'reason':reason,'sect':[sect_id,str(expected_sect_name),expected_owner_id,int(bool(expected_closed))],'member_ids':members,'active_candidate_ids':candidates,'checked_at':checked.isoformat(sep=' '),'inactivity_days':int(inactivity_days)}, ensure_ascii=True, sort_keys=True, separators=(',',':'))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._ensure(uow)
            old = uow.query_one('SELECT payload,sect_name,reason,member_count FROM sect_inactive_disband_operations WHERE operation_id=?',(operation_id,))
            if old:
                return {'status':'duplicate' if str(old['payload']) == payload else 'operation_conflict','sect_id':sect_id,'sect_name':str(old['sect_name']),'reason':str(old['reason']),'member_count':int(old['member_count'])}
            sect = uow.query_one('SELECT sect_name,sect_owner,COALESCE(closed,0) AS closed FROM sects WHERE sect_id=?',(sect_id,))
            if sect is None: return {'status':'sect_missing','sect_id':sect_id,'reason':reason,'member_count':0}
            name, owner, closed = str(sect['sect_name'] or ''), None if sect['sect_owner'] in (None,'') else str(sect['sect_owner']), bool(int(sect['closed'] or 0))
            base={'sect_id':sect_id,'sect_name':name,'reason':reason}
            if (name,owner,closed)!=(str(expected_sect_name),expected_owner_id,bool(expected_closed)): return {'status':'sect_changed',**base}
            rows=uow.query_all('SELECT user_id,sect_position FROM user_xiuxian WHERE sect_id=? ORDER BY user_id',(sect_id,))
            current_members=tuple(str(row['user_id']) for row in rows)
            if current_members != members: return {'status':'members_changed',**base,'member_count':len(rows)}
            active=[]
            for row in rows:
                if row['sect_position'] is None or int(row['sect_position']) == 0: continue
                cd=uow.query_one('SELECT last_check_info_time FROM user_cd WHERE user_id=?',(str(row['user_id']),))
                last=self._time(cd['last_check_info_time']) if cd else None
                if last is not None and (checked-last).days <= int(inactivity_days): active.append(str(row['user_id']))
            if tuple(sorted(active)) != candidates: return {'status':'candidates_changed',**base,'member_count':len(rows)}
            if reason == 'empty': valid=closed and not rows
            elif reason == 'no_active_successor': valid=closed and bool(rows) and not active
            else:
                valid=(not closed and owner is not None and len(rows)==1 and str(rows[0]['user_id'])==owner and int(rows[0]['sect_position'])==0 and (lambda last: last is not None and (checked-last).days >= int(inactivity_days))(self._time((uow.query_one('SELECT last_check_info_time FROM user_cd WHERE user_id=?',(owner,)) or {}).get('last_check_info_time'))))
            if not valid: return {'status':'condition_changed',**base,'member_count':len(rows)}
            cleared=uow.execute('UPDATE user_xiuxian SET sect_id=NULL,sect_position=NULL,sect_contribution=0 WHERE sect_id=?',(sect_id,)); deleted=uow.execute('DELETE FROM sects WHERE sect_id=?',(sect_id,))
            if cleared.rowcount != len(rows) or deleted.rowcount != 1: raise RuntimeError('inactive sect snapshot changed')
            uow.execute('INSERT INTO sect_inactive_disband_operations(operation_id,payload,sect_id,sect_name,reason,member_count) VALUES(?,?,?,?,?,?)',(operation_id,payload,sect_id,name,reason,len(rows)))
            return {'status':'disbanded',**base,'member_count':len(rows)}
