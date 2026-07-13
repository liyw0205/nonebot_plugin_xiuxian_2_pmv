from __future__ import annotations
import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend

@dataclass(frozen=True)
class RiftEntryResult:
 status:str; entries:int
 @property
 def succeeded(self):return self.status in {'applied','duplicate'}

class RiftEntryService:
 def __init__(self,database:str|Path,lock:RLock|None=None):self._database=Path(database);self._lock=lock or RLock()
 def enter(self,operation_id,user_id,rift_key,rift_data,duration,ticket_id=0):
  operation_id=str(operation_id).strip();user_id=str(user_id);rift_key=str(rift_key);duration=int(duration);ticket_id=int(ticket_id);snapshot=json.dumps(rift_data,ensure_ascii=False,sort_keys=True)
  if not operation_id or not rift_key or duration<=0 or ticket_id<0:raise ValueError('valid operation, rift and duration required')
  payload=json.dumps([user_id,rift_key,snapshot,duration,ticket_id],ensure_ascii=True)
  with self._lock,closing(db_backend.connect(self._database)) as c:
   try:
    c.execute('BEGIN IMMEDIATE');c.execute('CREATE TABLE IF NOT EXISTS rift_entries (user_id TEXT PRIMARY KEY,rift_key TEXT NOT NULL,rift_data TEXT NOT NULL,status TEXT NOT NULL,duration INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)');c.execute('CREATE TABLE IF NOT EXISTS rift_entry_counts (user_id TEXT PRIMARY KEY,entry_count INTEGER NOT NULL)');c.execute('CREATE TABLE IF NOT EXISTS rift_entry_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,entry_count INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
    old=c.execute('SELECT payload,entry_count FROM rift_entry_operations WHERE operation_id=%s',(operation_id,)).fetchone()
    if old:c.rollback();return RiftEntryResult('duplicate' if str(old[0])==payload else 'state_changed',int(old[1]) if str(old[0])==payload else 0)
    if c.execute('SELECT 1 FROM user_xiuxian WHERE user_id=%s',(user_id,)).fetchone() is None:c.rollback();return RiftEntryResult('user_missing',0)
    active=c.execute("SELECT 1 FROM rift_entries WHERE user_id=%s AND status='active'",(user_id,)).fetchone()
    if active:c.rollback();return RiftEntryResult('already_active',0)
    cd=c.execute('SELECT COALESCE(type,0) FROM user_cd WHERE user_id=%s',(user_id,)).fetchone()
    if cd is None or int(cd[0])!=0:c.rollback();return RiftEntryResult('busy',0)
    if ticket_id:
     item=c.execute('SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s',(user_id,ticket_id)).fetchone()
     if item is None or int(item[0])<1:c.rollback();return RiftEntryResult('ticket_missing',0)
     c.execute('UPDATE back SET goods_num=goods_num-1 WHERE user_id=%s AND goods_id=%s',(user_id,ticket_id))
    c.execute('INSERT INTO rift_entries VALUES (%s,%s,%s,%s,%s,CURRENT_TIMESTAMP) ON CONFLICT(user_id) DO UPDATE SET rift_key=EXCLUDED.rift_key,rift_data=EXCLUDED.rift_data,status=EXCLUDED.status,duration=EXCLUDED.duration,created_at=EXCLUDED.created_at',(user_id,rift_key,snapshot,'active',duration))
    c.execute('UPDATE user_cd SET type=3,create_time=CURRENT_TIMESTAMP,scheduled_time=%s WHERE user_id=%s',(duration,user_id));c.execute('INSERT INTO rift_entry_counts VALUES (%s,1) ON CONFLICT(user_id) DO UPDATE SET entry_count=rift_entry_counts.entry_count+1',(user_id,));count=int(c.execute('SELECT entry_count FROM rift_entry_counts WHERE user_id=%s',(user_id,)).fetchone()[0]);c.execute('INSERT INTO rift_entry_operations VALUES (%s,%s,%s,CURRENT_TIMESTAMP)',(operation_id,payload,count));c.commit();return RiftEntryResult('applied',count)
   except Exception:c.rollback();raise
__all__=['RiftEntryResult','RiftEntryService']
