from __future__ import annotations
import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend
@dataclass(frozen=True)
class RiftSpeedupResult:
 status:str; new_time:int
 @property
 def succeeded(self):return self.status in {'applied','duplicate'}
class RiftSpeedupService:
 def __init__(self,database:str|Path,lock:RLock|None=None):self.db=Path(database);self.lock=lock or RLock()
 def apply(self,op,user,item_id,expected_time,new_time):
  op=str(op);user=str(user);item_id=int(item_id);expected_time=int(expected_time);new_time=int(new_time);payload=json.dumps([user,item_id,expected_time,new_time])
  if not op or not 0<new_time<expected_time:raise ValueError('invalid speedup')
  with self.lock,closing(db_backend.connect(self.db)) as c:
   try:
    c.execute('BEGIN IMMEDIATE');c.execute('CREATE TABLE IF NOT EXISTS rift_speedup_operations(operation_id TEXT PRIMARY KEY,payload TEXT,new_time INTEGER)');old=c.execute('SELECT payload,new_time FROM rift_speedup_operations WHERE operation_id=%s',(op,)).fetchone()
    if old:c.rollback();return RiftSpeedupResult('duplicate' if str(old[0])==payload else 'state_changed',int(old[1]) if str(old[0])==payload else expected_time)
    row=c.execute("SELECT duration FROM rift_entries WHERE user_id=%s AND status='active'",(user,)).fetchone();item=c.execute('SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s',(user,item_id)).fetchone()
    if row is None or int(row[0])!=expected_time:c.rollback();return RiftSpeedupResult('state_changed',expected_time)
    if item is None or int(item[0])<1:c.rollback();return RiftSpeedupResult('item_missing',expected_time)
    c.execute('UPDATE back SET goods_num=goods_num-1 WHERE user_id=%s AND goods_id=%s',(user,item_id));c.execute('UPDATE rift_entries SET duration=%s WHERE user_id=%s',(new_time,user));c.execute('UPDATE user_cd SET scheduled_time=%s WHERE user_id=%s',(new_time,user));c.execute('INSERT INTO rift_speedup_operations VALUES (%s,%s,%s)',(op,payload,new_time));c.commit();return RiftSpeedupResult('applied',new_time)
   except Exception:c.rollback();raise
__all__=['RiftSpeedupResult','RiftSpeedupService']
