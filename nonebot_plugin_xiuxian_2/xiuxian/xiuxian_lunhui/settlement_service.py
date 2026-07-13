from __future__ import annotations
import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend
@dataclass(frozen=True)
class LunhuiSettlementResult:
 status:str; stone:int; root_level:int
 @property
 def succeeded(self):return self.status in {'applied','duplicate'}
class LunhuiSettlementService:
 def __init__(self,database:str|Path,lock:RLock|None=None):self.db=Path(database);self.lock=lock or RLock()
 def settle(self,op,user,expected_level,root_type,increment_root,reward_id,reward_name):
  op,user,expected_level,root_type=str(op),str(user),str(expected_level),str(root_type);increment_root=bool(increment_root);reward_id=int(reward_id);payload=json.dumps([user,expected_level,root_type,increment_root,reward_id,reward_name])
  with self.lock,closing(db_backend.connect(self.db)) as c:
   try:
    c.execute('BEGIN IMMEDIATE');c.execute('CREATE TABLE IF NOT EXISTS lunhui_settlement_operations(operation_id TEXT PRIMARY KEY,payload TEXT,stone INTEGER,root_level INTEGER)');old=c.execute('SELECT payload,stone,root_level FROM lunhui_settlement_operations WHERE operation_id=%s',(op,)).fetchone()
    if old:c.rollback();return LunhuiSettlementResult('duplicate' if str(old[0])==payload else 'state_changed',int(old[1]) if str(old[0])==payload else 0,int(old[2]) if str(old[0])==payload else 0)
    row=c.execute('SELECT level,COALESCE(stone,0),COALESCE(root_level,0) FROM user_xiuxian WHERE user_id=%s',(user,)).fetchone()
    if row is None or str(row[0])!=expected_level:c.rollback();return LunhuiSettlementResult('state_changed',0,0)
    stone=min(int(row[1]),100000000);root_level=int(row[2])+(1 if increment_root else 0);c.execute("UPDATE user_xiuxian SET level='江湖好手',exp=100,stone=%s,level_up_rate=0,root_type=%s,root_level=%s WHERE user_id=%s",(stone,root_type,root_level,user));c.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num) VALUES (%s,%s,%s,'特殊道具',1) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+1",(user,reward_id,reward_name));c.execute('INSERT INTO lunhui_settlement_operations VALUES (%s,%s,%s,%s)',(op,payload,stone,root_level));c.commit();return LunhuiSettlementResult('applied',stone,root_level)
   except Exception:c.rollback();raise
__all__=['LunhuiSettlementResult','LunhuiSettlementService']
