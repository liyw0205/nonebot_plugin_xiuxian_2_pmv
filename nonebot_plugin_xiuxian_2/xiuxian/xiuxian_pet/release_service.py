from __future__ import annotations
import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend
@dataclass(frozen=True)
class PetReleaseResult:
 status:str; refund:int
 @property
 def succeeded(self):return self.status in {'applied','duplicate'}
class PetReleaseService:
 def __init__(self,game:str|Path,player:str|Path,lock:RLock|None=None):self.g=Path(game);self.p=Path(player);self.lock=lock or RLock()
 def release(self,op,user,uid,expected_exp,refund_item,refund,max_goods):
  op,user,uid=str(op),str(user),str(uid);expected_exp,refund_item,refund,max_goods=map(int,(expected_exp,refund_item,refund,max_goods));payload=json.dumps([user,uid,expected_exp,refund_item,refund,max_goods])
  with self.lock,closing(db_backend.connect(self.g)) as c:
   attached=False
   try:
    c.execute('ATTACH DATABASE %s AS player_data',(str(self.p),));attached=True;c.execute('BEGIN IMMEDIATE');c.execute('CREATE TABLE IF NOT EXISTS pet_release_operations(operation_id TEXT PRIMARY KEY,payload TEXT,refund INTEGER)');old=c.execute('SELECT payload,refund FROM pet_release_operations WHERE operation_id=%s',(op,)).fetchone()
    if old:c.rollback();return PetReleaseResult('duplicate' if str(old[0])==payload else 'state_changed',int(old[1]) if str(old[0])==payload else 0)
    pet=c.execute('SELECT COALESCE(total_exp,0),COALESCE(is_active,0) FROM player_data.player_pet_item WHERE user_id=%s AND uid=%s',(user,uid)).fetchone()
    if pet is None:c.rollback();return PetReleaseResult('missing',0)
    if int(pet[0])!=expected_exp:c.rollback();return PetReleaseResult('state_changed',0)
    row=c.execute('SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s',(user,refund_item)).fetchone()
    if (int(row[0]) if row else 0)+refund>max_goods:c.rollback();return PetReleaseResult('inventory_full',0)
    c.execute('DELETE FROM player_data.player_pet_item WHERE user_id=%s AND uid=%s',(user,uid));c.execute("UPDATE player_data.player_pet SET active_uid=NULL,active=NULL WHERE user_id=%s AND active_uid=%s",(user,uid));c.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num) VALUES (%s,%s,'天地灵髓','特殊道具',%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num",(user,refund_item,refund));c.execute('INSERT INTO pet_release_operations VALUES (%s,%s,%s)',(op,payload,refund));c.commit();return PetReleaseResult('applied',refund)
   except Exception:c.rollback();raise
   finally:
    if attached:c.execute('DETACH DATABASE player_data')
__all__=['PetReleaseResult','PetReleaseService']
