from __future__ import annotations
import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend
@dataclass(frozen=True)
class PetAwakenResult:
 status:str; skill_id:str
 @property
 def succeeded(self):return self.status in {'applied','duplicate'}
class PetAwakenService:
 def __init__(self,game:str|Path,player:str|Path,lock:RLock|None=None):self.g=Path(game);self.p=Path(player);self.lock=lock or RLock()
 def awaken(self,op,user,uid,expected_skill,new_skill,item_id):
  vals=tuple(map(str,(user,uid,expected_skill,new_skill)));op=str(op);item_id=int(item_id);payload=json.dumps([*vals,item_id])
  with self.lock,closing(db_backend.connect(self.g)) as c:
   try:
    c.execute('ATTACH DATABASE %s AS player_data',(str(self.p),));c.execute('BEGIN IMMEDIATE');c.execute('CREATE TABLE IF NOT EXISTS pet_awaken_operations(operation_id TEXT PRIMARY KEY,payload TEXT,skill_id TEXT)');old=c.execute('SELECT payload,skill_id FROM pet_awaken_operations WHERE operation_id=%s',(op,)).fetchone()
    if old:c.rollback();return PetAwakenResult('duplicate' if str(old[0])==payload else 'state_changed',str(old[1]) if str(old[0])==payload else '')
    pet=c.execute('SELECT COALESCE(skill_id,\'\') FROM player_data.player_pet_item WHERE user_id=%s AND uid=%s',(vals[0],vals[1])).fetchone();item=c.execute('SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s',(vals[0],item_id)).fetchone()
    if pet is None or str(pet[0])!=vals[2]:c.rollback();return PetAwakenResult('state_changed','')
    if item is None or int(item[0])<1:c.rollback();return PetAwakenResult('item_missing','')
    c.execute('UPDATE back SET goods_num=goods_num-1 WHERE user_id=%s AND goods_id=%s',(vals[0],item_id));c.execute('UPDATE player_data.player_pet_item SET skill_id=%s WHERE user_id=%s AND uid=%s',(vals[3],vals[0],vals[1]));c.execute('INSERT INTO pet_awaken_operations VALUES (%s,%s,%s)',(op,payload,vals[3]));c.commit();return PetAwakenResult('applied',vals[3])
   except Exception:c.rollback();raise
__all__=['PetAwakenResult','PetAwakenService']
