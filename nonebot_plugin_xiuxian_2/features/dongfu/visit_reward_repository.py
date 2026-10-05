from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork
from .operation_schema import operation_databases_ready, operation_schema_ready

@dataclass(frozen=True)
class DongfuVisitRewardResult:
    status:str
    gain:int=0
    @property
    def succeeded(self): return self.status in {'rewarded','duplicate'}

class DongfuVisitRewardSqlRepository:
    def __init__(self,game_database:str|Path,player_database:str|Path): self.game_database,self.player_database=str(game_database),str(player_database)

    @staticmethod
    def _receipt(payload,stored_gain):
        value=str(payload or "")
        parts=value.split("|")
        if len(parts)==3:
            try: return (parts[0],parts[1],int(parts[2]))
            except ValueError: return None
        try:
            decoded=json.loads(value)
            if not isinstance(decoded,list) or len(decoded)<2: return None
            legacy_gain=decoded[2] if len(decoded)>2 else 0
            return (str(decoded[0]),str(decoded[1]),int(stored_gain or legacy_gain or 0))
        except (TypeError,ValueError,json.JSONDecodeError): return None

    def reward(self,operation_id,visitor_id,target_id,gain):
        operation_id,visitor_id,target_id,gain=str(operation_id).strip(),str(visitor_id),str(target_id),int(gain); payload=json.dumps([visitor_id,target_id],ensure_ascii=False,separators=(',',':'))
        if not operation_id or not visitor_id or not target_id or visitor_id==target_id or gain<0: raise ValueError('valid visit reward is required')
        if not operation_databases_ready(self.game_database, self.player_database): return DongfuVisitRewardResult('schema_missing')
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data')
            if not operation_schema_ready(uow, 'dongfu_visit_reward_operations'): return DongfuVisitRewardResult('schema_missing')
            old=uow.query_one('SELECT payload,gain FROM dongfu_visit_reward_operations WHERE operation_id=?',(operation_id,))
            if old is not None:
                receipt=self._receipt(old['payload'],old['gain'])
                return DongfuVisitRewardResult('duplicate',receipt[2]) if receipt is not None and receipt[:2]==(visitor_id,target_id) else DongfuVisitRewardResult('state_changed')
            if uow.query_one('SELECT 1 FROM user_xiuxian WHERE user_id=?',(visitor_id,)) is None: return DongfuVisitRewardResult('user_missing')
            visitor=uow.query_one('SELECT built FROM player_data.dongfu_status WHERE user_id=?',(visitor_id,)); target=uow.query_one('SELECT built FROM player_data.dongfu_status WHERE user_id=?',(target_id,))
            if visitor is None or int(visitor['built'] or 0)!=1 or target is None or int(target['built'] or 0)!=1: return DongfuVisitRewardResult('dongfu_changed')
            if uow.execute('UPDATE user_xiuxian SET stone=COALESCE(stone,0)+? WHERE user_id=?',(gain,visitor_id)).rowcount!=1: return DongfuVisitRewardResult('state_changed')
            uow.execute('INSERT INTO dongfu_visit_reward_operations(operation_id,payload,gain) VALUES(?,?,?)',(operation_id,payload,gain)); return DongfuVisitRewardResult('rewarded',gain)
