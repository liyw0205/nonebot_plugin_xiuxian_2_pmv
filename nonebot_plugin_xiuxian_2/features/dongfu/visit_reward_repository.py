from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

@dataclass(frozen=True)
class DongfuVisitRewardResult:
    status:str
    @property
    def succeeded(self): return self.status in {'rewarded','duplicate'}

class DongfuVisitRewardSqlRepository:
    def __init__(self,game_database:str|Path,player_database:str|Path): self.game_database,self.player_database=str(game_database),str(player_database)
    def reward(self,operation_id,visitor_id,target_id,gain):
        operation_id,visitor_id,target_id,gain=str(operation_id).strip(),str(visitor_id),str(target_id),int(gain); payload='|'.join((visitor_id,target_id,str(gain)))
        if not operation_id or not visitor_id or not target_id or visitor_id==target_id or gain<0: raise ValueError('valid visit reward is required')
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data'); uow.execute('CREATE TABLE IF NOT EXISTS dongfu_visit_reward_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'); old=uow.query_one('SELECT payload FROM dongfu_visit_reward_operations WHERE operation_id=?',(operation_id,))
            if old is not None: return DongfuVisitRewardResult('duplicate' if str(old['payload'])==payload else 'state_changed')
            if uow.query_one('SELECT 1 FROM user_xiuxian WHERE user_id=?',(visitor_id,)) is None: return DongfuVisitRewardResult('user_missing')
            visitor=uow.query_one('SELECT built FROM player_data.dongfu_status WHERE user_id=?',(visitor_id,)); target=uow.query_one('SELECT built FROM player_data.dongfu_status WHERE user_id=?',(target_id,))
            if visitor is None or int(visitor['built'] or 0)!=1 or target is None or int(target['built'] or 0)!=1: return DongfuVisitRewardResult('dongfu_changed')
            if uow.execute('UPDATE user_xiuxian SET stone=COALESCE(stone,0)+? WHERE user_id=?',(gain,visitor_id)).rowcount!=1: return DongfuVisitRewardResult('state_changed')
            uow.execute('INSERT INTO dongfu_visit_reward_operations(operation_id,payload) VALUES(?,?)',(operation_id,payload)); return DongfuVisitRewardResult('rewarded')
