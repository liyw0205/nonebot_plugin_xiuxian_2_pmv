from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

@dataclass(frozen=True)
class CompensationRewardClaimResult:
    status:str; reward_type:str=''; record_id:str=''; user_id:str=''; used_count:int=0
    @property
    def applied(self): return self.status=='claimed'

class CompensationRewardClaimSqlRepository:
    def __init__(self,database:str|Path,max_goods_num:int): self.database=str(database); self.max_goods_num=int(max_goods_num)
    @staticmethod
    def _inventory_type(goods_type:str)->str:
        if goods_type in {'辅修功法','神通','功法','身法','瞳术'}: return '技能'
        if goods_type in {'法器','防具'}: return '装备'
        return goods_type
    def claim(self,operation_id,reward_type,record_id,user_id,reward_items,usage_limit=0,legacy_used_count=0,expected_definition_version=None):
        operation_id,reward_type,record_id,user_id=map(str,(operation_id,reward_type,record_id,user_id)); usage_limit=max(int(usage_limit or 0),0); legacy_used_count=max(int(legacy_used_count or 0),0); expected=None if expected_definition_version in (None,'') else int(expected_definition_version)
        with DatabaseUnitOfWork(self.database,immediate=True) as uow:
            uow.execute('CREATE TABLE IF NOT EXISTS reward_claims(reward_type TEXT NOT NULL,record_id TEXT NOT NULL,user_id TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(reward_type,record_id,user_id))'); uow.execute('CREATE TABLE IF NOT EXISTS reward_claim_counters(reward_type TEXT NOT NULL,record_id TEXT NOT NULL,baseline_count INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(reward_type,record_id))')
            if expected is not None:
                definition=uow.query_one('SELECT version FROM compensation_definitions WHERE record_id=?',(record_id,))
                if definition is None: return CompensationRewardClaimResult('record_missing',reward_type,record_id,user_id)
                if int(definition['version'])!=expected: return CompensationRewardClaimResult('definition_changed',reward_type,record_id,user_id)
            if uow.query_one('SELECT 1 AS found FROM user_xiuxian WHERE user_id=?',(user_id,)) is None: return CompensationRewardClaimResult('user_missing',reward_type,record_id,user_id)
            if uow.query_one('SELECT 1 AS found FROM reward_claims WHERE reward_type=? AND record_id=? AND user_id=?',(reward_type,record_id,user_id)) is not None: return CompensationRewardClaimResult('duplicate',reward_type,record_id,user_id)
            if usage_limit:
                uow.execute('INSERT INTO reward_claim_counters VALUES(?,?,?) ON CONFLICT(reward_type,record_id) DO NOTHING',(reward_type,record_id,legacy_used_count)); used=int(uow.execute('SELECT COUNT(*) FROM reward_claims WHERE reward_type=? AND record_id=?',(reward_type,record_id)).fetchone()[0])+int(uow.execute('SELECT baseline_count FROM reward_claim_counters WHERE reward_type=? AND record_id=?',(reward_type,record_id)).fetchone()[0]);
                if used>=usage_limit: return CompensationRewardClaimResult('exhausted',reward_type,record_id,user_id,used)
            now=datetime.now().isoformat(sep=' ',timespec='seconds')
            for item in reward_items:
                quantity=max(int(item['quantity']),0)
                if quantity<=0: continue
                if item['type']=='stone':
                    if uow.execute('UPDATE user_xiuxian SET stone=COALESCE(stone,0)+? WHERE user_id=?',(quantity,user_id)).rowcount!=1: raise RuntimeError('reward user disappeared')
                    continue
                goods_id=int(item['id']); goods_type=self._inventory_type(str(item['type'])); quantity=min(quantity,self.max_goods_num)
                uow.execute('INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,goods_type=excluded.goods_type,update_time=excluded.update_time,goods_num=MIN(COALESCE(back.goods_num,0)+excluded.goods_num,?),bind_num=MIN(COALESCE(back.bind_num,0)+excluded.goods_num,MIN(COALESCE(back.goods_num,0)+excluded.goods_num,?))',(user_id,goods_id,str(item['name']),goods_type,quantity,now,now,quantity,self.max_goods_num,self.max_goods_num))
            uow.execute('INSERT INTO reward_claims VALUES(?,?,?,CURRENT_TIMESTAMP)',(reward_type,record_id,user_id)); used=int(uow.execute('SELECT COUNT(*) FROM reward_claims WHERE reward_type=? AND record_id=?',(reward_type,record_id)).fetchone()[0]); return CompensationRewardClaimResult('claimed',reward_type,record_id,user_id,used)
