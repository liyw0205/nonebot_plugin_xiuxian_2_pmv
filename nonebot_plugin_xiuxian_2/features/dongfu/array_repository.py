from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

@dataclass(frozen=True)
class DongfuArrayUpgradeResult:
    status:str; level:int=0
    @property
    def succeeded(self): return self.status in {'upgraded','duplicate'}

class DongfuArrayUpgradeSqlRepository:
    def __init__(self,game_database:str|Path,player_database:str|Path): self.game_database,self.player_database=str(game_database),str(player_database)
    def upgrade(self,operation_id,user_id,expected_level,next_level,stone_cost,item_id,item_cost):
        operation_id,user_id=str(operation_id).strip(),str(user_id); expected_level,next_level,stone_cost,item_id,item_cost=map(int,(expected_level,next_level,stone_cost,item_id,item_cost)); payload='|'.join(map(str,(user_id,expected_level,next_level,stone_cost,item_id,item_cost)))
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data'); uow.execute('CREATE TABLE IF NOT EXISTS dongfu_array_upgrade_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,level INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'); old=uow.query_one('SELECT payload,level FROM dongfu_array_upgrade_operations WHERE operation_id=?',(operation_id,))
            if old is not None: return DongfuArrayUpgradeResult('duplicate',int(old['level'])) if str(old['payload'])==payload else DongfuArrayUpgradeResult('state_changed')
            user=uow.query_one('SELECT stone FROM user_xiuxian WHERE user_id=?',(user_id,)); row=uow.query_one('SELECT built,array_level FROM player_data.dongfu_status WHERE user_id=?',(user_id,))
            if user is None: return DongfuArrayUpgradeResult('user_missing')
            if row is None or int(row['built'] or 0)!=1: return DongfuArrayUpgradeResult('dongfu_missing')
            if int(row['array_level'] or 0)!=expected_level: return DongfuArrayUpgradeResult('state_changed')
            if int(user['stone'] or 0)<stone_cost: return DongfuArrayUpgradeResult('stone_insufficient')
            if item_cost and (uow.query_one('SELECT goods_num FROM back WHERE user_id=? AND goods_id=? AND goods_num>=?',(user_id,item_id,item_cost)) is None): return DongfuArrayUpgradeResult('item_insufficient')
            if uow.execute('UPDATE user_xiuxian SET stone=stone-? WHERE user_id=? AND stone>=?',(stone_cost,user_id,stone_cost)).rowcount!=1: return DongfuArrayUpgradeResult('state_changed')
            if item_cost and uow.execute('UPDATE back SET goods_num=goods_num-? WHERE user_id=? AND goods_id=? AND goods_num>=?',(item_cost,user_id,item_id,item_cost)).rowcount!=1: return DongfuArrayUpgradeResult('state_changed')
            if uow.execute('UPDATE player_data.dongfu_status SET array_level=? WHERE user_id=? AND array_level=?',(next_level,user_id,expected_level)).rowcount!=1: return DongfuArrayUpgradeResult('state_changed')
            uow.execute('INSERT INTO dongfu_array_upgrade_operations(operation_id,payload,level) VALUES(?,?,?)',(operation_id,payload,next_level)); return DongfuArrayUpgradeResult('upgraded',next_level)
