from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

@dataclass(frozen=True)
class LoveSandResult:
    status:str; gained:int=0; stone_num:int=0; item_remaining:int=0
    @property
    def succeeded(self): return self.status in {'applied','duplicate'}

class LoveSandSqlRepository:
    def __init__(self, game_database:str|Path, impart_database:str|Path, player_database:str|Path): self.game_database,self.impart_database,self.player_database=map(str,(game_database,impart_database,player_database))
    def apply(self, operation_id,user_id,item_id,quantity,gained,expected_item_count,expected_stone_num):
        operation_id,user_id=str(operation_id).strip(),str(user_id); item_id,quantity,gained,expected_item_count,expected_stone_num=map(int,(item_id,quantity,gained,expected_item_count,expected_stone_num))
        if not operation_id or quantity<=0 or gained<0: raise ValueError('invalid love sand request')
        payload=json.dumps([user_id,item_id,quantity],separators=(',',':'))
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.execute('ATTACH DATABASE ? AS impart_data',(self.impart_database,)); uow.execute('ATTACH DATABASE ? AS player_data',(self.player_database,))
            uow.execute('CREATE TABLE IF NOT EXISTS love_sand_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,gained INTEGER NOT NULL,stone_num INTEGER NOT NULL,item_remaining INTEGER NOT NULL)')
            old=uow.query_one('SELECT payload,gained,stone_num,item_remaining FROM love_sand_operations WHERE operation_id=?',(operation_id,))
            if old is not None:
                return LoveSandResult('duplicate' if str(old['payload'])==payload else 'operation_conflict',int(old['gained']),int(old['stone_num']),int(old['item_remaining']))
            item=uow.query_one('SELECT COALESCE(goods_num,0) AS goods_num,COALESCE(bind_num,0) AS bind_num FROM back WHERE user_id=? AND goods_id=?',(user_id,item_id)); impart=uow.query_one('SELECT stone_num FROM impart_data.xiuxian_impart WHERE user_id=?',(user_id,))
            if not item or not impart or int(item['goods_num'])!=expected_item_count or int(impart['stone_num'])!=expected_stone_num: return LoveSandResult('state_changed')
            if expected_item_count<quantity: return LoveSandResult('item_missing')
            remaining,stone_num=expected_item_count-quantity,expected_stone_num+gained
            if uow.execute('UPDATE back SET goods_num=?,bind_num=? WHERE user_id=? AND goods_id=? AND COALESCE(goods_num,0)=?',(remaining,min(max(0,int(item['bind_num'])-quantity),remaining),user_id,item_id,expected_item_count)).rowcount!=1: return LoveSandResult('state_changed')
            uow.execute('UPDATE impart_data.xiuxian_impart SET stone_num=? WHERE user_id=?',(stone_num,user_id))
            uow.execute('CREATE TABLE IF NOT EXISTS player_data.statistics(user_id TEXT PRIMARY KEY)')
            for column in ('思恋流沙使用','思恋结晶获取'):
                columns={str(row[1]) for row in uow.execute('PRAGMA player_data.table_info(statistics)').fetchall()}
                if column not in columns:
                    uow.execute(f'ALTER TABLE player_data.statistics ADD COLUMN "{column}" INTEGER DEFAULT 0')
            uow.execute('INSERT INTO player_data.statistics(user_id,"思恋流沙使用","思恋结晶获取") VALUES(?,?,?) ON CONFLICT(user_id) DO UPDATE SET "思恋流沙使用"=COALESCE(statistics."思恋流沙使用",0)+excluded."思恋流沙使用","思恋结晶获取"=COALESCE(statistics."思恋结晶获取",0)+excluded."思恋结晶获取"',(user_id,quantity,gained))
            uow.execute('INSERT INTO love_sand_operations VALUES(?,?,?,?,?)',(operation_id,payload,gained,stone_num,remaining)); return LoveSandResult('applied',gained,stone_num,remaining)

__all__=['LoveSandSqlRepository','LoveSandResult']
