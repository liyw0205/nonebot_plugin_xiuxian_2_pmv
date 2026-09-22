from __future__ import annotations
from pathlib import Path
from .._migrated_application import MigratedFeatureApplication
from .repository import DongfuRepository

class DongfuApplication(MigratedFeatureApplication):
    def __init__(self,database:str|Path,player_database:str|Path|None=None,*,repository:DongfuRepository|None=None)->None:
        super().__init__(database,feature='dongfu',repository=repository or DongfuRepository(database,player_database))
    def accelerate(self,*,operation_id,user_id,expected_slots,slot_no,item_id,now,new_finish): return self.repository.accelerate(operation_id,user_id,expected_slots,slot_no,item_id,now,new_finish)
    def fertilize(self,*,operation_id,user_id,expected_slots,slot_no,item_id,fertilizer_max): return self.repository.fertilize(operation_id,user_id,expected_slots,slot_no,item_id,fertilizer_max)
    def patrol(self,*,operation_id,user_id,day,stamina_cost,daily_limit,stone_gain,reward,max_goods_num): return self.repository.patrol(operation_id,user_id,day,stamina_cost,daily_limit,stone_gain,reward,max_goods_num)

__all__=['DongfuApplication']
