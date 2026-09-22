from __future__ import annotations
from pathlib import Path
from .._migrated_application import MigratedFeatureApplication
from .repository import DongfuRepository

class DongfuApplication(MigratedFeatureApplication):
    def __init__(self,database:str|Path,player_database:str|Path|None=None,*,repository:DongfuRepository|None=None)->None:
        super().__init__(database,feature='dongfu',repository=repository or DongfuRepository(database,player_database))
    def accelerate(self,*,operation_id,user_id,expected_slots,slot_no,item_id,now,new_finish):
        return self.repository.accelerate(operation_id,user_id,expected_slots,slot_no,item_id,now,new_finish)

__all__=['DongfuApplication']
