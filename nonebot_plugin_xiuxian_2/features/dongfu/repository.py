from __future__ import annotations

from pathlib import Path
from .._service_port import ServicePort
from .accelerate_repository import DongfuAccelerateSqlRepository

class DongfuRepository(ServicePort):
    def __init__(self,database:str|Path,player_database:str|Path|None=None)->None:
        super().__init__('dongfu','nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dongfu'); self.database=str(database); self.player_database=str(player_database or database)
    def accelerate(self,operation_id,user_id,expected_slots,slot_no,item_id,now,new_finish):
        return DongfuAccelerateSqlRepository(self.database,self.player_database).accelerate(operation_id,user_id,expected_slots,slot_no,item_id,now,new_finish)

__all__=['DongfuRepository']
