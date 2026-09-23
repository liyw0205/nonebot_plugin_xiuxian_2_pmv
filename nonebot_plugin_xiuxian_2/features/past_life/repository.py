from __future__ import annotations
from pathlib import Path
from .._service_port import ServicePort
from .start_repository import PastLifeStartSqlRepository
class PastLifeRepository(ServicePort):
    def __init__(self,database:str|Path,player_database:str|Path|None=None)->None: super().__init__('past_life','nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life'); self.database=str(database); self.player_database=str(player_database or database)
    def start(self,*,operation_id,user_id,**kwargs): return PastLifeStartSqlRepository(self.database,self.player_database).start(operation_id,user_id,**kwargs)
__all__=['PastLifeRepository']
