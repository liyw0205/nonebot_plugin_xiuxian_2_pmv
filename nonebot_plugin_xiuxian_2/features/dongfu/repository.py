from __future__ import annotations
from pathlib import Path
from .._service_port import ServicePort
from .accelerate_repository import DongfuAccelerateSqlRepository
from .fertilize_repository import DongfuFertilizeSqlRepository
from .patrol_repository import DongfuPatrolSqlRepository
from .harvest_repository import DongfuHarvestSqlRepository

class DongfuRepository(ServicePort):
    def __init__(self,database:str|Path,player_database:str|Path|None=None)->None:
        super().__init__('dongfu','nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dongfu'); self.database=str(database); self.player_database=str(player_database or database)
    def accelerate(self,*args,**kwargs): return DongfuAccelerateSqlRepository(self.database,self.player_database).accelerate(*args,**kwargs)
    def fertilize(self,*args,**kwargs): return DongfuFertilizeSqlRepository(self.database,self.player_database).fertilize(*args,**kwargs)
    def patrol(self,*args,**kwargs): return DongfuPatrolSqlRepository(self.database,self.player_database).patrol(*args,**kwargs)
    def harvest(self,*args,**kwargs): return DongfuHarvestSqlRepository(self.database,self.player_database).harvest(*args,**kwargs)

__all__=['DongfuRepository']
