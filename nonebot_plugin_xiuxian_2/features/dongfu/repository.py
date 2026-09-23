from __future__ import annotations
from pathlib import Path
from .._service_port import ServicePort
from .accelerate_repository import DongfuAccelerateSqlRepository
from .fertilize_repository import DongfuFertilizeSqlRepository
from .patrol_repository import DongfuPatrolSqlRepository
from .harvest_repository import DongfuHarvestSqlRepository
from .visit_reward_repository import DongfuVisitRewardSqlRepository
from .array_repository import DongfuArrayUpgradeSqlRepository
from .plant_repository import DongfuPlantSqlRepository
from .expansion_repository import DongfuExpansionSqlRepository
from .infiltrate_success_repository import DongfuInfiltrateSuccessSqlRepository
from .infiltrate_failure_repository import DongfuInfiltrateFailureSqlRepository

class DongfuRepository(ServicePort):
    def __init__(self,database:str|Path,player_database:str|Path|None=None)->None: super().__init__('dongfu','nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dongfu'); self.database=str(database); self.player_database=str(player_database or database)
    def accelerate(self,*a,**k): return DongfuAccelerateSqlRepository(self.database,self.player_database).accelerate(*a,**k)
    def fertilize(self,*a,**k): return DongfuFertilizeSqlRepository(self.database,self.player_database).fertilize(*a,**k)
    def patrol(self,*a,**k): return DongfuPatrolSqlRepository(self.database,self.player_database).patrol(*a,**k)
    def harvest(self,*a,**k): return DongfuHarvestSqlRepository(self.database,self.player_database).harvest(*a,**k)
    def visit_reward(self,operation_id,visitor_id,target_id,gain): return DongfuVisitRewardSqlRepository(self.database,self.player_database).reward(operation_id,visitor_id,target_id,gain)
    def array_upgrade(self,*a,**k): return DongfuArrayUpgradeSqlRepository(self.database,self.player_database).upgrade(*a,**k)
    def plant(self,*a,**k): return DongfuPlantSqlRepository(self.database,self.player_database).plant(*a,**k)
    def expand(self,*a,**k): return DongfuExpansionSqlRepository(self.database,self.player_database).expand(*a,**k)
    def infiltrate_success(self,*a,**k): return DongfuInfiltrateSuccessSqlRepository(self.database,self.player_database).settle(*a,**k)
    def infiltrate_failure(self,*a,**k): return DongfuInfiltrateFailureSqlRepository(self.database,self.player_database).settle(*a,**k)

__all__=['DongfuRepository']
