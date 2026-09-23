from __future__ import annotations
from pathlib import Path
from .._migrated_application import MigratedFeatureApplication
from .repository import DongfuRepository
class DongfuApplication(MigratedFeatureApplication):
    def __init__(self,database:str|Path,player_database:str|Path|None=None,*,repository:DongfuRepository|None=None)->None: super().__init__(database,feature='dongfu',repository=repository or DongfuRepository(database,player_database))
    def accelerate(self,**kwargs): return self.repository.accelerate(**kwargs)
    def fertilize(self,**kwargs): return self.repository.fertilize(**kwargs)
    def patrol(self,**kwargs): return self.repository.patrol(**kwargs)
    def harvest(self,**kwargs): return self.repository.harvest(**kwargs)
    def visit_reward(self,*,operation_id,user_id,visitor_id,target_id,gain): return self.repository.visit_reward(operation_id,visitor_id,target_id,gain)
    def array_upgrade(self,**kwargs): return self.repository.array_upgrade(**kwargs)
    def plant(self,**kwargs): return self.repository.plant(**kwargs)
    def expand(self,**kwargs): return self.repository.expand(**kwargs)
__all__=['DongfuApplication']
