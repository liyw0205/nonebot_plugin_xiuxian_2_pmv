from __future__ import annotations
from pathlib import Path
from .._migrated_application import MigratedFeatureApplication
from .repository import DongfuRepository
class DongfuApplication(MigratedFeatureApplication):
    def __init__(self,database:str|Path,player_database:str|Path|None=None,*,repository:DongfuRepository|None=None)->None: super().__init__(database,feature='dongfu',repository=repository or DongfuRepository(database,player_database))
    def accelerate(self,**kwargs): return self.repository.accelerate(**kwargs)
    def fertilize(self,**kwargs): return self.repository.fertilize(**kwargs)
    def patrol(self,**kwargs): return self.repository.patrol(**kwargs)
    def prepare_harvest_snapshot(self,**kwargs): return self.repository.prepare_harvest_snapshot(**kwargs)
    def harvest(self,**kwargs): return self.repository.harvest(**kwargs)
    def visit_reward(self,*,operation_id,user_id,visitor_id,target_id,gain): return self.repository.visit_reward(operation_id,visitor_id,target_id,gain)
    def array_upgrade(self,**kwargs): return self.repository.array_upgrade(**kwargs)
    def plant(self,**kwargs): return self.repository.plant(**kwargs)
    def expand(self,**kwargs): return self.repository.expand(**kwargs)
    def infiltrate_success(self,**kwargs): return self.repository.infiltrate_success(**kwargs)
    def infiltrate_failure(self,**kwargs): return self.repository.infiltrate_failure(**kwargs)
    def status(self, user_id): return self.repository.status(user_id)
    def nearby_target(self, user_id, user_name): return self.repository.nearby_target(user_id,user_name)
__all__=['DongfuApplication']
