from __future__ import annotations
from dataclasses import replace
from pathlib import Path
from .._migrated_application import MigratedFeatureApplication
from .repository import DongfuRepository
class DongfuApplication(MigratedFeatureApplication):
    def __init__(self,database:str|Path,player_database:str|Path|None=None,*,repository:DongfuRepository|None=None,game_event_effects=None)->None:
        super().__init__(database,feature='dongfu',repository=repository or DongfuRepository(database,player_database))
        self.game_event_effects=game_event_effects
    def accelerate(self,**kwargs): return self.repository.accelerate(**kwargs)
    def fertilize(self,**kwargs): return self.repository.fertilize(**kwargs)
    def patrol(self,**kwargs): return self.repository.patrol(**kwargs)
    def prepare_harvest_snapshot(self,**kwargs): return self.repository.prepare_harvest_snapshot(**kwargs)
    def harvest(self,**kwargs):
        result=self.repository.harvest(**kwargs)
        if result.effects_event_id and self.game_event_effects is not None:
            if not self.game_event_effects.dispatch(result.effects_event_id):
                return replace(result,effects_pending=True)
        return result
    def visit_reward(self,*,operation_id,user_id,visitor_id,target_id,gain): return self.repository.visit_reward(operation_id,visitor_id,target_id,gain)
    def array_upgrade(self,**kwargs): return self.repository.array_upgrade(**kwargs)
    def plant(self,**kwargs): return self.repository.plant(**kwargs)
    def expand(self,**kwargs): return self.repository.expand(**kwargs)
    def infiltrate_success(self,**kwargs): return self.repository.infiltrate_success(**kwargs)
    def infiltrate_failure(self,**kwargs): return self.repository.infiltrate_failure(**kwargs)
    def status(self, user_id): return self.repository.status(user_id)
    def operation_receipt(self, action, operation_id): return self.repository.operation_receipt(action, operation_id)
    def infiltration_plan(self, operation_id, user_id, request_identity): return self.repository.infiltration_plan(operation_id,user_id,request_identity)
    def prepare_infiltration_plan(self, operation_id, user_id, request_identity, plan): return self.repository.prepare_infiltration_plan(operation_id,user_id,request_identity,plan)
    def nearby_target(self, user_id, user_name): return self.repository.nearby_target(user_id,user_name)
    async def random_target(self, **kwargs): return await self.repository.random_target(**kwargs)
__all__=['DongfuApplication']
