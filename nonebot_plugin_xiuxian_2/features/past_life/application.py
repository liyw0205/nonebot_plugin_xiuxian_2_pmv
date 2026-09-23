from __future__ import annotations
from pathlib import Path
from .._migrated_application import MigratedFeatureApplication
from .repository import PastLifeRepository
from types import SimpleNamespace
class PastLifeApplication(MigratedFeatureApplication):
    def __init__(self,database:str|Path,player_database:str|Path|None=None,*,repository:PastLifeRepository|None=None)->None: super().__init__(database,feature='past_life',repository=repository or PastLifeRepository(database,player_database))
    def start(self,*,operation_id,user_id,**kwargs): return self.repository.start(operation_id=operation_id,user_id=user_id,**kwargs)
    def choice(self,*,operation_id,user_id,choice_idx,expected_state,final_state,response): return self.repository.choice(operation_id=operation_id,user_id=user_id,choice_idx=choice_idx,expected_state=expected_state,final_state=final_state,response=response)
    def reset_one(self,*,operation_id,user_id,clear_history=False):
        outcome=self.repository.reset_one(operation_id=operation_id,user_id=user_id,clear_history=clear_history); return SimpleNamespace(status=outcome.status,data=outcome.data,replayed=outcome.status=='duplicate',ok=outcome.succeeded)
__all__=['PastLifeApplication']
