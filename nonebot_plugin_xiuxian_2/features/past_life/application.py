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
    def reset_all_create(self,*,operation_id,user_id,clear_history=False):
        outcome=self.repository.reset_all_create(operation_id=operation_id,clear_history=clear_history)
        return SimpleNamespace(status=outcome.status,data=outcome.data,operation_id=outcome.operation_id,
                                clear_history=outcome.clear_history,total=outcome.total,processed=outcome.processed,
                                applied=outcome.applied,conflicted=outcome.conflicted,missing=outcome.missing,
                                complete=outcome.complete,succeeded=outcome.succeeded)
    def reset_all_batch(self,*,operation_id,user_id,batch_size=500):
        outcome=self.repository.reset_all_batch(operation_id=operation_id,batch_size=batch_size)
        return SimpleNamespace(status=outcome.status,data=outcome.data,operation_id=outcome.operation_id,
                                clear_history=outcome.clear_history,total=outcome.total,processed=outcome.processed,
                                applied=outcome.applied,conflicted=outcome.conflicted,missing=outcome.missing,
                                last_error=outcome.last_error,complete=outcome.complete,succeeded=outcome.succeeded)
    def reset_all_pending(self):
        outcome=self.repository.reset_all_pending()
        if outcome is None:
            return None
        return SimpleNamespace(status=outcome.status,data=outcome.data,operation_id=outcome.operation_id,
                               clear_history=outcome.clear_history,total=outcome.total,processed=outcome.processed,
                               applied=outcome.applied,conflicted=outcome.conflicted,missing=outcome.missing,
                               last_error=outcome.last_error,complete=outcome.complete,succeeded=outcome.succeeded)
    def final_settle(self,*,operation_id,user_id,**kwargs):
        outcome=self.repository.final_settle(operation_id=operation_id,user_id=user_id,**kwargs); return SimpleNamespace(status=outcome.status,rewards=outcome.rewards,succeeded=outcome.succeeded)
__all__=['PastLifeApplication']
