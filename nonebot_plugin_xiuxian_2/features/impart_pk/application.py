from __future__ import annotations
from pathlib import Path
from .._migrated_application import MigratedFeatureApplication
from .repository import ImpartPkRepository
class ImpartPkApplication(MigratedFeatureApplication):
    def __init__(self,database:str|Path,impart_database:str|Path|None=None,player_database:str|Path|None=None,*,repository:ImpartPkRepository|None=None)->None: super().__init__(database,feature='impart_pk',repository=repository or ImpartPkRepository(database,impart_database,player_database))
    def training_settle(self, *, operation_id: str, user_id: str, **kwargs): return self.repository.training_settle(operation_id=operation_id,user_id=user_id,**kwargs)
    def closing_enter(self, *, operation_id: str, user_id: str, started_at: str): return self.repository.closing_enter(operation_id=operation_id,user_id=user_id,started_at=started_at)
    def closing_settle(self, *, operation_id: str, user_id: str, **kwargs): return self.repository.closing_settle(operation_id=operation_id,user_id=user_id,**kwargs)
    def explore_settle(self, *, operation_id: str, user_id: str, **kwargs): return self.repository.explore_settle(operation_id=operation_id,user_id=user_id,**kwargs)
    def battle_settle(self, *, operation_id: str, user_id: str, **kwargs): return self.repository.battle_settle(operation_id=operation_id,user_id=user_id,**kwargs)
__all__=['ImpartPkApplication']
