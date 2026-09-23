from __future__ import annotations
from pathlib import Path
from .._migrated_application import MigratedFeatureApplication
from .repository import PastLifeRepository
class PastLifeApplication(MigratedFeatureApplication):
    def __init__(self,database:str|Path,player_database:str|Path|None=None,*,repository:PastLifeRepository|None=None)->None: super().__init__(database,feature='past_life',repository=repository or PastLifeRepository(database,player_database))
    def start(self,*,operation_id,user_id,**kwargs): return self.repository.start(operation_id=operation_id,user_id=user_id,**kwargs)
__all__=['PastLifeApplication']
