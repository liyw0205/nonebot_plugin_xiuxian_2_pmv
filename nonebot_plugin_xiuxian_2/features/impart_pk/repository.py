from __future__ import annotations
from pathlib import Path
from .._service_port import ServicePort
from .training_repository import ImpartTrainingSqlRepository

class ImpartPkRepository(ServicePort):
    def __init__(self,database:str|Path,impart_database:str|Path|None=None,player_database:str|Path|None=None)->None:
        super().__init__('impart_pk','nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk'); self.database=str(database); self.impart_database=str(impart_database or database); self.player_database=str(player_database or database)
    def training_settle(self,*args,**kwargs): return ImpartTrainingSqlRepository(self.database,self.impart_database,self.player_database).settle(*args,**kwargs)

__all__=['ImpartPkRepository']
