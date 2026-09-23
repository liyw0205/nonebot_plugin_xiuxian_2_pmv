from __future__ import annotations
from pathlib import Path
from .._service_port import ServicePort
from .training_repository import ImpartTrainingSqlRepository
from .closing_enter_repository import ImpartClosingEnterSqlRepository
from .closing_settlement_repository import ImpartClosingSettlementSqlRepository
from .explore_repository import ImpartExploreSqlRepository
from .battle_repository import ImpartBattleBatchSqlRepository
from .project_join_repository import ImpartProjectJoinSqlRepository
class ImpartPkRepository(ServicePort):
    def __init__(self,database:str|Path,impart_database:str|Path|None=None,player_database:str|Path|None=None)->None: super().__init__('impart_pk','nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk'); self.database=str(database); self.impart_database=str(impart_database or database); self.player_database=str(player_database or database)
    def training_settle(self,*,operation_id,user_id,**kwargs): return ImpartTrainingSqlRepository(self.database,self.impart_database,self.player_database).settle(operation_id,user_id,**kwargs)
    def closing_enter(self,*,operation_id,user_id,started_at): return ImpartClosingEnterSqlRepository(self.database,self.player_database).enter(operation_id,user_id,started_at)
    def closing_settle(self,*,operation_id,user_id,**kwargs): return ImpartClosingSettlementSqlRepository(self.database,self.impart_database,self.player_database).settle(operation_id,user_id,**kwargs)
    def explore_settle(self,*,operation_id,user_id,**kwargs): return ImpartExploreSqlRepository(self.database,self.impart_database,self.player_database).settle(operation_id,user_id,**kwargs)
    def battle_settle(self,*,operation_id,user_id,**kwargs): return ImpartBattleBatchSqlRepository(self.impart_database,self.player_database).settle(operation_id,user_id,**kwargs)
    def project_join(self,*,operation_id,user_id,**kwargs): return ImpartProjectJoinSqlRepository(self.player_database).join(operation_id,user_id,**kwargs)
__all__=['ImpartPkRepository']
