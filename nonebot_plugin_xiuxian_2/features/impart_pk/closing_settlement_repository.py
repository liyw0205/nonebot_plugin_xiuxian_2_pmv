from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

@dataclass(frozen=True)
class ImpartClosingSettlementResult:
    status:str; exp_gain:int=0; blessing_cost:int=0; exp_day_remaining:int=0
    @property
    def succeeded(self): return self.status in {'applied','duplicate'}

class ImpartClosingSettlementSqlRepository:
    def __init__(self,game_database:str|Path,impart_database:str|Path,player_database:str|Path): self.game_database,self.impart_database,self.player_database=map(str,(game_database,impart_database,player_database))
    def settle(self,operation_id,user_id,expected_create_time,expected_exp,expected_exp_day,exp_gain,blessing_cost,closing_minutes,hp,mp,atk,power):
        operation_id,user_id=str(operation_id).strip(),str(user_id); expected_create_time=str(expected_create_time); expected_exp,expected_exp_day,exp_gain,blessing_cost,closing_minutes,hp,mp,atk,power=map(int,(expected_exp,expected_exp_day,exp_gain,blessing_cost,closing_minutes,hp,mp,atk,power)); payload=json.dumps([user_id,expected_create_time],separators=(',',':'))
        if not operation_id or expected_exp<0 or expected_exp_day<0 or blessing_cost<0 or closing_minutes<0 or blessing_cost>expected_exp_day: raise ValueError('invalid impart closing settlement')
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.impart_database,'impart_data'); uow.attach_database(self.player_database,'player_data'); uow.execute('CREATE TABLE IF NOT EXISTS impart_closing_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'); old=uow.query_one('SELECT payload,result_json FROM impart_closing_operations WHERE operation_id=?',(operation_id,))
            if old is not None: return ImpartClosingSettlementResult('duplicate',*json.loads(old['result_json'])) if str(old['payload'])==payload else ImpartClosingSettlementResult('operation_conflict')
            user=uow.query_one('SELECT exp FROM user_xiuxian WHERE user_id=?',(user_id,)); cd=uow.query_one('SELECT type,create_time FROM user_cd WHERE user_id=?',(user_id,)); impart=uow.query_one('SELECT COALESCE(exp_day,0) AS exp_day FROM impart_data.xiuxian_impart WHERE user_id=?',(user_id,))
            if user is None or cd is None or impart is None: return ImpartClosingSettlementResult('user_missing')
            if int(cd['type'] or 0)!=4 or str(cd['create_time']) not in {expected_create_time,'0',''} or int(impart['exp_day'] or 0)!=expected_exp_day: return ImpartClosingSettlementResult('state_changed')
            if uow.execute('UPDATE user_xiuxian SET exp=COALESCE(exp,0)+?,hp=?,mp=?,atk=?,power=? WHERE user_id=?',(exp_gain,hp,mp,atk,power,user_id)).rowcount!=1: return ImpartClosingSettlementResult('state_changed')
            if uow.execute('UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=? AND type=4',(user_id,)).rowcount!=1: return ImpartClosingSettlementResult('state_changed')
            if uow.execute('UPDATE impart_data.xiuxian_impart SET exp_day=exp_day-? WHERE user_id=? AND exp_day=? AND exp_day>=?',(blessing_cost,user_id,expected_exp_day,blessing_cost)).rowcount!=1: return ImpartClosingSettlementResult('state_changed')
            fields=('虚神界闭关时长','虚神界闭关修为','虚神界闭关祝福时长'); uow.execute('CREATE TABLE IF NOT EXISTS player_data.statistics(user_id TEXT PRIMARY KEY)'); columns={str(x['name']) for x in uow.query_all('PRAGMA player_data.table_info(statistics)')}
            for field,amount in zip(fields,(closing_minutes,exp_gain,blessing_cost)):
                if field not in columns: uow.execute(f'ALTER TABLE player_data.statistics ADD COLUMN "{field}" INTEGER DEFAULT 0')
                if uow.execute(f'UPDATE player_data.statistics SET "{field}"=COALESCE("{field}",0)+? WHERE user_id=?',(amount,user_id)).rowcount==0: uow.execute(f'INSERT INTO player_data.statistics(user_id,"{field}") VALUES(?,?)',(user_id,amount))
            saved=[exp_gain,blessing_cost,expected_exp_day-blessing_cost]; uow.execute('INSERT INTO impart_closing_operations(operation_id,payload,result_json) VALUES(?,?,?)',(operation_id,payload,json.dumps(saved,separators=(',',':')))); return ImpartClosingSettlementResult('applied',*saved)
