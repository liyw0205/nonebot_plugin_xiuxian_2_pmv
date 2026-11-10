from __future__ import annotations

from ...core.errors import DomainError
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork
from .migrations import operations_ready

@dataclass(frozen=True)
class ImpartBattleResult:
    status:str; challenger_pk_num:int=0; opponent_pk_num:int|None=None
    reason: str = ""
    subject: str = ""
    @property
    def succeeded(self): return self.status in {'applied','duplicate'}

class ImpartBattleBatchSqlRepository:
    def __init__(self,impart_database:str|Path,player_database:str|Path): self.impart_database,self.player_database=str(impart_database),str(player_database)
    def settle(self,operation_id,challenger_id,expected_challenger_pk_num,challenger_wins,challenger_losses,challenger_stones,opponent_id=None,expected_opponent_pk_num=None,opponent_wins=0,opponent_losses=0,opponent_stones=0):
        operation_id,challenger_id=str(operation_id).strip(),str(challenger_id); opponent_id=None if opponent_id is None else str(opponent_id); vals=tuple(map(int,(expected_challenger_pk_num,challenger_wins,challenger_losses,challenger_stones,opponent_wins,opponent_losses,opponent_stones))); expected_challenger_pk_num,challenger_wins,challenger_losses,challenger_stones,opponent_wins,opponent_losses,opponent_stones=vals; expected_opponent_pk_num=None if expected_opponent_pk_num is None else int(expected_opponent_pk_num); payload=json.dumps([challenger_id,opponent_id],separators=(',',':'))
        if not operation_id or min(vals)<0 or challenger_losses>expected_challenger_pk_num or (opponent_id is None)!=(expected_opponent_pk_num is None) or (expected_opponent_pk_num is not None and opponent_losses>expected_opponent_pk_num): raise ValueError('invalid impart battle batch')
        with DatabaseUnitOfWork(self.impart_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data')
            if not operations_ready(uow,'impart_battle_batch_operations'): return ImpartBattleResult('schema_missing')
            if not operations_ready(uow,'impart_pk_state','player_data'): return ImpartBattleResult('schema_missing')
            old=uow.query_one('SELECT payload,result_json FROM impart_battle_batch_operations WHERE operation_id=?',(operation_id,))
            if old is not None: return ImpartBattleResult('duplicate',*json.loads(old['result_json'])) if str(old['payload'])==payload else ImpartBattleResult('operation_conflict')
            participants=[(challenger_id,expected_challenger_pk_num,challenger_wins,challenger_losses,challenger_stones)]
            if opponent_id is not None: participants.append((opponent_id,expected_opponent_pk_num,opponent_wins,opponent_losses,opponent_stones))
            remaining=[]; has_membership=uow.query_one("SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name='impart_project_members'") is not None
            for uid,expected_pk,wins,losses,stones in participants:
                row=uow.query_one('SELECT pk_num FROM player_data.impart_pk_state WHERE user_id=?',(uid,));
                if row is not None and int(row['pk_num'])!=expected_pk: return ImpartBattleResult('state_changed', reason='pk_count_changed', subject=uid)
                if uow.query_one('SELECT 1 FROM xiuxian_impart WHERE user_id=?',(uid,)) is None: return ImpartBattleResult('user_missing', reason='impart_missing', subject=uid)
            for uid,expected_pk,wins,losses,stones in participants:
                uow.execute('INSERT OR IGNORE INTO player_data.impart_pk_state(user_id,pk_num,win_num) VALUES(?,?,0)',(uid,expected_pk))
                if uow.execute('UPDATE player_data.impart_pk_state SET pk_num=pk_num-?,win_num=win_num+? WHERE user_id=? AND pk_num=? AND pk_num>=?',(losses,wins,uid,expected_pk,losses)).rowcount!=1: raise DomainError("impart_pk_battle_write_rejected", 'battle participant write count mismatch')
                uow.execute('UPDATE xiuxian_impart SET stone_num=COALESCE(stone_num,0)+? WHERE user_id=?',(stones,uid)); remaining_pk=expected_pk-losses; remaining.append(remaining_pk)
                if remaining_pk<=0 and has_membership: uow.execute('DELETE FROM player_data.impart_project_members WHERE user_id=?',(uid,))
            saved=[remaining[0],remaining[1] if len(remaining)>1 else None]; uow.execute('INSERT INTO impart_battle_batch_operations(operation_id,payload,result_json) VALUES(?,?,?)',(operation_id,payload,json.dumps(saved,separators=(',',':')))); return ImpartBattleResult('applied',*saved)
