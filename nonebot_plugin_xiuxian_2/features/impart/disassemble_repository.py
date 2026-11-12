from __future__ import annotations

from ...core.errors import DomainError
from ...core.numeric import crystal_precision_reason
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork
from .card_bonus import refresh_card_bonuses
from .legacy_state import crystal_balance, state_row_count, check_state_write

@dataclass(frozen=True)
class CardDisassembleResult:
    status:str; card_quantity:int=0; stone_quantity:int=0
    reason: str = ""
    subject: str = ""
    @property
    def succeeded(self): return self.status in {'applied','duplicate'}

class ImpartCardDisassembleSqlRepository:
    def __init__(self,database:str|Path): self.database=str(database)
    def disassemble(self,operation_id,user_id,card_name,quantity,expected_card_quantity,expected_stone_quantity,reward_per_card=2,card_definitions=None):
        operation_id,user_id,card_name=str(operation_id).strip(),str(user_id),str(card_name); quantity,expected_card_quantity,expected_stone_quantity,reward_per_card=map(int,(quantity,expected_card_quantity,expected_stone_quantity,reward_per_card))
        if not operation_id or not card_name or quantity<=0 or reward_per_card<=0: raise ValueError('invalid disassemble request')
        payload=json.dumps([user_id,card_name,quantity,reward_per_card],separators=(',',':'))
        with DatabaseUnitOfWork(self.database,immediate=True) as uow:
            if uow.query_one("SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='impart_card_disassemble_operations'") is None:
                return CardDisassembleResult('schema_missing')
            old=uow.query_one('SELECT payload,card_quantity,stone_quantity,stone_quantity_exact,stone_quantity_precision_state FROM impart_card_disassemble_operations WHERE operation_id=?',(operation_id,))
            if old is not None:
                if str(old['payload'])!=payload: return CardDisassembleResult('state_changed',int(old['card_quantity']),int(old['stone_quantity']),reason='operation_conflict')
                if old['stone_quantity_precision_state'] != 'exact':
                    return CardDisassembleResult(
                        'precision_unavailable',
                        reason=crystal_precision_reason(old['stone_quantity_precision_state']),
                        subject=user_id,
                    )
                return CardDisassembleResult('duplicate',int(old['card_quantity']),crystal_balance(old, subject=user_id, field='stone_quantity'))
            card=uow.query_one('SELECT quantity FROM impart_cards WHERE user_id=? AND card_name=?',(user_id,card_name)); state=uow.query_one('SELECT * FROM xiuxian_impart WHERE user_id=? LIMIT 1',(user_id,)); card_quantity=0 if card is None else int(card['quantity'])
            if state is None: return CardDisassembleResult('user_missing',card_quantity)
            stone_quantity=crystal_balance(state, subject=user_id)
            if card_quantity != expected_card_quantity: return CardDisassembleResult('state_changed',card_quantity,stone_quantity,reason='card_changed')
            if stone_quantity != expected_stone_quantity: return CardDisassembleResult('state_changed',card_quantity,stone_quantity,reason='crystal_changed')
            if card_quantity-quantity<1: return CardDisassembleResult('card_missing',card_quantity,stone_quantity)
            state_count = state_row_count(uow, user_id, ("stone_num",))
            if uow.execute('UPDATE impart_cards SET quantity=quantity-? WHERE user_id=? AND card_name=? AND quantity=?',(quantity,user_id,card_name,card_quantity)).rowcount!=1: raise DomainError("card_write_rejected", 'card row count changed inside disassemble transaction')
            new_stone=stone_quantity+quantity*reward_per_card
            check_state_write(uow.execute("UPDATE xiuxian_impart SET stone_num=?,stone_num_exact=?,stone_num_precision_state='exact' WHERE user_id=? AND xiuxian_int_cmp(COALESCE(stone_num_exact,stone_num),?)=0 AND stone_num_precision_state='exact'",(new_stone,str(new_stone),user_id,stone_quantity)), state_count)
            new_card=card_quantity-quantity
            if card_definitions is not None: refresh_card_bonuses(uow.connection,user_id,card_definitions,schema='main')
            uow.execute("INSERT INTO impart_card_disassemble_operations(operation_id,payload,card_quantity,stone_quantity,stone_quantity_exact,stone_quantity_precision_state) VALUES(?,?,?,?,?,'exact')",(operation_id,payload,new_card,new_stone,str(new_stone))); return CardDisassembleResult('applied',new_card,new_stone)

__all__=['ImpartCardDisassembleSqlRepository','CardDisassembleResult']
