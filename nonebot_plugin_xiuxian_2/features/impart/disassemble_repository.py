from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork
from .card_bonus import refresh_card_bonuses

@dataclass(frozen=True)
class CardDisassembleResult:
    status:str; card_quantity:int=0; stone_quantity:int=0
    @property
    def succeeded(self): return self.status in {'applied','duplicate'}

class ImpartCardDisassembleSqlRepository:
    def __init__(self,database:str|Path): self.database=str(database)
    def disassemble(self,operation_id,user_id,card_name,quantity,expected_card_quantity,expected_stone_quantity,reward_per_card=2,card_definitions=None):
        operation_id,user_id,card_name=str(operation_id).strip(),str(user_id),str(card_name); quantity,expected_card_quantity,expected_stone_quantity,reward_per_card=map(int,(quantity,expected_card_quantity,expected_stone_quantity,reward_per_card))
        if not operation_id or not card_name or quantity<=0 or reward_per_card<=0: raise ValueError('invalid disassemble request')
        payload=json.dumps([user_id,card_name,quantity,reward_per_card],separators=(',',':'))
        with DatabaseUnitOfWork(self.database,immediate=True) as uow:
            uow.execute('CREATE TABLE IF NOT EXISTS impart_card_disassemble_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,card_quantity INTEGER NOT NULL,stone_quantity INTEGER NOT NULL)')
            old=uow.query_one('SELECT payload,card_quantity,stone_quantity FROM impart_card_disassemble_operations WHERE operation_id=?',(operation_id,))
            if old is not None:
                if str(old['payload'])!=payload: return CardDisassembleResult('state_changed',int(old['card_quantity']),int(old['stone_quantity']))
                return CardDisassembleResult('duplicate',int(old['card_quantity']),int(old['stone_quantity']))
            card=uow.query_one('SELECT quantity FROM impart_cards WHERE user_id=? AND card_name=?',(user_id,card_name)); state=uow.query_one('SELECT stone_num FROM xiuxian_impart WHERE user_id=?',(user_id,)); card_quantity=0 if card is None else int(card['quantity'])
            if state is None: return CardDisassembleResult('user_missing',card_quantity)
            stone_quantity=int(state['stone_num'] or 0)
            if (card_quantity,stone_quantity)!=(expected_card_quantity,expected_stone_quantity): return CardDisassembleResult('state_changed',card_quantity,stone_quantity)
            if card_quantity-quantity<1: return CardDisassembleResult('card_missing',card_quantity,stone_quantity)
            if uow.execute('UPDATE impart_cards SET quantity=quantity-? WHERE user_id=? AND card_name=? AND quantity=?',(quantity,user_id,card_name,card_quantity)).rowcount!=1: return CardDisassembleResult('state_changed')
            new_stone=stone_quantity+quantity*reward_per_card
            if uow.execute('UPDATE xiuxian_impart SET stone_num=? WHERE user_id=? AND stone_num=?',(new_stone,user_id,stone_quantity)).rowcount!=1: return CardDisassembleResult('state_changed')
            new_card=card_quantity-quantity
            if card_definitions is not None: refresh_card_bonuses(uow.connection,user_id,card_definitions,schema='main')
            uow.execute('INSERT INTO impart_card_disassemble_operations VALUES(?,?,?,?)',(operation_id,payload,new_card,new_stone)); return CardDisassembleResult('applied',new_card,new_stone)

__all__=['ImpartCardDisassembleSqlRepository','CardDisassembleResult']
