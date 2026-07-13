from __future__ import annotations
import json
from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend

@dataclass(frozen=True)
class ImpartDrawResult:
    status: str
    wish: int = 0
    draw_count: int = 0
    @property
    def succeeded(self): return self.status in {"applied", "duplicate"}

class ImpartDrawService:
    def __init__(self, game_db: str | Path, impart_db: str | Path, lock: RLock | None = None): self.game=Path(game_db); self.impart=Path(impart_db); self.lock=lock or RLock()
    def draw(self, operation_id, user_id, expected_stone, expected_wish, expected_count, cost, new_wish, pulls, cards):
        operation_id,user_id=str(operation_id),str(user_id); expected_stone,expected_wish,expected_count,cost,new_wish,pulls=map(int,(expected_stone,expected_wish,expected_count,cost,new_wish,pulls)); cards=tuple(map(str,cards)); payload=json.dumps([user_id,expected_stone,expected_wish,expected_count,cost,new_wish,pulls,cards],ensure_ascii=False)
        with self.lock, closing(db_backend.connect(self.game)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS impart_data",(str(self.impart),)); conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS impart_draw_operations(operation_id TEXT PRIMARY KEY,payload TEXT,wish INTEGER,draw_count INTEGER)")
                old=conn.execute("SELECT payload,wish,draw_count FROM impart_draw_operations WHERE operation_id=%s",(operation_id,)).fetchone()
                if old: conn.rollback(); return ImpartDrawResult("duplicate" if old[0]==payload else "state_changed",int(old[1]),int(old[2]))
                user=conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s",(user_id,)).fetchone(); state=conn.execute("SELECT wish,impart_num FROM impart_data.xiuxian_impart WHERE user_id=%s",(user_id,)).fetchone()
                if user is None or state is None or int(user[0])!=expected_stone or tuple(map(int,state))!=(expected_wish,expected_count): conn.rollback(); return ImpartDrawResult("state_changed")
                if expected_stone<cost: conn.rollback(); return ImpartDrawResult("stone_missing")
                if expected_count+pulls>100: conn.rollback(); return ImpartDrawResult("limit_exceeded")
                conn.execute("UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s",(cost,user_id)); conn.execute("UPDATE impart_data.xiuxian_impart SET wish=%s,impart_num=impart_num+%s WHERE user_id=%s",(new_wish,pulls,user_id))
                for card_name,quantity in Counter(cards).items(): conn.execute("INSERT INTO impart_data.impart_cards(user_id,card_name,quantity) VALUES (%s,%s,%s) ON CONFLICT(user_id,card_name) DO UPDATE SET quantity=impart_cards.quantity+EXCLUDED.quantity",(user_id,card_name,quantity))
                conn.execute("INSERT INTO impart_draw_operations VALUES (%s,%s,%s,%s)",(operation_id,payload,new_wish,pulls)); conn.commit(); return ImpartDrawResult("applied",new_wish,pulls)
            except Exception: conn.rollback(); raise

__all__=["ImpartDrawResult","ImpartDrawService"]
