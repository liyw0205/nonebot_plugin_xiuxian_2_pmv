from __future__ import annotations
import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend

FIELDS={"main_buff":("retrieved_main","main_buff"),"sub_buff":("retrieved_sub","sub_buff"),"sec_buff":("retrieved_sec","sec_buff"),"effect1_buff":("retrieved_effect1","effect1_buff"),"effect2_buff":("retrieved_effect2","effect2_buff")}
@dataclass(frozen=True)
class LunhuiRecallResult:
    status: str
    skill_id: int = 0
    @property
    def succeeded(self): return self.status in {"applied","duplicate"}

class LunhuiRecallService:
    def __init__(self, game_db: str | Path, player_db: str | Path, lock: RLock | None = None): self.game=Path(game_db); self.player=Path(player_db); self.lock=lock or RLock()
    def recall(self, operation_id, user_id, skill_type, expected_skill_id):
        operation_id,user_id,skill_type=str(operation_id),str(user_id),str(skill_type); expected_skill_id=int(expected_skill_id); payload=json.dumps([user_id,skill_type,expected_skill_id]); fields=FIELDS.get(skill_type)
        if not fields: return LunhuiRecallResult("invalid_type")
        retrieved_field,buff_field=fields
        with self.lock, closing(db_backend.connect(self.game)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS player_data",(str(self.player),)); conn.execute("BEGIN IMMEDIATE"); conn.execute("CREATE TABLE IF NOT EXISTS lunhui_recall_operations(operation_id TEXT PRIMARY KEY,payload TEXT,skill_id INTEGER)")
                old=conn.execute("SELECT payload,skill_id FROM lunhui_recall_operations WHERE operation_id=%s",(operation_id,)).fetchone()
                if old: conn.rollback(); return LunhuiRecallResult("duplicate" if old[0]==payload else "state_changed",int(old[1]) if old[0]==payload else 0)
                memory=conn.execute(f"SELECT {skill_type},{retrieved_field} FROM player_data.reincarnation_memory WHERE user_id=%s",(user_id,)).fetchone()
                if memory is None or int(memory[0] or 0)!=expected_skill_id or int(memory[1] or 0): conn.rollback(); return LunhuiRecallResult("state_changed")
                buff=conn.execute("SELECT 1 FROM BuffInfo WHERE user_id=%s",(user_id,)).fetchone()
                if buff is None: conn.rollback(); return LunhuiRecallResult("state_changed")
                conn.execute(f"UPDATE player_data.reincarnation_memory SET {retrieved_field}=1 WHERE user_id=%s",(user_id,)); conn.execute(f"UPDATE BuffInfo SET {buff_field}=%s WHERE user_id=%s",(expected_skill_id,user_id)); conn.execute("INSERT INTO lunhui_recall_operations VALUES (%s,%s,%s)",(operation_id,payload,expected_skill_id)); conn.commit(); return LunhuiRecallResult("applied",expected_skill_id)
            except Exception: conn.rollback(); raise

__all__=["LunhuiRecallResult","LunhuiRecallService"]
