from __future__ import annotations
import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend

@dataclass(frozen=True)
class PetSkillReplaceResult:
    status: str
    skill_id: str = ""
    @property
    def succeeded(self): return self.status in {"applied", "duplicate"}

class PetSkillReplaceService:
    def __init__(self, player_db: str | Path, lock: RLock | None = None): self.db=Path(player_db); self.lock=lock or RLock()
    def replace(self, operation_id, user_id, uid, expected_skill_id, new_skill_id):
        values=tuple(map(str,(user_id,uid,expected_skill_id,new_skill_id))); operation_id=str(operation_id); payload=json.dumps(values)
        with self.lock, closing(db_backend.connect(self.db)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE"); conn.execute("CREATE TABLE IF NOT EXISTS pet_skill_replace_operations(operation_id TEXT PRIMARY KEY,payload TEXT,skill_id TEXT)")
                old=conn.execute("SELECT payload,skill_id FROM pet_skill_replace_operations WHERE operation_id=%s",(operation_id,)).fetchone()
                if old: conn.rollback(); return PetSkillReplaceResult("duplicate" if old[0]==payload else "state_changed", str(old[1]) if old[0]==payload else "")
                row=conn.execute("SELECT COALESCE(skill_id,'') FROM player_pet_item WHERE user_id=%s AND uid=%s",values[:2]).fetchone()
                if row is None or str(row[0])!=values[2]: conn.rollback(); return PetSkillReplaceResult("state_changed")
                conn.execute("UPDATE player_pet_item SET skill_id=%s,updated_at=strftime('%%s','now') WHERE user_id=%s AND uid=%s",(values[3],values[0],values[1]))
                conn.execute("INSERT INTO pet_skill_replace_operations VALUES (%s,%s,%s)",(operation_id,payload,values[3])); conn.commit(); return PetSkillReplaceResult("applied",values[3])
            except Exception: conn.rollback(); raise

__all__=["PetSkillReplaceResult","PetSkillReplaceService"]
