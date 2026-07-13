from __future__ import annotations

import json
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PetFusionBreakthroughResult:
    status: str
    stars: int = 0
    exp: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PetFusionBreakthroughService:
    """Consume fixed pet materials and promote the active pet atomically."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(player_database)
        self._lock = lock or RLock()

    def breakthrough(
        self,
        operation_id,
        user_id,
        expected_main,
        expected_materials,
        updated_stars,
        updated_exp,
        skill_offer=None,
    ) -> PetFusionBreakthroughResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        main = tuple(expected_main)
        materials = tuple(sorted(tuple(row) for row in expected_materials))
        updated_stars, updated_exp = int(updated_stars), int(updated_exp)
        if not operation_id or len(main) != 7 or not materials:
            raise ValueError("operation, main snapshot and materials are required")
        if any(len(row) != 7 for row in materials) or len({str(row[0]) for row in materials}) != len(materials):
            raise ValueError("unique complete material snapshots are required")
        payload = json.dumps(
            [user_id, main, materials, updated_stars, updated_exp, skill_offer],
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS pet_fusion_breakthrough_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stars INTEGER NOT NULL,"
                    "exp INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,stars,exp FROM pet_fusion_breakthrough_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return PetFusionBreakthroughResult("state_changed")
                    return PetFusionBreakthroughResult("duplicate", int(old[1]), int(old[2]))

                current_main = conn.execute(
                    "SELECT uid,pet_id,stars,exp,total_exp,COALESCE(skill_id,''),is_active "
                    "FROM player_pet_item WHERE user_id=%s AND uid=%s",
                    (user_id, str(main[0])),
                ).fetchone()
                if current_main is None or tuple(current_main) != main or int(current_main[6]) != 1:
                    conn.rollback()
                    return PetFusionBreakthroughResult("state_changed")

                placeholders = ",".join(["%s"] * len(materials))
                material_uids = tuple(str(row[0]) for row in materials)
                rows = conn.execute(
                    "SELECT uid,pet_id,stars,exp,total_exp,COALESCE(skill_id,''),is_active "
                    f"FROM player_pet_item WHERE user_id=%s AND uid IN ({placeholders})",
                    (user_id, *material_uids),
                ).fetchall()
                current_materials = tuple(sorted(tuple(row) for row in rows))
                if current_materials != materials or any(int(row[6]) for row in current_materials):
                    conn.rollback()
                    return PetFusionBreakthroughResult("state_changed")

                deleted = conn.execute(
                    f"DELETE FROM player_pet_item WHERE user_id=%s AND uid IN ({placeholders})",
                    (user_id, *material_uids),
                ).rowcount
                if deleted != len(materials):
                    conn.rollback()
                    return PetFusionBreakthroughResult("state_changed")
                updated = conn.execute(
                    "UPDATE player_pet_item SET stars=%s,exp=%s,updated_at=%s "
                    "WHERE user_id=%s AND uid=%s AND stars=%s AND exp=%s AND is_active=1",
                    (updated_stars, updated_exp, int(time.time()), user_id, str(main[0]), int(main[2]), int(main[3])),
                ).rowcount
                if updated != 1:
                    conn.rollback()
                    return PetFusionBreakthroughResult("state_changed")
                conn.execute(
                    "INSERT INTO pet_fusion_breakthrough_operations(operation_id,payload,stars,exp) "
                    "VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, updated_stars, updated_exp),
                )
                conn.commit()
                return PetFusionBreakthroughResult("applied", updated_stars, updated_exp)
            except Exception:
                conn.rollback()
                raise


__all__ = ["PetFusionBreakthroughResult", "PetFusionBreakthroughService"]
