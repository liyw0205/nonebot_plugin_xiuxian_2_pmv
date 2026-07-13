from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class SectElixirClaimResult:
    status: str
    rewards: tuple[tuple[int, str, str, int], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class SectElixirClaimService:
    """Grant the complete daily elixir-room reward and mark it claimed."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def claim(self, operation_id, user_id, sect_id, contribution_required, materials_required, rewards, max_goods_num):
        operation_id = str(operation_id).strip()
        user_id, sect_id = str(user_id), int(sect_id)
        contribution_required, materials_required = int(contribution_required), int(materials_required)
        max_goods_num = int(max_goods_num)
        normalized = tuple((int(item_id), str(name), str(item_type), int(quantity)) for item_id, name, item_type, quantity in rewards)
        if not operation_id or not normalized or any(item_id < 0 or quantity <= 0 for item_id, _, _, quantity in normalized):
            raise ValueError("valid operation and rewards are required")
        payload = json.dumps([user_id, sect_id, contribution_required, materials_required, normalized], ensure_ascii=True)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS sect_elixir_claim_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, rewards TEXT NOT NULL, "
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, rewards FROM sect_elixir_claim_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous:
                    conn.rollback()
                    return SectElixirClaimResult("duplicate", tuple(tuple(item) for item in json.loads(previous[1])))

                user = conn.execute(
                    "SELECT sect_id, sect_position, COALESCE(sect_contribution, 0), COALESCE(sect_elixir_get, 0) "
                    "FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                sect = conn.execute(
                    "SELECT COALESCE(elixir_room_level, 0), COALESCE(sect_materials, 0) FROM sects WHERE sect_id=%s",
                    (sect_id,),
                ).fetchone()
                if user is None or sect is None or int(user[0] or 0) != sect_id:
                    conn.rollback()
                    return SectElixirClaimResult("membership_changed")
                if int(user[1] if user[1] is not None else 15) == 15:
                    conn.rollback()
                    return SectElixirClaimResult("position_ineligible")
                if int(sect[0]) <= 0:
                    conn.rollback()
                    return SectElixirClaimResult("room_missing")
                if int(user[2]) < contribution_required:
                    conn.rollback()
                    return SectElixirClaimResult("contribution_insufficient")
                if int(sect[1]) < materials_required:
                    conn.rollback()
                    return SectElixirClaimResult("materials_insufficient")
                if int(user[3]) == 1:
                    conn.rollback()
                    return SectElixirClaimResult("already_claimed")

                for item_id, _, _, quantity in normalized:
                    row = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                    ).fetchone()
                    if (int(row[0]) if row else 0) + quantity > max_goods_num:
                        conn.rollback()
                        return SectElixirClaimResult("inventory_full")

                now = datetime.now()
                for item_id, name, item_type, quantity in normalized:
                    conn.execute(
                        "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, create_time, update_time, bind_num) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, update_time=EXCLUDED.update_time, "
                        "goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num, 0)+EXCLUDED.goods_num",
                        (user_id, item_id, name, item_type, quantity, now, now, quantity),
                    )
                if conn.execute(
                    "UPDATE user_xiuxian SET sect_elixir_get=1 WHERE user_id=%s AND COALESCE(sect_elixir_get, 0)=0", (user_id,)
                ).rowcount != 1:
                    conn.rollback()
                    return SectElixirClaimResult("already_claimed")
                conn.execute(
                    "INSERT INTO sect_elixir_claim_operations (operation_id, payload, rewards) VALUES (%s, %s, %s)",
                    (operation_id, payload, json.dumps(normalized, ensure_ascii=True)),
                )
                conn.commit()
                return SectElixirClaimResult("applied", normalized)
            except Exception:
                conn.rollback()
                raise


__all__ = ["SectElixirClaimResult", "SectElixirClaimService"]
