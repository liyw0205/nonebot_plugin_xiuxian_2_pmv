from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .tianti_data import TiantiDataManager
from .tianti_service import grant_tianti_settle_minutes


@dataclass(frozen=True)
class TiantiItemRewardResult:
    status: str
    user_id: str
    item_id: int
    quantity: int
    minutes: int
    detail: dict

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class TiantiItemRewardService:
    """Consume an item and update tianti state across attached SQLite databases."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()
        self._manager = TiantiDataManager()

    @staticmethod
    def _ensure_schema(conn, fields) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tianti_item_reward_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, item_id INTEGER NOT NULL, "
            "quantity INTEGER NOT NULL, minutes INTEGER NOT NULL, detail_json TEXT NOT NULL, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.tianti_info (user_id TEXT PRIMARY KEY)"
        )
        columns = {
            str(row[1]) for row in conn.execute(
                "PRAGMA player_data.table_info(tianti_info)"
            ).fetchall()
        }
        for field in fields:
            if field not in columns:
                conn.execute(
                    f"ALTER TABLE player_data.tianti_info ADD COLUMN {db_backend.quote_ident(field)} TEXT"
                )

    def apply(self, operation_id, user_id, item_id, quantity, minutes,
              *, sect_fairyland_level=0) -> TiantiItemRewardResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        minutes = int(minutes)
        if not operation_id or quantity <= 0 or minutes <= 0:
            raise ValueError("operation_id, quantity and minutes must be positive")
        total_minutes = quantity * minutes

        def result(status, detail=None, result_quantity=quantity, result_minutes=total_minutes):
            return TiantiItemRewardResult(
                status, user_id, item_id, int(result_quantity), int(result_minutes), detail or {}
            )

        fields = tuple(self._manager._default().keys())
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn, fields)
                previous = conn.execute(
                    "SELECT user_id, item_id, quantity, minutes, detail_json "
                    "FROM tianti_item_reward_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if (
                        str(previous[0]) != user_id
                        or int(previous[1]) != item_id
                        or int(previous[2]) != quantity
                        or int(previous[3]) != total_minutes
                    ):
                        return result("state_changed")
                    return result("duplicate", json.loads(previous[4]), previous[2], previous[3])

                row = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM player_data.tianti_info WHERE user_id=%s", (user_id,)
                ).fetchone()
                raw = dict(zip(fields, row)) if row else {}
                data = self._manager._clean_user_data(raw)
                detail = grant_tianti_settle_minutes(
                    data, total_minutes, sect_fairyland_level=sect_fairyland_level
                )
                consumed = conn.execute(
                    "UPDATE back SET goods_num=goods_num-%s, "
                    "bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s) "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (quantity, quantity, user_id, item_id, quantity),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return result("item_insufficient")

                values = []
                for field in fields:
                    value = data[field]
                    values.append(json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict)) else value)
                columns = ", ".join(["user_id", *(db_backend.quote_ident(field) for field in fields)])
                placeholders = ", ".join(["%s"] * (len(fields) + 1))
                updates = ", ".join(
                    f"{db_backend.quote_ident(field)}=EXCLUDED.{db_backend.quote_ident(field)}"
                    for field in fields
                )
                conn.execute(
                    f"INSERT INTO player_data.tianti_info ({columns}) VALUES ({placeholders}) "
                    f"ON CONFLICT (user_id) DO UPDATE SET {updates}",
                    (user_id, *values),
                )
                conn.execute(
                    "INSERT INTO tianti_item_reward_operations "
                    "(operation_id, user_id, item_id, quantity, minutes, detail_json) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, item_id, quantity, total_minutes,
                     json.dumps(detail, ensure_ascii=False, default=str)),
                )
                conn.commit()
                return result("applied", detail)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")


__all__ = ["TiantiItemRewardResult", "TiantiItemRewardService"]
