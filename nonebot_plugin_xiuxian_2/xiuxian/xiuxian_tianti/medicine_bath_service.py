from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .tianti_data import TiantiDataManager
from .tianti_service import get_active_medicine_bath, settle_tianti_gain


@dataclass(frozen=True)
class MedicineBathResult:
    status: str
    user_id: str
    consumed: tuple[dict, ...]
    effect: float
    bath_name: str
    end_time: str
    settlement: dict
    insufficient: tuple[dict, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MedicineBathService:
    """Consume herbs and activate a medicine bath in one attached transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()
        self._manager = TiantiDataManager()

    @staticmethod
    def _ensure_schema(conn, fields) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tianti_medicine_bath_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, request_json TEXT NOT NULL, "
            "result_json TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute("CREATE TABLE IF NOT EXISTS player_data.tianti_info (user_id TEXT PRIMARY KEY)")
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

    @staticmethod
    def _result_from_payload(status: str, user_id: str, payload: dict) -> MedicineBathResult:
        return MedicineBathResult(
            status=status,
            user_id=user_id,
            consumed=tuple(payload.get("consumed", ())),
            effect=float(payload.get("effect", 0)),
            bath_name=str(payload.get("bath_name", "")),
            end_time=str(payload.get("end_time", "")),
            settlement=dict(payload.get("settlement", {})),
            insufficient=tuple(payload.get("insufficient", ())),
        )

    def get_result(self, operation_id: str) -> MedicineBathResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS tianti_medicine_bath_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, request_json TEXT NOT NULL, "
                "result_json TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT user_id, result_json FROM tianti_medicine_bath_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return self._result_from_payload("duplicate", str(previous[0]), json.loads(previous[1]))

    def apply(self, operation_id, user_id, consume_plan, effect, slot_name,
              now_t: datetime, duration_minutes: int, *, sect_fairyland_level=0):
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        effect = float(effect)
        duration_minutes = int(duration_minutes)
        plan = tuple(
            {"item_id": int(item["item_id"]), "name": str(item["name"]), "amount": int(item["amount"])}
            for item in consume_plan
        )
        if not operation_id or not plan or any(item["amount"] <= 0 for item in plan):
            raise ValueError("operation_id and positive consume plan are required")
        request = {
            "plan": plan,
            "effect": effect,
            "slot_name": str(slot_name),
            "duration_minutes": duration_minutes,
            "sect_fairyland_level": int(sect_fairyland_level),
        }
        now_text = now_t.strftime("%Y-%m-%d %H:%M:%S")
        request_json = json.dumps(request, ensure_ascii=False, sort_keys=True)
        fields = tuple(self._manager._default().keys())

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn, fields)
                previous = conn.execute(
                    "SELECT user_id, request_json, result_json FROM tianti_medicine_bath_operations "
                    "WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    # Request identity = user_id; plan/effect stored in result_json.
                    if str(previous[0]) != user_id:
                        return self._result_from_payload("state_changed", user_id, {})
                    return self._result_from_payload("duplicate", user_id, json.loads(previous[2]))

                row = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM player_data.tianti_info WHERE user_id=%s", (user_id,),
                ).fetchone()
                data = self._manager._clean_user_data(dict(zip(fields, row)) if row else {})
                if get_active_medicine_bath(data, now_t):
                    conn.rollback()
                    return self._result_from_payload("bath_active", user_id, {})

                insufficient = []
                for item in plan:
                    stock = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item["item_id"]),
                    ).fetchone()
                    have = int(stock[0]) if stock else 0
                    if have < item["amount"]:
                        insufficient.append({**item, "have": have})
                if insufficient:
                    conn.rollback()
                    return self._result_from_payload(
                        "item_insufficient", user_id, {"insufficient": insufficient}
                    )

                settlement = settle_tianti_gain(data, now_t, int(sect_fairyland_level))
                if settlement["status"] == "empty":
                    data["last_settle_time"] = now_text
                end_t = now_t + timedelta(minutes=duration_minutes)
                bath_name = f"{slot_name}药浴（" + "、".join(
                    f"{item['name']}x{item['amount']}" for item in plan
                ) + "）"
                data.update({
                    "medicine_last_time": now_text,
                    "medicine_end_time": end_t.strftime("%Y-%m-%d %H:%M:%S"),
                    "medicine_effect": effect,
                    "medicine_name": bath_name,
                })

                for item in plan:
                    consumed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-%s, "
                        "bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s) "
                        "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                        (item["amount"], item["amount"], user_id, item["item_id"], item["amount"]),
                    )
                    if consumed.rowcount != 1:
                        conn.rollback()
                        return self._result_from_payload("item_changed", user_id, {})

                values = [
                    json.dumps(data[field], ensure_ascii=False)
                    if isinstance(data[field], (list, dict)) else data[field]
                    for field in fields
                ]
                columns = ", ".join(["user_id", *(db_backend.quote_ident(field) for field in fields)])
                placeholders = ", ".join(["%s"] * (len(fields) + 1))
                updates = ", ".join(
                    f"{db_backend.quote_ident(field)}=EXCLUDED.{db_backend.quote_ident(field)}"
                    for field in fields
                )
                conn.execute(
                    f"INSERT INTO player_data.tianti_info ({columns}) VALUES ({placeholders}) "
                    f"ON CONFLICT (user_id) DO UPDATE SET {updates}", (user_id, *values),
                )
                payload = {
                    "consumed": plan,
                    "effect": effect,
                    "bath_name": bath_name,
                    "end_time": data["medicine_end_time"],
                    "settlement": settlement,
                }
                conn.execute(
                    "INSERT INTO tianti_medicine_bath_operations "
                    "(operation_id, user_id, request_json, result_json) VALUES (%s, %s, %s, %s)",
                    (operation_id, user_id, request_json,
                     json.dumps(payload, ensure_ascii=False, default=str)),
                )
                conn.commit()
                return self._result_from_payload("applied", user_id, payload)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")


__all__ = ["MedicineBathResult", "MedicineBathService"]
