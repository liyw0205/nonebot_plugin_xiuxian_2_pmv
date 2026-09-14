from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json
from typing import Any, Callable, Mapping, Protocol

from ...features.tianti_training.repository import TiantiProfileReader
from ...features.tianti_training.repository import _parse_time, _sect_bonus
from ...features.tianti_training.domain import decide_tianti_gain, decide_tianti_settlement_window
from ...infrastructure.database import DatabaseUnitOfWork


class TiantiProfile(Protocol):
    def default_data(self) -> dict[str, Any]: ...
    def levels(self) -> Mapping[str, Mapping[str, Any]]: ...
    def clean(self, row: Mapping[str, Any] | None) -> dict[str, Any]: ...
    def cap(self, data: Mapping[str, Any]) -> int: ...


class TiantiSettlementRepository(Protocol):
    def settle(self, operation_id: str, user_id: str, settled_at: datetime, *, sect_fairyland_level: int = 0) -> Any: ...


class TiantiSettlementSqlRepository:
    """Feature-owned elapsed-gain settlement on player.db."""

    def __init__(self, player_database: str, *, profile_reader: TiantiProfile | None = None, spirit_vein_multiplier: Callable[[], float] | None = None) -> None:
        self.player_database = str(player_database)
        self.profile = profile_reader or TiantiProfileReader(Path(player_database).parent / "xiuxian")
        self.spirit_vein_multiplier = spirit_vein_multiplier or (lambda: 1.0)

    def settle(self, operation_id: str, user_id: str, settled_at: datetime, *, sect_fairyland_level: int = 0) -> Any:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        if not operation_id:
            raise ValueError("operation_id is required")
        fields = tuple(self.profile.default_data().keys())
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            tables = {str(row["name"]) for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")}
            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(tianti_info)")}
            if "tianti_settlement_operations" not in tables or "tianti_info" not in tables or not set(fields).issubset(columns):
                raise RuntimeError("tianti settlement schema is not ready; run migrations first")
            previous = uow.query_one("SELECT user_id, sect_level, detail_json FROM tianti_settlement_operations WHERE operation_id=?", (operation_id,))
            if previous:
                if str(previous["user_id"]) != user_id or int(previous["sect_level"]) != int(sect_fairyland_level):
                    return {"status": "state_changed", "user_id": user_id, "detail": {}}
                return {"status": "duplicate", "user_id": user_id, "detail": json.loads(previous["detail_json"])}
            row = uow.query_one("SELECT * FROM tianti_info WHERE user_id=?", (user_id,))
            if row is None:
                return {"status": "user_missing", "user_id": user_id, "detail": {}}
            data = self.profile.clean(row)
            for field in ("opened_qiaoxue", "opened_qiaoxue_detail", "qiaoxue_stage_opened"):
                if isinstance(data.get(field), str):
                    try:
                        data[field] = json.loads(data[field])
                    except json.JSONDecodeError:
                        data[field] = [] if field != "qiaoxue_stage_opened" else {}
            window = decide_tianti_settlement_window(last_settlement=_parse_time(data.get("last_settle_time")), now=settled_at)
            detail: dict[str, Any] = {"status": window.status, "mins": window.minutes}
            if window.status == "settle":
                level = self.profile.levels()[str(data["tianti_level"])]
                details = list(data.get("opened_qiaoxue_detail", []) or [])
                base_ratio = sum(float(item.get("effect_value", 0)) for item in details if item.get("effect_type") == "base_per_min_ratio")
                gain_pct = sum(float(item.get("effect_value", 0)) for item in details if item.get("effect_type") == "hp_gain_pct")
                bath_end = _parse_time(data.get("medicine_end_time"))
                bath_effect = float(data.get("medicine_effect", 0) or 0) if bath_end and settled_at <= bath_end else 1.0
                gain = decide_tianti_gain(minutes=window.minutes, base_per_min=int(level.get("hp_gain_per_min", 0)), base_ratio=base_ratio, gain_pct=gain_pct, bath_effect=bath_effect if bath_effect > 1 else 1.0, sect_bonus=_sect_bonus(sect_fairyland_level), spirit_vein_multiplier=float(self.spirit_vein_multiplier()), old_hp=int(data["tianti_hp"]), hp_cap=self.profile.cap(data))
                data["tianti_hp"] = gain.new_hp
                detail.update({"real_gain": gain.real_gain, "new_hp": gain.new_hp})
            data["last_settle_time"] = settled_at.strftime("%Y-%m-%d %H:%M:%S")
            values = [json.dumps(data[field], ensure_ascii=False) if isinstance(data[field], (list, dict)) else data[field] for field in fields]
            columns_sql = ", ".join(["user_id", *fields])
            placeholders = ", ".join("?" for _ in range(len(values) + 1))
            updates = ", ".join(f'"{field}"=excluded."{field}"' for field in fields)
            uow.execute(f'INSERT INTO tianti_info ({columns_sql}) VALUES ({placeholders}) ON CONFLICT(user_id) DO UPDATE SET {updates}', (user_id, *values))
            uow.execute("INSERT INTO tianti_settlement_operations(operation_id,user_id,sect_level,result_status,detail_json) VALUES(?,?,?,?,?)", (operation_id, user_id, int(sect_fairyland_level), str(detail["status"]), json.dumps(detail, ensure_ascii=False, default=str)))
            return {"status": "settled", "user_id": user_id, "detail": detail}


class LegacyTiantiSettlementRepository:
    """Lazy adapter around the historical tianti player-db transaction."""

    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    def settle(self, operation_id: str, user_id: str, settled_at: datetime, *, sect_fairyland_level: int = 0) -> Any:
        try:
            from ...xiuxian.xiuxian_tianti.transaction_service import TiantiSettlementService
        except (ImportError, RuntimeError, ValueError):
            return {"status": "not_ready", "detail": {}}
        return TiantiSettlementService(self.player_database).settle(
            operation_id,
            user_id,
            settled_at,
            sect_fairyland_level=sect_fairyland_level,
        )


__all__ = ["LegacyTiantiSettlementRepository", "TiantiSettlementRepository", "TiantiSettlementSqlRepository"]
