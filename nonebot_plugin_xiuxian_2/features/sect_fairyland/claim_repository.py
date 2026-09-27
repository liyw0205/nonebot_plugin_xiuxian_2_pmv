from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork
from ...features.tianti_training.domain import decide_tianti_gain
from ...features.tianti_training.repository import TiantiProfileReader


class TiantiProfile(Protocol):
    def default_data(self) -> dict[str, Any]: ...
    def levels(self) -> Mapping[str, Mapping[str, Any]]: ...
    def clean(self, row: Mapping[str, Any] | None) -> dict[str, Any]: ...
    def cap(self, data: Mapping[str, Any]) -> int: ...


@dataclass(frozen=True)
class SectFairylandClaimResult:
    status: str
    user_id: str
    sect_id: str
    day: str
    level: int
    minutes: int
    detail: dict[str, Any]

    @property
    def succeeded(self) -> bool:
        return self.status in {"claimed", "duplicate"}


class SectFairylandSqlRepository:
    _OPERATION_COLUMNS = {
        "operation_id", "user_id", "sect_id", "claim_day", "level", "minutes", "detail_json", "created_at",
    }
    _CLAIM_COLUMNS = {"user_id", "sect_id", "claim_day", "updated_at"}

    def __init__(
        self,
        player_database: str | Path,
        *,
        profile_reader: TiantiProfile | None = None,
        clock: Any | None = None,
        spirit_vein_multiplier: Callable[[], float] | None = None,
    ) -> None:
        self.player_database = str(player_database)
        self.profile = profile_reader or TiantiProfileReader(Path(player_database).parent / "xiuxian")
        self.clock = clock or SystemClock()
        self.spirit_vein_multiplier = spirit_vein_multiplier or (lambda: 1.0)

    @staticmethod
    def _identifier(value: str) -> str:
        return '"' + str(value).replace('"', '""') + '"'

    @staticmethod
    def _parse_time(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value
        if not value:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(str(value), fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _decode_profile(data: dict[str, Any]) -> dict[str, Any]:
        for field, expected in (
            ("opened_qiaoxue", list),
            ("opened_qiaoxue_detail", list),
            ("qiaoxue_stage_opened", dict),
        ):
            value = data.get(field)
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    value = None
            data[field] = value if isinstance(value, expected) else expected()
        try:
            data["medicine_effect"] = float(data.get("medicine_effect", 0) or 0)
        except (TypeError, ValueError):
            data["medicine_effect"] = 0.0
        return data

    def _schema_ready(self, uow: DatabaseUnitOfWork) -> bool:
        tables = {
            str(row["name"])
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if not {"sect_fairyland_claim_operations", "sect_fairyland_claim_days", "tianti_info"}.issubset(tables):
            return False
        operation_columns = {
            str(row["name"]) for row in uow.query_all("PRAGMA table_info(sect_fairyland_claim_operations)")
        }
        claim_columns = {
            str(row["name"]) for row in uow.query_all("PRAGMA table_info(sect_fairyland_claim_days)")
        }
        profile_columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(tianti_info)")}
        return (
            self._OPERATION_COLUMNS.issubset(operation_columns)
            and self._CLAIM_COLUMNS.issubset(claim_columns)
            and set(self.profile.default_data()).issubset(profile_columns)
        )

    def _write_profile(self, uow: DatabaseUnitOfWork, user_id: str, data: Mapping[str, Any]) -> None:
        fields = tuple(self.profile.default_data())
        columns = ["user_id", *fields]
        quoted = ",".join(self._identifier(field) for field in columns)
        placeholders = ",".join("?" for _ in columns)
        updates = ",".join(
            f"{self._identifier(field)}=excluded.{self._identifier(field)}" for field in fields
        )
        values = [
            json.dumps(data[field], ensure_ascii=False) if isinstance(data[field], (list, dict)) else data[field]
            for field in fields
        ]
        uow.execute(
            f"INSERT INTO tianti_info ({quoted}) VALUES ({placeholders}) "
            f"ON CONFLICT(user_id) DO UPDATE SET {updates}",
            (user_id, *values),
        )

    def _write_legacy_projection(self, uow: DatabaseUnitOfWork, user_id: str, sect_id: str, day: str) -> None:
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='sect_fairyland_claim'"
        )
        if table is None:
            return
        field = f"last_claim_{sect_id}"
        columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(sect_fairyland_claim)")}
        if field not in columns:
            return
        quoted = self._identifier(field)
        uow.execute(
            f"INSERT INTO sect_fairyland_claim(user_id,{quoted}) VALUES(?,?) "
            f"ON CONFLICT(user_id) DO UPDATE SET {quoted}=excluded.{quoted}",
            (user_id, day),
        )

    def claim(self, operation_id: str, user_id: str, sect_id: str, day: str, level: int, minutes: int) -> SectFairylandClaimResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        sect_id, day, level, minutes = str(sect_id).strip(), str(day).strip(), int(level), int(minutes)
        if not operation_id or not user_id or not sect_id or not day or level <= 0 or minutes <= 0:
            raise ValueError("operation_id, user_id, sect_id, day, level and minutes must be valid")

        def result(status: str, detail: dict[str, Any] | None = None, result_level: int = level, result_minutes: int = minutes):
            return SectFairylandClaimResult(status, user_id, sect_id, day, int(result_level), int(result_minutes), detail or {})

        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return result("schema_missing")

            previous = uow.query_one(
                "SELECT user_id,sect_id,claim_day,level,minutes,detail_json "
                "FROM sect_fairyland_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                expected = (user_id, sect_id, day, level, minutes)
                actual = (
                    str(previous["user_id"]), str(previous["sect_id"]), str(previous["claim_day"]),
                    int(previous["level"]), int(previous["minutes"]),
                )
                if actual != expected:
                    return result("state_changed")
                return result("duplicate", json.loads(str(previous["detail_json"])))

            marker = uow.query_one(
                "SELECT claim_day FROM sect_fairyland_claim_days WHERE user_id=? AND sect_id=?",
                (user_id, sect_id),
            )
            if marker is not None and str(marker["claim_day"] or "") == day:
                return result("already_claimed")

            row = uow.query_one("SELECT * FROM tianti_info WHERE user_id=?", (user_id,))
            data = self._decode_profile(self.profile.clean(dict(row) if row is not None else None))
            now = self.clock.now()
            if now.tzinfo is not None:
                now = now.astimezone().replace(tzinfo=None)
            end_time = self._parse_time(data.get("medicine_end_time"))
            effect = float(data.get("medicine_effect", 0) or 0)
            bath = {"name": data.get("medicine_name") or "未知药材", "effect": effect, "end_time": end_time} if end_time and now <= end_time and effect > 1 else None
            bath_expired = bath is None and bool(data.get("medicine_end_time"))
            if bath_expired:
                data.update({"medicine_last_time": None, "medicine_end_time": None, "medicine_effect": 0.0, "medicine_name": ""})

            base_ratio = 0.0
            gain_pct = 0.0
            for item in data["opened_qiaoxue_detail"]:
                if not isinstance(item, Mapping):
                    continue
                try:
                    value = float(item.get("effect_value", 0) or 0)
                except (TypeError, ValueError):
                    continue
                if item.get("effect_type") == "base_per_min_ratio":
                    base_ratio += value
                elif item.get("effect_type") == "hp_gain_pct":
                    gain_pct += value

            base_per_min = int(self.profile.levels()[str(data["tianti_level"])].get("hp_gain_per_min", 0))
            sect_bonus = max(0, min(level, 10)) * 0.05
            spirit_vein_multiplier = float(self.spirit_vein_multiplier())
            decision = decide_tianti_gain(
                minutes=minutes,
                base_per_min=base_per_min,
                base_ratio=base_ratio,
                gain_pct=gain_pct,
                bath_effect=effect if bath is not None else 1.0,
                sect_bonus=sect_bonus,
                spirit_vein_multiplier=spirit_vein_multiplier,
                old_hp=int(data["tianti_hp"]),
                hp_cap=self.profile.cap(data),
            )
            data["tianti_hp"] = decision.new_hp
            detail = {
                "status": "ok",
                "mins": minutes,
                "real_gain": decision.real_gain,
                "new_hp": decision.new_hp,
                "cap": self.profile.cap(data),
                "bath": bath,
                "bath_expired": bath_expired,
                "sect_bonus": sect_bonus,
                "spirit_vein_bonus": spirit_vein_multiplier - 1,
            }

            self._write_profile(uow, user_id, data)
            uow.execute(
                "INSERT INTO sect_fairyland_claim_days(user_id,sect_id,claim_day) VALUES(?,?,?) "
                "ON CONFLICT(user_id,sect_id) DO UPDATE SET claim_day=excluded.claim_day,updated_at=CURRENT_TIMESTAMP",
                (user_id, sect_id, day),
            )
            self._write_legacy_projection(uow, user_id, sect_id, day)
            detail_json = json.dumps(detail, ensure_ascii=False, default=str)
            uow.execute(
                "INSERT INTO sect_fairyland_claim_operations(operation_id,user_id,sect_id,claim_day,level,minutes,detail_json) "
                "VALUES(?,?,?,?,?,?,?)",
                (operation_id, user_id, sect_id, day, level, minutes, detail_json),
            )
            return result("claimed", json.loads(detail_json))


__all__ = ["SectFairylandClaimResult", "SectFairylandSqlRepository", "TiantiProfile"]
