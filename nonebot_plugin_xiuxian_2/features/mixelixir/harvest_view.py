"""灵田收取只读快照。

`灵田收取` 原先在命令模块里直接 `get_player_info(user_id, "mix_elixir_info")`，
然后既读 ``['收取时间']``、又读 ``['药材速度']``/``['灵田数量']``/``['收取等级']``：
缺行或历史脏时间会抛异常，成熟窗口也硬编码在命令里，与仓储各写一份。

本模块只用一次只读 UoW 读取真实列集，成熟判定复用
`features/mixelixir/harvest_rules.py`，缺列/缺行/坏时间一律 fail-soft
（``state=invalid``），由调用方按「首次收取」处理，不抛错、不建表、不补列。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork
from .harvest_rules import (
    DEFAULT_SPEED_BASE,
    DEFAULT_TIME_COST_HOURS,
    INVALID,
    as_int,
    maturity_state,
)

FIELD_FIELDS = ("收取时间", "收取等级", "灵田数量", "药材速度")


@dataclass(frozen=True)
class FieldSnapshot:
    """One read-only view of the ripening field."""

    ok: bool
    notice: str | None = None
    state: str = INVALID
    remaining_hours: float = 0.0
    last_time: str = ""
    field_count: int = 0
    harvest_level: int = 0
    herb_speed: int = 0

    @property
    def ready(self) -> bool:
        return self.ok and self.state != "waiting"


class MixelixirHarvestFieldApplication:
    """Read-only snapshot behind the ``灵田收取`` command."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        clock: Any | None = None,
        time_cost_hours: float = DEFAULT_TIME_COST_HOURS,
        speed_base: float = DEFAULT_SPEED_BASE,
    ) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.clock = clock or SystemClock()
        self.time_cost_hours = float(time_cost_hours)
        self.speed_base = float(speed_base)

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str, schema: str = "main") -> set[str]:
        return {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA {schema}.table_info("{table}")')
        }

    def snapshot(self, user_id: str) -> FieldSnapshot:
        """Read the field state; never raises on missing/dirty legacy rows."""
        user_id = str(user_id)
        try:
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                uow.execute("ATTACH DATABASE ? AS player_data", (self.player_database,))
                tables = {
                    str(row["name"]).casefold()
                    for row in uow.query_all(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                if "user_xiuxian" not in tables:
                    return FieldSnapshot(False, "未找到修仙数据。")
                row = uow.query_one(
                    "SELECT 1 AS present FROM player_data.sqlite_master "
                    "WHERE type='table' AND name=?",
                    ("mix_elixir_info",),
                )
                if row is None:
                    return FieldSnapshot(False, "洞天福地数据尚未就绪，请稍后再试。")
                columns = self._columns(uow, "mix_elixir_info", "player_data")
                selected = [field for field in FIELD_FIELDS if field.casefold() in columns]
                if not selected:
                    return FieldSnapshot(False, "洞天福地数据尚未就绪，请稍后再试。")
                projection = ",".join(f'"{field}"' for field in selected)
                state_row = uow.query_one(
                    f"SELECT {projection} FROM player_data.mix_elixir_info WHERE user_id=?",
                    (user_id,),
                )
        except Exception:  # pragma: no cover - defensive: any read failure is fail-soft
            return FieldSnapshot(False, "灵田状态读取失败，请稍后再试。")

        if state_row is None:
            # No row yet: the historical behaviour is "first harvest", not an error.
            return FieldSnapshot(True, None, state=INVALID)
        raw = dict(state_row)
        herb_speed = as_int(raw.get("药材速度"))
        last_time = raw.get("收取时间")
        state, remaining = maturity_state(
            last_time,
            self.clock.now(),
            herb_speed=herb_speed,
            time_cost_hours=self.time_cost_hours,
            speed_base=self.speed_base,
        )
        return FieldSnapshot(
            True,
            None,
            state=state,
            remaining_hours=remaining,
            last_time="" if last_time is None else str(last_time),
            field_count=as_int(raw.get("灵田数量"), 1),
            harvest_level=as_int(raw.get("收取等级")),
            herb_speed=herb_speed,
        )


__all__ = ["FIELD_FIELDS", "FieldSnapshot", "MixelixirHarvestFieldApplication"]
