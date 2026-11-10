"""灵田读模型（只读）。

`我的灵田` 原先直接走旧 `get_player_info`，在缺行/脏时间时会抛异常，并把成熟
窗口硬编码成与收获仓储不同的一份常量。本模块用一次只读 UoW 读取
`user_xiuxian` 与 `mix_elixir_info` 的真实列集，成熟判定复用
`features/puppet/field_rules.py`，因此展示与自动收取不会各自漂移。

最近收取摘要只读 `puppet_harvest_operations`（启动迁移 `puppet.003` 创建），
时间窗只用下界，避免 UTC/本地时间混用把「刚刚发生」的行误判为未来。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork
from .field_rules import (
    DEFAULT_SPEED_BASE,
    DEFAULT_TIME_COST_HOURS,
    INVALID,
    READY,
    WAITING,
    as_int,
    maturity_state,
    normalize_now,
)

RECENT_DAYS = 7
RECENT_REWARD_NAMES = 5


@dataclass(frozen=True)
class HarvestSummary:
    count: int = 0
    stone_cost: int = 0
    last_at: str = ""
    rewards: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True)
class PuppetFieldSnapshot:
    ok: bool
    notice: str | None = None
    field_count: int = 0
    herb_speed: int = 0
    harvest_level: int = 0
    puppet_level: int = 0
    puppet_enabled: int = 0
    stone: int = 0
    maturity: str = INVALID
    remaining_hours: float = 0.0
    recent: HarvestSummary | None = None


def _parse_rewards(raw: Any) -> tuple[tuple[str, int], ...]:
    """Parse a stored ``rewards_json`` list-shaped payload; bad rows are ignored."""
    try:
        payload = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        return ()
    if isinstance(payload, Mapping):
        payload = [payload]
    if not isinstance(payload, list):
        return ()
    totals: dict[str, int] = {}
    for element in payload:
        if not isinstance(element, Mapping):
            continue
        name = str(element.get("goods_name") or element.get("name") or "").strip()
        quantity = as_int(element.get("quantity") or element.get("amount"))
        if not name or quantity <= 0:
            continue
        totals[name] = totals.get(name, 0) + quantity
    ordered = sorted(totals.items(), key=lambda item: (-item[1], item[0]))
    return tuple(ordered[:RECENT_REWARD_NAMES])


class PuppetFieldApplication:
    """Read-only snapshot behind the ``我的灵田`` view."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        clock: Any | None = None,
        time_cost_hours: float = DEFAULT_TIME_COST_HOURS,
        speed_base: float = DEFAULT_SPEED_BASE,
        recent_days: int = RECENT_DAYS,
        status_reader: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.clock = clock or SystemClock()
        self.time_cost_hours = float(time_cost_hours)
        self.speed_base = float(speed_base)
        self.recent_days = max(int(recent_days), 1)
        self.status_reader = status_reader

    @staticmethod
    def _table_exists(uow: DatabaseUnitOfWork, table: str, schema: str = "main") -> bool:
        return (
            uow.query_one(
                f"SELECT 1 AS present FROM {schema}.sqlite_master "
                "WHERE type='table' AND name=?",
                (table,),
            )
            is not None
        )

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str, schema: str = "main") -> set[str]:
        return {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA {schema}.table_info("{table}")')
        }

    def _recent(self, uow: DatabaseUnitOfWork, user_id: str, now: datetime) -> HarvestSummary | None:
        if not self._table_exists(uow, "puppet_harvest_operations"):
            return None
        columns = self._columns(uow, "puppet_harvest_operations")
        if not {"user_id", "rewards_json", "stone_cost", "created_at"} <= columns:
            return None
        cutoff = (normalize_now(now) - timedelta(days=self.recent_days)).strftime("%Y-%m-%d %H:%M:%S")
        aggregate = uow.query_one(
            "SELECT COUNT(*) AS count,COALESCE(xiuxian_int_sum(stone_cost),0) AS stone_cost,"
            "MAX(created_at) AS last_at FROM puppet_harvest_operations "
            "WHERE user_id=? AND created_at >= ?",
            (user_id, cutoff),
        )
        if aggregate is None or as_int(aggregate["count"]) <= 0:
            return HarvestSummary()
        latest = uow.query_one(
            "SELECT rewards_json FROM puppet_harvest_operations "
            "WHERE user_id=? AND created_at >= ? "
            "ORDER BY created_at DESC,rowid DESC LIMIT 1",
            (user_id, cutoff),
        )
        return HarvestSummary(
            count=as_int(aggregate["count"]),
            stone_cost=as_int(aggregate["stone_cost"]),
            last_at=str(aggregate["last_at"] or ""),
            rewards=_parse_rewards(latest["rewards_json"]) if latest is not None else (),
        )

    def snapshot(self, user_id: str) -> PuppetFieldSnapshot:
        user_id = str(user_id or "").strip()
        if not user_id:
            return PuppetFieldSnapshot(False, "缺少玩家身份，无法查询灵田。")
        if not Path(self.player_database).is_file():
            return PuppetFieldSnapshot(False, "灵田数据暂不可用（玩家数据库缺失）。")
        now = self.clock.now()
        try:
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                if not self._table_exists(uow, "user_xiuxian"):
                    return PuppetFieldSnapshot(False, "暂无灵田数据：玩家主表尚未创建。")
                user_columns = self._columns(uow, "user_xiuxian")
                if "user_id" not in user_columns:
                    return PuppetFieldSnapshot(False, "暂无灵田数据：玩家主表结构不完整。")
                level_sql = '"level"' if "level" in user_columns else "''"
                stone_sql = "COALESCE(stone,0)" if "stone" in user_columns else "0"
                blessed_sql = (
                    "COALESCE(blessed_spot_flag,0)" if "blessed_spot_flag" in user_columns else "1"
                )
                status_sql = (
                    "COALESCE(puppet_status,0)" if "puppet_status" in user_columns else "0"
                )
                user = uow.query_one(
                    f"SELECT {level_sql} AS level,{stone_sql} AS stone,"
                    f"{blessed_sql} AS blessed_spot_flag,{status_sql} AS puppet_status "
                    "FROM user_xiuxian WHERE user_id=?",
                    (user_id,),
                )
                if user is None:
                    return PuppetFieldSnapshot(False, "还没有修仙角色，请先发送【我要修仙】。")
                if as_int(user["blessed_spot_flag"]) == 0:
                    return PuppetFieldSnapshot(
                        False, "道友还没有洞天福地呢，请发送洞天福地购买来购买吧~"
                    )

                uow.attach_database(self.player_database, "player_data", read_only=True)
                if not self._table_exists(uow, "mix_elixir_info", "player_data"):
                    return PuppetFieldSnapshot(
                        False,
                        "暂无灵田记录：灵田数据表尚未创建。",
                        stone=as_int(user["stone"]),
                    )
                elixir_columns = self._columns(uow, "mix_elixir_info", "player_data")
                if not {"user_id", "收取时间", "灵田傀儡"} <= elixir_columns:
                    return PuppetFieldSnapshot(
                        False,
                        "暂无灵田记录：灵田数据结构不完整。",
                        stone=as_int(user["stone"]),
                    )
                field_sql = '"灵田数量"' if "灵田数量" in elixir_columns else "0"
                speed_sql = '"药材速度"' if "药材速度" in elixir_columns else "0"
                harvest_sql = '"收取等级"' if "收取等级" in elixir_columns else "0"
                elixir = uow.query_one(
                    f'SELECT "收取时间" AS last_time,{harvest_sql} AS harvest_level,'
                    f"{field_sql} AS field_count,{speed_sql} AS herb_speed,"
                    f'"灵田傀儡" AS puppet_level '
                    "FROM player_data.mix_elixir_info WHERE user_id=?",
                    (user_id,),
                )
                if elixir is None:
                    return PuppetFieldSnapshot(
                        False,
                        "还没有灵田记录，请先开通洞天福地并开垦灵田。",
                        stone=as_int(user["stone"]),
                    )
                herb_speed = as_int(elixir["herb_speed"])
                maturity, remaining = maturity_state(
                    elixir["last_time"],
                    now,
                    time_cost_hours=self.time_cost_hours,
                    speed_base=self.speed_base,
                    herb_speed=herb_speed,
                )
                puppet_level = as_int(elixir["puppet_level"])
                puppet_enabled = as_int(user["puppet_status"])
                if self.status_reader is not None:
                    # The enable switch is owned by PuppetStatusSqlRepository; ask the
                    # application instead of duplicating its read semantics here.
                    puppet_enabled = as_int(self.status_reader(user_id))
                recent = self._recent(uow, user_id, now)
                return PuppetFieldSnapshot(
                    True,
                    field_count=max(as_int(elixir["field_count"]), 0),
                    herb_speed=herb_speed,
                    harvest_level=max(as_int(elixir["harvest_level"]), 0),
                    puppet_level=max(puppet_level, 0),
                    puppet_enabled=puppet_enabled,
                    stone=as_int(user["stone"]),
                    maturity=maturity,
                    remaining_hours=remaining,
                    recent=recent,
                )
        except Exception:
            return PuppetFieldSnapshot(False, "暂时无法读取灵田信息，请稍后再试。")


def _maturity_text(maturity: str, remaining_hours: float) -> str:
    if maturity == READY:
        return "灵药已成熟，可以收取"
    if maturity == WAITING:
        remaining = max(float(remaining_hours), 0.0)
        hours = int(remaining)
        minutes = int((remaining - hours) * 60)
        return f"灵药成熟还需{hours}小时{minutes}分钟"
    return "暂时无法判断（收取时间异常）"


def format_puppet_field(snapshot: PuppetFieldSnapshot, *, format_number=None) -> str:
    """Render ``我的灵田``; pure function with no I/O or NoneBot dependency."""
    if not snapshot.ok:
        return snapshot.notice or "暂时无法读取灵田信息。"

    fmt = format_number or (lambda value: f"{int(value):,}")
    lines = [
        "道友的灵田信息：",
        f"灵田数量：{snapshot.field_count}",
        f"药材速度：增加{snapshot.herb_speed * 100}%",
        f"灵药状态：{_maturity_text(snapshot.maturity, snapshot.remaining_hours)}",
    ]
    if snapshot.puppet_level > 0:
        lines.append(f"傀儡等级：{snapshot.puppet_level}级")
        lines.append(f"傀儡状态：{'开启' if snapshot.puppet_enabled == 1 else '关闭'}")

    recent = snapshot.recent
    if recent is not None and recent.count > 0:
        lines.append("")
        lines.append(f"最近7天收取：{recent.count} 次，消耗灵石 {fmt(recent.stone_cost)}")
        if recent.last_at:
            stamp = recent.last_at[5:16]
            lines.append(f"最近一次：{stamp}")
        if recent.rewards:
            detail = "、".join(f"{name} x{quantity}" for name, quantity in recent.rewards)
            lines.append(f"上次收获：{detail}")
    return "\n".join(lines)


__all__ = [
    "HarvestSummary",
    "PuppetFieldApplication",
    "PuppetFieldSnapshot",
    "format_puppet_field",
]
