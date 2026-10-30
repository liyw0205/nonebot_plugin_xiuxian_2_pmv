"""玩家的跨库资产总览（只读）。

玩家的「身家」分散在两个数据库、五张表里，此前只能靠 5 条以上命令各自拼：
`灵石`（base）、`灵庄`（bank）、`我的宗门`（sect）、`我的背包`（back）、
`我的竞技场`（arena）。本模块给出一次调用的合计视图。

只读：复用上述既有表与索引，不建表、不补列、不写请求期 DDL。任何一段数据缺失
（旧库缺表、缺列、缺行）都只让该段消失，绝不让整条命令失败。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


class PlayerAssetsOverviewApplication:
    """Compose one player's assets from the existing per-feature tables."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    # -- schema probing ---------------------------------------------------
    @staticmethod
    def _table_exists(uow: DatabaseUnitOfWork, schema: str, table: str) -> bool:
        row = uow.query_one(
            f"SELECT 1 AS present FROM {schema}.sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        return row is not None

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, schema: str, table: str) -> set[str]:
        return {
            str(row["name"])
            for row in uow.query_all(f'PRAGMA {schema}.table_info("{table}")')
        }

    def _read_game(self, uow: DatabaseUnitOfWork, user_id: str) -> dict[str, Any]:
        result: dict[str, Any] = {
            "found": False,
            "stone": None,
            "sect_name": None,
            "sect_contribution": None,
            "bank_saved": None,
            "bank_level": None,
            "bag_kinds": None,
            "bag_items": None,
        }
        # 角色主表：缺失/缺行即「未找到角色」。
        if not self._table_exists(uow, "main", "user_xiuxian"):
            return result
        columns = self._columns(uow, "main", "user_xiuxian")
        if "user_id" not in columns or "stone" not in columns:
            return result
        row = uow.query_one(
            "SELECT stone, sect_id FROM user_xiuxian WHERE user_id=?",
            (user_id,),
        ) if "sect_id" in columns else uow.query_one(
            "SELECT stone, NULL AS sect_id FROM user_xiuxian WHERE user_id=?",
            (user_id,),
        )
        if row is None:
            return result
        result["found"] = True
        result["stone"] = _int(row["stone"])

        # 宗门归属与贡献
        sect_id = row["sect_id"]
        if sect_id not in (None, "", 0) and self._table_exists(uow, "main", "sects"):
            sect_columns = self._columns(uow, "main", "sects")
            if {"sect_id", "sect_name"} <= sect_columns:
                sect = uow.query_one(
                    "SELECT sect_name FROM sects WHERE sect_id=?", (_int(sect_id),)
                )
                if sect is not None and str(sect["sect_name"] or "").strip():
                    result["sect_name"] = str(sect["sect_name"]).strip()
                    if "sect_contribution" in columns:
                        contribution = uow.query_one(
                            "SELECT sect_contribution FROM user_xiuxian WHERE user_id=?",
                            (user_id,),
                        )
                        result["sect_contribution"] = _int(
                            (contribution or {}).get("sect_contribution")
                        )

        # 灵庄存款
        if self._table_exists(uow, "main", "bank_accounts"):
            bank_columns = self._columns(uow, "main", "bank_accounts")
            if {"user_id", "saved_stone"} <= bank_columns:
                level_sql = "bank_level" if "bank_level" in bank_columns else "NULL AS bank_level"
                bank = uow.query_one(
                    f"SELECT saved_stone,{level_sql} FROM bank_accounts WHERE user_id=?",
                    (user_id,),
                )
                if bank is not None:
                    result["bank_saved"] = _int(bank["saved_stone"])
                    result["bank_level"] = str(bank["bank_level"] or "").strip() or None

        # 背包规模
        if self._table_exists(uow, "main", "back"):
            back_columns = self._columns(uow, "main", "back")
            if {"user_id", "goods_id"} <= back_columns:
                quantity = (
                    "COALESCE(SUM(goods_num),0)" if "goods_num" in back_columns else "0"
                )
                bag = uow.query_one(
                    f"SELECT COUNT(*) AS kinds,{quantity} AS items FROM back "
                    "WHERE user_id=? AND COALESCE(goods_num,0)>0",
                    (user_id,),
                )
                if bag is not None and _int(bag["kinds"]) > 0:
                    result["bag_kinds"] = _int(bag["kinds"])
                    result["bag_items"] = _int(bag["items"])
        return result

    def _read_player(self, uow: DatabaseUnitOfWork, user_id: str) -> dict[str, Any]:
        result: dict[str, Any] = {
            "arena_rank": None,
            "arena_score": None,
            "arena_honor": None,
        }
        if not self._table_exists(uow, "player_data", "arena"):
            return result
        columns = self._columns(uow, "player_data", "arena")
        if "user_id" not in columns:
            return result
        selected = ", ".join(
            f"{name}" if name in columns else f"NULL AS {name}"
            for name in ("rank", "score", "honor_points")
        )
        row = uow.query_one(
            f"SELECT {selected} FROM player_data.arena WHERE user_id=?", (user_id,)
        )
        if row is None:
            return result
        result["arena_rank"] = str(row["rank"] or "").strip() or None
        result["arena_score"] = _int(row["score"]) if "score" in columns else None
        result["arena_honor"] = (
            _int(row["honor_points"]) if "honor_points" in columns else None
        )
        return result

    # -- public -----------------------------------------------------------
    def overview(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        payload: dict[str, Any] = {
            "found": False,
            "stone": None,
            "sect_name": None,
            "sect_contribution": None,
            "bank_saved": None,
            "bank_level": None,
            "bag_kinds": None,
            "bag_items": None,
            "arena_rank": None,
            "arena_score": None,
            "arena_honor": None,
        }
        if not user_id:
            return payload
        try:
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                try:
                    uow.attach_database(self.player_database, "player_data")
                    attached = True
                except Exception:
                    attached = False
                payload.update(self._read_game(uow, user_id))
                if attached and payload["found"]:
                    try:
                        payload.update(self._read_player(uow, user_id))
                    except Exception:
                        # 竞技场段不可读时只省略该段。
                        pass
        except Exception:
            # 主库不可读时保持未找到，由渲染层给出引导文案。
            payload["found"] = False
        return payload


def _line(label: str, value: str) -> str:
    return f"{label}：{value}"


def format_assets_overview(payload: Mapping[str, Any], *, format_number=None) -> str:
    """Render the personal asset overview. Pure: no I/O, no NoneBot."""
    if not payload.get("found"):
        return "未找到角色数据，请先发送【我要修仙】。"

    fmt = format_number or (lambda value: f"{int(value):,}")
    lines = ["【我的资产总览】", _line("灵石", fmt(int(payload.get("stone") or 0)))]

    if payload.get("bank_saved") is not None:
        level = payload.get("bank_level")
        amount = fmt(int(payload["bank_saved"]))
        lines.append(_line("灵庄存款", f"{amount}（{level}）" if level else amount))

    if payload.get("sect_name"):
        contribution = payload.get("sect_contribution")
        name = str(payload["sect_name"])
        lines.append(
            _line("宗门", f"{name}（贡献 {fmt(int(contribution or 0))}）")
            if contribution is not None
            else _line("宗门", name)
        )

    if payload.get("bag_kinds") is not None:
        lines.append(
            _line(
                "背包",
                f"{fmt(int(payload['bag_kinds']))} 种 / 共 {fmt(int(payload.get('bag_items') or 0))} 个",
            )
        )

    if payload.get("arena_rank") is not None or payload.get("arena_score") is not None:
        parts = []
        if payload.get("arena_rank"):
            parts.append(str(payload["arena_rank"]))
        if payload.get("arena_score") is not None:
            parts.append(f"{fmt(int(payload['arena_score']))} 分")
        if payload.get("arena_honor") is not None:
            parts.append(f"荣誉 {fmt(int(payload['arena_honor']))}")
        if parts:
            lines.append(_line("竞技场", " · ".join(parts)))
    return "\n".join(lines)


__all__ = ["PlayerAssetsOverviewApplication", "format_assets_overview"]
