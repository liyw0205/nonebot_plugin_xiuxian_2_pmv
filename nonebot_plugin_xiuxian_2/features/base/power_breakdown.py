"""玩家的战力拆解与升境预估（只读）。

战力是一个闭式三因子乘积，写在 `xiuxian2_handle.py` 的 `update_power2`：

    power = round(exp × root_rate × level_spend, 0)

- `exp`：玩家当前修为；
- `root_rate`：灵根倍率（`get_root_rate`；`命运道果` 走 `compute_fate_root_rate` 阶梯累加）；
- `level_spend`：境界系数（`jsondata.level_data()[level]["spend"]`）。

但玩家侧只有一个 `战力` 数字：既不知道它由什么组成，也不知道「突破」和「换灵根」
哪个更划算。本模块把三因子拆开，并给出同修为下的升境预估。

只读 `user_xiuxian` 单行，不建表、不补列、不写库，也不改 `update_power2` 的公式。
舍入刻意与 SQLite `round(x,0)` 对齐（.5 向远离零方向），避免与账面值出现 +1 的假差异。
"""

from __future__ import annotations

from ...core.numeric import as_int_like

import math
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork

TABLE = "user_xiuxian"


def _round_half_up(value: float) -> int:
    """与 SQLite `round(x,0)` 一致：.5 远离零方向。"""
    if value >= 0:
        return int(math.floor(value + 0.5))
    return -int(math.floor(-value + 0.5))


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value: Any) -> int:
    try:
        return as_int_like(value or 0)
    except (TypeError, ValueError):
        return 0


def _default_root_rate(root_type: str, root_level: Any) -> float:
    """与 `XiuxianDateManage.get_root_rate` 同语义的默认实现（延迟导入配置）。"""
    from ...xiuxian.xiuxian_utils import jsondata
    from ...xiuxian.xiuxian_utils.numeric_bind import compute_fate_root_rate

    data = jsondata.root_data()
    if root_type == "命运道果":
        base_rate = data["永恒道果"]["type_speeds"]
        fate_step = data["命运道果"]["type_speeds"]
        return float(compute_fate_root_rate(_int(root_level), base_rate, fate_step))
    entry = data.get(root_type)
    if not entry:
        raise KeyError(root_type)
    return float(entry["type_speeds"])


def _default_level_data() -> dict[str, dict[str, Any]]:
    from ...xiuxian.xiuxian_utils import jsondata

    return jsondata.level_data()


class PowerBreakdownApplication:
    """战力三因子拆解 + 同修为升境预估（只读）。"""

    def __init__(
        self,
        player_database: str | Path,
        *,
        root_rate_fn=None,
        level_data_fn=None,
    ) -> None:
        self.player_database = str(player_database)
        self._root_rate_fn = root_rate_fn or _default_root_rate
        self._level_data_fn = level_data_fn or _default_level_data

    # ---------------------------------------------------------------- helpers
    @staticmethod
    def _table_exists(uow: DatabaseUnitOfWork, table: str) -> bool:
        return (
            uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            is not None
        )

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {str(row["name"]) for row in uow.query_all(f'PRAGMA table_info("{table}")')}

    def _empty(self, notice: str | None = None) -> dict[str, Any]:
        return {
            "ok": notice is None,
            "notice": notice,
            "level": "",
            "root": "",
            "root_type": "",
            "root_level": 0,
            "exp": 0,
            "root_rate": None,
            "level_spend": None,
            "power_computed": None,
            "power_stored": None,
            "power_delta": None,
            "next_level": "",
            "next_level_spend": None,
            "next_power_if_same_exp": None,
            "exp_to_next": None,
            "growth": [],
            "dominant": "",
        }

    # --------------------------------------------------------------- main API
    def breakdown(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return self._empty("缺少玩家身份，无法拆解战力。")

        try:
            with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                if not self._table_exists(uow, TABLE):
                    return self._empty("暂无战力数据：玩家库尚未初始化。")
                columns = self._columns(uow, TABLE)
                if not {"user_id", "level"} <= columns:
                    return self._empty("暂无战力数据：玩家档案结构不完整。")
                exp_sql = "exp" if "exp" in columns else "0"
                root_sql = "root" if "root" in columns else "''"
                root_type_sql = "root_type" if "root_type" in columns else "''"
                root_level_sql = "root_level" if "root_level" in columns else "0"
                power_sql = "power" if "power" in columns else "NULL"
                row = uow.query_one(
                    f"SELECT level,{exp_sql} AS exp,{root_sql} AS root,"
                    f"{root_type_sql} AS root_type,{root_level_sql} AS root_level,"
                    f"{power_sql} AS power FROM {TABLE} WHERE user_id=?",
                    (user_id,),
                )
        except Exception:
            return self._empty("暂时无法读取玩家档案，请稍后再试。")

        if row is None:
            return self._empty("尚未创建修仙角色，先去 `我要修仙` 创建道号。")

        payload = self._empty()
        level = str(row["level"] or "").strip()
        root = str(row["root"] or "").strip()
        root_type = str(row["root_type"] or "").strip() or root
        root_level = _int(row["root_level"])
        exp = _int(row["exp"])
        payload.update(
            level=level,
            root=root,
            root_type=root_type,
            root_level=root_level,
            exp=exp,
        )

        try:
            level_data = self._level_data_fn() or {}
        except Exception:
            return self._empty("暂时无法读取境界配置，请稍后再试。")

        if level not in level_data:
            payload.update(
                ok=False,
                notice=f"境界「{level or '未知'}」不在境界表中，无法拆解战力。",
            )
            return payload

        try:
            root_rate = float(self._root_rate_fn(root_type, root_level))
        except Exception:
            payload.update(
                ok=False,
                notice=f"灵根「{root_type or '未知'}」倍率不可用，无法拆解战力。",
            )
            return payload

        level_spend = _num(level_data[level].get("spend"))
        if level_spend <= 0:
            payload.update(
                ok=False,
                notice=f"境界「{level}」系数异常，无法拆解战力。",
            )
            return payload

        computed = _round_half_up(exp * root_rate * level_spend)
        stored = None if row["power"] is None else _int(row["power"])

        names = list(level_data.keys())
        index = names.index(level)
        next_level = names[index + 1] if index + 1 < len(names) else ""
        next_spend = (
            _num(level_data[next_level].get("spend")) if next_level else None
        ) or None

        exp_to_next = None
        need = _num(level_data[level].get("power"))
        if need > 0:
            exp_to_next = max(0, _round_half_up(need - exp))

        growth: list[dict[str, Any]] = []
        growth.append({"lever": "修为", "factor": 1.0, "note": "修为每提升 1%，战力同步 +1%"})
        if next_spend:
            ratio = next_spend / level_spend
            growth.append(
                {
                    "lever": "突破",
                    "factor": round(ratio, 4),
                    "note": f"突破到「{next_level}」后境界系数 {level_spend:g}→{next_spend:g}",
                }
            )
        growth.append(
            {
                "lever": "灵根",
                "factor": round(root_rate, 4),
                "note": f"当前灵根「{root_type}」倍率 {root_rate:g}",
            }
        )
        # 主导项：突破与灵根的相对增幅里更大的那个；修为是线性基准。
        ranked = [item for item in growth if item["lever"] != "修为"]
        dominant = max(ranked, key=lambda item: item["factor"])["lever"] if ranked else "修为"

        payload.update(
            root_rate=round(root_rate, 6),
            level_spend=round(level_spend, 6),
            power_computed=computed,
            power_stored=stored,
            power_delta=None if stored is None else computed - stored,
            next_level=next_level,
            next_level_spend=None if next_spend is None else round(next_spend, 6),
            next_power_if_same_exp=(
                None if next_spend is None else _round_half_up(exp * root_rate * next_spend)
            ),
            exp_to_next=exp_to_next,
            growth=growth,
            dominant=dominant,
        )
        return payload


def format_power_breakdown(payload: dict[str, Any], *, format_number=None) -> str:
    """渲染战力拆解。纯函数：无 I/O、不依赖 NoneBot。"""
    notice = str(payload.get("notice") or "").strip()
    if not payload.get("ok"):
        return notice or "暂时无法拆解战力。"

    fmt = format_number or (lambda value: f"{int(value):,}")
    lines = [
        f"【我的战力拆解】{payload.get('level', '')}",
        f"修为：{fmt(payload.get('exp') or 0)}",
        f"灵根：{payload.get('root_type') or '未知'}"
        f"（倍率 ×{payload.get('root_rate')}）",
        f"境界系数：×{payload.get('level_spend')}",
        f"计算战力：{fmt(payload.get('power_computed') or 0)}",
    ]

    stored = payload.get("power_stored")
    delta = payload.get("power_delta")
    if stored is not None:
        if delta:
            lines.append(
                f"账面战力：{fmt(stored)}（与计算值相差 {delta:+d}，下次战力刷新会校正）"
            )
        else:
            lines.append(f"账面战力：{fmt(stored)}（与计算值一致）")

    if payload.get("next_level"):
        lines.append("")
        lines.append(f"下一境：{payload['next_level']}（系数 ×{payload['next_level_spend']}）")
        if payload.get("exp_to_next") is not None:
            lines.append(f"距突破还需修为：{fmt(payload['exp_to_next'])}")
        if payload.get("next_power_if_same_exp") is not None:
            lines.append(f"若现在突破（修为不变）：{fmt(payload['next_power_if_same_exp'])}")

    growth = list(payload.get("growth") or [])
    if growth:
        lines.append("")
        lines.append("提升手段：")
        for item in growth:
            lines.append(f"· {item.get('lever')}：{item.get('note')}")
        if payload.get("dominant"):
            lines.append(f"当前最划算：{payload['dominant']}")
    return "\n".join(lines)


__all__ = ["PowerBreakdownApplication", "format_power_breakdown"]
