from __future__ import annotations

from typing import Any

try:
    from nonebot.log import logger
except Exception:  # pragma: no cover
    logger = None

from ..xiuxian_config import XiuConfig
from .economy_log import safe_log_economy_change
from .item_json import Items
from .utils import number_to
from .xiuxian2_handle import OtherSet, PlayerDataManager, XiuxianDateManage


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class RewardService:
    def __init__(self):
        self.sql_message = XiuxianDateManage()
        self.player_data_manager = PlayerDataManager()
        self.items = Items()

    def _grant_exp(self, user_id: str, exp: int) -> int:
        user_info = self.sql_message.get_user_info_with_id(user_id)
        if not user_info:
            return 0

        exp = max(0, int(exp))
        current_exp = int(user_info.get("exp", 0) or 0)
        max_exp = int(OtherSet().set_closing_type(user_info["level"])) * XiuConfig().closing_exp_upper_limit
        grant_exp = min(exp, max(max_exp - current_exp, 0))
        if grant_exp <= 0:
            return 0

        self.sql_message.update_exp(user_id, grant_exp)
        self.sql_message.update_power2(user_id)
        return grant_exp

    def _grant_items(self, user_id: str, reward: dict[str, Any]) -> list[dict[str, Any]]:
        granted: list[dict[str, Any]] = []
        for item in reward.get("items", []) or []:
            item_id = item.get("id") or item.get("goods_id")
            amount = max(1, _to_int(item.get("amount", item.get("num", 1)), 1))
            bind_flag = _to_int(item.get("bind_flag", item.get("bind", 1)), 1)
            item_info = self.items.get_data_by_item_id(item_id)
            if not item_info:
                if logger:
                    logger.warning(f"奖励物品不存在：{item_id}")
                continue

            self.sql_message.send_back(
                user_id,
                int(item_id),
                item_info["name"],
                item_info["type"],
                amount,
                bind_flag,
            )
            granted.append(
                {
                    "id": int(item_id),
                    "name": item_info["name"],
                    "type": item_info["type"],
                    "amount": amount,
                    "bind_flag": bind_flag,
                }
            )
        return granted

    def _grant_sect_contribution(self, user_id: str, amount: int) -> int:
        amount = max(0, int(amount))
        if amount <= 0:
            return 0
        self.sql_message.update_user_sect_contribution(
            user_id,
            self._current_sect_contribution(user_id) + amount,
        )
        return amount

    def _current_sect_contribution(self, user_id: str) -> int:
        user_info = self.sql_message.get_user_info_with_id(user_id) or {}
        return _to_int(user_info.get("sect_contribution"), 0)

    def _grant_sect_resource(self, sect_id: int | None, amount: int, field: str) -> int:
        amount = max(0, int(amount))
        if not sect_id or amount <= 0:
            return 0
        sect_info = self.sql_message.get_sect_info_by_id(sect_id) or {}
        if field == "sect_scale":
            current_used_stone = _to_int(sect_info.get("sect_used_stone"), 0)
            current_scale = _to_int(sect_info.get("sect_scale"), 0)
            self.sql_message.update_sect_scale_and_used_stone(
                sect_id,
                current_used_stone,
                current_scale + amount,
            )
        elif field == "sect_materials":
            self.sql_message.update_sect_materials(sect_id, amount, 1)
        return amount

    def _grant_boss_integral(self, user_id: str, amount: int) -> int:
        amount = max(0, int(amount))
        if amount <= 0:
            return 0
        current = _to_int(self.player_data_manager.get_field_data(user_id, "boss_limit", "integral"), 0)
        self.player_data_manager.update_or_write_data(
            user_id,
            "boss_limit",
            "integral",
            current + amount,
            data_type="INTEGER",
        )
        return amount

    @staticmethod
    def _format_reward(granted: dict[str, Any], requested: dict[str, Any]) -> str:
        parts: list[str] = []
        for item in granted.get("items", []) or []:
            parts.append(f"{item['name']}x{item['amount']}")
        if granted.get("stone", 0) > 0:
            parts.append(f"灵石{number_to(granted['stone'])}")
        if granted.get("exp", 0) > 0:
            parts.append(f"修为{number_to(granted['exp'])}")
        elif _to_int(requested.get("exp"), 0) > 0:
            parts.append("修为已达上限")
        if granted.get("sect_contribution", 0) > 0:
            parts.append(f"宗门贡献{number_to(granted['sect_contribution'])}")
        if granted.get("sect_scale", 0) > 0:
            parts.append(f"宗门建设度{number_to(granted['sect_scale'])}")
        if granted.get("sect_materials", 0) > 0:
            parts.append(f"宗门资材{number_to(granted['sect_materials'])}")
        if granted.get("boss_integral", 0) > 0:
            parts.append(f"BOSS积分{number_to(granted['boss_integral'])}")
        return "、".join(parts) if parts else "无"

    def grant_reward(
        self,
        user_id: str,
        reward: dict[str, Any],
        source: str,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        user_id = str(user_id)
        reward = reward or {}
        meta = meta or {}
        sect_id = meta.get("sect_id")
        sect_id_int = _to_int(sect_id) if sect_id not in (None, "") else None

        granted: dict[str, Any] = {
            "stone": 0,
            "exp": 0,
            "items": [],
            "sect_contribution": 0,
            "sect_scale": 0,
            "sect_materials": 0,
            "boss_integral": 0,
        }

        stone = max(0, _to_int(reward.get("stone"), 0))
        if stone > 0:
            self.sql_message.update_ls(user_id, stone, 1)
            granted["stone"] = stone

        exp = max(0, _to_int(reward.get("exp"), 0))
        if exp > 0:
            granted["exp"] = self._grant_exp(user_id, exp)

        granted["items"] = self._grant_items(user_id, reward)
        granted["sect_contribution"] = self._grant_sect_contribution(
            user_id,
            _to_int(reward.get("sect_contribution"), 0),
        )
        granted["sect_scale"] = self._grant_sect_resource(
            sect_id_int,
            _to_int(reward.get("sect_scale"), 0),
            "sect_scale",
        )
        granted["sect_materials"] = self._grant_sect_resource(
            sect_id_int,
            _to_int(reward.get("sect_materials"), 0),
            "sect_materials",
        )
        granted["boss_integral"] = self._grant_boss_integral(
            user_id,
            _to_int(reward.get("boss_integral"), 0),
        )

        log_id = safe_log_economy_change(
            user_id=user_id,
            sect_id=sect_id_int,
            source=source,
            action=str(meta.get("action") or "grant_reward"),
            stone_delta=granted["stone"],
            exp_delta=granted["exp"],
            sect_contribution_delta=granted["sect_contribution"],
            sect_scale_delta=granted["sect_scale"],
            sect_materials_delta=granted["sect_materials"],
            item_delta=granted["items"],
            detail={"source": source, **dict(meta.get("detail") or {})},
        )

        return {
            "user_id": user_id,
            "source": source,
            "granted": granted,
            "text": self._format_reward(granted, reward),
            "economy_log_id": log_id,
        }


reward_service = RewardService()


def grant_reward(
    user_id: str,
    reward: dict[str, Any],
    source: str,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return reward_service.grant_reward(user_id, reward, source, meta)


def safe_grant_reward(
    user_id: str,
    reward: dict[str, Any],
    source: str,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        return grant_reward(user_id, reward, source, meta)
    except Exception as exc:
        if logger:
            logger.warning(f"发放奖励失败：user_id={user_id}, source={source}, error={exc}")
        return {
            "user_id": str(user_id),
            "source": source,
            "granted": {},
            "text": "奖励发放失败",
            "economy_log_id": 0,
        }
