# 文件路径：xiuxian_cmd/__init__.py
# （专注于本命法宝相关指令）

import random
import asyncio
from datetime import datetime
from pathlib import Path
from enum import IntEnum
from nonebot import on_command
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment
)
from nonebot.params import CommandArg
from nonebot.log import logger

from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage,
    UserBuffDate,
    PlayerDataManager
)
from ..xiuxian_utils.utils import (
    check_user,
    handle_send,
    number_to
)
from ..xiuxian_utils.lay_out import Cooldown
from ..xiuxian_config import XiuConfig

sql_message = XiuxianDateManage()
player_data = PlayerDataManager()


# ======================
#   本命法宝相关枚举与名字池
# ======================
class NatalEffectType(IntEnum):
    BLEED       = 1     # 流血
    ARMOR_BREAK = 2     # 破甲
    EVASION     = 3     # 闪避
    SHIELD      = 4     # 护盾


NATAL_TREASURE_NAMES = {
    NatalEffectType.BLEED: [
        "噬血魔刃", "赤焰血镰", "幽冥泣血"
    ],
    NatalEffectType.ARMOR_BREAK: [
        "裂空碎甲", "灭地魔戟", "破军星刃"
    ],
    NatalEffectType.EVASION: [
        "幻影流光", "瞬息千影", "虚空遁形"
    ],
    NatalEffectType.SHIELD: [
        "玄龟镇岳", "不灭金钟", "太初护魂"
    ]
}


# 效果类型的中文显示名称
EFFECT_NAME_MAP = {
    NatalEffectType.BLEED:       "流血",
    NatalEffectType.ARMOR_BREAK: "破甲",
    NatalEffectType.EVASION:     "闪避",
    NatalEffectType.SHIELD:      "护盾",
}


# ======================
#   本命法宝数据管理类
# ======================
class NatalTreasure:
    """本命法宝数据封装"""

    def __init__(self, user_id: int | str):
        self.user_id = str(user_id)
        self.table = "natal_treasure"

    def _ensure_record(self):
        """确保用户记录至少存在（会自动创建表 + 字段）"""
        data = player_data.get_fields(self.user_id, self.table)
        if data is None:
            default_data = {
                "form": 0,
                "name": "",
                "level": 0,
                "effect1_type": 0,
                "effect1_level": 0,
                "effect2_type": 0,
                "effect2_level": 0,
            }
            for field, value in default_data.items():
                player_data.update_or_write_data(
                    self.user_id, self.table, field, value
                )

    def exists(self) -> bool:
        self._ensure_record()
        form = player_data.get_field_data(self.user_id, self.table, "form")
        return form is not None and form != 0

    def awaken(self, force_new: bool = False):
        self._ensure_record()
        current_form = player_data.get_field_data(self.user_id, self.table, "form")

        if not current_form or force_new:
            form = random.randint(1, 4)
            player_data.update_or_write_data(self.user_id, self.table, "form", form)
        else:
            form = current_form

        # 决定单/双效果（可调整概率）
        is_double = random.random() < 0.30          # 30% 概率双效果

        selected_types = []
        if is_double:
            selected_types = random.sample(list(NatalEffectType), 2)
        else:
            selected_types = [random.choice(list(NatalEffectType))]

        # 名字暂时以第一个效果为准（可后续改进为组合名）
        first_type = selected_types[0]
        name_pool = NATAL_TREASURE_NAMES[first_type]
        selected_name = random.choice(name_pool)
        player_data.update_or_write_data(self.user_id, self.table, "name", selected_name)

        # 清空旧效果
        player_data.update_or_write_data(self.user_id, self.table, "effect1_type", 0)
        player_data.update_or_write_data(self.user_id, self.table, "effect1_level", 0)
        player_data.update_or_write_data(self.user_id, self.table, "effect2_type", 0)
        player_data.update_or_write_data(self.user_id, self.table, "effect2_level", 0)

        # 写入新效果
        for idx, etype in enumerate(selected_types, 1):
            field_type = f"effect{idx}_type"
            field_level = f"effect{idx}_level"
            init_level = 1                      # 双效果也可设更低，如 random.randint(1,2)

            player_data.update_or_write_data(self.user_id, self.table, field_type, etype.value)
            player_data.update_or_write_data(self.user_id, self.table, field_level, init_level)

    def upgrade(self):
        self._ensure_record()
        current_level = player_data.get_field_data(self.user_id, self.table, "level") or 0
        player_data.update_or_write_data(self.user_id, self.table, "level", current_level + 1)

        e1_type = player_data.get_field_data(self.user_id, self.table, "effect1_type")
        e2_type = player_data.get_field_data(self.user_id, self.table, "effect2_type")

        options = []

        # 单效果上限示例：10，双效果每个上限：8
        max_lv_single = 10
        max_lv_double = 8

        if e1_type and e1_type > 0:
            lv1 = player_data.get_field_data(self.user_id, self.table, "effect1_level") or 0
            max_lv = max_lv_single if not e2_type else max_lv_double
            if lv1 < max_lv:
                options.append("effect1_level")

        if e2_type and e2_type > 0:
            lv2 = player_data.get_field_data(self.user_id, self.table, "effect2_level") or 0
            max_lv = max_lv_single if not e1_type else max_lv_double
            if lv2 < max_lv:
                options.append("effect2_level")

        if options:
            field = random.choice(options)
            lv = player_data.get_field_data(self.user_id, self.table, field) or 0
            player_data.update_or_write_data(self.user_id, self.table, field, lv + 1)

    def get_data(self) -> dict | None:
        self._ensure_record()
        data = player_data.get_fields(self.user_id, self.table)
        if not data:
            return None
        return {k: v for k, v in data.items() if v is not None}

    def get_effect_desc(self) -> str:
        data = self.get_data()
        if not data or not data.get("form"):
            return "尚未觉醒本命法宝"

        lines = []
        name = data.get("name", "未知法宝")
        level = data.get("level", 0)

        lines.append(f"【{name}】  养成等级：{level}")

        for i in [1, 2]:
            etype = data.get(f"effect{i}_type")
            lv = data.get(f"effect{i}_level", 0)
            if etype and etype > 0:
                effect_name = EFFECT_NAME_MAP.get(NatalEffectType(etype), "未知效果")
                lines.append(f"  └─ {effect_name} Lv.{lv}")

        return "\n".join(lines)


# ======================
#        指令部分
# ======================

natal_awaken = on_command(
    "觉醒本命法宝",
    aliases={"本命觉醒", "觉醒法宝", "本命法宝觉醒"},
    priority=25,
    block=True
)


@natal_awaken.handle(parameterless=[Cooldown(cd_time=5)])
async def natal_awaken_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info['user_id']
    nt = NatalTreasure(user_id)

    if nt.exists():
        # 已存在 → 直接重置（生产环境建议加二次确认 + 消耗）
        nt.awaken(force_new=True)
        desc = nt.get_effect_desc()
        effect_count = "单效果" if player_data.get_field_data(user_id, nt.table, "effect2_type") in (0, None) else "双效果"
        await handle_send(bot, event, f"本命法宝已重塑！（{effect_count}）\n{desc}")
    else:
        # 首次觉醒
        nt.awaken()
        desc = nt.get_effect_desc()
        effect_count = "单效果" if player_data.get_field_data(user_id, nt.table, "effect2_type") in (0, None) else "双效果"
        await handle_send(bot, event, f"恭喜！本命法宝觉醒成功！（{effect_count}）\n{desc}")


natal_info = on_command(
    "我的本命法宝",
    aliases={"本命法宝", "法宝信息"},
    priority=25,
    block=True
)


@natal_info.handle(parameterless=[Cooldown(cd_time=3)])
async def natal_info_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info['user_id']
    nt = NatalTreasure(user_id)

    if not nt.exists():
        msg = "你尚未觉醒本命法宝！\n发送【觉醒本命法宝】进行首次觉醒"
    else:
        msg = nt.get_effect_desc()

    await handle_send(bot, event, msg)


natal_upgrade = on_command(
    "养成本命法宝",
    aliases={"法宝养成", "提升本命法宝"},
    priority=25,
    block=True
)


@natal_upgrade.handle(parameterless=[Cooldown(cd_time=5)])
async def natal_upgrade_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info['user_id']
    nt = NatalTreasure(user_id)

    if not nt.exists():
        msg = "你尚未觉醒本命法宝，无法养成！"
        await handle_send(bot, event, msg)
        return

    # 消耗示例：随总等级递增
    stone_cost = 500_0000 * (nt.get_data().get("level", 0) + 1)
    if user_info['stone'] < stone_cost:
        msg = f"本次养成需要{number_to(stone_cost)}灵石，你灵石不足！"
        await handle_send(bot, event, msg)
        return

    # 扣除灵石
    sql_message.update_ls(user_id, stone_cost, 2)

    nt.upgrade()
    level = player_data.get_field_data(user_id, nt.table, "level") or 0

    msg = f"本命法宝养成成功！\n当前总等级：{level}\n消耗灵石：{number_to(stone_cost)}"
    await handle_send(bot, event, msg)


natal_help = on_command("本命法宝帮助", priority=25, block=True)


@natal_help.handle(parameterless=[Cooldown(cd_time=3)])
async def natal_help_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = f"""
【本命法宝系统说明】

1. 觉醒方式
   发送：觉醒本命法宝
   首次觉醒随机获得形态，有一定概率获得双效果

2. 效果类型（4种）
   • 流血：每回合造成敌人最大生命值一定比例的真实伤害
   • 破甲：提升穿甲 / 降低敌人减伤
   • 闪避：提升自身闪避率
   • 护盾：开局获得一层基于最大生命值的护盾

3. 养成
   发送：养成本命法宝
   消耗灵石，提升总等级，并随机强化一个已有效果
   （双效果时单个效果成长上限会降低）

4. 开局额外伤害
   战斗开始时额外造成一次真实伤害（基础1%最大生命值，每级+0.3%）

5. 觉醒重置
   再次发送“觉醒本命法宝”可重塑（形态与效果全部随机）

当前支持单/双效果，后续可扩展更多效果或名字组合
"""
    await handle_send(bot, event, msg)