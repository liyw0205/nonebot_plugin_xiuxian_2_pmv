import random
import asyncio
from datetime import datetime
from pathlib import Path
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
from nonebot.typing import T_State # 导入 T_State

from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from ..xiuxian_utils.utils import (
    check_user,
    handle_send,
    number_to
)
from ..xiuxian_utils.lay_out import Cooldown
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.item_json import Items # 导入 Items

from .natal_data import * # 导入本命法宝数据管理类和相关配置
from .natal_config import INVINCIBLE_GROWTH_PER_LEVEL_NATAL_TREASURE # 导入无敌总等级成长系数

items = Items()
sql_message = XiuxianDateManage()

# 定义觉醒本命法宝命令
natal_awaken = on_command(
    "觉醒本命法宝",
    aliases={"本命觉醒", "觉醒法宝", "本命法宝觉醒"},
    priority=25,
    block=True # 阻断其他插件处理此事件
)

@natal_awaken.handle(parameterless=[Cooldown(cd_time=5)])
async def natal_awaken_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, state: T_State):
    """
    处理觉醒本命法宝命令。
    首次觉醒免费，重塑需要消耗神秘经书。
    """
    isUser, user_info, msg = check_user(event) # 检查用户是否已注册修仙
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await natal_awaken.finish()

    user_id = user_info['user_id']
    nt = NatalTreasure(user_id) # 获取本命法宝实例

    if not nt.exists(): # 如果用户尚未觉醒本命法宝
        nt.awaken() # 执行首次觉醒
        desc = nt.get_effect_desc() # 获取法宝描述
        await handle_send(bot, event, f"恭喜！本命法宝觉醒成功！\n{desc}",
                           md_type="法宝", k1="法宝", v1="我的本命法宝", k2="养成", v2="养成本命法宝", k3="升阶", v3="本命法宝升阶")
        await natal_awaken.finish()

    # 如果用户已拥有本命法宝，则进入重塑逻辑
    state["user_id"] = user_id # 将user_id存入state
    state["nt"] = nt # 将NatalTreasure实例存入state，以便后续确认环节使用

    current_desc = nt.get_effect_desc() # 获取当前法宝描述
    
    # 检查用户是否有足够的神秘经书进行重塑
    scripture_cost_for_reawaken = 1 # 重塑的基础消耗
    mysterious_scripture_info = items.get_data_by_item_id(MYSTERIOUS_SCRIPTURE_ID) # 获取神秘经书信息

    # 预计算可能的返还，以判断是否足够进行重塑（至少要能支付基础消耗）
    old_nt_data = nt.get_data()
    
    # 计算效果升阶的返还数量
    refund_from_effect_upgrades = 0
    # 效果1的返还（如果等级大于1）
    if old_nt_data.get("effect1_level", 0) > 1:
        # 每个效果等级大于1的部分返还1个神秘经书
        refund_from_effect_upgrades += (old_nt_data["effect1_level"] - 1)
    # 效果2的返还（如果存在且等级大于1）
    if old_nt_data.get("effect2_type", 0) != 0 and old_nt_data.get("effect2_level", 0) > 1:
        refund_from_effect_upgrades += (old_nt_data["effect2_level"] - 1)
    
    # 实际需要消耗的神秘经书 = 基础消耗 - 效果升阶返还
    # 如果结果为负数，表示重塑后会有神秘经书返还，不需检查库存
    net_cost_to_check = scripture_cost_for_reawaken - refund_from_effect_upgrades
    
    scripture_num = sql_message.goods_num(user_id, MYSTERIOUS_SCRIPTURE_ID) # 获取用户背包中的神秘经书数量
    
    if net_cost_to_check > 0 and scripture_num < net_cost_to_check: # 只有当净消耗大于0时才检查库存
        await handle_send(bot, event, f"重塑本命法宝需要消耗{net_cost_to_check}个【{mysterious_scripture_info['name']}】，你目前只有{scripture_num}个，无法重塑！",
                           md_type="法宝", k1="觉醒", v1="觉醒本命法宝", k2="法宝", v2="我的本命法宝", k3="升阶", v3="本命法宝升阶")
        await natal_awaken.finish()

    msg = f"你已拥有本命法宝：\n\n{current_desc}\n\n"
    msg += f"重新觉醒将会【完全随机重塑名称、形态与效果】，旧法宝将被覆盖，同时法宝等级和效果等级将被重置。\n"
    
    if net_cost_to_check > 0:
        msg += f"本次重塑需要消耗 {net_cost_to_check} 个【{mysterious_scripture_info['name']}】。"
    elif net_cost_to_check < 0:
        msg += f"本次重塑将返还 {abs(net_cost_to_check)} 个【{mysterious_scripture_info['name']}】。"
    else:
        msg += f"本次重塑无需额外消耗【{mysterious_scripture_info['name']}】。"
    
    msg += "\n确定要重塑本命法宝吗？\n\n"
    msg += "回复：【确定】 继续重塑\n"
    msg += "回复：【取消】 或其他内容 放弃操作"

    await handle_send(bot, event, msg, md_type="法宝", k1="确定", v1="确定", k2="取消", v2="取消", k3="法宝", v3="我的本命法宝")
    # 进入等待用户输入的挂起状态
    # NoneBot会自动将下一个匹配不到其他事件的普通消息路由到当前handler的receive()方法

@natal_awaken.receive()
async def natal_awaken_confirm(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, state: T_State):
    """
    处理觉醒本命法宝命令的用户确认。
    """
    user_input = event.get_plaintext().strip() # 获取用户输入

    # 状态中必须有user_id和nt对象
    if "user_id" not in state or "nt" not in state:
        await natal_awaken.finish("状态异常，已自动取消")

    user_id = state["user_id"]
    nt: NatalTreasure = state["nt"] # 从state中取出NatalTreasure实例
    
    scripture_cost_for_reawaken = 1 # 重塑的基础消耗
    mysterious_scripture_info = items.get_data_by_item_id(MYSTERIOUS_SCRIPTURE_ID) # 获取神秘经书信息

    # 判断用户意图
    if user_input in ["确定", "确认", "重塑", "是", "ok", "y", "yes"]:
        # 获取旧法宝数据，计算返还
        old_nt_data = nt.get_data()
        refund_from_effect_upgrades = 0
        if old_nt_data.get("effect1_level", 0) > 1:
            refund_from_effect_upgrades += (old_nt_data["effect1_level"] - 1)
        if old_nt_data.get("effect2_type", 0) != 0 and old_nt_data.get("effect2_level", 0) > 1: # 效果2存在才计算
            refund_from_effect_upgrades += (old_nt_data["effect2_level"] - 1)

        # 计算最终的神秘经书变动 (负数表示消耗，正数表示获得)
        total_scripture_change = refund_from_effect_upgrades - scripture_cost_for_reawaken
        
        # 再次检查神秘经书数量，防止在确认过程中被消耗（只检查需要消耗的情况）
        if total_scripture_change < 0: # 如果是净消耗
            scripture_num = sql_message.goods_num(user_id, MYSTERIOUS_SCRIPTURE_ID)
            if scripture_num < abs(total_scripture_change):
                await handle_send(bot, event, f"神秘经书不足{abs(total_scripture_change)}个【{mysterious_scripture_info['name']}】，重塑失败！",
                                   md_type="法宝", k1="觉醒", v1="觉醒本命法宝", k2="法宝", v2="我的本命法宝", k3="升阶", v3="本命法宝升阶")
                await natal_awaken.finish()
        
        # 执行神秘经书增减
        if total_scripture_change != 0:
            # update_back_j的num是正数时扣除，负数时增加。
            # 这里 total_scripture_change 如果是正数表示返还，负数表示消耗。
            # 所以直接传入 total_scripture_change 的负数，即可实现正确的扣除或增加
            sql_message.update_back_j(user_id, MYSTERIOUS_SCRIPTURE_ID, num=-total_scripture_change) 

        # 执行重塑 (force_new=True会清除所有效果和等级，以及次数统计)
        nt.awaken(force_new=True)
        
        desc = nt.get_effect_desc() # 获取新法宝的描述
        
        result_msg = f"本命法宝已重塑！\n{desc}\n"
        if total_scripture_change > 0:
            result_msg += f"返还了{total_scripture_change}个【{mysterious_scripture_info['name']}】。"
        elif total_scripture_change < 0:
            result_msg += f"消耗了{abs(total_scripture_change)}个【{mysterious_scripture_info['name']}】。"
        else:
            result_msg += f"本次重塑无需额外消耗【{mysterious_scripture_info['name']}】。"


        await handle_send(bot, event, result_msg,
                           md_type="法宝", k1="法宝", v1="我的本命法宝", k2="养成", v2="养成本命法宝", k3="升阶", v3="本命法宝升阶")
    
    else:
        # 取消或其他非确认输入
        await handle_send(bot, event, "已取消本命法宝重塑操作～",
                           md_type="法宝", k1="觉醒", v1="觉醒本命法宝", k2="法宝", v2="我的本命法宝", k3="帮助", v3="本命法宝帮助")

    # 结束会话
    await natal_awaken.finish()


# 定义查看本命法宝信息命令
natal_info = on_command(
    "我的本命法宝",
    aliases={"本命法宝", "法宝信息"},
    priority=25,
    block=True
)


@natal_info.handle(parameterless=[Cooldown(cd_time=3)])
async def natal_info_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """
    处理查看本命法宝信息命令。
    """
    isUser, user_info, msg = check_user(event) # 检查用户是否已注册修仙
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info['user_id']
    nt = NatalTreasure(user_id) # 获取本命法宝实例

    msg = nt.get_effect_desc() # 统一通过类方法获取信息描述

    await handle_send(bot, event, msg,
                       md_type="法宝", k1="觉醒", v1="觉醒本命法宝", k2="养成", v2="养成本命法宝", k3="升阶", v3="本命法宝升阶")


# 定义养成本命法宝命令
natal_upgrade = on_command(
    "养成本命法宝",
    aliases={"法宝养成", "提升本命法宝"},
    priority=25,
    block=True
)


@natal_upgrade.handle(parameterless=[Cooldown(cd_time=5)])
async def natal_upgrade_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    处理养成本命法宝命令。
    每次养成消耗灵石，增加法宝经验。
    支持指定经验数量，例如：养成本命法宝 50
    """
    isUser, user_info, msg = check_user(event) # 检查用户是否已注册修仙
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info['user_id']
    nt = NatalTreasure(user_id) # 获取本命法宝实例

    if not nt.exists(): # 检查是否已觉醒法宝
        msg = "你尚未觉醒本命法宝，无法养成！"
        await handle_send(bot, event, msg,
                           md_type="法宝", k1="觉醒", v1="觉醒本命法宝", k2="法宝", v2="我的本命法宝", k3="帮助", v3="本命法宝帮助")
        return
    
    nt_data = nt.get_data()
    current_level = nt_data.get("level", 0)

    if current_level >= nt.max_treasure_level: # 检查是否已达最高等级
        msg = f"你的本命法宝已达最高等级 {nt.max_treasure_level}，无法继续养成。"
        await handle_send(bot, event, msg,
                           md_type="法宝", k1="法宝", v1="我的本命法宝", k2="升阶", v2="本命法宝升阶", k3="帮助", v3="本命法宝帮助")
        return
    
    # 解析经验数量参数
    exp_amount_str = args.extract_plain_text().strip()
    try:
        exp_to_add = int(exp_amount_str) if exp_amount_str else 1 # 默认增加1点经验
        if exp_to_add <= 0:
            await handle_send(bot, event, "养成的经验数量必须是正整数！",
                               md_type="法宝", k1="养成", v1="养成本命法宝", k2="升阶", v2="本命法宝升阶", k3="法宝", v3="我的本命法宝")
            return
    except ValueError:
        await handle_send(bot, event, "养成的经验数量必须是整数！",
                           md_type="法宝", k1="养成", v1="养成本命法宝", k2="升阶", v2="本命法宝升阶", k3="法宝", v3="我的本命法宝")
        return

    # **新增逻辑：限制经验传参上限，使其不超过当前等级所需的剩余经验**
    current_exp = nt_data.get("exp", 0)
    max_exp_for_current_level_up = nt_data.get("max_exp", 100) # 当前等级升阶所需的总经验
    remaining_exp_needed = max_exp_for_current_level_up - current_exp # 当前等级还需多少经验才能升级
    
    # 如果当前经验已满，则直接提示，避免继续计算和扣费
    if remaining_exp_needed <= 0:
        await handle_send(bot, event, "你的本命法宝当前等级经验已满，请等待自动升级或继续养成以触发升级。",
                           md_type="法宝", k1="法宝", v1="我的本命法宝", k2="升阶", v2="本命法宝升阶", k3="养成", v3="养成本命法宝")
        return

    # 实际要增加的经验不能超过当前等级升级所需的剩余经验
    original_exp_to_add = exp_to_add
    if exp_to_add > remaining_exp_needed:
        exp_to_add = remaining_exp_needed
        # 这里发送提示后，继续执行扣费和加经验逻辑
        await handle_send(bot, event, f"你本次最多只能再增加{remaining_exp_needed}点经验达到当前等级上限，已为你调整为{exp_to_add}点。",
                           md_type="法宝", k1="养成", v1="养成本命法宝", k2="升阶", v2="本命法宝升阶", k3="法宝", v3="我的本命法宝")


    # 养成方案：每次养成操作消耗灵石，增加1点经验
    base_cost_per_exp = 1_000_000 # 从0级到1级时，获得1点经验所需的基础灵石
    cost_per_level_increase_rate = 0.5 # 每提升1级法宝总等级，消耗灵石增加50%

    # 计算每点经验的灵石消耗，基于当前的【法宝总等级】
    stone_cost_per_exp_unit = int(base_cost_per_exp * (1 + current_level * cost_per_level_increase_rate))
    
    total_stone_cost = stone_cost_per_exp_unit * exp_to_add

    if user_info['stone'] < total_stone_cost: # 检查灵石是否足够
        msg = f"本次养成{exp_to_add}点经验需要{number_to(total_stone_cost)}灵石，你灵石不足！"
        await handle_send(bot, event, msg,
                           md_type="法宝", k1="养成", v1="养成本命法宝", k2="法宝", v2="我的本命法宝", k3="灵石", v3="灵石")
        return

    # 扣除灵石
    sql_message.update_ls(user_id, total_stone_cost, 2) # type=2表示扣除
    
    # 增加本命法宝经验，并处理升级
    is_level_up, upgrade_msg = nt.add_exp(exp_to_add)
    
    final_msg = f"成功养成法宝，消耗灵石：{number_to(total_stone_cost)}\n{upgrade_msg}"
    await handle_send(bot, event, final_msg,
                       md_type="法宝", k1="法宝", v1="我的本命法宝", k2="养成", v2="养成本命法宝", k3="升阶", v3="本命法宝升阶")


# 定义本命法宝效果升阶命令
natal_effect_upgrade = on_command(
    "本命法宝升阶",
    aliases={"法宝升阶", "升阶本命法宝", "本命效果升阶"},
    priority=25,
    block=True
)

@natal_effect_upgrade.handle(parameterless=[Cooldown(cd_time=5)])
async def natal_effect_upgrade_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """
    处理本命法宝效果升阶命令。
    消耗神秘经书提升法宝效果等级。
    """
    isUser, user_info, msg = check_user(event) # 检查用户是否已注册修仙
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info['user_id']
    nt = NatalTreasure(user_id) # 获取本命法宝实例

    if not nt.exists(): # 检查是否已觉醒法宝
        msg = "你尚未觉醒本命法宝，无法进行效果升阶！"
        await handle_send(bot, event, msg,
                           md_type="法宝", k1="觉醒", v1="觉醒本命法宝", k2="法宝", v2="我的本命法宝", k3="帮助", v3="本命法宝帮助")
        return
    
    # 效果升阶消耗：1个神秘经书
    scripture_cost = 1
    scripture_num = sql_message.goods_num(user_id, MYSTERIOUS_SCRIPTURE_ID) # 获取用户背包中的神秘经书数量
    if scripture_num < scripture_cost: # 检查神秘经书是否足够
        mysterious_scripture_info = items.get_data_by_item_id(MYSTERIOUS_SCRIPTURE_ID)
        await handle_send(bot, event, f"效果升阶需要消耗{scripture_cost}个【{mysterious_scripture_info['name']}】，你目前只有{scripture_num}个！",
                           md_type="法宝", k1="升阶", v1="本命法宝升阶", k2="法宝", v2="我的本命法宝", k3="觉醒", v3="觉醒本命法宝")
        return

    # 尝试提升一个效果等级
    success, upgrade_result_msg = nt.upgrade_single_effect_level()

    if success:
        # 扣除神秘经书
        sql_message.update_back_j(user_id, MYSTERIOUS_SCRIPTURE_ID, num=scripture_cost) # num为正数时扣除
        await handle_send(bot, event, f"效果升阶成功！消耗{scripture_cost}个【神秘经书】。\n{upgrade_result_msg}",
                           md_type="法宝", k1="法宝", v1="我的本命法宝", k2="养成", v2="养成本命法宝", k3="升阶", v3="本命法宝升阶")
    else:
        await handle_send(bot, event, f"效果升阶失败：{upgrade_result_msg}",
                           md_type="法宝", k1="升阶", v1="本命法宝升阶", k2="法宝", v2="我的本命法宝", k3="帮助", v3="本命法宝帮助")


# 定义本命法宝帮助命令
natal_help = on_command("本命法宝帮助", priority=25, block=True)


@natal_help.handle(parameterless=[Cooldown(cd_time=3)])
async def natal_help_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """
    处理本命法宝帮助命令，显示系统说明。
    """
    # 仅用于获取常量，不进行数据操作
    # 斩命和破盾的基础值需要直接从配置中获取，因为它们没有Effect Level Growth来显示
    death_strike_base_value = EFFECT_BASE_AND_GROWTH[NatalEffectType.DEATH_STRIKE]["min_single"] * 100
    shield_break_base_value = EFFECT_BASE_AND_GROWTH[NatalEffectType.SHIELD_BREAK]["min_single"] * 100

    # 获取无敌和双生的初始值和成长值
    invincible_first_base_chance = INVINCIBLE_FIRST_GAIN_CHANCE * 100
    invincible_sub_base_chance = INVINCIBLE_SUBSEQUENT_GAIN_CHANCE * 100
    invincible_growth_per_level = INVINCIBLE_GROWTH_PER_LEVEL_NATAL_TREASURE * 100 # 无敌是法宝总等级成长

    twin_strike_config = EFFECT_BASE_AND_GROWTH[NatalEffectType.TWIN_STRIKE]
    twin_strike_base_chance_single = twin_strike_config["min_single"] * 100
    twin_strike_effect_growth = twin_strike_config.get("growth", 0.0) * 100 # 双生是效果等级成长
    twin_strike_damage_multiplier = twin_strike_config["max_single"] * 100 # 双生伤害倍率固定100%

    msg = f"""
【本命法宝】

1. 觉醒方式
>   发送：【觉醒本命法宝】
   首次觉醒免费，随机获得形态和效果，有75%概率获得单效果，25%概率获得双效果。
   再次发送【觉醒本命法宝】可以重塑（形态与效果全部随机），基础消耗1个【神秘经书】。
   重塑时会根据旧法宝的效果升阶情况，返还相应的【神秘经书】。
   注意：重塑会重置法宝等级、效果等级和所有复活/无敌次数统计。

2. 养成 (提升法宝总等级)
>   发送：【养成本命法宝 (数量)】
   例如：【养成本命法宝 50】，默认【养成本命法宝 1】
   每次养成消耗灵石（消耗随法宝总等级递增），增加指定数量的法宝经验。
   经验满后法宝总等级提升1级。
   法宝总等级上限{MAX_TREASURE_LEVEL}级。

3. 升阶 (提升法宝效果等级)
>   发送：【本命法宝升阶】
   每次升阶消耗1个【神秘经书】，提升一个法宝效果的等级。
   法宝有多个效果时，优先等级较低的效果升阶；如果等级相同，则随机选择一个。
   单效果等级上限{MAX_EFFECT_LEVEL_SINGLE}级，双效果每个等级上限{MAX_EFFECT_LEVEL_DOUBLE}级。

4. 效果类型说明
>   共有{len(NatalEffectType)}种效果：
   • 流血：每回合对敌方造成最大生命值百分比的持续伤害。
   • 破甲：降低敌方防御，提升自身穿甲。
   • 闪避：提升自身闪避率。
   • 护盾：战斗开局获得护盾，周期性刷新。
   • 破盾：攻击有护盾的敌人时，无视其{round(shield_break_base_value, 2)}%护盾并额外造成{round(SHIELD_BREAK_BONUS_DAMAGE * 100, 2)}%伤害。
   • 反伤：被攻击时反还部分伤害给攻击者。
   • 真伤：攻击时额外造成真实伤害，无视减伤和护盾。
   • 抗暴：减少被暴击时受到的伤害。
   • 天命：生命值低于0时有低概率恢复满血，每场战斗上限{FATE_REVIVE_COUNT_LIMIT}次。
   • 不灭：生命值低于0时有50%概率触发恢复部分血量，每场战斗上限{IMMORTAL_REVIVE_COUNT_LIMIT}次。
   • 斩命：攻击方拥有此效果时，目标的天命效果被禁止。且当目标血量低于{death_strike_base_value:.0f}%时，对其造成致命打击直接斩杀。
   • 无敌：周期性获得无敌效果，可免疫下次所受到的所有伤害，存储上限{INVINCIBLE_COUNT_LIMIT}次。首次获得概率{round(invincible_first_base_chance, 2)}%，后续获得概率{round(invincible_sub_base_chance, 2)}%，法宝总等级每提升1级，获得概率增加{round(invincible_growth_per_level, 2)}%。
   • 双生：普通攻击时有{round(twin_strike_base_chance_single, 2)}%概率触发连击，再造成一次额外{round(twin_strike_damage_multiplier, 2)}%伤害的攻击。效果等级每提升1级，触发概率增加{round(twin_strike_effect_growth, 2)}%。

5. 战斗中效果触发
>   • 道韵：每4回合对所有敌方造成当前生命 {round(PERIODIC_TRUE_DAMAGE_BASE * 100, 2)}% + 法宝总等级x{round(PERIODIC_TRUE_DAMAGE_GROWTH_PER_LEVEL * 100, 2)}% 的真实伤害。
   • 周期性效果（流血、护盾、无敌、破甲、闪避）：每4回合触发一次。
   • 被动效果（破盾、反伤、真伤、抗暴、双生）：在攻击或受到伤害时自动生效。
   • 复活/斩杀效果（天命、不灭、斩命）：在生命值变动或角色倒下时自动判定触发。

6. 查看信息
   发送：【我的本命法宝】或【法宝信息】查看法宝详情。
"""
    await handle_send(bot, event, msg,
                       md_type="法宝", k1="觉醒", v1="觉醒本命法宝", k2="养成", v2="养成本命法宝", k3="升阶", v3="本命法宝升阶")