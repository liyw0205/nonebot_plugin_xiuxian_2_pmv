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
from .natal_config import (
    INVINCIBLE_GROWTH_PER_LEVEL_NATAL_TREASURE, # 导入无敌总等级成长系数
    MYSTERIOUS_SCRIPTURE_COST_ENGRAVE,          # 铭刻道纹消耗
    MYSTERIOUS_SCRIPTURE_COST_FORGET,           # 遗忘道纹消耗
    MAX_EFFECT_SLOTS,                           # 最大效果槽位
    EFFECT_NAME_TO_TYPE,                        # 效果名称到枚举的映射
    EFFECT_NAME_MAP                             # 效果类型到中文名称的映射
)

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
        await handle_send(bot, event, f"恭喜！本命法宝觉醒成功！\n{desc}\n首次觉醒默认获得一个道纹，你可以通过铭刻道纹来增加效果槽位。",
                           md_type="法宝", k1="法宝", v1="我的本命法宝", k2="铭刻", v2="铭刻道纹", k3="养成", v3="养成本命法宝")
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
    # 遍历所有效果槽位计算返还
    for i in range(1, MAX_EFFECT_SLOTS + 1):
        if old_nt_data.get(f"effect{i}_level", 0) > 1:
            refund_from_effect_upgrades += (old_nt_data[f"effect{i}_level"] - 1)
    
    # 实际需要消耗的神秘经书 = 基础消耗 - 效果升阶返还
    # 如果结果为负数，表示重塑后会有神秘经书返还，不需检查库存
    net_cost_to_check = scripture_cost_for_reawaken - refund_from_effect_upgrades
    
    scripture_num = sql_message.goods_num(user_id, MYSTERIOUS_SCRIPTURE_ID) # 获取用户背包中的神秘经书数量
    
    if net_cost_to_check > 0 and scripture_num < net_cost_to_check: # 只有当净消耗大于0时才检查库存
        await handle_send(bot, event, f"重塑本命法宝需要消耗{net_cost_to_check}个【{mysterious_scripture_info['name']}】，你目前只有{scripture_num}个，无法重塑！",
                           md_type="法宝", k1="觉醒", v1="觉醒本命法宝", k2="法宝", v2="我的本命法宝", k3="升阶", v3="本命法宝升阶")
        await natal_awaken.finish()

    msg = f"你已拥有本命法宝：\n\n{current_desc}\n\n"
    msg += f"重新觉醒将会【完全随机重塑名称、形态与初始道纹】，旧法宝将被覆盖，同时法宝等级和所有道纹等级将被重置。\n"
    
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
        for i in range(1, MAX_EFFECT_SLOTS + 1): # 遍历所有效果槽位计算返还
            if old_nt_data.get(f"effect{i}_level", 0) > 1:
                refund_from_effect_upgrades += (old_nt_data[f"effect{i}_level"] - 1)

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
                           md_type="法宝", k1="法宝", v1="我的本命法宝", k2="铭刻", v2="铭刻道纹", k3="养成", v3="养成本命法宝")
    
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
                       md_type="法宝", k1="觉醒", v1="觉醒本命法宝", k2="铭刻", v2="铭刻道纹", k3="养成", v3="养成本命法宝")


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

    # 计算当前等级到下一级所需的总经验
    max_exp_for_current_level_up = int(MAX_EXP_BASE + current_level * MAX_EXP_GROWTH_PER_LEVEL)
    current_exp_gained = nt_data.get("exp", 0) # 已经获得的当前等级经验
    remaining_exp_needed = max_exp_for_current_level_up - current_exp_gained # 当前等级还需多少经验才能升级
    
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
                       md_type="法宝", k1="法宝", v1="我的本命法宝", k2="铭刻", v2="铭刻道纹", k3="升阶", v3="本命法宝升阶")


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
        sql_message.update_back_j(user_id, MYSTERIOUS_SCRIPTURE_ID, num=-scripture_cost) # num为负数表示扣除
        await handle_send(bot, event, f"效果升阶成功！消耗{scripture_cost}个【神秘经书】。\n{upgrade_result_msg}",
                           md_type="法宝", k1="法宝", v1="我的本命法宝", k2="铭刻", v2="铭刻道纹", k3="升阶", v3="本命法宝升阶")
    else:
        await handle_send(bot, event, f"效果升阶失败：{upgrade_result_msg}",
                           md_type="法宝", k1="升阶", v1="本命法宝升阶", k2="法宝", v2="我的本命法宝", k3="帮助", v3="本命法宝帮助")


# 定义铭刻道纹命令
natal_engrave = on_command(
    "铭刻道纹",
    aliases={"铭刻", "法宝铭刻"},
    priority=25,
    block=True
)

@natal_engrave.handle(parameterless=[Cooldown(cd_time=5)])
async def natal_engrave_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """
    处理铭刻道纹命令。
    消耗神秘经书1个，为本命法宝增加随机新效果，本命法宝效果上限（3）时不可铭刻。
    """
    isUser, user_info, msg = check_user(event) # 检查用户是否已注册修仙
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info['user_id']
    nt = NatalTreasure(user_id) # 获取本命法宝实例

    if not nt.exists(): # 检查是否已觉醒法宝
        msg = "你尚未觉醒本命法宝，无法铭刻道纹！"
        await handle_send(bot, event, msg,
                           md_type="法宝", k1="觉醒", v1="觉醒本命法宝", k2="法宝", v2="我的本命法宝", k3="帮助", v3="本命法宝帮助")
        return
    
    nt_data = nt.get_data()
    current_effect_count = sum(1 for i in range(1, MAX_EFFECT_SLOTS + 1) if nt_data.get(f"effect{i}_type", 0) > 0)

    if current_effect_count >= MAX_EFFECT_SLOTS:
        await handle_send(bot, event, f"你的本命法宝效果槽位已满 ({MAX_EFFECT_SLOTS}个)，无法继续铭刻新的道纹。",
                           md_type="法宝", k1="法宝", v1="我的本命法宝", k2="升阶", v2="本命法宝升阶", k3="帮助", v3="本命法宝帮助")
        await natal_engrave.finish()

    scripture_cost = MYSTERIOUS_SCRIPTURE_COST_ENGRAVE # 铭刻道纹消耗
    scripture_num = sql_message.goods_num(user_id, MYSTERIOUS_SCRIPTURE_ID) # 获取用户背包中的神秘经书数量
    mysterious_scripture_info = items.get_data_by_item_id(MYSTERIOUS_SCRIPTURE_ID)

    if scripture_num < scripture_cost:
        await handle_send(bot, event, f"铭刻道纹需要消耗{scripture_cost}个【{mysterious_scripture_info['name']}】，你目前只有{scripture_num}个！",
                           md_type="法宝", k1="铭刻", v1="铭刻道纹", k2="法宝", v2="我的本命法宝", k3="觉醒", v3="觉醒本命法宝")
        await natal_engrave.finish()

    success, result_msg = nt.engrave_effect()

    if success:
        sql_message.update_back_j(user_id, MYSTERIOUS_SCRIPTURE_ID, num=-scripture_cost)
        await handle_send(bot, event, f"铭刻道纹成功！消耗{scripture_cost}个【神秘经书】。\n{result_msg}",
                           md_type="法宝", k1="法宝", v1="我的本命法宝", k2="养成", v2="养成本命法宝", k3="升阶", v3="本命法宝升阶")
    else:
        await handle_send(bot, event, f"铭刻道纹失败：{result_msg}",
                           md_type="法宝", k1="铭刻", v1="铭刻道纹", k2="法宝", v2="我的本命法宝", k3="帮助", v3="本命法宝帮助")
    await natal_engrave.finish()


# 定义遗忘道纹命令
natal_forget = on_command(
    "遗忘道纹",
    aliases={"遗忘", "法宝遗忘"},
    priority=25,
    block=True
)

@natal_forget.handle(parameterless=[Cooldown(cd_time=5)])
async def natal_forget_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    处理遗忘道纹命令。
    消耗神秘经书3个，去除本命法宝指定的一个效果。
    """
    isUser, user_info, msg = check_user(event) # 检查用户是否已注册修仙
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info['user_id']
    nt = NatalTreasure(user_id) # 获取本命法宝实例

    if not nt.exists(): # 检查是否已觉醒法宝
        msg = "你尚未觉醒本命法宝，无法遗忘道纹！"
        await handle_send(bot, event, msg,
                           md_type="法宝", k1="觉醒", v1="觉醒本命法宝", k2="法宝", v2="我的本命法宝", k3="帮助", v3="本命法宝帮助")
        return

    effect_name_to_forget = args.extract_plain_text().strip()
    if not effect_name_to_forget:
        await handle_send(bot, event, "请指定要遗忘的道纹名称，例如：遗忘道纹 流血",
                           md_type="法宝", k1="遗忘", v1="遗忘道纹 流血", k2="法宝", v2="我的本命法宝", k3="帮助", v3="本命法宝帮助")
        await natal_forget.finish()
    
    effect_type_to_forget = EFFECT_NAME_TO_TYPE.get(effect_name_to_forget)
    if not effect_type_to_forget:
        await handle_send(bot, event, f"未识别的道纹名称【{effect_name_to_forget}】，请检查输入或发送【本命法宝道纹帮助】了解所有道纹。", # 提示查看子帮助
                           md_type="法宝", k1="遗忘", v1="遗忘道纹", k2="帮助", v2="本命法宝帮助", k3="法宝", v3="我的本命法宝")
        await natal_forget.finish()

    nt_data = nt.get_data()
    current_effect_count = sum(1 for i in range(1, MAX_EFFECT_SLOTS + 1) if nt_data.get(f"effect{i}_type", 0) > 0)

    if current_effect_count <= 1:
        await handle_send(bot, event, "你的本命法宝至少需要保留一个道纹，无法遗忘！",
                           md_type="法宝", k1="法宝", v1="我的本命法宝", k2="铭刻", v2="铭刻道纹", k3="帮助", v3="本命法宝帮助")
        await natal_forget.finish()

    scripture_cost = MYSTERIOUS_SCRIPTURE_COST_FORGET # 遗忘道纹消耗
    scripture_num = sql_message.goods_num(user_id, MYSTERIOUS_SCRIPTURE_ID) # 获取用户背包中的神秘经书数量
    mysterious_scripture_info = items.get_data_by_item_id(MYSTERIOUS_SCRIPTURE_ID)

    if scripture_num < scripture_cost:
        await handle_send(bot, event, f"遗忘道纹需要消耗{scripture_cost}个【{mysterious_scripture_info['name']}】，你目前只有{scripture_num}个！",
                           md_type="法宝", k1="遗忘", v1="遗忘道纹", k2="法宝", v2="我的本命法宝", k3="铭刻", v3="铭刻道纹")
        await natal_forget.finish()

    success, result_msg = nt.forget_effect(effect_type_to_forget)

    if success:
        sql_message.update_back_j(user_id, MYSTERIOUS_SCRIPTURE_ID, num=-scripture_cost)
        await handle_send(bot, event, f"遗忘道纹成功！消耗{scripture_cost}个【神秘经书】。\n{result_msg}",
                           md_type="法宝", k1="法宝", v1="我的本命法宝", k2="铭刻", v2="铭刻道纹", k3="遗忘", v3="遗忘道纹")
    else:
        await handle_send(bot, event, f"遗忘道纹失败：{result_msg}",
                           md_type="法宝", k1="遗忘", v1="遗忘道纹", k2="法宝", v2="我的本命法宝", k3="帮助", v3="本命法宝帮助")
    await natal_forget.finish()


# ==== 新增：本命法宝主帮助命令 ====
natal_help = on_command("本命法宝帮助", aliases={"法宝帮助"}, priority=25, block=True)

@natal_help.handle(parameterless=[Cooldown(cd_time=3)])
async def natal_help_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """
    处理本命法宝主帮助命令，提供所有子帮助的入口。
    """
    msg = """
【本命法宝系统帮助】

这里是你的专属本命法宝系统，助你叱咤修仙界！
你可以通过以下命令了解更多：

1.  **管理操作**：发送【本命法宝操作帮助】
    > 觉醒、重塑、铭刻、遗忘道纹、养成、升阶

2.  **道纹详情**：发送【本命法宝道纹帮助】
    > 查看所有道纹的具体效果说明

3.  **战斗机制**：发送【本命法宝战斗帮助】
    > 了解法宝效果在战斗中如何触发和作用

4.  **查看信息**：发送【我的本命法宝】
    > 查看你的法宝当前状态

"""
    await handle_send(bot, event, msg,
                       md_type="法宝", k1="操作帮助", v1="本命法宝操作帮助", k2="道纹帮助", v2="本命法宝道纹帮助", k3="战斗帮助", v3="本命法宝战斗帮助")


# ==== 新增：本命法宝操作帮助命令 ====
natal_operation_help = on_command("本命法宝操作帮助", aliases={"法宝操作帮助"}, priority=25, block=True)

@natal_operation_help.handle(parameterless=[Cooldown(cd_time=3)])
async def natal_operation_help_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """
    处理本命法宝操作帮助命令，专注于管理操作。
    """
    msg = f"""
【本命法宝操作帮助】

1.  **觉醒法宝**
    >   发送：【觉醒本命法宝】
    >   首次觉醒免费，随机获得形态和初始一个道纹。
    >   再次发送【觉醒本命法宝】可以重塑（名称、形态与所有道纹全部随机），基础消耗1个【神秘经书】。
    >   重塑时会根据旧法宝的道纹升阶情况，返还相应的【神秘经书】。
    >   **注意：重塑会重置法宝等级、所有道纹等级和所有复活/无敌次数统计。**

2.  **铭刻道纹** (增加效果槽位)
    >   发送：【铭刻道纹】
    >   消耗{MYSTERIOUS_SCRIPTURE_COST_ENGRAVE}个【神秘经书】，为本命法宝增加一个随机新道纹。
    >   法宝最多可拥有{MAX_EFFECT_SLOTS}个道纹。

3.  **遗忘道纹** (移除指定效果)
    >   发送：【遗忘道纹 <道纹名称>】
    >   例如：【遗忘道纹 流血】
    >   消耗{MYSTERIOUS_SCRIPTURE_COST_FORGET}个【神秘经书】，去除法宝指定的某个道纹。
    >   法宝至少需要保留一个道纹。

4.  **养成法宝** (提升法宝总等级)
    >   发送：【养成本命法宝 (数量)】
    >   例如：【养成本命法宝 50】，默认【养成本命法宝 1】
    >   每次养成消耗灵石（消耗随法宝总等级递增），增加指定数量的法宝经验。
    >   经验满后法宝总等级提升1级，每级所需经验递增。
    >   法宝总等级上限{MAX_TREASURE_LEVEL}级。

5.  **道纹升阶** (提升法宝道纹等级)
    >   发送：【本命法宝升阶】
    >   每次升阶消耗1个【神秘经书】，提升一个法宝道纹的等级。
    >   法宝有多个道纹时，优先等级较低的道纹升阶；如果等级相同，则随机选择一个。
    >   所有道纹等级上限统一为{MAX_EFFECT_LEVEL_ALL_EFFECTS}级。

"""
    await handle_send(bot, event, msg,
                       md_type="法宝", k1="觉醒", v1="觉醒本命法宝", k2="铭刻", v2="铭刻道纹", k3="养成", v3="养成本命法宝")


# ==== 新增：本命法宝道纹帮助命令 ====
natal_effects_help = on_command("本命法宝道纹帮助", aliases={"法宝道纹帮助", "道纹帮助"}, priority=25, block=True)

@natal_effects_help.handle(parameterless=[Cooldown(cd_time=3)])
async def natal_effects_help_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """
    处理本命法宝道纹帮助命令，详细解释每种道纹效果。
    """
    # 仅用于获取常量，不进行数据操作
    death_strike_base_value = EFFECT_BASE_AND_GROWTH[NatalEffectType.DEATH_STRIKE]["min_value"] * 100
    shield_break_base_value = EFFECT_BASE_AND_GROWTH[NatalEffectType.SHIELD_BREAK]["min_value"] * 100

    invincible_first_base_chance = INVINCIBLE_FIRST_GAIN_CHANCE * 100
    invincible_sub_base_chance = INVINCIBLE_SUBSEQUENT_GAIN_CHANCE * 100
    invincible_growth_per_level = INVINCIBLE_GROWTH_PER_LEVEL_NATAL_TREASURE * 100 # 无敌是法宝总等级成长

    twin_strike_config = EFFECT_BASE_AND_GROWTH[NatalEffectType.TWIN_STRIKE]
    twin_strike_base_chance_single = twin_strike_config["min_value"] * 100
    twin_strike_effect_growth = twin_strike_config.get("growth", 0.0) * 100 # 双生是效果等级成长
    twin_strike_damage_multiplier = twin_strike_config["max_value"] * 100 # 双生伤害倍率固定100%

    sleep_chance_base = EFFECT_BASE_AND_GROWTH[NatalEffectType.SLEEP]["min_value"] * 100
    sleep_duration = SLEEP_DURATION

    petrify_chance_base = EFFECT_BASE_AND_GROWTH[NatalEffectType.PETRIFY]["min_value"] * 100
    petrify_duration = PETRIFY_DURATION

    stun_chance_base = EFFECT_BASE_AND_GROWTH[NatalEffectType.STUN]["min_value"] * 100
    stun_duration = STUN_DURATION

    fatigue_chance_base = EFFECT_BASE_AND_GROWTH[NatalEffectType.FATIGUE]["min_value"] * 100
    fatigue_duration = FATIGUE_DURATION
    fatigue_atk_reduction = FATIGUE_ATTACK_REDUCTION * 100

    silence_chance_base = EFFECT_BASE_AND_GROWTH[NatalEffectType.SILENCE]["min_value"] * 100
    silence_duration = SILENCE_DURATION

    charge_bonus_base = CHARGE_BONUS_DAMAGE * 100
    charge_effect_bonus_base = EFFECT_BASE_AND_GROWTH[NatalEffectType.CHARGE]["min_value"] * 100

    divine_power_bonus_base = EFFECT_BASE_AND_GROWTH[NatalEffectType.DIVINE_POWER]["min_value"] * 100

    nirvana_duration = NIRVANA_DURATION
    nirvana_shield_base = NIRVANA_SHIELD_BASE * 100
    nirvana_revive_limit = NIRVANA_REVIVE_LIMIT

    soul_return_duration = SOUL_RETURN_DURATION
    soul_return_hp_base = SOUL_RETURN_HP_BASE * 100
    soul_return_revive_limit = SOUL_RETURN_REVIVE_LIMIT
    
    # 新增效果的配置
    soul_summon_chance_base = EFFECT_BASE_AND_GROWTH[NatalEffectType.SOUL_SUMMON]["min_value"] * 100
    enlightenment_chance_base = EFFECT_BASE_AND_GROWTH[NatalEffectType.ENLIGHTENMENT]["min_value"] * 100
    enlightenment_revive_hp = ENLIGHTENMENT_REVIVE_HP_PERCENT * 100


    msg = f"""
【本命法宝道纹帮助】

共有{len(NatalEffectType)}种道纹效果：
• **{EFFECT_NAME_MAP[NatalEffectType.BLEED]}**：每回合对敌方造成最大生命值百分比的持续伤害。
• **{EFFECT_NAME_MAP[NatalEffectType.ARMOR_BREAK]}**：降低敌方防御，提升自身穿甲。
• **{EFFECT_NAME_MAP[NatalEffectType.EVASION]}**：提升自身闪避率。
• **{EFFECT_NAME_MAP[NatalEffectType.SHIELD]}**：开局获得护盾，周期性刷新。
• **{EFFECT_NAME_MAP[NatalEffectType.SHIELD_BREAK]}**：攻击有护盾的敌人时，无视其{round(shield_break_base_value, 2)}%护盾并额外造成{round(SHIELD_BREAK_BONUS_DAMAGE * 100, 2)}%伤害。
• **{EFFECT_NAME_MAP[NatalEffectType.REFLECT_DAMAGE]}**：被攻击时反还部分伤害给攻击者。
• **{EFFECT_NAME_MAP[NatalEffectType.TRUE_DAMAGE]}**：攻击时额外造成真实伤害，无视减伤和护盾。
• **{EFFECT_NAME_MAP[NatalEffectType.CRIT_RESIST]}**：减少被暴击时受到的伤害。
• **{EFFECT_NAME_MAP[NatalEffectType.FATE]}**：生命值低于0时有低概率恢复满血，每场战斗上限{FATE_REVIVE_COUNT_LIMIT}次。
• **{EFFECT_NAME_MAP[NatalEffectType.IMMORTAL]}**：生命值低于0时有50%概率恢复部分血量，每场战斗上限{IMMORTAL_REVIVE_COUNT_LIMIT}次。
• **{EFFECT_NAME_MAP[NatalEffectType.DEATH_STRIKE]}**：攻击方拥有此效果时，目标的天命效果被禁止。且当目标血量低于{death_strike_base_value:.0f}%时，对其造成致命打击直接斩杀。
• **{EFFECT_NAME_MAP[NatalEffectType.INVINCIBLE]}**：周期性获得无敌效果，可免疫下次所受到的所有伤害，存储上限{INVINCIBLE_COUNT_LIMIT}次。首次获得概率{round(invincible_first_base_chance, 2)}%，后续获得概率{round(invincible_sub_base_chance, 2)}%，法宝总等级每提升1级，获得概率增加{round(invincible_growth_per_level, 2)}%。
• **{EFFECT_NAME_MAP[NatalEffectType.TWIN_STRIKE]}**：普通攻击时有{round(twin_strike_base_chance_single, 2)}%概率触发连击，再造成一次额外{round(twin_strike_damage_multiplier, 2)}%伤害的攻击。道纹等级每提升1级，触发概率增加{round(twin_strike_effect_growth, 2)}%。
• **{EFFECT_NAME_MAP[NatalEffectType.SLEEP]}**：攻击时有{round(sleep_chance_base, 2)}%概率使目标睡眠{sleep_duration}回合。
• **{EFFECT_NAME_MAP[NatalEffectType.PETRIFY]}**：攻击时有{round(petrify_chance_base, 2)}%概率使目标石化{petrify_duration}回合。
• **{EFFECT_NAME_MAP[NatalEffectType.STUN]}**：攻击时有{round(stun_chance_base, 2)}%概率使目标眩晕{stun_duration}回合。
• **{EFFECT_NAME_MAP[NatalEffectType.FATIGUE]}**：攻击时有{round(fatigue_chance_base, 2)}%概率使目标疲劳{fatigue_duration}回合，攻击力降低{round(fatigue_atk_reduction, 2)}%。
• **{EFFECT_NAME_MAP[NatalEffectType.SILENCE]}**：攻击时有{round(silence_chance_base, 2)}%概率使目标沉默{silence_duration}回合。
• **{EFFECT_NAME_MAP[NatalEffectType.CHARGE]}**：本回合不攻击，下回合攻击力额外提升{round(charge_bonus_base + charge_effect_bonus_base, 2)}%（基础{round(charge_bonus_base, 2)}% + 道纹等级提升）。
• **{EFFECT_NAME_MAP[NatalEffectType.DIVINE_POWER]}**：攻击力额外提升{round(divine_power_bonus_base, 2)}%。
• **{EFFECT_NAME_MAP[NatalEffectType.NIRVANA]}**：阵亡时有队友在场，进入涅槃状态{nirvana_duration}回合后满血复活，并使友方全体获得最大生命{round(nirvana_shield_base, 2)}%护盾，仅{nirvana_revive_limit}次。涅槃期间免疫所有伤害，但若所有队友阵亡则复活失败。
• **{EFFECT_NAME_MAP[NatalEffectType.SOUL_RETURN]}**：阵亡时有队友在场，进入灵体状态{soul_return_duration}回合后回复最大生命{round(soul_return_hp_base, 2)}%复活，期间可正常攻击且免疫所有伤害，仅{soul_return_revive_limit}次。魂返期间只可进行普通攻击，且若所有队友阵亡则复活失败。
• **{EFFECT_NAME_MAP[NatalEffectType.SOUL_SUMMON]}**：攻击时有{round(soul_summon_chance_base, 2)}%概率让已死亡的队友进入魂返状态，仅队友战斗触发，每个队友仅可触发{SOUL_SUMMON_LIMIT}次。
• **{EFFECT_NAME_MAP[NatalEffectType.ENLIGHTENMENT]}**：攻击时有{round(enlightenment_chance_base, 2)}%概率让已死亡的队友回复{round(enlightenment_revive_hp, 2)}%生命值复活，仅队友战斗触发，每个队友仅可触发{ENLIGHTENMENT_LIMIT}次。

"""
    await handle_send(bot, event, msg,
                       md_type="法宝", k1="主帮助", v1="本命法宝帮助", k2="遗忘", v2="遗忘道纹", k3="查看", v3="我的本命法宝")

# ==== 新增：本命法宝战斗帮助命令 ====
natal_battle_help = on_command("本命法宝战斗帮助", aliases={"法宝战斗帮助", "战斗帮助"}, priority=25, block=True)

@natal_battle_help.handle(parameterless=[Cooldown(cd_time=3)])
async def natal_battle_help_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """
    处理本命法宝战斗帮助命令，解释战斗机制。
    """
    periodic_true_dmg_rate_base = PERIODIC_TRUE_DAMAGE_BASE * 100
    periodic_true_dmg_growth_rate = PERIODIC_TRUE_DAMAGE_GROWTH_PER_LEVEL * 100

    msg = f"""
【本命法宝战斗帮助】

本命法宝的道纹效果在战斗中会以不同方式触发：

1.  **道韵效果**：
    >   每4回合对所有敌方造成当前生命 **{round(periodic_true_dmg_rate_base, 2)}% + (法宝总等级 x {round(periodic_true_dmg_growth_rate, 2)}%)** 的真实伤害。

2.  **周期性效果** (每4回合触发一次)：
    >   流血、护盾、无敌、破甲、闪避等。

3.  **被动效果** (在攻击或受到伤害时自动生效)：
    >   破盾、反伤、真伤、抗暴、双生、控制类道纹（睡眠、石化、眩晕、疲劳、沉默）、蓄力、神力。

4.  **复活/支援效果** (在生命值变动、角色倒下或攻击时判定触发)：
    >   天命、不灭、斩命、涅槃、魂返、招魂、启明。

5.  **石化Debuff**：
    >   石化状态下被攻击伤害减免**{PETRIFY_DAMAGE_REDUCTION_PERCENT * 100:.0f}%**。

6.  **特殊说明**：
    >   某些效果可能存在内部冷却或每场战斗触发次数上限。具体请参阅【本命法宝道纹帮助】中各个道纹的详细描述。

"""
    await handle_send(bot, event, msg,
                       md_type="法宝", k1="道纹帮助", v1="本命法宝道纹帮助", k2="操作帮助", v2="本命法宝操作帮助", k3="主帮助", v3="本命法宝帮助")