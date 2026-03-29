import asyncio
import random
import time
import re
import os
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any
from nonebot import on_command, require
from ..adapter_compat import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
)
from nonebot.log import logger
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER
from ..xiuxian_utils.lay_out import assign_bot, assign_bot_group, Cooldown, CooldownIsolateLevel
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import (
    check_user, get_msg_pic, 
    send_msg_handler,
    Txt2Img, number_to, handle_send
)
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, get_weapon_info_msg, get_armor_info_msg,
    get_sec_msg, get_main_info_msg, get_sub_info_msg, UserBuffDate, OtherSet
)
from ..xiuxian_rift import use_rift_explore, use_rift_key, use_rift_boss, use_rift_speedup, use_rift_big_speedup
from ..xiuxian_impart import use_wishing_stone, use_love_sand
from ..xiuxian_work import use_work_order, use_work_capture_order
from ..xiuxian_buff import use_two_exp_token
from ..xiuxian_config import XiuConfig, convert_rank, added_ranks
from .back_util import *
from urllib.parse import quote


# 初始化组件
items = Items()
sql_message = XiuxianDateManage()
scheduler = require("nonebot_plugin_apscheduler").scheduler
added_ranks = added_ranks()
# 技能学习确认缓存
confirm_use_cache = {}
# 通用物品类型和炼金最低价格
MIN_PRICE = 600000

# 物品类型映射，用于快速炼金等功能
type_mapping = {
    "装备": ["法器", "防具"],
    "技能": ["功法", "神通", "辅修功法", "身法", "瞳术"],
    "功法": ["功法"],
    "神通": ["神通"],
    "辅修功法": ["辅修功法"],
    "身法": ["身法"],
    "瞳术": ["瞳术"],
    "法器": ["法器"],
    "防具": ["防具"],
    "药材": ["药材"],
    "全部": ["法器", "防具", "药材", "功法", "神通", "辅修功法", "身法", "瞳术"]
}

# 品阶映射，用于快速炼金等功能
rank_map = {
    # --- 装备品阶 ---
    "符器": ["下品符器", "上品符器"],
    "法器": ["下品法器", "上品法器"],
    "玄器": ["下品玄器", "上品玄器"],
    "纯阳": ["下品纯阳", "上品纯阳"],
    "纯阳法器": ["下品纯阳法器", "上品纯阳法器"],
    "通天": ["下品通天", "上品通天"],
    "通天法器": ["下品通天法器", "上品通天法器"],
    "仙器": ["下品仙器", "上品仙器"],
    "极品仙器": ["极品仙器"], # 仙器中的最高品阶
    "无上": ["无上"], # 超越仙器的品阶

    "下品符器": ["下品符器"],
    "上品符器": ["上品符器"],
    "下品法器": ["下品法器"],
    "上品法器": ["上品法器"],
    "下品玄器": ["下品玄器"],
    "上品玄器": ["上品玄器"],
    "下品纯阳": ["下品纯阳"],
    "上品纯阳": ["上品纯阳"],
    "下品纯阳法器": ["下品纯阳法器"],
    "上品纯阳法器": ["上品纯阳法器"],
    "下品通天": ["下品通天"],
    "上品通天": ["上品通天"],
    "下品通天法器": ["下品通天法器"],
    "上品通天法器": ["上品通天法器"],
    "下品仙器": ["下品仙器"],
    "上品仙器": ["上品仙器"],
    
    # --- 药材品阶 ---
    "一品药材": ["一品药材"],
    "二品药材": ["二品药材"],
    "三品药材": ["三品药材"],
    "四品药材": ["四品药材"],
    "五品药材": ["五品药材"],
    "六品药材": ["六品药材"],
    "七品药材": ["七品药材"],
    "八品药材": ["八品药材"],
    "九品药材": ["九品药材"],
    
    # --- 功法品阶 ---
    "人阶下品": "人阶下品",
    "人阶上品": "人阶上品",
    "黄阶下品": "黄阶下品",
    "黄阶上品": "黄阶上品",
    "玄阶下品": "玄阶下品",
    "玄阶上品": "玄阶上品",
    "地阶下品": "地阶下品",
    "地阶上品": "地阶上品",
    "天阶下品": "天阶下品",
    "天阶上品": "天阶上品",
    "仙阶下品": "仙阶下品",
    "仙阶上品": "仙阶上品",
    "仙阶极品": "仙阶极品", # 仙阶中的最高品阶
    "人阶": ["人阶下品", "人阶上品"],
    "黄阶": ["黄阶下品", "黄阶上品"],
    "玄阶": ["玄阶下品", "玄阶上品"],
    "地阶": ["地阶下品", "地阶上品"],
    "天阶": ["天阶下品", "天阶上品"],
    "仙阶": ["仙阶下品", "仙阶上品", "仙阶极品"],
    
    # --- 全部品阶（不包含仙器、九品药材和仙阶功法） ---
    "全部": [
        # 装备
        "下品符器", "上品符器", "下品法器", "上品法器", "下品玄器", "上品玄器",
        "下品纯阳", "上品纯阳", "下品纯阳法器", "上品纯阳法器", 
        "下品通天", "上品通天", "下品通天法器", "上品通天法器",
        # 药材
        "一品药材", "二品药材", "三品药材", "四品药材",
        "五品药材", "六品药材", "七品药材", "八品药材",
        # 功法
        "人阶下品", "人阶上品", "黄阶下品", "黄阶上品",
        "玄阶下品", "玄阶上品", "地阶下品", "地阶上品",
        "天阶下品", "天阶上品"
    ]
}

# 禁止炼金的物品ID
BANNED_ITEM_IDS_ALCHEMY = ["15357", "9935", "9940"]

# 背包相关命令
chakan_wupin = on_command("查看修仙界物品", aliases={"查看"}, priority=20, block=True)
check_item_effect = on_command("查看效果", aliases={"查", "效果"}, priority=25, block=True)
goods_re_root = on_command("炼金", priority=6, block=True)
fast_alchemy = on_command("快速炼金", aliases={"一键炼金"}, priority=6, block=True)
main_back = on_command('我的背包', aliases={'我的物品'}, priority=10, block=True)
yaocai_back = on_command('药材背包', priority=10, block=True)
yaocai_detail_back = on_command('药材背包详细', aliases={'药材背包详情'}, priority=10, block=True)
danyao_back = on_command('丹药背包', priority=10, block=True)
my_equipment = on_command("我的装备", priority=10, block=True)
use_item = on_command("道具使用", priority=15, block=True)
use = on_command("使用", priority=15, block=True)
confirm_use = on_command('确认使用', aliases={"确认学习"}, priority=15, block=True)
no_use_zb = on_command("换装", aliases={'卸装'}, priority=5, block=True)
back_help = on_command("背包帮助", priority=8, block=True)
xiuxian_sone = on_command("灵石", priority=4, block=True)
compare_items = on_command("快速对比", priority=5, block=True)

# 管理员命令
check_user_equipment = on_command("装备检测", permission=SUPERUSER, priority=6, block=True)
check_user_back = on_command("背包检测", permission=SUPERUSER, priority=6, block=True)


def get_recover(goods_id, num):
    """
    根据物品ID和数量计算炼金后获得的灵石数量。
    灵石数量与物品品阶和数量相关。
    """
    # 假设品阶越高，炼金价格越高
    # convert_rank('江湖好手')[0] 获取最高境界的数字表示，这里作为基准
    # added_ranks 是一个修正值
    price = int((convert_rank('江湖好手')[0] - added_ranks) - get_item_msg_rank(goods_id)) * 100000
    # 确保价格在最低和最高炼金价之间
    price = min(max(price, MIN_PRICE), 5500000) * num
    return price

@check_item_effect.handle(parameterless=[Cooldown(cd_time=1.4)])
async def check_item_effect_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看物品效果，支持物品名或ID"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    # 检查用户是否已注册修仙
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await check_item_effect.finish()

    # 获取用户输入的物品名或ID
    input_str = args.extract_plain_text().strip()
    if not input_str:
        msg = "请输入物品名称或ID！\n例如：查看效果 渡厄丹 或 查看效果 1999"
        await handle_send(bot, event, msg, md_type="背包", k1="效果", v1="查看效果", k2="物品", v2="查看修仙界物品", k3="帮助", v3="修仙帮助")
        await check_item_effect.finish()

    # 判断输入是ID还是名称
    goods_id, goods_info = items.get_data_by_item_name(input_str)
    if not goods_id:
        msg = f"物品 {input_str} 不存在，请检查名称是否正确！"
        await handle_send(bot, event, msg, md_type="背包", k1="效果", v1="查看效果", k2="物品", v2="查看修仙界物品", k3="帮助", v3="修仙帮助")
        return
    item_msg = get_item_msg(goods_id, user_info['user_id'])
    if goods_id == 15053 or input_str == "补偿": # 某些特定物品可能不需要显示效果
        await check_item_effect.finish()
    # 构造返回消息
    msg = f"\nID：{goods_id}\n{item_msg}"
    await handle_send(bot, event, msg, md_type="背包", k1="效果", v1="查看效果", k2="物品", v2="查看修仙界物品", k3="帮助", v3="修仙帮助")
    await check_item_effect.finish()
    
@back_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def back_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """背包帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    msg = """
【背包帮助】
🔹 我的背包 [页码] - 查看背包物品
🔹 药材背包 [页码] - 查看药材类物品
🔹 丹药背包 [页码] - 查看丹药类物品
🔹 我的装备 [页码] - 查看背包装备
🔹 使用+物品名 [数量] - 使用物品
🔹 换装/卸装+装备名 - 卸下装备
🔹 炼金+物品名 [数量] - 将物品转化为灵石
🔹 快速炼金 类型 品阶 - 批量炼金指定类型物品
🔹 查看修仙界物品+类型 [页码] - 查看物品图鉴
🔹 查看效果+物品名 - 查看物品详情
🔹 灵石 - 查看当前灵石数量
🔹 快速对比 [物品1] [物品2] - 对比装备或者功法的属性
"""

    await handle_send(bot, event, msg)
    await back_help.finish()

@xiuxian_sone.handle(parameterless=[Cooldown(cd_time=1.4)])
async def xiuxian_sone_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的灵石信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await xiuxian_sone.finish()
    msg = f"当前灵石：{user_info['stone']}({number_to(user_info['stone'])})"
    await handle_send(bot, event, msg)
    await xiuxian_sone.finish()

def get_item_type_by_id(goods_id):
    """根据物品ID获取类型"""
    return items.get_data_by_item_id(goods_id)['type']

@goods_re_root.handle(parameterless=[Cooldown(cd_time=1.4)])
async def goods_re_root_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """炼金"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await goods_re_root.finish()
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    if not args:
        msg = "请输入要炼化的物品！"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await goods_re_root.finish()
        
    # 判断输入是ID还是名称
    item_name = args[0]
    # 检查背包物品
    goods_id, goods_info = items.get_data_by_item_name(item_name)
    if not goods_id:
        msg = f"物品 {item_name} 不存在，请检查名称是否正确！"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        return
    goods_num = sql_message.goods_num(user_info['user_id'], goods_id)
    if goods_num <= 0:
        msg = f"背包中没有足够的 {item_name} ！"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        return

    # 检查是否是禁止炼金的物品
    if str(goods_id) in BANNED_ITEM_IDS_ALCHEMY:
        msg = f"物品 {item_name} 禁止炼金！"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await goods_re_root.finish()

    if get_item_msg_rank(goods_id) == 520: # 520通常表示不支持的物品类型
        msg = "此类物品不支持炼金！"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await goods_re_root.finish()
    num = 1
    try:
        if len(args) > 1 and 1 <= int(args[1]) <= int(goods_num):
            num = int(args[1])
        elif len(args) > 1 and int(args[1]) > int(goods_num):
            msg = f"道友背包中的{item_name}数量不足，当前仅有{goods_num}个！"
            await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
            await goods_re_root.finish()
    except ValueError: # 如果第二个参数不是有效数字，则默认为1
            num = 1 
    
    price = get_recover(goods_id, num)
    if price <= 0: # 某些物品炼金价格可能为0或负数
        msg = f"物品：{item_name}炼金失败，凝聚{number_to(price)}枚灵石！"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await goods_re_root.finish()

    sql_message.update_back_j(user_id, goods_id, num=num)
    sql_message.update_ls(user_id, price, 1)
    msg = f"物品：{item_name} 数量：{num} 炼金成功，凝聚{number_to(price)}枚灵石！"
    await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
    await goods_re_root.finish()

@fast_alchemy.handle(parameterless=[Cooldown(cd_time=1.4)])
async def fast_alchemy_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """快速炼金（支持装备/药材/全部类型 + 全部品阶，以及回血丹）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await fast_alchemy.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    # === 特殊处理回血丹 ===
    if len(args) > 0 and args[0] == "回血丹":
        back_msg = sql_message.get_back_msg(user_id)
        if not back_msg:
            msg = "💼 道友的背包空空如也！"
            await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="快速炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
            await fast_alchemy.finish()
        
        # 筛选回血丹（buff_type为hp的丹药）
        elixirs = []
        for item in back_msg:
            item_info = items.get_data_by_item_id(item['goods_id'])
            if (item_info and item_info['type'] == "丹药" 
                and item_info.get('buff_type') == "hp"):
                # 回血丹都是绑定的，直接使用goods_num
                available = item['goods_num']
                if available > 0:
                    elixirs.append({
                        'id': item['goods_id'],
                        'name': item['goods_name'],
                        'num': available,
                        'info': item_info
                    })
        
        if not elixirs:
            msg = "🔍 背包中没有回血丹！"
            await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="快速炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
            await fast_alchemy.finish()
        
        # 执行炼金
        total_stone = 0
        results = []
        
        for elixir in elixirs:
            # 计算价格
            total_price = get_recover(elixir['id'], elixir['num'])
            
            # 从背包扣除
            sql_message.update_back_j(user_id, elixir['id'], num=elixir['num'])
            
            # 增加灵石
            sql_message.update_ls(user_id, total_price, 1)
            
            total_stone += total_price
            results.append(f"{elixir['name']} x{elixir['num']} → {number_to(total_price)}灵石")
        
        # 构建结果消息
        msg = [
            f"\n☆------快速炼金结果------☆",
            f"类型：回血丹",
            *results,
            f"总计获得：{number_to(total_stone)}灵石"
        ]
        await send_msg_handler(bot, event, '快速炼金', bot.self_id, msg)
        await fast_alchemy.finish()
    
    # === 原有类型处理逻辑 ===
    # 指令格式检查
    if len(args) < 1:
        msg = "指令格式：快速炼金 [类型] [品阶]\n" \
              "▶ 类型：装备|法器|防具|药材|回血丹|全部\n" \
              "▶ 品阶：全部|人阶|黄阶|...|上品通天法器（输入'品阶帮助'查看完整列表）"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="快速炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await fast_alchemy.finish()
    
    item_type = args[0]  # 物品类型
    rank_name = " ".join(args[1:]) if len(args) > 1 else "全部"  # 品阶
    
    if item_type not in type_mapping:
        msg = f"❌ 无效类型！可用类型：{', '.join(type_mapping.keys())}"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="快速炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await fast_alchemy.finish()
    
    if rank_name not in rank_map:
        msg = f"❌ 无效品阶！输入'品阶帮助'查看完整列表"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="快速炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await fast_alchemy.finish()
    
    # === 获取背包物品 ===
    back_msg = sql_message.get_back_msg(user_id)
    if not back_msg:
        msg = "💼 道友的背包空空如也！"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="快速炼in", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await fast_alchemy.finish()
    
    # === 筛选物品 ===
    target_types = type_mapping[item_type]
    target_ranks = rank_map[rank_name]
    
    items_to_alchemy = []
    for item in back_msg:
        item_info = items.get_data_by_item_id(item['goods_id'])
        if not item_info:
            continue
            
        # 类型匹配
        type_match = (
            item['goods_type'] in target_types or 
            item_info.get('item_type', '') in target_types
        )
        
        # 品阶匹配
        rank_match = item_info.get('level', '') in target_ranks
        
        if type_match and rank_match:
            # 对于装备类型，检查是否已被使用
            if item['goods_type'] == "装备":
                is_equipped = check_equipment_use_msg(user_id, item['goods_id'])
                if is_equipped:
                    # 如果装备已被使用，可炼金数量 = 总数量 - 绑定数量 - 1（已装备的）
                    available_num = item['goods_num'] - item['bind_num'] - 1
                else:
                    # 如果未装备，可炼金数量 = 总数量 - 绑定数量
                    available_num = item['goods_num'] - item['bind_num']
            else:
                # 非装备物品，正常计算
                available_num = item['goods_num'] - item['bind_num']
            
            # 确保可用数量不为负
            available_num = max(0, available_num)
            
            # 检查是否是禁止炼金的物品
            if str(item['goods_id']) in BANNED_ITEM_IDS_ALCHEMY:
                logger.opt(colors=True).info(f"<yellow>物品 {item['goods_name']} 禁止炼金，已跳过。</yellow>")
                continue
            
            if available_num > 0:
                items_to_alchemy.append({
                    'id': item['goods_id'],
                    'name': item['goods_name'],
                    'type': item['goods_type'],
                    'available_num': available_num,
                    'info': item_info,
                    'is_equipped': check_equipment_use_msg(user_id, item['goods_id']) if item['goods_type'] == "装备" else False
                })
    
    if not items_to_alchemy:
        msg = f"🔍 背包中没有符合条件的【{item_type}·{rank_name}】物品"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="快速炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await fast_alchemy.finish()
    
    # === 自动炼金逻辑 ===
    success_count = 0
    total_stone = 0
    result_msg = []
    
    for item in items_to_alchemy:
        
        # 计算价格
        total_price = get_recover(item['id'], item['available_num'])
        
        # 从背包扣除
        sql_message.update_back_j(user_id, item['id'], num=item['available_num'])
        
        # 增加灵石
        sql_message.update_ls(user_id, total_price, 1)
        
        success_count += item['available_num']
        total_stone += total_price
        
        # 添加装备状态信息到结果消息
        status_info = ""
        if item['type'] == "装备" and item['is_equipped']:
            status_info = " (已装备物品，保留1个)"
        
        result_msg.append(f"{item['name']} x{item['available_num']}{status_info} → {number_to(total_price)}灵石")
    
    # 构建结果消息
    msg = [
        f"\n☆------快速炼金结果------☆",
        f"类型：{item_type}",
        f"品阶：{rank_name}",
        *result_msg,
        f"总计获得：{number_to(total_stone)}灵石"
    ]
    
    await send_msg_handler(bot, event, '快速炼金', bot.self_id, msg)
    await fast_alchemy.finish()

@no_use_zb.handle(parameterless=[Cooldown(cd_time=1.4)])
async def no_use_zb_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """卸载物品（只支持装备）
    ["user_id", "goods_id", "goods_name", "goods_type", "goods_num", "create_time", "update_time",
    "remake", "day_num", "all_num", "action_time", "state"]
    """
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await no_use_zb.finish()
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()

    back_msg = sql_message.get_back_msg(user_id)  # 背包sql信息,list(back)
    if back_msg is None:
        msg = "道友的背包空空如也！"
        await handle_send(bot, event, msg, md_type="背包", k1="卸装", v1="卸装", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
        await no_use_zb.finish()
    in_flag = False  # 判断指令是否正确，道具是否在背包内
    goods_id = None
    goods_type = None
    for back in back_msg:
        if arg == back['goods_name']:
            in_flag = True
            goods_id = back['goods_id']
            goods_type = back['goods_type']
            break
    if not in_flag:
        msg = f"请检查道具 {arg} 是否在背包内！"
        await handle_send(bot, event, msg, md_type="背包", k1="卸装", v1="卸装", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
        await no_use_zb.finish()

    if goods_type == "装备":
        if check_equipment_use_msg(user_id, goods_id): # 检查装备是否在使用中
            sql_str, item_type = get_no_use_equipment_sql(user_id, goods_id)
            for sql in sql_str:
                sql_message.update_back_equipment(sql)
            if item_type == "法器":
                sql_message.updata_user_faqi_buff(user_id, 0)
            if item_type == "防具":
                sql_message.updata_user_armor_buff(user_id, 0)
            msg = f"成功卸载装备{arg}！"
            await handle_send(bot, event, msg, md_type="背包", k1="卸装", v1="卸装", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
            await no_use_zb.finish()
        else:
            msg = "装备没有被使用，无法卸载！"
            await handle_send(bot, event, msg, md_type="背包", k1="卸装", v1="卸装", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
            await no_use_zb.finish()
    else:
        msg = "目前只支持卸载装备！"
        await handle_send(bot, event, msg, md_type="背包", k1="卸装", v1="卸装", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
        await no_use_zb.finish()

@use.handle(parameterless=[Cooldown(cd_time=1.4)])
async def use_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """使用物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await use.finish()
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    if not args:
        msg = "请输入要使用的物品名称！"
        await handle_send(bot, event, msg, md_type="背包", k1="使用", v1="使用", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
        await use.finish()
    
    if str(user_id) in confirm_use_cache:
        del confirm_use_cache[str(user_id)]

    item_name = args[0]  
    goods_id, goods_info = items.get_data_by_item_name(item_name)
    if not goods_id:
        msg = f"物品 {item_name} 不存在，请检查名称是否正确！"
        await handle_send(bot, event, msg, md_type="背包", k1="使用", v1="使用", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
        return
    goods_num = sql_message.goods_num(user_info['user_id'], goods_id)
    if goods_num <= 0:
        msg = f"背包中没有足够的 {item_name} ！"
        await handle_send(bot, event, msg, md_type="背包", k1="使用", v1="使用", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
        return
    
    num = 1
    try:
        if len(args) > 1 and 1 <= int(args[1]) <= int(goods_num):
            num = int(args[1])
        elif len(args) > 1 and int(args[1]) > int(goods_num):
            msg = f"道友背包中的{item_name}数量不足，当前仅有{goods_num}个！"
            await handle_send(bot, event, msg, md_type="背包", k1="使用", v1="使用", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
            await use.finish()
    except ValueError:
        num = 1
    
    user_rank = convert_rank(user_info['level'])[0]
    goods_type = goods_info['type']
    required_rank_name, goods_rank_calculated = get_required_rank_name(goods_info, user_info)
    lh_msg = ""
    if user_info['root_type'] in ["轮回道果", "真·轮回道果", "永恒道果", "命运道果"]:
        lh_msg = "\n轮回重修：境界限制下降！"
        
    if goods_type == "礼包":
        package_name = goods_info['name']
        msg_parts = []
        i = 1
        while True:
            buff_key, name_key, type_key, amount_key = f'buff_{i}', f'name_{i}', f'type_{i}', f'amount_{i}'
            if name_key not in goods_info: break
            item_name_pkg, item_amount = goods_info[name_key], goods_info.get(amount_key, 1) * num
            item_type_pkg, buff_id_pkg = goods_info.get(type_key), goods_info.get(buff_key)
            if item_name_pkg == "灵石":
                sql_message.update_ls(user_id, abs(item_amount), 1 if item_amount > 0 else 2)
                msg_parts.append(f"获得灵石 {number_to(item_amount)} 枚\n")
            else:
                g_type = "技能" if item_type_pkg in ["辅修功法", "神通", "功法", "身法", "瞳术"] else "装备" if item_type_pkg in ["法器", "防具"] else item_type_pkg
                if buff_id_pkg is not None:
                    sql_message.send_back(user_id, buff_id_pkg, item_name_pkg, g_type, item_amount, 1)
                    msg_parts.append(f"获得 {item_name_pkg} x{item_amount}\n")
            i += 1
        sql_message.update_back_j(user_id, goods_id, num=num, use_key=1)
        msg = f"道友打开了 {num} 个 {package_name}:\n" + "".join(msg_parts)

    elif goods_type == "装备":
        if goods_rank_calculated <= user_rank: 
             msg = f"道友实力不足使用{goods_info['name']}\n请提升至：{required_rank_name}{lh_msg}"
        elif check_equipment_use_msg(user_id, goods_id): 
            msg = "该装备已被装备，请勿重复装备！"
        else: 
            sql_str, item_type = get_use_equipment_sql(user_id, goods_id)
            for sql in sql_str: sql_message.update_back_equipment(sql)
            if item_type == "法器": sql_message.updata_user_faqi_buff(user_id, goods_id)
            if item_type == "防具": sql_message.updata_user_armor_buff(user_id, goods_id)
            msg = f"成功装备 {item_name}！"

    elif goods_type == "技能":
        user_buff_info = UserBuffDate(user_id).BuffInfo
        skill_type = goods_info['item_type']
        if goods_rank_calculated <= user_rank: 
             msg = f"道友实力不足学习{goods_info['name']}\n请提升至：{required_rank_name}{lh_msg}"
        else:
            check_map = {"神通": 'sec_buff', "身法": 'effect1_buff', "瞳术": 'effect2_buff', "功法": 'main_buff', "辅修功法": 'sub_buff'}
            if int(user_buff_info.get(check_map.get(skill_type), 0)) == int(goods_id):
                msg = f"道友已学会该{skill_type}：{item_name}，请勿重复学习！"
            else:
                await confirm_use_invite(bot, event, user_id, goods_id, item_name, skill_type)
                return

    elif goods_type == "丹药":
        msg = check_use_elixir(user_id, goods_id, num)
    elif goods_type == "特殊道具": 
        msg = f"请使用【道具使用 {goods_info['name']}】命令来使用此道具。"
    elif goods_type == "神物": 
        user_info_full = sql_message.get_user_info_with_id(user_id)
        if (goods_info['rank'] + added_ranks) < convert_rank(user_info_full['level'])[0]:
            msg = f"神物：{goods_info['name']}的使用境界为{goods_info['境界']}以上，道友不满足条件！"
        else:
            exp = goods_info['buff'] * num
            sql_message.update_exp(user_id, exp)
            sql_message.update_power2(user_id)
            sql_message.update_user_attribute(user_id, int(user_info_full['hp'] + (exp / 2)), int(user_info_full['mp'] + exp), int(user_info_full['atk'] + (exp / 10)))
            sql_message.update_back_j(user_id, goods_id, num=num, use_key=1)
            msg = f"道友成功使用神物：{goods_info['name']} {num} 个，修为增加 {number_to(exp)}！"
    elif goods_type == "聚灵旗": 
        msg = get_use_jlq_msg(user_id, goods_id)
    else:
        msg = "该类型物品调试中，未开启使用！"

    await handle_send(bot, event, msg, md_type="背包", k1="使用", v1="使用", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
    await use.finish()

async def confirm_use_invite(bot, event, user_id, goods_id, item_name, skill_type):
    """发送确认使用"""
    invite_id = f"{user_id}_use_{datetime.now().timestamp()}"
    confirm_use_cache[str(user_id)] = {
        'goods_id': goods_id,
        'item_name': item_name,
        'skill_type': skill_type,
        'invite_id': invite_id
    }
    asyncio.create_task(expire_confirm_use_invite(user_id, invite_id, bot, event))
    msg = f"道友确定要学习【{skill_type}：{item_name}】吗？\n此操作将消耗物品，请在30秒内发送【确认使用】！"
    await handle_send(bot, event, msg, md_type="背包", k1="确认", v1="确认使用", k2="背包", v2="我的背包")

async def expire_confirm_use_invite(user_id, invite_id, bot, event):
    """确认使用过期"""
    await asyncio.sleep(30)
    if str(user_id) in confirm_use_cache and confirm_use_cache[str(user_id)]['invite_id'] == invite_id:
        del confirm_use_cache[str(user_id)]

@confirm_use.handle(parameterless=[Cooldown(cd_time=1.4)])
async def confirm_use_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理确认使用"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await confirm_use.finish()
    user_id = user_info['user_id']
    if str(user_id) not in confirm_use_cache:
        msg = "没有待处理的请求！"
        await handle_send(bot, event, msg)
        await confirm_use.finish()
    data = confirm_use_cache[str(user_id)]
    gid, name, s_type = data['goods_id'], data['item_name'], data['skill_type']
    if sql_message.goods_num(user_id, gid) <= 0:
        msg = f"背包中已无 {name}！"
    else:
        update_map = {"神通": sql_message.updata_user_sec_buff, "身法": sql_message.updata_user_effect1_buff, "瞳术": sql_message.updata_user_effect2_buff, "功法": sql_message.updata_user_main_buff, "辅修功法": sql_message.updata_user_sub_buff}
        sql_message.update_back_j(user_id, gid)
        update_map[s_type](user_id, gid)
        msg = f"恭喜道友成功学会{s_type}：{name}！"
    await handle_send(bot, event, msg, md_type="背包", k1="背包", v1="我的背包")
    del confirm_use_cache[str(user_id)]
    await confirm_use.finish()

@use_item.handle(parameterless=[Cooldown(cd_time=1.4)])
async def use_item_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """道具使用 - 用于使用特殊道具"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await use_item.finish()
    
    user_id = user_info['user_id']
    if isinstance(args, str):
        args_text = args.strip()
    else:
        # 正常用户调用
        args_text = args.extract_plain_text().strip()
    
    if not args_text:
        msg = "请输入要使用的道具名称！格式：道具使用 物品名 [数量]"
        await handle_send(bot, event, msg, md_type="背包", k1="使用", v1="道具使用", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
        await use_item.finish()
    
    # 解析物品名和数量
    parts = args_text.split()
    item_name = parts[0]
    quantity = 1
    
    if len(parts) > 1:
        try:
            quantity = int(parts[1])
            quantity = max(1, min(quantity, 100))  # 限制使用数量1-100
        except ValueError:
            msg = "请输入有效的数量！"
            await handle_send(bot, event, msg, md_type="背包", k1="使用", v1="道具使用", k2="存档", v2="我的修ian信息", k3="背包", v3="我的背包")
            await use_item.finish()
    
    # 检查背包物品
    goods_id, goods_info = items.get_data_by_item_name(item_name)
    if not goods_id:
        msg = f"物品 {item_name} 不存在，请检查名称是否正确！"
        await handle_send(bot, event, msg, md_type="背包", k1="使用", v1="道具使用", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
        return
    
    # 检查是否是特殊道具
    if goods_info['type'] != "特殊道具":
        msg = f"{item_name} 不是特殊道具，请使用【使用 {item_name}】命令！"
        await handle_send(bot, event, msg, md_type="背包", k1="使用", v1=f"使用 {item_name}", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
        await use_item.finish()
        
    goods_num = sql_message.goods_num(user_info['user_id'], goods_id)
    if goods_num <= 0:
        msg = f"背包中没有足够的 {item_name} ！"
        await handle_send(bot, event, msg, md_type="背包", k1="使用", v1="道具使用", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
        return
    
    # 检查数量是否足够
    if goods_num < quantity:
        quantity = goods_num

    # 特殊道具的处理函数映射
    ITEM_HANDLERS = {
        20005: use_wishing_stone, # 祈愿石
        20016: use_love_sand,     # 思恋沙
        20007: use_rift_explore,  # 秘境探索令
        20001: use_rift_key,      # 秘境钥匙
        20018: use_rift_boss,     # 秘境挑战令
        20012: use_rift_speedup,  # 秘境加速卡
        20013: use_rift_big_speedup, # 秘境高级加速卡
        20010: use_lottery_talisman, # 灵签宝箓
        20014: use_work_order,       # 悬赏令
        20015: use_work_capture_order, # 捕获悬赏令
        20017: use_two_exp_token,    # 双修令牌
        20019: use_unbind_charm,     # 解绑符
        20020: use_spirit_stone_bag, # 灵石福袋
        20021: use_tianji_stone_trigger, # 天机灵石引
        20022: use_three_cultivation_pill # 三转玄丹
    }
    
    handler_func = ITEM_HANDLERS.get(goods_id, None)
    if handler_func:
        # 调用对应的处理函数
        await handler_func(bot, event, goods_id, quantity)
    else:
        msg = f"{item_name} 是特殊道具，但目前没有对应的使用方法！"
        await handle_send(bot, event, msg, md_type="背包", k1="使用", v1="道具使用", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
        
    await use_item.finish()

async def use_lottery_talisman(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id: int, num: int):
    """使用灵签宝箓"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    user_id = user_info["user_id"]
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
        
    # 批量处理使用灵签宝箓
    success_count = 0
    obtained_items_dict = {} # 使用字典汇总获得的物品及数量
    
    for _ in range(num):
        # 50%概率判断成功
        roll = random.randint(1, 100)
        if roll <= 50:
            success_count += 1
            
            # 随机选择防具或法器类型
            item_type = random.choice(["防具", "法器"])
            # 随机生成品阶，min(随机数, 54) 确保不超过最大品阶
            zx_rank = random.randint(5, 10) # 基础品阶范围
            item_rank = min(random.randint(zx_rank, zx_rank + 50), 54)
            # 提高低品阶物品的概率（如果roll到5且不是100，则强制变为16，即下品符器）
            if item_rank == 5 and random.randint(1, 100) != 100:
                item_rank = 16
            
            # 获取随机物品
            item_id_list = items.get_random_id_list_by_rank_and_item_type(item_rank, item_type)
            if item_id_list:
                rank_id = random.choice(item_id_list)
                item_info = items.get_data_by_item_id(rank_id)
                
                # 给予物品
                sql_message.send_back(
                    user_id, 
                    rank_id, 
                    item_info["name"], 
                    item_info["type"], 
                    1,
                    1
                )
                
                # 汇总获得的物品
                obtained_items_dict[item_info["name"]] = obtained_items_dict.get(item_info["name"], 0) + 1
    
    # 批量消耗灵签宝箓
    sql_message.update_back_j(user_id, item_id, num=num)
    
    # 构建结果消息
    items_msg_list = [f"{name} x{count}" for name, count in obtained_items_dict.items()]
    
    if success_count > 0:
        result_msg = f"道友使用灵签宝箓 {num} 个，成功获得以下物品：\n" + "\n".join(items_msg_list)
    else:
        result_msg = f"道友使用灵签宝箓 {num} 个，未能获得任何物品，运气不佳啊！"
    
    await handle_send(bot, event, result_msg)
    return

async def use_unbind_charm(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id: int, num: int):
    """使用解绑符解除物品绑定状态"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    user_id = user_info["user_id"]
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    # 解析参数，获取要解绑的物品名称
    args_text = event.get_plaintext().strip()
    args_text = re.sub(r'^道具使用\s*', '', args_text).strip() # 移除指令前缀
    
    # 假设格式是 "道具使用 解绑符 <数量> <物品名>"
    # 例如："道具使用 解绑符 1 天罪"
    # parts[0] 是 "解绑符"， parts[1] 是数量， parts[2] 是物品名
    parts = args_text.split()
    
    if len(parts) < 3: # 至少需要 "解绑符", "数量", "物品名"
        msg = f"格式错误！正确格式：道具使用 解绑符 [数量] 物品名\n例如：道具使用 解绑符 1 天罪"
        await handle_send(bot, event, msg)
        return
    
    target_item_name = parts[2]  # 要解绑的物品名称
    
    # 检查要解绑的物品是否存在
    target_goods_id, target_goods_info = items.get_data_by_item_name(target_item_name)
    if not target_goods_id:
        msg = f"物品 {target_item_name} 不存在，请检查名称是否正确！"
        await handle_send(bot, event, msg)
        return

    if target_goods_info['type'] not in ["技能", "装备"]: # 只有技能和装备可以解绑
        msg = f"物品 {target_item_name} 类型不支持解绑，请更换物品！"
        await handle_send(bot, event, msg)
        return

    # 检查背包中是否有要解绑的物品
    # 注意：这里调用的是 goods_num(..., num_type=None)，获取的是总数量，不是绑定数量
    # 应该先检查是否有绑定数量，才能解绑
    target_goods_total_num = sql_message.goods_num(user_id, target_goods_id)
    if target_goods_total_num <= 0:
        msg = f"背包中没有 {target_item_name} ！"
        await handle_send(bot, event, msg)
        return
    
    # 获取物品的绑定数量
    bind_num_in_db = sql_message.goods_num(user_id, target_goods_id, num_type='bind')
    if bind_num_in_db <= 0:
        msg = f"{target_item_name} 没有绑定数量，无需解绑！"
        await handle_send(bot, event, msg)
        return
    
    # 计算实际可解绑的数量，不能超过解绑符数量，也不能超过物品的绑定数量
    actual_unbind_quantity = min(num, bind_num_in_db)
    
    if actual_unbind_quantity <= 0:
        msg = f"没有足够的可解绑数量或解绑符数量不足！"
        await handle_send(bot, event, msg)
        return
    
    # 使用解绑符解绑物品
    success = sql_message.unbind_item(user_id, target_goods_id, actual_unbind_quantity)
    
    if success:
        # 消耗解绑符
        sql_message.update_back_j(user_id, item_id, num=num) # 消耗用户输入的解绑符数量
        
        msg = f"成功使用解绑符，解除了 {target_item_name} 的 {actual_unbind_quantity} 个绑定状态！"
    else:
        msg = "解绑失败，请稍后重试！"
    
    await handle_send(bot, event, msg)

async def use_spirit_stone_bag(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id: int, num: int):
    """使用灵石福袋"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info["user_id"]

    # 灵石福袋的掉落阶梯及概率
    # (权重, 灵石数量, 描述)
    tiers = [
        (72,   5120000,  "普通"),
        (23, 12800000,  "优质"),
        (5,  64800000,  "极品！✨")
    ]

    total_stone = 0
    results = []

    for _ in range(num):
        # 根据权重随机选择灵石数量
        roll_stone = random.choices(
            [t[1] for t in tiers],
            weights=[t[0] for t in tiers],
            k=1
        )[0]

        # 获取对应的描述
        desc = next(t[2] for t in tiers if t[1] == roll_stone)
        total_stone += roll_stone
        results.append(f"{desc}档：获得 {number_to(roll_stone)} 灵石")

    # 增加用户灵石 & 扣除道具
    sql_message.update_ls(user_id, total_stone, 1)
    sql_message.update_back_j(user_id, item_id, num=num)

    # 构造消息
    lines = [
        f"☆── 灵石福袋 ×{num} ──☆",
        f"累计获得：{number_to(total_stone)} 灵石",
        "──────────────",
        *results,
        "──────────────",
        "祝道友财源滚滚～"
    ]

    await handle_send(bot, event, "\n".join(lines))

async def use_tianji_stone_trigger(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id: int, num: int):
    """使用天机灵石引"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info["user_id"]

    MIN_STONE = 10_000_000
    MAX_STONE = 100_000_000
    total_stone = 0
    results = []

    for i in range(num):
        roll = random.randint(MIN_STONE, MAX_STONE)
        total_stone += roll

        if roll <= 15_000_000:
            desc = "微薄"
        elif roll <= 50_000_000:
            desc = "尚可"
        elif roll <= 75_000_000:
            desc = "丰厚"
        else:
            desc = "惊人！✨"

        results.append(f"第{i+1}次：{desc} → {number_to(roll)} 灵石")

    # 增加用户灵石 & 扣除道具
    sql_message.update_ls(user_id, total_stone, 1)
    sql_message.update_back_j(user_id, item_id, num=num)

    # 构造消息
    lines = [
        f"☆── 天机灵石引 ×{num} ──☆",
        f"累计获得：{number_to(total_stone)} 灵石",
        "──────────────",
        *results,
        "──────────────",
        "天机莫测，道友保重～"
    ]

    await handle_send(bot, event, "\n".join(lines))

async def use_three_cultivation_pill(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id: int, num: int = 1):
    """使用三转玄丹"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info['user_id']
    user_mes = sql_message.get_user_info_with_id(user_id)

    if not user_mes:
        await handle_send(bot, event, "获取用户信息失败！")
        return

    level = user_mes['level']
    current_exp = user_mes['exp']

    # 获取单次修炼的修为
    level_rate = sql_message.get_root_rate(user_mes['root_type'], user_id)           # 灵根倍率
    realm_rate = jsondata.level_data()[level]["spend"]                               # 境界倍率

    user_buff_data = UserBuffDate(user_id)
    # 获取洞天福地加成，若无则为0
    user_blessed_spot_data = user_buff_data.BuffInfo.get('blessed_spot', 0) * 0.5 
    mainbuffdata = user_buff_data.get_user_main_buff_data()
    mainbuffratebuff = mainbuffdata.get('ratebuff', 0) # 主功法修炼倍率
    mainbuffcloexp   = mainbuffdata.get('clo_exp', 0)   # 主功法闭关经验加成

    # 单次普通修炼的修为
    single_exp = int(
        XiuConfig().closing_exp * 
        (level_rate * realm_rate * (1 + mainbuffratebuff) * (1 + mainbuffcloexp) * (1 + user_blessed_spot_data))
    )
    
    exp_rate = random.uniform(0.9, 1.3) # 随机波动
    single_exp = int(single_exp * exp_rate)

    # 总共获得的修为 (三转玄丹效果为单次修炼的333倍)
    total_exp_gain = single_exp * num * 333

    # 计算当前境界上限
    # OtherSet().set_closing_type(level) 返回的是当前境界突破到下一境界所需的修为，作为计算上限的基数
    max_exp_for_level_up = OtherSet().set_closing_type(level) 
    max_exp = max_exp_for_level_up * XiuConfig().closing_exp_upper_limit # 境界上限
    can_gain = max(0, max_exp - current_exp) # 距离上限还能获得的修为

    # 实际增加的修为（不超过上限）
    real_gain = min(total_exp_gain, can_gain)

    # 更新修为
    sql_message.update_exp(user_id, real_gain)
    sql_message.update_power2(user_id)

    # 更新气血真元
    # 这里的参数似乎是hp_change和mp_change，但原代码是传入current_exp / 10等
    # 假设这里传入的是恢复量，而不是新的值
    result_msg, result_hp_mp = OtherSet().send_hp_mp(
        user_id, 
        int(current_exp / 10), # 这里的参数含义需要根据send_hp_mp的实际逻辑确认
        int(current_exp / 20)  # 这里的参数含义需要根据send_hp_mp的实际逻辑确认
    )
    sql_message.update_user_attribute(
        user_id, 
        result_hp_mp[0], # 新的HP
        result_hp_mp[1], # 新的MP
        int(user_mes['atk']) # 攻击力保持不变
    )

    # 扣除道具
    sql_message.update_back_j(user_id, item_id, num=num, use_key=1)

    # 提示语
    msg_lines = [
        f"☆── 使用 三转玄丹 ×{num} ──☆",
        f"获得：{number_to(real_gain)}修为{result_msg[0]}{result_msg[1]}"
    ]

    if real_gain < total_exp_gain:
        msg_lines.append("（已达当前境界上限，剩余修为溢出）")

    msg_lines.extend([
        "──────────────",
        "玄丹入腹，灵气暴涨，道友修为精进！"
    ])

    await handle_send(bot, event, "\n".join(msg_lines))


@chakan_wupin.handle(parameterless=[Cooldown(cd_time=1.4)])
async def chakan_wupin_(
    bot: Bot, 
    event: GroupMessageEvent | PrivateMessageEvent, 
    args: Message = CommandArg()
):
    """查看修仙界物品（支持 类型+页码 或 类型 + 空格 + 页码）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    args_str = args.extract_plain_text().strip()
    
    # 支持的类型列表
    valid_types = ["功法", "辅修功法", "神通", "身法", "瞳术", "丹药", "合成丹药", "法器", "防具", "特殊物品", "神物"]
    
    # 解析类型和页码
    item_type = None
    current_page = 1  # 默认第一页
    
    # 情况1：用户输入类似 "神通2"（无空格）
    for t in valid_types:
        if args_str.startswith(t) and len(args_str) > len(t):
            remaining = args_str[len(t):].strip()
            if remaining.isdigit():  # 检查剩余部分是否是数字
                item_type = t
                current_page = int(remaining)
                break
    
    if item_type is None: # 如果没有匹配到无空格的情况，尝试匹配带空格的或只有类型的情况
        parts = args_str.split(maxsplit=1)  # 只分割第一个空格
        if len(parts) > 0 and parts[0] in valid_types:
            item_type = parts[0]
            if len(parts) > 1 and parts[1].isdigit():
                current_page = int(parts[1])
        else:
            msg = "请输入正确类型【功法|辅修功法|神通|身法|瞳术|丹药|合成丹药|法器|防具|特殊物品|神物】！！！"
            await handle_send(bot, event, msg, md_type="背包", k1="物品", v1="查看修仙界物品", k2="效果", v2="查看效果", k3="帮助", v3="修仙帮助")
            await chakan_wupin.finish()
    
    # 获取物品数据
    if item_type == "特殊物品":
        # 特殊物品包括聚灵旗和特殊道具
        jlq_data = items.get_data_by_item_type(["聚灵旗"])
        ldl_data = items.get_data_by_item_type(["炼丹炉"])
        special_data = items.get_data_by_item_type(["特殊道具"])
        item_data = {**jlq_data, **ldl_data, **special_data}
    else:
        item_data = items.get_data_by_item_type([item_type])
    
    msg_list = []
    
    for item_id, item_info in item_data.items():
        name = item_info['name']
        if item_type in ["功法", "辅修功法", "神通", "身法", "瞳术", "法器", "防具"]:
            desc = get_item_msg(item_id) # 获取详细描述
            msg = f"ID：{item_id}\n{desc}"
        elif item_type == "特殊物品":
            if item_info['type'] == "聚灵旗":
                msg = f"名字：{name}\n效果：{item_info['desc']}\n修炼速度：{item_info['修炼速度'] * 100}%\n药材速度：{item_info['药材速度'] * 100}%\n"
            elif item_info['type'] == "炼丹炉":
                msg = f"名字：{name}\n效果：{item_info['desc']}\n"
            else:  # 特殊道具
                msg = f"名字：{name}\n效果：{item_info.get('desc', '十分神秘的东西，谁也不知道它的作用')}\n"
        elif item_type == "神物":
            rank = item_info.get('境界', '')
            desc = item_info.get('desc', '')
            buff = item_info.get('buff', '')
            msg = f"※名字:{name}\n效果：{desc}\n境界：{rank}\n增加{number_to(buff)}修为\n"
        else:  # 丹药、合成丹药
            rank = item_info.get('境界', '')
            desc = item_info.get('desc', '')
            msg = f"※{rank}丹药:{name}，效果：{desc}\n"
        msg_list.append(msg)
    
    # 分页处理
    per_page = 15  # 每页显示15条
    total_pages = (len(msg_list) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = msg_list[start_idx:end_idx]
    
    title = f"{item_type}列表"
    final_msg = []
    final_msg.extend(paged_items)
    final_msg.append(f"\n第 {current_page}/{total_pages} 页")
    
    if total_pages > 1:
        next_page_cmd = f"查看{item_type}{current_page + 1}"
        final_msg.append(f"输入 {next_page_cmd} 查看下一页")
    page = ["翻页", f"查看修仙界物品{item_type} {current_page + 1}", "查看", "查看效果", "背包", "我的背包", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '修仙界物品', bot.self_id, final_msg, title=title, page=page) # 传入final_msg而非paged_items
    await chakan_wupin.finish()

from urllib.parse import quote


def _build_backpack_md_with_sections(
    title: str,
    sections: list[tuple[str, list[dict]]],
    current_page: int,
    total_pages: int,
    show_use_btn: bool = True,
    next_cmd: str = ""
) -> str:
    """
    构建带分类和交互按钮的背包 Markdown 消息
    sections: [
      ("分类标题", [{"name":"xxx","count":1,"bind":0, "goods_type": "装备/技能/特殊道具..."}, ...]),
      ...
    ]
    """
    lines = [f"☆------{title}------☆", ""]

    for sec_title, rows in sections:
        if not rows:
            continue
        lines.append(f"【{sec_title}】")
        lines.append("")
        for row in rows:
            name = row["name"]
            count = row.get("count", 0)
            bind = row.get("bind", 0)
            g_type = row.get("goods_type", "") # 获取物品大类

            # 查看效果按钮
            view_cmd = quote(f"查看效果 {name}")
            name_md = f"[{name}](mqqapi://aio/inlinecmd?command={view_cmd}&enter=false&reply=false)"

            equipped_flag = " ※已装备※" if row.get("is_equipped") else ""
            line = f"> - {name_md} 数量:{count} 绑定:{bind}{equipped_flag}"

            # 动态生成使用指令
            if show_use_btn:
                if g_type == "特殊道具":
                    use_cmd_str = f"道具使用 {name}"
                else:
                    use_cmd_str = f"使用 {name}"
                
                use_cmd_encoded = quote(use_cmd_str)
                use_md = f" [使用](mqqapi://aio/inlinecmd?command={use_cmd_encoded}&enter=false&reply=false)"
                line += use_md

            lines.append(line)
            lines.append("\r")

    lines.append("")
    lines.append(f"第 {current_page}/{total_pages} 页")

    # 下一页按钮
    if current_page < total_pages and next_cmd:
        next_cmd_q = quote(next_cmd)
        lines.append(f"[下一页](mqqapi://aio/inlinecmd?command={next_cmd_q}&enter=false&reply=false)")

    return "\r".join(lines)

def _paginate_sections(
    sections: list[tuple[str, list[dict]]],
    current_page: int,
    per_page: int = 15
):
    """
    按“物品条目数”分页，保留分类结构
    """
    flat = []
    for sec_title, rows in sections:
        for r in rows:
            flat.append((sec_title, r))

    if not flat:
        return [], 1, 1

    total_pages = (len(flat) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))

    start = (current_page - 1) * per_page
    end = start + per_page
    page_flat = flat[start:end]

    grouped = {}
    order = []
    for sec_title, row in page_flat:
        if sec_title not in grouped:
            grouped[sec_title] = []
            order.append(sec_title)
        grouped[sec_title].append(row)

    page_sections = [(k, grouped[k]) for k in order]
    return page_sections, current_page, total_pages


def _build_main_backpack_sections_for_md(user_id: str):
    back_data = sql_message.get_back_msg(user_id)
    if not back_data:
        return []

    # 只保留大类，不按品阶分段
    equip_map = {
        "法器": {"已装备": [], "未装备": []},
        "防具": {"已装备": [], "未装备": []},
    }
    skill_map = {
        "功法": [],
        "神通": [],
        "辅修功法": [],
        "身法": [],
        "瞳术": [],
    }
    other_map = {
        "神物": [],
        "聚灵旗": [],
        "特殊道具": [],
        "炼丹炉": [],
        "礼包": [],
    }

    def _rank_idx(level: str, rank_list: list[str], default_idx=999):
        for i, rk in enumerate(rank_list):
            if rk in str(level):
                return i
        return default_idx

    merged = {}
    for b in back_data:
        if int(b.get("goods_num", 0)) <= 0:
            continue
        gid = b["goods_id"]
        gname = b["goods_name"]
        key = (gid, gname, b.get("goods_type", ""))
        if key not in merged:
            merged[key] = {
                "goods_id": gid,
                "name": gname,
                "goods_type": b.get("goods_type", ""),
                "count": 0,
                "bind": 0
            }
        merged[key]["count"] += int(b.get("goods_num", 0))
        merged[key]["bind"] += int(b.get("bind_num", 0))

    for _, row in merged.items():
        goods_type = row["goods_type"]

        if goods_type in ["丹药", "药材"]:
            continue

        item_info = items.get_data_by_item_id(row["goods_id"])
        if not item_info:
            continue

        if goods_type == "装备":
            it = item_info.get("item_type", "")
            lv = item_info.get("level", "")
            if it in ["法器", "防具"]:
                row["_sort_idx"] = _rank_idx(lv, EQUIPMENT_RANK_ORDER)
                row["_is_equipped"] = check_equipment_use_msg(user_id, row["goods_id"])
                row["is_equipped"] = row["_is_equipped"]
                if row["_is_equipped"]:
                    equip_map[it]["已装备"].append(row)
                else:
                    equip_map[it]["未装备"].append(row)
            continue

        if goods_type == "技能":
            st = item_info.get("item_type", "")
            if st in skill_map:
                lv = item_info.get("level", "")
                row["_sort_idx"] = _rank_idx(lv, SKILL_RANK_ORDER)
                skill_map[st].append(row)
            continue

        if goods_type in other_map:
            other_map[goods_type].append(row)

    sections = []

    # 法器/防具：只按大类输出，内部按“已装备优先 + 品阶 + 名字”排序
    for equip_type in ["法器", "防具"]:
        rows = equip_map[equip_type]["已装备"] + equip_map[equip_type]["未装备"]
        if rows:
            rows = sorted(rows, key=lambda x: (0 if x.get("_is_equipped") else 1, x.get("_sort_idx", 999), x["name"]))
            sections.append((equip_type, rows))

    for st in ["功法", "神通", "辅修功法", "身法", "瞳术"]:
        rows = skill_map[st]
        if rows:
            rows = sorted(rows, key=lambda x: (x.get("_sort_idx", 999), x["name"]))
            sections.append((st, rows))

    for k in ["神物", "聚灵旗", "特殊道具", "炼丹炉", "礼包"]:
        rows = other_map[k]
        if rows:
            rows = sorted(rows, key=lambda x: x["name"])
            sections.append((k, rows))

    return sections

def _build_danyao_sections_for_md(user_id: str):
    """
    丹药背包：
    - 按 buff_type 分类
    - 同分类按名字排序
    """
    back_data = sql_message.get_back_msg(user_id)
    if not back_data:
        return []

    buff_type_order = {
        'hp': 1, 'all': 2, 'level_up_rate': 3, 'level_up_big': 4,
        'atk_buff': 5, 'exp_up': 6, 'level_up': 7, '未知': 999
    }
    buff_type_names = {
        'hp': '气血回复丹药',
        'all': '全状态回复丹药',
        'level_up_rate': '突破丹药',
        'level_up_big': '大境界突破丹药',
        'atk_buff': '永久攻击丹药',
        'exp_up': '经验增加丹药',
        'level_up': '突破辅助丹药',
        '未知': '未知类型丹药'
    }

    buckets = {}
    for b in back_data:
        if int(b.get("goods_num", 0)) <= 0 or b.get("goods_type") != "丹药":
            continue
        info = items.get_data_by_item_id(b["goods_id"])
        if not info:
            continue
        bt = info.get("buff_type", "未知")
        buckets.setdefault(bt, {})
        name = b["goods_name"]
        if name not in buckets[bt]:
            buckets[bt][name] = {"name": name, "count": 0, "bind": 0}
        buckets[bt][name]["count"] += int(b.get("goods_num", 0))
        buckets[bt][name]["bind"] += int(b.get("bind_num", 0))

    sections = []
    for bt in sorted(buckets.keys(), key=lambda x: buff_type_order.get(x, 999)):
        rows = sorted(list(buckets[bt].values()), key=lambda x: x["name"])
        if rows:
            sections.append((buff_type_names.get(bt, f"{bt}类丹药"), rows))
    return sections

def _build_yaocai_sections_for_md(user_id: str):
    """
    药材背包：
    - 按 一~九品药材 分类
    - 同分类按名字排序
    """
    back_data = sql_message.get_back_msg(user_id)
    if not back_data:
        return []

    level_order = YAOCAI_RANK_ORDER
    buckets = {lv: {} for lv in level_order}

    for b in back_data:
        if int(b.get("goods_num", 0)) <= 0 or b.get("goods_type") != "药材":
            continue
        info = items.get_data_by_item_id(b["goods_id"])
        if not info:
            continue
        lv = info.get("level", "")
        if lv not in buckets:
            continue
        name = b["goods_name"]
        if name not in buckets[lv]:
            buckets[lv][name] = {"name": name, "count": 0, "bind": 0}
        buckets[lv][name]["count"] += int(b.get("goods_num", 0))
        buckets[lv][name]["bind"] += int(b.get("bind_num", 0))

    sections = []
    for lv in level_order:
        rows = sorted(list(buckets[lv].values()), key=lambda x: x["name"])
        if rows:
            sections.append((lv, rows))
    return sections

@main_back.handle(parameterless=[Cooldown(cd_time=5)])
async def main_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """我的背包 - 显示所有物品（不含丹和材）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await main_back.finish()

    try:
        current_page = int(args.extract_plain_text().strip())
    except ValueError:
        current_page = 1

    user_id = user_info['user_id']

    if XiuConfig().markdown_status:
        sections = _build_main_backpack_sections_for_md(user_id)
        if not sections:
            await handle_send(bot, event, "道友的背包空空如也！")
            await main_back.finish()

        page_sections, current_page, total_pages = _paginate_sections(
            sections, current_page, per_page=15
        )

        md_text = _build_backpack_md_with_sections(
            title=f"{user_info['user_name']}的背包",
            sections=page_sections,
            current_page=current_page,
            total_pages=total_pages,
            show_use_btn=True,
            next_cmd=f"我的背包 {current_page + 1}"
        )
        await bot.send(event=event, message=MessageSegment.markdown(bot, md_text))
        await main_back.finish()

    # 非 markdown 逻辑保持原有
    msg_list = get_user_main_back_msg(user_id)
    title = f"{user_info['user_name']}的背包"

    if not msg_list:
        await handle_send(bot, event, "道友的背包空空如也！")
        await main_back.finish()

    per_page = 15
    total_pages = (len(msg_list) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))

    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = msg_list[start_idx:end_idx]

    title_display = f"\n☆------{title}------☆"
    final_msg = []
    final_msg.extend(paged_items)
    final_msg.append(f"\n第 {current_page}/{total_pages} 页")

    if total_pages > 1:
        next_page_cmd = f"我的背包 {current_page + 1}"
        final_msg.append(f"输入 {next_page_cmd} 查看下一页")
    page = ["翻页", f"我的背包 {current_page + 1}", "使用", "使用", "查看", "查看效果", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '背包', bot.self_id, final_msg, title=title_display, page=page)
    await main_back.finish()


@danyao_back.handle(parameterless=[Cooldown(cd_time=5)])
async def danyao_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """丹药背包 - 显示丹药类物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await danyao_back.finish()

    try:
        current_page = int(args.extract_plain_text().strip())
    except ValueError:
        current_page = 1

    user_id = user_info['user_id']

    if XiuConfig().markdown_status:
        sections = _build_danyao_sections_for_md(user_id)
        if not sections:
            await handle_send(bot, event, "道友的丹药背包空空如也！")
            await danyao_back.finish()

        page_sections, current_page, total_pages = _paginate_sections(
            sections, current_page, per_page=15
        )

        md_text = _build_backpack_md_with_sections(
            title=f"{user_info['user_name']}的丹药背包",
            sections=page_sections,
            current_page=current_page,
            total_pages=total_pages,
            show_use_btn=True,
            next_cmd=f"丹药背包 {current_page + 1}"
        )
        await bot.send(event=event, message=MessageSegment.markdown(bot, md_text))
        await danyao_back.finish()

    # 非 markdown 逻辑保持原有
    msg_list = get_user_danyao_back_msg(user_id)
    title = f"{user_info['user_name']}的丹药背包"

    if not msg_list:
        await handle_send(bot, event, "道友的丹药背包空空如也！")
        await danyao_back.finish()

    per_page = 15
    total_pages = (len(msg_list) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))

    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = msg_list[start_idx:end_idx]

    title_display = f"\n☆------{title}------☆"
    final_msg = []
    final_msg.extend(paged_items)
    final_msg.append(f"\n第 {current_page}/{total_pages} 页")

    if total_pages > 1:
        next_page_cmd = f"丹药背包 {current_page + 1}"
        final_msg.append(f"输入 {next_page_cmd} 查看下一页")
    page = ["翻页", f"丹药背包 {current_page + 1}", "使用", "使用", "查看", "查看效果", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '丹药背包', bot.self_id, final_msg, title=title_display, page=page)
    await danyao_back.finish()


@yaocai_back.handle(parameterless=[Cooldown(cd_time=5)])
async def yaocai_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """药材背包 - 显示药材类物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await yaocai_back.finish()

    try:
        current_page = int(args.extract_plain_text().strip())
    except ValueError:
        current_page = 1

    user_id = user_info['user_id']

    if XiuConfig().markdown_status:
        sections = _build_yaocai_sections_for_md(user_id)
        if not sections:
            await handle_send(bot, event, "道友的药材背包空空如也！")
            await yaocai_back.finish()

        page_sections, current_page, total_pages = _paginate_sections(
            sections, current_page, per_page=15
        )

        md_text = _build_backpack_md_with_sections(
            title=f"{user_info['user_name']}的药材背包",
            sections=page_sections,
            current_page=current_page,
            total_pages=total_pages,
            show_use_btn=False,  # 药材不显示“使用”
            next_cmd=f"药材背包 {current_page + 1}"
        )
        await bot.send(event=event, message=MessageSegment.markdown(bot, md_text))
        await yaocai_back.finish()

    # 非 markdown 逻辑保持原有
    msg_list = get_user_yaocai_back_msg(user_id)
    title = f"{user_info['user_name']}的药材背包"

    if not msg_list:
        await handle_send(bot, event, "道友的药材背包空空如也！")
        await yaocai_back.finish()

    per_page = 15
    total_pages = (len(msg_list) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))

    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = msg_list[start_idx:end_idx]

    title_display = f"\n☆------{title}------☆"
    final_msg = []
    final_msg.extend(paged_items)
    final_msg.append(f"\n第 {current_page}/{total_pages} 页")

    if total_pages > 1:
        next_page_cmd = f"药材背包 {current_page + 1}"
        final_msg.append(f"输入 {next_page_cmd} 查看下一页")
    page = ["翻页", f"药材背包 {current_page + 1}", "使用", "使用", "查看", "查看效果", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '药材背包', bot.self_id, final_msg, title=title_display, page=page)
    await yaocai_back.finish()

@my_equipment.handle(parameterless=[Cooldown(cd_time=5)])
async def my_equipment_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看我的装备及其详细信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await my_equipment.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except ValueError:
        current_page = 1
    
    user_id = user_info['user_id']
    msg_list = get_user_equipment_msg(user_id)
    title = f"{user_info['user_name']}的装备"
    
    if not msg_list:
        await handle_send(bot, event, "道友的背包中没有装备！")
        await my_equipment.finish()
    
    # 分页处理
    per_page = 15
    total_pages = (len(msg_list) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = msg_list[start_idx:end_idx]
    
    title_display = f"\n☆------{title}------☆"
    final_msg = []
    final_msg.extend(paged_items)
    final_msg.append(f"\n第 {current_page}/{total_pages} 页")
    
    if total_pages > 1:
        next_page_cmd = f"我的装备 {current_page + 1}"
        final_msg.append(f"输入 {next_page_cmd} 查看下一页")
    page = ["翻页", f"我的装备 {current_page + 1}", "使用", "使用", "查看", "查看效果", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '我的装备', bot.self_id, final_msg, title=title_display, page=page)
    await my_equipment.finish()

@yaocai_detail_back.handle(parameterless=[Cooldown(cd_time=5)])
async def yaocai_detail_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """药材背包详情版 - 显示药材详细信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await yaocai_detail_back.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except ValueError:
        current_page = 1
    
    user_id = user_info['user_id']
    msg_list = get_user_yaocai_detail_back_msg(user_id)
    title = f"{user_info['user_name']}的药材背包详情"
    
    if not msg_list:
        await handle_send(bot, event, "道友的药材背包空空如也！")
        await yaocai_detail_back.finish()
    
    # 分页处理
    per_page = 15
    total_pages = (len(msg_list) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = msg_list[start_idx:end_idx]
    
    title_display = f"\n☆------{title}------☆"
    final_msg = []
    final_msg.extend(paged_items)
    final_msg.append(f"\n第 {current_page}/{total_pages} 页")
    
    if total_pages > 1:
        next_page_cmd = f"药材背包详情 {current_page + 1}"
        final_msg.append(f"输入 {next_page_cmd} 查看下一页")
    page = ["翻页", f"药材背包详情 {current_page + 1}", "使用", "使用", "查看", "查看效果", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '药材背包详情', bot.self_id, final_msg, title=title_display, page=page)
    await yaocai_detail_back.finish()

@check_user_equipment.handle(parameterless=[Cooldown(cd_time=1.4)])
async def check_user_equipment_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """管理员装备检测与修复（仅检查已装备物品）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    # 检查权限
    if not await SUPERUSER(bot, event):
        msg = "此功能仅限管理员使用！"
        await handle_send(bot, event, msg)
        await check_user_equipment.finish()
    
    msg = "开始检测用户已装备物品，请稍候..."
    await handle_send(bot, event, msg)
    
    # 获取所有用户
    all_users = sql_message.get_all_user_id()
    if not all_users:
        msg = "未找到任何用户数据！"
        await handle_send(bot, event, msg)
        await check_user_equipment.finish()
    
    fixed_count = 0
    checked_users = 0
    problem_users = []
    
    for user_id in all_users:
        checked_users += 1
        user_info = sql_message.get_user_info_with_id(user_id)
        if not user_info:
            continue
        
        # 获取用户buff信息中的已装备物品
        user_buff_info = UserBuffDate(user_id).BuffInfo
        equipped_items = []
        
        # 检查法器
        if user_buff_info['faqi_buff'] != 0:
            equipped_items.append({
                'type': '法器',
                'id': user_buff_info['faqi_buff']
            })
        
        # 检查防具
        if user_buff_info['armor_buff'] != 0:
            equipped_items.append({
                'type': '防具',
                'id': user_buff_info['armor_buff']
            })
        
        for equipped_item in equipped_items:
            item_id = equipped_item['id']
            item_type = equipped_item['type']

            item_data = sql_message.get_item_by_good_id_and_user_id(user_id, item_id)
            item_info = items.get_data_by_item_id(item_id)
            
            if not item_data or item_data['goods_num'] <= 0: # 物品不存在于背包或数量异常
                # 尝试修复：如果物品不在背包或数量<=0，则重新添加到背包，并设置为已装备状态
                # 这里假设每个装备物品在背包中应该有至少1个 goods_num
                current_goods_num = item_data['goods_num'] if item_data else 0
                fix_quantity = max(1, 1 - current_goods_num) # 确保修复后至少1个
                
                sql_message.send_back(
                    user_id,
                    item_id,
                    item_info['name'],
                    "装备", # 假设所有装备的goods_type都是"装备"
                    fix_quantity,
                    1 # 绑定物品
                )
                # 额外更新state为1，确保背包记录是已装备状态
                now_time = datetime.now()
                sql_message.update_back_equipment(f"UPDATE back set state=1, update_time='{now_time}', action_time='{now_time}' WHERE user_id={user_id} and goods_id={item_id}")

                problem_users.append({
                    'user_id': user_id,
                    'user_name': user_info['user_name'],
                    'item_name': item_info['name'],
                    'issue': f"已装备{item_type}但背包中不存在或数量异常({current_goods_num})",
                    'fixed': f"已修复至背包数量为1，并设为已装备状态"
                })
                fixed_count += 1
            else: # 物品存在且数量正常，但需要确保state为1
                if item_data['state'] == 0:
                    now_time = datetime.now()
                    sql_message.update_back_equipment(f"UPDATE back set state=1, update_time='{now_time}', action_time='{now_time}' WHERE user_id={user_id} and goods_id={item_id}")
                    problem_users.append({
                        'user_id': user_id,
                        'user_name': user_info['user_name'],
                        'item_name': item_info['name'],
                        'issue': f"已装备{item_type}但背包记录状态为未装备",
                        'fixed': f"已修复背包记录状态为已装备"
                    })
                    fixed_count += 1
    
    # 构建结果消息
    result_msg = [
        f"☆------装备检测完成------☆",
        f"检测用户数: {checked_users}",
        f"修复问题数: {fixed_count}"
    ]
    
    if problem_users:
        result_msg.append("\n☆------修复详情------☆")
        for i, problem in enumerate(problem_users[:10]):  # 最多显示10条详情
            result_msg.append(
                f"{i+1}. {problem['user_name']}的{problem['item_name']}: "
                f"{problem['issue']} → {problem['fixed']}"
            )
        
        if len(problem_users) > 10:
            result_msg.append(f"...等共{len(problem_users)}个问题")
    
    result_msg.append("\n☆------说明------☆")
    result_msg.append("1. 仅检测用户已装备的物品")
    result_msg.append("2. 修复了装备不存在、数量异常或状态异常的问题")
    
    await send_msg_handler(bot, event, '装备检测', bot.self_id, result_msg)
    await check_user_equipment.finish()

@check_user_back.handle(parameterless=[Cooldown(cd_time=1.4)])
async def check_user_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """背包上限检测 - 管理员命令"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    # 检查权限
    if not await SUPERUSER(bot, event):
        msg = "此功能仅限管理员使用！"
        await handle_send(bot, event, msg)
        await check_user_back.finish()

    msg = "开始检测用户背包物品数量，请稍候..."
    await handle_send(bot, event, msg)
    
    result = sql_message.check_and_adjust_goods_quantity()
    
    msg = f"背包物品数量异常处理结果：\n{result}"
    await handle_send(bot, event, msg)
    await check_user_back.finish()

@compare_items.handle(parameterless=[Cooldown(cd_time=30)])
async def compare_items_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """快速对比两个物品的属性"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await compare_items.finish()
    
    user_id = user_info['user_id']
    item_names = args.extract_plain_text().split()
    
    if len(item_names) != 2:
        await handle_send(bot, event, "请提供两个物品名称进行对比，格式：快速对比 物品1 物品2")
        return

    item_name1, item_name2 = item_names

    # 获取物品1的信息
    item_id1, item1_info = items.get_data_by_item_name(item_name1)
    # 获取物品2的信息
    item_id2, item2_info = items.get_data_by_item_name(item_name2)

    if not item1_info:
        await handle_send(bot, event, f"物品 '{item_name1}' 不存在，请检查名称是否正确！")
        return
    if not item2_info:
        await handle_send(bot, event, f"物品 '{item_name2}' 不存在，请检查名称是否正确！")
        return

    # 检查物品类型是否一致，只有同类型物品才能对比
    if item1_info['item_type'] != item2_info['item_type']:
        await handle_send(bot, event, f"物品的类型不一致，无法进行对比！\n{item_name1}类型：{item1_info['item_type']}\n{item_name2}类型：{item2_info['item_type']}")
        return

    item_type = item1_info['item_type']

    basic_info = format_basic_info(item_name1, item1_info, item_name2, item2_info, item_type)

    comparison_result = ""
    if item_type == '功法':
        comparison_result = compare_main(item_name1, item1_info, item_name2, item2_info)
    elif item_type in ['法器', '防具']:
        comparison_result = compare_equipment(item_name1, item1_info, item_name2, item2_info)
    elif item_type == '神通':
        comparison_result = compare_skill_types(item_name1, item1_info, item_name2, item2_info)
    else:
        await handle_send(bot, event, f"暂不支持类型 '{item_type}' 的物品对比！")
        return
    
    # 组合消息并发送
    msg_parts = []
    msg_parts.append(basic_info)
    msg_parts.append(comparison_result)
    await send_msg_handler(bot, event, '快速对比', bot.self_id, msg_parts)
    await compare_items.finish()

def get_skill_type(skill_type: int) -> str:
    """根据神通类型编码返回描述"""
    if skill_type == 1:
        skill_desc = "伤害"
    elif skill_type == 2:
        skill_desc = "增强"
    elif skill_type == 3:
        skill_desc = "持续伤害"
    elif skill_type == 4:
        skill_desc = "封印"
    elif skill_type == 5:
        skill_desc = "随机伤害"
    elif skill_type == 6:
        skill_desc = "叠加伤害"
    elif skill_type == 7:
        skill_desc = "变化神通"
    else:
        skill_desc = "未知"
    return skill_desc

def format_basic_info(item_name1: str, item1_info: dict, item_name2: str, item2_info: dict, item_type: str) -> str:
    """格式化物品基础信息，用于对比"""
    rank_name_list = convert_rank("江湖好手")[1] # 获取境界列表
    
    # 计算物品1的所需境界
    item1_rank_raw = item1_info.get('rank', 1)
    if int(item1_rank_raw) == -5: # 特殊品阶处理
        item1_rank = 23
    else:
        item1_rank = int(item1_rank_raw) + added_ranks
    item1_required_rank_name = rank_name_list[min(item1_rank, len(rank_name_list) - 1)] # 确保索引不越界

    # 计算物品2的所需境界
    item2_rank_raw = item2_info.get('rank', 1)
    if int(item2_rank_raw) == -5: # 特殊品阶处理
        item2_rank = 23
    else:
        item2_rank = int(item2_rank_raw) + added_ranks
    item2_required_rank_name = rank_name_list[min(item2_rank, len(rank_name_list) - 1)] # 确保索引不越界
    
    # 根据物品类型构建基础信息字符串
    if item_type == '功法':
        basic_info = [
            f"📜 【功法信息】",
            f"═════════════",
            f"【{item_name1}】",
            f"• 品阶：{item1_info.get('level', '未知')}",
            f"• 类型：{item1_info.get('item_type', '未知')}",
            f"• 境界：{item1_required_rank_name}",
            f"• 描述：{item1_info.get('desc', '暂无描述')}",
            f"",
            f"【{item_name2}】",
            f"• 品阶：{item2_info.get('level', '未知')}",
            f"• 类型：{item2_info.get('item_type', '未知')}",
            f"• 境界：{item2_required_rank_name}",
            f"• 描述：{item2_info.get('desc', '暂无描述')}",
            f"═════════════"
        ]
    
    elif item_type in ['法器', '防具']:
        basic_info = [
            f"⚔️ 【{item_type}信息】",
            f"═════════════",
            f"【{item_name1}】",
            f"• 品阶：{item1_info.get('level', '未知')}",
            f"• 境界：{item1_required_rank_name}",
            f"• 描述：{item1_info.get('desc', '暂无描述')}",
            f"",
            f"【{item_name2}】",
            f"• 品阶：{item2_info.get('level', '未知')}",
            f"• 境界：{item2_required_rank_name}",
            f"• 描述：{item2_info.get('desc', '暂无描述')}",
            f"═════════════"
        ]
    
    elif item_type == '神通':
        skill_type1 = item1_info.get('skill_type', 0)
        skill_desc1 = get_skill_type(skill_type1)
        skill_type2 = item2_info.get('skill_type', 0)
        skill_desc2 = get_skill_type(skill_type2)
        
        basic_info = [
            f"✨ 【神通信息】",
            f"═════════════",
            f"【{item_name1}】",
            f"• 品阶：{item1_info.get('level', '未知')}",
            f"• 类型：{skill_desc1}",
            f"• 描述：{item1_info.get('desc', '暂无描述')}",
            f"",
            f"【{item_name2}】",
            f"• 品阶：{item2_info.get('level', '未知')}",
            f"• 类型：{skill_desc2}",
            f"• 描述：{item2_info.get('desc', '暂无描述')}",
            f"═════════════"
        ]
    else: # 其他物品类型暂时只显示通用信息
        basic_info = [
            f"【物品信息】",
            f"═════════════",
            f"【{item_name1}】",
            f"• 品阶：{item1_info.get('level', '未知')}",
            f"• 类型：{item1_info.get('type', '未知')}",
            f"• 描述：{item1_info.get('desc', '暂无描述')}",
            f"",
            f"【{item_name2}】",
            f"• 品阶：{item2_info.get('level', '未知')}",
            f"• 类型：{item2_info.get('type', '未知')}",
            f"• 描述：{item2_info.get('desc', '暂无描述')}",
            f"═════════════"
        ]
    
    return "\n".join(basic_info)

def format_number(value: Any, multiply_hundred: bool = True) -> str:
    """格式化数值为百分比或浮点数/整数"""
    if isinstance(value, (int, float)):
        if multiply_hundred:
            percentage = value * 100
            if isinstance(percentage, int) or percentage.is_integer():
                return f"{int(percentage)}%"
            # 如果是浮点数，保留一位小数
            return f"{percentage:.1f}%"
        else: # 不乘以100，直接格式化
            if isinstance(value, int) or value.is_integer():
                return f"{int(value)}"
            return f"{value:.1f}"
    return str(value)

def format_difference(diff: Any, multiply_hundred: bool = True) -> str:
    """格式化差异值，并添加符号"""
    if isinstance(diff, (int, float)):
        if multiply_hundred:
            percentage_diff = diff * 100
            if isinstance(percentage_diff, int) or percentage_diff.is_integer():
                return f"{abs(int(percentage_diff))}%"
            return f"{abs(percentage_diff):.1f}%"
        else:
            if isinstance(diff, int) or diff.is_integer():
                return f"{abs(int(diff))}"
            return f"{abs(diff):.1f}"
    return str(diff)

def compare_main(item_name1: str, item1_info: dict, item_name2: str, item2_info: dict) -> str:
    """对比两个主功法的属性"""
    comparison = [
        f"\n🎯 【{item_name1} ↔ {item_name2}】", 
        f"═════════════"
    ]
    skill_params = {
        'hpbuff': '气血',
        'mpbuff': '真元',
        'atkbuff': '攻击',
        'ratebuff': '修炼速度',
        'crit_buff': '会心',
        'def_buff': '减伤',
        'dan_exp': '炼丹经验',
        'dan_buff': '丹药数量',
        'reap_buff': '药材数量',
        'exp_buff': '经验保护',
        'critatk': '会心伤害',
        'two_buff': '双修次数',
        'number': '突破概率',
        'clo_exp': '闭关经验',
        'clo_rs': '闭关生命回复',
    }
    
    # 不乘以100的参数列表
    no_multiply_params = {'two_buff', 'number', 'dan_exp', 'dan_buff', 'reap_buff'}
    
    has_comparison = False
    for param, description in skill_params.items():
        value1 = item1_info.get(param, 0)
        value2 = item2_info.get(param, 0)
        
        if value1 == 0 and value2 == 0: # 如果两个物品该属性都为0，则跳过
            continue
        
        has_comparison = True
        multiply_hundred = param not in no_multiply_params # 判断是否需要乘以100显示百分比
    
        formatted_value1 = format_number(value1, multiply_hundred)
        formatted_value2 = format_number(value2, multiply_hundred)

        diff = value2 - value1
        formatted_diff = format_difference(diff, multiply_hundred) # 格式化差异值
        
        # 根据差异大小添加趋势符号
        if diff > 0:
            comp_symbol = f"(+{formatted_diff}) 📈"
        elif diff < 0:
            comp_symbol = f"(-{formatted_diff}) 📉"
        else:
            comp_symbol = "(相同)"
        
        comparison.append(f"• {description}: {formatted_value1} ↔ {formatted_value2} {comp_symbol}")
    
    if not has_comparison:
        comparison.append("• 两个物品在可对比的属性上均无特殊效果")
    
    comparison.append("═════════════")
    return "\n".join(comparison)

def compare_equipment(item_name1: str, item1_info: dict, item_name2: str, item2_info: dict) -> str:
    """对比两个装备的属性"""
    comparison = [
        f"\n⚔️ 【{item_name1} ↔ {item_name2}】", 
        f"═════════════"
    ]
    equipment_params = {
        'atk_buff': '攻击',
        'crit_buff': '会心',
        'def_buff': '减伤',
        'mp_buff': '真元降耗',
        'critatk': '会心伤害',
    }
    
    has_comparison = False
    for param, description in equipment_params.items():
        value1 = item1_info.get(param, 0)
        value2 = item2_info.get(param, 0)
        
        if value1 == 0 and value2 == 0:
            continue
        
        has_comparison = True
        formatted_value1 = format_number(value1)
        formatted_value2 = format_number(value2)
        diff = value2 - value1
        formatted_diff = format_difference(diff)
        
        if diff > 0:
            comp_symbol = f"(+{formatted_diff}) 📈"
        elif diff < 0:
            comp_symbol = f"(-{formatted_diff}) 📉"
        else:
            comp_symbol = "(相同)"
        
        comparison.append(f"• {description}: {formatted_value1} ↔ {formatted_value2} {comp_symbol}")
    
    if not has_comparison:
        comparison.append("• 两个装备在可对比的属性上均无特殊加成")
    
    comparison.append("═════════════")
    return "\n".join(comparison)

def compare_skill_types(item_name1: str, skill1: dict, item_name2: str, skill2: dict) -> str:
    """对比两个神通的属性"""
    comparison = []
    skill_type1 = skill1.get('skill_type', 0)
    skill_type2 = skill2.get('skill_type', 0)
    skill_desc1 = get_skill_type(skill_type1)
    skill_desc2 = get_skill_type(skill_type2)
    
    if skill_type1 == skill_type2: # 只有同类型神通才能进行细致对比
        if skill_type1 == 1:  # 伤害类神通
            comparison.append(f"🔥【{item_name1} ↔ {item_name2}】")
            comparison.append(f"═════════════")
            
            # 处理伤害值，支持列表（多段伤害）
            atkvalue1 = skill1.get('atkvalue', [0])
            atkvalue2 = skill2.get('atkvalue', [0])
            
            # 计算总伤害（如果atkvalue是列表，求和）
            total_atk1 = sum(atkvalue1) if isinstance(atkvalue1, list) else atkvalue1
            total_atk2 = sum(atkvalue2) if isinstance(atkvalue2, list) else atkvalue2
            
            formatted_total_atk1 = format_number(total_atk1)
            formatted_total_atk2 = format_number(total_atk2)
            diff_atk = total_atk2 - total_atk1
            formatted_diff_atk = format_difference(diff_atk)
            
            if diff_atk > 0:
                comp_symbol_atk = f"(+{formatted_diff_atk}) 📈"
            elif diff_atk < 0:
                comp_symbol_atk = f"(-{formatted_diff_atk}) 📉"
            else:
                comp_symbol_atk = "(相同)"
            
            comparison.append(f"• 总直接伤害: {formatted_total_atk1} ↔ {formatted_total_atk2} {comp_symbol_atk}")
            
            # 其他参数
            skill_params = {
                'hpcost': ('气血消耗', True),
                'mpcost': ('真元消耗', True),
                'turncost': ('冷却回合', False),
                'rate': ('触发概率', False),
            }
            
            has_comparison = False
            for param, (description, multiply_hundred) in skill_params.items():
                value1 = skill1.get(param, 0)
                value2 = skill2.get(param, 0)
                if value1 == 0 and value2 == 0:
                    continue
                has_comparison = True
                formatted_value1 = format_number(value1, multiply_hundred)
                formatted_value2 = format_number(value2, multiply_hundred)
                diff = value2 - value1
                formatted_diff = format_difference(diff, multiply_hundred)
                
                if diff > 0:
                    comp_symbol = f"(+{formatted_diff}) 📈"
                elif diff < 0:
                    comp_symbol = f"(-{formatted_diff}) 📉"
                else:
                    comp_symbol = "(相同)"
                
                comparison.append(f"• {description}: {formatted_value1} ↔ {formatted_value2} {comp_symbol}")
            
            if not has_comparison:
                comparison.append("• 两个神通在可对比的属性上均无特殊效果")
        
        elif skill_type1 == 2:  # 增强类神通
            comparison.append(f"💪【{item_name1} ↔ {item_name2}】")
            comparison.append(f"═════════════")
            enhance_params = {
                'atkvalue': ('攻击力提升', True),
                'def_buff': ('减伤提升', True),
                'turncost': ('持续回合', False),
                'hpcost': ('气血消耗', True),
                'mpcost': ('真元消耗', True),
                'rate': ('触发概率', False),
            }
            has_comparison = False
            for param, (description, multiply_hundred) in enhance_params.items():
                value1 = skill1.get(param, 0)
                value2 = skill2.get(param, 0)
                if value1 == 0 and value2 == 0:
                    continue
                has_comparison = True
                formatted_value1 = format_number(value1, multiply_hundred)
                formatted_value2 = format_number(value2, multiply_hundred)
                diff = value2 - value1
                formatted_diff = format_difference(diff, multiply_hundred)
                
                if diff > 0:
                    comp_symbol = f"(+{formatted_diff}) 📈"
                elif diff < 0:
                    comp_symbol = f"(-{formatted_diff}) 📉"
                else:
                    comp_symbol = "(相同)"
                
                comparison.append(f"• {description}: {formatted_value1} ↔ {formatted_value2} {comp_symbol}")
            
            if not has_comparison:
                comparison.append("• 两个神通在可对比的属性上均无特殊加成")
        
        elif skill_type1 == 3:  # 持续伤害类神通
            comparison.append(f"🔄【{item_name1} ↔ {item_name2}】")
            comparison.append(f"═════════════")
            continuous_params = {
                'atkvalue': ('伤害倍率', True), # 修正为atkvalue表示伤害倍率
                'turncost': ('持续回合', False),
                'hpcost': ('气血消耗', True),
                'mpcost': ('真元消耗', True),
                'rate': ('触发概率', False),
            }
            has_comparison = False
            for param, (description, multiply_hundred) in continuous_params.items():
                value1 = skill1.get(param, 0)
                value2 = skill2.get(param, 0)
                if value1 == 0 and value2 == 0:
                    continue
                has_comparison = True
                formatted_value1 = format_number(value1, multiply_hundred)
                formatted_value2 = format_number(value2, multiply_hundred)
                diff = value2 - value1
                formatted_diff = format_difference(diff, multiply_hundred)
                
                if diff > 0:
                    comp_symbol = f"(+{formatted_diff}) 📈"
                elif diff < 0:
                    comp_symbol = f"(-{formatted_diff}) 📉"
                else:
                    comp_symbol = "(相同)"
                
                comparison.append(f"• {description}: {formatted_value1} ↔ {formatted_value2} {comp_symbol}")
            
            if not has_comparison:
                comparison.append("• 两个神通在可对比的属性上均无特殊效果")
        
        elif skill_type1 == 6:  # 叠加伤害类神通 (之前是stack，现在是叠加伤害)
            comparison.append(f"📈【{item_name1} ↔ {item_name2}】")
            comparison.append(f"═════════════")
            stack_params = {
                'buffvalue': ('每回合伤害倍率', True), # buffvalue表示每回合伤害倍率
                'turncost': ('持续回合', False),
                'hpcost': ('气血消耗', True),
                'mpcost': ('真元消耗', True),
                'rate': ('触发概率', False),
            }
            has_comparison = False
            for param, (description, multiply_hundred) in stack_params.items():
                value1 = skill1.get(param, 0)
                value2 = skill2.get(param, 0)
                if value1 == 0 and value2 == 0:
                    continue
                has_comparison = True
                formatted_value1 = format_number(value1, multiply_hundred)
                formatted_value2 = format_number(value2, multiply_hundred)
                diff = value2 - value1
                formatted_diff = format_difference(diff, multiply_hundred)
                
                if diff > 0:
                    comp_symbol = f"(+{formatted_diff}) 📈"
                elif diff < 0:
                    comp_symbol = f"(-{formatted_diff}) 📉"
                else:
                    comp_symbol = "(相同)"
                
                comparison.append(f"• {description}: {formatted_value1} ↔ {formatted_value2} {comp_symbol}")
            
            if not has_comparison:
                comparison.append("• 两个神通在可对比的属性上均无特殊效果")
        
        elif skill_type1 == 5:  # 随机伤害类神通
            comparison.append(f"🌊【{item_name1} ↔ {item_name2}】")
            comparison.append(f"═════════════")
            wave_params = {
                'atkvalue': ('最小伤害倍率', True),
                'atkvalue2': ('最大伤害倍率', True),
                'turncost': ('冷却回合', False),
                'hpcost': ('气血消耗', True),
                'mpcost': ('真元消耗', True),
                'rate': ('触发概率', False),
            }
            has_comparison = False
            for param, (description, multiply_hundred) in wave_params.items():
                value1 = skill1.get(param, 0)
                value2 = skill2.get(param, 0)
                if value1 == 0 and value2 == 0:
                    continue
                has_comparison = True
                formatted_value1 = format_number(value1, multiply_hundred)
                formatted_value2 = format_number(value2, multiply_hundred)
                diff = value2 - value1
                formatted_diff = format_difference(diff, multiply_hundred)
                
                if diff > 0:
                    comp_symbol = f"(+{formatted_diff}) 📈"
                elif diff < 0:
                    comp_symbol = f"(-{formatted_diff}) 📉"
                else:
                    comp_symbol = "(相同)"
                
                comparison.append(f"• {description}: {formatted_value1} ↔ {formatted_value2} {comp_symbol}")
            
            if not has_comparison:
                comparison.append("• 两个神通在可对比的属性上均无特殊效果")
        
        elif skill_type1 == 4:  # 封印类神通
            comparison.append(f"🔒【{item_name1} ↔ {item_name2}】")
            comparison.append(f"═════════════")
            seal_params = {
                'success': ('命中成功率', False), # success表示命中率
                'turncost': ('持续回合', False),
                'hpcost': ('气血消耗', True),
                'mpcost': ('真元消耗', True),
                'rate': ('触发概率', False),
            }
            has_comparison = False
            for param, (description, multiply_hundred) in seal_params.items():
                value1 = skill1.get(param, 0)
                value2 = skill2.get(param, 0)
                if value1 == 0 and value2 == 0:
                    continue
                has_comparison = True
                formatted_value1 = format_number(value1, multiply_hundred)
                formatted_value2 = format_number(value2, multiply_hundred)
                diff = value2 - value1
                formatted_diff = format_difference(diff, multiply_hundred)
                
                if diff > 0:
                    comp_symbol = f"(+{formatted_diff}) 📈"
                elif diff < 0:
                    comp_symbol = f"(-{formatted_diff}) 📉"
                else:
                    comp_symbol = "(相同)"
                
                comparison.append(f"• {description}: {formatted_value1} ↔ {formatted_value2} {comp_symbol}")
            
            if not has_comparison:
                comparison.append("• 两个神通在可对比的属性上均无特殊效果")
        
        elif skill_type1 == 7: # 变化神通，效果特殊，暂不进行数值对比
            comparison.append(f"🎭【{item_name1} ↔ {item_name2}】")
            comparison.append(f"═════════════")
            comparison.append(f"• 变化神通效果特殊，暂无法进行数值对比，请查看其详细描述。")
        else:
            comparison.append("🤔 【未知类型神通】")
            comparison.append(f"• 该神通类型 ({skill_desc1}) 暂不支持对比！")
    else: # 神通类型不一致
        comparison.append("⚠️ 【类型不匹配】")
        comparison.append(f"• {item_name1}类型: {skill_desc1}，{item_name2}类型: {skill_desc2}")
        comparison.append("• 不同类型的神通无法进行对比！")
    
    comparison.append("═════════════")
    return "\n".join(comparison)