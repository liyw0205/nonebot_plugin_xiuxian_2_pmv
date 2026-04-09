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
    XiuxianDateManage, PlayerDataManager, get_weapon_info_msg, get_armor_info_msg,
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
player_data_manager = PlayerDataManager()
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
my_accessory = on_command("我的饰品", priority=10, block=True)
accessory_bag = on_command("饰品背包", priority=10, block=True)
equip_accessory = on_command("装备饰品", priority=10, block=True)
unequip_accessory = on_command("卸下饰品", priority=10, block=True)
wash_accessory = on_command("饰品洗练", priority=10, block=True)
decompose_accessory = on_command("饰品分解", priority=10, block=True)
quick_decompose_accessory = on_command("快速分解饰品", aliases={"饰品快速分解"}, priority=10, block=True)
accessory_help = on_command("饰品帮助", aliases={"饰品系统帮助"}, priority=10, block=True)
check_accessory = on_command("查看饰品", priority=10, block=True)
upgrade_accessory = on_command("饰品升阶", priority=10, block=True)
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
🔹 饰品帮助 - 查看饰品系统全部命令
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

    # 清理待确认缓存
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

    # 使用数量
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

    # =========================
    # 礼包分支（增强）
    # =========================
    if goods_type == "礼包":
        package_name = goods_info['name']
        all_msgs = []

        def _grant_one_reward(rwd: dict):
            """
            rwd字段建议：
            {
              "buff": 17001,
              "type": "饰品/丹药/法器...",
              "name": "xxx",
              "amount": 1,
              "quality": 1
            }
            """
            r_type = rwd.get("type")
            r_name = rwd.get("name", "未知物品")
            r_amount = int(rwd.get("amount", 1) or 1)
            r_buff = rwd.get("buff", None)

            # 灵石特殊处理
            if r_name == "灵石":
                if r_amount > 0:
                    sql_message.update_ls(user_id, abs(r_amount), 1)
                    return f"获得灵石 {number_to(r_amount)} 枚"
                else:
                    sql_message.update_ls(user_id, abs(r_amount), 2)
                    return f"扣除灵石 {number_to(abs(r_amount))} 枚"

            # 饰品处理（走实例）
            if r_type == "饰品":
                if r_buff is None:
                    return f"【失败】{r_name}：缺少buff(item_id)"
                q = int(rwd.get("quality", 1) or 1)
                q = max(1, min(5, q))
                for _ in range(r_amount):
                    add_accessory_to_bag(str(user_id), int(r_buff), q)
                return f"获得饰品 {r_name} x{r_amount}（{quality_to_cn(q)}）"

            # 普通物品
            if r_buff is None:
                return f"【失败】{r_name}：缺少buff(item_id)"
            g_type = "技能" if r_type in ["辅修功法", "神通", "功法", "身法", "瞳术"] else ("装备" if r_type in ["法器", "防具"] else r_type)
            sql_message.send_back(user_id, int(r_buff), r_name, g_type, r_amount, 1)
            return f"获得 {r_name} x{r_amount}"

        # 使用num个礼包
        for _ in range(num):
            # roll随机礼包模式
            if int(goods_info.get("roll", 0) or 0) == 1:
                roll_pool = goods_info.get("roll_pool", [])
                if not isinstance(roll_pool, list) or not roll_pool:
                    all_msgs.append(f"【失败】{package_name}：roll_pool为空或配置错误")
                else:
                    hit = random.choice(roll_pool)
                    all_msgs.append(_grant_one_reward(hit))
            else:
                # 固定礼包模式：遍历 buff_i / name_i / type_i / amount_i / quality_i
                i = 1
                while True:
                    buff_key = f"buff_{i}"
                    name_key = f"name_{i}"
                    type_key = f"type_{i}"
                    amount_key = f"amount_{i}"
                    quality_key = f"quality_{i}"

                    if name_key not in goods_info:
                        break

                    one = {
                        "buff": goods_info.get(buff_key, None),
                        "name": goods_info.get(name_key),
                        "type": goods_info.get(type_key, None),
                        "amount": goods_info.get(amount_key, 1),
                        "quality": goods_info.get(quality_key, 1)
                    }
                    all_msgs.append(_grant_one_reward(one))
                    i += 1

            # 每开1个礼包，扣1个礼包道具
            sql_message.update_back_j(user_id, goods_id, num=1, use_key=1)

        # 汇总消息
        msg = f"道友打开了 {num} 个 {package_name}：\n" + "\n".join(all_msgs[:80])
        if len(all_msgs) > 80:
            msg += f"\n...其余{len(all_msgs)-80}条已省略"

        await handle_send(bot, event, msg, md_type="背包", k1="背包", v1="我的背包", k2="道具", v2="道具使用", k3="饰品", v3="饰品背包")
        await use.finish()
        return

    elif goods_type == "装备":
        if goods_rank_calculated <= user_rank:
            msg = f"道友实力不足使用{goods_info['name']}\n请提升至：{required_rank_name}{lh_msg}"
        elif check_equipment_use_msg(user_id, goods_id):
            msg = "该装备已被装备，请勿重复装备！"
        else:
            sql_str, item_type = get_use_equipment_sql(user_id, goods_id)
            for sql in sql_str:
                sql_message.update_back_equipment(sql)
            if item_type == "法器":
                sql_message.updata_user_faqi_buff(user_id, goods_id)
            if item_type == "防具":
                sql_message.updata_user_armor_buff(user_id, goods_id)
            msg = f"成功装备 {item_name}！"

    elif goods_type == "技能":
        user_buff_info = UserBuffDate(user_id).BuffInfo
        skill_type = goods_info['item_type']
        if goods_rank_calculated <= user_rank:
            msg = f"道友实力不足学习{goods_info['name']}\n请提升至：{required_rank_name}{lh_msg}"
        else:
            check_map = {
                "神通": 'sec_buff',
                "身法": 'effect1_buff',
                "瞳术": 'effect2_buff',
                "功法": 'main_buff',
                "辅修功法": 'sub_buff'
            }
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
            sql_message.update_user_attribute(
                user_id,
                int(user_info_full['hp'] + (exp / 2)),
                int(user_info_full['mp'] + exp),
                int(user_info_full['atk'] + (exp / 10))
            )
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


@accessory_help.handle(parameterless=[Cooldown(cd_time=3)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """
    饰品系统帮助
    """
    msg = f"""
【饰品系统帮助】

一、基础功能
1）查看已装备饰品：
   发送：我的饰品

2）查看饰品背包：
   发送：饰品背包 [页码]
   例如：饰品背包 2

3）查看单件饰品详情：
   发送：查看饰品 饰品UID
   例如：查看饰品 acc_1730000000000_1234

4）装备饰品：
   发送：装备饰品 饰品UID
   例如：装备饰品 acc_1730000000000_1234

5）卸下饰品：
   发送：卸下饰品 部位
   可用部位：手镯 / 戒指 / 手环 / 项链
   例如：卸下饰品 戒指

二、成长功能
6）洗练饰品：
   发送：饰品洗练 饰品UID
   说明：
   - 消耗【洗练石】，消耗数量随品阶(Q1~Q5)提升
   - 每件饰品独立计算洗练次数
   - 洗练保底固定150次：
     达到后词条数值固定为该品阶上限，再次洗练仅变化词条类型

7）饰品升阶：
   发送：饰品升阶 部位 主饰品UID 材料UID
   例如：饰品升阶 项链 UID1 UID2
   规则：
   - 主饰品必须先装备在对应部位
   - 材料必须是同阶同款（同 item_id / 同部位 / 同套装）
   - 升3→4需2个材料，升4→5需3个材料
   - 最高五阶

三、分解功能
8）单件分解：
   发送：饰品分解 饰品UID
   说明：分解后获得【洗练石】，产出随品阶提升。
   注意：已装备饰品不能直接分解，请先卸下。

9）快速分解：
   发送：快速分解饰品 类型 品阶
   类型支持：
   - 全部
   - 套装：烈阳 / 玄渊 / 天衡 / 星痕 / 龙魄
   - 部位：手镯 / 戒指 / 手环 / 项链
   品阶支持：
   - 全部
   - 1~5
   - 一阶~五阶
   示例：
   - 快速分解饰品 全部 全部
   - 快速分解饰品 烈阳 三阶
   - 快速分解饰品 戒指 2
   安全规则：
   - 当“类型=全部”或“品阶=全部”时，系统自动忽略4/5阶，避免高品质饰品被误分解

四、补充说明
- 饰品为实例物品，每件都有唯一UID。
- 套装效果与战斗加成可通过“我的饰品 / 查看饰品”查看与搭配。

""".strip()

    await handle_send(
        bot, event, msg,
        md_type="背包",
        k1="我的饰品", v1="我的饰品",
        k2="饰品背包", v2="饰品背包",
        k3="升阶示例", v3="饰品升阶 项链 UID1 UID2"
    )

@upgrade_accessory.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    饰品升阶
    用法：饰品升阶 部位 主饰品UID 材料UID
    例如：饰品升阶 项链 acc_xxx acc_yyy

    规则：
    - 主饰品必须已装备在对应部位
    - 升阶需要消耗同阶同款材料（同 item_id / part / set_type / quality）
    - 3->4 需要2个材料，4->5需要3个材料
    """
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    parts = args.extract_plain_text().split()
    if len(parts) != 3:
        await handle_send(
            bot, event,
            "用法：饰品升阶 部位 主饰品UID 材料UID\n例如：饰品升阶 项链 UID1 UID2",
            md_type="背包", k1="饰品", v1="饰品背包", k2="帮助", v2="饰品帮助"
        )
        return

    part, main_uid, material_uid = parts
    if part not in SLOTS:
        await handle_send(bot, event, f"部位错误，可用：{'/'.join(SLOTS)}")
        return

    user_id = str(user_info["user_id"])
    data = _get_data(user_id)

    # 1) 主饰品必须已装备在指定部位
    main_acc = data.get("equipped", {}).get(part)
    if not main_acc:
        await handle_send(bot, event, f"{part}当前未装备饰品，无法升阶")
        return

    if str(main_acc.get("uid", "")) != str(main_uid):
        await handle_send(bot, event, f"主饰品UID不匹配：{part}当前装备并非该UID")
        return

    main_q = int(main_acc.get("quality", 1))
    if main_q >= 5:
        await handle_send(bot, event, "该饰品已达最高五阶，无法继续升阶")
        return

    need_cnt = _get_upgrade_cost(main_q)

    # 2) 在背包中收集可用材料（不能从已装备里扣）
    bag = data.get("bag", [])
    candidate_idx = []
    specified_idx = None

    for i, x in enumerate(bag):
        if str(x.get("uid", "")) == str(main_uid):
            continue

        if _is_same_accessory_for_upgrade(main_acc, x):
            candidate_idx.append(i)
            if str(x.get("uid", "")) == str(material_uid):
                specified_idx = i

    if specified_idx is None:
        await handle_send(bot, event, "材料UID无效：需为背包内同阶同款饰品")
        return

    if len(candidate_idx) < need_cnt:
        await handle_send(
            bot, event,
            f"材料不足：当前升阶需 {need_cnt} 个同阶同款饰品，你只有 {len(candidate_idx)} 个",
            md_type="背包", k1="饰品", v1="饰品背包", k2="查看", v2=f"查看饰品 {main_uid}"
        )
        return

    # 3) 组装消耗列表：优先消耗你指定的材料UID，再补齐
    consume_idx = [specified_idx]
    for i in candidate_idx:
        if i == specified_idx:
            continue
        if len(consume_idx) >= need_cnt:
            break
        consume_idx.append(i)

    # 4) 扣除材料（倒序删防止下标错位）
    for i in sorted(consume_idx, reverse=True):
        del bag[i]

    # 5) 主饰品升阶：quality+1，词条按新阶重roll（词条条数保留）
    new_q = main_q + 1
    old_cnt = len(main_acc.get("affixes", [])) if isinstance(main_acc.get("affixes", []), list) else 2
    old_cnt = max(1, min(4, old_cnt))

    main_acc["quality"] = new_q
    main_acc["affixes"] = roll_affixes(new_q, old_cnt)
    # wash_count 保留（不重置）
    main_acc["wash_count"] = int(main_acc.get("wash_count", 0))

    # 回写
    data["equipped"][part] = main_acc
    data["bag"] = bag
    _save_data(user_id, data)

    await handle_send(
        bot, event,
        f"升阶成功：{main_acc.get('name', '未知饰品')} "
        f"{quality_to_cn(main_q)} → {quality_to_cn(new_q)}\n"
        f"消耗材料：{need_cnt}件同阶同款饰品",
        md_type="背包",
        k1="我的饰品", v1="我的饰品",
        k2="饰品背包", v2="饰品背包",
        k3="查看饰品", v3=f"查看饰品 {main_uid}"
    )

@check_accessory.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    查看饰品 饰品UID
    """
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    uid = args.extract_plain_text().strip()
    if not uid:
        await handle_send(bot, event, "用法：查看饰品 饰品UID\n例如：查看饰品 acc_1730000000000_1234")
        return

    user_id = str(user_info["user_id"])
    data = _get_data(user_id)

    target = None
    where = "背包"

    # 先查背包
    for x in data.get("bag", []):
        if str(x.get("uid", "")) == uid:
            target = x
            where = "背包"
            break

    # 再查已装备
    if not target:
        for s in SLOTS:
            it = data.get("equipped", {}).get(s)
            if it and str(it.get("uid", "")) == uid:
                target = it
                where = f"已装备（{s}）"
                break

    if not target:
        await handle_send(bot, event, "未找到该饰品UID，请检查是否输入正确。")
        return

    # 基础静态信息（从饰品.json）
    item_id = int(target.get("item_id", 0))
    item_info = items.get_data_by_item_id(item_id) or {}

    name = target.get("name", item_info.get("name", "未知饰品"))
    part = target.get("part", item_info.get("part", "未知部位"))
    set_type = target.get("set_type", item_info.get("set_type", "未知套装"))
    quality = int(target.get("quality", 1))
    desc = item_info.get("desc", "暂无介绍")

    # 词条展示
    affixes = target.get("affixes", [])
    if not affixes:
        affix_lines = ["- 无词条"]
    else:
        affix_lines = []
        for af in affixes:
            t = af.get("type", "未知")
            v = float(af.get("value", 0))
            affix_lines.append(f"- {t}：+{round(v * 100, 2)}%")

    # 套装效果展示（中文）
    set_lines = []
    sb = SET_BONUS.get(set_type, {})
    if 2 in sb:
        t = sb[2].get("type")
        v = float(sb[2].get("value", 0))
        t_cn = SET_TYPE_CN.get(t, t)
        if t in SET_VALUE_POINT_TYPES:
            set_lines.append(f"2件：{t_cn} +{round(v, 2)}点")
        else:
            set_lines.append(f"2件：{t_cn} +{round(v * 100, 2)}%")
    if 4 in sb:
        t = sb[4].get("type")
        v = float(sb[4].get("value", 0))
        t_cn = SET_TYPE_CN.get(t, t)
        if t in SET_VALUE_POINT_TYPES:
            set_lines.append(f"4件：{t_cn} +{round(v, 2)}点")
        else:
            set_lines.append(f"4件：{t_cn} +{round(v * 100, 2)}%")
    if not set_lines:
        set_lines = ["暂无套装效果配置"]

    msg = (
        f"☆------饰品详情------☆\n"
        f"名称：{name}\n"
        f"UID：{uid}\n"
        f"品阶：{quality_to_cn(quality)}\n"
        f"部位：{part}\n"
        f"套装：{set_type}\n"
        f"状态：{where}\n"
        f"介绍：{desc}\n\n"
        f"【当前词条】\n" + "\n".join(affix_lines) + "\n\n"
        f"【套装效果】\n" + "\n".join(set_lines)
    )

    await handle_send(
        bot, event, msg,
        md_type="背包",
        k1="饰品背包", v1="饰品背包",
        k2="我的饰品", v2="我的饰品",
        k3="饰品帮助", v3="饰品帮助"
    )

@my_accessory.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    user_id = str(user_info["user_id"])
    data = _get_data(user_id)
    eq = data["equipped"]
    lines = ["☆------我的饰品------☆"]
    for s in SLOTS:
        it = eq.get(s)
        if not it:
            lines.append(f"{s}：未装备")
        else:
            lines.append(f"{s}：{it['name']}[{quality_to_cn(it.get('quality', 1))}]")
    await handle_send(bot, event, "\n".join(lines))


def _build_accessory_sections_for_md(user_id: str):
    """
    饰品背包：
    - 已装备优先显示
    - 再显示背包
    - 按套装分组（烈阳/玄渊/天衡/星痕/龙魄/其他）
    """
    data = _get_data(str(user_id))
    if not data:
        return []

    bag = data.get("bag", [])
    equipped = data.get("equipped", {})

    set_order = ["烈阳", "玄渊", "天衡", "星痕", "龙魄", "其他"]
    buckets = {k: [] for k in set_order}

    # 1) 先放已装备（优先）
    equipped_rows = []
    for s in SLOTS:
        it = equipped.get(s)
        if not it:
            continue
        row = {
            "name": it.get("name", "未知饰品"),
            "count": 1,
            "bind": 0,
            "goods_type": "饰品",
            "uid": it.get("uid", ""),
            "quality": int(it.get("quality", 1)),
            "part": it.get("part", s),
            "set_type": it.get("set_type", "其他"),
            "is_equipped": True
        }
        equipped_rows.append(row)

    # 2) 再放背包
    bag_rows = []
    for x in bag:
        row = {
            "name": x.get("name", "未知饰品"),
            "count": 1,
            "bind": 0,
            "goods_type": "饰品",
            "uid": x.get("uid", ""),
            "quality": int(x.get("quality", 1)),
            "part": x.get("part", ""),
            "set_type": x.get("set_type", "其他"),
            "is_equipped": False
        }
        bag_rows.append(row)

    # 合并（已装备在前）
    all_rows = equipped_rows + bag_rows

    # 分桶
    for row in all_rows:
        st = row.get("set_type", "其他")
        if st not in buckets:
            st = "其他"
        buckets[st].append(row)

    sections = []
    for st in set_order:
        rows = buckets.get(st, [])
        if not rows:
            continue

        # 排序规则：
        # 1. 已装备优先
        # 2. 品阶高优先
        # 3. 部位
        # 4. 名字
        rows = sorted(
            rows,
            key=lambda r: (
                0 if r.get("is_equipped") else 1,
                -r.get("quality", 1),
                r.get("part", ""),
                r.get("name", "")
            )
        )
        sections.append((f"{st}套装", rows))

    return sections


def _build_accessory_md_text(
    title: str,
    sections: list[tuple[str, list[dict]]],
    current_page: int,
    total_pages: int,
    next_cmd: str = ""
) -> str:
    """
    饰品专用Markdown构建（每条包含 UID 和操作按钮）
    """
    lines = [f"☆------{title}------☆", ""]

    for sec_title, rows in sections:
        if not rows:
            continue

        lines.append(f"【{sec_title}】")
        lines.append("")

        for row in rows:
            name = row.get("name", "未知饰品")
            uid = row.get("uid", "")
            q = int(row.get("quality", 1))
            part = row.get("part", "")
            set_type = row.get("set_type", "未知")

            # 查看详情（按UID）
            view_cmd = quote(f"查看饰品 {uid}", safe="")
            view_md = f"[{name}](mqqapi://aio/inlinecmd?command={view_cmd}&enter=false&reply=false)"

            # 操作按钮（按UID）
            equip_cmd = quote(f"装备饰品 {uid}", safe="")
            wash_cmd = quote(f"饰品洗练 {uid}", safe="")
            decompose_cmd = quote(f"饰品分解 {uid}", safe="")

            op_md = (
                f"[装备](mqqapi://aio/inlinecmd?command={equip_cmd}&enter=false&reply=false) "
                f"[洗练](mqqapi://aio/inlinecmd?command={wash_cmd}&enter=false&reply=false) "
                f"[分解](mqqapi://aio/inlinecmd?command={decompose_cmd}&enter=false&reply=false)"
            )

            eq_flag = "【已装备】" if row.get("is_equipped") else ""
            lines.append(
                f"> - {eq_flag}{view_md} | {part} | {set_type} | {quality_to_cn(q)} | UID:{uid} | {op_md}"
            )
            lines.append("\r")

    lines.append("")
    lines.append(f"第 {current_page}/{total_pages} 页")
    if current_page < total_pages and next_cmd:
        next_q = quote(next_cmd, safe="")
        lines.append(f"[下一页](mqqapi://aio/inlinecmd?command={next_q}&enter=false&reply=false)")

    return "\r".join(lines)


@accessory_bag.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    饰品背包 [页码]
    """
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])

    # 页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except ValueError:
        current_page = 1

    data = _get_data(user_id)
    bag = data.get("bag", []) if data else []
    if not bag:
        await handle_send(bot, event, "饰品背包为空")
        return

    # ===== Markdown 模式 =====
    if XiuConfig().markdown_status:
        sections = _build_accessory_sections_for_md(user_id)
        if not sections:
            await handle_send(bot, event, "饰品背包为空")
            return

        page_sections, current_page, total_pages = _paginate_sections(
            sections, current_page, per_page=15
        )

        md_text = _build_accessory_md_text(
            title=f"{user_info.get('user_name', '道友')}的饰品背包",
            sections=page_sections,
            current_page=current_page,
            total_pages=total_pages,
            next_cmd=f"饰品背包 {current_page + 1}"
        )

        try:
            await bot.send(event=event, message=MessageSegment.markdown(bot, md_text))
        except Exception:
            await handle_send(bot, event, md_text)
        return

    # ===== 普通消息模式 =====
    sections = _build_accessory_sections_for_md(user_id)
    flat_rows = []
    for sec_title, rows in sections:
        for r in rows:
            flat_rows.append((sec_title, r))

    per_page = 15
    total_pages = (len(flat_rows) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))

    start = (current_page - 1) * per_page
    end = start + per_page
    page_flat = flat_rows[start:end]

    title = [f"☆------{user_info.get('user_name', '道友')}的饰品背包------☆"]
    lines = []
    last_sec = None
    for sec_title, r in page_flat:
        if sec_title != last_sec:
            lines.append(f"\n【{sec_title}】")
            last_sec = sec_title

        lines.append(
            f"{r.get('name')} | {r.get('part')} | {r.get('set_type')} | {quality_to_cn(r.get('quality', 1))} | UID:{r.get('uid')}"
        )

    lines.append(f"\n第 {current_page}/{total_pages} 页")
    if current_page < total_pages:
        lines.append(f"输入 饰品背包 {current_page + 1} 查看下一页")
    lines.append("可用命令：装备饰品 UID / 饰品洗练 UID / 饰品分解 UID")
    page = ["翻页", f"饰品背包 {current_page + 1}", "装备", "装备饰品", "洗练", "饰品洗练", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '饰品背包', bot.self_id, lines, title=title, page=page)


@equip_accessory.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    uid = args.extract_plain_text().strip()
    if not uid:
        await handle_send(bot, event, "用法：装备饰品 饰品UID")
        return
    user_id = str(user_info["user_id"])
    data = _get_data(user_id)
    bag = data["bag"]
    hit = None
    for x in bag:
        if x["uid"] == uid:
            hit = x
            break
    if not hit:
        await handle_send(bot, event, "未找到该饰品UID")
        return
    part = hit["part"]
    old = data["equipped"].get(part)
    if old:
        bag.append(old)
    data["equipped"][part] = hit
    bag.remove(hit)
    _save_data(user_id, data)
    await handle_send(bot, event, f"已装备：{hit['name']} 到 {part}")


@unequip_accessory.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    part = args.extract_plain_text().strip()
    if part not in SLOTS:
        await handle_send(bot, event, "用法：卸下饰品 手镯/戒指/手环/项链")
        return
    user_id = str(user_info["user_id"])
    data = _get_data(user_id)
    cur = data["equipped"].get(part)
    if not cur:
        await handle_send(bot, event, f"{part}未装备饰品")
        return
    data["bag"].append(cur)
    data["equipped"][part] = None
    _save_data(user_id, data)
    await handle_send(bot, event, f"已卸下：{cur['name']}")


@wash_accessory.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    uid = args.extract_plain_text().strip()
    if not uid:
        await handle_send(bot, event, "用法：饰品洗练 饰品UID")
        return

    user_id = str(user_info["user_id"])
    data = _get_data(user_id)

    # 先在背包找
    target = None
    in_equipped = False
    target_slot = None

    for x in data.get("bag", []):
        if str(x.get("uid", "")) == uid:
            target = x
            break

    # 背包没找到，再看已装备
    if not target:
        for s in SLOTS:
            it = data.get("equipped", {}).get(s)
            if it and str(it.get("uid", "")) == uid:
                target = it
                in_equipped = True
                target_slot = s
                break

    if not target:
        await handle_send(bot, event, "未找到该饰品UID")
        return

    q = int(target.get("quality", 1))
    q = max(1, min(5, q))
    need = WASH_STONE_COST.get(q, 1)
    have = sql_message.goods_num(user_id, WASH_STONE_ID)

    if have < need:
        await handle_send(
            bot, event,
            f"洗练失败：{WASH_STONE_NAME}不足（需要{need}个，当前{have}个）",
            md_type="背包", k1="背包", v1="我的背包", k2="饰品", v2="饰品背包"
        )
        return

    # 扣除洗练石
    sql_message.update_back_j(user_id, WASH_STONE_ID, num=need)

    # 重roll词条，默认沿用原有词条数量
    old_cnt = len(target.get("affixes", [])) if isinstance(target.get("affixes", []), list) else 2
    old_cnt = max(1, min(4, old_cnt))
    target["affixes"] = roll_affixes(q, old_cnt)

    # 保存
    if in_equipped and target_slot:
        data["equipped"][target_slot] = target
    _save_data(user_id, data)

    await handle_send(
        bot, event,
        f"洗练完成：{target.get('name','未知饰品')}（{quality_to_cn(q)}）\n消耗{WASH_STONE_NAME}：{need}个",
        md_type="背包", k1="饰品", v1="饰品背包", k2="查看", v2="我的饰品"
    )

@decompose_accessory.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    uid = args.extract_plain_text().strip()
    if not uid:
        await handle_send(bot, event, "用法：饰品分解 饰品UID")
        return

    user_id = str(user_info["user_id"])
    data = _get_data(user_id)

    idx, target = _find_accessory_in_bag(data, uid)
    if idx < 0 or not target:
        await handle_send(bot, event, "分解失败：未在饰品背包中找到该UID（已装备饰品请先卸下）")
        return

    q = int(target.get("quality", 1))
    q = max(1, min(5, q))
    gain = ACCESSORY_DECOMPOSE_GAIN.get(q, 1)

    # 从背包移除
    del data["bag"][idx]
    _save_data(user_id, data)

    # 发放洗练石
    sql_message.send_back(user_id, WASH_STONE_ID, WASH_STONE_NAME, "特殊道具", gain, 1)

    await handle_send(
        bot, event,
        f"已分解：{target.get('name','未知饰品')}（{quality_to_cn(q)}）\n获得{WASH_STONE_NAME}：{gain}个",
        md_type="背包", k1="饰品", v1="饰品背包", k2="背包", v2="我的背包"
    )


@quick_decompose_accessory.handle(parameterless=[Cooldown(cd_time=2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    快速分解饰品 类型 品阶
    示例：
    - 快速分解饰品 全部 全部
    - 快速分解饰品 烈阳 三阶
    - 快速分解饰品 戒指 5
    """
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    parts = args.extract_plain_text().split()
    if len(parts) < 2:
        await handle_send(
            bot, event,
            "用法：快速分解饰品 类型 品阶\n类型：全部/烈阳/玄渊/天衡/星痕/龙魄/手镯/戒指/手环/项链\n品阶：全部/1~5/一阶~五阶"
        )
        return

    t = parts[0].strip()
    q_text = parts[1].strip()

    # 品阶解析
    if q_text == "全部":
        q_filter = None
    else:
        q_filter = _parse_quality_arg(q_text)
        if q_filter is None:
            await handle_send(bot, event, "品阶参数错误，请使用：全部/1~5/一阶~五阶")
            return

    # 类型合法性
    valid_types = ["全部", "烈阳", "玄渊", "天衡", "星痕", "龙魄", "手镯", "戒指", "手环", "项链"]
    if t not in valid_types:
        await handle_send(bot, event, f"类型参数错误：{t}\n可用类型：{'/'.join(valid_types)}")
        return

    user_id = str(user_info["user_id"])
    data = _get_data(user_id)
    bag = data.get("bag", [])

    if not bag:
        await handle_send(bot, event, "饰品背包为空，无可分解饰品")
        return

    keep = []
    hit = []
    total_gain = 0

    for acc in bag:
        ok_type = _match_accessory_type(acc, t)
        q = int(acc.get("quality", 1))
        q = max(1, min(5, q))
        ok_quality = (q_filter is None or q == q_filter)

        if ok_type and ok_quality:
            hit.append(acc)
            total_gain += ACCESSORY_DECOMPOSE_GAIN.get(q, 1)
        else:
            keep.append(acc)

    if not hit:
        await handle_send(bot, event, "未找到符合条件的饰品")
        return

    # 批量更新
    data["bag"] = keep
    _save_data(user_id, data)

    sql_message.send_back(user_id, WASH_STONE_ID, WASH_STONE_NAME, "特殊道具", total_gain, 1)

    await handle_send(
        bot, event,
        f"快速分解完成：{len(hit)}件\n筛选：{t} / {q_text}\n获得{WASH_STONE_NAME}：{total_gain}个",
        md_type="背包", k1="饰品", v1="饰品背包", k2="背包", v2="我的背包"
    )

TABLE = "player_accessory"

AFFIX_TYPES = ["气血", "抗暴", "防御", "会心", "会心伤害", "攻击"]

# 品阶1-5洗练区间
WASH_RANGE = {
    1: {"气血": (0.02, 0.05), "抗暴": (0.01, 0.03), "防御": (0.01, 0.03), "会心": (0.01, 0.03), "会心伤害": (0.02, 0.05), "攻击": (0.02, 0.05)},
    2: {"气血": (0.04, 0.08), "抗暴": (0.02, 0.05), "防御": (0.02, 0.05), "会心": (0.02, 0.05), "会心伤害": (0.04, 0.08), "攻击": (0.04, 0.08)},
    3: {"气血": (0.06, 0.12), "抗暴": (0.03, 0.07), "防御": (0.03, 0.07), "会心": (0.03, 0.07), "会心伤害": (0.06, 0.12), "攻击": (0.06, 0.12)},
    4: {"气血": (0.08, 0.16), "抗暴": (0.04, 0.10), "防御": (0.04, 0.10), "会心": (0.04, 0.10), "会心伤害": (0.08, 0.16), "攻击": (0.08, 0.16)},
    5: {"气血": (0.10, 0.20), "抗暴": (0.05, 0.12), "防御": (0.05, 0.12), "会心": (0.05, 0.12), "会心伤害": (0.10, 0.20), "攻击": (0.10, 0.20)},
}

SLOTS = ["手镯", "戒指", "手环", "项链"]

# 你的饰品词条中文 -> 统一属性键
AFFIX_KEY_MAP = {
    "气血": "hp_pct",              # 最大生命百分比
    "抗暴": "crit_resist",         # 抗暴（预留，当前战斗可先不生效）
    "防御": "dmg_reduction",       # 伤害减免
    "会心": "crit_rate",           # 会心率
    "会心伤害": "crit_damage",     # 会心伤害
    "攻击": "atk_pct",             # 攻击百分比
}

# =========================
# 套装效果（2件 / 4件）
# type 可用值：
# shield / true_damage / armor_pen / reflect / attack / dodge / dmg_reduction / crit_rate / shield_break
# =========================
SET_BONUS = {
    # 烈阳：攻击 + 真伤
    "烈阳": {
        2: {"type": "attack", "value": 0.08},        # 攻击 +8%
        4: {"type": "true_damage", "value": 0.06},   # 附加真伤 = 攻击 * 6%
    },

    # 玄渊：护盾 + 反伤
    "玄渊": {
        2: {"type": "shield", "value": 0.12},        # 开局护盾 = 最大生命 * 12%
        4: {"type": "reflect", "value": 0.12},       # 反伤 12%
    },

    # 天衡：穿甲 + 减伤
    "天衡": {
        2: {"type": "armor_pen", "value": 0.08},     # 穿甲 +8%
        4: {"type": "dmg_reduction", "value": 0.10}, # 减伤 +10%
    },

    # 星痕：会心 + 闪避
    "星痕": {
        2: {"type": "crit_rate", "value": 0.06},     # 会心 +6%
        4: {"type": "dodge", "value": 12}, # 闪避 +12（你的系统闪避是数值制）
    },

    # 龙魄：攻击 + 护盾穿透
    "龙魄": {
        2: {"type": "attack", "value": 0.06},        # 攻击 +6%
        4: {"type": "shield_break", "value": 0.10},  # 额外护盾穿透 +10%
    },
}

def quality_to_cn(q: int) -> str:
    return {
        1: "一阶",
        2: "二阶",
        3: "三阶",
        4: "四阶",
        5: "五阶",
    }.get(int(q), f"{q}阶")

SET_TYPE_CN = {
    "attack": "攻击提升",
    "true_damage": "附加真伤",
    "shield": "开场护盾",
    "reflect": "反伤",
    "armor_pen": "护甲穿透",
    "dmg_reduction": "伤害减免",
    "crit_rate": "会心率",
    "dodge": "闪避",
    "shield_break": "护盾穿透",
}

# 这些套装类型按“点数”显示，不加百分号
SET_VALUE_POINT_TYPES = {"dodge"}

# =========================
# 套装/部位基础定义（可选）
# =========================
ACCESSORY_SETS = ["烈阳", "玄渊", "天衡", "星痕", "龙魄"]
ACCESSORY_PARTS = ["手镯", "戒指", "手环", "项链"]
QUALITY_RANGE = [1, 2, 3, 4, 5]
# ===== 饰品洗练石配置 =====
WASH_STONE_ID = 20030
WASH_STONE_NAME = "洗练石"

# 洗练消耗（按品质Q1~Q5）
WASH_STONE_COST = {
    1: 1,
    2: 2,
    3: 4,
    4: 8,
    5: 12
}

# 分解产出（按品质Q1~Q5）
ACCESSORY_DECOMPOSE_GAIN = {
    1: 1,
    2: 3,
    3: 8,
    4: 20,
    5: 50
}


def _init_user(user_id: str):
    data = player_data_manager.get_fields(user_id, TABLE)
    if not data:
        data = {
            "equipped": {"手镯": None, "戒指": None, "手环": None, "项链": None},
            "bag": []
        }
        player_data_manager.update_or_write_data(user_id, TABLE, "equipped", data["equipped"], data_type="TEXT")
        player_data_manager.update_or_write_data(user_id, TABLE, "bag", data["bag"], data_type="TEXT")
    else:
        if "equipped" not in data or data["equipped"] is None:
            player_data_manager.update_or_write_data(user_id, TABLE, "equipped", {"手镯": None, "戒指": None, "手环": None, "项链": None}, data_type="TEXT")
        if "bag" not in data or data["bag"] is None:
            player_data_manager.update_or_write_data(user_id, TABLE, "bag", [], data_type="TEXT")

def _get_data(user_id: str):
    _init_user(user_id)
    data = player_data_manager.get_fields(user_id, TABLE)
    return data

def _save_data(user_id: str, data: dict):
    player_data_manager.update_or_write_data(user_id, TABLE, "equipped", data["equipped"], data_type="TEXT")
    player_data_manager.update_or_write_data(user_id, TABLE, "bag", data["bag"], data_type="TEXT")

def roll_affixes(quality: int, count: int = 2):
    count = max(1, min(4, count))
    pool = random.sample(AFFIX_TYPES, count)
    out = []
    for t in pool:
        lo, hi = WASH_RANGE[quality][t]
        out.append({"type": t, "value": round(random.uniform(lo, hi), 4)})
    return out

def roll_affixes_with_pity(quality: int, count: int = 2, pity_reached: bool = False):
    """
    pity_reached=True 时，词条值固定上限，只随机词条类型
    """
    count = max(1, min(4, count))
    pool = random.sample(AFFIX_TYPES, count)
    out = []
    for t in pool:
        lo, hi = WASH_RANGE[quality][t]
        v = hi if pity_reached else round(random.uniform(lo, hi), 4)
        out.append({"type": t, "value": v})
    return out

def create_accessory_instance(item_id: int, quality: int = 1):
    item = items.get_data_by_item_id(item_id)
    uid = f"acc_{int(time.time())}_{random.randint(1,9999)}"
    return {
        "uid": uid,
        "item_id": item_id,
        "name": item["name"],
        "part": item["part"],
        "set_type": item["set_type"],
        "quality": quality,
        "affixes": roll_affixes(quality, 2),
        "wash_count": 0
    }

def add_accessory_to_bag(user_id: str, item_id: int, quality: int = 1):
    data = _get_data(user_id)
    ins = create_accessory_instance(item_id, quality)
    data["bag"].append(ins)
    _save_data(user_id, data)
    return ins

def _find_accessory_in_bag(data: dict, uid: str):
    """在bag中按uid查找饰品，返回(index, item)"""
    bag = data.get("bag", [])
    for i, x in enumerate(bag):
        if str(x.get("uid", "")) == str(uid):
            return i, x
    return -1, None


def _parse_quality_arg(q_text: str):
    """支持 1/2/3/4/5 或 一阶/二阶/三阶/四阶/五阶"""
    q_text = str(q_text).strip()
    mapping = {
        "1": 1, "2": 2, "3": 3, "4": 4, "5": 5,
        "一阶": 1, "二阶": 2, "三阶": 3, "四阶": 4, "五阶": 5,
        "q1": 1, "q2": 2, "q3": 3, "q4": 4, "q5": 5,
        "Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4, "Q5": 5,
    }
    return mapping.get(q_text, None)


def _match_accessory_type(acc: dict, t: str):
    """
    类型匹配：
    - 套装：烈阳/玄渊/天衡/星痕/龙魄
    - 部位：手镯/戒指/手环/项链
    - 全部
    """
    t = str(t).strip()
    if t == "全部":
        return True
    if t in ["烈阳", "玄渊", "天衡", "星痕", "龙魄"]:
        return acc.get("set_type") == t
    if t in ["手镯", "戒指", "手环", "项链"]:
        return acc.get("part") == t
    return False

def _find_accessory_anywhere(data: dict, uid: str):
    """
    按uid查找饰品，返回:
    ("bag", idx, item) 或 ("equipped", slot, item) 或 (None, None, None)
    """
    for i, x in enumerate(data.get("bag", [])):
        if str(x.get("uid", "")) == str(uid):
            return "bag", i, x

    for s in SLOTS:
        it = data.get("equipped", {}).get(s)
        if it and str(it.get("uid", "")) == str(uid):
            return "equipped", s, it

    return None, None, None


def _get_upgrade_cost(cur_quality: int) -> int:
    """
    升阶材料数：
    2->3:1, 3->4:2, 4->5:3
    1->2建议给1，避免0消耗漏洞
    """
    if cur_quality <= 1:
        return 1
    return cur_quality - 1

