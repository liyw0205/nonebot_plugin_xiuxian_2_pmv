import asyncio
import random
import time
import re
import os
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List
from nonebot import on_command, require, on_fullmatch
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
    GROUP_ADMIN,
    GROUP_OWNER,
    ActionFailed
)
from ..xiuxian_utils.lay_out import assign_bot, assign_bot_group, Cooldown, CooldownIsolateLevel
from nonebot.log import logger
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import (
    check_user, get_msg_pic, 
    send_msg_handler, CommandObjectID,
    Txt2Img, number_to, handle_send
)
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, TradeDataManager, get_weapon_info_msg, get_armor_info_msg,
    get_sec_msg, get_main_info_msg, get_sub_info_msg, UserBuffDate
)
from ..xiuxian_back import ITEM_TYPES, type_mapping, rank_map, get_item_type_by_id
from ..xiuxian_back.back_util import check_equipment_use_msg, get_item_msg_rank
from ..xiuxian_config import XiuConfig, convert_rank

# 初始化组件
items = Items()
sql_message = XiuxianDateManage()
trade = TradeDataManager()
scheduler = require("nonebot_plugin_apscheduler").scheduler

BANNED_ITEM_IDS = ["15357", "9935", "9940"]  # 禁止交易的物品ID
ITEM_TYPES = ["药材", "装备", "丹药", "技能"]
MIN_PRICE = 600000
MAX_QUANTITY = 10000

xian_shop_add = on_command("仙肆上架", priority=5, block=True)
xianshi_auto_add = on_command("仙肆自动上架", priority=5, block=True)
xianshi_fast_add = on_command("仙肆快速上架", priority=5, block=True)
xian_shop_added_by_admin = on_command("系统仙肆上架", priority=5, permission=SUPERUSER, block=True)
my_xian_shop = on_command("我的仙肆", priority=5, block=True)
xiuxian_shop_view = on_command("仙肆查看", priority=5, block=True)
xian_shop_off_all = on_fullmatch("清空仙肆", priority=3, permission=SUPERUSER, block=True)
xianshi_fast_buy = on_command("仙肆快速购买", priority=5, block=True)
xian_shop_remove = on_command("仙肆下架", priority=5, block=True)
xian_buy = on_command("仙肆购买", priority=5, block=True)
xian_shop_added_by_admin = on_command("系统仙肆上架", priority=5, permission=SUPERUSER, block=True)
xian_shop_remove_by_admin = on_command("系统仙肆下架", priority=5, permission=SUPERUSER, block=True)

def get_xianshi_min_price(item_name):
    """获取仙肆中指定物品的最低价格"""
    trade = TradeDataManager()
    items = trade.get_xianshi_items(name=item_name)
    if not items:
        return None
    return min(item['price'] for item in items)

def get_fee_price(total_price):
    """获取仙肆中指定物品的最低价格"""
    if total_price <= 5000000:
        fee_rate = 0.1
    elif total_price <= 10000000:
        fee_rate = 0.15
    elif total_price <= 20000000:
        fee_rate = 0.2
    else:
        fee_rate = 0.3
    single_fee = int(total_price * fee_rate)
    return single_fee

@xian_shop_add.handle(parameterless=[Cooldown(cd_time=1.4)])
async def xian_shop_add_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆上架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 2:
        msg = "请输入正确指令！格式：仙肆上架 物品名称 价格 [数量]"
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    goods_name = args[0]
    try:
        price = max(int(args[1]), MIN_PRICE)
        quantity = int(args[2]) if len(args) > 2 else 1
        quantity = min(quantity, MAX_QUANTITY)
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    # 检查背包是否有该物品
    back_msg = sql_message.get_back_msg(user_id)
    goods_info = None
    for item in back_msg:
        if item['goods_name'] == goods_name:
            goods_info = item
            break
    
    if not goods_info:
        msg = f"请检查该道具 {goods_name} 是否在背包内！"
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    # 检查绑定物品
    if goods_info['bind_num'] >= goods_info['goods_num']:
        msg = f"该物品是绑定物品，无法上架！"
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    # 对于装备类型，检查是否已被使用
    if goods_info['goods_type'] == "装备":
        is_equipped = check_equipment_use_msg(user_id, goods_info['goods_id'])
        if is_equipped:
            # 如果装备已被使用，可上架数量 = 总数量 - 绑定数量 - 1（已装备的）
            available_num = goods_info['goods_num'] - goods_info['bind_num'] - 1
        else:
            # 如果未装备，可上架数量 = 总数量 - 绑定数量
            available_num = goods_info['goods_num'] - goods_info['bind_num']
    else:
        # 非装备物品，正常计算
        available_num = goods_info['goods_num'] - goods_info['bind_num']
    
    # 检查可上架数量
    if quantity > available_num:
        msg = f"可上架数量不足！\n最多可上架{available_num}个"
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    # 获取物品类型
    goods_type = get_item_type_by_id(goods_info['goods_id'])
    if goods_type not in ITEM_TYPES:
        msg = f"该物品类型不允许上架！允许类型：{', '.join(ITEM_TYPES)}"
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    # 检查禁止交易的物品
    if str(goods_info['goods_id']) in BANNED_ITEM_IDS:
        msg = f"物品 {goods_name} 禁止在仙肆交易！"
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()

    total_fee = get_fee_price(price * quantity)
    if user_info['stone'] < total_fee:
        msg = f"灵石不足支付手续费！需要{total_fee}灵石，当前拥有{user_info['stone']}灵石"
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    # 一次性扣除总手续费
    sql_message.update_ls(user_id, total_fee, 2)
    for _ in range(quantity):
        # 添加到仙肆系统        
        try:
            trade.add_xianshi_item(user_id, goods_info['goods_id'], goods_name, goods_type, price, 1)
            sql_message.update_back_j(user_id, goods_info['goods_id'], 1)
            success_count += 1
        except Exception as e:
            logger.error(f"仙肆上架失败: {e}")
            msg = "上架过程中出现错误，请稍后再试！"
            continue

    msg = f"\n成功上架 {goods_name} x{quantity} 到仙肆！\n"
    msg += f"单价: {number_to(price)} 灵石\n"
    msg += f"总手续费: {number_to(total_fee)} 灵石"
    await handle_send(bot, event, msg)    
    await xian_shop_add.finish()

@xianshi_auto_add.handle(parameterless=[Cooldown(cd_time=1.4)])
async def xianshi_auto_add_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆自动上架（按类型和品阶批量上架）优化版"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xianshi_auto_add.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    # 指令格式检查
    if len(args) < 2:
        msg = "指令格式：仙肆自动上架 [类型] [品阶] [数量]\n" \
              "▶ 类型：装备|法器|防具|药材|技能|全部\n" \
              "▶ 品阶：全部|人阶|黄阶|...|上品通天法器（输入'品阶帮助'查看完整列表）\n" \
              "▶ 数量：可选，默认1个，最多10个"
        await handle_send(bot, event, msg)
        await xianshi_auto_add.finish()
    
    item_type = args[0]
    rank_name = " ".join(args[1:-1]) if len(args) > 2 else args[1]
    quantity = int(args[-1]) if args[-1].isdigit() else 1
    quantity = max(1, min(quantity, MAX_QUANTITY))
    
    if item_type not in type_mapping:
        msg = f"❌ 无效类型！可用类型：{', '.join(type_mapping.keys())}"
        await handle_send(bot, event, msg)
        await xianshi_auto_add.finish()
    
    if rank_name not in rank_map:
        msg = f"❌ 无效品阶！输入'品阶帮助'查看完整列表"
        await handle_send(bot, event, msg)
        await xianshi_auto_add.finish()

    # 获取背包物品
    back_msg = sql_message.get_back_msg(user_id)
    if not back_msg:
        msg = "💼 道友的背包空空如也！"
        await handle_send(bot, event, msg)
        await xianshi_auto_add.finish()
    
    # 筛选物品
    target_types = type_mapping[item_type]
    target_ranks = rank_map[rank_name]
    
    items_to_add = []
    for item in back_msg:
        item_info = items.get_data_by_item_id(item['goods_id'])
        if not item_info:
            continue
            
        type_match = (
            item['goods_type'] in target_types or 
            item_info.get('item_type', '') in target_types
        )
        
        rank_match = item_info.get('level', '') in target_ranks
        
        if type_match and rank_match:
            # 对于装备类型，检查是否已被使用
            if item['goods_type'] == "装备":
                is_equipped = check_equipment_use_msg(user_id, item['goods_id'])
                if is_equipped:
                    # 如果装备已被使用，可上架数量 = 总数量 - 绑定数量 - 1（已装备的）
                    available_num = item['goods_num'] - item['bind_num'] - 1
                else:
                    # 如果未装备，可上架数量 = 总数量 - 绑定数量
                    available_num = item['goods_num'] - item['bind_num']
            else:
                # 非装备物品，正常计算
                available_num = item['goods_num'] - item['bind_num']
            
            if available_num > 0:
                items_to_add.append({
                    'id': item['goods_id'],
                    'name': item['goods_name'],
                    'type': item['goods_type'],
                    'available_num': available_num,
                    'info': item_info
                })
    
    if not items_to_add:
        msg = f"🔍 背包中没有符合条件的【{item_type}·{rank_name}】物品"
        await handle_send(bot, event, msg)
        await xianshi_auto_add.finish()
    
    # === 批量处理逻辑 ===
    # 先计算所有要上架的物品和总手续费
    items_to_process = []
    for item in items_to_add:
        if str(item['id']) in BANNED_ITEM_IDS:
            continue

        min_price = get_xianshi_min_price(item['name'])
        
        if min_price is None:
            base_rank = convert_rank('江湖好手')[0]
            item_rank = get_item_msg_rank(item['id'])
            price = max(MIN_PRICE, (base_rank - 16) * 100000 - item_rank * 100000 + 1000000)
        else:
            price = min_price
        
        actual_quantity = min(quantity, item['available_num'])
        
        total_price = price * actual_quantity
        
        single_fee = get_fee_price(total_price)
        
        items_to_process.append({
            'id': item['id'],
            'name': item['name'],
            'type': item['type'],
            'price': price,
            'quantity': actual_quantity,
            'fee': single_fee
        })
    
    total_fee = sum(item['fee'] for item in items_to_process)
    
    if user_info['stone'] < total_fee:
        msg = f"灵石不足支付手续费！需要{total_fee}灵石，当前拥有{user_info['stone']}灵石"
        await handle_send(bot, event, msg)
        await xianshi_auto_add.finish()
    
    # 一次性扣除总手续费
    sql_message.update_ls(user_id, total_fee, 2)
    
    success_count = 0
    result_msg = []
    for item in items_to_process:
        for _ in range(item['quantity']):            
            try:
                trade.add_xianshi_item(user_id, item['id'], item['name'], item['type'], item['price'], 1)
                sql_message.update_back_j(user_id, item['id'], 1)
                success_count += 1
                result_msg.append(f"{item['name']} x1 - 单价:{number_to(item['price'])}")
            except Exception as e:
                logger.error(f"批量上架失败: {e}")
                continue
    display_msg = result_msg[:10]
    if len(result_msg) > 10:
        display_msg.append(f"...等共{len(result_msg)}件物品")
    msg = "\n".join(display_msg)
    msg += f"\n✨ 成功上架 {success_count} 件物品\n"
    msg += f"💎 总手续费: {number_to(total_fee)}灵石"
    
    await handle_send(bot, event, msg)
    await xianshi_auto_add.finish()

@xianshi_fast_add.handle(parameterless=[Cooldown(cd_time=1.4)])
async def xianshi_fast_add_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆快速上架（按物品名快速上架）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xianshi_fast_add.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "指令格式：仙肆快速上架 物品名 [价格]\n" \
              "▶ 价格：可选，不填则自动匹配仙肆最低价\n" \
              "▶ 数量：固定为10个（或背包中全部数量）"
        await handle_send(bot, event, msg)
        await xianshi_fast_add.finish()
    
    goods_name = args[0]
    # 尝试解析价格参数
    try:
        price = int(args[1]) if len(args) > 1 else None
    except ValueError:
        msg = "请输入有效的价格！"
        await handle_send(bot, event, msg)
        await xianshi_fast_add.finish()
    
    # 检查背包是否有该物品
    back_msg = sql_message.get_back_msg(user_id)
    goods_info = None
    for item in back_msg:
        if item['goods_name'] == goods_name:
            goods_info = item
            break
    
    if not goods_info:
        msg = f"请检查该道具 {goods_name} 是否在背包内！"
        await handle_send(bot, event, msg)
        await xianshi_fast_add.finish()
    
    # 对于装备类型，检查是否已被使用
    if goods_info['goods_type'] == "装备":
        is_equipped = check_equipment_use_msg(user_id, goods_info['goods_id'])
        if is_equipped:
            # 如果装备已被使用，可上架数量 = 总数量 - 绑定数量 - 1（已装备的）
            available_num = goods_info['goods_num'] - goods_info['bind_num'] - 1
        else:
            # 如果未装备，可上架数量 = 总数量 - 绑定数量
            available_num = goods_info['goods_num'] - goods_info['bind_num']
    else:
        # 非装备物品，正常计算
        available_num = goods_info['goods_num'] - goods_info['bind_num']
    
    # 检查可上架数量（固定为10或背包中全部数量）
    quantity = min(10, available_num)  # 最多10个
    
    if quantity <= 0:
        msg = f"可上架数量不足！"
        await handle_send(bot, event, msg)
        await xianshi_fast_add.finish()
    
    # 获取物品类型
    goods_type = get_item_type_by_id(goods_info['goods_id'])
    if goods_type not in ITEM_TYPES:
        msg = f"该物品类型不允许上架！允许类型：{', '.join(ITEM_TYPES)}"
        await handle_send(bot, event, msg)
        await xianshi_fast_add.finish()

    # 检查禁止交易的物品
    if str(goods_info['goods_id']) in BANNED_ITEM_IDS:
        msg = f"物品 {goods_name} 禁止在仙肆交易！"
        await handle_send(bot, event, msg)
        await xianshi_fast_add.finish()

    # 获取价格（如果用户未指定价格）
    if price is None:
        # 获取仙肆最低价
        min_price = get_xianshi_min_price(goods_name)
        
        # 如果没有最低价，则使用炼金价格+100万
        if min_price is None:
            base_rank = convert_rank('江湖好手')[0]
            item_rank = get_item_msg_rank(goods_info['goods_id'])
            price = max(MIN_PRICE, (base_rank - 16) * 100000 - item_rank * 100000 + 1000000)
        else:
            price = min_price
    else:
        # 检查用户指定的价格是否低于最低价
        price = max(price, MIN_PRICE)  # 确保不低于系统最低价
    
    # 计算总手续费
    total_price = price * quantity
    single_fee = get_fee_price(total_price)
    
    if user_info['stone'] < single_fee:
        msg = f"灵石不足支付手续费！需要{single_fee}灵石，当前拥有{user_info['stone']}灵石"
        await handle_send(bot, event, msg)
        await xianshi_fast_add.finish()
    
    # 一次性扣除总手续费
    sql_message.update_ls(user_id, single_fee, 2)
    
    success_count = 0
    for _ in range(quantity):
        # 添加到仙肆系统        
        try:
            trade.add_xianshi_item(user_id, goods_info['goods_id'], goods_name, goods_type, price, 1)
            sql_message.update_back_j(user_id, goods_info['goods_id'], 1)
            success_count += 1
        except Exception as e:
            logger.error(f"快速上架失败: {e}")
            continue
    
    msg = f"\n成功上架 {goods_name} x{quantity} 到仙肆！\n"
    msg += f"单价: {number_to(price)} 灵石\n"
    msg += f"总价: {number_to(total_price)} 灵石\n"
    msg += f"手续费: {number_to(single_fee)} 灵石"
    
    await handle_send(bot, event, msg)
    await xianshi_fast_add.finish()

@xiuxian_shop_view.handle(parameterless=[Cooldown(cd_time=1.4)])
async def xiuxian_shop_view_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆查看"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()
    
    # 解析参数
    args_str = args.extract_plain_text().strip()
    
    # 情况1：无参数 - 显示可用类型
    if not args_str:
        msg = f"请指定查看类型：【{', '.join(ITEM_TYPES)}】"
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()
    
    # 解析类型和页码
    item_type = None
    current_page = 1
    
    # 检查是否直接拼接类型和页码（无空格）
    for t in ITEM_TYPES:
        if args_str.startswith(t):
            item_type = t
            remaining = args_str[len(t):].strip()
            if remaining.isdigit():
                current_page = int(remaining)
            break
    
    # 情况2：有空格分隔
    if item_type is None:
        parts = args_str.split(maxsplit=1)
        if len(parts) == 2 and parts[0] in ITEM_TYPES:
            item_type = parts[0]
            if len(parts) > 1 and parts[1].isdigit():
                current_page = int(parts[1])
    
    # 检查类型有效性
    if item_type not in ITEM_TYPES:
        msg = f"无效类型！可用类型：【{', '.join(ITEM_TYPES)}】"
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()
    
    type_items = trade.get_xianshi_items(type=item_type)
    
    if not type_items:
        msg = f"仙肆中暂无{item_type}类物品！"
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()
    
    # 处理物品显示逻辑
    system_items = []  # 存储系统物品
    user_items = {}    # 存储用户物品（按名称分组，只保留最低价）
    
    for item in type_items:
        if item['user_id'] == 0:  # 系统物品
            system_items.append(item)
        else:  # 用户物品
            item_name = item['name']
            # 如果还没有记录或者当前价格更低，更新记录
            if item_name not in user_items or item['price'] < user_items[item_name]['price']:
                user_items[item_name] = item
    
    # 合并系统物品和用户物品，并按价格排序
    items_list = sorted(system_items + list(user_items.values()), key=lambda x: x['name'])
    
    # 分页处理
    per_page = 10
    total_pages = (len(items_list) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    if current_page > total_pages:
        msg = f"页码超出范围，最多{total_pages}页！"
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = items_list[start_idx:end_idx]

    # 构建消息
    msg_list = [f"\n☆------仙肆 {item_type}------☆"]
    for item in paged_items:
        price_str = number_to(item['price'])
        msg = f"\n{item['name']} {price_str}灵石 \nID:{item['id']}"
        
        # 处理数量显示
        if str(item['quantity']) == "-1":
            msg += f" 不限量"
        elif item['quantity'] > 1:
            msg += f" 限售:{item['quantity']}"
        
        msg_list.append(msg)
    
    msg_list.append(f"\n第 {current_page}/{total_pages} 页")
    if total_pages > 1:
        next_page_cmd = f"仙肆查看{item_type}{current_page + 1}"
        msg_list.append(f"输入 {next_page_cmd} 查看下一页")
    
    await send_msg_handler(bot, event, '仙肆查看', bot.self_id, msg_list)
    await xiuxian_shop_view.finish()

@my_xian_shop.handle(parameterless=[Cooldown(cd_time=1.4)])
async def my_xian_shop_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """我的仙肆"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await my_xian_shop.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    user_id = user_info['user_id']
    
    user_items = trade.get_xianshi_items(user_id=user_id)
    
    if not user_items:
        msg = "您在仙肆中没有上架任何物品！"
        await handle_send(bot, event, msg)
        await my_xian_shop.finish()
    
    # 按价格排序
    user_items.sort(key=lambda x: x['name'])
    
    # 检查是否有上架物品
    if not user_items:
        msg = "您在仙肆中没有上架任何物品！"
        await handle_send(bot, event, msg)
        await my_xian_shop.finish()
    
    # 分页处理
    per_page = 20
    total_pages = (len(user_items) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = user_items[start_idx:end_idx]
    
    msg_list = [f"\n☆------{user_info['user_name']}的仙肆物品------☆"]
    for item in paged_items:
        price_str = number_to(item['price'])
        msg = f"{item['name']} {price_str}灵石"
        if item['quantity'] > 1:
            msg += f" x{item['quantity']}"
        msg_list.append(msg)
    
    msg_list.append(f"\n第 {current_page}/{total_pages} 页")
    if total_pages > 1:
        msg_list.append(f"输入 我的仙肆 {current_page + 1} 查看下一页")
    
    await send_msg_handler(bot, event, '我的仙肆', bot.self_id, msg_list)
    await my_xian_shop.finish()

@xian_shop_remove.handle(parameterless=[Cooldown(cd_time=1.4)])
async def xian_shop_remove_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆下架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xian_shop_remove.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if not args:
        msg = "请输入要下架的物品名称！"
        await handle_send(bot, event, msg)
        await xian_shop_remove.finish()
    
    goods_name = args[0]
    quantity = int(args[1]) if len(args) > 1 and args[1].isdigit() else None
    
    # 获取所有用户上架的该物品
    user_items = trade.get_xianshi_items(user_id=user_id, type=None)
    filtered_items = [item for item in user_items if item['name'] == goods_name]
    
    if not filtered_items:
        msg = f"您在仙肆中没有上架 {goods_name}！"
        await handle_send(bot, event, msg)
        await xian_shop_remove.finish()
    
    # 按价格从低到高排序
    filtered_items.sort(key=lambda x: x['price'])
    
    # 确定要下架的数量
    if quantity is None:
        # 没指定数量则下架最低价的1个
        items_to_remove = [filtered_items[0]]
    else:
        # 指定数量则下架价格从低到高的指定数量
        items_to_remove = filtered_items[:quantity]
    
    # 执行下架操作
    removed_count = 0
    for item in items_to_remove:
        trade.remove_xianshi_item(item['id'])
        removed_count += 1
        sql_message.send_back(
            user_id,
            item["goods_id"],
            item["name"],
            item["type"],
            1
        )
    msg = f"成功下架 {goods_name} x{removed_count}！已退回背包"
    if len(filtered_items) > removed_count:
        msg += f"\n(仙肆中仍有 {len(filtered_items)-removed_count} 个 {goods_name})"
    
    await handle_send(bot, event, msg)
    await xian_shop_remove.finish()

@xian_buy.handle(parameterless=[Cooldown(cd_time=1.4)])
async def xian_buy_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆购买"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xian_buy.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "请输入要购买的仙肆ID！"
        await handle_send(bot, event, msg)
        await xian_buy.finish()
    
    xianshi_id = args[0]
    quantity = int(args[1]) if len(args) > 1 else 1
    if quantity < 0:
        quantity = 1
    # 从系统中查找物品
    item = trade.get_xianshi_items(id=xianshi_id)
    
    if not item:
        msg = f"未找到仙肆ID为 {xianshi_id} 的物品！"
        await handle_send(bot, event, msg)
        await xian_buy.finish()
    
    item = item[0] 
    
    # 检查是否是自己的物品
    if item['user_id'] == user_id:
        msg = "不能购买自己上架的物品！"
        await handle_send(bot, event, msg)
        await xian_buy.finish()
    
    # 检查库存（系统无限物品跳过检查）
    if item["quantity"] > 0:
        if item["quantity"] < quantity:
            msg = f"库存不足！只有 {item['quantity']} 个可用"
            await handle_send(bot, event, msg)
            await xian_buy.finish()
    
    # 计算总价
    total_price = item["price"] * quantity
    
    # 检查灵石是否足够
    if user_info["stone"] < total_price:
        msg = f"灵石不足！需要 {number_to(total_price)} 灵石，当前拥有 {number_to(user_info['stone'])} 灵石"
        await handle_send(bot, event, msg)
        await xian_buy.finish()
    
    try:
        # 扣除买家灵石
        sql_message.update_ls(user_id, total_price, 2)
        
        # 给卖家灵石（如果不是系统物品）
        if item['user_id'] != 0:
            seller_id = item['user_id']
            sql_message.update_ls(seller_id, total_price, 1)
        
        # 给买家物品
        sql_message.send_back(
            user_id,
            item["goods_id"],
            item["name"],
            item["type"],
            quantity,
            1
        )
        # 从系统中移除
        trade.remove_xianshi_item(xianshi_id)
        msg = f"成功购买 {item['name']} x{quantity}\n花费 {number_to(total_price)} 灵石"
        await handle_send(bot, event, msg)
    except Exception as e:
        logger.error(f"仙肆购买出错: {e}")
        msg = "购买过程中出现错误，请稍后再试！"
        await handle_send(bot, event, msg)
    
    await xian_buy.finish()

@xianshi_fast_buy.handle(parameterless=[Cooldown(cd_time=1.4)])
async def xianshi_fast_buy_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆快速购买"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xianshi_fast_buy.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "指令格式：仙肆快速购买 物品名1,物品名2,... [数量1,数量2,...]\n" \
              "▶ 物品名：支持1-5个物品（可重复），用逗号分隔\n" \
              "▶ 数量：可选，支持1-10个数量，用逗号分隔，没有数量默认每个物品买1个"
        await handle_send(bot, event, msg)
        await xianshi_fast_buy.finish()
    
    # 解析物品名列表（允许重复且保留顺序）
    goods_names = args[0].split(",")
    if len(goods_names) > 5:
        msg = "一次最多指定5个物品名（可重复）！"
        await handle_send(bot, event, msg)
        await xianshi_fast_buy.finish()
    
    # 解析数量列表
    quantities_input = args[1] if len(args) > 1 else ""
    quantities = quantities_input.split(",") if quantities_input else ["" for _ in goods_names]
    quantities = [int(q) if q.isdigit() else 1 for q in quantities]
    
    # 确保数量列表长度不超过物品名列表长度
    if len(quantities) > len(goods_names):
        msg = "数量列表长度不能超过物品名列表长度！"
        await handle_send(bot, event, msg)
        await xianshi_fast_buy.finish()
    
    # 补齐数量列表
    quantities += [1] * (len(goods_names) - len(quantities))
    
    # 获取所有用户物品（不包括系统物品）
    user_items = trade.get_xianshi_items()
    filtered_items = [item for item in user_items if item['user_id'] != 0 and item['name'] in goods_names]
    
    if not filtered_items:
        msg = "仙肆中没有符合条件的用户物品！"
        await handle_send(bot, event, msg)
        await xianshi_fast_buy.finish()
    
    # 按价格从低到高排序
    filtered_items.sort(key=lambda x: x['price'])
    
    # 执行购买（严格按照输入顺序处理每个物品名）
    total_cost = 0
    user_stone = user_info["stone"]
    user_stone_cost = False
    success_items = []
    failed_items = []
    
    for i, name in enumerate(goods_names):
        # 查找该物品所有可购买项（按价格排序）
        available = [item for item in filtered_items if item["name"] == name]
        remaining = quantities[i]
        purchased = 0
        item_total = 0
        
        for item in available:
            if remaining <= 0:
                break
            
            try:
                # 检查物品是否已被购买（可能被前一轮购买）
                if item["id"] not in [i['id'] for i in filtered_items]:
                    continue

                # 检查是否是自己上架的物品
                if item["user_id"] == user_id or item["user_id"] == 0:
                    continue

                # 检查用户是否有足够的灵石购买这个物品
                if user_stone < item["price"]:
                    user_stone_cost = True
                    break  # 灵石不足，停止购买

                # 执行购买
                sql_message.update_ls(user_id, item["price"], 2)  # 扣钱
                sql_message.update_ls(item["user_id"], item["price"], 1)  # 给卖家
                sql_message.send_back(user_id, item["goods_id"], item["name"], item["type"], 1, 1)
                
                # 从系统中移除
                trade.remove_xianshi_item(item["id"])
                
                purchased += 1
                item_total += item["price"]
                total_cost += item["price"]
                user_stone -= item["price"]
                remaining -= 1
                
            except Exception as e:
                logger.error(f"快速购买出错: {e}")
                continue
        
        if purchased > 0:
            success_items.append(f"{name}×{purchased} ({number_to(item_total)}灵石)")
        if user_stone_cost:
            failed_items.append(f"{name}×{remaining}（灵石不足）")
        else:
            if remaining > 0:
                failed_items.append(f"{name}×{remaining}（库存不足）")
    
    # 构建结果消息
    msg_parts = []
    if success_items:
        msg_parts.append("成功购买：")
        msg_parts.extend(success_items)
        msg_parts.append(f"总计花费：{number_to(total_cost)}灵石")
    if failed_items:
        msg_parts.append("购买失败：")
        msg_parts.extend(failed_items)
    
    msg = "\n".join(msg_parts)
    await handle_send(bot, event, msg)
    await xianshi_fast_buy.finish()

@xian_shop_off_all.handle(parameterless=[Cooldown(60, isolate_level=CooldownIsolateLevel.GLOBAL, parallel=1)])
async def xian_shop_off_all_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """清空仙肆"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xian_shop_off_all.finish()
    
    msg = "正在清空全服仙肆，请稍候..."
    await handle_send(bot, event, msg)
    
    # 获取所有用户上架的物品
    all_user_items = trade.get_xianshi_items()
    
    if not all_user_items:
        msg = "仙肆已经是空的，没有物品被下架！"
        await handle_send(bot, event, msg)
        await xian_shop_off_all.finish()
    
    # 删除所有物品
    for item in all_user_items:
        trade.remove_xianshi_all_item(item['id'])
        if item["user_id"] == 0:
            continue
        sql_message.send_back(
            item["user_id"],
            item["goods_id"],
            item["name"],
            item["type"],
            1
        )
    
    msg = "成功清空全服仙肆！"
    await handle_send(bot, event, msg)
    await xian_shop_off_all.finish()

@xian_shop_added_by_admin.handle(parameterless=[Cooldown(cd_time=1.4)])
async def xian_shop_added_by_admin_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """系统仙肆上架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "请输入正确指令！格式：系统仙肆上架 物品名称 [价格] [数量]"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    
    goods_name = args[0]
    try:
        price = int(args[1]) if len(args) > 1 else MIN_PRICE
        quantity = int(args[2]) if len(args) > 2 else -1
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    if quantity < -1:
        quantity = -1
    # 检查物品是否存在
    goods_id, item_info = items.get_data_by_item_name(goods_name)
    if not item_info:
        msg = f"物品 {goods_name} 不存在！"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    
    # 检查物品类型是否允许上架
    goods_type = get_item_type_by_id(goods_id)
    if goods_type not in ITEM_TYPES:
        msg = f"该物品类型不允许上架！允许类型：{', '.join(ITEM_TYPES)}"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    
    # 上架物品
    try:
        trade.add_xianshi_item(0, goods_id, goods_name, goods_type, price, quantity)
        if quantity == -1:
            quantity_msg = "无限"
        else:
            quantity_msg = f"x{quantity}"
        msg = f"\n成功上架 {goods_name} {quantity_msg} 到仙肆！\n"
        msg += f"单价: {number_to(price)} 灵石"
        await handle_send(bot, event, msg)
    except Exception as e:
        logger.error(f"系统仙肆上架失败: {e}")
        msg = "上架过程中出现错误，请稍后再试！"
        await handle_send(bot, event, msg)
    
    await xian_shop_added_by_admin.finish()

@xian_shop_remove_by_admin.handle(parameterless=[Cooldown(cd_time=1.4)])
async def xian_shop_remove_by_admin_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """系统仙肆下架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xian_shop_remove_by_admin.finish()
    
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "请输入正确指令！格式：系统仙肆下架 [物品ID/名称] [数量]"
        await handle_send(bot, event, msg)
        await xian_shop_remove_by_admin.finish()
    
    identifier = args[0]
    quantity = int(args[1]) if len(args) > 1 else 1
    
    # 查找物品
    item = None
    if identifier.isdigit():
        item = trade.get_xianshi_items(id=int(identifier))
    else:
        item = trade.get_xianshi_items(name=identifier)
    
    if not item:
        msg = f"未找到物品 {identifier}！"
        await handle_send(bot, event, msg)
        await xian_shop_remove_by_admin.finish()
    
    # 确定要下架的物品
    items_to_remove = [i for i in item]
    if not items_to_remove:
        msg = f"没有找到物品 {identifier}！"
        await handle_send(bot, event, msg)
        await xian_shop_remove_by_admin.finish()
    
    removed_count = 0
    for i in items_to_remove:
        try:
            if removed_count >= quantity:
                logger.info(f"系统仙肆下架成功: {removed_count}个")
                break
            trade.remove_xianshi_all_item(i['id'])
            removed_count += 1
        except Exception as e:
            logger.error(f"系统仙肆下架失败: {e}")
            continue
        if i['user_id'] != 0:
            sql_message.send_back(
            i["user_id"],
            i["goods_id"],
            i["name"],
            i["type"],
            1
        )
    
    msg = f"成功下架 {identifier} x{removed_count}！"
    await handle_send(bot, event, msg)
    
    await xian_shop_remove_by_admin.finish()