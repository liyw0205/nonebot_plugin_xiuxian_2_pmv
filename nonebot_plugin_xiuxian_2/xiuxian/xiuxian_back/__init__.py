import asyncio
import random
import time
import re
import os
import json
from pathlib import Path
from datetime import datetime, timedelta
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
from .back_util import (
    get_user_main_back_msg, get_user_yaocai_back_msg, get_user_yaocai_detail_back_msg, get_user_danyao_back_msg, check_equipment_can_use,
    get_use_equipment_sql,
    get_item_msg, get_item_msg_rank, check_use_elixir,
    get_use_jlq_msg, get_no_use_equipment_sql,
    get_user_equipment_msg,
    check_equipment_use_msg
)
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import (
    check_user, get_msg_pic, 
    send_msg_handler, CommandObjectID,
    Txt2Img, number_to, handle_send
)
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, get_weapon_info_msg, get_armor_info_msg,
    get_sec_msg, get_main_info_msg, get_sub_info_msg, UserBuffDate
)
from ..xiuxian_rift import use_rift_explore, use_rift_key, use_rift_boss, use_rift_speedup, use_rift_big_speedup
from ..xiuxian_impart import use_wishing_stone, use_love_sand
from ..xiuxian_work import use_work_order, use_work_capture_order
from ..xiuxian_buff import use_two_exp_token
from ..xiuxian_config import XiuConfig, convert_rank, added_ranks
from .auction_config import *

# 初始化组件
items = Items()
sql_message = XiuxianDateManage()
scheduler = require("nonebot_plugin_apscheduler").scheduler
added_ranks = added_ranks()

# 通用物品类型
BANNED_ITEM_IDS = ["15357", "9935", "9940"]  # 禁止交易的物品ID
ITEM_TYPES = ["药材", "装备", "丹药", "技能"]
MIN_PRICE = 600000

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
    "人阶": ["人阶下品", "人阶上品"],
    "黄阶": ["黄阶下品", "黄阶上品"],
    "玄阶": ["玄阶下品", "玄阶上品"],
    "地阶": ["地阶下品", "地阶上品"],
    "天阶": ["天阶下品", "天阶上品"],
    "仙阶": ["仙阶下品", "仙阶上品"],
    
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

# 拍卖命令
auction_view = on_command("拍卖查看", aliases={"查看拍卖"}, priority=5, block=True)
auction_bid = on_command("拍卖竞拍", aliases={"竞拍"}, priority=5, block=True)
auction_add = on_command("拍卖上架", priority=5, block=True)
auction_remove = on_command("拍卖下架", priority=5, block=True)
my_auction = on_command("我的拍卖", priority=5, block=True)
auction_info = on_command("拍卖信息", priority=5, block=True)
auction_start = on_fullmatch("开启拍卖", priority=4, permission=SUPERUSER, block=True)
auction_end = on_fullmatch("结束拍卖", priority=4, permission=SUPERUSER, block=True)
auction_lock = on_fullmatch("封闭拍卖", priority=4, permission=SUPERUSER, block=True)
auction_unlock = on_fullmatch("解封拍卖", priority=4, permission=SUPERUSER, block=True)

# === 其他命令 ===
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
no_use_zb = on_command("换装", aliases={'卸装'}, priority=5, block=True)
back_help = on_command("背包帮助", priority=8, block=True)
xiuxian_sone = on_fullmatch("灵石", priority=4, block=True)
compare_items = on_command("快速对比", priority=5, block=True)

def get_recover(goods_id, num):
    price = int((convert_rank('江湖好手')[0] - added_ranks) - get_item_msg_rank(goods_id)) * 100000
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
    if goods_id == 15053 or input_str == "补偿":
        await check_item_effect.finish()
    # 构造返回消息
    msg = f"\nID：{goods_id}\n{item_msg}"
    await handle_send(bot, event, msg, md_type="背包", k1="效果", v1="查看效果", k2="物品", v2="查看修仙界物品", k3="帮助", v3="修仙帮助")
    await check_item_effect.finish()
    
@back_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def back_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """背包帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    message = str(event.message)
    
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

# 数据文件路径
PLAYER_AUCTIONS_FILE = AUCTION_DATA_PATH / "player_auctions.json"
CURRENT_AUCTIONS_FILE = AUCTION_DATA_PATH / "current_auctions.json"
DISPLAY_AUCTIONS_FILE = AUCTION_DATA_PATH / "display_auctions.json"
AUCTION_HISTORY_FILE = AUCTION_DATA_PATH / "auction_history.json"

def generate_auction_id(existing_ids=None):
    """生成6-10位不重复纯数字ID"""
    existing_ids = existing_ids or set()
    while True:
        # 生成6-10位随机数字
        auction_id = str(random.randint(100000, 9999999999))
        auction_id = auction_id[:random.randint(6, 10)]
        if auction_id not in existing_ids:
            return auction_id

def get_player_auctions():
    """获取玩家上架物品"""
    try:
        if PLAYER_AUCTIONS_FILE.exists():
            with open(PLAYER_AUCTIONS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"读取玩家上架数据失败: {e}")
    return {}

def save_player_auctions(data):
    """保存玩家上架物品"""
    try:
        with open(PLAYER_AUCTIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        print(f"保存玩家上架数据失败: {e}")
        return False

def get_current_auctions():
    """获取当前拍卖品竞拍列表"""
    try:
        if CURRENT_AUCTIONS_FILE.exists():
            with open(CURRENT_AUCTIONS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"读取当前拍卖数据失败: {e}")
    return {}

def save_current_auctions(data):
    """保存当前拍卖品竞拍列表"""
    try:
        with open(CURRENT_AUCTIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        print(f"保存当前拍卖数据失败: {e}")
        return False

def get_display_auctions():
    """获取展示拍卖品"""
    try:
        if DISPLAY_AUCTIONS_FILE.exists():
            with open(DISPLAY_AUCTIONS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"读取展示拍卖数据失败: {e}")
    return {}

def save_display_auctions(data):
    """保存展示拍卖品"""
    try:
        with open(DISPLAY_AUCTIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        print(f"保存展示拍卖数据失败: {e}")
        return False

def get_auction_history():
    """获取拍卖历史"""
    try:
        if AUCTION_HISTORY_FILE.exists():
            with open(AUCTION_HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"读取拍卖历史失败: {e}")
    return []

def save_auction_history(data):
    """保存拍卖历史"""
    try:
        with open(AUCTION_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        print(f"保存拍卖历史失败: {e}")
        return False

def get_auction_status():
    """获取拍卖状态"""
    current_auctions = get_current_auctions()
    if not current_auctions:
        return {
            "active": False,
            "start_time": None,
            "end_time": None
        }
    
    schedule = get_auction_schedule()
    start_time = datetime.fromtimestamp(current_auctions["start_time"])
    duration = timedelta(hours=schedule["duration_hours"])
    end_time = start_time + duration
    
    return {
        "active": True,
        "start_time": start_time,
        "end_time": end_time
    }

def start_auction():
    """开启拍卖"""
    player_auctions = get_player_auctions()
    system_items = get_system_items()
    
    # 生成系统拍卖品 (随机5个)
    selected_system_items = random.sample(list(system_items.items()), min(5, len(system_items)))
    
    # 生成拍卖品列表
    current_auctions = {
        "start_time": time.time(),
        "items": {}
    }
    
    # 添加系统拍卖品
    for item_name, item in selected_system_items:
        auction_id = generate_auction_id(set(current_auctions["items"].keys()))
        current_auctions["items"][auction_id] = {
            "id": auction_id,
            "item_id": item["id"],
            "name": item_name,
            "start_price": item["start_price"],
            "current_price": item["start_price"],
            "seller_id": 0,  # 系统
            "seller_name": "系统",
            "bids": {},
            "is_system": True,
            "last_bid_time": None
        }
    
    # 添加玩家拍卖品
    for user_id, items_list in player_auctions.items():
        for item in items_list:
            auction_id = generate_auction_id(set(current_auctions["items"].keys()))
            current_auctions["items"][auction_id] = {
                "id": auction_id,
                "item_id": item["item_id"],
                "name": item["name"],
                "start_price": item["price"],
                "current_price": item["price"],
                "seller_id": user_id,
                "seller_name": item.get("user_name", ""),
                "bids": {},
                "is_system": False,
                "last_bid_time": None
            }
    
    # 保存当前拍卖
    save_current_auctions(current_auctions)
    
    # 生成初始展示列表
    refresh_display_auctions()
    
    # 清空玩家上架数据
    save_player_auctions({})
    
    return True

def end_auction():
    """结束拍卖，处理所有拍卖品结算"""
    current_auctions = get_current_auctions()
    if not current_auctions or "items" not in current_auctions:
        return []
    
    auction_history = get_auction_history()
    results = []
    rules = get_auction_rules()
    
    # 保存最后一次展示的拍卖品到历史展示
    last_display = get_display_auctions()
    if last_display:
        save_display_auctions({
            **last_display,
            "is_history": True,
            "end_time": time.time()
        })
    
    for auction_id, item in current_auctions["items"].items():
        # 准备拍卖结果记录
        result = {
            "auction_id": auction_id,
            "item_id": item["item_id"],
            "item_name": item["name"],
            "start_price": item["start_price"],
            "seller_id": item["seller_id"],
            "seller_name": item["seller_name"],
            "start_time": current_auctions["start_time"],
            "end_time": time.time(),
            "bids": item["bids"]
        }
        
        if item["bids"]:
            # 有出价，成交
            winner_id, final_price = max(item["bids"].items(), key=lambda x: x[1])
            winner_info = sql_message.get_user_info_with_id(winner_id)
            
            # 给买家物品
            item_info = items.get_data_by_item_id(item["item_id"])
            if item_info:
                sql_message.send_back(
                    winner_id,
                    item["item_id"],
                    item["name"],
                    item_info["type"],
                    1,
                    1
                )
            
            # 给卖家灵石（系统物品不处理）
            if not item["is_system"]:
                earnings = int(final_price * (1 - rules["fee_rate"]))  # 扣除手续费
                sql_message.update_ls(item["seller_id"], earnings, 1)
            
            result.update({
                "winner_id": winner_id,
                "winner_name": winner_info["user_name"] if winner_info else str(winner_id),
                "final_price": final_price,
                "status": "成交",
                "fee": final_price * rules["fee_rate"],
                "seller_earnings": earnings if not item["is_system"] else 0
            })
        else:
            # 无出价，流拍（系统物品不处理，玩家物品不退）
            result.update({
                "winner_id": None,
                "winner_name": None,
                "final_price": None,
                "status": "流拍",
                "fee": 0,
                "seller_earnings": 0
            })
        
        results.append(result)
        auction_history.append(result)
    
    # 保存历史记录
    save_auction_history(auction_history)
    
    # 清空当前拍卖
    save_current_auctions({})
    
    return results

def refresh_display_auctions():
    """刷新展示拍卖品（随机15个）"""
    current_auctions = get_current_auctions()
    if not current_auctions or "items" not in current_auctions:
        return False
    
    all_items = list(current_auctions["items"].values())
    if len(all_items) <= 15:
        display_items = all_items
    else:
        display_items = random.sample(all_items, 15)
    
    # 按当前价格排序
    display_items.sort(key=lambda x: -x["current_price"])
    
    save_display_auctions({
        "items": {item["id"]: item for item in display_items},
        "last_refresh": time.time()
    })
    
    return True

def add_player_auction(user_id, user_name, item_id, item_name, price):
    """玩家上架拍卖品"""
    player_auctions = get_player_auctions()
    
    # 检查是否已经上架过相同物品
    if str(user_id) in player_auctions:
        for item in player_auctions[str(user_id)]:
            if item["item_id"] == item_id:
                return False, "不能重复上架相同物品！"
    
    # 检查上架数量限制
    rules = get_auction_rules()
    if str(user_id) not in player_auctions:
        player_auctions[str(user_id)] = []
    
    if len(player_auctions[str(user_id)]) >= rules["max_user_items"]:
        return False, f"每人最多上架{rules['max_user_items']}件物品！"
    
    # 检查最低价格
    if price < rules["min_price"]:
        return False, f"最低上架价格为{rules['min_price']}灵石！"
    
    # 添加上架记录
    player_auctions[str(user_id)].append({
        "item_id": item_id,
        "name": item_name,
        "price": price,
        "user_name": user_name
    })
    
    save_player_auctions(player_auctions)
    return True, "上架成功！"

def remove_player_auction(user_id, item_name):
    """玩家下架拍卖品"""
    player_auctions = get_player_auctions()
    if str(user_id) not in player_auctions:
        return False, "你没有上架任何物品！"
    
    # 查找要下架的物品
    item_to_remove = None
    for item in player_auctions[str(user_id)]:
        if item["name"] == item_name:
            item_to_remove = item
            break
    
    if not item_to_remove:
        return False, f"没有找到名为{item_name}的上架物品！"
    
    # 移除物品
    player_auctions[str(user_id)].remove(item_to_remove)
    if not player_auctions[str(user_id)]:
        del player_auctions[str(user_id)]
    
    save_player_auctions(player_auctions)
    return True, "下架成功！"

def place_bid(user_id, user_name, auction_id, bid_price):
    """参与竞拍（首次出价需≥起拍价，后续加价需≥当前价10%或100万灵石）"""
    ABSOLUTE_MIN_INCREMENT = 1000000  # 绝对最低加价100万
    
    current_auctions = get_current_auctions()
    if not current_auctions or "items" not in current_auctions:
        return False, "拍卖当前未开启！"
    
    if auction_id not in current_auctions["items"]:
        return False, "无效的拍卖品ID！"
    
    item = current_auctions["items"][auction_id]
    
    # 检查是否是首次出价
    if not item["bids"]:
        # 首次出价必须≥起拍价
        if bid_price < item["start_price"]:
            return False, (
                f"首次出价不得低于起拍价！\n"
                f"起拍价: {number_to(item['start_price'])}\n"
                f"你的出价: {number_to(bid_price)}"
            )
    else:
        # 计算最低加价（当前价格的10%，但不低于100万）
        min_increment = max(
            int(item["current_price"] * 0.1),
            ABSOLUTE_MIN_INCREMENT
        )
        required_min_bid = item["current_price"] + min_increment
        
        if bid_price < required_min_bid:
            return False, (
                f"每次加价不得少于当前价格的10%或100万灵石！\n"
                f"当前价: {number_to(item['current_price'])}\n"
                f"最低出价: {number_to(required_min_bid)}\n"
                f"你的出价: {number_to(bid_price)}"
            )
    
    # 获取用户当前灵石
    user_info = sql_message.get_user_info_with_id(user_id)
    if not user_info:
        return False, "用户信息获取失败！"
    
    if user_info['stone'] < bid_price:
        return False, f"灵石不足！当前拥有 {number_to(user_info['stone'])} 灵石"
    
    # 处理上一个最高出价者
    prev_winner_id = None
    prev_price = 0
    if item["bids"]:
        prev_winner_id, prev_price = max(item["bids"].items(), key=lambda x: x[1])
        
        # 退还上一个出价者的灵石
        if prev_winner_id:
            sql_message.update_ls(prev_winner_id, prev_price, 1)  # 1表示增加
    
    # 扣除当前出价者的灵石
    sql_message.update_ls(user_id, bid_price, 2)  # 2表示扣除
    
    # 添加出价记录和时间戳
    item["bids"][str(user_id)] = bid_price
    if "bid_times" not in item:
        item["bid_times"] = {}
    item["bid_times"][str(user_id)] = time.time()
    item["current_price"] = bid_price
    item["last_bid_time"] = time.time()
    
    # 保存更新
    current_auctions["items"][auction_id] = item
    save_current_auctions(current_auctions)
    
    # 刷新展示列表
    refresh_display_auctions()
    
    # 构造返回消息
    msg = [
        f"\n☆------竞拍成功------☆",
        f"物品: {item['name']}",
        f"出价: {number_to(bid_price)}灵石",
        f"当前最高价: {number_to(bid_price)}灵石"
    ]
    
    if prev_winner_id:
        prev_winner = sql_message.get_user_info_with_id(prev_winner_id)
        msg.append(f"已退还 {prev_winner['user_name']} 的 {number_to(prev_price)} 灵石")
    
    # 计算下次最低加价
    next_min_increment = max(int(bid_price * 0.1), ABSOLUTE_MIN_INCREMENT)
    msg.append(f"\n下次最低加价: {number_to(next_min_increment)}灵石 (当前价的10%或100万)")
    
    return True, "\n".join(msg)

@auction_view.handle(parameterless=[Cooldown(cd_time=1.4)])
async def auction_view_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看拍卖"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    arg = args.extract_plain_text().strip()
    
    # 查看指定ID
    if arg and arg.isdigit():
        auction_id = arg
        current_auctions = get_current_auctions()
        auction_history = get_auction_history()
        
        # 先查当前拍卖
        if current_auctions and "items" in current_auctions and auction_id in current_auctions["items"]:
            item = current_auctions["items"][auction_id]
            
            # 构造详情消息
            msg = [
                f"\n☆------拍卖品详情------☆",
                f"编号: {item['id']}",
                f"物品: {item['name']}",
                f"当前价: {number_to(item['current_price'])}灵石",
                f"起拍价: {number_to(item['start_price'])}灵石"
            ]
            
            if item["bids"]:
                # 按时间排序获取最近的5条记录
                bid_records = []
                for bidder_id, price in item["bids"].items():
                    bid_time = item.get("bid_times", {}).get(bidder_id, 0)
                    bid_records.append({
                        "bidder_id": bidder_id,
                        "price": price,
                        "time": bid_time
                    })
                
                # 按时间降序排序
                bid_records.sort(key=lambda x: -x["time"])
                recent_bids = bid_records[:5]  # 只取最近的5条
                
                msg.append("\n☆------竞拍记录------☆")
                for i, bid in enumerate(recent_bids):
                    bidder = sql_message.get_user_info_with_id(bid["bidder_id"])
                    time_str = datetime.fromtimestamp(bid["time"]).strftime("%H:%M:%S") if bid["time"] else ""
                    msg.append(f"{i+1}. {bidder['user_name'] if bidder else bid['bidder_id']}: {number_to(bid['price'])}灵石 {time_str}")
            await send_msg_handler(bot, event, '拍卖品', bot.self_id, msg)
            return
        
        # 查历史记录
        for record in reversed(auction_history):
            if record["auction_id"] == auction_id:
                msg = [
                    f"\n☆------拍卖历史详情------☆",
                    f"编号: {record['auction_id']}",
                    f"物品: {record['item_name']}",
                    f"状态: {record['status']}"
                ]
                
                if record["status"] == "成交":
                    winner = sql_message.get_user_info_with_id(record["winner_id"])
                    msg.extend([
                        f"成交价: {number_to(record['final_price'])}灵石",
                        f"买家: {winner['user_name'] if winner else record['winner_id']}",
                        f"卖家: {record['seller_name']}",
                        f"手续费: {number_to(record['fee'])}灵石"
                    ])
                else:
                    msg.append(f"卖家: {record['seller_name']}")
                
                start_time = datetime.fromtimestamp(record["start_time"]).strftime("%Y-%m-%d %H:%M")
                end_time = datetime.fromtimestamp(record["end_time"]).strftime("%Y-%m-%d %H:%M")
                msg.append(f"时间: {start_time} 至 {end_time}")
                await send_msg_handler(bot, event, '拍卖品', bot.self_id, msg)
                return
        
        await handle_send(bot, event, "未找到该拍卖品！")
        return
    
    # 查看展示列表
    display_auctions = get_display_auctions()
    auction_status = get_auction_status()
    
    if not display_auctions or "items" not in display_auctions:
        msg = "当前没有拍卖品展示！"
        if auction_status["active"]:
            msg += "\n拍卖正在进行中，请稍后再试或查看指定ID"
        await handle_send(bot, event, msg, md_type="拍卖", k1="查看", v1="拍卖查看", k2="竞拍", v2="拍卖竞拍", k3="帮助", v3="拍卖帮助")
        return
    
    items_list = list(display_auctions["items"].values())
    items_list.sort(key=lambda x: -x["current_price"])
    
    title = f"\n☆------拍卖物品列表------☆"
    msg = []
    for item in items_list[:10]:  # 最多显示10个
        status = ""
        if display_auctions.get("is_history"):
            # 历史拍卖显示成交状态
            if item["bids"]:
                winner_id, final_price = max(item["bids"].items(), key=lambda x: x[1])
                winner = sql_message.get_user_info_with_id(winner_id)
                status = f" (已成交: {winner['user_name'] if winner else winner_id} {number_to(final_price)}灵石)"
            else:
                status = " (流拍)"
        
        msg.append(
            f"\n编号: {item['id']}\n"
            f"物品: {item['name']}\n"
            f"当前价: {number_to(item['current_price'])}灵石{status}"
        )
    if display_auctions.get("is_history"):
        start_time = datetime.fromtimestamp(display_auctions["last_refresh"]).strftime("%H:%M")
        end_time = datetime.fromtimestamp(display_auctions["end_time"]).strftime("%H:%M")
        msg.append(f"\n☆------历史拍卖记录------☆")
        msg.append(f"拍卖结束时间: {end_time}")
    elif auction_status["active"]:
        start_time = auction_status["start_time"].strftime("%H:%M")
        end_time = auction_status["end_time"].strftime("%H:%M")
        msg.append(f"\n拍卖进行中，预计{end_time}结束")
    else:
        msg.append("\n拍卖当前未开启")
    
    msg.append("\n输入【拍卖查看 ID】查看详情")
    page = ["查看", "拍卖查看", "竞拍", "拍卖竞拍", "灵石", "灵石", f"{start_time}/{end_time}"]
    await send_msg_handler(bot, event, '拍卖品', bot.self_id, msg, title=title, page=page)

@auction_bid.handle(parameterless=[Cooldown(cd_time=1.4)])
async def auction_bid_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """参与拍卖竞拍"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    args = args.extract_plain_text().split()
    if len(args) < 2:
        msg = "格式错误！正确格式：拍卖竞拍 [拍卖品ID] [出价]"
        await handle_send(bot, event, msg, md_type="拍卖", k1="竞拍", v1="拍卖竞拍", k2="查看", v2="拍卖查看", k3="帮助", v3="拍卖帮助")
        return
    
    auction_id, price = args[0], args[1]
    try:
        price = int(price)
    except ValueError:
        msg = "出价必须是整数！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="竞拍", v1="拍卖竞拍", k2="查看", v2="拍卖查看", k3="帮助", v3="拍卖帮助")
        return
    
    success, result = place_bid(
        user_info['user_id'],
        user_info['user_name'],
        auction_id,
        price
    )
    await handle_send(bot, event, result)

@auction_add.handle(parameterless=[Cooldown(cd_time=1.4)])
async def auction_add_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """上架物品到拍卖（限制ITEM_TYPES类型）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    # 检查拍卖状态
    auction_status = get_auction_status()
    if auction_status["active"]:
        await handle_send(bot, event, "拍卖进行中时不能上架物品！")
        return
    
    args = args.extract_plain_text().split()
    if len(args) < 2:
        rules = get_auction_rules()
        msg = f"格式错误！正确格式：拍卖上架 [物品名] [起拍价]\n最低起拍价：{rules['min_price']}灵石"
        await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="查看", v2="拍卖查看", k3="帮助", v3="拍卖帮助")
        return
    
    item_name, price = args[0], args[1]
    try:
        price = int(price)
        price = max(price, MIN_PRICE)
    except ValueError:
        msg = "价格必须是整数！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="查看", v2="拍卖查看", k3="帮助", v3="拍卖帮助")
        return

    # 检查背包物品
    goods_id, goods_info = items.get_data_by_item_name(item_name)
    if not goods_id:
        msg = f"物品 {item_name} 不存在，请检查名称是否正确！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="查看", v2="拍卖查看", k3="帮助", v3="拍卖帮助")
        return
    goods_num = sql_message.goods_num(user_info['user_id'], goods_id, num_type='trade')
    if goods_num <= 0:
        msg = f"背包中没有足够的 {item_name} ！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="查看", v2="拍卖查看", k3="帮助", v3="拍卖帮助")
        return
    
    # 检查物品类型是否允许
    if goods_info['type'] not in ITEM_TYPES:
        msg = f"该物品类型不允许交易！允许类型：{', '.join(ITEM_TYPES)}"
        await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="查看", v2="拍卖查看", k3="帮助", v3="拍卖帮助")
        return
    
    # 检查禁止交易的物品
    if str(goods_id) in BANNED_ITEM_IDS:
        msg = f"物品 {item_name} 禁止交易！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="查看", v2="拍卖查看", k3="帮助", v3="拍卖帮助")
        return

    # 从背包移除
    sql_message.update_back_j(user_info['user_id'], goods_id, num=1)
    
    # 添加上架记录
    success, result = add_player_auction(
        user_info['user_id'],
        user_info['user_name'],
        goods_id,
        item_name,
        price
    )
    await handle_send(bot, event, result)

@auction_remove.handle(parameterless=[Cooldown(cd_time=1.4)])
async def auction_remove_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """下架拍卖品（仅在非拍卖期间有效）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    # 检查拍卖状态
    auction_status = get_auction_status()
    if auction_status["active"]:
        await handle_send(bot, event, "拍卖进行中时不能下架物品！")
        return
    
    item_name = args.extract_plain_text().strip()
    if not item_name:
        msg = "请输入要下架的物品名！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="下架", v1="拍卖下架", k2="查看", v2="拍卖查看", k3="帮助", v3="拍卖帮助")
        return
    
    # 下架物品
    success, result = remove_player_auction(user_info['user_id'], item_name)
    if success:
        # 退还物品到背包
        item_info = None
        for item_id, item in items.items.items():
            if item["name"] == item_name:
                item_info = {
                    "id": item_id,
                    "name": item_name,
                    "type": item["type"]
                }
                break
        
        if item_info:
            sql_message.send_back(
                user_info['user_id'],
                item_info["id"],
                item_info["name"],
                item_info["type"],
                1
            )
    
    await handle_send(bot, event, result)

@my_auction.handle(parameterless=[Cooldown(cd_time=1.4)])
async def my_auction_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看我上架的拍卖物品（不显示ID）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await my_auction.finish()
    
    user_id = user_info['user_id']
    player_auctions = get_player_auctions()
    
    # 获取当前用户上架的物品
    user_items = player_auctions.get(str(user_id), [])
    
    if not user_items:
        msg = "您当前没有上架任何拍卖物品！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="查看", v1="拍卖查看", k2="下架", v2="拍卖下架", k3="帮助", v3="拍卖帮助")
        await my_auction.finish()
    
    # 构建消息
    msg = [f"\n☆------我的拍卖物品------☆"]
    for item in user_items:
        msg.append(f"\n物品: {item['name']}")
        msg.append(f"起拍价: {number_to(item['price'])}灵石")
    
    msg.append("\n使用【拍卖下架 物品名】可以下架物品")
    
    await handle_send(bot, event, "\n".join(msg))
    await my_auction.finish()

@auction_info.handle(parameterless=[Cooldown(cd_time=1.4)])
async def auction_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看拍卖信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    schedule = get_auction_schedule()
    rules = get_auction_rules()
    auction_status = get_auction_status()
    player_auctions = get_player_auctions()
    auction_history = get_auction_history()
    
    # 计算玩家上架物品总数
    total_player_items = sum(len(items) for items in player_auctions.values())
    
    msg = [
        "\n成功上架拍卖信息------☆",
        f"状态: {'运行中' if auction_status['active'] else '未运行'}",
        f"自动拍卖时间: 每天{schedule['start_hour']}点{schedule['start_minute']}分",
        f"持续时间: {schedule['duration_hours']}小时",
        f"自动拍卖: {'开启' if schedule['enabled'] else '关闭'}",
        f"每人最大上架数: {rules['max_user_items']}",
        f"最低起拍价: {number_to(rules['min_price'])}灵石",
        f"手续费率: {int(rules['fee_rate'] * 100)}%",
        f"当前拍卖品数量: {len(auction_status.get('items', [])) if auction_status['active'] else 0}",
        f"等待上架的玩家物品: {total_player_items}",
        f"历史拍卖记录: {len(auction_history)}次"
    ]
    
    if auction_status["active"]:
        start_time = auction_status["start_time"].strftime("%H:%M")
        end_time = auction_status["end_time"].strftime("%H:%M")
        msg.append(f"\n本次拍卖时间: {start_time} 至 {end_time}")
    
    await handle_send(bot, event, "\n".join(msg))

@auction_start.handle(parameterless=[Cooldown(cd_time=1.4)])
async def auction_start_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """管理员开启拍卖"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    auction_status = get_auction_status()
    if auction_status["active"]:
        await handle_send(bot, event, "拍卖已经在运行中！")
        return
    
    # 解封拍卖
    update_schedule({"enabled": True})
    
    # 开启拍卖
    success = start_auction()
    if not success:
        await handle_send(bot, event, "开启拍卖失败！")
        return
    
    schedule = get_auction_schedule()
    end_time = (datetime.now() + timedelta(hours=schedule["duration_hours"])).strftime("%H:%M")
    msg = f"拍卖已开启！本次拍卖将持续{schedule['duration_hours']}小时，预计{end_time}结束。"
    await handle_send(bot, event, msg)

@auction_end.handle(parameterless=[Cooldown(cd_time=1.4)])
async def auction_end_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """管理员结束拍卖"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    auction_status = get_auction_status()
    if not auction_status["active"]:
        await handle_send(bot, event, "拍卖当前未开启！")
        return
    
    results = end_auction()
    if not results:
        await handle_send(bot, event, "结束拍卖失败！")
        return
    
    # 构造结果消息
    msg = ["拍卖已结束！成交结果："]
    for result in results[:5]:  # 最多显示5条
        if result["status"] == "成交":
            winner = sql_message.get_user_info_with_id(result["winner_id"])
            msg.append(
                f"{result['item_name']} 成交价: {number_to(result['final_price'])}灵石 手续费: {number_to(result['fee'])}灵石 "
                f"买家: {winner['user_name'] if winner else result['winner_id']}"
            )
        else:
            msg.append(f"{result['item_name']} 流拍")
    
    await handle_send(bot, event, "\n".join(msg))

@auction_lock.handle(parameterless=[Cooldown(cd_time=1.4)])
async def auction_lock_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """封闭拍卖（取消自动开启）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    update_schedule({"enabled": False})
    msg = "拍卖已封闭，将不再自动开启！"
    await handle_send(bot, event, msg)

@auction_unlock.handle(parameterless=[Cooldown(cd_time=1.4)])
async def auction_unlock_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """解封拍卖（恢复自动开启）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    update_schedule({"enabled": True})
    msg = "拍卖已解封，将按照计划自动开启！"
    await handle_send(bot, event, msg)

@scheduler.scheduled_job("cron", hour=get_auction_schedule()["start_hour"], 
                        minute=get_auction_schedule()["start_minute"])
async def auto_start_auction():
    """根据配置时间自动开启拍卖"""
    schedule = get_auction_schedule()
    if schedule["enabled"]:
        success = start_auction()
        if success:
            logger.info("拍卖已自动开启")
        else:
            logger.error("拍卖自动开启失败")

@scheduler.scheduled_job("interval", minutes=10)
async def check_auction_status():
    """每10分钟检查拍卖状态"""
    auction_status = get_auction_status()
    if not auction_status["active"]:
        return
    
    # 刷新展示列表
    refresh_display_auctions()
    
    # 检查是否需要结束
    if datetime.now() >= auction_status["end_time"]:
        results = end_auction()
        if results:
            logger.info(f"拍卖已自动结束，共处理{len(results)}件拍卖品")
        else:
            logger.error("拍卖自动结束失败")

@scheduler.scheduled_job("interval", minutes=1)
async def check_auction_end():
    """每分钟检查是否需要结束（更精确的检查）"""
    auction_status = get_auction_status()
    if auction_status["active"] and datetime.now() >= auction_status["end_time"]:
        results = end_auction()
        if results:
            logger.info(f"拍卖已自动结束，共处理{len(results)}件拍卖品")

@goods_re_root.handle(parameterless=[Cooldown(cd_time=1.4)])
async def goods_re_root_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """炼金"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await goods_re_root.finish()
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    if args is None:
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

    if get_item_msg_rank(goods_id) == 520:
        msg = "此类物品不支持！"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await goods_re_root.finish()
    num = 1
    try:
        if 1 <= int(args[1]) <= int(goods_num):
            num = int(args[1])
    except:
            num = 1 
    price = get_recover(goods_id, num)
    if price <= 0:
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
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="快速炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
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
        if str(item['id']) in BANNED_ITEM_IDS:
            continue  # 跳过禁止交易的物品
        
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
        if not check_equipment_can_use(user_id, goods_id):
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
    
    item_name = args[0]  # 物品名称
        # 检查背包物品
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
    
    # 处理使用数量的通用逻辑
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
    
    # 根据物品类型处理逻辑
    user_rank = convert_rank(user_info['level'])[0]
    rank_name_list = convert_rank("江湖好手")[1]
    goods_rank = int(goods_info.get('rank', 1))
    goods_type = goods_info['type']
    lh_msg = ""
    if goods_rank == -5:
        goods_rank = 23
    else:
        goods_rank = int(goods_rank) + added_ranks
    if user_info['root_type'] in ["轮回道果", "真·轮回道果", "永恒道果", "命运道果"]:
        goods_rank = goods_rank + 3
        lh_msg = "\n轮回重修：境界限制下降！"
    required_rank_name = rank_name_list[len(rank_name_list) - goods_rank]
        
    if goods_type == "礼包":
        package_name = goods_info['name']
        msg_parts = []
        i = 1
        while True:
            buff_key = f'buff_{i}'
            name_key = f'name_{i}'
            type_key = f'type_{i}'
            amount_key = f'amount_{i}'

            if name_key not in goods_info:
                break

            item_name = goods_info[name_key]
            item_amount = goods_info.get(amount_key, 1) * num
            item_type = goods_info.get(type_key)
            buff_id = goods_info.get(buff_key)

            if item_name == "灵石":
                key = 1 if item_amount > 0 else 2  # 正数增加，负数减少
                sql_message.update_ls(user_id, abs(item_amount), key)
                msg_parts.append(f"获得灵石 {item_amount} 枚\n")
            else:
                # 调整 goods_type
                if item_type in ["辅修功法", "神通", "功法", "身法", "瞳术"]:
                    goods_type_item = "技能"
                elif item_type in ["法器", "防具"]:
                    goods_type_item = "装备"
                else:
                    goods_type_item = item_type  # 包括 "礼包" 类型，直接放入背包

                if buff_id is not None:
                    sql_message.send_back(user_id, buff_id, item_name, goods_type_item, item_amount, 1)
                    msg_parts.append(f"获得 {item_name} x{item_amount}\n")
            
            i += 1
        sql_message.update_back_j(user_id, goods_id, num=num, use_key=1)
        msg = f"道友打开了 {num} 个 {package_name}:\n" + "".join(msg_parts)

    elif goods_type == "装备":
        if goods_rank < user_rank:
             msg = f"道友实力不足使用{goods_info['name']}\n请提升至：{required_rank_name}{lh_msg}"
        elif not check_equipment_can_use(user_id, goods_id):
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
        skill_info = goods_info
        skill_type = skill_info['item_type']
        if goods_rank <= user_rank:
             msg = f"道友实力不足使用{goods_info['name']}\n请提升至：{required_rank_name}{lh_msg}"
        elif skill_type == "神通":
            if int(user_buff_info['sec_buff']) == int(goods_id):
                msg = f"道友已学会该神通：{skill_info['name']}，请勿重复学习！"
            else:
                sql_message.update_back_j(user_id, goods_id)
                sql_message.updata_user_sec_buff(user_id, goods_id)
                msg = f"恭喜道友学会神通：{skill_info['name']}！"
        elif skill_type == "身法":
            if int(user_buff_info['effect1_buff']) == int(goods_id):
                msg = f"道友已学会该身法：{skill_info['name']}，请勿重复学习！"
            else:
                sql_message.update_back_j(user_id, goods_id)
                sql_message.updata_user_effect1_buff(user_id, goods_id)
                msg = f"恭喜道友学会身法：{skill_info['name']}！"
        elif skill_type == "瞳术":
            if int(user_buff_info['effect2_buff']) == int(goods_id):
                msg = f"道友已学会该瞳术：{skill_info['name']}，请勿重复学习！"
            else:
                sql_message.update_back_j(user_id, goods_id)
                sql_message.updata_user_effect2_buff(user_id, goods_id)
                msg = f"恭喜道友学会瞳术：{skill_info['name']}！"
        elif skill_type == "功法":
            if int(user_buff_info['main_buff']) == int(goods_id):
                msg = f"道友已学会该功法：{skill_info['name']}，请勿重复学习！"
            else:
                sql_message.update_back_j(user_id, goods_id)
                sql_message.updata_user_main_buff(user_id, goods_id)
                msg = f"恭喜道友学会功法：{skill_info['name']}！"
        elif skill_type == "辅修功法":
            if int(user_buff_info['sub_buff']) == int(goods_id):
                msg = f"道友已学会该辅修功法：{skill_info['name']}，请勿重复学习！"
            else:
                sql_message.update_back_j(user_id, goods_id)
                sql_message.updata_user_sub_buff(user_id, goods_id)
                msg = f"恭喜道友学会辅修功法：{skill_info['name']}！"
        else:
            msg = f"发生未知错误！"

    elif goods_type == "丹药":
        msg = check_use_elixir(user_id, goods_id, num)
        
    elif goods_type == "特殊道具":
        msg = f"请输入：道具使用 {goods_info['name']}"
        await handle_send(bot, event, msg, md_type="背包", k1="使用", v1=f"道具使用 {goods_info['name']}", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
        await use.finish()
    elif goods_type == "神物":
        user_info = sql_message.get_user_info_with_id(user_id)
        user_rank = convert_rank(user_info['level'])[0]
        goods_rank = goods_info['rank'] + added_ranks
        goods_name = goods_info['name']
        if goods_rank < user_rank:
            msg = f"神物：{goods_name}的使用境界为{goods_info['境界']}以上，道友不满足使用条件！"
        else:
            exp = goods_info['buff'] * num
            user_hp = int(user_info['hp'] + (exp / 2))
            user_mp = int(user_info['mp'] + exp)
            user_atk = int(user_info['atk'] + (exp / 10))
            sql_message.update_exp(user_id, exp)
            sql_message.update_power2(user_id)
            sql_message.update_user_attribute(user_id, user_hp, user_mp, user_atk)
            sql_message.update_back_j(user_id, goods_id, num=num, use_key=1)
            msg = f"道友成功使用神物：{goods_name} {num} 个，修为增加 {number_to(exp)}！"

    elif goods_type == "聚灵旗":
        msg = get_use_jlq_msg(user_id, goods_id)

    else:
        msg = "该类型物品调试中，未开启！"

    # 发送结果消息
    await handle_send(bot, event, msg, md_type="背包", k1="使用", v1="使用", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
    await use.finish()

@use_item.handle(parameterless=[Cooldown(cd_time=1.4)])
async def use_item_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """道具使用"""
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
            quantity = max(1, min(quantity, 100))  # 限制使用数量1-10
        except ValueError:
            msg = "请输入有效的数量！"
            await handle_send(bot, event, msg, md_type="背包", k1="使用", v1="道具使用", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
            await use_item.finish()
    
    # 检查背包物品
    goods_id, goods_info = items.get_data_by_item_name(item_name)
    if not goods_id:
        msg = f"物品 {item_name} 不存在，请检查名称是否正确！"
        await handle_send(bot, event, msg, md_type="背包", k1="使用", v1="道具使用", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
        return
    goods_num = sql_message.goods_num(user_info['user_id'], goods_id)
    if goods_num <= 0:
        msg = f"背包中没有足够的 {item_name} ！"
        await handle_send(bot, event, msg, md_type="背包", k1="使用", v1="道具使用", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
        return
    
    # 检查数量是否足够
    if goods_num < quantity:
        quantity = goods_num
    ITEM_HANDLERS = {
        20005: use_wishing_stone,
        20016: use_love_sand,
        20007: use_rift_explore,
        20001: use_rift_key,
        20018: use_rift_boss,
        20012: use_rift_speedup,
        20013: use_rift_big_speedup,
        20010: use_lottery_talisman,
        20014: use_work_order,
        20015: use_work_capture_order,
        20017: use_two_exp_token,
        20019: use_unbind_charm
    }
    handler_func = ITEM_HANDLERS.get(goods_id, None)
    if handler_func:
        await handler_func(bot, event, goods_id, quantity)
    else:
        msg = f"{item_name} 不可直接使用！"
        await handle_send(bot, event, msg, md_type="背包", k1="使用", v1="道具使用", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
        await use_item.finish()

async def use_lottery_talisman(bot, event, item_id, num):
    """使用灵签宝箓"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    user_id = user_info["user_id"]
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
        
    # 批量处理使用灵签宝箓
    success_count = 0
    obtained_items = []
    
    for _ in range(num):
        # 50%概率判断成功
        roll = random.randint(1, 100)
        if roll <= 50:
            success_count += 1
            
            # 随机选择防具或法器类型
            item_type = random.choice(["防具", "法器"])
            zx_rank = random.randint(5, 10)
            item_rank = min(random.randint(zx_rank, zx_rank + 50), 54)
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
                
                obtained_items.append(f"{item_info['level']}:{item_info['name']}")
    
    # 批量消耗灵签宝箓
    sql_message.update_back_j(user_id, item_id, num=num)
    
    # 构建结果消息
    if success_count > 0:
        items_msg = "\n".join(obtained_items)
        result_msg = f"获得以下物品：\n{items_msg}"
    else:
        result_msg = f"未能获得任何物品，运气不佳啊！"
    
    try:
        await handle_send(bot, event, result_msg)
    except ActionFailed:
        await handle_send(bot, event, "使用灵签宝箓结果发送失败！")
    return

async def use_unbind_charm(bot, event, item_id, num):
    """使用解绑符解除物品绑定状态"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    user_id = user_info["user_id"]
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    # 解析参数，获取要解绑的物品名称
    args_text = event.get_plaintext().strip()
    args_text = re.sub(r'^道具使用\s*', '', args_text).strip()
    parts = args_text.split()
    
    if len(parts) < 3:
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

    if target_goods_info['type'] not in ["技能", "装备"]:
        msg = f"物品 {target_item_name} 类型不支持解绑，请更换物品！"
        await handle_send(bot, event, msg)
        return

    # 检查背包中是否有要解绑的物品
    target_goods_num = sql_message.goods_num(user_id, target_goods_id)
    if target_goods_num <= 0:
        msg = f"背包中没有 {target_item_name} ！"
        await handle_send(bot, event, msg)
        return
    
    # 检查物品的绑定数量
    bind_num = sql_message.goods_num(user_id, target_goods_id, num_type='bind')
    if bind_num <= 0:
        msg = f"{target_item_name} 没有绑定数量，无需解绑！"
        await handle_send(bot, event, msg)
        return
    
    # 计算实际可解绑的数量
    actual_unbind = min(num, bind_num)
    
    
    # 使用解绑符解绑物品
    success = sql_message.unbind_item(user_id, target_goods_id, actual_unbind)
    
    if success:
        # 消耗解绑符
        sql_message.update_back_j(user_id, item_id, num=actual_unbind)
        
        msg = f"成功使用解绑符，解除了 {target_item_name} 的 {actual_unbind} 个绑定状态！"
    else:
        msg = "解绑失败，请稍后重试！"
    
    await handle_send(bot, event, msg)
    
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
    
    if item_type is None:
        parts = args_str.split(maxsplit=1)  # 只分割第一个空格
        if len(parts) == 2 and parts[0] in valid_types and parts[1].isdigit():
            item_type = parts[0]
            current_page = int(parts[1])
        elif args_str in valid_types:  # 仅类型，无页码
            item_type = args_str
        else:
            msg = "请输入正确类型【功法|辅修功法|神通|身法|瞳术|丹药|合成丹药|法器|防具|特殊物品|神物】！！！"
            await handle_send(bot, event, msg, md_type="背包", k1="物品", v1="查看修仙界物品", k2="效果", v2="查看效果", k3="帮助", v3="修仙帮助")
            await chakan_wupin.finish()
    
    # 获取物品数据
    if item_type == "特殊物品":
        # 特殊物品包括聚灵旗和特殊道具
        jlq_data = items.get_data_by_item_type(["聚灵旗"])
        ldl_data = items.get_data_by_item_type(["炼丹炉"])
        special_data = items.get_data_by_item_type(["特殊物品"])
        item_data = {**jlq_data, **ldl_data, **special_data}
    else:
        item_data = items.get_data_by_item_type([item_type])
    
    msg_list = []
    
    for item_id, item_info in item_data.items():
        name = item_info['name']
        if item_type in ["功法", "辅修功法", "神通", "身法", "瞳术", "法器", "防具"]:
            desc = get_item_msg(item_id)
            msg = f"ID：{item_id}\n{desc}"
        elif item_type == "特殊物品":
            if item_info['type'] == "聚灵旗":
                msg = f"名字：{name}\n效果：{item_info['desc']}\n修炼速度：{item_info['修炼速度'] * 100}%\n药材速度：{item_info['药材速度'] * 100}%\n"
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
    final_msg = [f"\n☆------{title}------☆"]
    final_msg.extend(paged_items)
    final_msg.append(f"\n第 {current_page}/{total_pages} 页")
    
    if total_pages > 1:
        next_page_cmd = f"查看{item_type}{current_page + 1}"
        final_msg.append(f"输入 {next_page_cmd} 查看下一页")

    page = ["翻页", f"查看修仙界物品{item_type} {current_page + 1}", "查看", "查看效果", "背包", "我的背包", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '修仙界物品', bot.self_id, paged_items, title=title, page=page)
    await chakan_wupin.finish()

@main_back.handle(parameterless=[Cooldown(cd_time=5)])
async def main_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """我的背包"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await main_back.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    user_id = user_info['user_id']
    msg_list = get_user_main_back_msg(user_id)
    title = f"{user_info['user_name']}的背包"
    
    # 分页处理
    per_page = 15
    total_pages = (len(msg_list) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    if not msg_list:
        await handle_send(bot, event, "道友的背包空空如也！")
        await main_back.finish()
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = msg_list[start_idx:end_idx]
    
    title = f"\n☆------{title}------☆"
    final_msg = []
    final_msg.extend(paged_items)
    final_msg.append(f"\n第 {current_page}/{total_pages} 页")
    
    if total_pages > 1:
        next_page_cmd = f"我的背包 {current_page + 1}"
        final_msg.append(f"输入 {next_page_cmd} 查看下一页")
    page = ["翻页", f"我的背包 {current_page + 1}", "使用", "使用", "查看", "查看效果", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '背包', bot.self_id, final_msg, title=title, page=page)
    await main_back.finish()

@yaocai_back.handle(parameterless=[Cooldown(cd_time=5)])
async def yaocai_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """药材背包"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await yaocai_back.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    user_id = user_info['user_id']
    msg_list = get_user_yaocai_back_msg(user_id)
    title = f"{user_info['user_name']}的药材背包"
    
    # 分页处理
    per_page = 15
    total_pages = (len(msg_list) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    if not msg_list:
        await handle_send(bot, event, "道友的药材背包空空如也！")
        await yaocai_back.finish()
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = msg_list[start_idx:end_idx]
    
    title = f"\n☆------{title}------☆"
    final_msg = []
    final_msg.extend(paged_items)
    final_msg.append(f"\n第 {current_page}/{total_pages} 页")
    
    if total_pages > 1:
        next_page_cmd = f"药材背包 {current_page + 1}"
        final_msg.append(f"输入 {next_page_cmd} 查看下一页")
    page = ["翻页", f"药材背包 {current_page + 1}", "使用", "使用", "查看", "查看效果", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '药材背包', bot.self_id, final_msg, title=title, page=page)
    await yaocai_back.finish()

@danyao_back.handle(parameterless=[Cooldown(cd_time=5)])
async def danyao_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """丹药背包"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await danyao_back.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    user_id = user_info['user_id']
    msg_list = get_user_danyao_back_msg(user_id)
    title = f"{user_info['user_name']}的丹药背包"
    
    # 分页处理
    per_page = 15
    total_pages = (len(msg_list) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    if not msg_list:
        await handle_send(bot, event, "道友的丹药背包空空如也！")
        await danyao_back.finish()
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = msg_list[start_idx:end_idx]
    
    title = f"\n☆------{title}------☆"
    final_msg = []
    final_msg.extend(paged_items)
    final_msg.append(f"\n第 {current_page}/{total_pages} 页")
    
    if total_pages > 1:
        next_page_cmd = f"丹药背包 {current_page + 1}"
        final_msg.append(f"输入 {next_page_cmd} 查看下一页")
    page = ["翻页", f"丹药背包 {current_page + 1}", "使用", "使用", "查看", "查看效果", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '丹药背包', bot.self_id, final_msg, title=title, page=page)
    await danyao_back.finish()

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
    except:
        current_page = 1
    
    user_id = user_info['user_id']
    msg_list = get_user_equipment_msg(user_id)
    title = f"{user_info['user_name']}的装备"
    
    # 分页处理
    per_page = 15
    total_pages = (len(msg_list) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    if not msg_list:
        await handle_send(bot, event, "道友的背包中没有装备！")
        await my_equipment.finish()
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = msg_list[start_idx:end_idx]
    
    title = f"\n☆------{title}------☆"
    final_msg = []
    final_msg.extend(paged_items)
    final_msg.append(f"\n第 {current_page}/{total_pages} 页")
    
    if total_pages > 1:
        next_page_cmd = f"我的装备 {current_page + 1}"
        final_msg.append(f"输入 {next_page_cmd} 查看下一页")
    page = ["翻页", f"我的装备 {current_page + 1}", "使用", "使用", "查看", "查看效果", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '我的装备', bot.self_id, final_msg, title=title, page=page)
    await my_equipment.finish()

@yaocai_detail_back.handle(parameterless=[Cooldown(cd_time=5)])
async def yaocai_detail_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """药材背包详情版"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await yaocai_detail_back.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    user_id = user_info['user_id']
    msg_list = get_user_yaocai_detail_back_msg(user_id)
    title = f"{user_info['user_name']}的药材背包详情"
    
    # 分页处理
    per_page = 15
    total_pages = (len(msg_list) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    if not msg_list:
        await handle_send(bot, event, "道友的药材背包空空如也！")
        await yaocai_detail_back.finish()
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = msg_list[start_idx:end_idx]
    
    title = f"\n☆------{title}------☆"
    final_msg = []
    final_msg.extend(paged_items)
    final_msg.append(f"\n第 {current_page}/{total_pages} 页")
    
    if total_pages > 1:
        next_page_cmd = f"药材背包详情 {current_page + 1}"
        final_msg.append(f"输入 {next_page_cmd} 查看下一页")
    page = ["翻页", f"药材背包详情 {current_page + 1}", "使用", "使用", "查看", "查看效果", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '药材背包详情', bot.self_id, final_msg, title=title, page=page)
    await yaocai_detail_back.finish()

check_user_equipment = on_fullmatch("装备检测", priority=4, permission=SUPERUSER, block=True)
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
            
            if not item_data:
                sql_message.send_back(
                    user_id,
                    item_id,
                    item_info['name'],
                    "装备",
                    1,
                    1
                )
                
                problem_users.append({
                    'user_id': user_id,
                    'user_name': user_info['user_name'],
                    'item_name': item_info['name'],
                    'issue': f"已装备{item_type}但背包中不存在",
                    'fixed': "已重新添加到背包"
                })
                fixed_count += 1
            else:
                # 检查装备数量是否为0或负数
                if item_data['goods_num'] <= 0:
                    # 修复数量为1
                    new_num = 1 + abs(item_data['goods_num'])
                    sql_message.send_back(
                        user_id,
                        item_id,
                        item_data['goods_name'],
                        "装备",
                        new_num,
                        1
                    )
                    
                    problem_users.append({
                        'user_id': user_id,
                        'user_name': user_info['user_name'],
                        'item_name': item_data['goods_name'],
                        'issue': f"已装备{item_type}但数量异常({item_data['goods_num']})",
                        'fixed': f"数量修复为{new_num}"
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
    result_msg.append("2. 修复了装备不存在或数量异常的问题")
    result_msg.append("3. 修复了绑定数量异常的问题")
    
    await send_msg_handler(bot, event, '装备检测', bot.self_id, result_msg)
    await check_user_equipment.finish()

check_user_back = on_fullmatch("背包检测", priority=4, permission=SUPERUSER, block=True)
@check_user_back.handle(parameterless=[Cooldown(cd_time=1.4)])
async def check_user_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """背包上限检测"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    msg = "开始检测用户背包，请稍候..."
    await handle_send(bot, event, msg)
    result = sql_message.check_and_adjust_goods_quantity()
    msg = f"处理物品数量异常用户：{result}"
    await handle_send(bot, event, msg)

@compare_items.handle(parameterless=[Cooldown(cd_time=30)])
async def compare_items_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await compare_items.finish()
    
    user_id = user_info['user_id']
    item_names = args.extract_plain_text().split()
    
    if len(item_names) != 2:
        await handle_send(bot, event, "请提供两个物品名称进行对比，格式：对比 物品1 物品2")
        return

    item_name1, item_name2 = item_names

    item1_info = items.get_data_by_item_name(item_name1)[1]
    item2_info = items.get_data_by_item_name(item_name2)[1]

    if not item1_info:
        await handle_send(bot, event, f"物品 '{item_name1}' 不存在，请检查名称是否正确！")
        return
    if not item2_info:
        await handle_send(bot, event, f"物品 '{item_name2}' 不存在，请检查名称是否正确！")
        return

    if item1_info['item_type'] != item2_info['item_type']:
        await handle_send(bot, event, f"物品的类型不一致，无法进行对比！\n{item_name1}类型：{item1_info['item_type']}\n{item_name2}类型：{item2_info['item_type']}")
        return

    item_type = item1_info['item_type']

    basic_info = format_basic_info(item_name1, item1_info, item_name2, item2_info, item_type)

    if item_type == '功法':
        comparison_result = compare_main(item_name1, item1_info, item_name2, item2_info)
    elif item_type in ['法器', '防具']:
        comparison_result = compare_equipment(item_name1, item1_info, item_name2, item2_info)
    elif item_type == '神通':
        comparison_result = compare_skill_types(item_name1, item1_info, item_name2, item2_info)
    else:
        await handle_send(bot, event, f"暂不支持类型 '{item_type}' 的物品对比！")
        return
    msg = []
    msg.append(basic_info)
    msg.append(comparison_result)
    await send_msg_handler(bot, event, '快速对比', bot.self_id, msg)
    await compare_items.finish()

def get_skill_type(skill_type):
    if skill_type == 1:
        skill_desc = "伤害"
    elif skill_type == 2:
        skill_desc = "增强"
    elif skill_type == 3:
        skill_desc = "持续"
    elif skill_type == 4:
        skill_desc = "叠加"
    elif skill_type == 5:
        skill_desc = "波动"
    elif skill_type == 6:
        skill_desc = "封印"
    elif skill_type == 7:
        skill_desc = "变化"
    else:
        skill_desc = "未知"
    return skill_desc

def format_basic_info(item_name1, item1_info, item_name2, item2_info, item_type):
    rank_name_list = convert_rank("江湖好手")[1]
    if int(item1_info['rank']) == -5:
        item1_rank = 23
    else:
        item1_rank = int(item1_info['rank']) + added_ranks
    item1_required_rank_name = rank_name_list[len(rank_name_list) - item1_rank]
    if int(item2_info['rank']) == -5:
        item2_rank = 23
    else:
        item2_rank = int(item2_info['rank']) + added_ranks
    item2_required_rank_name = rank_name_list[len(rank_name_list) - item2_rank]
    
    if item_type == '功法':
        basic_info = [
            f"📜 【功法信息】",
            f"═════════════",
            f"【{item_name1}】",
            f"• 品阶：{item1_info.get('level', '未知')}",
            f"• 类型：{item1_info.get('type', '未知')}",
            f"• 境界：{item1_required_rank_name}",
            f"• 描述：{item1_info.get('desc', '暂无描述')}",
            f"",
            f"【{item_name2}】",
            f"• 品阶：{item2_info.get('level', '未知')}",
            f"• 类型：{item2_info.get('type', '未知')}",
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
    
    return "\n".join(basic_info)

def format_number(value, multiply_hundred=True):
    if isinstance(value, (int, float)):
        if multiply_hundred:
            percentage = value * 100
            if isinstance(percentage, int) or percentage.is_integer():
                return f"{int(percentage)}%"
            rounded = round(percentage, 0)
            if rounded.is_integer():
                return f"{int(rounded)}%"
            return f"{rounded:.0f}%"
        else:
            if isinstance(value, int) or value.is_integer():
                return f"{int(value)}"
            return f"{value:.1f}"
    return str(value)

def format_difference(diff, multiply_hundred=True):
    if isinstance(diff, (int, float)):
        if multiply_hundred:
            percentage_diff = diff * 100
            if isinstance(percentage_diff, int) or percentage_diff.is_integer():
                return f"{abs(int(percentage_diff))}%"
            rounded = round(percentage_diff, 0)
            if rounded.is_integer():
                return f"{abs(int(rounded))}%"
            return f"{abs(rounded):.0f}%"
        else:
            if isinstance(diff, int) or diff.is_integer():
                return f"{abs(int(diff))}"
            return f"{abs(diff):.1f}"
    return str(diff)

def compare_main(item_name1, item1_info, item_name2, item2_info):
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
    
    no_multiply_params = {'two_buff', 'number', 'dan_exp', 'dan_buff', 'reap_buff', 'exp_buff'}
    
    has_comparison = False
    for param, description in skill_params.items():
        value1 = item1_info.get(param, 0)
        value2 = item2_info.get(param, 0)
        
        if value1 == 0 and value2 == 0:
            continue
        else:
            has_comparison = True
            multiply_hundred = param not in no_multiply_params
        
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
        comparison.append("• 两个物品在可对比的属性上均无特殊效果")
    
    comparison.append("═════════════")
    return "\n".join(comparison)

def compare_equipment(item_name1, item1_info, item_name2, item2_info):
    comparison = [
        f"\n⚔️ 【{item_name1} ↔ {item_name2}】", 
        f"═════════════"
    ]
    equipment_params = {
        'atk_buff': '攻击',
        'crit_buff': '会心',
        'def_buff': '减伤',
        'mp_buff': '降耗',
        'critatk': '会心伤害',
    }
    
    has_comparison = False
    for param, description in equipment_params.items():
        value1 = item1_info.get(param, 0)
        value2 = item2_info.get(param, 0)
        
        if value1 == 0 and value2 == 0:
            continue
        else:
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

def compare_skill_types(item_name1, skill1, item_name2, skill2):
    comparison = []
    skill_type1 = skill1.get('skill_type', 0)
    skill_type2 = skill2.get('skill_type', 0)
    skill_desc1 = get_skill_type(skill_type1)
    skill_desc2 = get_skill_type(skill_type2)
    
    if skill_type1 == skill_type2:
        if skill_type1 == 1:  # 伤害类
            comparison.append(f"🔥【{item_name1} ↔ {item_name2}】")
            comparison.append(f"═════════════")
            
            # 处理伤害值，支持列表（多段伤害）
            atkvalue1 = skill1.get('atkvalue', [0])
            atkvalue2 = skill2.get('atkvalue', [0])
            
            # 计算总伤害
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
        
        elif skill_type1 == 2:  # 增强类
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
        
        elif skill_type1 == 3:  # 持续类
            comparison.append(f"🔄【{item_name1} ↔ {item_name2}】")
            comparison.append(f"═════════════")
            continuous_params = {
                'buffvalue': ('效果强度', True),
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
        
        elif skill_type1 == 4:  # 叠加类
            comparison.append(f"📈【{item_name1} ↔ {item_name2}】")
            comparison.append(f"═════════════")
            stack_params = {
                'stack': ('叠加层数', False),
                'buffvalue': ('每层效果', True),
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
        
        elif skill_type1 == 5:  # 波动类
            comparison.append(f"🌊【{item_name1} ↔ {item_name2}】")
            comparison.append(f"═════════════")
            wave_params = {
                'min_effect': ('最小效果', True),
                'max_effect': ('最大效果', True),
                'turncost': ('持续回合', False),
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
        
        elif skill_type1 == 6:  # 封印类
            comparison.append(f"🔒【{item_name1} ↔ {item_name2}】")
            comparison.append(f"═════════════")
            seal_params = {
                'seal_effect': ('封印效果', True),
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
        
        else:
            comparison.append("🤔 【未知类型】")
            comparison.append(f"• 该神通类型暂不支持对比！类型: {skill_type1}")
    else:
        comparison.append("⚠️ 【类型不匹配】")
        comparison.append(f"• {item_name1}类型: {skill_desc1}，{item_name2}类型: {skill_desc2}")
        comparison.append("• 不同类型的神通无法进行对比！")
    
    comparison.append("═════════════")
    return "\n".join(comparison)

