import asyncio
import random
import time
import re
import os
import json
from pathlib import Path
from datetime import datetime, timedelta
from nonebot import require
from ..on_compat import on_command
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
    Txt2Img, number_to, handle_send, send_help_message
)
from ..xiuxian_utils.sect_utils import get_user_sect_fairyland_level as _get_user_sect_fairyland_level
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, PlayerDataManager, get_weapon_info_msg, get_armor_info_msg,
    get_sec_msg, get_main_info_msg, get_sub_info_msg, UserBuffDate, OtherSet, calc_accessory_effects
)
from ..xiuxian_rift import use_rift_explore, use_rift_key, use_rift_boss, use_rift_speedup, use_rift_big_speedup
from ..xiuxian_impart import use_wishing_stone, use_love_sand
from ..xiuxian_work import use_work_order, use_work_capture_order
from ..xiuxian_buff import use_two_exp_token
from ..xiuxian_arena import use_arena_challenge_ticket
from ..xiuxian_tianti.tianti_data import TiantiDataManager
from ..xiuxian_tianti.tianti_service import grant_tianti_settle_minutes
from ..xiuxian_config import XiuConfig, convert_rank, added_ranks
from ...paths import get_paths
from ..xiuxian_utils.pet_system import PET_BAG_LIMIT, PET_EGG_IDS, PET_EGG_RARITY_KEY, can_add_pets, grant_pet_by_rarity
from .back_util import *
from .cultivation_item_service import CultivationItemService
from .equipment_service import EquipmentService
from .lottery_talisman_service import LotteryReward, LotteryTalismanService
from .stone_reward_service import StoneItemRewardService
from .three_cultivation_pill_service import ThreeCultivationPillService
from .unbind_item_service import UnbindItemService
from . import accessory as _accessory  # noqa: F401
from .accessory_helpers import AFFIX_KEY_MAP, SET_BONUS, ACCESSORY_BAG_LIMIT, add_accessory_to_bag, can_add_accessories, quality_to_cn  # noqa: F401
from .backpack_render import (
    _build_backpack_md_with_sections,
    _build_backpack_fallback_with_sections,
    _paginate_sections,
    get_skill_type,
    format_basic_info,
    format_number,
    format_difference,
    compare_main,
    compare_equipment,
    compare_skill_types,
)


# 初始化组件
items = Items()
sql_message = XiuxianDateManage()
equipment_service = EquipmentService(get_paths().game_db)
cultivation_item_service = CultivationItemService(get_paths().game_db)
stone_reward_service = StoneItemRewardService(get_paths().game_db)
three_cultivation_pill_service = ThreeCultivationPillService(get_paths().game_db)
unbind_item_service = UnbindItemService(get_paths().game_db)
lottery_talisman_service = LotteryTalismanService(get_paths().game_db)
player_data_manager = PlayerDataManager()
tianti_manager = TiantiDataManager()
scheduler = require("nonebot_plugin_apscheduler").scheduler
added_ranks = added_ranks()
# 技能学习确认缓存
confirm_use_cache = {}


def _equipment_operation_id(event, action, goods_id):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"equipment:{event_id}:{action}:{goods_id}"
    return f"equipment:{action}:{goods_id}:{time.time_ns()}"


def _stone_reward_operation_id(event, reward_type, user_id):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"stone-reward:{event_id}:{reward_type}:{user_id}"
    return f"stone-reward:{reward_type}:{user_id}:{time.time_ns()}"


def _cultivation_item_operation_id(event, user_id, goods_id):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"cultivation-item:{event_id}:{user_id}:{goods_id}"
    return f"cultivation-item:{user_id}:{goods_id}:{time.time_ns()}"


def _lottery_talisman_operation_id(event, user_id, goods_id):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"lottery-talisman:{event_id}:{user_id}:{goods_id}"
    return f"lottery-talisman:{user_id}:{goods_id}:{time.time_ns()}"


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


def get_alchemy_available_num(back_item):
    """炼金允许消耗绑定物品，但保留已装备/使用状态占用的数量。"""
    goods_num = int(back_item.get("goods_num", 0) or 0)
    state = int(back_item.get("state", 0) or 0)
    return max(0, goods_num - max(0, state))


def get_alchemy_reserved_num(back_item):
    """返回炼金时需要保留的已装备/使用数量。"""
    return max(0, int(back_item.get("state", 0) or 0))


@check_item_effect.handle(parameterless=[Cooldown(cd_time=0)])
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
    
@back_help.handle(parameterless=[Cooldown(cd_time=0)])
async def back_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """背包帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    msg = """
**背包帮助**
---
**查看**
- 我的背包 [页码]：查看背包物品
- 药材背包 [页码]：查看药材类物品
- 丹药背包 [页码]：查看丹药类物品
- 我的装备 [页码]：查看背包装备
- 查看修仙界物品+类型 [页码]：查看物品图鉴
- 查看效果+物品名：查看物品详情
- 灵石：查看当前灵石数量

**使用与整理**
- 使用+物品名 [数量]：使用物品
- 换装/卸装+装备名：卸下装备
- 炼金+物品名 [数量]：将物品转化为灵石
- 快速炼金 类型 品阶：批量炼金指定类型物品
- 快速对比 [物品1] [物品2]：对比装备或者功法的属性

**其他入口**
- 饰品帮助：查看饰品系统全部命令
""".strip()

    await send_help_message(
        bot, event, msg,
        k1="背包", v1="我的背包",
        k2="饰品", v2="饰品帮助",
        k3="效果", v3="查看效果"
    )
    await back_help.finish()

@xiuxian_sone.handle(parameterless=[Cooldown(cd_time=0)])
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

@goods_re_root.handle(parameterless=[Cooldown(cd_time=0)])
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
    back_item = sql_message.get_item_by_good_id_and_user_id(user_id, goods_id)
    if not back_item or int(back_item.get("goods_num", 0) or 0) <= 0:
        msg = f"背包中没有足够的 {item_name} ！"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        return
    goods_num = int(back_item.get("goods_num", 0) or 0)

    # 检查是否是禁止炼金的物品
    if str(goods_id) in BANNED_ITEM_IDS_ALCHEMY:
        msg = f"物品 {item_name} 禁止炼金！"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await goods_re_root.finish()

    if get_item_msg_rank(goods_id) == 520: # 520通常表示不支持的物品类型
        msg = "此类物品不支持炼金！"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await goods_re_root.finish()

    available_num = get_alchemy_available_num(back_item)
    if available_num <= 0:
        msg = f"{item_name}当前没有可炼金数量，已装备或使用中的物品会被保留！"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await goods_re_root.finish()

    num = 1
    try:
        if len(args) > 1:
            input_num = int(args[1])
            if input_num <= 0:
                msg = "炼金数量必须大于0！"
                await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
                await goods_re_root.finish()
            if input_num > available_num:
                reserved_num = get_alchemy_reserved_num(back_item)
                msg = f"道友背包中的{item_name}可炼金数量不足，当前共有{goods_num}个，可炼金{available_num}个"
                if reserved_num > 0:
                    msg += f"，已装备或使用中保留{reserved_num}个"
                msg += "！"
                await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
                await goods_re_root.finish()
            num = input_num
    except ValueError: # 如果第二个参数不是有效数字，则默认为1
            num = 1 
    
    price = get_recover(goods_id, num)
    if price <= 0: # 某些物品炼金价格可能为0或负数
        msg = f"物品：{item_name}炼金失败，凝聚{number_to(price)}枚灵石！"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await goods_re_root.finish()

    if not sql_message.alchemy_items(user_id, price, [(goods_id, num)]):
        msg = "炼金失败，背包数量发生变化，请重试！"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await goods_re_root.finish()

    msg = f"物品：{item_name} 数量：{num} 炼金成功，凝聚{number_to(price)}枚灵石！"
    await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
    await goods_re_root.finish()

@fast_alchemy.handle(parameterless=[Cooldown(cd_time=0)])
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
                available = get_alchemy_available_num(item)
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
        consume_items = []
        
        for elixir in elixirs:
            # 计算价格
            total_price = get_recover(elixir['id'], elixir['num'])

            total_stone += total_price
            consume_items.append((elixir['id'], elixir['num']))
            results.append(f"{elixir['name']} x{elixir['num']} → {number_to(total_price)}灵石")

        if not sql_message.alchemy_items(user_id, total_stone, consume_items):
            msg = "快速炼金失败，背包数量发生变化，请重试！"
            await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="快速炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
            await fast_alchemy.finish()
        
        # 构建结果消息
        msg = [
            f"【快速炼金结果】",
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
            available_num = get_alchemy_available_num(item)
            reserved_num = get_alchemy_reserved_num(item)

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
                    'reserved_num': reserved_num,
                })
    
    if not items_to_alchemy:
        msg = f"🔍 背包中没有符合条件的【{item_type}·{rank_name}】物品"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="快速炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await fast_alchemy.finish()
    
    # === 自动炼金逻辑 ===
    total_stone = 0
    result_msg = []
    consume_items = []
    
    for item in items_to_alchemy:
        
        # 计算价格
        total_price = get_recover(item['id'], item['available_num'])

        total_stone += total_price
        consume_items.append((item['id'], item['available_num']))
        
        # 添加装备状态信息到结果消息
        status_info = ""
        if item['reserved_num'] > 0:
            status_info = f" (已装备或使用中，保留{item['reserved_num']}个)"
        
        result_msg.append(f"{item['name']} x{item['available_num']}{status_info} → {number_to(total_price)}灵石")

    if not sql_message.alchemy_items(user_id, total_stone, consume_items):
        msg = "快速炼金失败，背包数量发生变化，请重试！"
        await handle_send(bot, event, msg, md_type="背包", k1="炼金", v1="快速炼金", k2="灵石", v2="灵石", k3="背包", v3="我的背包")
        await fast_alchemy.finish()
    
    # 构建结果消息
    msg = [
        f"【快速炼金结果】",
        f"类型：{item_type}",
        f"品阶：{rank_name}",
        *result_msg,
        f"总计获得：{number_to(total_stone)}灵石"
    ]
    
    await send_msg_handler(bot, event, '快速炼金', bot.self_id, msg)
    await fast_alchemy.finish()

@no_use_zb.handle(parameterless=[Cooldown(cd_time=0)])
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
    if not back_msg:
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
            item_type = items.get_data_by_item_id(goods_id)["item_type"]
            result = equipment_service.change(
                _equipment_operation_id(event, "unequip", goods_id),
                user_id,
                goods_id,
                item_type,
                equip=False,
            )
            msg = (
                f"成功卸载装备{arg}！"
                if result.succeeded
                else "装备状态发生变化，请刷新背包后重试！"
            )
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

@use.handle(parameterless=[Cooldown(cd_time=0)])
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

        def _collect_package_rewards():
            rewards = []
            errors = []
            if int(goods_info.get("roll", 0) or 0) == 1:
                roll_pool = goods_info.get("roll_pool", [])
                if not isinstance(roll_pool, list) or not roll_pool:
                    errors.append(f"【失败】{package_name}：roll_pool为空或配置错误")
                else:
                    rewards.append(random.choice(roll_pool))
                return rewards, errors

            i = 1
            while True:
                buff_key = f"buff_{i}"
                name_key = f"name_{i}"
                type_key = f"type_{i}"
                amount_key = f"amount_{i}"
                quality_key = f"quality_{i}"

                if name_key not in goods_info:
                    break

                rewards.append({
                    "buff": goods_info.get(buff_key, None),
                    "name": goods_info.get(name_key),
                    "type": goods_info.get(type_key, None),
                    "amount": goods_info.get(amount_key, 1),
                    "quality": goods_info.get(quality_key, 1)
                })
                i += 1

            return rewards, errors

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

        package_rewards = []
        accessory_need = 0
        for _ in range(num):
            rewards, errors = _collect_package_rewards()
            package_rewards.append(rewards)
            all_msgs.extend(errors)
            for rwd in rewards:
                if rwd.get("type") != "饰品" or rwd.get("buff", None) is None:
                    continue
                try:
                    accessory_need += max(0, int(rwd.get("amount", 1) or 1))
                except Exception:
                    accessory_need += 1

        if accessory_need > 0:
            ok, owned, remaining = can_add_accessories(str(user_id), accessory_need)
            if not ok:
                msg = (
                    f"饰品背包容量不足，无法打开{package_name}。\n"
                    f"当前容量：{owned}/{ACCESSORY_BAG_LIMIT}，剩余{remaining}格；"
                    f"本次将获得饰品{accessory_need}件。\n"
                    "请先分解或整理饰品。"
                )
                await handle_send(bot, event, msg, md_type="背包", k1="饰品", v1="饰品背包", k2="分解", v2="快速分解饰品")
                await use.finish()
                return

        # 使用num个礼包
        for rewards in package_rewards:
            for rwd in rewards:
                try:
                    all_msgs.append(_grant_one_reward(rwd))
                except Exception as e:
                    all_msgs.append(f"【失败】{rwd.get('name', '未知物品')}：{e}")

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
            item_type = goods_info["item_type"]
            result = equipment_service.change(
                _equipment_operation_id(event, "equip", goods_id),
                user_id,
                goods_id,
                item_type,
                equip=True,
            )
            msg = (
                f"成功装备 {item_name}！"
                if result.succeeded
                else "装备状态发生变化，请刷新背包后重试！"
            )

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
        msg = check_use_elixir(
            user_id,
            goods_id,
            num,
            _cultivation_item_operation_id(event, user_id, goods_id),
        )

    elif goods_type == "特殊道具":
        msg = f"请使用【道具使用 {goods_info['name']}】命令来使用此道具。"

    elif goods_type == "神物":
        user_info_full = sql_message.get_user_info_with_id(user_id)
        if (goods_info['rank'] + added_ranks) < convert_rank(user_info_full['level'])[0]:
            msg = f"神物：{goods_info['name']}的使用境界为{goods_info['境界']}以上，道友不满足条件！"
        else:
            tianti_minutes = int(goods_info.get("tianti_settle_minutes", 0) or 0)
            if goods_info.get("buff_type") == "tianti_hp_time" and tianti_minutes > 0:
                total_minutes = tianti_minutes * num
                data = tianti_manager.get_user_tianti_info(str(user_id))
                result = grant_tianti_settle_minutes(
                    data,
                    total_minutes,
                    sect_fairyland_level=_get_user_sect_fairyland_level(user_info_full),
                )
                tianti_manager.save_user_tianti_info(str(user_id), data)
                sql_message.update_back_j(user_id, goods_id, num=num, use_key=1)

                bath_msg = ""
                if result.get("bath"):
                    bath = result["bath"]
                    bath_msg = f"\n药浴加成：{bath['name']}，有效至{bath['end_time'].strftime('%H:%M')}"
                elif result.get("bath_expired"):
                    bath_msg = "\n药浴已过期，本次未获得药浴加成。"
                sect_bonus_msg = ""
                if float(result.get("sect_bonus", 0) or 0) > 0:
                    sect_bonus_msg = f"\n宗门炼体堂加成：{float(result['sect_bonus']) * 100:.0f}%"

                msg = (
                    f"道友成功使用神物：{goods_info['name']} {num} 个。\n"
                    f"获得炼体结算时间：{total_minutes}分钟\n"
                    f"本次获得炼体气血：{number_to(result['real_gain'])}\n"
                    f"当前炼体气血：{number_to(result['new_hp'])}"
                    f"{bath_msg}"
                    f"{sect_bonus_msg}"
                )
            else:
                exp = goods_info['buff'] * num
                root_rate = sql_message.get_root_rate(user_info_full['root_type'], user_id)
                level_spend = jsondata.level_data()[user_info_full['level']]["spend"]
                result = cultivation_item_service.apply(
                    _cultivation_item_operation_id(event, user_id, goods_id),
                    user_id,
                    goods_id,
                    num,
                    exp,
                    hp_gain=int(exp / 2),
                    mp_gain=exp,
                    atk_gain=int(exp / 10),
                    power_multiplier=float(root_rate) * float(level_spend),
                )
                msg = (
                    f"道友成功使用神物：{goods_info['name']} {num} 个，修为增加 {number_to(exp)}！"
                    if result.succeeded
                    else "神物数量或角色状态已经变化，请刷新背包后重试！"
                )

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

@confirm_use.handle(parameterless=[Cooldown(cd_time=0)])
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

@use_item.handle(parameterless=[Cooldown(cd_time=0)])
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
        20022: use_three_cultivation_pill, # 三转玄丹
        20024: use_arena_challenge_ticket, # 竞技场挑战券
        **{egg_id: use_pet_egg_item for egg_id in PET_EGG_IDS},
    }
    
    handler_func = ITEM_HANDLERS.get(goods_id, None)
    if handler_func:
        # 调用对应的处理函数
        await handler_func(bot, event, goods_id, quantity)
    else:
        msg = f"{item_name} 是特殊道具，但目前没有对应的使用方法！"
        await handle_send(bot, event, msg, md_type="背包", k1="使用", v1="道具使用", k2="存档", v2="我的修仙信息", k3="背包", v3="我的背包")
        
    await use_item.finish()

async def use_pet_egg_item(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id: int, num: int):
    """使用宠物蛋，按道具配置孵化指定稀有度宠物。"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    item_info = items.get_data_by_item_id(item_id)
    if not item_info:
        await handle_send(bot, event, "宠物蛋配置不存在，无法孵化。")
        return

    rarity = item_info.get(PET_EGG_RARITY_KEY) or PET_EGG_IDS.get(int(item_id))
    if rarity not in ["常见", "普通", "卓越", "传说", "神话"]:
        await handle_send(bot, event, "宠物蛋稀有度配置异常，无法孵化。")
        return

    have = sql_message.goods_num(user_id, item_id)
    use_num = min(max(1, int(num)), int(have))
    if use_num <= 0:
        await handle_send(bot, event, f"背包中没有{item_info.get('name', '宠物蛋')}。")
        return
    ok, owned, remaining = can_add_pets(user_id, use_num)
    if not ok:
        await handle_send(
            bot,
            event,
            (
                f"宠物背包容量不足，无法使用{item_info.get('name', '宠物蛋')}。\n"
                f"当前容量：{owned}/{PET_BAG_LIMIT}，剩余{remaining}格；本次需要{use_num}格。\n"
                "请先放生或整理宠物。"
            ),
            md_type="背包",
            k1="宠物背包",
            v1="宠物背包",
            k2="放生",
            v2="一键放生",
        )
        return

    lines = []
    success_count = 0
    for _ in range(use_num):
        try:
            pet, location = grant_pet_by_rarity(user_id, rarity)
        except Exception as e:
            lines.append(f"孵化失败：{e}")
            break

        success_count += 1
        loc_msg = "已自动出战" if location == "active" else "已放入宠物背包"
        lines.append(
            f"{pet.get('form_name', pet.get('name', '未知宠物'))}"
            f"（{pet.get('rarity', rarity)}·{pet.get('race', '凡兽')}·{pet.get('type', '攻击')}，UID:{pet.get('uid')}，{loc_msg}）"
        )

    if success_count > 0:
        sql_message.update_back_j(user_id, item_id, num=success_count)

    msg = f"道友使用{item_info.get('name', '宠物蛋')} x{success_count}，孵化结果：\n" + "\n".join(lines[:30])
    if len(lines) > 30:
        msg += f"\n...其余{len(lines) - 30}条已省略"

    await handle_send(bot, event, msg, md_type="背包", k1="宠物", v1="我的宠物", k2="宠物背包", v2="宠物背包")
    return

async def use_lottery_talisman(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id: int, num: int):
    """使用灵签宝箓"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    user_id = user_info["user_id"]
    rewards = []
    for _ in range(num):
        roll = random.randint(1, 100)
        if roll <= 50:
            item_type = random.choice(["防具", "法器"])
            zx_rank = random.randint(5, 10)
            item_rank = min(random.randint(zx_rank, zx_rank + 50), 54)
            if item_rank == 5 and random.randint(1, 100) != 100:
                item_rank = 16

            item_id_list = items.get_random_id_list_by_rank_and_item_type(item_rank, item_type)
            if item_id_list:
                rank_id = random.choice(item_id_list)
                item_info = items.get_data_by_item_id(rank_id)
                rewards.append(
                    LotteryReward(rank_id, item_info["name"], item_info["type"], 1)
                )

    result = lottery_talisman_service.apply(
        _lottery_talisman_operation_id(event, user_id, item_id),
        user_id,
        item_id,
        num,
        rewards,
        max_goods_num=XiuConfig().max_goods_num,
    )
    if not result.succeeded:
        await handle_send(bot, event, "灵签宝箓数量已经变化，请刷新背包后重试！")
        return

    obtained_items = {}
    for reward in result.rewards:
        obtained_items[reward.name] = obtained_items.get(reward.name, 0) + reward.quantity
    if obtained_items:
        items_msg = "\n".join(f"{name} x{count}" for name, count in obtained_items.items())
        result_msg = f"道友使用灵签宝箓 {result.quantity} 个，成功获得以下物品：\n{items_msg}"
    else:
        result_msg = f"道友使用灵签宝箓 {result.quantity} 个，未能获得任何物品，运气不佳啊！"

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

    result = unbind_item_service.apply(
        _cultivation_item_operation_id(event, user_id, item_id),
        user_id,
        item_id,
        target_goods_id,
        num,
    )
    if result.succeeded:
        msg = f"成功使用解绑符，解除了 {target_item_name} 的 {result.quantity} 个绑定状态！"
    elif result.status == "target_missing":
        msg = f"背包中没有 {target_item_name} ！"
    elif result.status == "not_bound":
        msg = f"{target_item_name} 没有绑定数量，无需解绑！"
    else:
        msg = "解绑符数量或物品绑定状态已经变化，请刷新背包后重试！"
    
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

    rolled_rewards = [
        random.choices(
            [t[1] for t in tiers],
            weights=[t[0] for t in tiers],
            k=1
        )[0]
        for _ in range(num)
    ]
    reward = stone_reward_service.apply(
        _stone_reward_operation_id(event, "spirit_stone_bag", user_id),
        user_id,
        reward_type="spirit_stone_bag",
        item_id=item_id,
        rewards=rolled_rewards,
    )
    if not reward.succeeded:
        await handle_send(bot, event, "灵石福袋数量或角色状态已发生变化，请重新查看背包。")
        return

    results = [
        f"{next(t[2] for t in tiers if t[1] == value)}档：获得 {number_to(value)} 灵石"
        for value in reward.rewards
    ]

    # 构造消息
    lines = [
        f"【灵石福袋 ×{reward.quantity}】",
        f"累计获得：{number_to(reward.total_stone)} 灵石",
        *results,
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
    rolled_rewards = [random.randint(MIN_STONE, MAX_STONE) for _ in range(num)]
    reward = stone_reward_service.apply(
        _stone_reward_operation_id(event, "tianji_stone_trigger", user_id),
        user_id,
        reward_type="tianji_stone_trigger",
        item_id=item_id,
        rewards=rolled_rewards,
    )
    if not reward.succeeded:
        await handle_send(bot, event, "天机灵石引数量或角色状态已发生变化，请重新查看背包。")
        return

    results = []
    for i, roll in enumerate(reward.rewards):
        if roll <= 15_000_000:
            desc = "微薄"
        elif roll <= 50_000_000:
            desc = "尚可"
        elif roll <= 75_000_000:
            desc = "丰厚"
        else:
            desc = "惊人！✨"

        results.append(f"第{i+1}次：{desc} → {number_to(roll)} 灵石")

    # 构造消息
    lines = [
        f"【天机灵石引 ×{reward.quantity}】",
        f"累计获得：{number_to(reward.total_stone)} 灵石",
        *results,
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

    result = three_cultivation_pill_service.apply(
        _cultivation_item_operation_id(event, user_id, item_id),
        user_id,
        item_id,
        num,
        total_exp_gain,
        max_exp=max_exp,
        power_multiplier=float(level_rate) * float(realm_rate),
    )
    if not result.succeeded:
        await handle_send(bot, event, "三转玄丹数量或角色状态已经变化，请刷新背包后重试！")
        return

    recovery_msg = ""
    if result.hp_after > result.hp_before:
        recovery_msg += f",回复气血：{number_to(result.hp_after - result.hp_before)}"
    if result.mp_after > result.mp_before:
        recovery_msg += f",回复真元：{number_to(result.mp_after - result.mp_before)}"

    # 提示语
    msg_lines = [
        f"【使用 三转玄丹 ×{result.quantity}】",
        f"获得：{number_to(result.exp_gain)}修为{recovery_msg}"
    ]

    if result.exp_gain < result.requested_exp:
        msg_lines.append("（已达当前境界上限，剩余修为溢出）")

    msg_lines.extend([
        "玄丹入腹，灵气暴涨，道友修为精进！"
    ])

    await handle_send(bot, event, "\n".join(msg_lines))


@chakan_wupin.handle(parameterless=[Cooldown(cd_time=0)])
async def chakan_wupin_(
    bot: Bot, 
    event: GroupMessageEvent | PrivateMessageEvent, 
    args: Message = CommandArg()
):
    """查看修仙界物品（支持 类型+页码 或 类型 + 空格 + 页码）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    args_str = args.extract_plain_text().strip()
    
    # 支持的类型列表
    valid_types = ["功法", "辅修功法", "神通", "身法", "瞳术", "丹药", "合成丹药", "法器", "防具", "饰品", "特殊物品", "神物"]
    
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
            msg = "请输入正确类型【功法|辅修功法|神通|身法|瞳术|丹药|合成丹药|法器|防具|饰品|特殊物品|神物】！！！"
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
        if item_type in ["功法", "辅修功法", "神通", "身法", "瞳术", "法器", "防具", "饰品"]:
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
            if item_info.get("buff_type") == "tianti_hp_time":
                minutes = int(item_info.get("tianti_settle_minutes", 0) or 0)
                effect = f"炼体结算时间{minutes}分钟"
            else:
                effect = f"增加{number_to(item_info.get('buff', 0))}修为"
            msg = f"※名字:{name}\n效果：{desc}\n境界：{rank}\n{effect}\n"
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
        'hp': 1, 'stamina': 2, 'all': 3, 'level_up_rate': 4,
        'level_up_big': 5, 'atk_buff': 6, 'exp_up': 7, 'level_up': 8,
        '未知': 999
    }
    buff_type_names = {
        'hp': '气血回复丹药',
        'stamina': '体力回复丹药',
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
        fallback_text = _build_backpack_fallback_with_sections(
            title=f"{user_info['user_name']}的背包",
            sections=page_sections,
            current_page=current_page,
            total_pages=total_pages,
            show_use_btn=True,
            next_cmd=f"我的背包 {current_page + 1}"
        )
        await handle_send(bot, event, md_text, native_markdown=True, fallback_msg=fallback_text)
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

    title_display = f"【{title}】"
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
        fallback_text = _build_backpack_fallback_with_sections(
            title=f"{user_info['user_name']}的丹药背包",
            sections=page_sections,
            current_page=current_page,
            total_pages=total_pages,
            show_use_btn=True,
            next_cmd=f"丹药背包 {current_page + 1}"
        )
        await handle_send(bot, event, md_text, native_markdown=True, fallback_msg=fallback_text)
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

    title_display = f"【{title}】"
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
        fallback_text = _build_backpack_fallback_with_sections(
            title=f"{user_info['user_name']}的药材背包",
            sections=page_sections,
            current_page=current_page,
            total_pages=total_pages,
            show_use_btn=False,
            next_cmd=f"药材背包 {current_page + 1}"
        )
        await handle_send(bot, event, md_text, native_markdown=True, fallback_msg=fallback_text)
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

    title_display = f"【{title}】"
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
    
    title_display = f"【{title}】"
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
    
    title_display = f"【{title}】"
    final_msg = []
    final_msg.extend(paged_items)
    final_msg.append(f"\n第 {current_page}/{total_pages} 页")
    
    if total_pages > 1:
        next_page_cmd = f"药材背包详情 {current_page + 1}"
        final_msg.append(f"输入 {next_page_cmd} 查看下一页")
    page = ["翻页", f"药材背包详情 {current_page + 1}", "使用", "使用", "查看", "查看效果", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '药材背包详情', bot.self_id, final_msg, title=title_display, page=page)
    await yaocai_detail_back.finish()

@check_user_back.handle(parameterless=[Cooldown(cd_time=0)])
async def check_user_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """背包检测 - 管理员命令"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    # 检查权限
    if not await SUPERUSER(bot, event):
        msg = "此功能仅限管理员使用！"
        await handle_send(bot, event, msg)
        await check_user_back.finish()

    msg = "开始检测用户背包物品数量、名称和已装备状态，请稍候..."
    await handle_send(bot, event, msg)
    
    quantity_name_result = sql_message.check_and_adjust_goods_quantity()

    all_users = sql_message.get_all_user_id()
    checked_users = 0
    fixed_equipment_count = 0
    equipment_problems = []

    for user_id in all_users or []:
        checked_users += 1
        user_info = sql_message.get_user_info_with_id(user_id)
        if not user_info:
            continue

        user_buff_info = UserBuffDate(user_id).BuffInfo
        equipped_items = []

        if user_buff_info.get('faqi_buff', 0) != 0:
            equipped_items.append({'type': '法器', 'id': user_buff_info['faqi_buff']})
        if user_buff_info.get('armor_buff', 0) != 0:
            equipped_items.append({'type': '防具', 'id': user_buff_info['armor_buff']})

        for equipped_item in equipped_items:
            item_id = equipped_item['id']
            item_type = equipped_item['type']
            item_info = items.get_data_by_item_id(item_id)
            if not item_info:
                equipment_problems.append(
                    f"{user_info['user_name']} 已装备{item_type}ID {item_id} 已不存在，未自动修复"
                )
                continue

            item_data = sql_message.get_item_by_good_id_and_user_id(user_id, item_id)
            now_time = datetime.now()

            if not item_data or item_data['goods_num'] <= 0:
                current_goods_num = item_data['goods_num'] if item_data else 0
                fix_quantity = max(1, 1 - current_goods_num)
                sql_message.send_back(
                    user_id,
                    item_id,
                    item_info['name'],
                    "装备",
                    fix_quantity,
                    1
                )
                sql_message.update_back_equipment(
                    "UPDATE back SET state=1, goods_name=%s, update_time=%s, action_time=%s WHERE user_id=%s AND goods_id=%s",
                    (item_info['name'], now_time, now_time, user_id, item_id)
                )
                equipment_problems.append(
                    f"{user_info['user_name']} 的{item_info['name']}：已装备{item_type}但背包数量异常({current_goods_num})，已补足并设为已装备"
                )
                fixed_equipment_count += 1
                continue

            fixes = []
            if item_data['state'] == 0:
                fixes.append("状态修正为已装备")
            if item_data.get('goods_name') != item_info['name']:
                fixes.append(f"名称修正为{item_info['name']}")

            if fixes:
                sql_message.update_back_equipment(
                    "UPDATE back SET state=1, goods_name=%s, update_time=%s, action_time=%s WHERE user_id=%s AND goods_id=%s",
                    (item_info['name'], now_time, now_time, user_id, item_id)
                )
                equipment_problems.append(
                    f"{user_info['user_name']} 的{item_info['name']}：{'; '.join(fixes)}"
                )
                fixed_equipment_count += 1

    result_msg = [
        "【背包检测完成】",
        f"背包数量/名称检测结果：\n{quantity_name_result}",
        f"已检查用户数：{checked_users}",
        f"已装备物品修复数：{fixed_equipment_count}",
    ]

    if equipment_problems:
        result_msg.append("【已装备修复详情】")
        result_msg.extend(equipment_problems[:10])
        if len(equipment_problems) > 10:
            result_msg.append(f"...等共{len(equipment_problems)}个已装备问题")

    result_msg.append("\n备注：背包名称按 goods_id 读取当前物品配置，名称一致则跳过，不一致则修正。")
    await send_msg_handler(bot, event, '背包检测', bot.self_id, result_msg)
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
