import asyncio
import random
import time
import re
import os
import json
from pathlib import Path
from datetime import datetime
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
    get_use_equipment_sql, get_shop_data, save_shop,
    get_item_msg, get_item_msg_rank, check_use_elixir,
    get_use_jlq_msg, get_no_use_equipment_sql,
    get_user_equipment_msg
)
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import (
    check_user, get_msg_pic, 
    send_msg_handler, CommandObjectID,
    Txt2Img, number_to, handle_send, handle_pagination
)
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, get_weapon_info_msg, get_armor_info_msg,
    get_sec_msg, get_main_info_msg, get_sub_info_msg, UserBuffDate
)
from ..xiuxian_config import XiuConfig, convert_rank
from datetime import datetime, timedelta
from .auction_config import *
from nonebot import require

# 初始化组件
items = Items()
sql_message = XiuxianDateManage()
scheduler = require("nonebot_plugin_apscheduler").scheduler
reset_day_num_scheduler = require("nonebot_plugin_apscheduler").scheduler
clear_expired_baitan = require("nonebot_plugin_apscheduler").scheduler
rebuild_guishi_index = require("nonebot_plugin_apscheduler").scheduler

# === 通用配置 ===
# 数据文件路径
DATA_PATH = Path(__file__).parent
XIANSHI_DATA_PATH = DATA_PATH / "xianshi_data"
FANGSHI_DATA_PATH = DATA_PATH / "fangshi_data"
GUISHI_DATA_PATH = DATA_PATH / "guishi_data"

# 创建目录
for path in [XIANSHI_DATA_PATH, FANGSHI_DATA_PATH, GUISHI_DATA_PATH]:
    path.mkdir(parents=True, exist_ok=True)

# 通用物品类型
BANNED_ITEM_IDS = ["15357", "9935", "9940"]  # 禁止交易的物品ID
ITEM_TYPES = ["药材", "装备", "丹药", "技能"]
MIN_PRICE = 600000
MAX_QUANTITY = 10
GUISHI_TYPES = ["药材", "装备", "技能"]
GUISHI_BAITAN_START_HOUR = 18  # 18点开始
GUISHI_BAITAN_END_HOUR = 8     # 次日8点结束
GUISHI_MAX_QUANTITY = 100   # 单次最大交易数量
MAX_QIUGOU_ORDERS = 10  # 最大求购订单数
MAX_BAITAN_ORDERS = 10  # 最大摆摊订单数

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

# === 仙肆系统 ===

# 仙肆命令
xiuxian_shop_view = on_command("仙肆查看", priority=5, block=True)
xian_shop_add = on_command("仙肆上架", priority=5, block=True)
xianshi_auto_add = on_command("仙肆自动上架", priority=5, block=True)
xianshi_fast_add = on_command("仙肆快速上架", priority=5, block=True)
xianshi_fast_buy = on_command("仙肆快速购买", priority=5, block=True)
xian_shop_remove = on_command("仙肆下架", priority=5, block=True)
xian_buy = on_command("仙肆购买", priority=5, block=True)
my_xian_shop = on_command("我的仙肆", priority=5, block=True)
xian_shop_added_by_admin = on_command("系统仙肆上架", priority=5, permission=SUPERUSER, block=True)
xian_shop_remove_by_admin = on_command("系统仙肆下架", priority=5, permission=SUPERUSER, block=True)
xian_shop_off_all = on_fullmatch("清空仙肆", priority=3, permission=SUPERUSER, block=True)

# === 坊市系统 ===
# 坊市命令
shop_view = on_command("坊市查看", priority=5, permission=GROUP, block=True)
shop_added = on_command("坊市上架", priority=5, permission=GROUP, block=True)
fangshi_auto_add = on_command("坊市自动上架", priority=5, permission=GROUP, block=True)
fangshi_fast_add = on_command("坊市快速上架", priority=5, permission=GROUP, block=True)
fangshi_fast_buy = on_command("坊市快速购买", priority=5, permission=GROUP, block=True)
shop_remove = on_command("坊市下架", priority=5, permission=GROUP, block=True)
buy = on_command("坊市购买", priority=5, permission=GROUP, block=True)
my_shop = on_command("我的坊市", priority=5, permission=GROUP, block=True)
shop_added_by_admin = on_command("系统坊市上架", priority=5, permission=SUPERUSER, block=True)
shop_remove_by_admin = on_command("系统坊市下架", priority=5, permission=SUPERUSER, block=True)
shop_off_all = on_fullmatch("清空坊市", priority=3, permission=SUPERUSER, block=True)

# === 鬼市系统 ===
# 鬼市命令
guishi_deposit = on_command("鬼市存灵石", priority=5, block=True)
guishi_withdraw = on_command("鬼市取灵石", priority=5, block=True)
guishi_take_item = on_command("鬼市取物品", priority=5, block=True)
guishi_info = on_command("鬼市信息", priority=5, block=True)
guishi_qiugou = on_command("鬼市求购", priority=5, block=True)
guishi_cancel_qiugou = on_command("鬼市取消求购", priority=5, block=True)
guishi_baitan = on_command("鬼市摆摊", priority=5, block=True)
guishi_shoutan = on_command("鬼市收摊", priority=5, block=True)
clear_all_guishi = on_fullmatch("清空鬼市", priority=3, permission=SUPERUSER, block=True)

# === 其他原有命令 ===
chakan_wupin = on_command("查看修仙界物品", aliases={"查看"}, priority=20, block=True)
check_item_effect = on_command("查看效果", aliases={"查", "效果"}, priority=25, block=True)
goods_re_root = on_command("炼金", priority=6, block=True)
fast_alchemy = on_command("快速炼金", aliases={"一键炼金"}, priority=6, block=True)
main_back = on_command('我的背包', aliases={'我的物品'}, priority=10, block=True)
yaocai_back = on_command('药材背包', priority=10, block=True)
yaocai_detail_back = on_command('药材背包详细', aliases={'药材背包详情'}, priority=10, block=True)
danyao_back = on_command('丹药背包', priority=10, block=True)
my_equipment = on_command("我的装备", priority=10, block=True)
use = on_command("使用", priority=15, block=True)
no_use_zb = on_command("换装", aliases={'卸装'}, priority=5, block=True)
back_help = on_command("交易帮助", aliases={"背包帮助", "仙肆帮助", "坊市帮助", "鬼市帮助", "拍卖帮助"}, priority=8, block=True)
xiuxian_sone = on_fullmatch("灵石", priority=4, block=True)

# 重置丹药每日使用次数
@reset_day_num_scheduler.scheduled_job("cron", hour=0, minute=0, )
async def reset_day_num_scheduler_():
    sql_message.day_num_reset()
    logger.opt(colors=True).info(f"<green>每日丹药使用次数重置成功！</green>")

@check_item_effect.handle(parameterless=[Cooldown(at_sender=False)])
async def check_item_effect_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看物品效果，支持物品名或ID"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    # 检查用户是否已注册修仙
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await check_item_effect.finish()

    # 获取用户输入的物品名或ID
    input_str = args.extract_plain_text().strip()
    if not input_str:
        msg = "请输入物品名称或ID！\n例如：查看效果 渡厄丹 或 查看效果 1999"
        await handle_send(bot, event, msg)
        await check_item_effect.finish()

    # 判断输入是ID还是名称
    goods_id = None
    if input_str.isdigit():  # 如果是纯数字，视为ID
        goods_id = int(input_str)
        item_info = items.get_data_by_item_id(goods_id)
        if not item_info:
            msg = f"ID {goods_id} 对应的物品不存在，请检查输入！"
            await handle_send(bot, event, msg)
            await check_item_effect.finish()
    else:  # 视为物品名称
        for k, v in items.items.items():
            if input_str == v['name']:
                goods_id = k
                break
        if goods_id is None:
            msg = f"物品 {input_str} 不存在，请检查名称是否正确！"
            await handle_send(bot, event, msg)
            await check_item_effect.finish()
    item_msg = get_item_msg(goods_id)
    if goods_id == 15053 or input_str == "补偿":
        await check_item_effect.finish()
    # 构造返回消息
    msg = f"\nID：{goods_id}\n{item_msg}"
    await handle_send(bot, event, msg)
    await check_item_effect.finish()
    
@back_help.handle(parameterless=[Cooldown(at_sender=False)])
async def back_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """交易系统帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    message = str(event.message)
    
    # 提取中文关键词
    rank_msg = r'[\u4e00-\u9fa5]+'
    message = re.findall(rank_msg, message)
    
    # 帮助内容分块
    help_sections = {
        "背包": """
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
""".strip(),
        "仙肆": """
【仙肆帮助】（全服交易）
🔸 仙肆查看 [类型] [页码] - 查看全服仙肆
  ▶ 支持类型：技能|装备|丹药|药材
🔸 仙肆上架 物品 金额 [数量] - 上架物品
  ▶ 最低金额60万灵石，手续费10-30%
🔸 仙肆快速上架 物品 [金额] - 快速上架10个物品
  ▶ 自动匹配最低价，数量固定10个（或全部）
🔸 仙肆快速购买 物品 - 快速购买物品
  ▶ 自动匹配最低价，可快速购买5种物品
🔸 仙肆自动上架 类型 品阶 [数量] - 批量上架
  ▶ 示例：仙肆自动上架 装备 通天
🔸 仙肆购买 编号 [数量] - 购买物品
🔸 仙肆下架 编号 - 下架自己的物品
🔸 我的仙肆 [页码] - 查看自己上架的物品
""".strip(),
        "坊市": """
【坊市帮助】（群内交易）
🔸 坊市查看 [类型] [页码] - 查看群坊市
  ▶ 支持类型：技能|装备|丹药|药材
🔸 坊市上架 物品 金额 [数量] - 上架物品
  ▶ 最低金额60万灵石，手续费10-30%
🔸 坊市快速上架 物品 [金额] - 快速上架10个物品
  ▶ 自动匹配最低价，数量固定10个（或全部）
🔸 坊市快速购买 物品 - 快速购买物品
  ▶ 自动匹配最低价，可快速购买5种物品
🔸 坊市自动上架 类型 品阶 [数量] - 批量上架
  ▶ 示例：坊市自动上架 药材 五品
🔸 坊市购买 编号 [数量] - 购买物品
🔸 坊市下架 编号 - 下架自己的物品
🔸 我的坊市 [页码] - 查看自己上架的物品
""".strip(),
        "鬼市": """
【鬼市帮助】（匿名交易）
👻 鬼市存灵石 数量 - 存入灵石到鬼市账户
👻 鬼市取灵石 数量 - 取出灵石（收取20%暂存费）
👻 鬼市信息 - 查看鬼市账户和交易信息
👻 鬼市求购 物品 价格 [数量] - 发布求购订单
👻 鬼市取消求购 订单ID - 取消求购订单
👻 鬼市摆摊 物品 价格 [数量] - 摆摊出售物品
👻 鬼市收摊 摊位ID - 收摊并结算
""".strip(),
        "拍卖": f"""
【拍卖帮助】🎫
🔹 拍卖查看 [ID] - 查看拍卖品
  ▶ 无参数：查看当前拍卖列表
  ▶ 加ID：查看指定拍卖品详情

🔹 拍卖竞拍 ID 价格 - 参与竞拍
  ▶ 每次加价不得少于100万灵石
  ▶ 示例：拍卖竞拍 123456 5000000

🔹 拍卖上架 物品名 底价 - 提交拍卖品
  ▶ 最低底价：100万灵石
  ▶ 每人最多上架3件

🔹 拍卖下架 物品名 - 撤回拍卖品
  ▶ 仅在非拍卖期间可操作

🔹 我的拍卖 - 查看已上架物品
  
🔹 拍卖信息 - 查看拍卖状态
  ▶ 包含开启时间、当前状态等信息

⏰ 自动拍卖时间：每日17点
⏳ 持续时间：5小时
💼 手续费：20%
""".strip(),
        "交易": """
【交易系统总览】
输入以下关键词查看详细帮助：
🔹 背包帮助 - 背包相关功能
🔹 仙肆帮助 - 全服交易市场
🔹 坊市帮助 - 群内交易市场
🔹 拍卖帮助 - 拍卖行功能

【系统规则】
💰 手续费规则：
  - 500万以下：10%
  - 500-1000万：15% 
  - 1000-2000万：20%
  - 2000万以上：30%
""".strip()
    }
    
    # 默认显示交易总览
    if not message:
        msg = help_sections["交易"]
    else:
        # 获取第一个中文关键词
        keyword = message[0]
        
        # 检查是否包含特定关键词
        if "背包" in keyword:
            msg = help_sections["背包"]
        elif "仙肆" in keyword:
            msg = help_sections["仙肆"]
        elif "坊市" in keyword:
            msg = help_sections["坊市"]
        elif "鬼市" in keyword:
            msg = help_sections["鬼市"]
        elif "拍卖" in keyword or "拍卖会" in keyword:
            msg = help_sections["拍卖"]
        elif "全部" in keyword:
            msg = (
                help_sections["背包"] + "\n\n" + 
                help_sections["仙肆"] + "\n\n" + 
                help_sections["坊市"] + "\n\n" + 
                help_sections["鬼市"] + "\n\n" + 
                help_sections["拍卖"]
            )
        elif "交易" in keyword:
            msg = help_sections["交易"]
        else:
            # 默认显示交易总览和可用指令
            msg = "请输入正确的帮助关键词：\n"
            msg += "背包帮助 | 仙肆帮助 | 坊市帮助 | 拍卖帮助 | 交易帮助\n"
            msg += "或输入'交易帮助全部'查看完整帮助"
    
    await handle_send(bot, event, f"\n{msg}")
    await back_help.finish()

@xiuxian_sone.handle(parameterless=[Cooldown(at_sender=False)])
async def xiuxian_sone_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的灵石信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await xiuxian_sone.finish()
    msg = f"当前灵石：{user_info['stone']}({number_to(user_info['stone'])})"
    await handle_send(bot, event, msg)
    await xiuxian_sone.finish()

def get_xianshi_index():
    """获取仙肆索引数据"""
    index_file = XIANSHI_DATA_PATH / "仙肆索引.json"
    try:
        if index_file.exists():
            with open(index_file, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"读取仙肆索引失败: {e}")
    return {"next_id": 1, "items": {}}

def save_xianshi_index(data):
    """保存仙肆索引"""
    index_file = XIANSHI_DATA_PATH / "仙肆索引.json"
    try:
        with open(index_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        logger.error(f"保存仙肆索引失败: {e}")
        return False

def get_xianshi_type_data(item_type):
    """获取指定类型的仙肆数据"""
    if item_type not in ITEM_TYPES:
        return None
    
    type_file = XIANSHI_DATA_PATH / f"仙肆_{item_type}.json"
    try:
        if type_file.exists():
            with open(type_file, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"读取仙肆{item_type}数据失败: {e}")
    return {}

def save_xianshi_type_data(item_type, data):
    """保存指定类型的仙肆数据"""
    if item_type not in ITEM_TYPES:
        return False
    
    type_file = XIANSHI_DATA_PATH / f"仙肆_{item_type}.json"
    try:
        with open(type_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        logger.error(f"保存仙肆{item_type}数据失败: {e}")
        return False

def get_item_type_by_id(goods_id):
    """根据物品ID获取类型"""
    # 这里需要接入您的物品系统
    # 示例: return items.get_data_by_item_id(goods_id)['type']
    return items.get_data_by_item_id(goods_id)['type']

def generate_unique_id(existing_ids):
    """生成6-10位随机不重复ID"""
    while True:
        # 使用时间戳+随机数确保唯一性
        timestamp_part = int(time.time() % 10000)
        random_part = random.randint(100, 99999)
        new_id = int(f"{timestamp_part}{random_part}") % 10**10  # 确保不超过10位
        
        # 限制在6-10位
        new_id = max(100000, min(new_id, 9999999999))
        
        # 检查是否已存在
        if str(new_id) not in existing_ids:
            return str(new_id)

def generate_fangshi_id(existing_ids):
    """生成6-10位随机不重复坊市ID"""
    while True:
        # 组合时间戳和随机数确保唯一性
        timestamp_part = int(time.time() % 10000)  # 取时间戳后4位
        random_part = random.randint(100, 99999)   # 5位随机数
        new_id = int(f"{timestamp_part}{random_part}") % 10**10  # 确保不超过10位
        
        # 限制在6-10位范围
        new_id = max(100000, min(new_id, 9999999999))
        
        # 检查是否已存在
        if str(new_id) not in existing_ids:
            return str(new_id)

def get_fangshi_index(group_id):
    """获取坊市索引数据"""
    index_file = FANGSHI_DATA_PATH / f"坊市索引_{group_id}.json"
    try:
        if index_file.exists():
            with open(index_file, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"读取坊市索引失败: {e}")
    return {"next_id": 1, "items": {}}

def save_fangshi_index(group_id, data):
    """保存坊市索引"""
    index_file = FANGSHI_DATA_PATH / f"坊市索引_{group_id}.json"
    try:
        with open(index_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        logger.error(f"保存坊市索引失败: {e}")
        return False

def get_fangshi_type_data(group_id, item_type):
    """获取指定类型的坊市数据"""
    if item_type not in ITEM_TYPES:
        return None
    
    type_file = FANGSHI_DATA_PATH / f"坊市_{group_id}_{item_type}.json"
    try:
        if type_file.exists():
            with open(type_file, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"读取坊市{item_type}数据失败: {e}")
    return {}

def save_fangshi_type_data(group_id, item_type, data):
    """保存指定类型的坊市数据"""
    if item_type not in ITEM_TYPES:
        return False
    
    type_file = FANGSHI_DATA_PATH / f"坊市_{group_id}_{item_type}.json"
    try:
        with open(type_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        logger.error(f"保存坊市{item_type}数据失败: {e}")
        return False

def get_xianshi_min_price(goods_name):
    """获取仙肆中该物品的最低价格"""
    min_price = None
    
    # 遍历所有类型
    for item_type in ["药材", "装备", "丹药", "技能"]:
        type_file = XIANSHI_DATA_PATH / f"仙肆_{item_type}.json"
        if not type_file.exists():
            continue
        
        with open(type_file, "r", encoding="utf-8") as f:
            type_items = json.load(f)
        
        for item in type_items.values():
            if item["name"] == goods_name:
                if min_price is None or item["price"] < min_price:
                    min_price = item["price"]
    
    return min_price

def get_fangshi_min_price(group_id, goods_name):
    """获取坊市中该物品的最低价格，如果不存在则获取仙肆最低价"""
    min_price = None
    
    # 1. 先在坊市查找最低价
    for item_type in ["药材", "装备", "丹药", "技能"]:
        type_file = FANGSHI_DATA_PATH / f"坊市_{group_id}_{item_type}.json"
        if not type_file.exists():
            continue
        
        with open(type_file, "r", encoding="utf-8") as f:
            type_items = json.load(f)
        
        for item in type_items.values():
            if item["name"] == goods_name:
                if min_price is None or item["price"] < min_price:
                    min_price = item["price"]
    
    # 2. 如果坊市没有，再查找仙肆最低价
    if min_price is None:
        min_price = get_xianshi_min_price(goods_name)
    
    return min_price
        
@xian_shop_add.handle(parameterless=[Cooldown(1.4, at_sender=False)])
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
    
    # 检查背包是否有该物品（需要接入您的背包系统）
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
    
    # 检查可上架数量
    available_num = goods_info['goods_num'] - goods_info['bind_num']
    if quantity > available_num:
        msg = f"可上架数量不足！背包有{goods_info['goods_num']}个（{goods_info['bind_num']}个绑定），最多可上架{available_num}个"
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

    # 计算手续费
    total_price = price * quantity
    if total_price <= 5000000:
        fee_rate = 0.1
    elif total_price <= 10000000:
        fee_rate = 0.15
    elif total_price <= 20000000:
        fee_rate = 0.2
    else:
        fee_rate = 0.3
    
    # 计算手续费（按单个物品计算）
    single_fee = int(price * fee_rate)
    total_fee = single_fee * quantity
    
    if user_info['stone'] < total_fee:
        msg = f"灵石不足支付手续费！需要{total_fee}灵石，当前拥有{user_info['stone']}灵石"
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    # 扣除总手续费
    sql_message.update_ls(user_id, total_fee, 2)
    
    # 为每个物品创建独立条目
    success_count = 0
    for _ in range(quantity):
        # 生成唯一ID
        index_data = get_xianshi_index()
        existing_ids = set(index_data["items"].keys())
        xianshi_id = generate_unique_id(existing_ids)
        
        # 添加到索引
        index_data["items"][xianshi_id] = {
            "type": goods_type,
            "user_id": user_id
        }
        save_xianshi_index(index_data)
        
        # 添加到类型文件，数量固定为1
        type_items = get_xianshi_type_data(goods_type)
        type_items[xianshi_id] = {
            "id": xianshi_id,
            "goods_id": goods_info['goods_id'],
            "name": goods_name,
            "type": goods_type,
            "price": price,
            "quantity": 1,  # 固定为1
            "user_id": user_id,
            "user_name": user_info['user_name'],
            "desc": get_item_msg(goods_info['goods_id'])
        }
        save_xianshi_type_data(goods_type, type_items)
        
        # 从背包扣除1个物品
        sql_message.update_back_j(user_id, goods_info['goods_id'], num=1)
        success_count += 1
    
    msg = f"\n成功上架 {goods_name} x{success_count} 到仙肆！\n"
    msg += f"单价: {number_to(price)} 灵石\n"
    msg += f"总手续费: {number_to(total_fee)} 灵石"
    
    await handle_send(bot, event, msg)
    await xian_shop_add.finish()

@xian_shop_remove.handle(parameterless=[Cooldown(1.4, at_sender=False)])
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
    
    # 获取所有类型中该用户上架的该物品
    user_items = []
    index_data = get_xianshi_index()
    
    for xianshi_id, item_info in index_data["items"].items():
        if str(item_info["user_id"]) == str(user_id):
            type_items = get_xianshi_type_data(item_info["type"])
            if xianshi_id in type_items and type_items[xianshi_id]["name"] == goods_name:
                user_items.append({
                    "id": xianshi_id,
                    "price": type_items[xianshi_id]["price"],
                    "type": item_info["type"]
                })
    
    if not user_items:
        msg = f"您在仙肆中没有上架 {goods_name}！"
        await handle_send(bot, event, msg)
        await xian_shop_remove.finish()
    
    # 按价格从低到高排序
    user_items.sort(key=lambda x: x["price"])
    
    # 确定要下架的数量
    if quantity is None:
        # 没指定数量则下架最低价的1个
        items_to_remove = [user_items[0]]
    else:
        # 指定数量则下架价格从低到高的指定数量
        items_to_remove = user_items[:quantity]
    
    # 执行下架操作
    removed_count = 0
    for item in items_to_remove:
        # 从类型文件中移除
        type_items = get_xianshi_type_data(item["type"])
        if item["id"] in type_items:
            # 退回物品到背包
            item_data = type_items[item["id"]]
            sql_message.send_back(
                user_id,
                item_data["goods_id"],
                goods_name,
                item["type"],
                1  # 每个条目数量固定为1
            )
            
            # 从系统中移除
            del type_items[item["id"]]
            save_xianshi_type_data(item["type"], type_items)
            
            # 从索引中移除
            del index_data["items"][item["id"]]
            save_xianshi_index(index_data)
            
            removed_count += 1
    
    msg = f"成功下架 {goods_name} x{removed_count}！已退回背包"
    if len(user_items) > removed_count:
        msg += f"\n(仙肆中仍有 {len(user_items)-removed_count} 个 {goods_name})"
    
    await handle_send(bot, event, msg)
    await xian_shop_remove.finish()

@xiuxian_shop_view.handle(parameterless=[Cooldown(at_sender=False)])
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
        msg = "请指定查看类型：【药材、装备、丹药、技能】"
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
        if parts[0] in ITEM_TYPES:
            item_type = parts[0]
            if len(parts) > 1 and parts[1].isdigit():
                current_page = int(parts[1])
    
    # 检查类型有效性
    if item_type not in ITEM_TYPES:
        msg = f"无效类型！可用类型：【{', '.join(ITEM_TYPES)}】"
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()
    
    # 读取对应类型的物品数据
    type_items = get_xianshi_type_data(item_type)
    if not type_items:
        msg = f"仙肆中暂无{item_type}类物品！"
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()
    
    # 处理物品显示逻辑
    system_items = []  # 存储系统物品
    user_items = {}    # 存储用户物品（按名称分组，只保留最低价）
    
    for item_id, item in type_items.items():
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
        if isinstance(item['quantity'], str) and item['quantity'] == "无限":
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

@my_xian_shop.handle(parameterless=[Cooldown(at_sender=False)])
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
    
    # 从索引获取用户所有物品
    index_data = get_xianshi_index()
    user_items = []
    
    for xianshi_id, item_info in index_data["items"].items():
        if str(item_info["user_id"]) == str(user_id):
            # 从对应类型文件读取详细信息
            type_items = get_xianshi_type_data(item_info["type"])
            if xianshi_id in type_items:
                user_items.append(type_items[xianshi_id])
    
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

@xian_buy.handle(parameterless=[Cooldown(1.4, at_sender=False)])
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
    
    # 从索引查找物品
    index_data = get_xianshi_index()
    if xianshi_id not in index_data["items"]:
        msg = f"未找到仙肆ID为 {xianshi_id} 的物品！"
        await handle_send(bot, event, msg)
        await xian_buy.finish()
    
    item_info = index_data["items"][xianshi_id]
    
    # 检查是否是自己的物品
    if str(item_info["user_id"]) == str(user_id):
        msg = "不能购买自己上架的物品！"
        await handle_send(bot, event, msg)
        await xian_buy.finish()
    
    # 从类型文件获取详细信息
    type_items = get_xianshi_type_data(item_info["type"])
    if xianshi_id not in type_items:
        msg = "物品数据异常，请联系管理员！"
        await handle_send(bot, event, msg)
        await xian_buy.finish()
    
    item = type_items[xianshi_id]
    seller_name = "系统" if item["user_id"] == 0 else item["user_name"]
    
    # 检查库存（系统无限物品跳过检查）
    if not (isinstance(item["quantity"], str) and item["quantity"] == "无限"):
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
        if item_info["user_id"] != 0:
            seller_id = item_info["user_id"]
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
        
        # 更新库存（系统无限物品不更新）
        if not (isinstance(item["quantity"], str) and item["quantity"] == "无限"):
            item["quantity"] -= quantity
            if item["quantity"] <= 0:
                # 从系统中移除
                del index_data["items"][xianshi_id]
                del type_items[xianshi_id]
            else:
                type_items[xianshi_id] = item
            
            # 保存更改
            save_xianshi_index(index_data)
            save_xianshi_type_data(item_info["type"], type_items)
        
        msg = f"成功购买 {item['name']} x{quantity}（来自{seller_name}）！花费 {number_to(total_price)} 灵石"
        await handle_send(bot, event, msg)
    except Exception as e:
        logger.error(f"仙肆购买出错: {e}")
        msg = "购买过程中出现错误，请稍后再试！"
        await handle_send(bot, event, msg)
    
    await xian_buy.finish()

@xian_shop_added_by_admin.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def xian_shop_added_by_admin_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """系统仙肆上架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "请输入正确指令！格式：系统仙肆上架 物品名称 [价格] [数量]"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    
    goods_name = args[0]
    try:
        price = int(args[1]) if len(args) > 1 else MIN_PRICE
        price = max(price, MIN_PRICE)
        quantity = int(args[2]) if len(args) > 2 else 0  # 0表示无限
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    
    # 查找物品ID
    goods_id = None
    for k, v in items.items.items():
        if goods_name == v['name']:
            goods_id = k
            break
    
    if not goods_id:
        msg = f"未找到物品 {goods_name}！"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    
    # 获取物品类型
    goods_type = get_item_type_by_id(goods_id)
    if goods_type not in ITEM_TYPES:
        msg = f"该物品类型不允许上架！允许类型：{', '.join(ITEM_TYPES)}"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    
    # 添加到仙肆系统
    # 获取索引数据
    index_data = get_xianshi_index()
    xianshi_id = str(index_data["next_id"])  # 使用顺序ID
    
    # 更新下一个ID
    index_data["next_id"] += 1
    
    # 添加到索引
    index_data["items"][xianshi_id] = {
        "type": goods_type,
        "user_id": 0  # 0表示系统物品
    }
    save_xianshi_index(index_data)
    
    # 添加到类型文件
    type_items = get_xianshi_type_data(goods_type)
    type_items[xianshi_id] = {
        "id": xianshi_id,
        "goods_id": goods_id,
        "name": goods_name,
        "type": goods_type,
        "price": price,
        "quantity": "无限" if quantity == 0 else quantity,  # 0表示无限
        "user_id": 0,
        "user_name": "系统",
        "desc": get_item_msg(goods_id)
    }
    save_xianshi_type_data(goods_type, type_items)
    
    msg = f"系统成功上架 {goods_name} 到仙肆！\n"
    msg += f"价格: {number_to(price)} 灵石\n"
    msg += f"数量: {'无限' if quantity == 0 else quantity}\n"
    msg += f"仙肆ID: {xianshi_id}"
    
    await handle_send(bot, event, msg)
    await xian_shop_added_by_admin.finish()

@xianshi_auto_add.handle(parameterless=[Cooldown(1.4, at_sender=False)])
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
        msg = f"❌❌❌❌❌❌❌❌ 无效类型！可用类型：{', '.join(type_mapping.keys())}"
        await handle_send(bot, event, msg)
        await xianshi_auto_add.finish()
    
    if rank_name not in rank_map:
        msg = f"❌❌❌❌❌❌❌❌ 无效品阶！输入'品阶帮助'查看完整列表"
        await handle_send(bot, event, msg)
        await xianshi_auto_add.finish()

    # 获取背包物品
    back_msg = sql_message.get_back_msg(user_id)
    if not back_msg:
        msg = "💼💼 道友的背包空空如也！"
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
        msg = f"🔍🔍 背包中没有符合条件的【{item_type}·{rank_name}】物品"
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
        if total_price <= 5000000:
            fee_rate = 0.1
        elif total_price <= 10000000:
            fee_rate = 0.15
        elif total_price <= 20000000:
            fee_rate = 0.2
        else:
            fee_rate = 0.3
        
        single_fee = int(total_price * fee_rate)
        
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
    
    # 获取当前索引数据
    index_data = get_xianshi_index()
    existing_ids = set(index_data["items"].keys())
    
    # 准备批量写入的数据
    type_updates = {}  # 按类型分组的数据更新
    result_msg = []
    success_count = 0

    for item in items_to_process:
        # 为每个物品创建独立条目
        for _ in range(item['quantity']):
            # 生成唯一ID
            xianshi_id = generate_unique_id(existing_ids)
            existing_ids.add(xianshi_id)
            
            # 添加到索引
            index_data["items"][xianshi_id] = {
                "type": item['type'],
                "user_id": user_id
            }
            
            # 添加到类型文件更新
            if item['type'] not in type_updates:
                type_updates[item['type']] = get_xianshi_type_data(item['type'])
            
            type_updates[item['type']][xianshi_id] = {
                "id": xianshi_id,
                "goods_id": item['id'],
                "name": item['name'],
                "type": item['type'],
                "price": item['price'],
                "quantity": 1,
                "user_id": user_id,
                "user_name": user_info['user_name'],
                "desc": get_item_msg(item['id'])
            }
            
            # 从背包扣除1个物品
            sql_message.update_back_j(user_id, item['id'], num=1)
            
            success_count += 1
            result_msg.append(f"{item['name']} x1 - 单价:{number_to(item['price'])}")
    
    # 批量保存所有更新
    save_xianshi_index(index_data)
    for item_type, type_items in type_updates.items():
        save_xianshi_type_data(item_type, type_items)
    
    # 构建结果消息
    msg = [
        f"\n✨ 成功上架 {success_count} 件物品",
        *result_msg[:10],  # 最多显示10条
        f"💎💎 总手续费: {number_to(total_fee)}灵石"
    ]
    
    if len(result_msg) > 10:
        msg.append(f"...等共{len(result_msg)}件物品")
    
    await send_msg_handler(bot, event, '仙肆自动上架', bot.self_id, msg)
    await xianshi_auto_add.finish()

@xianshi_fast_add.handle(parameterless=[Cooldown(1.4, at_sender=False)])
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
    
    # 检查可上架数量（固定为10或背包中全部数量）
    available_num = goods_info['goods_num'] - goods_info['bind_num']
    quantity = min(10, available_num)  # 最多10个
    
    if quantity <= 0:
        msg = f"可上架数量不足！背包有{goods_info['goods_num']}个（{goods_info['bind_num']}个绑定），没有可上架数量"
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
    if total_price <= 5000000:
        fee_rate = 0.1
    elif total_price <= 10000000:
        fee_rate = 0.15
    elif total_price <= 20000000:
        fee_rate = 0.2
    else:
        fee_rate = 0.3
    
    fee = int(total_price * fee_rate)
    
    if user_info['stone'] < fee:
        msg = f"灵石不足支付手续费！需要{fee}灵石，当前拥有{user_info['stone']}灵石"
        await handle_send(bot, event, msg)
        await xianshi_fast_add.finish()
    
    # 扣除总手续费
    sql_message.update_ls(user_id, fee, 2)
    
    # 为每个物品创建独立条目
    for _ in range(quantity):
        # 从背包扣除1个物品
        sql_message.update_back_j(user_id, goods_info['goods_id'], num=1)
        
        # 添加到仙肆系统
        index_data = get_xianshi_index()
        existing_ids = set(index_data["items"].keys())
        xianshi_id = generate_unique_id(existing_ids)
        
        # 添加到索引
        index_data["items"][xianshi_id] = {
            "type": goods_type,
            "user_id": user_id
        }
        save_xianshi_index(index_data)
        
        # 添加到类型文件
        type_items = get_xianshi_type_data(goods_type)
        type_items[xianshi_id] = {
            "id": xianshi_id,
            "goods_id": goods_info['goods_id'],
            "name": goods_name,
            "type": goods_type,
            "price": price,
            "quantity": 1,  # 每个条目数量固定为1
            "user_id": user_id,
            "user_name": user_info['user_name'],
            "desc": get_item_msg(goods_info['goods_id'])
        }
        save_xianshi_type_data(goods_type, type_items)
    
    msg = f"\n成功上架 {goods_name} x{quantity} 到仙肆！\n"
    msg += f"单价: {number_to(price)} 灵石\n"
    msg += f"总价: {number_to(total_price)} 灵石\n"
    msg += f"手续费: {number_to(fee)} 灵石"
    
    await handle_send(bot, event, msg)
    await xianshi_fast_add.finish()

@xianshi_fast_buy.handle(parameterless=[Cooldown(1.4, at_sender=False)])
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
        msg = "指令格式：仙肆快速购买 物品名1,物品名2 [数量]\n" \
              "▶ 物品名：支持1-5个物品（可重复），用逗号分隔\n" \
              "▶ 示例：仙肆快速购买 两仪心经,两仪心经,两仪心经"
        await handle_send(bot, event, msg)
        await xianshi_fast_buy.finish()
    
    # 解析物品名列表（允许重复且保留顺序）
    goods_names = args[0].split(",")
    if len(goods_names) > 5:
        msg = "一次最多指定5个物品名（可重复）！"
        await handle_send(bot, event, msg)
        await xianshi_fast_buy.finish()
    
    quantity_per_item = 1
    
    # 获取所有用户物品（不包括系统物品）
    index_data = get_xianshi_index()
    user_items = []
    
    for xianshi_id, item_info in index_data["items"].items():
        if item_info["user_id"] != 0:  # 排除系统物品
            type_items = get_xianshi_type_data(item_info["type"])
            if xianshi_id in type_items:
                item_data = type_items[xianshi_id]
                if item_data["name"] in goods_names:
                    user_items.append({
                        "id": xianshi_id,
                        "goods_id": item_data["goods_id"],
                        "name": item_data["name"],
                        "type": item_info["type"],
                        "price": item_data["price"],
                        "seller_id": item_info["user_id"],
                        "seller_name": item_data["user_name"]
                    })
    
    if not user_items:
        msg = "仙肆中没有符合条件的用户物品！"
        await handle_send(bot, event, msg)
        await xianshi_fast_buy.finish()
    
    # 按价格从低到高排序
    user_items.sort(key=lambda x: x["price"])
    
    # 执行购买（严格按照输入顺序处理每个物品名）
    total_cost = 0
    success_items = []
    failed_items = []
    
    for name in goods_names:
        # 查找该物品所有可购买项（按价格排序）
        available = [item for item in user_items if item["name"] == name]
        remaining = quantity_per_item
        purchased = 0
        item_total = 0
        
        for item in available:
            if remaining <= 0:
                break
            
            try:
                # 检查物品是否已被购买（可能被前一轮购买）
                if item["id"] not in index_data["items"]:
                    continue
                
                # 执行购买
                sql_message.update_ls(user_id, item["price"], 2)  # 扣钱
                sql_message.update_ls(item["seller_id"], item["price"], 1)  # 给卖家
                sql_message.send_back(user_id, item["goods_id"], item["name"], item["type"], 1, 1)
                
                # 从系统中移除
                type_items = get_xianshi_type_data(item["type"])
                del index_data["items"][item["id"]]
                del type_items[item["id"]]
                save_xianshi_index(index_data)
                save_xianshi_type_data(item["type"], type_items)
                
                purchased += 1
                item_total += item["price"]
                total_cost += item["price"]
                remaining -= 1
                
            except Exception as e:
                logger.error(f"快速购买出错: {e}")
                continue
        
        # 记录结果（每个name单独记录）
        if purchased > 0:
            success_items.append(f"{name}×{purchased} ({number_to(item_total)}灵石)")
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
    
@xian_shop_remove_by_admin.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def xian_shop_remove_by_admin_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """系统仙肆下架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    arg = args.extract_plain_text().strip()
    
    if not arg:
        msg = "请输入要下架的仙肆ID或物品名称！"
        await handle_send(bot, event, msg)
        await xian_shop_remove_by_admin.finish()
    
    index_data = get_xianshi_index()
    removed_items = []
    
    if arg.isdigit():  # 按ID下架
        xianshi_id = arg
        if xianshi_id in index_data["items"]:
            item_info = index_data["items"][xianshi_id]
            type_items = get_xianshi_type_data(item_info["type"])
            if xianshi_id in type_items:
                item_data = type_items[xianshi_id]
                removed_items.append(item_data)
                
                # 如果是用户物品，退回
                if item_info["user_id"] != 0:
                    sql_message.send_back(
                        item_info["user_id"],
                        item_data["goods_id"],
                        item_data["name"],
                        item_info["type"],
                        item_data["quantity"]
                    )
                
                # 从系统中移除
                del index_data["items"][xianshi_id]
                del type_items[xianshi_id]
                save_xianshi_index(index_data)
                save_xianshi_type_data(item_info["type"], type_items)
        else:
            msg = f"未找到仙肆ID为 {xianshi_id} 的物品！"
            await handle_send(bot, event, msg)
            await xian_shop_remove_by_admin.finish()
    else:  # 按名称下架
        goods_name = arg
        # 解析数量
        parts = goods_name.split()
        quantity = None
        if len(parts) > 1 and parts[-1].isdigit():
            quantity = int(parts[-1])
            goods_name = " ".join(parts[:-1])
        
        # 查找所有匹配的用户物品（不包括系统物品）
        user_items = []
        for xianshi_id, item_info in index_data["items"].items():
            if item_info["user_id"] != 0:  # 排除系统物品
                type_items = get_xianshi_type_data(item_info["type"])
                if xianshi_id in type_items and type_items[xianshi_id]["name"] == goods_name:
                    user_items.append({
                        "id": xianshi_id,
                        "price": type_items[xianshi_id]["price"],
                        "type": item_info["type"],
                        "user_id": item_info["user_id"],
                        "item_data": type_items[xianshi_id]
                    })
        
        if not user_items:
            msg = f"仙肆中没有用户上架的 {goods_name} 物品！"
            await handle_send(bot, event, msg)
            await xian_shop_remove_by_admin.finish()
        
        # 按价格从低到高排序
        user_items.sort(key=lambda x: x["price"])
        
        # 确定要下架的数量
        if quantity is None:
            # 没指定数量则下架最低价的1个
            items_to_remove = [user_items[0]]
        else:
            # 指定数量则下架价格从低到高的指定数量
            items_to_remove = user_items[:quantity]
        
        # 执行下架操作
        for item in items_to_remove:
            # 从类型文件中移除
            type_items = get_xianshi_type_data(item["type"])
            if item["id"] in type_items:
                item_data = item["item_data"]
                removed_items.append(item_data)
                
                # 退回物品给用户
                sql_message.send_back(
                    item["user_id"],
                    item_data["goods_id"],
                    item_data["name"],
                    item["type"],
                    item_data["quantity"]
                )
                
                # 从系统中移除
                del index_data["items"][item["id"]]
                del type_items[item["id"]]
                save_xianshi_index(index_data)
                save_xianshi_type_data(item["type"], type_items)
    
    if removed_items:
        msg = "成功下架以下物品：\n"
        for item in removed_items:
            owner = "系统" if item["user_id"] == 0 else item["user_name"]
            msg += f"ID:{item['id']} {item['name']} x{item['quantity']} (已退回给:{owner})\n"
    else:
        msg = "没有物品被下架！"
    
    await handle_send(bot, event, msg)
    await xian_shop_remove_by_admin.finish()

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
    
    # 获取所有物品
    index_data = get_xianshi_index()
    removed_items = []
    
    for xianshi_id, item_info in index_data["items"].items():
        type_items = get_xianshi_type_data(item_info["type"])
        if xianshi_id in type_items:
            item = type_items[xianshi_id]
            removed_items.append(item)
            
            # 如果是用户物品，退回
            if item_info["user_id"] != 0:
                sql_message.send_back(
                    item_info["user_id"],
                    item["goods_id"],
                    item["name"],
                    item_info["type"],
                    item["quantity"]
                )
    
    # 清空所有数据
    for item_type in ITEM_TYPES:
        save_xianshi_type_data(item_type, {})
    
    save_xianshi_index({"next_id": 1, "items": {}})
    
    if removed_items:
        msg = "成功清空仙肆！共下架以下物品：\n"
        for item in removed_items[:10]:  # 最多显示10条
            owner = "系统" if item["user_id"] == 0 else item["user_name"]
            msg += f"ID:{item['id']} {item['name']} x{item['quantity']} (来自:{owner})\n"
        if len(removed_items) > 10:
            msg += f"...等共{len(removed_items)}件物品"
    else:
        msg = "仙肆已经是空的，没有物品被下架！"
    
    await handle_send(bot, event, msg)
    await xian_shop_off_all.finish()

@shop_added.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def shop_added_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """坊市上架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await shop_added.finish()
    
    group_id = str(event.group_id)
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 2:
        msg = "请输入正确指令！格式：坊市上架 物品名称 价格 [数量]"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    
    goods_name = args[0]
    try:
        price = int(args[1])
        quantity = int(args[2]) if len(args) > 2 else 1
        quantity = min(quantity, MAX_QUANTITY)
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    
    # 原有价格限制逻辑
    if price < MIN_PRICE:  # 最低60万灵石
        msg = "坊市最低价格为60万灵石！"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    
    # 检查仙肆最低价
    xianshi_min_price = get_xianshi_min_price(goods_name)
    if xianshi_min_price is not None:
        min_price = max(MIN_PRICE, xianshi_min_price // 2)
        max_price = xianshi_min_price * 2
        if price < min_price or price > max_price:
            msg = f"该物品在仙肆的最低价格为{xianshi_min_price}，坊市价格限制为{min_price}-{max_price}灵石！"
            await handle_send(bot, event, msg)
            await shop_added.finish()
    
    # 检查背包物品
    back_msg = sql_message.get_back_msg(user_id)
    goods_info = None
    for item in back_msg:
        if item['goods_name'] == goods_name:
            goods_info = item
            break
    
    if not goods_info:
        msg = f"请检查该道具 {goods_name} 是否在背包内！"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    
    # 检查可上架数量
    available_num = goods_info['goods_num'] - goods_info['bind_num']
    if quantity > available_num:
        msg = f"可上架数量不足！背包有{goods_info['goods_num']}个（{goods_info['bind_num']}个绑定），最多可上架{available_num}个"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    
    # 获取物品类型
    goods_type = get_item_type_by_id(goods_info['goods_id'])
    if goods_type not in ["药材", "装备", "丹药", "技能"]:
        msg = "只能上架药材、装备、丹药或技能类物品！"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    # 检查禁止交易的物品
    if str(goods_info['goods_id']) in BANNED_ITEM_IDS:
        msg = f"物品 {goods_name} 禁止在坊市交易！"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    # 计算总手续费
    total_price = price * quantity
    if total_price <= 6000000:
        fee_rate = 0.1
    elif total_price <= 10000000:
        fee_rate = 0.15
    elif total_price <= 20000000:
        fee_rate = 0.2
    else:
        fee_rate = 0.3
    
    fee = int(total_price * fee_rate)
    if user_info['stone'] < fee:
        msg = f"灵石不足支付手续费！需要{fee}灵石，当前拥有{user_info['stone']}灵石"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    
    # 扣除手续费和物品
    sql_message.update_ls(user_id, fee, 2)
    sql_message.update_back_j(user_id, goods_info['goods_id'], num=quantity)
    
    # 为每个物品创建独立条目
    success_count = 0
    for _ in range(quantity):
        # 添加到坊市系统
        # 生成唯一坊市ID
        index_data = get_fangshi_index(group_id)
        existing_ids = set(index_data["items"].keys())
        fangshi_id = generate_fangshi_id(existing_ids)
        
        # 添加到索引
        index_data["items"][fangshi_id] = {
            "type": goods_type,
            "user_id": user_id
        }
        save_fangshi_index(group_id, index_data)
        
        # 添加到类型文件
        type_items = get_fangshi_type_data(group_id, goods_type)
        type_items[fangshi_id] = {
            "id": fangshi_id,
            "goods_id": goods_info['goods_id'],
            "name": goods_name,
            "type": goods_type,
            "price": price,
            "quantity": 1,  # 每个条目数量固定为1
            "user_id": user_id,
            "user_name": user_info['user_name'],
            "desc": get_item_msg(goods_info['goods_id'])
        }
        save_fangshi_type_data(group_id, goods_type, type_items)
        success_count += 1
    
    msg = f"\n成功上架 {goods_name} x{success_count} 到坊市！\n"
    msg += f"单价: {number_to(price)} 灵石\n"
    msg += f"总价: {number_to(total_price)} 灵石\n"
    msg += f"手续费: {number_to(fee)} 灵石"
    
    await handle_send(bot, event, msg)
    await shop_added.finish()

@fangshi_auto_add.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def fangshi_auto_add_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """坊市自动上架（批量优化版）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await fangshi_auto_add.finish()
    
    group_id = str(event.group_id)
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    # 指令格式检查
    if len(args) < 2:
        msg = "指令格式：坊市自动上架 [类型] [品阶] [数量]\n" \
              "▶ 类型：装备|法器|防具|药材|技能|全部\n" \
              "▶ 品阶：全部|人阶|黄阶|...|上品通天法器（输入'品阶帮助'查看完整列表）\n" \
              "▶ 数量：可选，默认1个，最多10个"
        await handle_send(bot, event, msg)
        await fangshi_auto_add.finish()
    
    item_type = args[0]
    rank_name = " ".join(args[1:-1]) if len(args) > 2 else args[1]
    quantity = int(args[-1]) if args[-1].isdigit() else 1
    quantity = max(1, min(quantity, MAX_QUANTITY))


    if item_type not in type_mapping:
        msg = f"❌❌❌❌❌❌❌❌ 无效类型！可用类型：{', '.join(type_mapping.keys())}"
        await handle_send(bot, event, msg)
        await fangshi_auto_add.finish()
    
    if rank_name not in rank_map:
        msg = f"❌❌❌❌❌❌❌❌ 无效品阶！输入'品阶帮助'查看完整列表"
        await handle_send(bot, event, msg)
        await fangshi_auto_add.finish()

    # 获取背包物品
    back_msg = sql_message.get_back_msg(user_id)
    if not back_msg:
        msg = "💼💼 道友的背包空空如也！"
        await handle_send(bot, event, msg)
        await fangshi_auto_add.finish()
    
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
        msg = f"🔍🔍 背包中没有符合条件的【{item_type}·{rank_name}】物品"
        await handle_send(bot, event, msg)
        await fangshi_auto_add.finish()
    
    # === 批量处理逻辑 ===
    items_to_process = []
    for item in items_to_add:
        if str(item['id']) in BANNED_ITEM_IDS:
            continue

        min_price = get_fangshi_min_price(group_id, item['name'])
        
        if min_price is None:
            base_rank = convert_rank('江湖好手')[0]
            item_rank = get_item_msg_rank(item['id'])
            price = max(MIN_PRICE, (base_rank - 16) * 100000 - item_rank * 100000 + 1000000)
        else:
            price = min_price
        
        actual_quantity = min(quantity, item['available_num'])
        
        total_price = price * actual_quantity
        if total_price <= 5000000:
            fee_rate = 0.1
        elif total_price <= 10000000:
            fee_rate = 0.15
        elif total_price <= 20000000:
            fee_rate = 0.2
        else:
            fee_rate = 0.3
        
        single_fee = int(total_price * fee_rate)
        
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
        await fangshi_auto_add.finish()
    
    # 一次性扣除总手续费
    sql_message.update_ls(user_id, total_fee, 2)
    
    # 获取当前索引数据
    index_data = get_fangshi_index(group_id)
    existing_ids = set(index_data["items"].keys())
    
    # 准备批量写入的数据
    type_updates = {}
    result_msg = []
    success_count = 0

    for item in items_to_process:
        for _ in range(item['quantity']):
            fangshi_id = generate_fangshi_id(existing_ids)
            existing_ids.add(fangshi_id)
            
            index_data["items"][fangshi_id] = {
                "type": item['type'],
                "user_id": user_id
            }
            
            if item['type'] not in type_updates:
                type_updates[item['type']] = get_fangshi_type_data(group_id, item['type'])
            
            type_updates[item['type']][fangshi_id] = {
                "id": fangshi_id,
                "goods_id": item['id'],
                "name": item['name'],
                "type": item['type'],
                "price": item['price'],
                "quantity": 1,
                "user_id": user_id,
                "user_name": user_info['user_name'],
                "desc": get_item_msg(item['id'])
            }
            
            sql_message.update_back_j(user_id, item['id'], num=1)
            success_count += 1
            result_msg.append(f"{item['name']} x1 - 单价:{number_to(item['price'])}")
    
    # 批量保存
    save_fangshi_index(group_id, index_data)
    for item_type, type_items in type_updates.items():
        save_fangshi_type_data(group_id, item_type, type_items)
    
    # 构建结果消息
    msg = [
        f"\n✨ 成功上架 {success_count} 件物品",
        *result_msg[:10],
        f"💎💎 总手续费: {number_to(total_fee)}灵石"
    ]
    
    if len(result_msg) > 10:
        msg.append(f"...等共{len(result_msg)}件物品")
    
    await send_msg_handler(bot, event, '坊市自动上架', bot.self_id, msg)
    await fangshi_auto_add.finish()

@fangshi_fast_add.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def fangshi_fast_add_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """坊市快速上架（按物品名快速上架）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await fangshi_fast_add.finish()
    
    group_id = str(event.group_id)
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "指令格式：坊市快速上架 物品名 [价格]\n" \
              "▶ 价格：可选，不填则自动匹配坊市最低价\n" \
              "▶ 数量：固定为10个（或背包中全部数量）"
        await handle_send(bot, event, msg)
        await fangshi_fast_add.finish()
    
    goods_name = args[0]
    # 尝试解析价格参数
    try:
        price = int(args[1]) if len(args) > 1 else None
    except ValueError:
        msg = "请输入有效的价格！"
        await handle_send(bot, event, msg)
        await fangshi_fast_add.finish()
    
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
        await fangshi_fast_add.finish()
    
    # 检查可上架数量（固定为10或背包中全部数量）
    available_num = goods_info['goods_num'] - goods_info['bind_num']
    quantity = min(10, available_num)  # 最多10个
    
    if quantity <= 0:
        msg = f"可上架数量不足！背包有{goods_info['goods_num']}个（{goods_info['bind_num']}个绑定），没有可上架数量"
        await handle_send(bot, event, msg)
        await fangshi_fast_add.finish()
    
    # 获取物品类型
    goods_type = get_item_type_by_id(goods_info['goods_id'])
    if goods_type not in ITEM_TYPES:
        msg = f"该物品类型不允许上架！允许类型：{', '.join(ITEM_TYPES)}"
        await handle_send(bot, event, msg)
        await fangshi_fast_add.finish()

    # 检查禁止交易的物品
    if str(goods_info['goods_id']) in BANNED_ITEM_IDS:
        msg = f"物品 {goods_name} 禁止在坊市交易！"
        await handle_send(bot, event, msg)
        await fangshi_fast_add.finish()

    # 获取价格（如果用户未指定价格）
    if price is None:
        # 获取坊市最低价
        min_price = get_fangshi_min_price(group_id, goods_name)
        
        # 如果没有最低价，则使用炼金价格+100万
        if min_price is None:
            base_rank = convert_rank('江湖好手')[0]
            item_rank = get_item_msg_rank(goods_info['goods_id'])
            price = max(MIN_PRICE, (base_rank - 16) * 100000 - item_rank * 100000 + 1000000)
        else:
            price = min_price
    else:
        # 检查用户指定的价格是否符合限制
        xianshi_min = get_xianshi_min_price(goods_name)
        if xianshi_min is not None:
            min_price = max(MIN_PRICE, xianshi_min // 2)
            max_price = xianshi_min * 2
            if price < min_price or price > max_price:
                msg = f"该物品在仙肆的最低价格为{xianshi_min}，坊市价格限制为{min_price}-{max_price}灵石！"
                await handle_send(bot, event, msg)
                await fangshi_fast_add.finish()
        else:
            if price < MIN_PRICE:
                price = max(price, MIN_PRICE)
    
    # 计算总手续费
    total_price = price * quantity
    if total_price <= 5000000:
        fee_rate = 0.1
    elif total_price <= 10000000:
        fee_rate = 0.15
    elif total_price <= 20000000:
        fee_rate = 0.2
    else:
        fee_rate = 0.3
    
    fee = int(total_price * fee_rate)
    
    if user_info['stone'] < fee:
        msg = f"灵石不足支付手续费！需要{fee}灵石，当前拥有{user_info['stone']}灵石"
        await handle_send(bot, event, msg)
        await fangshi_fast_add.finish()
    
    # 扣除总手续费
    sql_message.update_ls(user_id, fee, 2)
    
    # 为每个物品创建独立条目
    for _ in range(quantity):
        # 从背包扣除1个物品
        sql_message.update_back_j(user_id, goods_info['goods_id'], num=1)
        
        # 添加到坊市系统
        index_data = get_fangshi_index(group_id)
        existing_ids = set(index_data["items"].keys())
        fangshi_id = generate_fangshi_id(existing_ids)
        
        # 添加到索引
        index_data["items"][fangshi_id] = {
            "type": goods_type,
            "user_id": user_id
        }
        save_fangshi_index(group_id, index_data)
        
        # 添加到类型文件
        type_items = get_fangshi_type_data(group_id, goods_type)
        type_items[fangshi_id] = {
            "id": fangshi_id,
            "goods_id": goods_info['goods_id'],
            "name": goods_name,
            "type": goods_type,
            "price": price,
            "quantity": 1,  # 每个条目数量固定为1
            "user_id": user_id,
            "user_name": user_info['user_name'],
            "desc": get_item_msg(goods_info['goods_id'])
        }
        save_fangshi_type_data(group_id, goods_type, type_items)
    
    msg = f"\n成功上架 {goods_name} x{quantity} 到坊市！\n"
    msg += f"单价: {number_to(price)} 灵石\n"
    msg += f"总价: {number_to(total_price)} 灵石\n"
    msg += f"手续费: {number_to(fee)} 灵石"
    
    await handle_send(bot, event, msg)
    await fangshi_fast_add.finish()

@fangshi_fast_buy.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def fangshi_fast_buy_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """坊市快速购买"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await fangshi_fast_buy.finish()
    
    group_id = str(event.group_id)
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "指令格式：坊市快速购买 物品名1,物品名2 [数量]\n" \
              "▶ 物品名：支持1-5个物品（可重复），用逗号分隔\n" \
              "▶ 示例：坊市快速购买 两仪心经,两仪心经,两仪心经"
        await handle_send(bot, event, msg)
        await fangshi_fast_buy.finish()
    
    # 解析物品名列表（允许重复且保留顺序）
    goods_names = args[0].split(",")
    if len(goods_names) > 5:
        msg = "一次最多指定5个物品名（可重复）！"
        await handle_send(bot, event, msg)
        await fangshi_fast_buy.finish()
    
    quantity_per_item = 1
    
    # 获取所有用户物品（不包括系统物品）
    index_data = get_fangshi_index(group_id)
    user_items = []
    
    for fangshi_id, item_info in index_data["items"].items():
        if item_info["user_id"] != 0:  # 排除系统物品
            type_items = get_fangshi_type_data(group_id, item_info["type"])
            if fangshi_id in type_items:
                item_data = type_items[fangshi_id]
                if item_data["name"] in goods_names:
                    user_items.append({
                        "id": fangshi_id,
                        "goods_id": item_data["goods_id"],
                        "name": item_data["name"],
                        "type": item_info["type"],
                        "price": item_data["price"],
                        "seller_id": item_info["user_id"],
                        "seller_name": item_data["user_name"]
                    })
    
    if not user_items:
        msg = "坊市中没有符合条件的用户物品！"
        await handle_send(bot, event, msg)
        await fangshi_fast_buy.finish()
    
    # 按价格从低到高排序
    user_items.sort(key=lambda x: x["price"])
    
    # 执行购买（严格按照输入顺序处理每个物品名）
    total_cost = 0
    success_items = []
    failed_items = []
    
    for name in goods_names:
        # 查找该物品所有可购买项（按价格排序）
        available = [item for item in user_items if item["name"] == name]
        remaining = quantity_per_item
        purchased = 0
        item_total = 0
        
        for item in available:
            if remaining <= 0:
                break
            
            try:
                # 检查物品是否已被购买（可能被前一轮购买）
                if item["id"] not in index_data["items"]:
                    continue
                
                # 检查灵石是否足够
                if user_info['stone'] < item["price"]:
                    failed_items.append(f"{item['name']}×1（灵石不足）")
                    continue
                
                # 执行购买
                sql_message.update_ls(user_id, item["price"], 2)  # 扣钱
                sql_message.update_ls(item["seller_id"], item["price"], 1)  # 给卖家
                sql_message.send_back(user_id, item["goods_id"], item["name"], item["type"], 1, 1)
                
                # 从系统中移除
                type_items = get_fangshi_type_data(group_id, item["type"])
                del index_data["items"][item["id"]]
                del type_items[item["id"]]
                save_fangshi_index(group_id, index_data)
                save_fangshi_type_data(group_id, item["type"], type_items)
                
                purchased += 1
                item_total += item["price"]
                total_cost += item["price"]
                remaining -= 1
                
            except Exception as e:
                logger.error(f"坊市快速购买出错: {e}")
                continue
        
        # 记录结果（每个name单独记录）
        if purchased > 0:
            success_items.append(f"{name}×{purchased} ({number_to(item_total)}灵石)")
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
    await fangshi_fast_buy.finish()
    
@shop_remove.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def shop_remove_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """坊市下架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await shop_remove.finish()
    
    group_id = str(event.group_id)
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if not args:
        msg = "请输入要下架的物品名称！"
        await handle_send(bot, event, msg)
        await shop_remove.finish()
    
    goods_name = args[0]
    quantity = int(args[1]) if len(args) > 1 and args[1].isdigit() else None
    
    # 获取所有类型中该用户上架的该物品
    user_items = []
    index_data = get_fangshi_index(group_id)
    
    for fangshi_id, item_info in index_data["items"].items():
        if str(item_info["user_id"]) == str(user_id):
            type_items = get_fangshi_type_data(group_id, item_info["type"])
            if fangshi_id in type_items and type_items[fangshi_id]["name"] == goods_name:
                user_items.append({
                    "id": fangshi_id,
                    "price": type_items[fangshi_id]["price"],
                    "type": item_info["type"]
                })
    
    if not user_items:
        msg = f"您在坊市中没有上架 {goods_name}！"
        await handle_send(bot, event, msg)
        await shop_remove.finish()
    
    # 按价格从低到高排序
    user_items.sort(key=lambda x: x["price"])
    
    # 确定要下架的数量
    if quantity is None:
        # 没指定数量则下架最低价的1个
        items_to_remove = [user_items[0]]
    else:
        # 指定数量则下架价格从低到高的指定数量
        items_to_remove = user_items[:quantity]
    
    # 执行下架操作
    removed_count = 0
    for item in items_to_remove:
        # 从类型文件中移除
        type_items = get_fangshi_type_data(group_id, item["type"])
        if item["id"] in type_items:
            # 退回物品到背包
            item_data = type_items[item["id"]]
            sql_message.send_back(
                user_id,
                item_data["goods_id"],
                goods_name,
                item["type"],
                1  # 每个条目数量固定为1
            )
            
            # 从系统中移除
            del type_items[item["id"]]
            save_fangshi_type_data(group_id, item["type"], type_items)
            
            # 从索引中移除
            del index_data["items"][item["id"]]
            save_fangshi_index(group_id, index_data)
            
            removed_count += 1
    
    msg = f"成功下架 {goods_name} x{removed_count}！已退回背包"
    if len(user_items) > removed_count:
        msg += f"\n(坊市中仍有 {len(user_items)-removed_count} 个 {goods_name})"
    
    await handle_send(bot, event, msg)
    await shop_remove.finish()

@buy.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def buy_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """坊市购买"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await buy.finish()
    
    group_id = str(event.group_id)
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "请输入要购买的坊市ID！"
        await handle_send(bot, event, msg)
        await buy.finish()
    
    fangshi_id = args[0]
    quantity = int(args[1]) if len(args) > 1 else 1
    
    # 从索引查找物品
    index_data = get_fangshi_index(group_id)
    if fangshi_id not in index_data["items"]:
        msg = f"未找到坊市ID为 {fangshi_id} 的物品！"
        await handle_send(bot, event, msg)
        await buy.finish()
    
    item_info = index_data["items"][fangshi_id]
    
    # 检查是否是自己的物品
    if str(item_info["user_id"]) == str(user_id):
        msg = "不能购买自己上架的物品！"
        await handle_send(bot, event, msg)
        await buy.finish()
    
    # 从类型文件获取详细信息
    type_items = get_fangshi_type_data(group_id, item_info["type"])
    if fangshi_id not in type_items:
        msg = "物品数据异常，请联系管理员！"
        await handle_send(bot, event, msg)
        await buy.finish()
    
    item = type_items[fangshi_id]
    seller_name = "系统" if item["user_id"] == 0 else item["user_name"]
    # 检查库存（无限物品跳过检查）
    if not (isinstance(item["quantity"], str) and item["quantity"] == "无限"):
        if item["quantity"] < quantity:
            msg = f"库存不足！只有 {item['quantity']} 个可用"
            await handle_send(bot, event, msg)
            await buy.finish()
    
    # 计算总价
    total_price = item["price"] * quantity
    
    # 检查灵石是否足够
    if user_info["stone"] < total_price:
        msg = f"灵石不足！需要 {number_to(total_price)} 灵石，当前拥有 {number_to(user_info['stone'])} 灵石"
        await handle_send(bot, event, msg)
        await buy.finish()
    
    try:
        # 扣除买家灵石
        sql_message.update_ls(user_id, total_price, 2)
        
        # 给卖家灵石（如果不是系统物品）
        if item_info["user_id"] != 0:
            seller_id = item_info["user_id"]
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
        
        # 更新库存（无限物品不更新）
        if not (isinstance(item["quantity"], str) and item["quantity"] == "无限"):
            item["quantity"] -= quantity
            if item["quantity"] <= 0:
                # 从系统中移除
                del index_data["items"][fangshi_id]
                del type_items[fangshi_id]
            else:
                type_items[fangshi_id] = item
            
            # 保存更改
            save_fangshi_index(group_id, index_data)
            save_fangshi_type_data(group_id, item_info["type"], type_items)
        
        msg = f"成功购买 {item['name']} x{quantity}（来自{seller_name}）！花费 {number_to(total_price)} 灵石"
        await handle_send(bot, event, msg)
    except Exception as e:
        logger.error(f"坊市购买出错: {e}")
        msg = "购买过程中出现错误，请稍后再试！"
        await handle_send(bot, event, msg)
    
    await buy.finish()

@shop_view.handle(parameterless=[Cooldown(at_sender=False)])
async def shop_view_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """坊市查看"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await shop_view.finish()
    
    group_id = str(event.group_id)
    
    # 解析参数
    args_str = args.extract_plain_text().strip()
    
    # 情况1：无参数 - 显示可用类型
    if not args_str:
        msg = "请指定查看类型：【药材、装备、丹药、技能】"
        await handle_send(bot, event, msg)
        await shop_view.finish()
    
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
        if parts[0] in ITEM_TYPES:
            item_type = parts[0]
            if len(parts) > 1 and parts[1].isdigit():
                current_page = int(parts[1])
    
    # 检查类型有效性
    if item_type not in ITEM_TYPES:
        msg = f"无效类型！可用类型：【{', '.join(ITEM_TYPES)}】"
        await handle_send(bot, event, msg)
        await shop_view.finish()
    
    # 读取对应类型的物品数据
    type_items = get_fangshi_type_data(group_id, item_type)
    if not type_items:
        msg = f"坊市中暂无{item_type}类物品！"
        await handle_send(bot, event, msg)
        await shop_view.finish()
    
    # 处理物品显示逻辑
    system_items = []  # 存储系统物品
    user_items = {}    # 存储用户物品（按名称分组，只保留最低价）
    
    for item_id, item in type_items.items():
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
        await shop_view.finish()
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = items_list[start_idx:end_idx]

    # 构建消息
    msg_list = [f"\n☆------坊市 {item_type}------☆"]
    for item in paged_items:
        price_str = number_to(item['price'])
        msg = f"\n{item['name']} {price_str}灵石 \nID:{item['id']}"
        
        # 处理数量显示
        if isinstance(item['quantity'], str) and item['quantity'] == "无限":
            msg += f" 不限量"
        elif item['quantity'] > 1:
            msg += f" 限售:{item['quantity']}"
        
        msg_list.append(msg)
    
    msg_list.append(f"\n第 {current_page}/{total_pages} 页")
    if total_pages > 1:
        next_page_cmd = f"坊市查看{item_type}{current_page + 1}"
        msg_list.append(f"输入 {next_page_cmd} 查看下一页")
    
    await send_msg_handler(bot, event, '坊市查看', bot.self_id, msg_list)
    await shop_view.finish()

@my_shop.handle(parameterless=[Cooldown(at_sender=False)])
async def my_shop_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """我的坊市"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await my_shop.finish()
    
    group_id = str(event.group_id)
    user_id = user_info['user_id']
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    # 从索引获取用户所有物品
    index_data = get_fangshi_index(group_id)
    user_items = []
    
    for fangshi_id, item_info in index_data["items"].items():
        if str(item_info["user_id"]) == str(user_id):
            # 从对应类型文件读取详细信息
            type_items = get_fangshi_type_data(group_id, item_info["type"])
            if fangshi_id in type_items:
                user_items.append(type_items[fangshi_id])
    
    # 按价格排序
    user_items.sort(key=lambda x: x['name'])
    
    # 检查是否有上架物品
    if not user_items:
        msg = "您在坊市中没有上架任何物品！"
        await handle_send(bot, event, msg)
        await my_shop.finish()
    
    # 分页处理
    per_page = 20
    total_pages = (len(user_items) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = user_items[start_idx:end_idx]
    
    msg_list = [f"\n☆------{user_info['user_name']}的坊市物品------☆"]
    for item in paged_items:
        price_str = number_to(item['price'])
        msg = f"{item['name']} {price_str}灵石"
        if isinstance(item['quantity'], int) and item['quantity'] > 1:
            msg += f" x{item['quantity']}"
        msg_list.append(msg)
    
    msg_list.append(f"\n第 {current_page}/{total_pages} 页")
    if total_pages > 1:
        msg_list.append(f"输入 我的坊市 {current_page + 1} 查看下一页")
    
    await send_msg_handler(bot, event, '我的坊市', bot.self_id, msg_list)
    await my_shop.finish()

@shop_added_by_admin.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def shop_added_by_admin_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """系统坊市上架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "请输入正确指令！格式：系统坊市上架 物品名称 [价格] [数量]"
        await handle_send(bot, event, msg)
        await shop_added_by_admin.finish()
    
    group_id = str(event.group_id)
    goods_name = args[0]
    try:
        price = int(args[1]) if len(args) > 1 else MIN_PRICE
        price = max(price, MIN_PRICE)
        quantity = int(args[2]) if len(args) > 2 else 0  # 0表示无限
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg)
        await shop_added_by_admin.finish()
    
    # 查找物品ID
    goods_id = None
    for k, v in items.items.items():
        if goods_name == v['name']:
            goods_id = k
            break
    
    if not goods_id:
        msg = f"未找到物品 {goods_name}！"
        await handle_send(bot, event, msg)
        await shop_added_by_admin.finish()
    
    # 获取物品类型
    goods_type = get_item_type_by_id(goods_id)
    if goods_type not in ITEM_TYPES:
        msg = f"该物品类型不允许上架！允许类型：{', '.join(ITEM_TYPES)}"
        await handle_send(bot, event, msg)
        await shop_added_by_admin.finish()
    
    # 添加到坊市系统
    # 获取索引数据
    index_data = get_fangshi_index(group_id)
    fangshi_id = str(index_data["next_id"])  # 使用顺序ID
    
    # 更新下一个ID
    index_data["next_id"] += 1
    
    # 添加到索引
    index_data["items"][fangshi_id] = {
        "type": goods_type,
        "user_id": 0  # 0表示系统物品
    }
    save_fangshi_index(group_id, index_data)
    
    # 添加到类型文件
    type_items = get_fangshi_type_data(group_id, goods_type)
    type_items[fangshi_id] = {
        "id": fangshi_id,
        "goods_id": goods_id,
        "name": goods_name,
        "type": goods_type,
        "price": price,
        "quantity": "无限" if quantity == 0 else quantity,  # 0表示无限
        "user_id": 0,
        "user_name": "系统",
        "desc": get_item_msg(goods_id)
    }
    save_fangshi_type_data(group_id, goods_type, type_items)
    
    msg = f"系统成功上架 {goods_name} 到坊市！\n"
    msg += f"价格: {number_to(price)} 灵石\n"
    msg += f"数量: {'无限' if quantity == 0 else quantity}\n"
    msg += f"坊市ID: {fangshi_id}"
    
    await handle_send(bot, event, msg)
    await shop_added_by_admin.finish()

@shop_remove_by_admin.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def shop_remove_by_admin_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """系统坊市下架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = str(event.group_id)
    arg = args.extract_plain_text().strip()
    
    if not arg:
        msg = "请输入要下架的坊市ID或物品名称！"
        await handle_send(bot, event, msg)
        await shop_remove_by_admin.finish()
    
    index_data = get_fangshi_index(group_id)
    removed_items = []
    
    if arg.isdigit():  # 按ID下架
        fangshi_id = arg
        if fangshi_id in index_data["items"]:
            item_info = index_data["items"][fangshi_id]
            type_items = get_fangshi_type_data(group_id, item_info["type"])
            if fangshi_id in type_items:
                item_data = type_items[fangshi_id]
                removed_items.append(item_data)
                
                # 如果是用户物品，退回给用户
                if item_info["user_id"] != 0:
                    sql_message.send_back(
                        item_info["user_id"],
                        item_data["goods_id"],
                        item_data["name"],
                        item_info["type"],
                        item_data["quantity"]
                    )
                
                # 从系统中移除
                del index_data["items"][fangshi_id]
                del type_items[fangshi_id]
                save_fangshi_index(group_id, index_data)
                save_fangshi_type_data(group_id, item_info["type"], type_items)
        else:
            msg = f"未找到坊市ID为 {fangshi_id} 的物品！"
            await handle_send(bot, event, msg)
            await shop_remove_by_admin.finish()
    else:  # 按名称下架
        goods_name = arg
        # 解析数量
        parts = goods_name.split()
        quantity = None
        if len(parts) > 1 and parts[-1].isdigit():
            quantity = int(parts[-1])
            goods_name = " ".join(parts[:-1])
        
        # 查找所有匹配的用户物品（不包括系统物品）
        user_items = []
        for fangshi_id, item_info in index_data["items"].items():
            if item_info["user_id"] != 0:  # 排除系统物品
                type_items = get_fangshi_type_data(group_id, item_info["type"])
                if fangshi_id in type_items and type_items[fangshi_id]["name"] == goods_name:
                    user_items.append({
                        "id": fangshi_id,
                        "price": type_items[fangshi_id]["price"],
                        "type": item_info["type"],
                        "user_id": item_info["user_id"],
                        "item_data": type_items[fangshi_id]
                    })
        
        if not user_items:
            msg = f"坊市中没有用户上架的 {goods_name} 物品！"
            await handle_send(bot, event, msg)
            await shop_remove_by_admin.finish()
        
        # 按价格从低到高排序
        user_items.sort(key=lambda x: x["price"])
        
        # 确定要下架的数量
        if quantity is None:
            # 没指定数量则下架最低价的1个
            items_to_remove = [user_items[0]]
        else:
            # 指定数量则下架价格从低到高的指定数量
            items_to_remove = user_items[:quantity]
        
        # 执行下架操作
        for item in items_to_remove:
            # 从类型文件中移除
            type_items = get_fangshi_type_data(group_id, item["type"])
            if item["id"] in type_items:
                removed_items.append(item["item_data"])
                
                # 退回物品给用户
                sql_message.send_back(
                    item["user_id"],
                    item["item_data"]["goods_id"],
                    item["item_data"]["name"],
                    item["type"],
                    item["item_data"]["quantity"]
                )
                
                # 从系统中移除
                del index_data["items"][item["id"]]
                del type_items[item["id"]]
                save_fangshi_index(group_id, index_data)
                save_fangshi_type_data(group_id, item["type"], type_items)
    
    if removed_items:
        msg = "成功下架以下物品：\n"
        for item in removed_items:
            owner = "系统" if item["user_id"] == 0 else sql_message.get_user_info_with_id(item["user_id"])["user_name"]
            msg += f"ID:{item['id']} {item['name']} x{item['quantity']} (已退回给:{owner})\n"
    else:
        msg = "没有物品被下架！"
    
    await handle_send(bot, event, msg)
    await shop_remove_by_admin.finish()

@shop_off_all.handle(parameterless=[Cooldown(60, isolate_level=CooldownIsolateLevel.GROUP, parallel=1)])
async def shop_off_all_(bot: Bot, event: GroupMessageEvent):
    """清空坊市"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await shop_off_all.finish()
    
    group_id = str(event.group_id)
    
    msg = "正在清空坊市，请稍候..."
    await handle_send(bot, event, msg)
    
    # 获取所有物品
    index_data = get_fangshi_index(group_id)
    removed_items = []
    
    for fangshi_id, item_info in index_data["items"].items():
        type_items = get_fangshi_type_data(group_id, item_info["type"])
        if fangshi_id in type_items:
            item = type_items[fangshi_id]
            removed_items.append(item)
            
            # 如果是用户物品，退回
            if item_info["user_id"] != 0:
                sql_message.send_back(
                    item_info["user_id"],
                    item["goods_id"],
                    item["name"],
                    item_info["type"],
                    item["quantity"]
                )
    
    # 清空所有数据
    for item_type in ITEM_TYPES:
        save_fangshi_type_data(group_id, item_type, {})
    
    save_fangshi_index(group_id, {"next_id": 1, "items": {}})
    
    if removed_items:
        msg = "成功清空坊市！共下架以下物品：\n"
        for item in removed_items[:10]:  # 最多显示10条
            owner = "系统" if item["user_id"] == 0 else item["user_name"]
            msg += f"{item['name']} x{item['quantity']} (来自:{owner})\n"
        if len(removed_items) > 10:
            msg += f"...等共{len(removed_items)}件物品"
    else:
        msg = "坊市已经是空的，没有物品被下架！"
    
    await handle_send(bot, event, msg)
    await shop_off_all.finish()

GUISHI_QIUGOU_INDEX = GUISHI_DATA_PATH / "guishi_qiugou_index.json"
GUISHI_BAITAN_INDEX = GUISHI_DATA_PATH / "guishi_baitan_index.json"

# === 索引功能 ===
def get_guishi_index(index_type):
    """获取鬼市索引"""
    index_file = GUISHI_QIUGOU_INDEX if index_type == "qiugou" else GUISHI_BAITAN_INDEX
    try:
        if index_file.exists():
            with open(index_file, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"读取鬼市{index_type}索引失败: {e}")
    return {"by_item": {}, "by_user": {}}

def save_guishi_index(index_type, data):
    """保存鬼市索引"""
    index_file = GUISHI_QIUGOU_INDEX if index_type == "qiugou" else GUISHI_BAITAN_INDEX
    try:
        with open(index_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        logger.error(f"保存鬼市{index_type}索引失败: {e}")
        return False

def update_qiugou_index(order_id, item_name, user_id, action="add"):
    """更新求购索引"""
    index = get_guishi_index("qiugou")
    
    # 按物品索引
    if action == "add":
        if item_name not in index["by_item"]:
            index["by_item"][item_name] = []
        if user_id not in index["by_item"][item_name]:
            index["by_item"][item_name].append(user_id)
    else:  # remove
        if item_name in index["by_item"] and user_id in index["by_item"][item_name]:
            index["by_item"][item_name].remove(user_id)
            if not index["by_item"][item_name]:
                del index["by_item"][item_name]
    
    # 按用户索引
    if action == "add":
        if user_id not in index["by_user"]:
            index["by_user"][user_id] = []
        if order_id not in index["by_user"][user_id]:
            index["by_user"][user_id].append(order_id)
    else:  # remove
        if user_id in index["by_user"] and order_id in index["by_user"][user_id]:
            index["by_user"][user_id].remove(order_id)
            if not index["by_user"][user_id]:
                del index["by_user"][user_id]
    
    save_guishi_index("qiugou", index)

def update_baitan_index(order_id, item_name, user_id, action="add"):
    """更新摆摊索引"""
    index = get_guishi_index("baitan")
    
    # 按物品索引
    if action == "add":
        if item_name not in index["by_item"]:
            index["by_item"][item_name] = []
        if user_id not in index["by_item"][item_name]:
            index["by_item"][item_name].append(user_id)
    else:  # remove
        if item_name in index["by_item"] and user_id in index["by_item"][item_name]:
            index["by_item"][item_name].remove(user_id)
            if not index["by_item"][item_name]:
                del index["by_item"][item_name]
    
    # 按用户索引
    if action == "add":
        if user_id not in index["by_user"]:
            index["by_user"][user_id] = []
        if order_id not in index["by_user"][user_id]:
            index["by_user"][user_id].append(order_id)
    else:  # remove
        if user_id in index["by_user"] and order_id in index["by_user"][user_id]:
            index["by_user"][user_id].remove(order_id)
            if not index["by_user"][user_id]:
                del index["by_user"][user_id]
    
    save_guishi_index("baitan", index)

# === 核心功能 ===
def get_guishi_user_data(user_id):
    """获取用户鬼市数据"""
    user_file = GUISHI_DATA_PATH / f"user_{user_id}.json"
    try:
        if user_file.exists():
            with open(user_file, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"读取鬼市用户数据失败: {e}")
    return {
        "stone": 0,  # 鬼市账户灵石
        "qiugou_orders": {},  # 求购订单 {order_id: {item_name, price, quantity, filled}}
        "baitan_orders": {},  # 摆摊订单 {order_id: {item_id, item_name, price, quantity, sold}}
        "items": {}  # 暂存物品 {item_id: {name, type, quantity}}
    }

def save_guishi_user_data(user_id, data):
    """保存用户鬼市数据"""
    user_file = GUISHI_DATA_PATH / f"user_{user_id}.json"
    try:
        with open(user_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        logger.error(f"保存鬼市用户数据失败: {e}")
        return False

def generate_guishi_id(existing_ids=None):
    """生成6-10位随机不重复ID"""
    existing_ids = existing_ids or set()
    while True:
        # 使用时间戳+随机数确保唯一性
        timestamp_part = int(time.time() % 10000)
        random_part = random.randint(100, 99999)
        new_id = int(f"{timestamp_part}{random_part}") % 10**10  # 确保不超过10位
        
        # 限制在6-10位
        new_id = max(100000, min(new_id, 9999999999))
        
        # 检查是否已存在
        if str(new_id) not in existing_ids:
            return str(new_id)

async def process_guishi_transactions(user_id):
    """使用索引优化交易处理"""
    user_data = get_guishi_user_data(user_id)
    transactions = []
    
    # 获取索引
    qiugou_index = get_guishi_index("qiugou")
    baitan_index = get_guishi_index("baitan")
    
    # 处理求购订单
    for order_id, order in list(user_data["qiugou_orders"].items()):
        item_name = order["item_name"]
        
        # 使用索引快速查找匹配的摆摊订单
        matched_sellers = baitan_index["by_item"].get(item_name, [])
        
        for seller_id in matched_sellers:
            if order.get("filled", 0) >= order["quantity"]:
                break  # 订单已完成
                
            seller_data = get_guishi_user_data(seller_id)
            for seller_order_id, seller_order in list(seller_data["baitan_orders"].items()):
                if (seller_order["item_name"] == item_name and 
                    seller_order["price"] <= order["price"] and
                    seller_order["quantity"] - seller_order.get("sold", 0) > 0):
                    
                    # 计算可交易数量
                    available = seller_order["quantity"] - seller_order.get("sold", 0)
                    needed = order["quantity"] - order.get("filled", 0)
                    trade_num = min(available, needed)
                    
                    # 检查鬼市账户余额
                    total_cost = trade_num * seller_order["price"]
                    if user_data["stone"] < total_cost:
                        continue
                        
                    # 执行交易
                    user_data["stone"] -= total_cost
                    seller_data["stone"] += total_cost
                    
                    # 更新订单状态
                    order["filled"] = order.get("filled", 0) + trade_num
                    seller_order["sold"] = seller_order.get("sold", 0) + trade_num
                    
                    # 转移物品
                    item_id = seller_order["item_id"]
                    if item_id not in user_data["items"]:
                        user_data["items"][item_id] = {
                            "name": seller_order["item_name"],
                            "type": items.get_data_by_item_id(item_id)["type"],
                            "quantity": 0
                        }
                    user_data["items"][item_id]["quantity"] += trade_num
                    
                    # 记录交易
                    transactions.append(f"求购：已收购 {seller_order['item_name']} x{trade_num} (花费{number_to(total_cost)}灵石)")
                    
                    # 保存对方数据
                    save_guishi_user_data(seller_id, seller_data)
                    
                    # 检查订单是否完成
                    if seller_order["sold"] >= seller_order["quantity"]:
                        del seller_data["baitan_orders"][seller_order_id]
                        update_baitan_index(seller_order_id, item_name, seller_id, "remove")
                    
            # 保存卖家数据
            save_guishi_user_data(seller_id, seller_data)
        
        # 检查求购订单是否完成
        if order.get("filled", 0) >= order["quantity"]:
            del user_data["qiugou_orders"][order_id]
            update_qiugou_index(order_id, item_name, user_id, "remove")
            transactions.append(f"求购订单 {order_id} 已完成")
    
    # 处理摆摊订单
    for order_id, order in list(user_data["baitan_orders"].items()):
        item_name = order["item_name"]
        
        # 使用索引快速查找匹配的求购订单
        matched_buyers = qiugou_index["by_item"].get(item_name, [])
        
        for buyer_id in matched_buyers:
            if order.get("sold", 0) >= order["quantity"]:
                break  # 订单已完成
                
            buyer_data = get_guishi_user_data(buyer_id)
            for buyer_order_id, buyer_order in list(buyer_data["qiugou_orders"].items()):
                if (buyer_order["item_name"] == item_name and 
                    buyer_order["price"] >= order["price"] and
                    buyer_order["quantity"] - buyer_order.get("filled", 0) > 0):
                    
                    # 计算可交易数量
                    available = order["quantity"] - order.get("sold", 0)
                    needed = buyer_order["quantity"] - buyer_order.get("filled", 0)
                    trade_num = min(available, needed)
                    
                    # 检查对方鬼市账户余额
                    total_cost = trade_num * order["price"]
                    if buyer_data["stone"] < total_cost:
                        continue
                        
                    # 执行交易
                    buyer_data["stone"] -= total_cost
                    user_data["stone"] += total_cost
                    
                    # 更新订单状态
                    order["sold"] = order.get("sold", 0) + trade_num
                    buyer_order["filled"] = buyer_order.get("filled", 0) + trade_num
                    
                    # 转移物品
                    item_id = order["item_id"]
                    if item_id not in buyer_data["items"]:
                        buyer_data["items"][item_id] = {
                            "name": order["item_name"],
                            "type": items.get_data_by_item_id(item_id)["type"],
                            "quantity": 0
                        }
                    buyer_data["items"][item_id]["quantity"] += trade_num
                    
                    # 记录交易
                    transactions.append(f"摆摊：已出售 {order['item_name']} x{trade_num} (获得{number_to(total_cost)}灵石)")
                    
                    # 保存对方数据
                    save_guishi_user_data(buyer_id, buyer_data)
                    
                    # 检查订单是否完成
                    if buyer_order["filled"] >= buyer_order["quantity"]:
                        del buyer_data["qiugou_orders"][buyer_order_id]
                        update_qiugou_index(buyer_order_id, item_name, buyer_id, "remove")
                    
            # 保存买家数据
            save_guishi_user_data(buyer_id, buyer_data)
        
        # 检查摆摊订单是否完成
        if order.get("sold", 0) >= order["quantity"]:
            del user_data["baitan_orders"][order_id]
            update_baitan_index(order_id, item_name, user_id, "remove")
            transactions.append(f"摆摊订单 {order_id} 已完成")
    
    # 保存用户数据
    save_guishi_user_data(user_id, user_data)
    
    return transactions

# === 命令处理 ===
@guishi_deposit.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def guishi_deposit_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市存灵石"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await guishi_deposit.finish()
    
    user_id = user_info['user_id']
    amount_str = args.extract_plain_text().strip()
    
    if not amount_str.isdigit():
        msg = "请输入正确的灵石数量！"
        await handle_send(bot, event, msg)
        await guishi_deposit.finish()
    
    amount = int(amount_str)
    if amount <= 0:
        msg = "存入数量必须大于0！"
        await handle_send(bot, event, msg)
        await guishi_deposit.finish()
    
    if user_info['stone'] < amount:
        msg = f"灵石不足！当前拥有 {user_info['stone']} 灵石"
        await handle_send(bot, event, msg)
        await guishi_deposit.finish()
    
    # 扣除用户灵石
    sql_message.update_ls(user_id, amount, 2)
    
    # 存入鬼市账户
    user_data = get_guishi_user_data(user_id)
    user_data["stone"] += amount
    save_guishi_user_data(user_id, user_data)
    
    msg = f"成功存入 {number_to(amount)} 灵石到鬼市账户！"
    await handle_send(bot, event, msg)
    await guishi_deposit.finish()

@guishi_withdraw.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def guishi_withdraw_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市取灵石（收取20%暂存费）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await guishi_withdraw.finish()
    
    user_id = user_info['user_id']
    amount_str = args.extract_plain_text().strip()
    
    if not amount_str.isdigit():
        msg = "请输入正确的灵石数量！"
        await handle_send(bot, event, msg)
        await guishi_withdraw.finish()
    
    amount = int(amount_str)
    if amount <= 0:
        msg = "取出数量必须大于0！"
        await handle_send(bot, event, msg)
        await guishi_withdraw.finish()
    
    user_data = get_guishi_user_data(user_id)
    if user_data["stone"] < amount:
        msg = f"鬼市账户余额不足！当前余额 {user_data['stone']} 灵石"
        await handle_send(bot, event, msg)
        await guishi_withdraw.finish()
    
    # 计算手续费（20%）
    fee = int(amount * 0.2)
    actual_amount = amount - fee
    
    # 更新鬼市账户
    user_data["stone"] -= amount
    save_guishi_user_data(user_id, user_data)
    
    # 给用户灵石
    sql_message.update_ls(user_id, actual_amount, 1)
    
    msg = f"成功取出 {number_to(amount)} 灵石（扣除20%暂存费，实际到账 {number_to(actual_amount)} 灵石）"
    await handle_send(bot, event, msg)
    await guishi_withdraw.finish()

@guishi_info.handle(parameterless=[Cooldown(at_sender=False)])
async def guishi_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """鬼市信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await guishi_info.finish()
    
    user_id = user_info['user_id']
    user_data = get_guishi_user_data(user_id)
    
    # 构建消息
    msg = f"\n☆------鬼市账户信息------☆\n"
    msg += f"账户余额：{number_to(user_data['stone'])} 灵石"
    
    msg += f"\n☆------求购订单------☆\n"
    if user_data["qiugou_orders"]:
        for order_id, order in user_data["qiugou_orders"].items():
            filled = order.get("filled", 0)
            status = f"{filled}/{order['quantity']}" if order["quantity"] > 1 else "进行中"
            if filled >= order["quantity"]:
                status = "已完成"
            msg += f"ID:{order_id} {order['item_name']} 单价:{number_to(order['price'])} 状态:{status}\n"
    else:
        msg += "无\n"
    
    msg += f"\n☆------摆摊订单------☆\n"
    if user_data["baitan_orders"]:
        for order_id, order in user_data["baitan_orders"].items():
            sold = order.get("sold", 0)
            status = f"{sold}/{order['quantity']}" if order["quantity"] > 1 else "进行中"
            if sold >= order["quantity"]:
                status = "已完成"
            msg += f"ID:{order_id} {order['item_name']} 单价:{number_to(order['price'])} 状态:{status}\n"
    else:
        msg += "无\n"
    
    msg += f"\n☆------暂存物品------☆\n"
    if user_data["items"]:
        for item_id, item in user_data["items"].items():
            msg += f"{item['name']} x{item['quantity']}\n"
    else:
        msg += "无\n"
    
    await handle_send(bot, event, msg)
    await guishi_info.finish()

@guishi_qiugou.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def guishi_qiugou_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市求购"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await guishi_qiugou.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 2:
        msg = "指令格式：鬼市求购 物品名称 价格 [数量]\n数量不填默认为1"
        await handle_send(bot, event, msg)
        await guishi_qiugou.finish()
    
    goods_name = args[0]
    try:
        price = int(args[1])
        quantity = int(args[2]) if len(args) > 2 else 1
        quantity = min(quantity, GUISHI_MAX_QUANTITY)
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg)
        await guishi_qiugou.finish()
    
    # 检查禁止交易的物品
    goods_id = None
    for k, v in items.items.items():
        if goods_name == v['name']:
            if str(k) in BANNED_ITEM_IDS:
                msg = f"物品 {goods_name} 禁止在鬼市交易！"
                await handle_send(bot, event, msg)
                await guishi_qiugou.finish()
            goods_id = k
            break
    
    if not goods_id:
        msg = f"物品 {goods_name} 不存在！"
        await handle_send(bot, event, msg)
        await guishi_qiugou.finish()
    
    # 检查订单数量限制
    user_data = get_guishi_user_data(user_id)
    if len(user_data["qiugou_orders"]) >= MAX_QIUGOU_ORDERS:
        msg = f"您的求购订单已达上限({MAX_QIUGOU_ORDERS})，请先取消部分订单！"
        await handle_send(bot, event, msg)
        await guishi_qiugou.finish()
    
    # 检查鬼市账户余额是否足够
    user_data = get_guishi_user_data(user_id)
    total_cost = price * quantity
    if user_data["stone"] < total_cost:
        msg = f"鬼市账户余额不足！需要 {number_to(total_cost)} 灵石，当前余额 {number_to(user_data['stone'])} 灵石"
        await handle_send(bot, event, msg)
        await guishi_qiugou.finish()
    
    # 生成订单ID
    existing_ids = set(user_data["qiugou_orders"].keys())
    order_id = generate_guishi_id(existing_ids)
    
    # 添加求购订单
    user_data["qiugou_orders"][order_id] = {
        "item_name": goods_name,
        "price": price,
        "quantity": quantity,
        "filled": 0
    }
    
    # 冻结相应灵石
    user_data["stone"] -= total_cost
    save_guishi_user_data(user_id, user_data)
    
    # 更新索引
    update_qiugou_index(order_id, goods_name, user_id, "add")
    
    # 处理可能的即时交易
    transactions = await process_guishi_transactions(user_id)
    
    msg = f"成功发布求购订单！\n"
    msg += f"物品：{goods_name}\n"
    msg += f"价格：{number_to(price)} 灵石\n"
    msg += f"数量：{quantity}\n"
    msg += f"订单ID：{order_id}\n"
    
    if transactions:
        msg += f"\n☆------交易结果------☆\n"
        msg += "\n".join(transactions)
    
    await handle_send(bot, event, msg)
    await guishi_qiugou.finish()

@guishi_cancel_qiugou.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def guishi_cancel_qiugou_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """取消鬼市求购（支持无参数自动取消已完成、指定ID或全部取消）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await guishi_cancel_qiugou.finish()
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    user_data = get_guishi_user_data(user_id)
    if not user_data["qiugou_orders"]:
        msg = "您当前没有求购订单！"
        await handle_send(bot, event, msg)
        await guishi_cancel_qiugou.finish()
    
    # 处理不同参数情况
    if not arg:  # 无参数，自动取消已完成的
        completed_orders = []
        refund_total = 0
        for order_id, order in list(user_data["qiugou_orders"].items()):
            if order.get("filled", 0) >= order["quantity"]:
                completed_orders.append(order_id)
                # 已完成订单的灵石已全部扣除，无需退还
        
        if not completed_orders:
            msg = "没有已完成的求购订单可自动取消！"
            await handle_send(bot, event, msg)
            await guishi_cancel_qiugou.finish()
        
        msg = "已自动取消以下已完成订单：\n"
        for order_id in completed_orders:
            order = user_data["qiugou_orders"][order_id]
            msg += f"ID:{order_id} {order['item_name']} x{order['quantity']}\n"
            del user_data["qiugou_orders"][order_id]
            update_qiugou_index(order_id, order["item_name"], user_id, "remove")
        
    elif arg == "全部":  # 取消所有求购订单
        msg = "已取消所有求购订单：\n"
        refund_total = 0
        for order_id, order in list(user_data["qiugou_orders"].items()):
            filled = order.get("filled", 0)
            refund = (order["quantity"] - filled) * order["price"]
            refund_total += refund
            
            msg += f"ID:{order_id} {order['item_name']} 已购:{filled}/{order['quantity']}\n"
            del user_data["qiugou_orders"][order_id]
            update_qiugou_index(order_id, order["item_name"], user_id, "remove")
        
        if refund_total > 0:
            user_data["stone"] += refund_total
            msg += f"\n退还 {number_to(refund_total)} 灵石到鬼市账户"
        
    else:  # 指定ID取消
        order_id = arg
        if order_id not in user_data["qiugou_orders"]:
            msg = f"未找到求购订单 {order_id}！"
            await handle_send(bot, event, msg)
            await guishi_cancel_qiugou.finish()
        
        order = user_data["qiugou_orders"][order_id]
        filled = order.get("filled", 0)
        refund = (order["quantity"] - filled) * order["price"]
        
        # 退还灵石
        user_data["stone"] += refund
        del user_data["qiugou_orders"][order_id]
        update_qiugou_index(order_id, order["item_name"], user_id, "remove")
        
        msg = f"已取消求购订单 {order_id}：\n"
        msg += f"物品：{order['item_name']}\n"
        msg += f"价格：{number_to(order['price'])} 灵石\n"
        msg += f"已购：{filled}/{order['quantity']}\n"
        if refund > 0:
            msg += f"退还 {number_to(refund)} 灵石到鬼市账户"
    
    save_guishi_user_data(user_id, user_data)
    await handle_send(bot, event, msg)
    await guishi_cancel_qiugou.finish()

@guishi_baitan.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def guishi_baitan_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市摆摊（每天18:00-次日8:00开放）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await guishi_baitan.finish()
    
    # 检查摆摊时间
    now = datetime.now()
    current_hour = now.hour
    
    # 判断是否在允许摆摊的时间段 (18:00-23:59 或 00:00-08:00)
    if not (GUISHI_BAITAN_START_HOUR <= current_hour <= 23 or 0 <= current_hour < GUISHI_BAITAN_END_HOUR):
        next_start = now.replace(hour=GUISHI_BAITAN_START_HOUR, minute=0, second=0, microsecond=0)
        if now.hour >= GUISHI_BAITAN_END_HOUR:  # 如果当前时间已经过了8点，则下个开始时间是今天18点
            if now.hour >= GUISHI_BAITAN_START_HOUR:  # 如果已经过了18点，则下个开始时间是明天18点
                next_start += timedelta(days=1)
        else:  # 如果当前时间小于8点，则下个开始时间是今天18点
            pass
        
        time_left = next_start - now
        hours = time_left.seconds // 3600
        minutes = (time_left.seconds % 3600) // 60
        
        msg = f"鬼市摆摊时间：每天18:00-次日8:00\n"
        msg += f"下次可摆摊时间：{next_start.strftime('%m月%d日 %H:%M')}（{hours}小时{minutes}分钟后）"
        await handle_send(bot, event, msg)
        await guishi_baitan.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 2:
        msg = "指令格式：鬼市摆摊 物品名称 价格 [数量]\n数量不填默认为1"
        await handle_send(bot, event, msg)
        await guishi_baitan.finish()
    
    goods_name = args[0]
    try:
        price = int(args[1])
        quantity = int(args[2]) if len(args) > 2 else 1
        quantity = min(quantity, GUISHI_MAX_QUANTITY)
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg)
        await guishi_baitan.finish()
    
    # 检查禁止交易的物品
    goods_id = None
    for k, v in items.items.items():
        if goods_name == v['name']:
            if str(k) in BANNED_ITEM_IDS:
                msg = f"物品 {goods_name} 禁止在鬼市交易！"
                await handle_send(bot, event, msg)
                await guishi_baitan.finish()
            goods_id = k
            break
    
    if not goods_id:
        msg = f"物品 {goods_name} 不存在！"
        await handle_send(bot, event, msg)
        await guishi_baitan.finish()
    
    # 检查订单数量限制
    user_data = get_guishi_user_data(user_id)
    if len(user_data["baitan_orders"]) >= MAX_BAITAN_ORDERS:
        msg = f"您的摆摊订单已达上限({MAX_BAITAN_ORDERS})，请先收摊部分订单！"
        await handle_send(bot, event, msg)
        await guishi_baitan.finish()
    
    # 检查背包物品
    back_msg = sql_message.get_back_msg(user_id)
    goods_info = None
    for item in back_msg:
        if item['goods_name'] == goods_name:
            goods_info = item
            break
    
    if not goods_info:
        msg = f"请检查该道具 {goods_name} 是否在背包内！"
        await handle_send(bot, event, msg)
        await guishi_baitan.finish()
    
    # 检查物品总数量
    if goods_info['goods_num'] < quantity:
        msg = f"数量不足！背包仅有 {goods_info['goods_num']} 个 {goods_name}"
        await handle_send(bot, event, msg)
        await guishi_baitan.finish()
    
    # 获取物品类型
    goods_type = get_item_type_by_id(goods_info['goods_id'])
    if goods_type not in GUISHI_TYPES:
        msg = f"该物品类型不允许交易！允许类型：{', '.join(GUISHI_TYPES)}"
        await handle_send(bot, event, msg)
        await guishi_baitan.finish()
    
    # 从背包扣除物品
    sql_message.update_back_j(user_id, goods_info['goods_id'], num=quantity)
    
    # 生成订单ID
    user_data = get_guishi_user_data(user_id)
    existing_ids = set(user_data["baitan_orders"].keys())
    order_id = generate_guishi_id(existing_ids)
    
    # 计算收摊时间（次日8点）
    end_time = now.replace(hour=GUISHI_BAITAN_END_HOUR, minute=0, second=0, microsecond=0)
    if now.hour >= GUISHI_BAITAN_END_HOUR:  # 如果当前时间已经过了8点，则结束时间是明天8点
        end_time += timedelta(days=1)
    
    # 添加摆摊订单
    user_data["baitan_orders"][order_id] = {
        "item_id": goods_info['goods_id'],
        "item_name": goods_name,
        "price": price,
        "quantity": quantity,
        "sold": 0,
        "create_time": time.time(),
        "end_time": end_time.timestamp()
    }
    save_guishi_user_data(user_id, user_data)
    
    # 更新索引
    update_baitan_index(order_id, goods_name, user_id, "add")
    
    # 处理可能的即时交易
    transactions = await process_guishi_transactions(user_id)
    
    # 计算剩余时间
    time_left = end_time - now
    hours = time_left.seconds // 3600
    minutes = (time_left.seconds % 3600) // 60
    
    msg = f"成功摆摊！\n"
    msg += f"物品：{goods_name}\n"
    msg += f"价格：{number_to(price)} 灵石\n"
    msg += f"数量：{quantity}\n"
    msg += f"摊位ID：{order_id}\n"
    msg += f"⚠️ 请在 {hours}小时{minutes}分钟内收摊（{end_time.strftime('%m月%d日 %H:%M')}前）\n"
    msg += f"⚠️ 超时未收摊将自动清空摊位，物品不退还！"
    
    if transactions:
        msg += "\n☆------交易结果------☆\n"
        msg += "\n".join(transactions)
    
    await handle_send(bot, event, msg)
    await guishi_baitan.finish()

# 添加定时任务检查超时摊位
@clear_expired_baitan.scheduled_job("cron", hour=GUISHI_BAITAN_END_HOUR, minute=0)
async def clear_expired_baitan_():
    """每天8点自动清空未收摊的摊位"""
    logger.info("开始检查超时鬼市摊位...")
    
    # 获取所有用户数据
    expired_count = 0
    for user_file in GUISHI_DATA_PATH.glob("user_*.json"):
        try:
            user_id = user_file.stem.split("_")[1]
            user_data = json.loads(user_file.read_text(encoding="utf-8"))
            
            # 检查是否有超时摊位
            expired_orders = []
            for order_id, order in list(user_data["baitan_orders"].items()):
                if time.time() > order.get("end_time", 0):
                    expired_orders.append((order_id, order))
                    expired_count += 1
            
            # 移除超时订单并更新索引
            for order_id, order in expired_orders:
                del user_data["baitan_orders"][order_id]
                update_baitan_index(order_id, order["item_name"], user_id, "remove")
            
            if expired_orders:
                save_guishi_user_data(user_id, user_data)
                logger.info(f"已清空用户 {user_id} 的 {len(expired_orders)} 个超时摊位")
                
        except Exception as e:
            logger.error(f"处理用户 {user_file} 时出错: {e}")
    
    logger.info(f"共清空 {expired_count} 个超时摊位")

@guishi_shoutan.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def guishi_shoutan_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市收摊（支持无参数自动收摊已完成、指定ID或全部收摊）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await guishi_shoutan.finish()
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    user_data = get_guishi_user_data(user_id)
    if not user_data["baitan_orders"]:
        msg = "您当前没有摆摊订单！"
        await handle_send(bot, event, msg)
        await guishi_shoutan.finish()
    
    # 处理不同参数情况
    if not arg:  # 无参数，自动收摊已完成的
        completed_orders = []
        for order_id, order in list(user_data["baitan_orders"].items()):
            if order.get("sold", 0) >= order["quantity"]:
                completed_orders.append(order_id)
        
        if not completed_orders:
            msg = "没有已完成的摆摊订单可自动收摊！"
            await handle_send(bot, event, msg)
            await guishi_shoutan.finish()
        
        msg = "已自动收摊以下已完成订单：\n"
        for order_id in completed_orders:
            order = user_data["baitan_orders"][order_id]
            msg += f"ID:{order_id} {order['item_name']} x{order['quantity']}\n"
            del user_data["baitan_orders"][order_id]
            update_baitan_index(order_id, order["item_name"], user_id, "remove")
        
    elif arg == "全部":  # 收摊所有订单
        msg = "已收摊所有摆摊订单：\n"
        for order_id, order in list(user_data["baitan_orders"].items()):
            sold = order.get("sold", 0)
            remaining = order["quantity"] - sold
            
            # 退还未售出的物品
            if remaining > 0:
                sql_message.send_back(
                    user_id,
                    order["item_id"],
                    order["item_name"],
                    items.get_data_by_item_id(order["item_id"])["type"],
                    remaining
                )
            
            msg += f"ID:{order_id} {order['item_name']} 已售:{sold}/{order['quantity']}\n"
            del user_data["baitan_orders"][order_id]
            update_baitan_index(order_id, order["item_name"], user_id, "remove")
        
    else:  # 指定ID收摊
        order_id = arg
        if order_id not in user_data["baitan_orders"]:
            msg = f"未找到摆摊订单 {order_id}！"
            await handle_send(bot, event, msg)
            await guishi_shoutan.finish()
        
        order = user_data["baitan_orders"][order_id]
        sold = order.get("sold", 0)
        remaining = order["quantity"] - sold
        
        # 退还未售出的物品
        if remaining > 0:
            sql_message.send_back(
                user_id,
                order["item_id"],
                order["item_name"],
                items.get_data_by_item_id(order["item_id"])["type"],
                remaining
            )
        
        msg = f"已收摊订单 {order_id}：\n"
        msg += f"物品：{order['item_name']}\n"
        msg += f"价格：{number_to(order['price'])} 灵石\n"
        msg += f"已售：{sold}/{order['quantity']}\n"
        if remaining > 0:
            msg += f"退还 {remaining} 个到背包"
        
        del user_data["baitan_orders"][order_id]
        update_baitan_index(order_id, order["item_name"], user_id, "remove")
    
    save_guishi_user_data(user_id, user_data)
    await handle_send(bot, event, msg)
    await guishi_shoutan.finish()

@guishi_take_item.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def guishi_take_item_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """取出暂存在鬼市的物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await guishi_take_item.finish()
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    user_data = get_guishi_user_data(user_id)
    if not user_data["items"]:
        msg = "您的鬼市暂存中没有物品！"
        await handle_send(bot, event, msg)
        await guishi_take_item.finish()
    
    # 处理不同参数情况
    if not arg:  # 无参数，显示暂存物品列表
        msg = f"\n☆------鬼市暂存物品------☆"
        msg += "请使用'鬼市取物品 物品名'或'鬼市取物品 全部'取出物品\n\n"
        for item_id, item in user_data["items"].items():
            msg += f"{item['name']} x{item['quantity']}\n"
        await handle_send(bot, event, msg)
        await guishi_take_item.finish()
    
    if arg == "全部":  # 取出所有物品
        msg = "已从鬼市取出以下物品：\n"
        for item_id, item in list(user_data["items"].items()):
            sql_message.send_back(
                user_id,
                item_id,
                item["name"],
                item["type"],
                item["quantity"]
            )
            msg += f"{item['name']} x{item['quantity']}\n"
            del user_data["items"][item_id]
        
        save_guishi_user_data(user_id, user_data)
        await handle_send(bot, event, msg)
        await guishi_take_item.finish()
    
    # 取出指定物品
    matched_items = []
    for item_id, item in user_data["items"].items():
        if arg == item["name"]:
            matched_items.append((item_id, item))
    
    if not matched_items:
        msg = f"暂存中没有名为 {arg} 的物品！"
        await handle_send(bot, event, msg)
        await guishi_take_item.finish()
    
    # 处理多个同名物品情况（理论上不会出现，因为鬼市合并了同名物品）
    for item_id, item in matched_items:
        sql_message.send_back(
            user_id,
            item_id,
            item["name"],
            item["type"],
            item["quantity"]
        )
        del user_data["items"][item_id]
    
    save_guishi_user_data(user_id, user_data)
    msg = f"已从鬼市取出 {arg} x{matched_items[0][1]['quantity']}"
    await handle_send(bot, event, msg)
    await guishi_take_item.finish()

# 索引重建定时任务
@rebuild_guishi_index.scheduled_job("cron", hour=3)  # 每天凌晨3点重建索引
async def rebuild_guishi_index_():
    """重建鬼市索引"""
    logger.info("开始重建鬼市索引...")
    
    # 重建求购索引
    qiugou_index = {"by_item": {}, "by_user": {}}
    for user_file in GUISHI_DATA_PATH.glob("user_*.json"):
        user_id = user_file.stem.split("_")[1]
        user_data = json.loads(user_file.read_text(encoding="utf-8"))
        
        for order_id, order in user_data.get("qiugou_orders", {}).items():
            item_name = order["item_name"]
            if item_name not in qiugou_index["by_item"]:
                qiugou_index["by_item"][item_name] = []
            if user_id not in qiugou_index["by_item"][item_name]:
                qiugou_index["by_item"][item_name].append(user_id)
                
            if user_id not in qiugou_index["by_user"]:
                qiugou_index["by_user"][user_id] = []
            if order_id not in qiugou_index["by_user"][user_id]:
                qiugou_index["by_user"][user_id].append(order_id)
    
    save_guishi_index("qiugou", qiugou_index)
    
    # 重建摆摊索引
    baitan_index = {"by_item": {}, "by_user": {}}
    for user_file in GUISHI_DATA_PATH.glob("user_*.json"):
        user_id = user_file.stem.split("_")[1]
        user_data = json.loads(user_file.read_text(encoding="utf-8"))
        
        for order_id, order in user_data.get("baitan_orders", {}).items():
            item_name = order["item_name"]
            if item_name not in baitan_index["by_item"]:
                baitan_index["by_item"][item_name] = []
            if user_id not in baitan_index["by_item"][item_name]:
                baitan_index["by_item"][item_name].append(user_id)
                
            if user_id not in baitan_index["by_user"]:
                baitan_index["by_user"][user_id] = []
            if order_id not in baitan_index["by_user"][user_id]:
                baitan_index["by_user"][user_id].append(order_id)
    
    save_guishi_index("baitan", baitan_index)
    
    logger.info("鬼市索引重建完成")

@clear_all_guishi.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def clear_all_guishi_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """清空鬼市（管理员命令）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    msg = "正在清空全服鬼市，请稍候..."
    await handle_send(bot, event, msg)
    
    total_qiugou = 0
    total_baitan = 0
    
    # 遍历所有用户文件
    for user_file in GUISHI_DATA_PATH.glob("user_*.json"):
        try:
            user_id = user_file.stem.split("_")[1]
            user_data = json.loads(user_file.read_text(encoding="utf-8"))
            changed = False
            
            # 取消所有求购订单
            if user_data.get("qiugou_orders"):
                # 退还冻结的灵石
                refund_total = 0
                for order_id, order in list(user_data["qiugou_orders"].items()):
                    filled = order.get("filled", 0)
                    refund = (order["quantity"] - filled) * order["price"]
                    refund_total += refund
                    # 更新索引
                    update_qiugou_index(order_id, order["item_name"], user_id, "remove")
                
                user_data["stone"] += refund_total
                total_qiugou += len(user_data["qiugou_orders"])
                user_data["qiugou_orders"] = {}
                changed = True
            
            # 收摊所有摆摊订单
            if user_data.get("baitan_orders"):
                for order_id, order in list(user_data["baitan_orders"].items()):
                    # 退还未售出的物品
                    remaining = order["quantity"] - order.get("sold", 0)
                    if remaining > 0:
                        sql_message.send_back(
                            user_id,
                            order["item_id"],
                            order["item_name"],
                            items.get_data_by_item_id(order["item_id"])["type"],
                            remaining
                        )
                    # 更新索引
                    update_baitan_index(order_id, order["item_name"], user_id, "remove")
                
                total_baitan += len(user_data["baitan_orders"])
                user_data["baitan_orders"] = {}
                changed = True
            
            if changed:
                save_guishi_user_data(user_id, user_data)
                
        except Exception as e:
            logger.error(f"处理用户 {user_file} 时出错: {e}")
            continue
    
    # 清空索引
    save_guishi_index("qiugou", {"by_item": {}, "by_user": {}})
    save_guishi_index("baitan", {"by_item": {}, "by_user": {}})
    
    msg = f"鬼市已清空！\n"
    msg += f"共取消求购订单: {total_qiugou} 个\n"
    msg += f"共收摊摆摊订单: {total_baitan} 个\n"
    msg += "所有未完成的订单已处理，物品和灵石已退还"
    
    await handle_send(bot, event, msg)

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
    
    # 生成系统拍卖品 (随机3个)
    selected_system_items = random.sample(list(system_items.items()), min(3, len(system_items)))
    
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
    """刷新展示拍卖品（随机10个）"""
    current_auctions = get_current_auctions()
    if not current_auctions or "items" not in current_auctions:
        return False
    
    all_items = list(current_auctions["items"].values())
    if len(all_items) <= 10:
        display_items = all_items
    else:
        display_items = random.sample(all_items, 10)
    
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
    """参与竞拍（每次加价不得少于100万灵石）"""
    MIN_INCREMENT = 1000000  # 最低加价100万
    
    current_auctions = get_current_auctions()
    if not current_auctions or "items" not in current_auctions:
        return False, "拍卖当前未开启！"
    
    if auction_id not in current_auctions["items"]:
        return False, "无效的拍卖品ID！"
    
    item = current_auctions["items"][auction_id]
    
    # 检查最低加价
    required_min_bid = item["current_price"] + MIN_INCREMENT
    if bid_price < required_min_bid:
        return False, (
            f"每次加价不得少于100万灵石！\n"
            f"当前价: {number_to(item['current_price'])}\n"
            f"最低出价: {number_to(required_min_bid)}"
        )
    
    # 检查是否是自己的拍卖品
    #if str(user_id) == str(item["seller_id"]):
    #    return False, "不能竞拍自己上架的物品！"
    
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
    
    msg.append(f"\n下次最低加价: {number_to(bid_price + MIN_INCREMENT)}灵石")
    
    return True, "\n".join(msg)

@auction_view.handle()
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
            
            await handle_send(bot, event, "\n".join(msg))
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
                
                await handle_send(bot, event, "\n".join(msg))
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
        await handle_send(bot, event, msg)
        return
    
    items_list = list(display_auctions["items"].values())
    items_list.sort(key=lambda x: -x["current_price"])
    
    msg = [f"\n☆------拍卖物品列表------☆"]
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
        end_time = datetime.fromtimestamp(display_auctions["end_time"]).strftime("%Y-%m-%d %H:%M")
        msg.append(f"\n☆------历史拍卖记录------☆")
        msg.append(f"拍卖结束时间: {end_time}")
    elif auction_status["active"]:
        end_time = auction_status["end_time"].strftime("%H:%M")
        msg.append(f"\n拍卖进行中，预计{end_time}结束")
    else:
        msg.append("\n拍卖当前未开启")
    
    msg.append("\n输入【拍卖查看 ID】查看详情")
    await handle_send(bot, event, "\n".join(msg))

@auction_bid.handle()
async def auction_bid_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """参与拍卖竞拍"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        return
    
    args = args.extract_plain_text().split()
    if len(args) < 2:
        msg = "格式错误！正确格式：拍卖竞拍 [拍卖品ID] [出价]"
        await handle_send(bot, event, msg)
        return
    
    auction_id, price = args[0], args[1]
    try:
        price = int(price)
    except ValueError:
        msg = "出价必须是整数！"
        await handle_send(bot, event, msg)
        return
    
    success, result = place_bid(
        user_info['user_id'],
        user_info['user_name'],
        auction_id,
        price
    )
    await handle_send(bot, event, result)

@auction_add.handle()
async def auction_add_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """上架物品到拍卖（限制ITEM_TYPES类型）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
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
        await handle_send(bot, event, msg)
        return
    
    item_name, price = args[0], args[1]
    try:
        price = int(price)
    except ValueError:
        msg = "价格必须是整数！"
        await handle_send(bot, event, msg)
        return
    
    # 检查背包物品
    back_msg = sql_message.get_back_msg(user_info['user_id'])
    item_data = None
    for item in back_msg:
        if item['goods_name'] == item_name:
            if item['bind_num'] >= item['goods_num']:
                msg = "绑定物品不能上架！"
                await handle_send(bot, event, msg)
                return
            
            # 检查物品类型是否允许
            goods_type = get_item_type_by_id(item['goods_id'])
            if goods_type not in ITEM_TYPES:
                msg = f"该物品类型不允许拍卖！允许类型：{', '.join(ITEM_TYPES)}"
                await handle_send(bot, event, msg)
                return
                
            item_data = item
            break
    # 检查禁止交易的物品
    if str(item['goods_id']) in BANNED_ITEM_IDS:
        msg = f"物品 {item_name} 禁止拍卖！"
        await handle_send(bot, event, msg)
        return

    if not item_data:
        msg = f"背包中没有 {item_name} 或物品已绑定！"
        await handle_send(bot, event, msg)
        return
    
    # 从背包移除
    sql_message.update_back_j(user_info['user_id'], item_data['goods_id'], num=1)
    
    # 添加上架记录
    success, result = add_player_auction(
        user_info['user_id'],
        user_info['user_name'],
        item_data['goods_id'],
        item_name,
        price
    )
    await handle_send(bot, event, result)

@auction_remove.handle()
async def auction_remove_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """下架拍卖品（仅在非拍卖期间有效）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        return
    
    # 检查拍卖状态
    auction_status = get_auction_status()
    if auction_status["active"]:
        await handle_send(bot, event, "拍卖进行中时不能下架物品！")
        return
    
    item_name = args.extract_plain_text().strip()
    if not item_name:
        msg = "请输入要下架的物品名！"
        await handle_send(bot, event, msg)
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

@my_auction.handle(parameterless=[Cooldown(at_sender=False)])
async def my_auction_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看我上架的拍卖物品（不显示ID）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await my_auction.finish()
    
    user_id = user_info['user_id']
    player_auctions = get_player_auctions()
    
    # 获取当前用户上架的物品
    user_items = player_auctions.get(str(user_id), [])
    
    if not user_items:
        msg = "您当前没有上架任何拍卖物品！"
        await handle_send(bot, event, msg)
        await my_auction.finish()
    
    # 构建消息
    msg = [f"\n☆------我的拍卖物品------☆"]
    for item in user_items:
        msg.append(f"\n物品: {item['name']}")
        msg.append(f"起拍价: {number_to(item['price'])}灵石")
    
    msg.append("\n使用【拍卖下架 物品名】可以下架物品")
    
    await handle_send(bot, event, "\n".join(msg))
    await my_auction.finish()

@auction_info.handle()
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

@auction_start.handle()
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

@auction_end.handle()
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

@auction_lock.handle()
async def auction_lock_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """封闭拍卖（取消自动开启）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    update_schedule({"enabled": False})
    msg = "拍卖已封闭，将不再自动开启！"
    await handle_send(bot, event, msg)

@auction_unlock.handle()
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

@goods_re_root.handle(parameterless=[Cooldown(at_sender=False)])
async def goods_re_root_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """炼金"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await goods_re_root.finish()
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    if args is None:
        msg = "请输入要炼化的物品！"
        await handle_send(bot, event, msg)
        await goods_re_root.finish()
        
    # 判断输入是ID还是名称
    goods_id = None
    if args[0].isdigit():
        goods_id = int(args[0])
        item_info = items.get_data_by_item_id(goods_id)
        if not item_info:
            msg = f"ID {goods_id} 对应的物品不存在，请检查输入！"
            await handle_send(bot, event, msg)
            await goods_re_root.finish()
        goods_name = item_info['name']
    else:  # 视为物品名称
        goods_name = args[0]
    back_msg = sql_message.get_back_msg(user_id)  # 背包sql信息,list(back)
    if back_msg is None:
        msg = "道友的背包空空如也！"
        await handle_send(bot, event, msg)
        await goods_re_root.finish()
    in_flag = False  # 判断指令是否正确，道具是否在背包内
    goods_id = None
    goods_type = None
    goods_state = None
    goods_num = None
    for back in back_msg:
        if goods_name == back['goods_name']:
            in_flag = True
            goods_id = back['goods_id']
            goods_type = back['goods_type']
            goods_state = back['state']
            goods_num = back['goods_num']
            break
    if not in_flag:
        msg = f"请检查该道具 {goods_name} 是否在背包内！"
        await handle_send(bot, event, msg)
        await goods_re_root.finish()

    if goods_type == "装备" and int(goods_state) == 1 and int(goods_num) == 1:
        msg = f"装备：{goods_name}已经被道友装备在身，无法炼金！"
        await handle_send(bot, event, msg)
        await goods_re_root.finish()

    if get_item_msg_rank(goods_id) == 520:
        msg = "此类物品不支持！"
        await handle_send(bot, event, msg)
        await goods_re_root.finish()
    try:
        if 1 <= int(args[1]) <= int(goods_num):
            num = int(args[1])
    except:
            num = 1 
    price = int((convert_rank('江湖好手')[0] - 16) * 100000 - get_item_msg_rank(goods_id) * 100000) * num
    if price <= 0:
        msg = f"物品：{goods_name}炼金失败，凝聚{number_to(price)}枚灵石，记得通知晓楠！"
        await handle_send(bot, event, msg)
        await goods_re_root.finish()

    sql_message.update_back_j(user_id, goods_id, num=num)
    sql_message.update_ls(user_id, price, 1)
    msg = f"物品：{goods_name} 数量：{num} 炼金成功，凝聚{number_to(price)}枚灵石！"
    await handle_send(bot, event, msg)
    await goods_re_root.finish()

@fast_alchemy.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def fast_alchemy_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """快速炼金（支持装备/药材/全部类型 + 全部品阶，以及回血丹）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await fast_alchemy.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    # === 特殊处理回血丹 ===
    if len(args) > 0 and args[0] == "回血丹":
        back_msg = sql_message.get_back_msg(user_id)
        if not back_msg:
            msg = "💼 道友的背包空空如也！"
            await handle_send(bot, event, msg)
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
            await handle_send(bot, event, msg)
            await fast_alchemy.finish()
        
        # 执行炼金
        total_stone = 0
        results = []
        
        for elixir in elixirs:
            # 计算价格（基础rank - 物品rank）* 100000 + 100万
            base_rank = convert_rank('江湖好手')[0]
            item_rank = get_item_msg_rank(elixir['id'])
            price = max(MIN_PRICE, (base_rank - 16) * 100000 - item_rank * 100000 + 1000000)
            total_price = price * elixir['num']
            
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
        await handle_send(bot, event, msg)
        await fast_alchemy.finish()
    
    item_type = args[0]  # 物品类型
    rank_name = " ".join(args[1:]) if len(args) > 1 else "全部"  # 品阶
    
    if item_type not in type_mapping:
        msg = f"❌❌❌❌❌❌❌❌ 无效类型！可用类型：{', '.join(type_mapping.keys())}"
        await handle_send(bot, event, msg)
        await fast_alchemy.finish()
    
    if rank_name not in rank_map:
        msg = f"❌❌❌❌❌❌❌❌ 无效品阶！输入'品阶帮助'查看完整列表"
        await handle_send(bot, event, msg)
        await fast_alchemy.finish()
    
    # === 获取背包物品 ===
    back_msg = sql_message.get_back_msg(user_id)
    if not back_msg:
        msg = "💼 道友的背包空空如也！"
        await handle_send(bot, event, msg)
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
            available_num = item['goods_num'] - item['bind_num']
            if available_num > 0:
                items_to_alchemy.append({
                    'id': item['goods_id'],
                    'name': item['goods_name'],
                    'type': item['goods_type'],
                    'available_num': available_num,
                    'info': item_info
                })
    
    if not items_to_alchemy:
        msg = f"🔍 背包中没有符合条件的【{item_type}·{rank_name}】物品"
        await handle_send(bot, event, msg)
        await fast_alchemy.finish()
    
    # === 自动炼金逻辑 ===
    success_count = 0
    total_stone = 0
    result_msg = []
    
    for item in items_to_alchemy:
        if str(item['id']) in BANNED_ITEM_IDS:
            continue  # 跳过禁止交易的物品
        
        # 计算价格（基础rank - 物品rank）* 100000 + 100万
        base_rank = convert_rank('江湖好手')[0]
        item_rank = get_item_msg_rank(item['id'])
        price = max(MIN_PRICE, (base_rank - 16) * 100000 - item_rank * 100000 + 1000000)
        total_price = price * item['available_num']
        
        # 从背包扣除
        sql_message.update_back_j(user_id, item['id'], num=item['available_num'])
        
        # 增加灵石
        sql_message.update_ls(user_id, total_price, 1)
        
        success_count += item['available_num']
        total_stone += total_price
        result_msg.append(f"{item['name']} x{item['available_num']} → {number_to(total_price)}灵石")
    
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

@no_use_zb.handle(parameterless=[Cooldown(at_sender=False)])
async def no_use_zb_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """卸载物品（只支持装备）
    ["user_id", "goods_id", "goods_name", "goods_type", "goods_num", "create_time", "update_time",
    "remake", "day_num", "all_num", "action_time", "state"]
    """
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await no_use_zb.finish()
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()

    back_msg = sql_message.get_back_msg(user_id)  # 背包sql信息,list(back)
    if back_msg is None:
        msg = "道友的背包空空如也！"
        await handle_send(bot, event, msg)
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
        await handle_send(bot, event, msg)
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
            await handle_send(bot, event, msg)
            await no_use_zb.finish()
        else:
            msg = "装备没有被使用，无法卸载！"
            await handle_send(bot, event, msg)
            await no_use_zb.finish()
    else:
        msg = "目前只支持卸载装备！"
        await handle_send(bot, event, msg)
        await no_use_zb.finish()

@use.handle(parameterless=[Cooldown(at_sender=False)])
async def use_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """使用物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await use.finish()
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    if not args:
        msg = "请输入要使用的物品名称！"
        await handle_send(bot, event, msg)
        await use.finish()
    
    arg = args[0]  # 物品名称
    back_msg = sql_message.get_back_msg(user_id)  # 获取背包信息
    if back_msg is None:
        msg = "道友的背包空空如也！"
        await handle_send(bot, event, msg)
        await use.finish()
    
    # 检查物品是否在背包中
    in_flag = False
    goods_id = None
    goods_type = None
    goods_num = None
    for back in back_msg:
        if arg == back['goods_name']:
            in_flag = True
            goods_id = back['goods_id']
            goods_type = back['goods_type']
            goods_num = back['goods_num']
            break
    
    if not in_flag:
        msg = f"请检查该道具 {arg} 是否在背包内！"
        await handle_send(bot, event, msg)
        await use.finish()
    
    # 处理使用数量的通用逻辑
    num = 1
    try:
        if len(args) > 1 and 1 <= int(args[1]) <= int(goods_num):
            num = int(args[1])
        elif len(args) > 1 and int(args[1]) > int(goods_num):
            msg = f"道友背包中的{arg}数量不足，当前仅有{goods_num}个！"
            await handle_send(bot, event, msg)
            await use.finish()
    except ValueError:
        num = 1
    
    # 根据物品类型处理逻辑
    goods_info = items.get_data_by_item_id(goods_id)
    user_rank = convert_rank(user_info['level'])[0]
    rank_name_list = convert_rank("江湖好手")[1]
    if goods_info['rank'] == -5:
        goods_rank = 23
    else:
        goods_rank = int(goods_info['rank']) + 19
    if user_info['root_type'] in ["轮回道果", "真·轮回道果", "永恒道果", "命运道果"]:
        goods_rank = goods_rank + 3
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
             msg = f"道友实力不足使用{goods_info['name']}\n请提升至：{required_rank_name}"
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
            msg = f"成功装备 {arg}！"

    elif goods_type == "技能":
        user_buff_info = UserBuffDate(user_id).BuffInfo
        skill_info = goods_info
        skill_type = skill_info['item_type']
        if goods_rank < user_rank:
             msg = f"道友实力不足使用{goods_info['name']}\n请提升至：{required_rank_name}"
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
        msg = f"请发送:道具使用{goods_info['name']}"

    elif goods_type == "神物":
        user_info = sql_message.get_user_info_with_id(user_id)
        user_rank = convert_rank(user_info['level'])[0]
        goods_rank = goods_info['rank']
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
            msg = f"道友成功使用神物：{goods_name} {num} 个，修为增加 {exp} 点！"

    elif goods_type == "聚灵旗":
        msg = get_use_jlq_msg(user_id, goods_id)

    else:
        msg = "该类型物品调试中，未开启！"

    # 发送结果消息
    await handle_send(bot, event, msg)
    await use.finish()


@chakan_wupin.handle(parameterless=[Cooldown(at_sender=False)])
async def chakan_wupin_(
    bot: Bot, 
    event: GroupMessageEvent | PrivateMessageEvent, 
    args: Message = CommandArg()
):
    """查看修仙界物品（支持 类型+页码 或 类型 + 空格 + 页码）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    args_str = args.extract_plain_text().strip()
    
    # 支持的类型列表
    valid_types = ["功法", "辅修功法", "神通", "身法", "瞳术", "丹药", "合成丹药", "法器", "防具", "特殊物品"]
    
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
            msg = "请输入正确类型【功法|辅修功法|神通|身法|瞳术|丹药|合成丹药|法器|防具|特殊物品】！！！"
            await handle_send(bot, event, msg)
            await chakan_wupin.finish()
    
    # 获取物品数据
    if item_type == "特殊物品":
        # 特殊物品包括聚灵旗和特殊道具
        jlq_data = items.get_data_by_item_type(["聚灵旗"])
        special_data = items.get_data_by_item_type(["特殊物品"])
        item_data = {**jlq_data, **special_data}
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
                msg = f"ID：{item_id}\n名字：{name}\n效果：{item_info['desc']}\n修炼速度：{item_info['修炼速度'] * 100}%\n药材速度：{item_info['药材速度'] * 100}%"
            else:  # 特殊道具
                msg = f"ID：{item_id}\n名字：{name}\n效果：{item_info.get('desc', '十分神秘的东西，谁也不知道它的作用')}"
        else:  # 丹药、合成丹药
            rank = item_info.get('境界', '')
            desc = item_info.get('desc', '')
            msg = f"※{rank}丹药:{name}，效果：{desc}\n"
        msg_list.append(msg)
    
    # 分页处理
    title = f"修仙界物品列表-{item_type}"
    msgs = await handle_pagination(
        msg_list, 
        current_page, 
        title=title, 
        empty_msg=f"修仙界暂无{item_type}类物品"
    )
    
    if isinstance(msgs, str):  # 空提示消息
        await handle_send(bot, event, msgs)
    else:  # 分页消息列表
        await send_msg_handler(bot, event, title, bot.self_id, msgs)
    
    await chakan_wupin.finish()

@main_back.handle(parameterless=[Cooldown(cd_time=10, at_sender=False)])
async def main_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """我的背包"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await main_back.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    user_id = user_info['user_id']
    msg_list = get_user_main_back_msg(user_id)
    title = f"{user_info['user_name']}的背包"
    msgs = await handle_pagination(
        msg_list, 
        current_page,
        title=title,
        empty_msg="道友的背包空空如也！"
    )
    
    if isinstance(msgs, str):
        await handle_send(bot, event, msgs)
    else:
        await send_msg_handler(bot, event, '背包', bot.self_id, msgs)
    
    await main_back.finish()

@my_equipment.handle(parameterless=[Cooldown(cd_time=10, at_sender=False)])
async def my_equipment_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看我的装备及其详细信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await my_equipment.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    user_id = user_info['user_id']
    msg_list = get_user_equipment_msg(user_id)
    title = f"{user_info['user_name']}的装备"
    msgs = await handle_pagination(
        msg_list, 
        current_page,
        title=title,
        empty_msg="道友的背包中没有装备！"
    )
    
    if isinstance(msgs, str):
        await handle_send(bot, event, msgs)
    else:
        await send_msg_handler(bot, event, '我的装备', bot.self_id, msgs)
    
    await my_equipment.finish()

@yaocai_back.handle(parameterless=[Cooldown(cd_time=10, at_sender=False)])
async def yaocai_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """药材背包"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await yaocai_back.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    user_id = user_info['user_id']
    msg_list = get_user_yaocai_back_msg(user_id)
    title = f"{user_info['user_name']}的药材背包"
    msgs = await handle_pagination(
        msg_list, 
        current_page,
        title=title,
        empty_msg="道友的药材背包空空如也！"
    )
    
    if isinstance(msgs, str):
        await handle_send(bot, event, msgs)
    else:
        await send_msg_handler(bot, event, '药材背包', bot.self_id, msgs)
    
    await yaocai_back.finish()

@yaocai_detail_back.handle(parameterless=[Cooldown(cd_time=10, at_sender=False)])
async def yaocai_detail_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """药材背包详情版"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await yaocai_detail_back.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    user_id = user_info['user_id']
    msg_list = get_user_yaocai_detail_back_msg(user_id)
    title = f"{user_info['user_name']}的药材背包详情"
    msgs = await handle_pagination(
        msg_list, 
        current_page,
        title=title,
        empty_msg="道友的药材背包空空如也！"
    )
    
    if isinstance(msgs, str):
        await handle_send(bot, event, msgs)
    else:
        await send_msg_handler(bot, event, '药材背包详情', bot.self_id, msgs)
    
    await yaocai_detail_back.finish()

@danyao_back.handle(parameterless=[Cooldown(cd_time=10, at_sender=False)])
async def danyao_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """丹药背包"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await danyao_back.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    user_id = user_info['user_id']
    msg_list = get_user_danyao_back_msg(user_id)
    title = f"{user_info['user_name']}的丹药背包"
    msgs = await handle_pagination(
        msg_list, 
        current_page,
        title=title,
        empty_msg="道友的丹药背包空空如也！"
    )
    
    if isinstance(msgs, str):
        await handle_send(bot, event, msgs)
    else:
        await send_msg_handler(bot, event, '丹药背包', bot.self_id, msgs)
    
    await danyao_back.finish()

def reset_dict_num(dict_):
    i = 1
    temp_dict = {}
    for k, v in dict_.items():
        temp_dict[i] = v
        temp_dict[i]['编号'] = i
        i += 1
    return temp_dict


def get_user_auction_id_list():
    user_auctions = config['user_auctions']
    user_auction_id_list = []
    for auction in user_auctions:
        for k, v in auction.items():
            user_auction_id_list.append(v['id'])
    return user_auction_id_list

def get_auction_id_list():
    auctions = config['auctions']
    auction_id_list = []
    for k, v in auctions.items():
        auction_id_list.append(v['id'])
    return auction_id_list

def get_user_auction_price_by_id(id):
    user_auctions = config['user_auctions']
    user_auction_info = None
    for auction in user_auctions:
        for k, v in auction.items():
            if int(v['id']) == int(id):
                user_auction_info = v
                break
        if user_auction_info:
            break
    return user_auction_info

def get_auction_price_by_id(id):
    auctions = config['auctions']
    auction_info = None
    for k, v in auctions.items():
        if int(v['id']) == int(id):
            auction_info = v
            break
    return auction_info


def is_in_groups(event: GroupMessageEvent):
    return str(event.group_id) in groups


def get_auction_msg(auction_id):
    item_info = items.get_data_by_item_id(auction_id)
    _type = item_info['type']
    msg = None
    if _type == "装备":
        if item_info['item_type'] == "防具":
            msg = get_armor_info_msg(auction_id, item_info)
        if item_info['item_type'] == '法器':
            msg = get_weapon_info_msg(auction_id, item_info)

    if _type == "技能":
        if item_info['item_type'] == '神通':
            msg = f"{item_info['level']}-{item_info['name']}:\n"
            msg += f"效果：{get_sec_msg(item_info)}"
        if item_info['item_type'] == '功法':
            msg = f"{item_info['level']}-{item_info['name']}\n"
            msg += f"效果：{get_main_info_msg(auction_id)[1]}"
        if item_info['item_type'] == '辅修功法': #辅修功法10
            msg = f"{item_info['level']}-{item_info['name']}\n"
            msg += f"效果：{get_sub_info_msg(auction_id)[1]}"
            
    if _type == "神物":
        msg = f"{item_info['name']}\n"
        msg += f"效果：{item_info['desc']}"

    if _type == "丹药":
        msg = f"{item_info['name']}\n"
        msg += f"效果：{item_info['desc']}"

    return msg
