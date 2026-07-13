import asyncio
import re
import time
from datetime import datetime, timedelta
from nonebot import require
from .. import DRIVER
from ..on_compat import on_command
from ..adapter_compat import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
)
from ..xiuxian_utils.lay_out import assign_bot, assign_bot_group, Cooldown, CooldownIsolateLevel
from nonebot.log import logger
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import (
    check_user, get_msg_pic,
    send_msg_handler,
    Txt2Img, number_to, handle_send,
)
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, TradeDataManager, get_weapon_info_msg, get_armor_info_msg,
    get_sec_msg, get_main_info_msg, get_sub_info_msg, UserBuffDate
)
from ..xiuxian_back import type_mapping, rank_map, get_recover # 引用 xiuxian_back 中的类型和炼金函数
from ..xiuxian_back.back_util import check_equipment_use_msg, get_item_msg_rank # 引用 xiuxian_back.back_util
from ..xiuxian_config import XiuConfig, convert_rank
from . import auction_config
from .auction_config import * # 显式导入模块，避免冲突和混乱
from .trade_help import trade_help
from .trade_utils import (
    _trade_economy_context,
    get_item_trade_rank,
    get_trade_forbid_reason,
    record_trade_event,
)
from .auction_utils import (
    get_auction_status,
    _safe_auction_int,
    _format_auction_duration,
    _format_auction_datetime,
    _get_next_auction_start,
    _format_hot_auction_items,
    _format_recent_auction_deals,
    _format_user_auction_quota,
    bind_auction_repository,
)
from .auction_service import (
    bind_auction_service_dependencies,
    start_auction_process,
    end_auction_process,
    reconcile_auction_after_restart,
    place_auction_bid,
)
from .auction_jobs import run_auction_job
from .repository import TradeRepository
from .service import XianshiPurchaseService
from .guishi_stone_service import GuishiStoneService
from .auction_queue_service import AuctionQueueService
from .auction_session_service import AuctionSessionService
from ...paths import get_paths
from urllib.parse import quote

# 初始化全局组件
items = Items()
sql_message = XiuxianDateManage()
trade_manager = TradeDataManager()
xianshi_repository = TradeRepository(
    get_paths().game_db,
    max_goods_num=XiuConfig().max_goods_num,
)
xianshi_purchase_service = XianshiPurchaseService(xianshi_repository)
guishi_stone_service = GuishiStoneService(get_paths().game_db, get_paths().trade_db)
auction_queue_service = AuctionQueueService(
    get_paths().game_db,
    get_paths().trade_db,
    XiuConfig().max_goods_num,
)
auction_session_service = AuctionSessionService(
    get_paths().game_db, get_paths().trade_db
)
scheduler = require("nonebot_plugin_apscheduler").scheduler # 全局调度器，用于鬼市
auction_scheduler = require("nonebot_plugin_apscheduler").scheduler # 独立的拍卖调度器，避免冲突
bind_auction_repository(xianshi_repository)
bind_auction_service_dependencies(
    items=items,
    sql_message=sql_message,
    trade_manager=trade_manager,
    auction_repository=xianshi_repository,
    auction_session_service=auction_session_service,
)


@DRIVER.on_startup
async def initialize_xianshi_repository():
    xianshi_repository.initialize(get_paths().trade_db)


# === 全局常量配置 ===
ITEM_TYPES = ["技能", "装备", "药材", "丹药"] # 仙肆允许上架的物品类型
MIN_PRICE = 600000 # 仙肆最大上架价格
MAX_QUANTITY = 999 #仙肆最大上架数量

GUISHI_TYPES = ["药材", "装备", "技能"] # 鬼市允许交易的物品类型
GUISHI_BAITAN_START_HOUR = 20  # 鬼市摆摊开始时间（20点）
GUISHI_BAITAN_END_HOUR = 8     # 鬼市摆摊结束时间（次日8点）
GUISHI_AUTO_HOUR = 2           # 鬼市自动交易频率（每2小时）
GUISHI_MAX_QUANTITY = 10       # 鬼市单次最大交易数量（求购/摆摊）
MAX_QIUGOU_ORDERS = 10         # 每个用户最大求购订单数
MAX_BAITAN_ORDERS = 10         # 每个用户最大摆摊订单数


def _xianshi_removal_operation_id(event, listing_id):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"xianshi-remove:{event_id}:{listing_id}"
    return f"xianshi-remove:{listing_id}:{time.time_ns()}"


def _xianshi_clear_operation_id(event):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"xianshi-clear:{event_id}"
    return f"xianshi-clear:{time.time_ns()}"


def _xianshi_name_removal_operation_id(event, user_id, item_name, quantity):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"xianshi-remove-name:{event_id}:{user_id}:{item_name}:{quantity}"
    return f"xianshi-remove-name:{user_id}:{item_name}:{quantity}:{time.time_ns()}"


# === 仙肆命令 ===
xian_shop_add = on_command("仙肆上架", priority=5, block=True)
xianshi_auto_add = on_command("仙肆自动上架", priority=5, block=True)
xianshi_fast_add = on_command("仙肆快速上架", priority=5, block=True)
my_xian_shop = on_command("我的仙肆", priority=5, block=True)
xiuxian_shop_view = on_command("仙肆查看", priority=5, block=True)
xian_shop_off_all = on_command("清空仙肆", permission=SUPERUSER, priority=6, block=True)
xianshi_fast_buy = on_command("仙肆快速购买", priority=5, block=True)
xian_shop_remove = on_command("仙肆下架", priority=5, block=True)
xian_buy = on_command("仙肆购买", priority=5, block=True)
xian_shop_added_by_admin = on_command("系统仙肆上架", permission=SUPERUSER, priority=6, block=True)
xian_shop_remove_by_admin = on_command("系统仙肆下架", permission=SUPERUSER, priority=6, block=True)

# === 鬼市命令 ===
guishi_deposit = on_command("鬼市存灵石", priority=5, block=True)
guishi_withdraw = on_command("鬼市取灵石", priority=5, block=True)
guishi_take_item = on_command("鬼市取物品", priority=5, block=True)
guishi_info = on_command("鬼市信息", priority=5, block=True)
guishi_qiugou = on_command("鬼市求购", priority=5, block=True)
guishi_cancel_qiugou = on_command("鬼市取消求购", priority=5, block=True)
guishi_baitan = on_command("鬼市摆摊", priority=5, block=True)
guishi_shoutan = on_command("鬼市收摊", priority=5, block=True)
clear_all_guishi = on_command("清空鬼市", permission=SUPERUSER, priority=6, block=True)

# === 拍卖命令 ===
auction_view = on_command("拍卖查看", aliases={"查看拍卖"}, priority=5, block=True)
auction_bid = on_command("拍卖竞拍", aliases={"竞拍"}, priority=5, block=True)
auction_add = on_command("拍卖上架", priority=5, block=True)
auction_remove = on_command("拍卖下架", priority=5, block=True)
my_auction = on_command("我的拍卖", priority=5, block=True)
auction_info = on_command("拍卖信息", priority=5, block=True)
auction_activity = on_command("拍卖活动", aliases={"限时交易"}, priority=5, block=True)
auction_start = on_command("开启拍卖", permission=SUPERUSER, priority=6, block=True)
auction_end = on_command("结束拍卖", permission=SUPERUSER, priority=6, block=True)
auction_lock = on_command("封闭拍卖", permission=SUPERUSER, priority=6, block=True)
auction_unlock = on_command("解封拍卖", permission=SUPERUSER, priority=6, block=True)

AUCTION_ACTIVITY_BUTTONS = {
    "md_type": "拍卖",
    "k1": "拍卖查看",
    "v1": "拍卖查看",
    "k2": "拍卖竞拍",
    "v2": "拍卖竞拍",
    "k3": "拍卖上架",
    "v3": "拍卖上架",
    "k4": "我的拍卖",
    "v4": "我的拍卖",
}

# 获取仙肆物品的最低价格
def get_xianshi_min_price(item_name: str) -> int | None:
    """获取仙肆中指定物品的最低价格"""
    items_in_xianshi = xianshi_repository.get_xianshi_items(name=item_name)
    if not items_in_xianshi:
        return None
    return min(item['price'] for item in items_in_xianshi)


# 计算仙肆手续费
def get_fee_price(total_price: int) -> int:
    """根据总价计算仙肆手续费"""
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


def _xianshi_operation_id(event, listing_id, suffix=""):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if not event_id:
        return None
    extra = f":{suffix}" if suffix else ""
    return f"xianshi-buy:{event_id}:{listing_id}{extra}"


def _xianshi_listing_operation_id(event, seller_id, goods_id, price, quantity, suffix=""):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    extra = f":{suffix}" if suffix else ""
    if event_id:
        return f"xianshi-list:{event_id}:{seller_id}:{goods_id}:{price}:{quantity}{extra}"
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    return f"xianshi-list:{seller_id}:{goods_id}:{price}:{quantity}:{timestamp}{extra}"


def _guishi_stone_operation_id(event, operation_type, user_id):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"guishi-stone:{operation_type}:{event_id}:{user_id}"
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    return f"guishi-stone:{operation_type}:{user_id}:{timestamp}"


def _guishi_take_item_operation_id(event, user_id, goods_id):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"guishi-item-take:{event_id}:{user_id}:{goods_id}"
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    return f"guishi-item-take:{user_id}:{goods_id}:{timestamp}"


def _guishi_order_operation_id(event, order_type, user_id, item_id, price, quantity):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"guishi-order:{order_type}:{event_id}:{user_id}:{item_id}:{price}:{quantity}"
    return f"guishi-order:{order_type}:{user_id}:{item_id}:{price}:{quantity}:{time.time_ns()}"


def _auction_queue_operation_id(event, action, user_id, item_id):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"auction-queue:{action}:{event_id}:{user_id}:{item_id}"
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    return f"auction-queue:{action}:{user_id}:{item_id}:{timestamp}"


def _auction_session_operation_id(event, action):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"auction-session:{action}:{event_id}"
    return f"auction-session:{action}:{time.time_ns()}"


def buy_xianshi_item_safely(
    buyer_id,
    item_to_buy,
    quantity_to_buy,
    *,
    operation_id=None,
):
    """通过同库事务完成扣款、减库存、发货、卖家入账和幂等记录。"""
    result = xianshi_purchase_service.purchase(
        buyer_id,
        item_to_buy["id"],
        quantity_to_buy,
        operation_id=operation_id,
    )
    messages = {
        "listing_missing": f"库存不足！{item_to_buy['name']} 已被其他道友购买。",
        "self_purchase": "不能购买自己上架的物品！",
        "stock_insufficient": f"库存不足！仙肆中没有足够的 {item_to_buy['name']}",
        "buyer_missing": "未找到购买者修仙数据！",
        "seller_missing": "卖家修仙数据不存在，购买已取消！",
        "stone_insufficient": f"灵石不足！需要 {number_to(result.total_cost)} 灵石",
        "inventory_full": f"背包中的 {item_to_buy['name']} 已达到数量上限！",
    }
    if not result.succeeded:
        return False, messages[result.status], None

    if result.applied:
        trace_id = operation_id or f"xianshi:{result.listing_id}:{buyer_id}"
        common_detail = {
            "xianshi_id": result.listing_id,
            "goods_id": result.goods_id,
            "item_name": result.name,
            "seller_id": result.seller_id,
            "quantity": result.quantity,
            "total_cost": result.total_cost,
        }
        sql_message._safe_log_economy_context(
            _trade_economy_context("xianshi_buy_cost", trace_id, **common_detail),
            user_id=str(buyer_id),
            stone_delta=-result.total_cost,
        )
        sql_message._safe_log_economy_context(
            _trade_economy_context("xianshi_buy_item", trace_id, **common_detail),
            user_id=str(buyer_id),
            item_delta=[
                {
                    "id": result.goods_id,
                    "name": result.name,
                    "type": result.goods_type,
                    "amount": result.quantity,
                    "bind_flag": 1,
                }
            ],
        )
        if result.seller_id != "0":
            sql_message._safe_log_economy_context(
                _trade_economy_context(
                    "xianshi_seller_income",
                    trace_id,
                    buyer_id=str(buyer_id),
                    **common_detail,
                ),
                user_id=result.seller_id,
                stone_delta=result.total_cost,
            )

    msg = (
        f"成功购买 {result.name} x{result.quantity}\n花费 {number_to(result.total_cost)} 灵石"
        if result.applied
        else "该购买请求已经处理，无需重复提交。"
    )
    trade_info = {
        "seller_id": result.seller_id,
        "quantity": result.quantity,
        "total_cost": result.total_cost,
        "applied": result.applied,
    }
    return True, msg, trade_info


# --- 仙肆命令处理 ---

@xian_shop_add.handle(parameterless=[Cooldown(cd_time=0)])
async def xian_shop_add_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆上架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await xian_shop_add.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 2:
        msg = "请输入正确指令！格式：仙肆上架 物品名称 价格 [数量]"
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1="仙肆上架", k2="查看", v2="仙肆查看", k3="购买", v3="仙肆购买")
        await xian_shop_add.finish()
    
    item_name = args[0]
    try:
        price = max(int(args[1]), MIN_PRICE) # 价格不得低于最低限额
        quantity = int(args[2]) if len(args) > 2 else 1
        quantity = max(1, min(quantity, MAX_QUANTITY)) # 数量限制在1到最大值之间
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1=f"仙肆上架 {item_name}", k2="查看", v2="仙肆查看", k3="购买", v3="仙肆购买")
        await xian_shop_add.finish()

    # 检查背包物品是否存在
    goods_id, goods_info = items.get_data_by_item_name(item_name)
    if not goods_id:
        msg = f"物品 {item_name} 不存在，请检查名称是否正确！"
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1="仙肆上架", k2="查看", v2="仙肆查看", k3="购买", v3="仙肆购买")
        return
    
    # 检查用户背包中可交易的物品数量
    goods_num = sql_message.goods_num(str(user_info['user_id']), goods_id, num_type='trade')
    if goods_num <= 0:
        msg = f"背包中没有足够的 {item_name} 用于交易！"
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1="仙肆上架", k2="查看", v2="仙肆查看", k3="购买", v3="仙肆购买")
        return
    
    # 检查物品类型是否允许上架
    if goods_info['type'] not in ITEM_TYPES:
        msg = f"该物品类型不允许在仙肆上架！允许类型：{', '.join(ITEM_TYPES)}"
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1="仙肆上架", k2="查看", v2="仙肆查看", k3="购买", v3="仙肆购买")
        return
    
    forbid_reason = get_trade_forbid_reason(goods_id, goods_info)
    if forbid_reason:
        msg = forbid_reason
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1="仙肆上架", k2="查看", v2="仙肆查看", k3="购买", v3="仙肆购买")
        return
        
    if quantity > goods_num: # 实际可上架数量不能超过背包现有数量
        quantity = goods_num
    
    total_fee = get_fee_price(price * quantity) # 计算总手续费
    if user_info['stone'] < total_fee:
        msg = f"灵石不足支付手续费！需要{number_to(total_fee)}灵石，当前拥有{number_to(user_info['stone'])}灵石"
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1=f"仙肆上架 {item_name} {price}", k2="查看", v2=f"仙肆查看 {goods_info['type']}", k3="购买", v3="仙肆购买")
        await xian_shop_add.finish()

    operation_id = _xianshi_listing_operation_id(
        event, user_id, goods_id, price, quantity
    )
    result = xianshi_repository.add_xianshi_items(
        operation_id, user_id, goods_id, item_name, goods_info['type'], price,
        quantity, fee_charged=total_fee, consume_assets=True,
    )
    if result.status in {"stone_insufficient", "stock_insufficient"}:
        msg = f"灵石或可交易的 {item_name} 数量不足，上架失败！"
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1=f"仙肆上架 {item_name} {price}", k2="查看", v2=f"仙肆查看 {goods_info['type']}", k3="购买", v3="仙肆购买")
        await xian_shop_add.finish()
    if not result.succeeded:
        raise RuntimeError(f"unexpected xianshi listing status: {result.status}")
    success_count = result.listed_quantity
    msg = f"\n成功上架 {item_name} x{success_count} 到仙肆！\n"
    msg += f"单价: {number_to(price)} 灵石\n"
    msg += f"总手续费: {number_to(result.fee_charged)} 灵石"
    if result.applied:
        record_trade_event(
            user_id,
            "仙肆上架",
            f"上架{item_name}x{success_count}，单价{number_to(price)}灵石，手续费{number_to(result.fee_charged)}灵石",
            {"仙肆上架次数": 1, "仙肆上架数量": success_count, "仙肆手续费消耗": result.fee_charged}
        )
    await handle_send(bot, event, msg, md_type="交易", k1="上架", v1=f"仙肆上架 {item_name} {price}", k2="查看", v2=f"仙肆查看 {goods_info['type']}", k3="购买", v3="仙肆购买")
    await xian_shop_add.finish()

@xianshi_auto_add.handle(parameterless=[Cooldown(cd_time=0, stamina_cost=30)])
async def xianshi_auto_add_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆自动上架（按类型和品阶批量上架）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await xianshi_auto_add.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    # 指令格式检查
    if len(args) < 2:
        msg = "指令格式：仙肆自动上架 [类型] [品阶] [数量]\n" \
              "▶ 类型：装备|法器|防具|药材|技能|全部\n" \
              "▶ 品阶：全部|人阶|黄阶|...|上品通天法器（输入'品阶帮助'查看完整列表）\n" \
              "▶ 数量：可选，默认1个，最多10个"
        sql_message.update_user_stamina(user_id, 30, 1) # 恢复体力
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1="仙肆自动上架", k2="查看", v2="仙肆查看", k3="品阶", v3="品阶帮助")
        await xianshi_auto_add.finish()
    
    item_type = args[0]
    # 品阶可能包含空格，所以需要特殊处理
    if args[-1].isdigit(): # 如果最后一个参数是数字，则为数量
        rank_name = " ".join(args[1:-1])
        quantity = int(args[-1])
    else: # 否则，最后一个参数是品阶的一部分
        rank_name = " ".join(args[1:])
        quantity = 1 # 默认数量1
    
    quantity = max(1, min(quantity, MAX_QUANTITY))
    
    if item_type not in type_mapping:
        msg = f"❌ 无效类型！可用类型：{', '.join(type_mapping.keys())}"
        sql_message.update_user_stamina(user_id, 30, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1="仙肆自动上架", k2="查看", v2="仙肆查看", k3="购买", v3="仙肆购买")
        await xianshi_auto_add.finish()
    
    if rank_name not in rank_map:
        msg = f"❌ 无效品阶！输入'品阶帮助'查看完整列表"
        sql_message.update_user_stamina(user_id, 30, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1=f"仙肆自动上架 {item_type}", k2="查看", v2="仙肆查看", k3="购买", v3="仙肆购买")
        await xianshi_auto_add.finish()

    # 获取背包物品
    back_msg = sql_message.get_back_msg(user_id)
    if not back_msg:
        msg = "💼 道友的背包空空如也！"
        sql_message.update_user_stamina(user_id, 30, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1="仙肆自动上架", k2="查看", v2="仙肆查看", k3="购买", v3="仙肆购买")
        await xianshi_auto_add.finish()
    
    # 筛选符合类型和品阶的物品
    target_types = type_mapping[item_type]
    target_ranks = rank_map[rank_name]
    
    items_to_add = []
    for item in back_msg:
        item_info = items.get_data_by_item_id(item['goods_id'])
        if not item_info:
            continue
            
        type_match = (
            item['goods_type'] in target_types or # 直接匹配goods_type
            item_info.get('item_type', '') in target_types # 匹配item_info中的item_type
        )
        
        rank_match = item_info.get('level', '') in target_ranks # 匹配品阶
        
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
                # 非装备物品，可上架数量 = 总数量 - 绑定数量
                available_num = item['goods_num'] - item['bind_num']
            
            available_num = max(0, available_num) # 确保可用数量不为负
            
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
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1=f"仙肆自动上架 {item_type} {rank_name}", k2="查看", v2=f"仙肆查看 {item_type}", k3="购买", v3="仙肆购买")
        await xianshi_auto_add.finish()
    
    # === 批量处理逻辑 ===
    processed_items_summary = []
    total_fees_to_deduct = 0
    
    for item in items_to_add:
        if get_trade_forbid_reason(item['id'], item['info']): # 跳过禁止交易的物品
            continue

        min_price_in_xianshi = get_xianshi_min_price(item['name'])
        
        if min_price_in_xianshi is None: # 如果仙肆中没有该物品，则根据炼金价+100万设定价格
            price_for_item = int(get_recover(item['id'], 1) + 1000000)
        else: # 否则，以仙肆最低价出售
            price_for_item = min_price_in_xianshi
        
        actual_quantity_to_add = min(quantity, item['available_num']) # 实际要上架的数量
        
        total_price_for_item = price_for_item * actual_quantity_to_add
        fee_for_item = get_fee_price(total_price_for_item)
        
        processed_items_summary.append({
            'id': item['id'],
            'name': item['name'],
            'type': item['type'],
            'price': price_for_item,
            'quantity': actual_quantity_to_add,
            'fee': fee_for_item
        })
        total_fees_to_deduct += fee_for_item

    if not processed_items_summary:
        msg = "符合条件的物品均为不可交易的珍贵物品，或没有可交易物品。"
        sql_message.update_user_stamina(user_id, 30, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1=f"仙肆自动上架 {item_type} {rank_name}", k2="查看", v2=f"仙肆查看 {item_type}", k3="购买", v3="仙肆购买")
        await xianshi_auto_add.finish()
    
    if user_info['stone'] < total_fees_to_deduct:
        msg = f"灵石不足支付总手续费！需要{number_to(total_fees_to_deduct)}灵石，当前拥有{number_to(user_info['stone'])}灵石"
        sql_message.update_user_stamina(user_id, 30, 1) # 恢复体力
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1=f"仙肆自动上架 {item_type} {rank_name}", k2="查看", v2=f"仙肆查看 {item_type}", k3="购买", v3="仙肆购买")
        await xianshi_auto_add.finish()

    listing_plan = [
        {
            "goods_id": item_summary["id"],
            "name": item_summary["name"],
            "goods_type": item_summary["type"],
            "price": item_summary["price"],
            "quantity": item_summary["quantity"],
        }
        for item_summary in processed_items_summary
    ]
    operation_id = _xianshi_listing_operation_id(
        event, user_id, 0, total_fees_to_deduct, len(listing_plan), "auto"
    )
    result = xianshi_repository.add_xianshi_plan_items(
        operation_id, user_id, listing_plan,
        fee_charged=total_fees_to_deduct, consume_assets=True,
    )
    if result.status in {"stone_insufficient", "stock_insufficient"}:
        msg = "灵石或可交易物品数量不足，自动上架失败！"
        sql_message.update_user_stamina(user_id, 30, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1=f"仙肆自动上架 {item_type} {rank_name}", k2="查看", v2=f"仙肆查看 {item_type}", k3="购买", v3="仙肆购买")
        await xianshi_auto_add.finish()
    if not result.succeeded:
        raise RuntimeError(f"unexpected xianshi plan listing status: {result.status}")
    
    # 限制显示数量，防止消息过长
    result_messages = []
    for item_summary in processed_items_summary:
        for _ in range(item_summary['quantity']):
            result_messages.append(
                f"{item_summary['name']} x1 - 单价:{number_to(item_summary['price'])}"
            )
    display_msg_lines = result_messages[:20]
    if len(result_messages) > 20:
        display_msg_lines.append(f"...等共{len(result_messages)}件物品")
    
    msg = f"\n✨ 成功上架 {result.listed_quantity} 件物品\n"
    msg += f"💎 总手续费: {number_to(result.fee_charged)}灵石"
    if result.applied:
        record_trade_event(
            user_id,
            "仙肆自动上架",
            f"按{item_type}/{rank_name}上架{result.listed_quantity}件物品，手续费{number_to(result.fee_charged)}灵石",
            {"仙肆上架次数": 1, "仙肆上架数量": result.listed_quantity, "仙肆手续费消耗": result.fee_charged}
        )
    
    await send_msg_handler(bot, event, '仙肆上架', bot.self_id, display_msg_lines, title=f"【仙肆自动上架 {item_type} {rank_name}】", page_param=msg)
    await xianshi_auto_add.finish()

@xianshi_fast_add.handle(parameterless=[Cooldown(cd_time=0, stamina_cost=10)])
async def xianshi_fast_add_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆快速上架（按物品名快速上架）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await xianshi_fast_add.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "指令格式：仙肆快速上架 物品名 [价格]\n" \
              "▶ 价格：可选，不填则自动匹配仙肆最低价\n" \
              "▶ 数量：固定为10个（或背包中全部数量）"
        sql_message.update_user_stamina(user_id, 10, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1="仙肆快速上架", k2="查看", v2="仙肆查看", k3="购买", v3="仙肆购买")
        await xianshi_fast_add.finish()
    
    item_name = args[0]
    # 尝试解析价格参数
    try:
        price = int(args[1]) if len(args) > 1 else None
    except ValueError:
        msg = "请输入有效的价格！"
        sql_message.update_user_stamina(user_id, 10, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1=f"仙肆快速上架 {item_name}", k2="查看", v2="仙肆查看", k3="购买", v3="仙肆购买")
        await xianshi_fast_add.finish()
    
    # 检查背包物品
    goods_id, goods_info = items.get_data_by_item_name(item_name)
    if not goods_id:
        msg = f"物品 {item_name} 不存在，请检查名称是否正确！"
        sql_message.update_user_stamina(user_id, 10, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1="仙肆快速上架", k2="查看", v2="仙肆查看", k3="购买", v3="仙肆购买")
        return
    
    goods_num = sql_message.goods_num(str(user_info['user_id']), goods_id, num_type='trade')
    if goods_num <= 0:
        msg = f"背包中没有足够的 {item_name} 用于交易！"
        sql_message.update_user_stamina(user_id, 10, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1="仙肆快速上架", k2="查看", v2="仙肆查看", k3="购买", v3="仙肆购买")
        return
    
    # 检查物品类型是否允许
    if goods_info['type'] not in ITEM_TYPES:
        msg = f"该物品类型不允许交易！允许类型：{', '.join(ITEM_TYPES)}"
        sql_message.update_user_stamina(user_id, 10, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1="仙肆快速上架", k2="查看", v2="仙肆查看", k3="购买", v3="仙肆购买")
        return
    
    forbid_reason = get_trade_forbid_reason(goods_id, goods_info)
    if forbid_reason:
        msg = forbid_reason
        sql_message.update_user_stamina(user_id, 10, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1="仙肆快速上架", k2="查看", v2="仙肆查看", k3="购买", v3="仙肆购买")
        return

    # 检查可上架数量（固定为10或背包中全部数量）
    quantity = min(10, goods_num)  # 最多上架10个
    
    if quantity <= 0:
        msg = f"可上架数量不足！"
        sql_message.update_user_stamina(user_id, 10, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1="仙肆快速上架", k2="查看", v2="仙肆查看", k3="购买", v3="仙肆购买")
        await xianshi_fast_add.finish()

    # 获取价格（如果用户未指定价格）
    if price is None:
        min_price_in_xianshi = get_xianshi_min_price(item_name)
        
        if min_price_in_xianshi is None: # 如果仙肆中没有该物品，则根据炼金价+100万设定价格
            price = int(get_recover(goods_id, 1) + 1000000)
        else: # 否则，以仙肆最低价出售
            price = min_price_in_xianshi
    else:
        price = max(price, MIN_PRICE) # 确保用户指定的价格不低于系统最低价
    
    # 计算总手续费
    total_price = price * quantity
    single_fee = get_fee_price(total_price)
    
    if user_info['stone'] < single_fee:
        msg = f"灵石不足支付手续费！需要{number_to(single_fee)}灵石，当前拥有{number_to(user_info['stone'])}灵石"
        sql_message.update_user_stamina(user_id, 10, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1=f"仙肆快速上架 {item_name} {price}", k2="查看", v2=f"仙肆查看 {goods_info['type']}", k3="购买", v3="仙肆购买")
        await xianshi_fast_add.finish()

    operation_id = _xianshi_listing_operation_id(
        event, user_id, goods_id, price, quantity, "fast"
    )
    result = xianshi_repository.add_xianshi_items(
        operation_id, user_id, goods_id, item_name, goods_info['type'], price,
        quantity, fee_charged=single_fee, consume_assets=True,
    )
    if result.status in {"stone_insufficient", "stock_insufficient"}:
        msg = f"灵石或可交易的 {item_name} 数量不足，上架失败！"
        sql_message.update_user_stamina(user_id, 10, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="上架", v1=f"仙肆快速上架 {item_name} {price}", k2="查看", v2=f"仙肆查看 {goods_info['type']}", k3="购买", v3="仙肆购买")
        await xianshi_fast_add.finish()
    if not result.succeeded:
        raise RuntimeError(f"unexpected xianshi fast listing status: {result.status}")
    success_count = result.listed_quantity
    
    msg = f"\n成功上架 {item_name} x{success_count} 到仙肆！\n"
    msg += f"单价: {number_to(price)} 灵石\n"
    msg += f"总价: {number_to(total_price)} 灵石\n"
    msg += f"手续费: {number_to(result.fee_charged)} 灵石"
    if result.applied:
        record_trade_event(
            user_id,
            "仙肆快速上架",
            f"快速上架{item_name}x{success_count}，单价{number_to(price)}灵石，手续费{number_to(result.fee_charged)}灵石",
            {"仙肆上架次数": 1, "仙肆上架数量": success_count, "仙肆手续费消耗": result.fee_charged}
        )
    
    await handle_send(bot, event, msg, md_type="交易", k1="上架", v1=f"仙肆快速上架 {item_name} {price}", k2="查看", v2=f"仙肆查看 {goods_info['type']}", k3="购买", v3="仙肆购买")
    await xianshi_fast_add.finish()

@xiuxian_shop_view.handle(parameterless=[Cooldown(cd_time=0)])
async def xiuxian_shop_view_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆查看"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await xiuxian_shop_view.finish()
    
    # 解析参数
    args_str = args.extract_plain_text().strip()
    
    # 情况1：无参数 - 显示可用类型
    if not args_str:
        msg = f"请指定查看类型：【{', '.join(ITEM_TYPES)}】"
        await handle_send(bot, event, msg, md_type="交易", k1="技能", v1="仙肆查看 技能", k2="装备", v2="仙肆查看 装备", k3="药材", v3="仙肆查看 药材", k4="丹药", v4="仙肆查看 丹药")
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
        if len(parts) > 0 and parts[0] in ITEM_TYPES:
            item_type = parts[0]
            if len(parts) > 1 and parts[1].isdigit():
                current_page = int(parts[1])
    
    # 检查类型有效性
    if item_type not in ITEM_TYPES:
        msg = f"无效类型！可用类型：【{', '.join(ITEM_TYPES)}】"
        await handle_send(bot, event, msg, md_type="交易", k1="技能", v1="仙肆查看 技能", k2="装备", v2="仙肆查看 装备", k3="药材", v3="仙肆查看 药材", k4="丹药", v4="仙肆查看 丹药")
        await xiuxian_shop_view.finish()
    
    type_items = xianshi_repository.get_xianshi_items(type=item_type)
    
    if not type_items:
        msg = f"仙肆中暂无{item_type}类物品！"
        await handle_send(bot, event, msg, md_type="交易", k1="查看", v1=f"仙肆查看 {item_type}", k2="我的", v2="我的仙肆", k3="购买", v3="仙肆购买")
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
    
    # 合并系统物品和用户物品，并按名称排序
    items_list = sorted(system_items + list(user_items.values()), key=lambda x: x['name'])
    
    # 分页处理
    per_page = 10
    total_pages = (len(items_list) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    if current_page > total_pages:
        msg = f"页码超出范围，最多{total_pages}页！"
        await handle_send(bot, event, msg, md_type="交易", k1="查看", v1=f"仙肆查看 {item_type} {total_pages}", k2="我的", v2="我的仙肆", k3="购买", v3="仙肆购买")
        await xiuxian_shop_view.finish()
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = items_list[start_idx:end_idx]

    # 构建消息内容
    if XiuConfig().markdown_status:
        # 构建 markdown 文本
        lines = [f"【仙肆 {item_type}】", ""]

        for item in paged_items:
            price_str = number_to(item['price'])
            xianshi_id = str(item["id"])

            # 交互命令，建议url编码
            cmd = quote(f"仙肆购买 {xianshi_id}")
            id_md = f"[购买](mqqapi://aio/inlinecmd?command={cmd}&enter=false&reply=false)"
            name_cmd = quote(f"查看效果 {item['name']}")
            name_md = f"[{item['name']}](mqqapi://aio/inlinecmd?command={name_cmd}&enter=false&reply=false)"
            line = f"> - {name_md} {price_str}灵石 {id_md}"
            if item['quantity'] == -1:
                line += " ｜ 不限量"
            elif item['quantity'] > 1:
                line += f" ｜ 剩余:{item['quantity']}"
            lines.append(line)
            lines.append("\r")

        lines.append("")
        lines.append(f"第 {current_page}/{total_pages} 页")
        lines.append(
            f"[下一页](mqqapi://aio/inlinecmd?command={quote(f'仙肆查看{item_type} {current_page + 1}')}&enter=false&reply=false)"
        )

        md_text = "\r".join(lines)  # QQ md更建议 \r
        fallback_lines = [f"【仙肆 {item_type}】", ""]
        for item in paged_items:
            price_str = number_to(item['price'])
            fallback_line = f"- {item['name']} {price_str}灵石\n  ID:{item['id']}"
            if item['quantity'] == -1:
                fallback_line += " 不限量"
            elif item['quantity'] > 1:
                fallback_line += f" 剩余:{item['quantity']}"
            fallback_lines.append(fallback_line)
        fallback_lines.append("")
        fallback_lines.append(f"第 {current_page}/{total_pages} 页")
        if current_page < total_pages:
            fallback_lines.append(f"下一页：仙肆查看{item_type} {current_page + 1}")
        fallback_text = "\n".join(fallback_lines)
        await handle_send(bot, event, md_text, native_markdown=True, fallback_msg=fallback_text)
        await xiuxian_shop_view.finish()

    title = f"【仙肆 {item_type}】"
    msg_list = []
    for item in paged_items:
        price_str = number_to(item['price'])
        msg_line = f"\n{item['name']} {price_str}灵石 \nID:{item['id']}"
        
        # 处理数量显示
        if item['quantity'] == -1: # 系统物品不限量
            msg_line += f" 不限量"
        elif item['quantity'] > 1: # 用户物品的剩余数量
            msg_line += f" 剩余:{item['quantity']}"
        
        msg_list.append(msg_line)
    
    pages_info = f"\n第 {current_page}/{total_pages} 页"
    msg_list.append(pages_info)

    page = ["翻页", f"仙肆查看{item_type} {current_page + 1}", "我的", "我的仙肆", "购买", "仙肆购买", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '仙肆查看', bot.self_id, msg_list, title=title, page=page)
    await xiuxian_shop_view.finish()

@my_xian_shop.handle(parameterless=[Cooldown(cd_time=0)])
async def my_xian_shop_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """我的仙肆 - 查看自己上架的物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await my_xian_shop.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except ValueError:
        current_page = 1
    
    user_id = user_info['user_id']
    
    user_items = xianshi_repository.get_xianshi_items(user_id=user_id)

    if not user_items:
        msg = "您在仙肆中没有上架任何物品！"
        await handle_send(bot, event, msg, md_type="交易", k1="查看", v1="仙肆查看", k2="我的", v2="我的仙肆", k3="购买", v3="仙肆购买")
        await my_xian_shop.finish()
    
    # 按名称排序
    user_items.sort(key=lambda x: x['name'])
    
    # 分页处理
    per_page = 20
    total_pages = (len(user_items) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = user_items[start_idx:end_idx]
    
    title = f"【{user_info['user_name']}的仙肆物品】"
    msg_list = []
    for item in paged_items:
        price_str = number_to(item['price'])
        msg_line = f"{item['name']} {price_str}灵石"
        if item['quantity'] > 1:
            msg_line += f" x{item['quantity']}"
        msg_list.append(msg_line)
    
    msg_list.append(f"\n第 {current_page}/{total_pages} 页")
    page = ["翻页", f"我的仙肆 {current_page + 1}", "下架", "仙肆下架", "查看", "仙肆查看", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '我的仙肆', bot.self_id, msg_list, title=title, page=page)
    await my_xian_shop.finish()

@xian_shop_remove.handle(parameterless=[Cooldown(cd_time=0)])
async def xian_shop_remove_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆下架 - 按物品名下架，可选数量"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await xian_shop_remove.finish()

    user_id = user_info['user_id']
    parts = args.extract_plain_text().split()
    if not parts:
        msg = "请输入正确指令！格式：仙肆下架 物品名 [数量]"
        await handle_send(bot, event, msg, md_type="交易", k1="下架", v1="仙肆下架", k2="上架", v2="仙肆上架", k3="我的", v3="我的仙肆")
        await xian_shop_remove.finish()

    if len(parts) >= 2 and parts[-1].isdigit():
        quantity = int(parts[-1])
        item_name = " ".join(parts[:-1])
    else:
        item_name = " ".join(parts)
        quantity = 1

    if not item_name.strip():
        msg = "请输入要下架的物品名！"
        await handle_send(bot, event, msg, md_type="交易", k1="下架", v1="仙肆下架", k2="上架", v2="仙肆上架", k3="我的", v3="我的仙肆")
        await xian_shop_remove.finish()

    item_name = item_name.strip()
    result = xianshi_repository.remove_xianshi_by_name(
        _xianshi_name_removal_operation_id(event, user_id, item_name, quantity),
        user_id,
        item_name,
        quantity,
    )
    if result.status == "listing_missing":
        await handle_send(bot, event, f"您在仙肆未上架可下架的【{item_name}】！", md_type="交易", k1="下架", v1=f"仙肆下架 {item_name}", k2="上架", v2="仙肆上架", k3="我的", v3="我的仙肆")
        await xian_shop_remove.finish()
    if result.status == "inventory_full":
        await handle_send(bot, event, "背包空间不足，无法下架并退还物品！", md_type="交易", k1="下架", v1=f"仙肆下架 {item_name}", k2="上架", v2="仙肆上架", k3="我的", v3="我的仙肆")
        await xian_shop_remove.finish()
    if result.status == "listing_conflict":
        await handle_send(bot, event, "同名仙肆记录数据不一致，请联系管理员处理！")
        await xian_shop_remove.finish()
    if not result.succeeded:
        raise RuntimeError(f"unexpected xianshi name removal status: {result.status}")

    removed = result.removed_quantity
    qty_msg = f"x{removed}" if removed > 1 else ""
    msg = f"成功下架【{item_name}】{qty_msg}，已退回背包！"
    record_trade_event(
        user_id,
        "仙肆下架",
        f"下架{item_name}{qty_msg}",
        {"仙肆下架次数": 1, "仙肆下架数量": removed}
    )

    await handle_send(bot, event, msg, md_type="交易", k1="下架", v1=f"仙肆下架 {item_name}", k2="上架", v2="仙肆上架", k3="我的", v3="我的仙肆")
    await xian_shop_remove.finish()

@xian_buy.handle(parameterless=[Cooldown(cd_time=0)])
async def xian_buy_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆购买 - 根据仙肆ID购买物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await xian_buy.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "请输入要购买的仙肆ID！"
        await handle_send(bot, event, msg, md_type="交易", k1="购买", v1="仙肆购买", k2="查看", v2="仙肆查看", k3="我的", v3="我的仙肆")
        await xian_buy.finish()
    
    xianshi_id = args[0]
    quantity_to_buy = int(args[1]) if len(args) > 1 else 1
    if quantity_to_buy <= 0:
        quantity_to_buy = 1 # 购买数量至少为1

    # 从系统中查找物品
    item_list = xianshi_repository.get_xianshi_items(id=xianshi_id)
    
    if not item_list:
        msg = f"未找到仙肆ID为 {xianshi_id} 的物品！"
        await handle_send(bot, event, msg, md_type="交易", k1="购买", v1="仙肆购买", k2="查看", v2="仙肆查看", k3="我的", v3="我的仙肆")
        await xian_buy.finish()
    
    item_to_buy = item_list[0] # get_xianshi_items返回列表，取第一个
    
    try:
        success, msg, trade_info = buy_xianshi_item_safely(
            user_id,
            item_to_buy,
            quantity_to_buy,
            operation_id=_xianshi_operation_id(event, xianshi_id),
        )
        if not success:
            await handle_send(bot, event, msg, md_type="交易", k1="购买", v1="仙肆购买", k2="查看", v2="仙肆查看", k3="我的", v3="我的仙肆")
            await xian_buy.finish()

        if trade_info["applied"]:
            record_trade_event(
                user_id,
                "仙肆购买",
                f"购买{item_to_buy['name']}x{trade_info['quantity']}，花费{number_to(trade_info['total_cost'])}灵石",
                {"仙肆购买次数": 1, "仙肆购买数量": trade_info['quantity'], "仙肆消费灵石": trade_info['total_cost']}
            )
            if trade_info['seller_id'] != "0":
                record_trade_event(
                    trade_info['seller_id'],
                    "仙肆售出",
                    f"售出{item_to_buy['name']}x{trade_info['quantity']}，收入{number_to(trade_info['total_cost'])}灵石",
                    {"仙肆售出次数": 1, "仙肆售出数量": trade_info['quantity'], "仙肆收入灵石": trade_info['total_cost']}
                )
        await handle_send(bot, event, msg, md_type="交易", k1="购买", v1="仙肆购买", k2="查看", v2="仙肆查看", k3="我的", v3="我的仙肆")
    except Exception as e:
        logger.error(f"仙肆购买出错: {e}")
        msg = "购买过程中出现错误，请稍后再试！"
        await handle_send(bot, event, msg, md_type="交易", k1="购买", v1="仙肆购买", k2="查看", v2="仙肆查看", k3="我的", v3="我的仙肆")
    
    await xian_buy.finish()

@xianshi_fast_buy.handle(parameterless=[Cooldown(cd_time=0, stamina_cost=10)])
async def xianshi_fast_buy_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆快速购买 - 自动匹配最低价购买指定物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await xianshi_fast_buy.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "指令格式：仙肆快速购买 物品名1,物品名2,... [数量1,数量2,...]\n" \
              "▶ 物品名：支持1-5个物品（可重复），用逗号分隔\n" \
              "▶ 数量：可选，支持1-10个数量，用逗号分隔，没有数量默认每个物品买1个"
        sql_message.update_user_stamina(user_id, 10, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="购买", v1="仙肆快速购买", k2="查看", v2="仙肆查看", k3="我的", v3="我的仙肆")
        await xianshi_fast_buy.finish()
    
    # 解析物品名列表（允许重复且保留顺序）
    goods_names = args[0].split(",")
    if len(goods_names) > 5:
        msg = "一次最多指定5个物品名（可重复）！"
        sql_message.update_user_stamina(user_id, 10, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="购买", v1="仙肆快速购买", k2="查看", v2="仙肆查看", k3="我的", v3="我的仙肆")
        await xianshi_fast_buy.finish()
    
    # 解析数量列表
    quantities_input = args[1] if len(args) > 1 else ""
    quantities = quantities_input.split(",") if quantities_input else ["" for _ in goods_names]
    quantities = [int(q) if q.isdigit() else 1 for q in quantities]
    
    # 确保数量列表长度不超过物品名列表长度，不足则补1
    if len(quantities) < len(goods_names):
        quantities.extend([1] * (len(goods_names) - len(quantities)))
    elif len(quantities) > len(goods_names): # 数量列表过长则截断
        quantities = quantities[:len(goods_names)]

    # 获取所有仙肆中的物品
    all_xianshi_items = xianshi_repository.get_xianshi_items()
    if not all_xianshi_items:
        msg = "仙肆中没有物品可供购买！"
        sql_message.update_user_stamina(user_id, 10, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="购买", v1="仙肆快速购买", k2="查看", v2="仙肆查看", k3="我的", v3="我的仙肆")
        await xianshi_fast_buy.finish()
    
    # 过滤出用户可购买的物品（非自己上架，非系统物品）
    purchasable_items = [item for item in all_xianshi_items if item['user_id'] != user_id and item['user_id'] != 0]
    
    if not purchasable_items:
        msg = "仙肆中没有符合条件的用户物品可供购买！"
        sql_message.update_user_stamina(user_id, 10, 1)
        await handle_send(bot, event, msg, md_type="交易", k1="购买", v1="仙肆快速购买", k2="查看", v2="仙肆查看", k3="我的", v3="我的仙肆")
        await xianshi_fast_buy.finish()
    
    # 按价格从低到高排序
    purchasable_items.sort(key=lambda x: x['price'])
    
    # 执行购买（严格按照输入顺序处理每个物品名）
    total_cost = 0
    current_user_stone = user_info["stone"]
    
    success_items = []
    failed_items = []
    seller_trade_summary = {}
    
    for i, target_item_name in enumerate(goods_names):
        target_quantity = quantities[i]
        purchased_count = 0
        item_total_cost = 0
        
        # 查找该物品所有可购买项（按价格排序）
        available_target_items = [item for item in purchasable_items if item["name"] == target_item_name]
        
        for item_data in available_target_items:
            if purchased_count >= target_quantity:
                break # 达到目标购买数量
            
            # 检查用户是否有足够的灵石购买当前这个物品
            if current_user_stone < item_data["price"]:
                failed_items.append(f"{target_item_name}×{target_quantity - purchased_count}（灵石不足，跳过后续购买）")
                break # 灵石不足，停止购买该物品
            
            try:
                success, result_msg, trade_info = buy_xianshi_item_safely(
                    user_id,
                    item_data,
                    1,
                    operation_id=_xianshi_operation_id(
                        event,
                        item_data["id"],
                        f"{i}:{purchased_count}",
                    ),
                )
                if not success:
                    failed_items.append(f"{item_data['name']}×1 ({result_msg})")
                    continue
                if not trade_info["applied"]:
                    failed_items.append(f"{item_data['name']}×1（请求已处理）")
                    continue
                current_user_stone -= item_data["price"]
                seller_id = trade_info['seller_id']
                purchased_count += 1
                item_total_cost += item_data["price"]
                total_cost += item_data["price"]
                seller_summary = seller_trade_summary.setdefault(
                    seller_id,
                    {"quantity": 0, "income": 0, "items": {}}
                )
                seller_summary["quantity"] += 1
                seller_summary["income"] += item_data["price"]
                seller_summary["items"][item_data["name"]] = seller_summary["items"].get(item_data["name"], 0) + 1
                
            except Exception as e:
                logger.error(f"快速购买 {item_data['name']} (ID:{item_data['id']}) 时出错: {e}")
                failed_items.append(f"{item_data['name']}×1 (购买失败)")
                # 即使失败也继续尝试下一个物品
                continue
        
        if purchased_count > 0:
            success_items.append(f"{target_item_name}×{purchased_count} ({number_to(item_total_cost)}灵石)")
        
        if purchased_count < target_quantity and not any(f.startswith(target_item_name) and "灵石不足" in f for f in failed_items):
            failed_items.append(f"{target_item_name}×{target_quantity - purchased_count}（库存不足）")
            
    sql_message.update_user_stamina(user_id, 10, 1) # 恢复体力

    # 构建结果消息
    msg_parts = []
    if success_items:
        msg_parts.append("成功购买：")
        msg_parts.extend(success_items)
        msg_parts.append(f"总计花费：{number_to(total_cost)}灵石")
    if failed_items:
        if success_items:
            msg_parts.append("\n购买失败：")
        else:
            msg_parts.append("购买失败：")
        msg_parts.extend(failed_items)
    
    if not msg_parts:
        msg_parts.append("未进行任何购买操作！")

    msg = "\n".join(msg_parts)
    if total_cost > 0:
        total_quantity = sum(summary["quantity"] for summary in seller_trade_summary.values())
        record_trade_event(
            user_id,
            "仙肆快速购买",
            f"快速购买{total_quantity}件物品，花费{number_to(total_cost)}灵石",
            {"仙肆购买次数": 1, "仙肆购买数量": total_quantity, "仙肆消费灵石": total_cost}
        )
        for seller_id, summary in seller_trade_summary.items():
            items_detail = "、".join(f"{name}x{count}" for name, count in summary["items"].items())
            record_trade_event(
                seller_id,
                "仙肆售出",
                f"售出{items_detail}，收入{number_to(summary['income'])}灵石",
                {"仙肆售出次数": 1, "仙肆售出数量": summary["quantity"], "仙肆收入灵石": summary["income"]}
            )
    await handle_send(bot, event, msg, md_type="交易", k1="购买", v1="仙肆快速购买", k2="查看", v2="仙肆查看", k3="我的", v3="我的仙肆")
    await xianshi_fast_buy.finish()

@xian_shop_off_all.handle(parameterless=[Cooldown(60, isolate_level=CooldownIsolateLevel.GLOBAL, parallel=1)])
async def xian_shop_off_all_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """清空仙肆 - 管理员命令，清空所有仙肆上架物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    # 再次检查是否为超级用户
    if not await SUPERUSER(bot, event):
        await handle_send(bot, event, "此功能仅限管理员使用！")
        await xian_shop_off_all.finish()

    msg = "正在清空全服仙肆，请稍候..."
    await handle_send(bot, event, msg)
    
    result = xianshi_repository.clear_all_xianshi_listings(
        _xianshi_clear_operation_id(event)
    )
    if result.status == "empty":
        msg = "仙肆已经是空的，没有物品被下架！"
        await handle_send(bot, event, msg)
        await xian_shop_off_all.finish()
    if result.status == "inventory_full":
        msg = "存在用户背包空间不足，仙肆未清空，请先处理背包容量！"
        await handle_send(bot, event, msg)
        await xian_shop_off_all.finish()
    if not result.succeeded:
        raise RuntimeError(f"unexpected xianshi clear status: {result.status}")

    msg = (
        f"成功清空全服仙肆！共下架 {result.listing_count} 条记录，"
        f"退回用户物品 {result.refunded_quantity} 件。"
    )
    await handle_send(bot, event, msg)
    await xian_shop_off_all.finish()

@xian_shop_added_by_admin.handle(parameterless=[Cooldown(cd_time=0)])
async def xian_shop_added_by_admin_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """系统仙肆上架 - 管理员命令，上架系统物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    if not await SUPERUSER(bot, event):
        await handle_send(bot, event, "此功能仅限管理员使用！")
        await xian_shop_added_by_admin.finish()
    
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "请输入正确指令！格式：系统仙肆上架 物品名称 [价格] [数量]"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    
    goods_name = args[0]
    try:
        price = int(args[1]) if len(args) > 1 else MIN_PRICE
        quantity = int(args[2]) if len(args) > 2 else -1 # 数量-1表示不限量
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    
    if quantity < -1: # 数量不能小于-1
        quantity = -1

    # 检查物品是否存在
    goods_id, item_info = items.get_data_by_item_name(goods_name)
    if not item_info:
        msg = f"物品 {goods_name} 不存在！"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    
    # 检查物品类型是否允许上架
    goods_type = item_info['type']
    if goods_type not in ITEM_TYPES:
        msg = f"该物品类型不允许上架！允许类型：{', '.join(ITEM_TYPES)}"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()

    forbid_reason = get_trade_forbid_reason(goods_id, item_info)
    if forbid_reason:
        await handle_send(bot, event, forbid_reason)
        await xian_shop_added_by_admin.finish()
    
    # 上架物品
    try:
        xianshi_repository.add_xianshi_item(0, goods_id, goods_name, goods_type, price, quantity) # user_id=0表示系统物品
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

@xian_shop_remove_by_admin.handle(parameterless=[Cooldown(cd_time=0)])
async def xian_shop_remove_by_admin_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """系统仙肆下架 - 管理员命令，下架系统物品或用户物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    if not await SUPERUSER(bot, event):
        await handle_send(bot, event, "此功能仅限管理员使用！")
        await xian_shop_remove_by_admin.finish()
    
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "请输入正确指令！格式：系统仙肆下架 [物品仙肆ID]"
        await handle_send(bot, event, msg)
        await xian_shop_remove_by_admin.finish()
    
    xianshi_id = args[0]
    
    # 查找物品
    item_list = xianshi_repository.get_xianshi_items(id=xianshi_id)
    
    if not item_list:
        msg = f"未找到仙肆ID为 {xianshi_id} 的物品！"
        await handle_send(bot, event, msg)
        await xian_shop_remove_by_admin.finish()
    
    item_to_remove = item_list[0]
    
    result = xianshi_repository.remove_xianshi_listing(
        _xianshi_removal_operation_id(event, xianshi_id), xianshi_id
    )
    if result.status == "inventory_full":
        await handle_send(bot, event, "用户背包空间不足，无法下架并退还物品！")
        await xian_shop_remove_by_admin.finish()
    if result.status == "listing_missing":
        await handle_send(bot, event, f"仙肆ID为 {xianshi_id} 的物品状态已变化！")
        await xian_shop_remove_by_admin.finish()
    if not result.succeeded:
        raise RuntimeError(f"unexpected xianshi removal status: {result.status}")
    msg = f"成功下架仙肆ID为 {xianshi_id} 的 {result.name}！"
    if result.refunded_quantity:
        msg += f"\n已退还给用户 x{result.refunded_quantity}。"
    await handle_send(bot, event, msg)
    
    await xian_shop_remove_by_admin.finish()

# --- 鬼市命令处理 ---

@guishi_deposit.handle(parameterless=[Cooldown(cd_time=0)])
async def guishi_deposit_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市存灵石"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await guishi_deposit.finish()
    
    user_id = user_info['user_id']
    amount_str = args.extract_plain_text().strip()
    
    if not amount_str.isdigit():
        msg = "请输入正确的灵石数量！"
        await handle_send(bot, event, msg, md_type="交易", k1="存灵石", v1="鬼市存灵石", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_deposit.finish()
    
    amount = int(amount_str)
    if amount <= 0:
        msg = "存入数量必须大于0！"
        await handle_send(bot, event, msg, md_type="交易", k1="存灵石", v1="鬼市存灵石", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_deposit.finish()
    
    result = guishi_stone_service.deposit(
        _guishi_stone_operation_id(event, "deposit", user_id),
        user_id,
        amount,
    )
    if result.status == "stone_insufficient":
        msg = "灵石不足，存入失败！"
        await handle_send(bot, event, msg, md_type="交易", k1="存灵石", v1="鬼市存灵石", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_deposit.finish()
    if not result.succeeded:
        msg = "鬼市账户状态已经变化，请稍后重试！"
        await handle_send(bot, event, msg, md_type="交易", k1="存灵石", v1="鬼市存灵石", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_deposit.finish()
    
    msg = f"成功存入 {number_to(result.amount)} 灵石到鬼市账户！"
    if result.applied:
        record_trade_event(
            user_id,
            "鬼市存灵石",
            f"存入{number_to(result.amount)}灵石到鬼市账户",
            {"鬼市存入灵石": result.amount}
        )
    await handle_send(bot, event, msg, md_type="交易", k1="取灵石", v1="鬼市取灵石", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
    await guishi_deposit.finish()

@guishi_withdraw.handle(parameterless=[Cooldown(cd_time=0)])
async def guishi_withdraw_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市取灵石（收取动态手续费，仅周末开放）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await guishi_withdraw.finish()
    
    # 检查是否是周末
    today = datetime.now().weekday()
    if today not in [5, 6]:  # 5 是周六，6 是周日
        msg = "鬼市取灵石功能仅在周六和周日开放！"
        await handle_send(bot, event, msg)
        await guishi_withdraw.finish()
    
    user_id = user_info['user_id']
    amount_str = args.extract_plain_text().strip()
    
    if not amount_str.isdigit():
        msg = "请输入正确的灵石数量！"
        await handle_send(bot, event, msg, md_type="交易", k1="取灵石", v1="鬼市取灵石", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_withdraw.finish()
    
    amount = int(amount_str)
    if amount <= 0:
        msg = "取出数量必须大于0！"
        await handle_send(bot, event, msg, md_type="交易", k1="取灵石", v1="鬼市取灵石", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_withdraw.finish()
    
    result = guishi_stone_service.withdraw(
        _guishi_stone_operation_id(event, "withdraw", user_id),
        user_id,
        amount,
    )
    if result.status == "stored_insufficient":
        msg = f"鬼市账户余额不足！当前余额 {number_to(result.stored_balance)} 灵石"
        await handle_send(bot, event, msg, md_type="交易", k1="取灵石", v1="鬼市取灵石", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_withdraw.finish()
    if not result.succeeded:
        msg = "鬼市账户状态已经变化，请稍后重试！"
        await handle_send(bot, event, msg, md_type="交易", k1="取灵石", v1="鬼市取灵石", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_withdraw.finish()

    fee_rate = result.fee / result.amount if result.amount else 0
    msg = f"成功取出 {number_to(result.amount)} 灵石（手续费：{fee_rate*100:.0f}%，扣除{number_to(result.fee)}灵石，实际到账 {number_to(result.actual_amount)} 灵石）"
    if result.applied:
        record_trade_event(
            user_id,
            "鬼市取灵石",
            f"取出{number_to(result.amount)}灵石，手续费{number_to(result.fee)}灵石，到账{number_to(result.actual_amount)}灵石",
            {"鬼市取出灵石": result.actual_amount, "鬼市取灵石手续费": result.fee}
        )
    await handle_send(bot, event, msg, md_type="交易", k1="存灵石", v1="鬼市存灵石", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
    await guishi_withdraw.finish()

@guishi_qiugou.handle(parameterless=[Cooldown(cd_time=0)])
async def guishi_qiugou_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市求购"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await guishi_qiugou.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 2:
        msg = "指令格式：鬼市求购 物品名称 价格 [数量]\n数量不填默认为1"
        await handle_send(bot, event, msg, md_type="交易", k1="求购", v1="鬼市求购", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_qiugou.finish()
    
    item_name = args[0]
    try:
        price = int(args[1])
        if price < int(MIN_PRICE * 10): # 鬼市求购最低价格为仙肆最低价的10倍
            msg = f"当前价格过低！最低{number_to(MIN_PRICE * 10)}灵石"
            await handle_send(bot, event, msg, md_type="交易", k1="求购", v1="鬼市求购", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
            await guishi_qiugou.finish()
        quantity = int(args[2]) if len(args) > 2 else 1
        quantity = max(1, min(quantity, GUISHI_MAX_QUANTITY)) # 数量限制
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg, md_type="交易", k1="求购", v1="鬼市求购", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_qiugou.finish()

    # 检查物品是否存在
    goods_id, goods_info = items.get_data_by_item_name(item_name)
    if not goods_id:
        msg = f"物品 {item_name} 不存在，请检查名称是否正确！"
        await handle_send(bot, event, msg, md_type="交易", k1="求购", v1="鬼市求购", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        return

    # 检查物品类型是否允许鬼市交易
    if goods_info['type'] not in GUISHI_TYPES:
        msg = f"该物品类型不允许在鬼市交易！允许类型：{', '.join(GUISHI_TYPES)}"
        await handle_send(bot, event, msg, md_type="交易", k1="求购", v1="鬼市求购", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_qiugou.finish()

    forbid_reason = get_trade_forbid_reason(goods_id, goods_info)
    if forbid_reason:
        await handle_send(bot, event, forbid_reason, md_type="交易", k1="求购", v1="鬼市求购", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_qiugou.finish()

    result = xianshi_repository.create_guishi_qiugou_order(
        get_paths().trade_db,
        user_id,
        goods_id,
        item_name,
        price,
        quantity,
        max_orders=MAX_QIUGOU_ORDERS,
        operation_id=_guishi_order_operation_id(
            event, "qiugou", user_id, goods_id, price, quantity
        ),
    )
    if result.status == "limit_reached":
        msg = f"您的求购订单已达上限({MAX_QIUGOU_ORDERS})，请明日再来！"
        await handle_send(bot, event, msg, md_type="交易", k1="求购", v1="鬼市求购", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_qiugou.finish()
    if result.status == "stone_insufficient":
        msg = f"鬼市账户余额不足！需要 {number_to(result.total_cost)} 灵石"
        await handle_send(bot, event, msg, md_type="交易", k1="求购", v1="鬼市求购", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_qiugou.finish()
    if not result.created:
        raise RuntimeError(f"unexpected guishi qiugou status: {result.status}")
    order_id = result.order_id
    
    msg = f"成功发布求购订单！\n"
    msg += f"物品：{item_name}\n"
    msg += f"总价：{number_to(result.total_cost)} 灵石\n"
    msg += f"单价：{number_to(price)} 灵石\n"
    msg += f"数量：{quantity}\n"
    msg += f"订单ID：{order_id}\n"
    msg += f"♻️ 次日{GUISHI_BAITAN_END_HOUR}点自动取消订单，并退还未购得物品的灵石！"
    record_trade_event(
        user_id,
        "鬼市求购",
        f"发布求购{item_name}x{quantity}，单价{number_to(price)}灵石，冻结{number_to(result.total_cost)}灵石，订单ID:{order_id}",
        {"鬼市求购次数": 1, "鬼市求购数量": quantity, "鬼市求购冻结灵石": result.total_cost}
    )
    
    # 立即尝试进行交易匹配，避免等待调度器
    transaction_result_msg = await process_guishi_transactions(user_id=user_id)
    await handle_send(bot, event, msg, md_type="交易", k1="求购", v1="鬼市求购", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
    if transaction_result_msg:
        await handle_send(bot, event, transaction_result_msg) # 发送交易结果
    await guishi_qiugou.finish()

@guishi_cancel_qiugou.handle(parameterless=[Cooldown(cd_time=0)])
async def guishi_cancel_qiugou_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市取消求购订单"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await guishi_cancel_qiugou.finish()
    
    user_id = user_info['user_id']
    order_id = args.extract_plain_text().strip()
    
    if not order_id.isdigit():
        msg = "请输入要取消的求购订单ID！"
        await handle_send(bot, event, msg, md_type="交易", k1="取消求购", v1="鬼市取消求购", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_cancel_qiugou.finish()
    
    result = xianshi_repository.clear_guishi_qiugou_order(
        get_paths().trade_db,
        order_id,
        expected_user_id=user_id,
    )
    if not result.cleared:
        msg = f"未找到您的ID为 {order_id} 的求购订单！"
        await handle_send(bot, event, msg, md_type="交易", k1="取消求购", v1="鬼市取消求购", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_cancel_qiugou.finish()

    msg = f"成功取消求购订单（ID:{order_id}）！\n"
    msg += f"已退还 {number_to(result.refunded_stone)} 灵石到您的鬼市账户。"
    record_trade_event(
        user_id,
        "鬼市取消求购",
        f"取消求购订单ID:{order_id}，退还{number_to(result.refunded_stone)}灵石",
        {"鬼市取消求购次数": 1, "鬼市求购退还灵石": result.refunded_stone}
    )
    await handle_send(bot, event, msg, md_type="交易", k1="取消求购", v1="鬼市取消求购", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
    await guishi_cancel_qiugou.finish()

@guishi_baitan.handle(parameterless=[Cooldown(cd_time=0)])
async def guishi_baitan_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市摆摊（每天20:00-次日8:00开放）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await guishi_baitan.finish()
    
    # 检查摆摊时间
    now = datetime.now()
    current_hour = now.hour
    
    # 判断是否在允许摆摊的时间段 (20:00-23:59 或 00:00-07:59)
    if not (GUISHI_BAITAN_START_HOUR <= current_hour <= 23 or 0 <= current_hour < GUISHI_BAITAN_END_HOUR):
        # 计算下一次摆摊开始时间
        next_start_time = now.replace(minute=0, second=0, microsecond=0)
        if current_hour >= GUISHI_BAITAN_END_HOUR: # 如果当前时间已经过了早上结束时间
            next_start_time = next_start_time.replace(hour=GUISHI_BAITAN_START_HOUR) # 今天晚上开始
            if current_hour >= GUISHI_BAITAN_START_HOUR: # 如果已经过了晚上开始时间，则是明晚
                 next_start_time += timedelta(days=1)
        else: # 如果当前时间在早上结束时间之前 (即0-7点)
            next_start_time = next_start_time.replace(hour=GUISHI_BAITAN_START_HOUR) # 今天晚上开始
        
        # 计算距离下次开始的时间
        time_left = next_start_time - now
        hours = time_left.total_seconds() // 3600
        minutes = (time_left.total_seconds() % 3600) // 60
        
        msg = f"鬼市摆摊时间：每天{GUISHI_BAITAN_START_HOUR}:00-次日{GUISHI_BAITAN_END_HOUR}:00\n"
        msg += f"下次可摆摊时间：{next_start_time.strftime('%m月%d日 %H:%M')}（{int(hours)}小时{int(minutes)}分钟后）"
        await handle_send(bot, event, msg, md_type="交易", k1="摆摊", v1="鬼市摆摊", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_baitan.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 2:
        msg = "指令格式：鬼市摆摊 物品名称 价格 [数量]\n数量不填默认为1"
        await handle_send(bot, event, msg, md_type="交易", k1="摆摊", v1="鬼市摆摊", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_baitan.finish()
    
    item_name = args[0]
    try:
        price = int(args[1])
        if price < int(MIN_PRICE * 10): # 鬼市摆摊最低价格为仙肆最低价的10倍
            msg = f"当前价格过低！最低{number_to(MIN_PRICE * 10)}灵石"
            await handle_send(bot, event, msg, md_type="交易", k1="摆摊", v1="鬼市摆摊", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
            await guishi_baitan.finish()
        quantity = int(args[2]) if len(args) > 2 else 1
        quantity = max(1, min(quantity, GUISHI_MAX_QUANTITY))
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg, md_type="交易", k1="摆摊", v1="鬼市摆摊", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_baitan.finish()
    
    # 检查订单数量限制
    baitan_orders = trade_manager.get_guishi_orders(user_id=user_id, type="baitan")
    
    if baitan_orders and len(baitan_orders) >= MAX_BAITAN_ORDERS:
        msg = f"您的摆摊订单已达上限({MAX_BAITAN_ORDERS})，请先收摊部分订单！"
        await handle_send(bot, event, msg, md_type="交易", k1="摆摊", v1="鬼市摆摊", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_baitan.finish()
    
    # 检查背包物品
    goods_id, goods_info = items.get_data_by_item_name(item_name)
    if not goods_id:
        msg = f"物品 {item_name} 不存在，请检查名称是否正确！"
        await handle_send(bot, event, msg, md_type="交易", k1="摆摊", v1="鬼市摆摊", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        return
    
    # 检查用户背包中可交易的物品数量
    goods_num = sql_message.goods_num(str(user_info['user_id']), goods_id, num_type='trade')
    if goods_num <= 0:
        msg = f"背包中没有足够的 {item_name} 用于交易！"
        await handle_send(bot, event, msg, md_type="交易", k1="摆摊", v1="鬼市摆摊", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        return
    
    # 检查物品类型是否允许鬼市交易
    if goods_info['type'] not in GUISHI_TYPES:
        msg = f"该物品类型不允许在鬼市交易！允许类型：{', '.join(GUISHI_TYPES)}"
        await handle_send(bot, event, msg, md_type="交易", k1="摆摊", v1="鬼市摆摊", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_baitan.finish()
    
    forbid_reason = get_trade_forbid_reason(goods_id, goods_info)
    if forbid_reason:
        msg = forbid_reason
        await handle_send(bot, event, msg, md_type="交易", k1="摆摊", v1="鬼市摆摊", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_baitan.finish()
    
    if quantity > goods_num: # 实际可上架数量不能超过背包现有数量
        quantity = goods_num
        
    result = xianshi_repository.create_guishi_baitan_order(
        get_paths().trade_db,
        user_id,
        goods_id,
        item_name,
        price,
        quantity,
        max_orders=MAX_BAITAN_ORDERS,
        operation_id=_guishi_order_operation_id(
            event, "baitan", user_id, goods_id, price, quantity
        ),
    )
    if result.status == "limit_reached":
        msg = f"您的摆摊订单已达上限({MAX_BAITAN_ORDERS})，请先收摊部分订单！"
        await handle_send(bot, event, msg, md_type="交易", k1="摆摊", v1="鬼市摆摊", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_baitan.finish()
    if result.status == "stock_insufficient":
        msg = f"可交易的 {item_name} 数量不足，摆摊失败！"
        await handle_send(bot, event, msg, md_type="交易", k1="摆摊", v1="鬼市摆摊", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_baitan.finish()
    if not result.created:
        raise RuntimeError(f"unexpected guishi baitan status: {result.status}")
    order_id = result.order_id
    
    msg = f"成功摆摊！\n"
    msg += f"物品：{item_name}\n"
    msg += f"价格：{number_to(price)} 灵石\n"
    msg += f"数量：{quantity}\n"
    msg += f"摊位ID：{order_id}\n"
    msg += f"⚠️ 请在次日{GUISHI_BAITAN_END_HOUR}点前收摊，超时未收摊将自动清空摊位，物品不退还！"
    record_trade_event(
        user_id,
        "鬼市摆摊",
        f"摆摊{item_name}x{quantity}，单价{number_to(price)}灵石，摊位ID:{order_id}",
        {"鬼市摆摊次数": 1, "鬼市摆摊数量": quantity}
    )
    
    await handle_send(bot, event, msg, md_type="交易", k1="摆摊", v1="鬼市摆摊", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
    await guishi_baitan.finish()

@guishi_shoutan.handle(parameterless=[Cooldown(cd_time=0)])
async def guishi_shoutan_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市收摊 - 收回所有摆摊订单"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await guishi_shoutan.finish()
    
    user_id = user_info['user_id']
    
    # 获取用户的摆摊订单
    baitan_orders = trade_manager.get_guishi_orders(user_id=user_id, type="baitan")
    
    if not baitan_orders:
        msg = "您当前没有摆摊订单！"
        await handle_send(bot, event, msg, md_type="交易", k1="收摊", v1="鬼市收摊", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_shoutan.finish()
    
    # 取消所有摆摊订单
    total_refunded_items = 0
    refunded_items_summary = {} # 统计退还的物品
    
    for order in baitan_orders:
        _, item_info = items.get_data_by_item_name(order['item_name'])
        if not item_info:
            logger.warning(f"鬼市摆摊订单 {order['id']} 的物品不存在，已保留订单")
            continue
        result = xianshi_repository.clear_expired_guishi_order(
            get_paths().trade_db,
            order['id'],
            item_info['type'],
            expected_user_id=user_id,
        )
        if not result.cleared:
            continue
        if result.refunded_quantity:
            refunded_items_summary[result.item_name] = (
                refunded_items_summary.get(result.item_name, 0)
                + result.refunded_quantity
            )
            total_refunded_items += result.refunded_quantity
    
    if total_refunded_items > 0:
        refund_msg = "\n已退回物品：" + "\n".join([f"{name} x{count}" for name, count in refunded_items_summary.items()])
    else:
        refund_msg = "\n所有摆摊物品均已售出，没有物品退回。"
        
    msg = f"成功收摊！所有摆摊订单已取消。{refund_msg}"
    record_trade_event(
        user_id,
        "鬼市收摊",
        f"取消{len(baitan_orders)}条摆摊订单，退回{total_refunded_items}件物品",
        {"鬼市收摊次数": 1, "鬼市收摊退回物品": total_refunded_items}
    )
    await handle_send(bot, event, msg, md_type="交易", k1="收摊", v1="鬼市收摊", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
    await guishi_shoutan.finish()

@guishi_take_item.handle(parameterless=[Cooldown(cd_time=0)])
async def guishi_take_item_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市取物品 - 从鬼市暂存区取出物品（仅周末开放）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await guishi_take_item.finish()
    
    # 检查是否是周末
    today = datetime.now().weekday()
    if today not in [5, 6]:  # 5 是周六，6 是周日
        msg = "鬼市取物品功能仅在周六和周日开放！"
        await handle_send(bot, event, msg)
        await guishi_take_item.finish()

    user_id = user_info['user_id']
    goods_name = args.extract_plain_text().strip()

    if not goods_name:
        msg = "请输入要取出的物品名称！"
        await handle_send(bot, event, msg, md_type="交易", k1="取物品", v1="鬼市取物品", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_take_item.finish()
    
    # 通过物品名获取ID
    goods_id, item_info = items.get_data_by_item_name(goods_name)
    if not goods_id:
        msg = f"物品 {goods_name} 不存在！"
        await handle_send(bot, event, msg, md_type="交易", k1="取物品", v1="鬼市取物品", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_take_item.finish()
    
    result = xianshi_repository.take_guishi_stored_item(
        _guishi_take_item_operation_id(event, user_id, goods_id),
        get_paths().trade_db,
        user_id,
        goods_id,
        item_info['name'],
        item_info['type'],
    )
    if result.status == "item_missing":
        msg = f"您没有暂存物品 {goods_name}！"
        await handle_send(bot, event, msg, md_type="交易", k1="取物品", v1="鬼市取物品", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_take_item.finish()
    if result.status == "inventory_full":
        msg = f"背包空间不足，无法取出 {item_info['name']} x{result.quantity}！"
        await handle_send(bot, event, msg, md_type="交易", k1="取物品", v1="鬼市取物品", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_take_item.finish()
    if not result.succeeded:
        msg = "鬼市暂存区状态已经变化，请稍后重试！"
        await handle_send(bot, event, msg, md_type="交易", k1="取物品", v1="鬼市取物品", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
        await guishi_take_item.finish()

    msg = f"成功取出 {item_info['name']} x{result.quantity}！"
    if result.applied:
        record_trade_event(
            user_id,
            "鬼市取物品",
            f"取出暂存物品{item_info['name']}x{result.quantity}",
            {"鬼市取物品次数": 1, "鬼市取物品数量": result.quantity}
        )
    await handle_send(bot, event, msg, md_type="交易", k1="取物品", v1="鬼市取物品", k2="信息", v2="鬼市信息", k3="帮助", v3="鬼市帮助")
    await guishi_take_item.finish()

@guishi_info.handle(parameterless=[Cooldown(cd_time=0)])
async def guishi_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市信息 - 查看鬼市账户、求购和摆摊订单"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await guishi_info.finish()
    
    user_id = user_info['user_id']
    
    # 获取用户的鬼市账户信息
    stored_stone = trade_manager.get_stored_stone(user_id)
    stored_items = trade_manager.get_stored_items(user_id)
    
    msg_parts = [f"【鬼市账户信息】\n"]
    msg_parts.append(f"账户余额：{number_to(stored_stone)}\n")
    
    if stored_items:
        msg_parts.append(f"\n【暂存物品】\n")
        for item_id_str, quantity in stored_items.items():
            item_info = items.get_data_by_item_id(int(item_id_str)) # key是字符串，需要转回int
            if item_info:
                msg_parts.append(f"  {item_info['name']} x{quantity}\n")

    # 获取用户的求购订单
    qiugou_orders = trade_manager.get_guishi_orders(user_id=user_id, type="qiugou")
    if qiugou_orders:
        msg_parts.append(f"\n☆------求购列表------☆\n")
        for order in qiugou_orders:
            unfilled_quantity = order['quantity'] - order['filled_quantity']
            msg_parts.append(f"ID:{order['id']} {order['item_name']} {number_to(order['price'])}灵石 x{order['quantity']} (待购:{unfilled_quantity})\n")

    # 获取用户的摆摊订单
    baitan_orders = trade_manager.get_guishi_orders(user_id=user_id, type="baitan")
    if baitan_orders:
        msg_parts.append(f"\n☆------摆摊列表------☆\n")
        for order in baitan_orders:
            unsold_quantity = order['quantity'] - order['filled_quantity']
            msg_parts.append(f"ID:{order['id']} {order['item_name']} {number_to(order['price'])}灵石 x{order['quantity']} (待售:{unsold_quantity})\n")
    
    await handle_send(bot, event, "\n".join(msg_parts), md_type="交易", k1="取物品", v1="鬼市取物品", k2="求购", v2="鬼市求购", k3="摆摊", v3="鬼市摆摊")
    await guishi_info.finish()

@clear_all_guishi.handle(parameterless=[Cooldown(60, isolate_level=CooldownIsolateLevel.GLOBAL, parallel=1)])
async def clear_all_guishi_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """清空鬼市 - 管理员命令，清空所有鬼市订单并处理退还"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    if not await SUPERUSER(bot, event):
        await handle_send(bot, event, "此功能仅限管理员使用！")
        await clear_all_guishi.finish()

    msg = "正在清空全服鬼市，请稍候..."
    await handle_send(bot, event, msg)
    
    all_guishi_orders = trade_manager.get_guishi_orders() # 获取所有鬼市订单
    
    if not all_guishi_orders:
        msg = "鬼市中没有订单可供清空！"
        await handle_send(bot, event, msg)
        await clear_all_guishi.finish()

    cleared_count = 0
    refund_stone_summary = {} # 统计退还的灵石
    refund_item_summary = {} # 统计退还的物品
    
    for order in all_guishi_orders:
        if order['item_type'] == "qiugou": # 求购订单，退还灵石
            result = xianshi_repository.clear_guishi_qiugou_order(
                get_paths().trade_db,
                order['id'],
            )
            if not result.cleared:
                continue
            if result.refunded_stone:
                refund_stone_summary[result.user_id] = (
                    refund_stone_summary.get(result.user_id, 0) + result.refunded_stone
                )
        elif order['item_type'] == "baitan": # 摆摊订单，退还物品
            _, item_info = items.get_data_by_item_name(order['item_name'])
            if not item_info:
                logger.warning(f"鬼市摆摊订单 {order['id']} 的物品不存在，已保留订单")
                continue
            result = xianshi_repository.clear_expired_guishi_order(
                get_paths().trade_db,
                order['id'],
                item_info['type'],
            )
            if not result.cleared:
                continue
            if result.refunded_quantity:
                key = (result.user_id, result.item_name)
                refund_item_summary[key] = (
                    refund_item_summary.get(key, 0) + result.refunded_quantity
                )
        else:
            logger.warning(f"鬼市订单 {order['id']} 类型未知，已保留订单")
            continue
        
        cleared_count += 1
    
    msg_parts = [f"成功清空鬼市！共处理 {cleared_count} 条订单。"]
    if refund_stone_summary:
        msg_parts.append("\n☆------灵石退还------☆")
        for user_id, amount in refund_stone_summary.items():
            user_info = sql_message.get_user_info_with_id(user_id)
            msg_parts.append(f"{user_info['user_name'] if user_info else user_id}: {number_to(amount)}灵石")
    if refund_item_summary:
        msg_parts.append("\n☆------物品退还------☆")
        for (user_id, item_name), quantity in refund_item_summary.items():
            user_info = sql_message.get_user_info_with_id(user_id)
            msg_parts.append(f"{user_info['user_name'] if user_info else user_id}: {item_name} x{quantity}")

    await handle_send(bot, event, "\n".join(msg_parts))
    await clear_all_guishi.finish()

async def process_guishi_transactions(user_id: str = None) -> str:
    """
    处理鬼市的求购与摆摊交易匹配。
    :param user_id: 如果指定，则只处理该用户的求购订单，并返回详细交易信息；否则处理所有订单，只记录日志。
    :return: 如果user_id指定，返回交易匹配的消息；否则返回空字符串。
    """
    if user_id: # 如果是单个用户触发，只处理该用户的求购订单
        qiugou_orders = trade_manager.get_guishi_orders(user_id=user_id, type="qiugou")
        transaction_log = "开始处理您的鬼市交易...\n"
    else: # 否则处理所有求购订单
        qiugou_orders = trade_manager.get_guishi_orders(type="qiugou")
        transaction_log = "开始处理鬼市交易...\n"

    if not qiugou_orders:
        return transaction_log + "没有求购订单可供匹配。" if user_id else ""
    
    # 转换为字典，方便根据id更新
    qiugou_orders_dict = {order['id']: order for order in qiugou_orders}
    
    for qiugou_order_id in list(qiugou_orders_dict.keys()): # 迭代副本，防止循环中修改
        qiugou_order = qiugou_orders_dict[qiugou_order_id]

        qiugou_user_id = qiugou_order['user_id']
        qiugou_item_name = qiugou_order['item_name']
        qiugou_price = qiugou_order['price']
        
        unfilled_qiugou_quantity = qiugou_order['quantity'] - qiugou_order['filled_quantity']
        
        if unfilled_qiugou_quantity <= 0: # 订单已完成
            if user_id: transaction_log += f"求购订单 {qiugou_order_id} 已完成，等待事务清理。\n"
            continue
        
        # 获取所有符合条件的摆摊订单（物品名称相同，价格低于或等于求购价，且非自己的摆摊）
        baitan_orders = trade_manager.get_guishi_orders(type="baitan", name=qiugou_item_name)
        
        if not baitan_orders:
            if user_id: transaction_log += f"【{qiugou_item_name}】没有匹配的摆摊订单。\n"
            continue
        
        # 筛选非自己的摆摊订单，并按价格升序排序
        available_baitan_orders = sorted([
            o for o in baitan_orders if o['user_id'] != qiugou_user_id and o['price'] <= qiugou_price
        ], key=lambda x: x['price'])

        if not available_baitan_orders:
            if user_id: transaction_log += f"【{qiugou_item_name}】没有符合价格或非自己的摆摊订单。\n"
            continue
        
        for baitan_order in available_baitan_orders:
            baitan_order_id = baitan_order['id']
            result = xianshi_repository.match_guishi_orders(
                get_paths().trade_db,
                qiugou_order_id,
                baitan_order_id,
                operation_id=f"guishi-match:{qiugou_order_id}:{baitan_order_id}",
            )
            if result.status == "qiugou_completed":
                if user_id:
                    transaction_log += f"求购订单 {qiugou_order_id} 已完成，已移除。\n"
                break
            if not result.matched:
                continue

            qiugou_user_info = sql_message.get_user_info_with_id(result.buyer_id)
            baitan_user_info = sql_message.get_user_info_with_id(result.seller_id)
            qiugou_user_name = (
                qiugou_user_info['user_name'] if qiugou_user_info else result.buyer_id
            )
            baitan_user_name = (
                baitan_user_info['user_name'] if baitan_user_info else result.seller_id
            )
            record_trade_event(
                result.buyer_id,
                "鬼市成交",
                f"购得{result.item_name}x{result.quantity}，花费{number_to(result.amount)}灵石，卖家:{baitan_user_name}",
                {"鬼市购买次数": 1, "鬼市购买数量": result.quantity, "鬼市消费灵石": result.amount}
            )
            record_trade_event(
                result.seller_id,
                "鬼市成交",
                f"售出{result.item_name}x{result.quantity}，收入{number_to(result.amount)}灵石，买家:{qiugou_user_name}",
                {"鬼市售出次数": 1, "鬼市售出数量": result.quantity, "鬼市收入灵石": result.amount}
            )
            transaction_log += (f"{qiugou_user_name} 从 {baitan_user_name} 处\n"
                                f"购买了 {result.quantity} 个 【{result.item_name}】，花费 {number_to(result.amount)} 灵石。\n")
            if result.baitan_completed and user_id:
                transaction_log += f"  摆摊订单 {baitan_order_id} 已完成，已移除。\n"
            if result.qiugou_completed:
                if user_id:
                    transaction_log += f"  求购订单 {qiugou_order_id} 已完成，已移除。\n"
                break
    
    transaction_log += "鬼市交易处理完成。"
    logger.info(transaction_log)
    return transaction_log if user_id else ""


@scheduler.scheduled_job(
    "cron",
    hour=GUISHI_AUTO_HOUR,
    minute=0,
    id="auto_guishi_transactions",
    coalesce=True,
    max_instances=1,
    misfire_grace_time=300,
)
async def auto_guishi_transactions_job():
    """定时鬼市自动交易匹配"""
    logger.info("执行鬼市自动交易匹配任务...")
    await process_guishi_transactions()


@scheduler.scheduled_job(
    "cron",
    hour=GUISHI_BAITAN_END_HOUR,
    minute=0,
    id="clear_expired_baitan_orders",
    coalesce=True,
    max_instances=1,
    misfire_grace_time=300,
)
async def clear_expired_baitan_orders_job():
    """每天摆摊时间结束后，自动清空所有未售罄的摆摊订单，并退还未售出的物品。"""
    logger.info("开始检查并清理超时鬼市摆摊订单...")
    
    all_baitan_orders = trade_manager.get_guishi_orders(type="baitan")
    if not all_baitan_orders:
        logger.info("没有鬼市摆摊订单可供清理。")
        return

    cleared_count = 0
    refund_item_summary = {}
    
    for order in all_baitan_orders:
        _, item_info = items.get_data_by_item_name(order['item_name'])
        if not item_info:
            logger.warning(f"鬼市摆摊订单 {order['id']} 的物品不存在，已保留订单")
            continue
        result = xianshi_repository.clear_expired_guishi_order(
            get_paths().trade_db,
            order['id'],
            item_info['type'],
        )
        if result.status == "inventory_full":
            logger.warning(
                f"鬼市摆摊订单 {result.order_id} 退货后将超过背包上限，已保留订单"
            )
            continue
        if not result.cleared:
            continue
        if result.refunded_quantity > 0:
            user_info = sql_message.get_user_info_with_id(result.user_id)
            user_name = user_info['user_name'] if user_info else f"用户{result.user_id}"
            user_key = f"{user_name} ({result.user_id})"
            refund_item_summary.setdefault(user_key, []).append(
                f"{result.item_name} x{result.refunded_quantity}"
            )
        cleared_count += 1
    
    logger.info(f"共清理 {cleared_count} 条超时鬼市摆摊订单。")
    if refund_item_summary:
        summary_msg = "以下物品已从鬼市摆摊中退还给玩家：\n"
        for user_key, item_list in refund_item_summary.items():
            summary_msg += f"{user_key}:\n  " + "\n  ".join(item_list) + "\n"
        logger.info(summary_msg)


@auction_view.handle(parameterless=[Cooldown(cd_time=0)])
async def auction_view_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看拍卖品列表或详情"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    arg_str = args.extract_plain_text().strip()
    
    # 如果指定了ID，查看单个拍卖品详情
    if arg_str and arg_str.isdigit():
        auction_id = arg_str
        item = xianshi_repository.get_current_auction(auction_id) # 从当前拍卖中查找
        
        if item: # 如果在当前拍卖中找到
            # 构造详情消息
            msg_list = [
                f"【拍卖品详情】",
                f"编号: {item['id']}",
                f"物品: {item['name']}",
                f"起拍价: {number_to(item['start_price'])}灵石",
                f"当前价: {number_to(item['current_price'])}灵石"
            ]
            
            if item["bids"]:
                # 获取所有出价记录并按时间降序排序
                bid_records = []
                for bidder_id_str, bid_price in item["bids"].items():
                    bid_time = item["bid_times"].get(bidder_id_str, 0)
                    bid_records.append({"bidder_id": bidder_id_str, "price": bid_price, "time": bid_time})
                
                bid_records.sort(key=lambda x: x["time"], reverse=True)
                recent_bids = bid_records[:5] # 只显示最近的5条出价
                
                msg_list.append("\n☆------竞拍记录(最近5条)------☆")
                for i, bid in enumerate(recent_bids):
                    bidder_info = sql_message.get_user_info_with_id(bid["bidder_id"])
                    bidder_name = bidder_info["user_name"] if bidder_info else str(bid["bidder_id"])
                    time_str = datetime.fromtimestamp(bid["time"]).strftime("%H:%M:%S") if bid["time"] else ""
                    msg_list.append(f"{i+1}. {bidder_name}: {number_to(bid['price'])}灵石 ({time_str})")
            
            await handle_send(bot, event, "\n".join(msg_list), md_type="拍卖", k1="查看", v1="拍卖查看", k2="竞拍", v2="拍卖竞拍", k3="帮助", v3="拍卖帮助")
            await auction_view.finish()
        
        else: # 如果在当前拍卖中没找到，尝试从历史记录中查找
            history_record_list = xianshi_repository.get_auction_history(auction_id)
            if history_record_list:
                record = history_record_list[0] # 取最新的一条
                msg_list = [
                    f"【拍卖历史详情】",
                    f"编号: {record['auction_id']}",
                    f"物品: {record['item_name']}",
                    f"状态: {record['status']}"
                ]
                
                if record["status"] == "成交":
                    winner_info = sql_message.get_user_info_with_id(record["winner_id"])
                    winner_name = winner_info["user_name"] if winner_info else str(record["winner_id"])
                    msg_list.extend([
                        f"成交价: {number_to(record['final_price'])}灵石",
                        f"买家: {winner_name}",
                        f"卖家: {record['seller_name']}",
                        f"手续费: {number_to(record['fee'])}灵石"
                    ])
                else:
                    msg_list.append(f"卖家: {record['seller_name']}")
                
                start_dt = datetime.fromtimestamp(record["start_time"]).strftime("%Y-%m-%d %H:%M")
                end_dt = datetime.fromtimestamp(record["end_time"]).strftime("%Y-%m-%d %H:%M")
                msg_list.append(f"时间: {start_dt} 至 {end_dt}")
                
                await handle_send(bot, event, "\n".join(msg_list), md_type="拍卖", k1="查看", v1="拍卖查看", k2="竞拍", v2="拍卖竞拍", k3="帮助", v3="拍卖帮助")
                await auction_view.finish()
        
        await handle_send(bot, event, "未找到该拍卖品！", md_type="拍卖", k1="查看", v1="拍卖查看", k2="竞拍", v2="拍卖竞拍", k3="帮助", v3="拍卖帮助")
        await auction_view.finish()
    
    # 如果没有指定ID，查看当前活跃拍卖品列表
    current_auctions_list = xianshi_repository.get_current_auction() # 从数据库获取所有当前拍卖品
    auction_current_status = get_auction_status()
    
    if not current_auctions_list:
        msg = "当前没有拍卖品！"
        if auction_current_status["active"]:
            msg += "\n拍卖正在进行中，但目前没有物品展示。"
        await handle_send(bot, event, msg, md_type="拍卖", k1="查看", v1="拍卖查看", k2="竞拍", v2="拍卖竞拍", k3="帮助", v3="拍卖帮助")
        await auction_view.finish()
    
    # 按照当前价格从高到低排序，显示最多20个
    current_auctions_list.sort(key=lambda x: x["current_price"], reverse=True)
    display_items = current_auctions_list[:20]
    
    title = f"【拍卖物品列表】"
    msg_list = []
    for item in display_items:
        msg_list.append(
            f"\n编号: {item['id']}\n"
            f"物品: {item['name']}\n"
            f"当前价: {number_to(item['current_price'])}灵石"
        )
    
    if auction_current_status["active"]:
        if auction_current_status["start_time"] and auction_current_status["end_time"]:
            start_time_str = auction_current_status["start_time"].strftime("%H:%M")
            end_time_str = auction_current_status["end_time"].strftime("%H:%M")
            msg_list.append(f"\n拍卖进行中，预计 {end_time_str} 结束 (开始于 {start_time_str})")
        else:
            msg_list.append("\n拍卖进行中，但时间信息不完整。")
    else:
        msg_list.append("\n拍卖当前未开启。")
    
    msg_list.append("\n输入【拍卖查看 ID】查看详情")
    start_time_display = auction_current_status["start_time"].strftime("%H:%M") if auction_current_status["start_time"] else "N/A"
    end_time_display = auction_current_status["end_time"].strftime("%H:%M") if auction_current_status["end_time"] else "N/A"
    page = ["查看", "拍卖查看", "竞拍", "拍卖竞拍", "灵石", "灵石", f"{start_time_display}/{end_time_display}"]
    await send_msg_handler(bot, event, '拍卖品', bot.self_id, msg_list, title=title, page=page)
    await auction_view.finish()

@auction_bid.handle(parameterless=[Cooldown(cd_time=0)])
async def auction_bid_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """参与拍卖竞拍"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await auction_bid.finish()
    
    args_list = args.extract_plain_text().split()
    if len(args_list) < 2:
        msg = "格式错误！正确格式：拍卖竞拍 [拍卖品ID] [出价]"
        await handle_send(bot, event, msg, md_type="拍卖", k1="竞拍", v1="拍卖竞拍", k2="查看", v2="拍卖查看", k3="帮助", v3="拍卖帮助")
        await auction_bid.finish()
    
    auction_id = args_list[0]
    bid_price_str = args_list[1]
    
    try:
        bid_price = int(bid_price_str)
        if bid_price <= 0:
            raise ValueError("出价必须是正整数")
    except ValueError:
        msg = "出价必须是正整数！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="竞拍", v1="拍卖竞拍", k2="查看", v2="拍卖查看", k3="帮助", v3="拍卖帮助")
        await auction_bid.finish()
    
    success, result_msg = await place_auction_bid(
        bot,
        str(user_info['user_id']),
        user_info['user_name'],
        auction_id,
        bid_price
    )
    await handle_send(bot, event, result_msg, md_type="拍卖", k1="竞拍", v1="拍卖竞拍", k2="查看", v2="拍卖查看", k3="帮助", v3="拍卖帮助")
    await auction_bid.finish()

@auction_add.handle(parameterless=[Cooldown(cd_time=0)])
async def auction_add_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """上架物品到拍卖等待区"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await auction_add.finish()
    
    user_id = user_info['user_id']
    args_list = args.extract_plain_text().split()
    
    auction_current_status = get_auction_status()
    if auction_current_status["active"]:
        await handle_send(bot, event, "拍卖进行中时不能上架物品，请等待拍卖结束后再上架！", md_type="拍卖", k1="上架", v1="拍卖上架", k2="我的", v2="我的拍卖", k3="帮助", v3="拍卖帮助")
        await auction_add.finish()
    
    if len(args_list) < 2:
        auction_rules = auction_config.get_auction_rules()
        msg = f"格式错误！正确格式：拍卖上架 [物品名] [底价]\n最低底价：{number_to(auction_rules['min_price'])}灵石"
        await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="我的", v2="我的拍卖", k3="帮助", v3="拍卖帮助")
        await auction_add.finish()
    
    item_name = args_list[0]
    base_price_str = args_list[1]
    
    try:
        base_price = int(base_price_str)
        auction_rules = auction_config.get_auction_rules()
        if base_price < auction_rules["min_price"]:
            msg = f"最低起拍价：{number_to(auction_rules['min_price'])}灵石！"
            await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
            await auction_add.finish()
    except ValueError:
        msg = "底价必须是整数！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
        await auction_add.finish()

    # 检查背包物品
    goods_id, goods_info = items.get_data_by_item_name(item_name)
    if not goods_id:
        msg = f"物品 {item_name} 不存在，请检查名称是否正确！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
        await auction_add.finish()
    
    # 检查物品类型是否允许拍卖 (这里与仙肆交易类型一致)
    if goods_info['type'] not in ITEM_TYPES:
        msg = f"该物品类型不允许拍卖！允许类型：{', '.join(ITEM_TYPES)}"
        await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
        await auction_add.finish()
    
    forbid_reason = get_trade_forbid_reason(goods_id, goods_info, "拍卖")
    if forbid_reason:
        msg = forbid_reason
        await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
        await auction_add.finish()

    auction_rules = auction_config.get_auction_rules()
    result = auction_queue_service.enqueue(
        _auction_queue_operation_id(event, "enqueue", user_id, goods_id),
        user_id,
        goods_id,
        item_name,
        base_price,
        user_info['user_name'],
        max_user_items=auction_rules["max_user_items"],
    )
    if result.status == "limit_reached":
        msg = f"每人最多上架{auction_rules['max_user_items']}件物品到拍卖等待区！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
        await auction_add.finish()
    if result.status == "stock_insufficient":
        msg = f"可交易的 {item_name} 数量不足，上架失败！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
        await auction_add.finish()
    if result.status == "already_queued":
        msg = f"{item_name} 已在拍卖等待区，请勿重复上架！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
        await auction_add.finish()
    if not result.succeeded:
        msg = "拍卖等待区状态已经变化，请稍后重试！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
        await auction_add.finish()
    
    msg = f"成功上架 {item_name} 到拍卖等待区！底价：{number_to(base_price)}灵石"
    if result.applied:
        record_trade_event(
            user_id,
            "拍卖上架",
            f"上架{item_name}到底价{number_to(base_price)}灵石的拍卖等待区",
            {"拍卖上架次数": 1}
        )
    await handle_send(bot, event, msg, md_type="拍卖", k1="上架", v1="拍卖上架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
    await auction_add.finish()

@auction_remove.handle(parameterless=[Cooldown(cd_time=0)])
async def auction_remove_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """从拍卖等待区下架物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await auction_remove.finish()
    
    user_id = user_info['user_id']
    item_name = args.extract_plain_text().strip()
    
    auction_current_status = get_auction_status()
    if auction_current_status["active"]:
        await handle_send(bot, event, "拍卖进行中时不能下架物品！", md_type="拍卖", k1="下架", v1="拍卖下架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
        await auction_remove.finish()
    
    if not item_name:
        msg = "请输入要下架的物品名！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="下架", v1="拍卖下架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
        await auction_remove.finish()

    goods_id, item_info = items.get_data_by_item_name(item_name)
    if goods_id:
        operation_id = _auction_queue_operation_id(
            event, "dequeue", user_id, goods_id
        )
        previous = auction_queue_service.get_operation(
            operation_id, "dequeue", user_id, goods_id
        )
        if previous is not None and previous.succeeded:
            msg = f"成功从拍卖等待区下架 {item_name}！物品已退回背包。"
            await handle_send(bot, event, msg, md_type="拍卖", k1="下架", v1="拍卖下架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
            await auction_remove.finish()
    
    # 查找用户上架的该物品
    player_items_in_queue = trade_manager.get_player_auction_items(user_id)
    item_to_remove = None
    for item in player_items_in_queue:
        if item["item_name"] == item_name: # item_name是玩家上架时的名称
            item_to_remove = item
            break
    
    if not item_to_remove:
        msg = f"没有找到名为{item_name}的上架物品在拍卖等待区！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="下架", v1="拍卖下架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
        await auction_remove.finish()
    
    item_info = items.get_data_by_item_id(item_to_remove["item_id"])
    if not item_info:
        msg = "无法读取该物品信息，请稍后重试！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="下架", v1="拍卖下架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
        await auction_remove.finish()
    operation_id = _auction_queue_operation_id(
        event, "dequeue", user_id, item_to_remove["item_id"]
    )
    result = auction_queue_service.dequeue(
        operation_id,
        user_id,
        item_to_remove["item_id"],
        item_info["type"],
    )
    if result.status == "queue_missing":
        msg = "该物品已不在拍卖等待区，请刷新后重试！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="下架", v1="拍卖下架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
        await auction_remove.finish()
    if result.status == "inventory_full":
        msg = "背包中该物品数量已达上限，无法下架！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="下架", v1="拍卖下架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
        await auction_remove.finish()
    if not result.succeeded:
        msg = "拍卖等待区状态已经变化，请稍后重试！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="下架", v1="拍卖下架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
        await auction_remove.finish()
    
    msg = f"成功从拍卖等待区下架 {item_name}！物品已退回背包。"
    if result.applied:
        record_trade_event(
            user_id,
            "拍卖下架",
            f"从拍卖等待区下架{item_name}，已退回背包",
            {"拍卖下架次数": 1}
        )
    await handle_send(bot, event, msg, md_type="拍卖", k1="下架", v1="拍卖下架", k2="my", v2="我的拍卖", k3="help", v3="拍卖帮助")
    await auction_remove.finish()

@my_auction.handle(parameterless=[Cooldown(cd_time=0)])
async def my_auction_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看我上架的拍卖物品（在等待区）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await my_auction.finish()
    
    user_id = user_info['user_id']
    player_auction_items = trade_manager.get_player_auction_items(user_id) # 从数据库获取玩家上架物品
    
    if not player_auction_items:
        msg = "您当前没有上架任何拍卖物品在等待区！"
        await handle_send(bot, event, msg, md_type="拍卖", k1="查看", v1="拍卖查看", k2="下架", v2="拍卖下架", k3="帮助", v3="拍卖帮助")
        await my_auction.finish()
    
    msg_list = [f"【我的拍卖等待区物品】"]
    for item in player_auction_items:
        msg_list.append(f"\n物品: {item['item_name']}") # item_name是玩家上架时的名称
        msg_list.append(f"起拍价: {number_to(item['start_price'])}灵石")
    
    msg_list.append("\n使用【拍卖下架 物品名】可以从等待区下架物品")
    
    await handle_send(bot, event, "\n".join(msg_list), md_type="拍卖", k1="查看", v1="拍卖查看", k2="下架", v2="拍卖下架", k3="帮助", v3="拍卖帮助")
    await my_auction.finish()

@auction_info.handle(parameterless=[Cooldown(cd_time=0)])
async def auction_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看拍卖信息和规则"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    schedule = auction_config.get_auction_schedule()
    rules = auction_config.get_auction_rules()
    auction_current_status = get_auction_status()
    
    # 获取等待上架的玩家物品数量
    player_auctions_in_queue = trade_manager.get_player_auction_items()
    total_player_items_in_queue = len(player_auctions_in_queue)
    
    # 获取拍卖历史记录数量
    auction_history_count = len(xianshi_repository.get_auction_history())
    
    msg_list = [
        "【拍卖信息】",
        f"当前状态: {'运行中' if auction_current_status['active'] else '未运行'}",
        f"自动开启: {'开启' if schedule['enabled'] else '关闭'}",
        f"自动拍卖时间: 每日{schedule['start_hour']:02d}:{schedule['start_minute']:02d}",
        f"持续时间: {schedule['duration_hours']}小时",
        f"每人最大上架数: {rules['max_user_items']}",
        f"最低起拍价: {number_to(rules['min_price'])}灵石",
        f"最低加价金额: {number_to(rules['min_bid_increment'])}灵石",
        f"最低加价百分比: {int(rules['min_increment_percent'] * 100)}%",
        f"手续费率: {int(rules['fee_rate'] * 100)}%",
        f"当前拍卖品数量: {auction_current_status['items_count']}件",
        f"等待上架的玩家物品: {total_player_items_in_queue}件",
        f"历史拍卖记录: {auction_history_count}条"
    ]
    
    if auction_current_status["active"]:
        if auction_current_status["start_time"] and auction_current_status["end_time"]:
            start_time_str = auction_current_status["start_time"].strftime("%H:%M")
            end_time_str = auction_current_status["end_time"].strftime("%H:%M")
            msg_list.append(f"\n本次拍卖时间: {start_time_str} 至 {end_time_str}")
        else:
            msg_list.append("\n拍卖进行中，但时间信息不完整。")
    
    await handle_send(bot, event, "\n".join(msg_list), md_type="拍卖", k1="查看", v1="拍卖查看", k2="上架", v2="拍卖上架", k3="帮助", v3="拍卖帮助")
    await auction_info.finish()

@auction_activity.handle(parameterless=[Cooldown(cd_time=0)])
async def auction_activity_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看拍卖活动/限时交易入口信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    schedule = auction_config.get_auction_schedule()
    rules = auction_config.get_auction_rules()
    activity_config = auction_config.get_auction_activity_config()
    auction_current_status = get_auction_status()
    current_auctions = xianshi_repository.get_current_auction() or []
    current_auctions_count = len(current_auctions)
    waiting_auctions_count = len(trade_manager.get_player_auction_items() or [])
    now = datetime.now()
    max_user_items = _safe_auction_int(rules.get("max_user_items"), 3)
    hot_items_limit = min(_safe_auction_int(activity_config.get("hot_items_limit"), 5), 5)
    recent_deals_limit = _safe_auction_int(activity_config.get("recent_deals_limit"), 5)

    msg_list = [
        "【拍卖活动】",
        f"活动状态: {'进行中' if auction_current_status['active'] else '未开启'}",
        f"计划开启: {'每日' if schedule['enabled'] else '已暂停'}{schedule['start_hour']:02d}:{schedule['start_minute']:02d}",
        f"持续时间: {schedule['duration_hours']}小时",
        f"手续费率: {int(rules['fee_rate'] * 100)}%",
        f"最低起拍价: {number_to(rules['min_price'])}灵石",
        f"最低加价: {number_to(rules['min_bid_increment'])}灵石或当前价的{int(rules['min_increment_percent'] * 100)}%",
        f"每人最多上架: {rules['max_user_items']}件",
        f"当前拍品数量: {current_auctions_count}件"
    ]

    if auction_current_status["active"]:
        start_time = auction_current_status["start_time"]
        end_time = auction_current_status["end_time"]
        if start_time and end_time:
            msg_list.append(
                f"本次时间: {_format_auction_datetime(start_time, now)} 至 {_format_auction_datetime(end_time, now)}"
            )
            msg_list.append(f"结束时间: {_format_auction_datetime(end_time, now)}")
            msg_list.append(f"距离结束: {_format_auction_duration((end_time - now).total_seconds())}")
        else:
            msg_list.append("本次时间: 拍卖进行中，时间信息不完整")
    else:
        next_start = _get_next_auction_start(schedule, now)
        if next_start:
            msg_list.append(f"下次开启: {_format_auction_datetime(next_start, now)}")
            msg_list.append(f"开启倒计时: {_format_auction_duration((next_start - now).total_seconds())}")
        else:
            msg_list.append("下次开启: 自动拍卖已暂停")
            msg_list.append("开启倒计时: 暂无")
        msg_list.append(f"等待入场拍品: {waiting_auctions_count}件")

    msg_list.append("\n☆------我的拍卖------☆")
    msg_list.extend(_format_user_auction_quota(event, current_auctions, max_user_items, auction_current_status["active"]))

    msg_list.append("\n☆------热门拍品 TOP5------☆")
    msg_list.extend(_format_hot_auction_items(current_auctions, hot_items_limit))
    if not current_auctions:
        msg_list.append("提示: 当前没有拍品，可在非拍卖期间使用【拍卖上架 物品名 底价】提交拍品。")

    msg_list.append("\n☆------最近成交------☆")
    msg_list.extend(_format_recent_auction_deals(recent_deals_limit))

    await handle_send(bot, event, "\n".join(msg_list), **AUCTION_ACTIVITY_BUTTONS)
    await auction_activity.finish()

@auction_start.handle(parameterless=[Cooldown(cd_time=0)])
async def auction_start_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """管理员手动开启拍卖"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    if not await SUPERUSER(bot, event):
        await handle_send(bot, event, "此功能仅限管理员使用！")
        await auction_start.finish()
    
    if auction_session_service.get_active_session() is not None:
        await handle_send(bot, event, "拍卖已经在运行中！", md_type="拍卖", k1="查看", v1="拍卖查看", k2="结束", v2="结束拍卖", k3="帮助", v3="拍卖帮助")
        await auction_start.finish()
    
    # 解封拍卖 (如果被封闭)
    auction_config.update_schedule({"enabled": True})
    
    # 开启拍卖
    success = start_auction_process(bot, _auction_session_operation_id(event, "start"))
    if not success:
        await handle_send(bot, event, "开启拍卖失败或没有物品可供拍卖！", md_type="拍卖", k1="查看", v1="拍卖查看", k2="结束", v2="结束拍卖", k3="帮助", v3="拍卖帮助")
        await auction_start.finish()
    
    # 重新获取最新的拍卖状态以得到准确的结束时间
    current_auction_status = get_auction_status()
    end_time_str = current_auction_status["end_time"].strftime("%H:%M")
    
    msg = f"拍卖已开启！本次拍卖将持续{auction_config.get_auction_schedule()['duration_hours']}小时，预计{end_time_str}结束。"
    await handle_send(bot, event, msg, md_type="拍卖", k1="查看", v1="拍卖查看", k2="结束", v2="结束拍卖", k3="帮助", v3="拍卖帮助")
    await auction_start.finish()

@auction_end.handle(parameterless=[Cooldown(cd_time=0)])
async def auction_end_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """管理员手动结束拍卖"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    if not await SUPERUSER(bot, event):
        await handle_send(bot, event, "此功能仅限管理员使用！")
        await auction_end.finish()
    
    active_session = auction_session_service.get_active_session()
    pending_items = xianshi_repository.get_current_auction()
    if active_session is None and not pending_items:
        await handle_send(bot, event, "拍卖当前未开启！", md_type="拍卖", k1="查看", v1="拍卖查看", k2="开启", v2="开启拍卖", k3="帮助", v3="拍卖帮助")
        await auction_end.finish()
    
    results = await end_auction_process(
        bot, _auction_session_operation_id(event, "finish")
    )
    if not results:
        await handle_send(bot, event, "结束拍卖失败或没有拍卖品需要结算！", md_type="拍卖", k1="查看", v1="拍卖查看", k2="开启", v2="开启拍卖", k3="帮助", v3="拍卖帮助")
        await auction_end.finish()
    
    # 构造结果消息
    msg_list = ["拍卖已结束！成交结果："]
    for result in results[:5]:  # 最多显示5条结果
        if result["status"] == "成交":
            winner_info = sql_message.get_user_info_with_id(result["winner_id"])
            winner_name = winner_info["user_name"] if winner_info else str(result["winner_id"])
            msg_list.append(
                f"{result['item_name']} 成交价: {number_to(result['final_price'])}灵石 "
                f"手续费: {number_to(result['fee'])}灵石 "
                f"买家: {winner_name}"
            )
        else:
            msg_list.append(f"{result['item_name']} 流拍")
    
    if len(results) > 5:
        msg_list.append(f"...等共 {len(results)} 件拍卖品。")
    
    await handle_send(bot, event, "\n".join(msg_list), md_type="拍卖", k1="查看", v1="拍卖查看", k2="开启", v2="开启拍卖", k3="帮助", v3="拍卖帮助")
    await auction_end.finish()

@auction_lock.handle(parameterless=[Cooldown(cd_time=0)])
async def auction_lock_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """管理员封闭拍卖（取消自动开启）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    if not await SUPERUSER(bot, event):
        await handle_send(bot, event, "此功能仅限管理员使用！")
        await auction_lock.finish()
    
    auction_config.set_auction_config_value("schedule", False, "enabled")
    msg = "拍卖已封闭，将不再自动开启！"
    await handle_send(bot, event, msg, md_type="拍卖", k1="解封", v1="解封拍卖", k2="信息", v2="拍卖信息", k3="帮助", v3="拍卖帮助")
    await auction_lock.finish()

@auction_unlock.handle(parameterless=[Cooldown(cd_time=0)])
async def auction_unlock_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """管理员解封拍卖（恢复自动开启）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    if not await SUPERUSER(bot, event):
        await handle_send(bot, event, "此功能仅限管理员使用！")
        await auction_unlock.finish()
    
    auction_config.set_auction_config_value("schedule", True, "enabled")
    msg = "拍卖已解封，将按照计划自动开启！"
    await handle_send(bot, event, msg, md_type="拍卖", k1="封闭", v1="封闭拍卖", k2="信息", v2="拍卖信息", k3="帮助", v3="拍卖帮助")
    await auction_unlock.finish()

@scheduler.scheduled_job(
    "cron",
    hour=auction_config.get_auction_schedule()["start_hour"],
    minute=auction_config.get_auction_schedule()["start_minute"],
    id="auto_start_auction",
    coalesce=True,
    max_instances=1,
    misfire_grace_time=300,
)
async def auto_start_auction_job():
    """根据配置时间自动开启拍卖"""
    return await run_auction_job("auto_start", _auto_start_auction_job_impl)


async def _auto_start_auction_job_impl():
    schedule_config = auction_config.get_auction_schedule()
    
    if not schedule_config["enabled"]:
        logger.info("自动拍卖功能已禁用。")
        return
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    if schedule_config.get("last_auto_start_date") == current_date:
        logger.info("今日自动拍卖已开启，跳过本次调度。")
        return  # 今日已开启过，防止重复
    
    if auction_session_service.get_active_session() is not None:
        logger.warning("拍卖已在运行中，自动开启任务跳过。")
        return

    logger.info("开始执行自动拍卖开启任务...")
    
    success = start_auction_process(
        None, f"auction-session:auto-start:{current_date}"
    )  # 传入None表示不需要Bot实例发送消息
    if success:
        # 获取最新的拍卖状态以得到准确的结束时间
        current_auction_status = get_auction_status()
        end_time_str = current_auction_status["end_time"].strftime("%H:%M")
        logger.info(f"拍卖已自动开启！本次拍卖将持续{schedule_config['duration_hours']}小时，预计{end_time_str}结束。")
    else:
        logger.warning("自动开启拍卖失败，可能因为没有物品可供拍卖。")

@scheduler.scheduled_job(
    "interval",
    minutes=5,
    id="check_auction_end",
    coalesce=True,
    max_instances=1,
    misfire_grace_time=300,
)
async def check_auction_end_job():
    """每 5 分钟看一场是否该收尾。"""
    return await run_auction_job("end_check", _check_auction_end_job_impl)


async def _check_auction_end_job_impl():
    current_auctions = xianshi_repository.get_current_auction()
    if not current_auctions:
        return

    session = auction_session_service.get_active_session()
    if session is None:
        logger.error("拍卖库内存在拍品，但数据库场次不存在，停止自动收尾。")
        return
    now_dt = datetime.now()
    end_dt = datetime.fromtimestamp(session["end_time"])
    n = len(current_auctions)

    if now_dt >= end_dt:
        logger.info(f"拍卖到点收尾，拍品 {n} 件，开始结算。")
        await end_auction_process(
            None, f"auction-session:auto-finish:{session['session_id']}"
        )
        return

    remaining_seconds = int((end_dt - now_dt).total_seconds())
    remaining_minutes = remaining_seconds // 60
    if remaining_minutes > 0 and remaining_minutes % 30 == 0:
        logger.info(f"拍卖进行中，距结束约 {remaining_minutes} 分钟。")


@DRIVER.on_startup
async def recover_orphan_auction_on_startup():
    await run_auction_job(
        "startup_reconcile",
        reconcile_auction_after_restart,
        suppress=True,
    )
