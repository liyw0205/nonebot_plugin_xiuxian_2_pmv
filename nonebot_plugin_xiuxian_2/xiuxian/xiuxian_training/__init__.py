import time
import json
import re
from pathlib import Path
from datetime import datetime, timedelta
from ..on_compat import on_command, on_regex
from nonebot.log import logger
from nonebot.params import CommandArg, RegexGroup
from ..adapter_compat import Bot, Message, GroupMessageEvent, PrivateMessageEvent
from nonebot.permission import SUPERUSER
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import check_user, check_user_type, get_msg_pic, log_message, handle_send, send_msg_handler, update_statistics_value, send_help_message
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, leave_harm_time
from ..xiuxian_utils.item_json import Items
from .training_data import training_data
from .training_limit import training_limit
from ...paths import get_paths
from ...features.training.application import TrainingApplication
from ...features.player_state.application import PlayerStateApplication
from ...infrastructure.clock import SystemClock
from ...infrastructure.ids import UUIDGenerator
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import number_to

_player_data_manager_instance = None
_sql_message_instance = None
_items_instance = None
training_application = TrainingApplication(get_paths().game_db, get_paths().player_db)
player_state_application = PlayerStateApplication(get_paths().player_db)
runtime_clock = SystemClock()
runtime_ids = UUIDGenerator()


def configure_training_application(application: TrainingApplication) -> None:
    """Inject the composition-root application used by command handlers."""
    global training_application
    training_application = application
    if application.state_application is not None:
        training_limit.configure(application.state_application)


def _sql_message():
    global _sql_message_instance
    if _sql_message_instance is None:
        _sql_message_instance = XiuxianDateManage()
    return _sql_message_instance


def _items():
    """Load the shared item catalog only when training needs item metadata."""
    global _items_instance
    if _items_instance is None:
        _items_instance = Items()
    return _items_instance


def _run_training_action(action: str, operation_id: str, user_id: str, **payload):
    outcome = training_application.execute(
        operation_id=operation_id,
        user_id=str(user_id),
        payload={"action": action, **payload},
    )
    data = dict(outcome.data or {})
    data.setdefault("status", outcome.status)
    data["succeeded"] = outcome.ok
    from types import SimpleNamespace
    return SimpleNamespace(**data)


def _initialize_player_state(user_id: str, profile=None):
    # Compatibility fallback remains available as _sql_message().update_user_hp(user_id).
    result = player_state_application.initialize_if_empty(
        user_id,
        fallback=lambda value: _sql_message().update_user_hp(value),
    )
    if profile is not None and result.hp is not None:
        profile["hp"], profile["mp"], profile["atk"] = result.hp, result.mp, result.atk
    return result
# 定义命令
training_start = on_command("开始历练", aliases={"历练开始"}, priority=5, block=True)
training_status = on_command("历练状态", priority=5, block=True)
training_shop = on_command("历练商店", priority=5, block=True)
training_buy = on_command("历练兑换", priority=5, block=True)
training_help = on_command("历练帮助", priority=5, block=True)
training_rank = on_command("历练排行榜", priority=5, block=True)
training_integral_rank = on_command("历练积分排行榜", priority=5, block=True)

@training_help.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """历练帮助信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    msg = (
        "**历练帮助**\n"
        "---\n"
        "**指令**\n"
        "- 开始历练\n"
        "> 开始新的历练\n"
        "- 历练状态\n"
        "> 查看当前历练进度\n"
        "- 历练商店\n"
        "> 查看历练商店\n"
        "- 历练兑换+编号\n"
        "> 兑换商店商品\n"
        "- 历练排行榜\n"
        "> 查看历练排行\n"
        "- 历练积分排行榜\n"
        "> 查看历练积分排行\n"
        "\n"
        "**规则**\n"
        "> 每小时可历练一次（整点刷新）。\n"
        "> 每周一0点重置商店限购。\n"
        "> 每完成一个历练进程（12步）可获得奖励。\n"
        "\n"
        "> 常用：开始历练 / 历练状态 / 历练商店"
    )
    
    await send_help_message(bot, event, msg, k1="开始历练", v1="开始历练", k2="历练状态", v2="历练状态", k3="商店", v3="历练商店")
    await training_help.finish()

@training_start.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """开始历练"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await training_start.finish()
    
    user_id = user_info["user_id"]
    event_id = getattr(event, "message_id", None)
    operation_id = f"training-completion:{event_id}:{user_id}" if event_id else f"training-completion:{runtime_ids.new_id()}:{user_id}"

    try:
        resumed = training_application.resume_event(operation_id=operation_id, user_id=user_id)
    except Exception:
        logger.exception("历练事件恢复失败 user_id={}", user_id)
        resumed = {"status": "error"}
    if resumed is not None:
        result = (
            str(resumed.get("message", ""))
            if resumed.get("status") in {"applied", "duplicate"}
            else "历练事件结算失败：结算过程异常，请稍后再试。"
        )
        await handle_send(bot, event, result, md_type="历练", k1="开始历练", v1="开始历练", k2="历练状态", v2="历练状态", k3="商店", v3="历练商店")
        if resumed.get("status") in {"applied", "duplicate"}:
            log_message(user_id, result)
        await training_start.finish()
    
    # 检查气血
    if user_info['hp'] is None or user_info['hp'] == 0:
        _initialize_player_state(user_id, user_info)
    
    if user_info['hp'] <= user_info['exp'] / 10:
        time = leave_harm_time(user_id)
        msg = f"重伤未愈，动弹不得！距离脱离危险还需要{time}分钟！"
        await handle_send(bot, event, msg, md_type="历练", k1="再次", v1="开始历练", k2="丹药", v2="丹药背包", k3="状态", v3="我的状态")
        await training_start.finish()
    
    # 检查历练时间 - 同小时内不可重复历练
    training_info = training_application.get_state(user_id)
    now = runtime_clock.now()
    last_time = training_info["last_time"]
    
    if last_time and last_time.year == now.year and last_time.month == now.month and last_time.day == now.day and last_time.hour == now.hour:
        next_hour = (last_time + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        wait_minutes = (next_hour - now).seconds // 60
        msg = f"本小时内已历练过，下次可历练时间: {next_hour.strftime('%H:%M')} (还需等待{wait_minutes}分钟)"
        await handle_send(bot, event, msg, md_type="历练", k1="开始历练", v1="开始历练", k2="历练状态", v2="历练状态", k3="商店", v3="历练商店")
        await training_start.finish()
    
    # 开始历练 - 随机选择事件类型
    try:
        outcome = make_choice(user_id, operation_id)
        result = (
            str(outcome.get("message", ""))
            if outcome.get("status") in {"applied", "duplicate"}
            else "历练事件结算失败：结算过程异常，请稍后再试。"
        )
    except Exception:
        logger.exception("历练事件事务失败 user_id={}", user_id)
        result = "历练事件结算失败：结算过程异常，请稍后再试。"
    
    msg = f"{result}"
    await handle_send(bot, event, msg, md_type="历练", k1="开始历练", v1="开始历练", k2="历练状态", v2="历练状态", k3="商店", v3="历练商店")
    log_message(user_id, result)
    await training_start.finish()

@training_status.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看历练状态"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await training_status.finish()
    
    user_id = user_info["user_id"]
    training_info = training_application.get_state(user_id)
    now = runtime_clock.now()
    
    # 计算下次可历练时间
    if training_info["last_time"]:
        last_time = training_info["last_time"]
        in_same_hour = last_time.year == now.year and last_time.month == now.month and last_time.day == now.day and last_time.hour == now.hour
        
        if in_same_hour:
            next_time = (last_time + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            wait_minutes = (next_time - now).seconds // 60
            status_msg = f"本小时内已历练过，还需等待{wait_minutes}分钟"
            next_time_str = next_time.strftime("%H:%M")
        else:
            status_msg = "可立即开始历练"
            next_time_str = "现在"
    else:
        status_msg = "可立即开始历练"
        next_time_str = "现在"
    
    msg = (
        f"【历练状态】\n"
        f"当前状态：{status_msg}\n"
        f"下次可历练时间：{next_time_str}\n"
        f"当前进度：{training_info['progress']}/12\n"
        f"累计完成次数：{training_info['completed']}\n"
    )
    
    if training_info.get("last_event"):
        msg += f"上次历练事件：\n{training_info['last_event']}"
    else:
        msg += f"道友可以【开始历练】了"
    
    await handle_send(bot, event, msg, md_type="历练", k1="开始历练", v1="开始历练", k2="排行榜", v2="历练排行榜", k3="商店", v3="历练商店")
    await training_status.finish()

@training_shop.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看历练商店"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    shop_items = training_data.config["商店商品"]
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await training_shop.finish()
    
    user_id = user_info["user_id"]
    training_info = training_application.get_state(user_id)
    weekly_purchases = training_info["weekly_purchases"]
    
    if not shop_items:
        msg = "历练商店暂无商品！"
        await handle_send(bot, event, msg)
        await training_shop.finish()
    
    # 获取页码参数
    page_input = args.extract_plain_text().strip()
    try:
        page = int(page_input) if page_input else 1
    except ValueError:
        page = 1
    
    # 分页设置
    items_per_page = 10
    total_pages = (len(shop_items) + items_per_page - 1) // items_per_page
    page = max(1, min(page, total_pages))
    
    # 获取当前页的商品
    sorted_items = sorted(shop_items.items(), key=lambda x: int(x[0]))
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    current_page_items = sorted_items[start_idx:end_idx]
    
    title = f"道友目前拥有的历练成就点：{training_info['points']}点"
    msg_list = []
    msg_list.append(f"【历练商店】第{page}/{total_pages}页")
    
    for item_id, item_data in current_page_items:
        # 动态获取物品信息
        item_info = _items().get_data_by_item_id(item_id)
        already_purchased = int(weekly_purchases.get(str(item_id), 0))
        if not item_info:
            continue
            
        msg_list.append(
            f"编号：{item_id}\n"
            f"名称：{item_info['name']}\n"
            f"描述：{item_info.get('desc', '暂无描述')}\n"
            f"价格：{item_data['cost']}成就点\n"
            f"每周限购：{item_data['weekly_limit'] - already_purchased}/{item_data['weekly_limit']}个"
        )
    
    msg_list.append(f"提示：发送 历练商店+页码 查看其他页（共{total_pages}页）")
    page = ["翻页", f"历练商店 {page + 1}", "状态", "历练状态", "兑换", "历练兑换", f"{page}/{total_pages}"]
    await send_msg_handler(bot, event, "历练商店", bot.self_id, msg_list, title=title, page=page)
    await training_shop.finish()

@training_buy.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """兑换历练商店物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await training_buy.finish()
    
    user_id = user_info["user_id"]
    msg = args.extract_plain_text().strip()
    shop_info = re.findall(r"(\d+)\s*(\d*)", msg)
    
    if not shop_info:
        msg = "请输入正确的商品编号！"
        await handle_send(bot, event, msg, md_type="历练", k1="兑换", v1="历练兑换", k2="商店", v2="历练商店", k3="历练状态", v3="历练状态")
        await training_buy.finish()
    
    shop_id = shop_info[0][0]
    quantity = int(shop_info[0][1]) if shop_info[0][1] else 1
    
    shop_items = training_data.config["商店商品"]
    if shop_id not in shop_items:
        msg = "没有这个商品编号！"
        await handle_send(bot, event, msg, md_type="历练", k1="兑换", v1="历练兑换", k2="商店", v2="历练商店", k3="历练状态", v3="历练状态")
        await training_buy.finish()
    
    item_data = shop_items[shop_id]
    item_info = _items().get_data_by_item_id(shop_id)
    training_info = training_application.get_state(user_id)
    # 检查限购
    already_purchased = int(training_info["weekly_purchases"].get(str(shop_id), 0))
    max_quantity = item_data['weekly_limit'] - already_purchased
    if quantity > max_quantity:
        quantity = max_quantity
    if quantity <= 0:
        msg = f"{item_info['name']}已到限购无法再购买！"
        await handle_send(bot, event, msg, md_type="历练", k1="兑换", v1="历练兑换", k2="商店", v2="历练商店", k3="历练状态", v3="历练状态")
        await training_buy.finish()
                
    # 检查积分是否足够
    total_cost = item_data["cost"] * quantity
    if training_info["points"] < total_cost:
        msg = f"成就点不足！需要{total_cost}点，当前拥有{training_info['points']}点"
        await handle_send(bot, event, msg, md_type="历练", k1="兑换", v1="历练兑换", k2="商店", v2="历练商店", k3="历练状态", v3="历练状态")
        await training_buy.finish()
    
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"training-purchase:{event_id}:{user_id}" if event_id else f"training-purchase:{runtime_ids.new_id()}:{user_id}"
    try:
        purchase_result = _run_training_action(
            "purchase", operation_id, user_id,
            item_id=shop_id,
            item_name=item_info["name"],
            item_type=item_info["type"],
            quantity=quantity,
            unit_cost=item_data["cost"],
            weekly_limit=item_data["weekly_limit"],
            expected_points=training_info["points"],
            expected_weekly_purchases=training_info["weekly_purchases"],
            max_goods_num=XiuConfig().max_goods_num,
            bind_flag=1,
        )
    except Exception:
        logger.exception("历练兑换事务失败 user_id={} item_id={}", user_id, shop_id)
        await handle_send(bot, event, "历练兑换失败：结算过程异常。", md_type="历练")
        await training_buy.finish()
    if purchase_result.status == "points_insufficient":
        await handle_send(bot, event, "兑换失败：成就点不足。", md_type="历练")
        await training_buy.finish()
    if purchase_result.status == "limit_reached":
        await handle_send(bot, event, f"{item_info['name']}已到限购无法再购买！", md_type="历练")
        await training_buy.finish()
    if purchase_result.status == "inventory_full":
        await handle_send(bot, event, f"{item_info['name']}持有数量已达上限！", md_type="历练")
        await training_buy.finish()
    if purchase_result.status in {"state_changed", "user_missing"}:
        await handle_send(bot, event, "兑换未完成：活动进度已更新，请重新兑换，请重新兑换。", md_type="历练")
        await training_buy.finish()
    
    msg = f"成功兑换{item_info['name']}×{quantity}，消耗{total_cost}成就点！"
    await handle_send(bot, event, msg, md_type="历练", k1="兑换", v1="历练兑换", k2="商店", v2="历练商店", k3="历练状态", v3="历练状态")
    await training_buy.finish()

@training_rank.handle(parameterless=[Cooldown(cd_time=0)])
async def training_rank_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """历练排行榜"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await training_rank.finish()

    try:
        rank_msg = _training_rank_message("completed", "历练排行榜")
    except Exception:
        logger.exception("历练排行榜读取失败")
        rank_msg = "暂时无法查询历练排行榜，请稍后再试。"
    await handle_send(bot, event, rank_msg)
    await training_rank.finish()

@training_integral_rank.handle(parameterless=[Cooldown(cd_time=0)])
async def training_integral_rank_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """历练积分排行榜"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await training_integral_rank.finish()

    try:
        rank_msg = _training_rank_message("points", "历练积分排行榜")
    except Exception:
        logger.exception("历练积分排行榜读取失败")
        rank_msg = "暂时无法查询历练积分排行榜，请稍后再试。"
    await handle_send(bot, event, rank_msg)
    await training_integral_rank.finish()


def _training_rank_message(field: str, title: str) -> str:
    rows = training_application.leaderboard(field, limit=50)
    return f"【{title}】\n" + "".join(
        f"第{index}位 | {row['user_name']} | {number_to(row['value'])}\n"
        for index, row in enumerate(rows, start=1)
    )


def make_choice(user_id, operation_id):
    """Compatibility entry point for the feature-owned event application."""
    return training_application.run_event(
        operation_id=operation_id,
        user_id=str(user_id),
        items=_items(),
        max_goods_num=XiuConfig().max_goods_num,
    )

def training_reset_limits(operation_id, operator_id, *, chunk_size=500):
    return training_application.reset_limits(
        operation_id=operation_id,
        operator_id=operator_id,
        chunk_size=chunk_size,
    )


__all__ = ["configure_training_application", "make_choice", "training_reset_limits"]
    
