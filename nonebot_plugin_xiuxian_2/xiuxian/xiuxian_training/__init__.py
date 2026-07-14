import random
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
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager, leave_harm_time
from ..xiuxian_utils.item_json import Items
from .training_data import training_data
from .training_limit import training_limit
from .training_events import training_events
from .completion_service import TrainingCompletionService
from .event_service import TrainingEventService
from .purchase_service import TrainingPurchaseService
from .reset_service import TrainingResetService
from ...paths import get_paths
from ..xiuxian_config import XiuConfig, convert_rank
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import number_to

player_data_manager = PlayerDataManager()
sql_message = XiuxianDateManage()
items = Items()
training_completion_service = TrainingCompletionService(get_paths().game_db, get_paths().player_db)
training_event_service = TrainingEventService(get_paths().game_db, get_paths().player_db)
training_purchase_service = TrainingPurchaseService(get_paths().game_db, get_paths().player_db)
training_reset_service = TrainingResetService(get_paths().game_db, get_paths().player_db)
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
        "**修仙历练**\n"
        "---\n"
        "**指令**\n"
        "- 开始历练：开始新的历练旅程\n"
        "- 历练状态：查看当前历练进度\n"
        "- 历练商店：查看历练商店商品\n"
        "- 历练兑换+编号：兑换商店商品\n"
        "- 历练排行榜：查看历练排行榜\n"
        "- 历练积分排行榜：查看历练积分排行榜\n"
        "\n"
        "**历练规则**\n"
        "1. 每小时可进行一次历练（整点刷新）\n"
        "2. 每周一0点重置商店限购\n"
        "3. 每完成一个历练进程(12步)可获得丰厚奖励\n"
        "\n"
        "> 常用入口：开始历练 / 历练状态 / 历练商店"
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
    
    # 检查气血
    if user_info['hp'] is None or user_info['hp'] == 0:
        sql_message.update_user_hp(user_id)
    
    if user_info['hp'] <= user_info['exp'] / 10:
        time = leave_harm_time(user_id)
        msg = f"重伤未愈，动弹不得！距离脱离危险还需要{time}分钟！"
        await handle_send(bot, event, msg, md_type="历练", k1="再次", v1="开始历练", k2="丹药", v2="丹药背包", k3="状态", v3="我的状态")
        await training_start.finish()
    
    # 检查历练时间 - 同小时内不可重复历练
    training_info = training_limit.get_user_training_info(user_id)
    now = datetime.now()
    last_time = training_info["last_time"]
    
    if last_time and last_time.year == now.year and last_time.month == now.month and last_time.day == now.day and last_time.hour == now.hour:
        next_hour = (last_time + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        wait_minutes = (next_hour - now).seconds // 60
        msg = f"本小时内已历练过，下次可历练时间: {next_hour.strftime('%H:%M')} (还需等待{wait_minutes}分钟)"
        await handle_send(bot, event, msg, md_type="历练", k1="开始历练", v1="开始历练", k2="历练状态", v2="历练状态", k3="商店", v3="历练商店")
        await training_start.finish()
    
    # 开始历练 - 随机选择事件类型
    event_id = getattr(event, "message_id", None)
    operation_id = f"training-completion:{event_id}:{user_id}" if event_id else f"training-completion:{time.time_ns()}:{user_id}"
    try:
        result = make_choice(user_id, operation_id)
    except Exception:
        logger.exception("历练事件事务失败 user_id={}", user_id)
        result = "历练事件结算失败，请稍后重试。"
    
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
    training_info = training_limit.get_user_training_info(user_id)
    now = datetime.now()
    
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
    training_info = training_limit.get_user_training_info(user_id)
    
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
        item_info = items.get_data_by_item_id(item_id)
        already_purchased = training_limit.get_weekly_purchases(user_id, item_id)
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
    item_info = items.get_data_by_item_id(shop_id)
    training_info = training_limit.get_user_training_info(user_id)
    # 检查限购
    already_purchased = training_limit.get_weekly_purchases(user_id, shop_id)
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
    operation_id = f"training-purchase:{event_id}:{user_id}" if event_id else f"training-purchase:{time.time_ns()}:{user_id}"
    try:
        purchase_result = training_purchase_service.purchase(
            operation_id, user_id, shop_id, item_info["name"], item_info["type"], quantity,
            item_data["cost"], item_data["weekly_limit"], training_info["points"],
            training_info["weekly_purchases"], XiuConfig().max_goods_num, 1,
        )
    except Exception:
        logger.exception("历练兑换事务失败 user_id={} item_id={}", user_id, shop_id)
        await handle_send(bot, event, "历练兑换失败，请稍后重试。", md_type="历练")
        await training_buy.finish()
    if purchase_result.status == "points_insufficient":
        await handle_send(bot, event, "成就点状态已变化，当前成就点不足！", md_type="历练")
        await training_buy.finish()
    if purchase_result.status == "limit_reached":
        await handle_send(bot, event, f"{item_info['name']}已到限购无法再购买！", md_type="历练")
        await training_buy.finish()
    if purchase_result.status == "inventory_full":
        await handle_send(bot, event, f"{item_info['name']}持有数量已达上限！", md_type="历练")
        await training_buy.finish()
    if purchase_result.status in {"state_changed", "user_missing"}:
        await handle_send(bot, event, "历练兑换状态已变化，请重新兑换！", md_type="历练")
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

    # 获取所有用户的completed数据
    all_user_integral = player_data_manager.get_all_field_data("training", "completed")
    
    # 排序数据
    sorted_integral = sorted(all_user_integral, key=lambda x: x[1], reverse=True)
    
    # 生成排行榜
    rank_msg = "【历练排行榜】\n"
    for i, (user_id, integral) in enumerate(sorted_integral[:50], start=1):
        user_info = sql_message.get_user_info_with_id(user_id)
        rank_msg += f"第{i}位 | {user_info['user_name']} | {number_to(integral)}\n"
    
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

    # 获取所有用户的completed数据
    all_user_integral = player_data_manager.get_all_field_data("training", "points")
    
    # 排序数据
    sorted_integral = sorted(all_user_integral, key=lambda x: x[1], reverse=True)
    
    # 生成排行榜
    rank_msg = "【历练积分排行榜】\n"
    for i, (user_id, integral) in enumerate(sorted_integral[:50], start=1):
        user_info = sql_message.get_user_info_with_id(user_id)
        rank_msg += f"第{i}位 | {user_info['user_name']} | {number_to(integral)}\n"
    
    await handle_send(bot, event, rank_msg)
    await training_integral_rank.finish()

def make_choice(user_id, operation_id):
    """进行历练选择"""
    training_info = training_limit.get_user_training_info(user_id)
    expected_training_info = training_info.copy()
    expected_training_info["weekly_purchases"] = dict(training_info["weekly_purchases"])
    if isinstance(expected_training_info["last_time"], datetime):
        expected_training_info["last_time"] = expected_training_info["last_time"].strftime("%Y-%m-%d %H:%M:%S")
    user_info = sql_message.get_user_info_with_id(user_id)
    now = datetime.now()
    
    # 记录本次历练时间
    training_info["last_time"] = now
    
    # weights = {  # 等价于原版
    #     "progress_plus_1": 33,
    #     "progress_plus_2": 20,
    #     "nothing": 27,
    #     "progress_minus_1": 13,
    #     "progress_minus_2": 7
    # }
    weights = {
        "progress_plus_1": 35,
        "progress_plus_2": 30,
        "nothing": 20,
        "progress_minus_1": 10,
        "progress_minus_2": 5,
    }
    # 随机选择事件
    event_type = random.choices(list(weights.keys()), weights=list(weights.values()))[0]
    
    # 调用事件处理器，传入用户信息
    event_result = training_events.handle_event(user_id, user_info, event_type)
    stone_delta = int(event_result.get("amount", 0)) if isinstance(event_result, dict) and event_result.get("type") == "stone" else 0
    exp_delta = int(event_result.get("amount", 0)) if isinstance(event_result, dict) and event_result.get("type") == "exp" else 0
    hp_delta = int(event_result.get("amount", 0)) if isinstance(event_result, dict) and event_result.get("type") == "hp" else 0
    event_items = []
    if isinstance(event_result, dict) and event_result.get("type") == "item":
        item_info = items.get_data_by_item_id(event_result["item_id"])
        event_items.append({"id": event_result["item_id"], "name": event_result["item_name"], "type": item_info["type"], "amount": -1 if event_result.get("lost") else 1})
    
    # 更新进度 - 默认+1
    base_progress = 1
    
    if "plus_1" in event_type:  # 小奖励: +1 (总+2)
        progress_change = base_progress + 1
    elif "plus_2" in event_type:  # 大奖励: +1 (总+2)
        progress_change = base_progress + 1
    elif "minus_1" in event_type:  # 小惩罚: -1 (总0)
        progress_change = base_progress - 1
    elif "minus_2" in event_type:  # 大惩罚: -2 (总-1)
        progress_change = base_progress - 2
    else:  # nothing: 0 (总+1)
        progress_change = base_progress
    
    training_info["progress"] = max(0, training_info["progress"] + progress_change)
    
    # 处理事件结果
    if isinstance(event_result, dict):
        # 更新成就点
        if event_result.get("type") == "points":
            training_info["points"] += event_result["amount"]
        
        # 记录最后事件
        training_info["last_event"] = event_result.get("message", "")
    else:
        training_info["last_event"] = str(event_result)
    
    # 检查是否完成一个进程
    if training_info["progress"] >= 12:
        training_info["progress"] = 0
        training_info["completed"] += 1
        training_info["max_progress"] = max(training_info["max_progress"], 12)
        user_rank = convert_rank(user_info["level"])[0]

        # 完成奖励
        exp_reward = int(user_info["exp"] * 0.01)  # 1%修为
        stone_reward = random.randint(5000000, 10000000)  # 500万-1000万灵石
        points_reward = 1000  # 1000成就点
        exp_reward = int(exp_reward * min(0.1 * max(user_rank // 3, 1), 1))
        training_info["points"] += points_reward
        
        # 添加随机物品奖励
        min_rank = max(user_rank - 16, 16)
        item_rank = random.randint(min_rank, min_rank + 20)
        item_types = ["功法", "神通", "药材"]
        item_type = random.choice(item_types)
        item_id_list = items.get_random_id_list_by_rank_and_item_type(item_rank, item_type)
        
        if item_id_list:
            item_id = random.choice(item_id_list)
            item_info = items.get_data_by_item_id(item_id)
            reward_items = [{"id": item_id, "name": item_info["name"], "type": item_info["type"], "amount": 1}]
            item_reward_msg = f"\n随机物品：{item_info['level']}:{item_info['name']}"
        else:
            reward_items = []
            item_reward_msg = ""
            
        training_info["last_event"] += (
            f"\n恭喜道友完成一个历练进程！获得：\n"
            f"修为+{number_to(exp_reward)}\n"
            f"灵石+{number_to(stone_reward)}\n"
            f"成就点+{points_reward}{item_reward_msg}"
        )
    
    # 更新最高进度
    training_info["max_progress"] = max(training_info["max_progress"], training_info["progress"])
    
    if training_info["completed"] > expected_training_info["completed"]:
        saved_training_info = training_info.copy()
        saved_training_info["weekly_purchases"] = dict(training_info["weekly_purchases"])
        if isinstance(saved_training_info["last_time"], datetime):
            saved_training_info["last_time"] = saved_training_info["last_time"].strftime("%Y-%m-%d %H:%M:%S")
        stone_delta += stone_reward
        exp_delta += exp_reward
        event_items.extend(reward_items)

    saved_training_info = training_info.copy()
    saved_training_info["weekly_purchases"] = dict(training_info["weekly_purchases"])
    if isinstance(saved_training_info["last_time"], datetime):
        saved_training_info["last_time"] = saved_training_info["last_time"].strftime("%Y-%m-%d %H:%M:%S")
    settlement = training_event_service.apply(
        operation_id, user_id, expected_training_info, saved_training_info,
        {key: user_info[key] for key in ("stone", "exp", "hp", "mp")},
        stone_delta, exp_delta, hp_delta, event_items, XiuConfig().max_goods_num,
    )
    if not settlement.succeeded:
        return "历练事件结算失败，请稍后重试。"
    if settlement.status == "duplicate":
        return settlement.message

    return training_info["last_event"]

def training_reset_limits(operation_id, operator_id, *, chunk_size=500):
    return training_reset_service.reset(
        operation_id,
        operator_id,
        chunk_size=chunk_size,
    )
    
