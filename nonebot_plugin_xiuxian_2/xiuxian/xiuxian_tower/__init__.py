import re
import asyncio
import json
from nonebot_plugin_apscheduler import scheduler
from datetime import datetime
from nonebot import on_command, on_regex
from nonebot.params import CommandArg, RegexGroup
from nonebot.adapters.onebot.v11 import (
    Bot,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent
)
from nonebot.permission import SUPERUSER
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import (
    check_user, check_user_type, 
    get_msg_pic, log_message, handle_send, 
    number_to, send_msg_handler
)
from .tower_data import tower_data, PLAYERSDATA
from .tower_battle import tower_battle

# 定义命令
tower_challenge = on_command("爬塔", aliases={"挑战通天塔", "通天塔挑战"}, priority=5, block=True)
tower_continuous = on_command("连续爬塔", aliases={"通天塔速通", "速通通天塔"}, priority=5, block=True)
tower_info = on_command("通天塔信息", priority=5, block=True)
tower_rank = on_command("通天塔排行", priority=5, block=True)
tower_shop = on_command("通天塔商店", priority=5, block=True)
tower_buy = on_command("通天塔兑换", priority=5, block=True)
tower_reset = on_command("重置通天塔", permission=SUPERUSER, priority=5, block=True)

# 每月1号0点重置通天塔层数
@scheduler.scheduled_job("cron", day=1, hour=0, minute=0)
async def reset_tower_floors():
    tower_data.reset_all_floors()
    print("通天塔层数已重置")

# 每周一0点重置商店限购
@scheduler.scheduled_job("cron", day_of_week="mon", hour=0, minute=0)
async def reset_shop_limits():
    tower_data.reset_weekly_limits()
    print("通天塔商店限购已重置")

tower_help = on_command("通天塔帮助", priority=5, block=True)

@tower_help.handle()
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """通天塔帮助信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    msg = (
        "\n═══  通天塔帮助  ═════\n"
        "【挑战通天塔】 - 挑战通天塔下一层\n"
        "【速通通天塔】 - 连续挑战10层通天塔\n"
        "【通天塔信息】 - 查看当前通天塔进度\n"
        "【通天塔排行】 - 查看通天塔排行榜\n"
        "【通天塔商店】 - 查看通天塔商店商品\n"
        "【通天塔兑换+编号】 - 兑换商店商品\n"
        "════════════\n"
        "通天塔规则说明：\n"
        "1. 每月1号0点重置所有用户层数\n"
        "2. 每周一0点重置商店限购\n"
        "3. 每10层可获得额外奖励\n"
        "════════════\n"
        "积分获取方式：\n"
        "1. 每通关1层获得100积分\n"
        "2. 每通关10层额外获得500积分\n"
        "════════════\n"
        "输入对应命令开始你的通天塔之旅吧！"
    )
    
    await handle_send(bot, event, msg)
    await tower_help.finish()

@tower_challenge.handle(parameterless=[Cooldown(stamina_cost=tower_data.config["体力消耗"]["单层爬塔"], at_sender=False)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """单层爬塔"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await tower_challenge.finish()
    
    user_id = user_info["user_id"]
    success, msg = await tower_battle.challenge_floor(bot, event, user_id)
    
    await handle_send(bot, event, msg)
    log_message(user_id, msg)
    await tower_challenge.finish()

@tower_continuous.handle(parameterless=[Cooldown(stamina_cost=tower_data.config["体力消耗"]["连续爬塔"], at_sender=False)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """连续爬塔10层"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await tower_continuous.finish()
    
    user_id = user_info["user_id"]
    success, msg = await tower_battle.challenge_floor(bot, event, user_id, continuous=True)
    
    await handle_send(bot, event, msg)
    log_message(user_id, msg)
    await tower_continuous.finish()

@tower_info.handle()
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看通天塔信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await tower_info.finish()
    
    user_id = user_info["user_id"]
    tower_info_data = tower_data.get_user_tower_info(user_id)
    
    msg = (
        f"\n═══  通天塔信息  ════\n"
        f"当前层数：{tower_info_data['current_floor']}\n"
        f"历史最高：{tower_info_data['max_floor']}\n"
        f"累计积分：{tower_info_data['score']}\n"
        f"════════════\n"
        f"输入【挑战通天塔】挑战下一层\n"
        f"输入【速通通天塔】连续挑战10层"
    )
    
    await handle_send(bot, event, msg)
    await tower_info.finish()

@tower_rank.handle()
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看通天塔排行榜"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    rank_data = tower_data.get_tower_rank(50)
    
    if not rank_data:
        msg = "暂无通天塔排行榜数据！"
        await handle_send(bot, event, msg)
        await tower_rank.finish()
    
    msg_list = ["\n═══  通天塔排行榜  ════"]
    for i, (user_id, data) in enumerate(rank_data, 1):
        msg_list.append(
            f"第{i}名：{data['name']} - 第{data['floor']}层\n"
            f"达成时间：{data['time']}"
        )
    
    await send_msg_handler(bot, event, "通天塔排行榜", bot.self_id, msg_list)
    await tower_rank.finish()

@tower_shop.handle()
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看通天塔商店"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    shop_items = tower_data.config["商店商品"]
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await tower_buy.finish()
    
    user_id = user_info["user_id"]
    tower_info = tower_data.get_user_tower_info(user_id)
    
    if not shop_items:
        msg = "通天塔商店暂无商品！"
        await handle_send(bot, event, msg)
        await tower_shop.finish()
    
    # 获取页码参数
    page_input = args.extract_plain_text().strip()
    try:
        page = int(page_input) if page_input else 1
    except ValueError:
        page = 1
    
    # 分页设置
    items_per_page = 5
    total_pages = (len(shop_items) + items_per_page - 1) // items_per_page
    page = max(1, min(page, total_pages))
    
    # 获取当前页的商品
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    current_page_items = list(shop_items.items())[start_idx:end_idx]
    
    msg_list = [f"\n道友目前拥有的通天塔积分：{tower_info['score']}点"]
    msg_list.append(f"════════════\n【通天塔商店】第{page}/{total_pages}页")
    for item_id, item_data in current_page_items:
        msg_list.append(
            f"编号：{item_id}\n"
            f"名称：{item_data['desc']}\n"
            f"价格：{item_data['cost']}积分\n"
            f"每周限购：{item_data['weekly_limit']}个\n"
            f"════════════"
        )
    
    # 添加分页导航提示
    if total_pages > 1:
        msg_list.append(f"提示：发送 通天塔商店+页码 查看其他页（共{total_pages}页）")
    
    await send_msg_handler(bot, event, "通天塔商店", bot.self_id, msg_list)
    await tower_shop.finish()

@tower_buy.handle()
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """兑换通天塔商店物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await tower_buy.finish()
    
    user_id = user_info["user_id"]
    msg = args.extract_plain_text().strip()
    shop_info = re.findall(r"(\d+)\s*(\d*)", msg)
    
    if not shop_info:
        msg = "请输入正确的商品编号！"
        await handle_send(bot, event, msg)
        await tower_buy.finish()
    
    shop_id = shop_info[0][0]
    quantity = int(shop_info[0][1]) if shop_info[0][1] else 1
    
    shop_items = tower_data.config["商店商品"]
    if shop_id not in shop_items:
        msg = "没有这个商品编号！"
        await handle_send(bot, event, msg)
        await tower_buy.finish()
    
    item_data = shop_items[shop_id]
    tower_info = tower_data.get_user_tower_info(user_id)
    
    # 检查积分是否足够
    total_cost = item_data["cost"] * quantity
    if tower_info["score"] < total_cost:
        msg = f"积分不足！需要{total_cost}点，当前拥有{tower_info['score']}点"
        await handle_send(bot, event, msg)
        await tower_buy.finish()
    
    # 检查限购
    already_purchased = tower_data.get_weekly_purchases(user_id, item_data["id"])
    if already_purchased + quantity > item_data["weekly_limit"]:
        msg = (
            f"该商品每周限购{item_data['weekly_limit']}个\n"
            f"本周已购买{already_purchased}个\n"
            f"无法再购买{quantity}个！"
        )
        await handle_send(bot, event, msg)
        await tower_buy.finish()
    
    # 兑换商品
    tower_info["score"] -= total_cost
    tower_data.save_user_tower_info(user_id, tower_info)
    tower_data.update_weekly_purchase(user_id, item_data["id"], quantity)
    
    # 给予物品
    item_info = items.get_data_by_item_id(item_data["id"])
    sql_message.send_back(
        user_id, 
        item_data["id"], 
        item_info["name"], 
        item_info["type"], 
        quantity
    )
    
    msg = f"成功兑换{item_info['name']}×{quantity}，消耗{total_cost}积分！"
    await handle_send(bot, event, msg)
    await tower_buy.finish()

@tower_reset.handle()
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重置通天塔数据(管理员)"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    # 重置所有用户层数
    for user_file in PLAYERSDATA.glob("*/tower_info.json"):
        with open(user_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        data["current_floor"] = 0
        data["last_reset"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(user_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    
    msg = "所有用户的通天塔层数已重置！"
    await handle_send(bot, event, msg)
    await tower_reset.finish()
