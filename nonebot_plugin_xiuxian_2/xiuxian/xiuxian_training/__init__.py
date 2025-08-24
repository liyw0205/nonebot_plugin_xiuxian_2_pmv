import random
import json
import re
from pathlib import Path
from datetime import datetime, timedelta
from nonebot import on_command, on_regex
from nonebot.params import CommandArg, RegexGroup
from nonebot.adapters.onebot.v11 import Bot, Message, GroupMessageEvent, PrivateMessageEvent
from nonebot.permission import SUPERUSER
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import check_user, check_user_type, get_msg_pic, log_message, handle_send, send_msg_handler
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, leave_harm_time
from .training_data import training_data, PLAYERSDATA

sql_message = XiuxianDateManage()

# 定义命令
training_start = on_command("开始历练", aliases={"历练开始"}, priority=5, block=True)
training_status = on_command("历练状态", priority=5, block=True)
training_shop = on_command("历练商店", priority=5, block=True)
training_buy = on_command("历练兑换", priority=5, block=True)
training_rank = on_command("历练排行", priority=5, block=True)
training_reset = on_command("重置历练", permission=SUPERUSER, priority=5, block=True)
training_help = on_command("历练帮助", priority=5, block=True)

@training_help.handle()
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """历练帮助信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    msg = (
        "\n═══  修仙历练  ═════\n"
        "【开始历练】 - 开始新的历练旅程\n"
        "【历练状态】 - 查看当前历练进度\n"
        "【历练商店】 - 查看历练商店商品\n"
        "【历练兑换+编号】 - 兑换商店商品\n"
        "【历练排行】 - 查看历练排行榜\n"
        "════════════\n"
        "历练规则说明：\n"
        "1. 每小时可进行一次历练（整点刷新）\n"
        "2. 每周一0点重置商店限购\n"
        "3. 每完成一个历练进程(12步)可获得丰厚奖励\n"
        "════════════\n"
        "输入对应命令开始你的历练之旅吧！"
    )
    
    await handle_send(bot, event, msg)
    await training_help.finish()

@training_start.handle(parameterless=[Cooldown(at_sender=False)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """开始历练"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await training_start.finish()
    
    user_id = user_info["user_id"]
    
    # 检查气血
    if user_info['hp'] is None or user_info['hp'] == 0:
        sql_message.update_user_hp(user_id)
    
    if user_info['hp'] <= user_info['exp'] / 10:
        time = leave_harm_time(user_id)
        msg = f"重伤未愈，动弹不得！距离脱离危险还需要{time}分钟！"
        await handle_send(bot, event, msg)
        await training_start.finish()
    
    # 检查历练时间 - 同小时内不可重复历练
    training_info = training_data.get_user_training_info(user_id)
    now = datetime.now()
    last_time = training_info["last_time"]
    
    if last_time and last_time.year == now.year and last_time.month == now.month and last_time.day == now.day and last_time.hour == now.hour:
        next_hour = (last_time + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        wait_minutes = (next_hour - now).seconds // 60
        msg = f"本小时内已历练过，下次可历练时间: {next_hour.strftime('%H:%M')} (还需等待{wait_minutes}分钟)"
        await handle_send(bot, event, msg)
        await training_start.finish()
    
    # 开始历练 - 随机选择事件类型
    choice_type = random.randint(1, 3)  # 1:前进 2:后退 3:休息
    success, result = training_data.make_choice(user_id, choice_type)
    
    if not success:
        await handle_send(bot, event, result)
        await training_start.finish()
    
    msg = f"道友开始了新的历练旅程！\n{result}"
    await handle_send(bot, event, msg)
    await training_start.finish()

@training_status.handle()
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看历练状态"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await training_status.finish()
    
    user_id = user_info["user_id"]
    training_info = training_data.get_user_training_info(user_id)
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
        f"\n═══  历练状态  ═════\n"
        f"当前状态：{status_msg}\n"
        f"下次可历练时间：{next_time_str}\n"
        f"当前进度：{training_info['progress']}/12\n"
        f"累计完成次数：{training_info['completed']}\n"
        f"════════════\n"
        f"输入【开始历练】开始新的历练"
    )
    
    if training_info.get("last_event"):
        msg += f"\n════════════\n上次历练事件：\n{training_info['last_event']}"
    
    await handle_send(bot, event, msg)
    await training_status.finish()

@training_shop.handle()
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看历练商店"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    shop_items = training_data.config["商店商品"]
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await training_shop.finish()
    
    user_id = user_info["user_id"]
    training_info = training_data.get_user_training_info(user_id)
    
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
    items_per_page = 5
    total_pages = (len(shop_items) + items_per_page - 1) // items_per_page
    page = max(1, min(page, total_pages))
    
    # 获取当前页的商品
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    current_page_items = list(shop_items.items())[start_idx:end_idx]
    
    msg_list = [f"\n道友目前拥有的历练成就点：{training_info['points']}点"]
    msg_list.append(f"════════════\n【历练商店】第{page}/{total_pages}页")
    for item_id, item_data in current_page_items:
        msg_list.append(
            f"编号：{item_id}\n"
            f"名称：{item_data['desc']}\n"
            f"价格：{item_data['cost']}成就点\n"
            f"每周限购：{item_data['weekly_limit']}个\n"
            f"════════════"
        )
    
    # 添加分页导航提示
    if total_pages > 1:
        msg_list.append(f"提示：发送 历练商店+页码 查看其他页（共{total_pages}页）")
    
    await send_msg_handler(bot, event, "历练商店", bot.self_id, msg_list)
    await training_shop.finish()

@training_buy.handle()
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """兑换历练商店物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await training_buy.finish()
    
    user_id = user_info["user_id"]
    msg = args.extract_plain_text().strip()
    shop_info = re.findall(r"(\d+)\s*(\d*)", msg)
    
    if not shop_info:
        msg = "请输入正确的商品编号！"
        await handle_send(bot, event, msg)
        await training_buy.finish()
    
    shop_id = shop_info[0][0]
    quantity = int(shop_info[0][1]) if shop_info[0][1] else 1
    
    shop_items = training_data.config["商店商品"]
    if shop_id not in shop_items:
        msg = "没有这个商品编号！"
        await handle_send(bot, event, msg)
        await training_buy.finish()
    
    item_data = shop_items[shop_id]
    training_info = training_data.get_user_training_info(user_id)
    
    # 检查积分是否足够
    total_cost = item_data["cost"] * quantity
    if training_info["points"] < total_cost:
        msg = f"成就点不足！需要{total_cost}点，当前拥有{training_info['points']}点"
        await handle_send(bot, event, msg)
        await training_buy.finish()
    
    # 检查限购
    already_purchased = training_data.get_weekly_purchases(user_id, item_data["id"])
    if already_purchased + quantity > item_data["weekly_limit"]:
        msg = (
            f"该商品每周限购{item_data['weekly_limit']}个\n"
            f"本周已购买{already_purchased}个\n"
            f"无法再购买{quantity}个！"
        )
        await handle_send(bot, event, msg)
        await training_buy.finish()
    
    # 兑换商品
    training_info["points"] -= total_cost
    training_data.save_user_training_info(user_id, training_info)
    training_data.update_weekly_purchase(user_id, item_data["id"], quantity)
    
    # 给予物品
    item_info = items.get_data_by_item_id(item_data["id"])
    sql_message.send_back(
        user_id, 
        item_data["id"], 
        item_info["name"], 
        item_info["type"], 
        quantity
    )
    
    msg = f"成功兑换{item_info['name']}×{quantity}，消耗{total_cost}成就点！"
    await handle_send(bot, event, msg)
    await training_buy.finish()

@training_rank.handle()
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看历练排行榜"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    rank_data = training_data.get_training_rank(50)
    
    if not rank_data:
        msg = "暂无历练排行榜数据！"
        await handle_send(bot, event, msg)
        await training_rank.finish()
    
    msg_list = ["\n═══  历练排行榜  ═════════"]
    for i, (user_id, data) in enumerate(rank_data, 1):
        msg_list.append(
            f"第{i}名：{data['name']} - 完成{data['completed']}次\n"
            f"最高进度：{data['max_progress']}步"
        )
    
    await send_msg_handler(bot, event, "历练排行榜", bot.self_id, msg_list)
    await training_rank.finish()

@training_reset.handle()
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重置历练数据(管理员)"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    # 重置所有用户数据
    for user_file in PLAYERSDATA.glob("*/training_info.json"):
        with open(user_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        data["progress"] = 0
        data["last_time"] = None
        data["points"] = 0
        data["completed"] = 0
        data["max_progress"] = 0
        data["last_event"] = ""
        
        with open(user_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    
    msg = "所有用户的历练数据已重置！"
    await handle_send(bot, event, msg)
    await training_reset.finish()
