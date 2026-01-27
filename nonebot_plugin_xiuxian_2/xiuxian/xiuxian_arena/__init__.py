import random
import re
from datetime import datetime
from nonebot.log import logger
from nonebot.params import CommandArg
from nonebot import on_command
from nonebot.adapters.onebot.v11 import Bot, Message, GroupMessageEvent, PrivateMessageEvent
from nonebot.permission import SUPERUSER
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import check_user, log_message, handle_send, send_msg_handler, update_statistics_value
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager
from ..xiuxian_utils.player_fight import Player_fight
from ..xiuxian_utils.item_json import Items

items = Items()
player_data_manager = PlayerDataManager()
sql_message = XiuxianDateManage()

from .arena_limit import arena_limit
from .arena_shop import arena_shop_data

arena_challenge = on_command("竞技场挑战", priority=10, block=True)
arena_ranking = on_command("竞技场排行榜", priority=10, block=True)
arena_myinfo = on_command("我的竞技场", priority=10, block=True)
arena_help = on_command("竞技场帮助", priority=10, block=True)
arena_shop = on_command("竞技场商店", priority=10, block=True)
arena_buy = on_command("竞技场兑换", priority=10, block=True)
arena_honor = on_command("我的荣誉", priority=10, block=True)

__arena_help__ = """
⚔️ 【竞技场玩法】⚔️

• 竞技场挑战 - 进行竞技场挑战
• 竞技场商店 - 查看竞技场商店
• 竞技场兑换 - 兑换商店物品
• 我的荣誉 - 查看荣誉信息
• 竞技场排行榜 - 查看排行榜
• 我的竞技场 - 查看个人战绩

基础规则：

> • 初始积分：1000分
• 每日挑战次数：10次
• 胜利：+20积分
• 失败：积分不变
• 无匹配：+10积分

段位系统：

> • 王者（2500+）- 1000荣誉值
• 钻石（2000-2499）- 600荣誉值  
• 铂金（1500-1999）- 400荣誉值
• 黄金（1200-1499）- 300荣誉值
• 白银（1000-1199）- 200荣誉值
• 青铜（1000以下）- 100荣誉值

排名奖励（前100名额外）：

> • 第1名：+500荣誉值
• 第2-3名：+300荣誉值
• 第4-10名：+200荣誉值
• 第11-50名：+100荣誉值
• 第51-100名：+50荣誉值
"""

@arena_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def arena_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """竞技场帮助信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    await handle_send(bot, event, __arena_help__)
    await arena_help.finish()

@arena_challenge.handle(parameterless=[Cooldown(cd_time=30)])
async def arena_challenge_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """竞技场挑战"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await arena_challenge.finish()
    
    user_id = user_info['user_id']
    
    # 检查挑战次数
    if not arena_limit.can_challenge_today(user_id):
        msg = "今日挑战次数已用完，请明日再来！"
        await handle_send(bot, event, msg)
        await arena_challenge.finish()
    
    # 使用一次挑战次数
    arena_limit.use_challenge(user_id)
    
    # 随机匹配对手（积分相近的玩家）
    opponent_id = await find_arena_opponent(user_id)
    
    if opponent_id:
        # 有匹配对手，进行战斗
        user1_info = sql_message.get_user_real_info(user_id)
        user2_info = sql_message.get_user_real_info(opponent_id)
        
        # 进行战斗
        result, victor = Player_fight(user_id, opponent_id, 1, bot.self_id)
        
        # 发送战斗过程
        await send_msg_handler(bot, event, result)
        
        # 处理战斗结果
        if victor == user1_info['user_name']:
            # 挑战者胜利
            new_score, new_rank = arena_limit.update_after_battle(user_id, True)
            arena_limit.update_after_battle(opponent_id, False, is_opponent=True)
            msg = f"🎉 挑战胜利！获得{arena_limit.win_points}积分！\n当前积分：{new_score} ({new_rank})"
        else:
            # 挑战者失败
            new_score, new_rank = arena_limit.update_after_battle(user_id, False, opponent_id=opponent_id)
            msg = f"💔 挑战失败，积分不变。\n当前积分：{new_score} ({new_rank})"
    else:
        # 无匹配对手
        new_score, new_rank = arena_limit.update_after_battle(user_id, False, opponent_id=None)
        msg = f"⚪ 未找到合适对手，获得安慰积分{arena_limit.no_match_points}点！\n当前积分：{new_score} ({new_rank})"
    
    await handle_send(bot, event, msg)
    await arena_challenge.finish()

@arena_ranking.handle(parameterless=[Cooldown(cd_time=1.4)])
async def arena_ranking_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """竞技场排行榜"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    ranking = arena_limit.get_arena_ranking(limit=10)
    
    msg = "🏆 【竞技场排行榜】🏆\n"
    msg += "═" * 12 + "\n"
    
    for i, (user_id, score) in enumerate(ranking, 1):
        user_info = sql_message.get_user_info_with_id(user_id)
        if user_info:
            rank_icon = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
            icon = rank_icon[i-1] if i <= 10 else f"{i}."
            msg += f"{icon} {user_info['user_name']} - {score}分 ({arena_limit.calculate_rank(score)})\n"
    
    await handle_send(bot, event, msg)
    await arena_ranking.finish()

@arena_myinfo.handle(parameterless=[Cooldown(cd_time=1.4)])
async def arena_myinfo_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的竞技场信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await arena_myinfo.finish()
    
    user_id = user_info['user_id']
    arena_data = arena_limit.get_user_arena_info(user_id)
    
    # 计算胜率
    total_battles = arena_data['total_wins'] + arena_data['total_losses']
    win_rate = (arena_data['total_wins'] / total_battles * 100) if total_battles > 0 else 0
    
    msg = f"""
⚔️  【竞技场信息】⚔️

道号：{user_info['user_name']}
当前积分：{arena_data['score']}
当前段位：{arena_data['rank']}
今日挑战：{arena_limit.daily_challenges - arena_data['daily_challenges_used']}/{arena_limit.daily_challenges}次

战斗统计：
总战斗：{total_battles}次
胜利：{arena_data['total_wins']}次
失败：{arena_data['total_losses']}次
胜率：{win_rate:.1f}%

连胜记录：
当前连胜：{arena_data['win_streak']}次
最高连胜：{arena_data['max_win_streak']}次
"""
    await handle_send(bot, event, msg)
    await arena_myinfo.finish()

@arena_shop.handle(parameterless=[Cooldown(cd_time=1.4)])
async def arena_shop_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看竞技场商店"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    shop_items = arena_shop_data.config["商店商品"]
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await arena_shop.finish()
    
    user_id = user_info["user_id"]
    arena_info = arena_limit.get_user_arena_info(user_id)
    
    # 获取页码参数
    page_input = args.extract_plain_text().strip()
    try:
        page = int(page_input) if page_input else 1
    except ValueError:
        page = 1
    
    # 分页设置
    items_per_page = 8
    total_pages = (len(shop_items) + items_per_page - 1) // items_per_page
    page = max(1, min(page, total_pages))
    
    # 获取当前页的商品
    sorted_items = sorted(shop_items.items(), key=lambda x: x[1]["cost"])
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    current_page_items = sorted_items[start_idx:end_idx]
    
    title = f"道友当前荣誉值：{arena_info['honor_points']}点 | 段位：{arena_info['rank']}"
    msg_list = []
    msg_list.append(f"════════════\n【竞技场商店】第{page}/{total_pages}页")
    
    for item_id, item_data in current_page_items:
        item_info = items.get_data_by_item_id(item_id)
        if not item_info:
            continue
            
        # 检查段位要求
        rank_requirement = item_data.get("required_rank", "青铜")
        
        msg_list.append(
            f"编号：{item_id}\n"
            f"名称：{item_info['name']}\n"
            f"要求段位：{rank_requirement}\n" 
            f"价格：{item_data['cost']}荣誉值\n"
            f"每周限购：{item_data['weekly_limit']}个\n"
            f"════════════"
        )
    
    msg_list.append(f"提示：发送 竞技场商店+页码 查看其他页（共{total_pages}页）")
    await send_msg_handler(bot, event, "竞技场商店", bot.self_id, msg_list, title=title)
    await arena_shop.finish()

@arena_buy.handle(parameterless=[Cooldown(cd_time=1.4)])
async def arena_buy_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """竞技场商店兑换"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await arena_buy.finish()
    
    user_id = user_info["user_id"]
    msg_text = args.extract_plain_text().strip()
    
    # 解析商品编号和数量
    shop_info = re.findall(r"(\d+|\w+)\s*(\d*)", msg_text)
    
    if not shop_info:
        msg = "请输入正确的商品编号！格式：竞技场兑换 编号 [数量]"
        await handle_send(bot, event, msg)
        await arena_buy.finish()
    
    shop_id = shop_info[0][0]
    quantity = int(shop_info[0][1]) if shop_info[0][1] else 1
    
    shop_items = arena_shop_data.config["商店商品"]
    if shop_id not in shop_items:
        msg = "没有这个商品编号！"
        await handle_send(bot, event, msg)
        await arena_buy.finish()
    
    item_data = shop_items[shop_id]
    item_info = items.get_data_by_item_id(shop_id)
    arena_info = arena_limit.get_user_arena_info(user_id)
    
    # 检查段位要求
    rank_requirement = item_data.get("required_rank", "青铜")
    if not check_rank_requirement(arena_info["rank"], rank_requirement):
        msg = f"段位不足！需要{rank_requirement}段位才能购买{item_info['name']}"
        await handle_send(bot, event, msg)
        await arena_buy.finish()
    
    # 检查荣誉值是否足够
    total_cost = item_data["cost"] * quantity
    if arena_info["honor_points"] < total_cost:
        msg = f"荣誉值不足！需要{total_cost}点，当前拥有{arena_info['honor_points']}点"
        await handle_send(bot, event, msg)
        await arena_buy.finish()
    
    # 兑换商品
    new_honor = arena_info["honor_points"] - total_cost
    arena_limit.update_arena_data(user_id, {"honor_points": new_honor})
    
    # 给予物品
    sql_message.send_back(
        user_id, 
        shop_id, 
        item_info["name"], 
        item_info["type"], 
        quantity,
        1
    )
    
    msg = f"成功兑换{item_info['name']}×{quantity}，消耗{total_cost}荣誉值！"
    await handle_send(bot, event, msg)
    await arena_buy.finish()

@arena_honor.handle(parameterless=[Cooldown(cd_time=1.4)])
async def arena_honor_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看我的荣誉信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await arena_honor.finish()
    
    user_id = user_info['user_id']
    arena_data = arena_limit.get_user_arena_info(user_id)
    
    # 计算明日预计奖励
    tomorrow_honor, base_honor, ranking_bonus = arena_limit.calculate_daily_honor(user_id)
    user_ranking = arena_limit.get_user_ranking(user_id)
    
    msg = f"""
🎖️ 【我的荣誉信息】

道号：{user_info['user_name']}
当前段位：{arena_data['rank']}
当前排名：第{user_ranking}名
当前荣誉值：{arena_data['honor_points']}点
累计获得荣誉值：{arena_data['total_honor_earned']}点

明日预计奖励：
基础奖励：{base_honor}点（{arena_data['rank']}段位）
排名奖励：{ranking_bonus}点（第{user_ranking}名）
总计：{tomorrow_honor}点
"""
    await handle_send(bot, event, msg)
    await arena_honor.finish()

def check_rank_requirement(current_rank, required_rank):
    """检查段位要求"""
    rank_order = ["青铜", "白银", "黄金", "铂金", "钻石", "王者"]
    current_index = rank_order.index(current_rank)
    required_index = rank_order.index(required_rank)
    return current_index >= required_index

async def find_arena_opponent(user_id):
    """为玩家寻找合适的竞技场对手"""
    user_arena_data = arena_limit.get_user_arena_info(user_id)
    user_score = user_arena_data['score']
    
    # 获取所有玩家数据
    all_players = player_data_manager.get_all_field_data("arena", "score")
    
    # 过滤掉自己和今日已挑战过的玩家，寻找积分相近的对手
    potential_opponents = []
    for opponent_id, opponent_score in all_players:
        if opponent_id == user_id:
            continue
        
        # 积分相差在200分以内视为合适对手
        if abs(opponent_score - user_score) <= 200:
            potential_opponents.append(opponent_id)
    
    # 随机选择一个对手
    if potential_opponents:
        return random.choice(potential_opponents)
    else:
        return None

async def reset_arena_daily_challenges():
    """每日重置竞技场挑战次数并发放荣誉值奖励"""
    # 获取所有有竞技场数据的用户
    all_users = player_data_manager.get_all_field_data("arena", "score")
    
    honor_distribution = {}
    
    for user_id, _ in all_users:
        user_id = str(user_id)
        
        # 重置挑战次数
        player_data_manager.update_or_write_data(user_id, "arena", "daily_challenges_used", 0)
        player_data_manager.update_or_write_data(user_id, "arena", "last_reset_date", datetime.now().strftime("%Y-%m-%d"))
        
        # 计算并发放荣誉值
        total_honor, base_honor, ranking_bonus = arena_limit.calculate_daily_honor(user_id)
        if total_honor > 0:
            arena_limit.add_honor_points(user_id, total_honor)
            
            user_info = sql_message.get_user_info_with_id(user_id)
            honor_distribution[user_info['user_name']] = {
                'total': total_honor,
                'base': base_honor,
                'bonus': ranking_bonus
            }
    
    logger.opt(colors=True).info(f"<green>竞技场每日挑战次数已重置！荣誉值发放完成，共发放{len(honor_distribution)}名玩家</green>")
