import asyncio
import random
import re
import time
from datetime import datetime
from nonebot.log import logger
from nonebot.params import CommandArg
from ..on_compat import on_command
from ..adapter_compat import Bot, Message, GroupMessageEvent, PrivateMessageEvent
from nonebot.permission import SUPERUSER
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import check_user, log_message, handle_send, send_msg_handler, update_statistics_value, number_to, send_help_message
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager
from ..xiuxian_utils.player_fight import BattleSystem, Entity, apply_player_buffs, get_players_attributes
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import XiuConfig
from ...paths import get_paths

items = Items()
player_data_manager = PlayerDataManager()
sql_message = XiuxianDateManage()

from .arena_limit import arena_limit
from .arena_shop import arena_shop_data
from .transaction_service import ArenaPurchaseService
from .transaction_service import ArenaChallengePurchaseService
from .transaction_service import ArenaChallengeTicketService
from .transaction_service import ArenaChallengeSettlementService
from .transaction_service import ArenaWeeklyRankReductionService
from .transaction_service import ArenaSeasonRewardService

arena_purchase_service = ArenaPurchaseService(get_paths().game_db, get_paths().player_db)
arena_challenge_purchase_service = ArenaChallengePurchaseService(get_paths().game_db, get_paths().player_db)
arena_challenge_ticket_service = ArenaChallengeTicketService(get_paths().game_db, get_paths().player_db)
arena_challenge_settlement_service = ArenaChallengeSettlementService(get_paths().game_db, get_paths().player_db)
arena_weekly_rank_reduction_service = ArenaWeeklyRankReductionService(get_paths().player_db)
arena_season_reward_service = ArenaSeasonRewardService(get_paths().game_db, get_paths().player_db)

arena_challenge = on_command("竞技场挑战", priority=10, block=True)
arena_view = on_command("竞技场查看", priority=10, block=True)
arena_ranking = on_command("竞技场排行榜", priority=10, block=True)
arena_myinfo = on_command("我的竞技场", priority=10, block=True)
arena_help = on_command("竞技场帮助", priority=10, block=True)
arena_shop = on_command("竞技场商店", priority=10, block=True)
arena_buy = on_command("竞技场兑换", priority=10, block=True)
arena_honor = on_command("我的荣誉", priority=10, block=True)
arena_buy_challenge = on_command("竞技场购买次数", aliases={"购买竞技场次数", "竞技场买次数"}, priority=10, block=True)

ARENA_CHALLENGE_BUY_COST = 2000000
ARENA_CHALLENGE_STAMINA_COST = 0


def _arena_fight(user_id, opponent_id, bot_id, operation_id):
    random_state = random.getstate()
    try:
        random.seed(operation_id)
        players = []
        for team_id, current_id in enumerate((user_id, opponent_id)):
            data = get_players_attributes(current_id)
            attributes = data["属性"]
            attributes["natal_data"] = data.get("本命法宝")
            player = Entity(attributes, team_id=team_id)
            apply_player_buffs(player, data)
            players.append(player)
        # BattleSystem requires two teams, not a single flat player list.
        return BattleSystem([players[0]], [players[1]], bot_id).run_battle()
    finally:
        random.setstate(random_state)


def _arena_final_vitals(status_list, user_id, fallback):
    for item in status_list:
        for attributes in item.values():
            if str(attributes.get("user_id")) == str(user_id):
                hp_multiplier = attributes.get("hp_multiplier", 1) or 1
                mp_multiplier = attributes.get("mp_multiplier", 1) or 1
                return max(1, int(attributes.get("hp", 1) / hp_multiplier)), max(1, int(attributes.get("mp", 1) / mp_multiplier))
    return int(fallback["hp"]), int(fallback["mp"])


def _arena_challenge_result_message(result):
    if result.outcome == "win":
        return (
            f"挑战胜利！获得{result.score_delta}积分！\n"
            f"当前积分：{result.challenger_score} ({result.challenger_rank})"
        )
    if result.outcome in {"loss", "draw"}:
        return (
            "挑战失败，积分不变。\n"
            f"当前积分：{result.challenger_score} ({result.challenger_rank})"
        )
    return (
        f"未找到有效对手，获得安慰积分{result.score_delta}点！\n"
        f"当前积分：{result.challenger_score} ({result.challenger_rank})"
    )

__arena_help__ = """
竞技场玩法

指令
- 竞技场挑战：进行竞技场挑战
- 竞技场商店：查看竞技场商店
- 竞技场兑换：兑换商店物品
- 竞技场购买次数 [次数]：花费灵石购买今日挑战次数
- 我的荣誉：查看荣誉信息
- 竞技场排行榜：查看排行榜
- 我的竞技场：查看个人战绩
- 竞技场查看：查看与你积分最近的3位道友

基础规则
- 初始积分：1000分
- 每日挑战次数：10次
- 每日购买次数：每次2000000灵石，最多3次
- 胜利：+20积分
- 失败：积分不变
- 无匹配：+10积分

段位系统
- 王者（3200+）：1000荣誉值
- 钻石（2700-3199）：600荣誉值
- 铂金（2300-2699）：400荣誉值
- 黄金（1900-2299）：300荣誉值
- 白银（1500-1899）：200荣誉值
- 青铜（1500以下）：100荣誉值

排名奖励（前100名额外）
- 第1名：+500荣誉值
- 第2-3名：+300荣誉值
- 第4-10名：+200荣誉值
- 第11-50名：+100荣誉值
- 第51-100名：+50荣誉值
""".strip()

# 竞技场最近查看对手缓存
# {
#   user_id: {
#       "targets": [{"user_id": "...", "score": 1000}, ...],
#       "expire_time": datetime
#   }
# }
arena_opponent_cache = {}
ARENA_CACHE_EXPIRE_SECONDS = 180

@arena_help.handle(parameterless=[Cooldown(cd_time=0)])
async def arena_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """竞技场帮助信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    await send_help_message(
        bot, event, __arena_help__,
        k1="挑战", v1="竞技场挑战",
        k2="商店", v2="竞技场商店",
        k3="排行", v3="竞技场排行榜"
    )
    await arena_help.finish()

@arena_challenge.handle(parameterless=[Cooldown(cd_time=30)])
async def arena_challenge_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """竞技场挑战，支持挑战最近查看缓存的1/2/3号对手"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await arena_challenge.finish()
    
    user_id = str(user_info['user_id'])
    arg_text = args.extract_plain_text().strip()
    event_id = str(
        getattr(event, "message_id", "")
        or getattr(event, "id", "")
        or time.time_ns()
    )
    operation_id = f"arena-challenge:{event_id}:{user_id}"
    previous = arena_challenge_settlement_service.get_result(operation_id, user_id)
    if previous is not None:
        if not previous.succeeded:
            await handle_send(bot, event, "竞技场挑战请求冲突，请重新操作。")
            await arena_challenge.finish()
        await handle_send(bot, event, _arena_challenge_result_message(previous))
        await arena_challenge.finish()

    opponent_id = None
    use_cached_target = False

    # 支持竞技场挑战 1/2/3
    if arg_text in {"1", "2", "3"}:
        cache_targets = get_arena_opponent_cache(user_id)
        if not cache_targets:
            await handle_send(bot, event, "最近查看缓存不存在或已过期，请先发送【竞技场查看】")
            await arena_challenge.finish()

        idx = int(arg_text) - 1
        if idx < 0 or idx >= len(cache_targets):
            await handle_send(bot, event, f"缓存中没有第{arg_text}位对手，请先重新发送【竞技场查看】")
            await arena_challenge.finish()

        opponent_id = str(cache_targets[idx]["user_id"])
        use_cached_target = True
    else:
        # 无参数则随机匹配
        opponent_id = await find_arena_opponent(user_id, operation_id)

    arena_info = arena_limit.get_user_arena_info(user_id)
    challenger_player = sql_message.get_user_info_with_id(user_id)
    challenged_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    challenge_cap = arena_limit.daily_challenges + int(
        arena_info.get("daily_extra_challenges", 0)
    )
    if int(arena_info.get("daily_challenges_used", 0)) >= challenge_cap:
        await handle_send(bot, event, "今日挑战次数已用完，请明日再来！")
        await arena_challenge.finish()
    if int(challenger_player.get("user_stamina", 0)) < ARENA_CHALLENGE_STAMINA_COST:
        await handle_send(bot, event, "体力不足，无法发起竞技场挑战。")
        await arena_challenge.finish()
    challenger_arena = dict(arena_info)
    challenger_player = dict(challenger_player)
    opponent_arena = opponent_player = None
    outcome, final_challenger = "no_match", (int(challenger_player["hp"]), int(challenger_player["mp"]))
    final_opponent = (None, None)
    battle_messages = None

    if opponent_id:
        opponent_arena = arena_limit.get_user_arena_info(opponent_id)
        opponent_player = sql_message.get_user_info_with_id(opponent_id)
        if opponent_player:
            battle_messages, winner, status_list = _arena_fight(
                user_id, opponent_id, bot.self_id, operation_id
            )
            final_challenger = _arena_final_vitals(status_list, user_id, challenger_player)
            final_opponent = _arena_final_vitals(status_list, opponent_id, opponent_player)
            outcome = "win" if winner == 0 else "loss" if winner == 1 else "draw"
        else:
            opponent_id = None
            opponent_arena = None

    settlement = arena_challenge_settlement_service.settle(
        operation_id, user_id, opponent_id, outcome,
        challenge_cap,
        ARENA_CHALLENGE_STAMINA_COST, challenged_at,
        challenger_arena, opponent_arena, challenger_player, opponent_player,
        final_challenger[0], final_challenger[1], final_opponent[0], final_opponent[1],
        arena_limit.win_points, arena_limit.lose_points, arena_limit.no_match_points,
    )
    if not settlement.succeeded:
        if settlement.status == "limit_reached":
            message = "今日挑战次数已用完，请明日再来！"
        elif settlement.status == "stamina_insufficient":
            message = "体力不足，无法发起竞技场挑战。"
        else:
            message = "竞技场挑战状态已变化，请重新操作。"
        await handle_send(bot, event, message)
        await arena_challenge.finish()
    if use_cached_target:
        clear_arena_opponent_cache(user_id)
    if battle_messages is not None:
        await send_msg_handler(bot, event, battle_messages)
    msg = _arena_challenge_result_message(settlement)
    
    await handle_send(bot, event, msg)
    await arena_challenge.finish()

@arena_view.handle(parameterless=[Cooldown(cd_time=5)])
async def arena_view_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """竞技场查看：优先展示缓存，没有缓存才重新生成"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await arena_view.finish()

    user_id = str(user_info['user_id'])
    my_arena_info = arena_limit.get_user_arena_info(user_id)
    my_score = int(my_arena_info['score'])

    # 先读缓存
    cache_targets = get_arena_opponent_cache(user_id)
    from_cache = False

    if cache_targets:
        nearest_three = []
        for item in cache_targets:
            opponent_id = str(item.get("user_id", ""))
            cached_score = item.get("score", 0)

            opponent_user_info = sql_message.get_user_info_with_id(opponent_id)
            if not opponent_user_info:
                continue

            try:
                cached_score = int(cached_score)
            except (TypeError, ValueError):
                cached_score = 0

            nearest_three.append({
                "user_id": opponent_id,
                "user_name": opponent_user_info["user_name"],
                "score": cached_score,
                "diff": abs(cached_score - my_score)
            })

        if nearest_three:
            from_cache = True
        else:
            clear_arena_opponent_cache(user_id)
            cache_targets = None

    # 没缓存才重新生成
    if not cache_targets:
        all_players = player_data_manager.get_all_field_data("arena", "score")
        if not all_players:
            await handle_send(bot, event, "当前竞技场暂无其他对手。")
            await arena_view.finish()

        candidates = []
        for opponent_id, opponent_score in all_players:
            opponent_id = str(opponent_id)
            if opponent_id == user_id:
                continue

            try:
                opponent_score = int(opponent_score)
            except (TypeError, ValueError):
                continue

            opponent_user_info = sql_message.get_user_info_with_id(opponent_id)
            if not opponent_user_info:
                continue

            candidates.append({
                "user_id": opponent_id,
                "user_name": opponent_user_info["user_name"],
                "score": opponent_score,
                "diff": abs(opponent_score - my_score)
            })

        if not candidates:
            await handle_send(bot, event, "当前竞技场暂无可挑战的对手。")
            await arena_view.finish()

        candidates.sort(key=lambda x: (x["diff"], x["score"]))
        nearest_three = candidates[:3]

        # 写缓存
        cache_targets = [{"user_id": x["user_id"], "score": x["score"]} for x in nearest_three]
        set_arena_opponent_cache(user_id, cache_targets)

    msg_lines = [
        "⚔️ 【竞技场查看】",
        f"当前积分：{my_score}",
    ]

    if from_cache:
        msg_lines.append("以下为最近一次查看缓存（可能不是实时积分）：")
    else:
        msg_lines.append("以下是与你积分最近的3位道友：")

    for idx, opponent in enumerate(nearest_three, start=1):
        msg_lines.append(
            f"{idx}. {opponent['user_name']} | 积分：{opponent['score']} | 差值：{opponent['diff']}"
        )

    msg_lines.append("")
    msg_lines.append("请输入【竞技场挑战 1】、【竞技场挑战 2】或【竞技场挑战 3】发起挑战")
    msg_lines.append("缓存有效期：3分钟，被挑战后自动清除")
    msg_lines.append("挑战时会按对手实时数据进行结算")

    await handle_send(
        bot, event, "\n".join(msg_lines),
        md_type="竞技场",
        k1="挑战1", v1="竞技场挑战 1",
        k2="挑战2", v2="竞技场挑战 2",
        k3="挑战3", v3="竞技场挑战 3"
    )
    await arena_view.finish()

@arena_ranking.handle(parameterless=[Cooldown(cd_time=0)])
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

@arena_myinfo.handle(parameterless=[Cooldown(cd_time=0)])
async def arena_myinfo_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的竞技场信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await arena_myinfo.finish()
    
    user_id = user_info['user_id']
    arena_data = arena_limit.get_user_arena_info(user_id)
    challenge_cap = arena_limit.get_daily_challenge_cap(user_id)
    challenge_used = int(arena_data.get("daily_challenges_used", 0))
    challenge_remaining = max(0, challenge_cap - challenge_used)
    daily_buys = int(arena_data.get("daily_challenge_buys", 0))
    
    # 计算胜率
    total_battles = arena_data['total_wins'] + arena_data['total_losses']
    win_rate = (arena_data['total_wins'] / total_battles * 100) if total_battles > 0 else 0
    
    msg = f"""
⚔️  【竞技场信息】⚔️

道号：{user_info['user_name']}
当前积分：{arena_data['score']}
当前段位：{arena_data['rank']}
今日挑战：{challenge_remaining}/{challenge_cap}次
今日购买：{daily_buys}/{arena_limit.daily_buy_limit}次（{number_to(ARENA_CHALLENGE_BUY_COST)}灵石/次）

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

@arena_shop.handle(parameterless=[Cooldown(cd_time=0)])
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
        already_purchased = arena_limit.get_weekly_purchases(user_id, item_id)
        
        msg_list.append(
            f"编号：{item_id}\n"
            f"名称：{item_info['name']}\n"
            f"要求段位：{rank_requirement}\n" 
            f"价格：{item_data['cost']}荣誉值\n"
            f"每周限购：{item_data['weekly_limit'] - already_purchased}/{item_data['weekly_limit']}个"
        )
    
    msg_list.append(f"提示：发送 竞技场商店+页码 查看其他页（共{total_pages}页）")
    await send_msg_handler(bot, event, "竞技场商店", bot.self_id, msg_list, title=title)
    await arena_shop.finish()

@arena_buy.handle(parameterless=[Cooldown(cd_time=0)])
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

    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = (
        f"arena-purchase:{event_id}:{user_id}"
        if event_id
        else f"arena-purchase:{time.time_ns()}:{user_id}"
    )
    # 先走 operation 事务：重放必须在限购/荣誉值前置拦截之前完成。
    already_purchased = arena_limit.get_weekly_purchases(user_id, shop_id)
    max_quantity = item_data["weekly_limit"] - already_purchased
    request_quantity = quantity
    if request_quantity > max_quantity:
        request_quantity = max_quantity
    if request_quantity <= 0 and not event_id:
        msg = f"{item_info['name']}已到限购无法再购买！"
        await handle_send(bot, event, msg)
        await arena_buy.finish()
    if request_quantity <= 0:
        request_quantity = max(1, quantity)

    purchase_result = arena_purchase_service.purchase(
        operation_id,
        user_id,
        shop_id,
        item_info["name"],
        item_info["type"],
        request_quantity,
        item_data["cost"],
        item_data["weekly_limit"],
        arena_info["honor_points"],
        arena_info["weekly_purchases"],
        XiuConfig().max_goods_num,
        1,
    )
    if purchase_result.status == "duplicate":
        msg = (
            f"成功兑换{item_info['name']}×{purchase_result.quantity}，"
            f"消耗{purchase_result.cost}荣誉值！"
        )
        await handle_send(bot, event, msg)
        await arena_buy.finish()
    if purchase_result.status == "honor_insufficient":
        await handle_send(bot, event, "荣誉值状态已变化，当前荣誉值不足！")
        await arena_buy.finish()
    if purchase_result.status == "limit_reached":
        await handle_send(bot, event, f"{item_info['name']}已到限购无法再购买！")
        await arena_buy.finish()
    if purchase_result.status == "inventory_full":
        await handle_send(bot, event, f"{item_info['name']}持有数量已达上限！")
        await arena_buy.finish()
    if purchase_result.status in {"state_changed", "user_missing"}:
        await handle_send(bot, event, "竞技场兑换状态已变化，请重新兑换！")
        await arena_buy.finish()
    if not purchase_result.succeeded:
        await handle_send(bot, event, "竞技场兑换状态已变化，请重新兑换！")
        await arena_buy.finish()

    msg = (
        f"成功兑换{item_info['name']}×{purchase_result.quantity}，"
        f"消耗{purchase_result.cost}荣誉值！"
    )
    await handle_send(bot, event, msg)
    await arena_buy.finish()

@arena_honor.handle(parameterless=[Cooldown(cd_time=0)])
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

@arena_buy_challenge.handle(parameterless=[Cooldown(cd_time=0)])
async def arena_buy_challenge_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """花费灵石购买今日竞技场挑战次数"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await arena_buy_challenge.finish()

    user_id = str(user_info["user_id"])
    msg_text = args.extract_plain_text().strip()
    buy_amount = 1
    if msg_text:
        match = re.search(r"\d+", msg_text)
        if not match:
            await handle_send(bot, event, "请输入正确的购买次数，例如：竞技场购买次数 1")
            await arena_buy_challenge.finish()
        buy_amount = max(1, int(match.group()))

    arena_info = arena_limit.get_user_arena_info(user_id)
    bought = int(arena_info.get("daily_challenge_buys", 0))
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = (
        f"arena-challenge-purchase:{event_id}:{user_id}"
        if event_id
        else f"arena-challenge-purchase:{time.time_ns()}:{user_id}"
    )
    # 先走 operation：同一事件重放不能被“今日已买满”前置拦截。
    result = arena_challenge_purchase_service.purchase(
        operation_id,
        user_id,
        buy_amount,
        ARENA_CHALLENGE_BUY_COST,
        arena_limit.daily_buy_limit,
        int(user_info.get("stone", 0)),
        bought,
        int(arena_info.get("daily_extra_challenges", 0)),
        arena_info["last_buy_date"],
    )
    if result.status == "duplicate" or result.succeeded:
        real_amount, new_bought, total_cap = (
            result.amount,
            result.bought,
            arena_limit.daily_challenges + result.extra,
        )
        arena_info = arena_limit.get_user_arena_info(user_id)
        remaining = max(0, int(total_cap) - int(arena_info.get("daily_challenges_used", 0)))
        await handle_send(
            bot,
            event,
            f"竞技场挑战次数购买成功！\n"
            f"本次购买：{real_amount}次\n"
            f"消耗灵石：{number_to(result.cost)}\n"
            f"今日购买：{new_bought}/{arena_limit.daily_buy_limit}\n"
            f"今日剩余挑战：{remaining}/{total_cap}",
            md_type="竞技场",
            k1="挑战", v1="竞技场挑战",
            k2="查看", v2="竞技场查看",
            k3="我的", v3="我的竞技场",
        )
        await arena_buy_challenge.finish()
    if result.status == "limit_reached":
        await handle_send(
            bot,
            event,
            f"今日竞技场购买次数已用完（{arena_limit.daily_buy_limit}/{arena_limit.daily_buy_limit}）。",
        )
        await arena_buy_challenge.finish()
    if result.status == "stone_insufficient":
        total_cost = ARENA_CHALLENGE_BUY_COST * buy_amount
        await handle_send(
            bot,
            event,
            f"灵石不足，购买{buy_amount}次需要{number_to(total_cost)}灵石。\n"
            f"当前灵石：{number_to(int(user_info.get('stone', 0)))}",
        )
        await arena_buy_challenge.finish()
    await handle_send(bot, event, "竞技场购买状态已变化，请重新操作。")
    await arena_buy_challenge.finish()

def check_rank_requirement(current_rank, required_rank):
    """检查段位要求"""
    rank_order = ["青铜", "白银", "黄金", "铂金", "钻石", "王者"]
    current_index = rank_order.index(current_rank)
    required_index = rank_order.index(required_rank)
    return current_index >= required_index

async def find_arena_opponent(user_id, operation_id=None):
    """为玩家寻找合适的竞技场对手，优先积分相近，否则返回最近的一个"""
    user_id = str(user_id)
    user_arena_data = arena_limit.get_user_arena_info(user_id)
    user_score = int(user_arena_data['score'])
    
    # 获取所有玩家数据
    all_players = player_data_manager.get_all_field_data("arena", "score")
    if not all_players:
        return None

    candidates = []
    close_candidates = []

    for opponent_id, opponent_score in all_players:
        opponent_id = str(opponent_id)
        if opponent_id == user_id:
            continue

        try:
            opponent_score = int(opponent_score)
        except (TypeError, ValueError):
            continue

        opponent_user_info = sql_message.get_user_info_with_id(opponent_id)
        if not opponent_user_info:
            continue

        diff = abs(opponent_score - user_score)
        candidates.append((opponent_id, opponent_score, diff))

        if diff <= 200:
            close_candidates.append(opponent_id)

    # 优先随机选取积分接近的
    if close_candidates:
        return random.Random(operation_id).choice(close_candidates)

    # 没有接近的，就选最近的一个
    if candidates:
        candidates.sort(key=lambda x: x[2])
        return candidates[0][0]

    return None

def set_arena_opponent_cache(user_id: str, targets: list):
    """设置竞技场对手缓存"""
    arena_opponent_cache[str(user_id)] = {
        "targets": targets,
        "expire_time": datetime.now().timestamp() + ARENA_CACHE_EXPIRE_SECONDS
    }


def get_arena_opponent_cache(user_id: str):
    """获取竞技场对手缓存，若过期则自动清除"""
    user_id = str(user_id)
    cache = arena_opponent_cache.get(user_id)
    if not cache:
        return None

    expire_time = cache.get("expire_time", 0)
    if datetime.now().timestamp() > expire_time:
        arena_opponent_cache.pop(user_id, None)
        return None

    return cache.get("targets", [])


def clear_arena_opponent_cache(user_id: str):
    """清除竞技场对手缓存"""
    arena_opponent_cache.pop(str(user_id), None)

async def reset_arena_daily_challenges():
    """每日重置竞技场挑战次数并发放荣誉值奖励"""
    all_users = player_data_manager.get_all_field_data("arena", "score")
    honor_distribution = {}
    season_key = datetime.now().strftime("%Y-%m-%d")

    for user_id, _ in all_users:
        user_id = str(user_id)
        arena_info = arena_limit.get_user_arena_info(user_id)
        total_honor, base_honor, ranking_bonus = arena_limit.calculate_daily_honor(user_id)
        claimed = arena_season_reward_service.claim(
            f"arena-season-reward:{season_key}:{user_id}", user_id, season_key,
            int(arena_info["score"]), arena_info["rank"], arena_limit.get_user_ranking(user_id),
            int(arena_info["honor_points"]), int(arena_info["total_honor_earned"]),
            base_honor, ranking_bonus, expected_reset=arena_info,
        )
        if claimed.succeeded and total_honor > 0:
            user_info = sql_message.get_user_info_with_id(user_id)
            honor_distribution[user_info['user_name'] if user_info else user_id] = {
                'total': total_honor,
                'base': base_honor,
                'bonus': ranking_bonus
            }
    
    logger.opt(colors=True).info(f"<green>竞技场每日挑战次数已重置！荣誉值发放完成，共发放{len(honor_distribution)}名玩家</green>")

async def reduce_arena_rank(reduce_steps=2, business_week=None, *, chunk_size=500):
    """每周竞技场统一降段"""
    while True:
        result = arena_weekly_rank_reduction_service.reduce(
            business_week,
            reduce_steps,
            chunk_size=chunk_size,
        )
        if result.status != "applied" or result.task_status == "completed":
            break
        await asyncio.sleep(0)
    logger.opt(colors=True).info(
        f"<green>竞技场降段任务{result.task_status}！"
        f"已处理{result.completed}/{result.total}名玩家，"
        f"实际降段{result.changed}名，跳过{result.skipped}名，"
        f"降段数：{result.reduce_steps}</green>"
    )
    return result

async def use_arena_challenge_ticket(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用竞技场挑战券增加今日竞技场挑战次数"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info["user_id"]
    arena_info = arena_limit.get_user_arena_info(user_id)
    used_count = int(arena_info.get("daily_challenges_used", 0))
    item_count = int(sql_message.goods_num(user_id, item_id))
    extra_challenges = int(arena_info.get("daily_extra_challenges", 0))
    challenge_cap = arena_limit.daily_challenges + extra_challenges
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    # 先走 operation：成功后 used_count 可能已归零，重放不能被前置拦截。
    result = arena_challenge_ticket_service.use(
        f"arena-challenge-ticket:{event_id}:{user_id}" if event_id
        else f"arena-challenge-ticket:{time.time_ns()}:{user_id}",
        user_id,
        item_id,
        int(quantity),
        item_count,
        used_count,
        extra_challenges,
        challenge_cap,
    )
    if result.status == "duplicate" or result.succeeded:
        await handle_send(
            bot, event,
            f"使用竞技场挑战券 {result.used_tickets} 张，今日剩余竞技场挑战次数："
            f"{result.challenges_remaining}/{result.challenge_cap}",
            md_type="竞技场",
            k1="挑战", v1="竞技场挑战",
            k2="查看", v2="竞技场查看",
            k3="我的", v3="我的竞技场"
        )
        return
    if result.status == "no_challenges_used":
        message = "今日竞技场挑战次数未消耗，无需使用挑战券。"
    elif result.status == "item_missing":
        message = "背包中没有可用的竞技场挑战券。"
    else:
        message = "竞技场挑战券使用状态已变化，请重新操作。"
    await handle_send(bot, event, message, md_type="竞技场")
