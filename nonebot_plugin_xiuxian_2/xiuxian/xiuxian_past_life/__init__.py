"""
前尘往事 - 命令入口
修仙版人生重开 · 剧本杀
"""
import random
from ..on_compat import on_command
from nonebot.permission import SUPERUSER
from ..adapter_compat import Bot, Message, GroupMessageEvent, PrivateMessageEvent, get_at_user_id
from nonebot.params import CommandArg
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import (
    check_user, handle_send, send_msg_handler, log_message, update_statistics_value, number_to, send_help_message
)
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager
from .past_life_limit import past_life_limit
from .past_life_events import past_life_engine, ATTR_NAMES

player_data_manager = PlayerDataManager()
sql_message = XiuxianDateManage()

INITIAL_APTITUDE_MIN = 3
INITIAL_APTITUDE_MAX = 15
INITIAL_APTITUDE_TOTAL_MIN = INITIAL_APTITUDE_MIN * len(ATTR_NAMES)
INITIAL_APTITUDE_TOTAL_MAX = 20
PAST_LIFE_RESET_ALL_TOKENS = {"all", "全部", "全体", "所有"}
PAST_LIFE_RESET_CLEAR_TOKENS = {"全清", "清空", "清空历史"}
PAST_LIFE_RESET_HELP_TOKENS = {"help", "帮助", "用法", "?"}

# ═══ 命令定义 ═══
past_life_cmd = on_command("前尘往事", aliases={"前世今生"}, priority=5, block=True)
reincarnate_cmd = on_command("投胎", priority=5, block=True)
past_choice_cmd = on_command("前尘选择", aliases={"前世选择"}, priority=5, block=True)
past_memory_cmd = on_command("前尘回忆", aliases={"前世回忆"}, priority=5, block=True)
past_rank_cmd = on_command("前尘排行", aliases={"前世排行"}, priority=5, block=True)
past_help_cmd = on_command("前尘帮助", priority=5, block=True)
reset_past_life_cmd = on_command("重置前尘", permission=SUPERUSER, priority=5, block=True)


# ═══ 前尘帮助 ═══
@past_help_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = (
        "\n═══  前尘往事  ═════\n"
        "【前尘往事】- 查看/开启前世回忆\n"
        "【投胎】- 定下并锁定本轮先天资质\n"
        "【前尘选择 1/2/3】- 在剧情中做出选择\n"
        "【前尘回忆】- 查看过往前世记录\n"
        "【前尘排行】- 查看前世评分排行\n"
        "═════════════\n"
        "规则说明：\n"
        "1. 投胎后五项先天资质即刻定下，本轮不可重抽\n"
        "   初始资质总和15~20随机，单项不低于3，也可能偏科极高\n"
        "2. 经历十幕人生，每幕做出抉择\n"
        "3. 选择会根据当前资质产生更佳、受挫或平稳结果\n"
        "4. 前世评分为百分制：抉择50、最终资质30、完成幕数20\n"
        "5. 不同结局获得不同奖励\n"
        "6. 每日00:00与12:00刷新，每个刷新段可完成一次\n"
        "═════════════\n"
        "十九种结局等你解锁！"
    )
    await send_help_message(bot, event, msg, k1="开始", v1="前尘往事", k2="回忆", v2="前尘回忆", k3="排行", v3="前尘排行")
    await past_help_cmd.finish()


# ═══ 前尘往事（主入口） ═══
@past_life_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await past_life_cmd.finish()

    user_id = user_info["user_id"]
    result = past_life_engine.get_current_display(user_id)

    if result["state"] == 0:
        await handle_send(bot, event, result["message"], md_type="前尘", k1="投胎", v1="投胎", k2="回忆", v2="前尘回忆", k3="排行", v3="前尘排行")
    elif result["state"] == 1:
        await handle_send(bot, event, result["message"], md_type="前尘", k1="投胎", v1="投胎", k2="回忆", v2="前尘回忆", k3="排行", v3="前尘排行")
    elif result["state"] == 2:
        await handle_send(bot, event, result["message"], md_type="前尘", k1="选择1", v1="前尘选择 1", k2="选择2", v2="前尘选择 2", k3="选择3", v3="前尘选择 3")
    else:
        await handle_send(bot, event, result["message"])

    await past_life_cmd.finish()


# ═══ 投胎（定下资质） ═══
@reincarnate_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await reincarnate_cmd.finish()

    user_id = user_info["user_id"]

    state = past_life_limit.get_user_state(user_id)
    if state.get("state") == 2:
        result = past_life_engine.get_current_display(user_id)
        result["message"] = "本轮前尘已开始，资质与事件已锁定。\n" + result["message"]
        if result["state"] == 2:
            await handle_send(bot, event, result["message"], md_type="前尘",
                              k1="选择1", v1="前尘选择 1",
                              k2="选择2", v2="前尘选择 2",
                              k3="选择3", v3="前尘选择 3")
        else:
            await handle_send(bot, event, result["message"], md_type="前尘",
                              k1="往事", v1="前尘往事",
                              k2="回忆", v2="前尘回忆",
                              k3="帮助", v3="前尘帮助")
        await reincarnate_cmd.finish()

    # 检查冷却
    if not past_life_limit.check_cooldown(user_id):
        msg = f"前尘往事尚未刷新，{past_life_limit.get_cooldown_text(user_id)}"
        await handle_send(bot, event, msg, md_type="前尘", k1="回忆", v1="前尘回忆", k2="帮助", v2="前尘帮助", k3="排行", v3="前尘排行")
        await reincarnate_cmd.finish()

    legacy_text = args.extract_plain_text().strip()
    alloc = _generate_initial_aptitude()
    result = past_life_engine.start_new_life(user_id, alloc)
    if legacy_text:
        result["message"] = "投胎时资质已由命数定下，输入的分配不会生效。\n" + result["message"]
    log_message(user_id, f"[前尘往事] 开始新人生 - {alloc}")
    update_statistics_value(user_id, "前尘往事次数")

    await handle_send(bot, event, result["message"], md_type="前尘",
                      k1="选择1", v1="前尘选择 1",
                      k2="选择2", v2="前尘选择 2",
                      k3="选择3", v3="前尘选择 3")
    await reincarnate_cmd.finish()


# ═══ 前尘选择 ═══
@past_choice_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await past_choice_cmd.finish()

    user_id = user_info["user_id"]
    text = args.extract_plain_text().strip()

    try:
        choice_idx = int(text)
    except ValueError:
        msg = "请输入数字选项！例如：前尘选择 1"
        await handle_send(bot, event, msg, md_type="前尘", k1="往事", v1="前尘往事", k2="回忆", v2="前尘回忆", k3="帮助", v3="前尘帮助")
        await past_choice_cmd.finish()

    # 处理选择
    result = past_life_engine.process_choice(user_id, choice_idx)

    if result["is_end"]:
        log_message(user_id, f"[前尘往事] 结局：{result['ending']['name']}")
        await handle_send(bot, event, result["message"], md_type="前尘",
                          k1="回忆", v1="前尘回忆",
                          k2="排行", v2="前尘排行",
                          k3="再来", v3="前尘往事")
    else:
        await handle_send(bot, event, result["message"], md_type="前尘",
                          k1="选择1", v1="前尘选择 1",
                          k2="选择2", v2="前尘选择 2",
                          k3="选择3", v3="前尘选择 3")

    await past_choice_cmd.finish()


# ═══ 前尘回忆 ═══
@past_memory_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await past_memory_cmd.finish()

    user_id = user_info["user_id"]
    state = past_life_limit.get_user_state(user_id)

    endings_log = state.get("endings_log", [])
    if not isinstance(endings_log, list):
        endings_log = []

    if not endings_log:
        msg = "道友尚未经历任何前世，发送【投胎】开始你的第一段前尘往事吧！"
        await handle_send(bot, event, msg, md_type="前尘", k1="投胎", v1="投胎", k2="往事", v2="前尘往事", k3="帮助", v3="前尘帮助")
        await past_memory_cmd.finish()

    msg = "═══  前尘回忆录  ═════\n"
    for i, log in enumerate(endings_log[:10], 1):
        msg += f"\n第{i}世 | {log.get('time', '未知')}\n"
        msg += f"结局：【{log.get('name', '未知')}】 评分：{log.get('score', 0)}分\n"
        msg += f"─────────────\n"

    msg += (
        f"\n累计前世：{state.get('total_runs', 0)}次\n"
        f"最佳结局：{state.get('best_ending', '无')}（{state.get('best_score', 0)}分）\n"
        f"═════════════"
    )

    await handle_send(bot, event, msg, md_type="前尘", k1="再来", v1="前尘往事", k2="排行", v2="前尘排行", k3="帮助", v3="前尘帮助")
    await past_memory_cmd.finish()


# ═══ 前尘排行 ═══
@past_rank_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await past_rank_cmd.finish()

    all_scores = player_data_manager.get_all_field_data("past_life", "best_score")

    sorted_scores = sorted(
        [(uid, score) for uid, score in all_scores if isinstance(score, (int, float)) and score > 0],
        key=lambda x: x[1],
        reverse=True
    )

    if not sorted_scores:
        msg = "暂无前尘排行数据！快去开启你的前世回忆吧！"
        await handle_send(bot, event, msg)
        await past_rank_cmd.finish()

    rank_msg = "═══  前尘排行榜  ═════\n"
    rank_msg += "排名 | 道号 | 前世评分 | 最佳结局\n"
    rank_msg += "─────────────\n"

    for i, (uid, score) in enumerate(sorted_scores[:30], 1):
        u_info = sql_message.get_user_info_with_id(uid)
        if not u_info:
            continue
        u_state = past_life_limit.get_user_state(uid)
        best_ending = u_state.get("best_ending", "未知")
        rank_msg += f"第{i}位 | {u_info['user_name']} | {score}分 | {best_ending}\n"

    rank_msg += "═════════════"

    await handle_send(bot, event, rank_msg)
    await past_rank_cmd.finish()

@reset_past_life_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    text = args.extract_plain_text().strip()

    if _is_reset_help(text):
        await handle_send(
            bot, event,
            "用法：\n"
            "重置前尘（重置所有用户，保留历史）\n"
            "重置前尘 all\n"
            "重置前尘 all 全清（清空所有历史）\n"
            "重置前尘 道号\n"
            "重置前尘 @某人\n"
            "重置前尘 道号 全清（清空历史）"
        )
        return

    # 是否全清
    clear_history = _has_clear_history_token(text)

    if _is_reset_all(text):
        count = past_life_limit.reset_all_user_state(clear_history=clear_history)
        mode = "（已清空历史）" if clear_history else "（保留历史）"
        await handle_send(bot, event, f"已重置所有用户的前尘状态，共{count}条记录 {mode}")
        return

    # 先尝试@目标
    target_user = None
    qq = get_at_user_id(args)
    if qq:
        target_user = sql_message.get_user_info_with_id(qq)

    # 没@就按道号
    if not target_user:
        target_name = _get_reset_target_name(text)
        if target_name:
            target_user = sql_message.get_user_info_with_name(target_name)

    if not target_user:
        await handle_send(bot, event, "未找到目标用户（请@或输入正确道号）")
        return

    past_life_limit.reset_user_state(target_user["user_id"], clear_history=clear_history)
    mode = "（已清空历史）" if clear_history else "（保留历史）"
    await handle_send(bot, event, f"已重置 {target_user['user_name']} 的前尘状态 {mode}")


# ═══ 工具函数 ═══
def _generate_initial_aptitude():
    """定下本轮先天资质：总和15~20随机，单项不低于3。"""
    shuffled_attrs = random.sample(ATTR_NAMES, len(ATTR_NAMES))
    remaining = random.randint(INITIAL_APTITUDE_TOTAL_MIN, INITIAL_APTITUDE_TOTAL_MAX)
    values = {}

    for idx, attr in enumerate(shuffled_attrs):
        slots_left = len(shuffled_attrs) - idx - 1
        low = max(INITIAL_APTITUDE_MIN, remaining - INITIAL_APTITUDE_MAX * slots_left)
        high = min(INITIAL_APTITUDE_MAX, remaining - INITIAL_APTITUDE_MIN * slots_left)
        value = random.randint(low, high)
        values[attr] = value
        remaining -= value

    return {attr: values[attr] for attr in ATTR_NAMES}


def _split_reset_tokens(text: str):
    return [part.strip() for part in text.split() if part.strip()]


def _is_reset_help(text: str):
    parts = _split_reset_tokens(text)
    return bool(parts) and all(part.lower() in PAST_LIFE_RESET_HELP_TOKENS for part in parts)


def _has_clear_history_token(text: str):
    parts = _split_reset_tokens(text)
    return any(part in PAST_LIFE_RESET_CLEAR_TOKENS for part in parts) or "全清" in text


def _get_reset_target_name(text: str):
    for part in _split_reset_tokens(text):
        if part in PAST_LIFE_RESET_CLEAR_TOKENS:
            continue
        return part.lstrip("@＠")
    return ""


def _is_reset_all(text: str):
    parts = _split_reset_tokens(text)
    if not parts:
        return True

    meaningful_parts = [
        part for part in parts
        if part not in PAST_LIFE_RESET_CLEAR_TOKENS
    ]
    if not meaningful_parts:
        return True

    return any(part.lower() in PAST_LIFE_RESET_ALL_TOKENS for part in meaningful_parts)
