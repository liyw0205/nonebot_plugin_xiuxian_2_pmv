import random
import asyncio
from datetime import datetime

from nonebot import on_command
from nonebot.params import CommandArg

from ..command import *


# =========================
# 配置
# =========================
GUESS_MIN = 1
GUESS_MAX = 100
GUESS_TIMEOUT = 300  # 秒，无操作自动结束（5分钟）


# =========================
# 状态存储（内存）
# 每个用户只能有一局：user_id -> game_data
# =========================
guess_number_sessions: dict[str, dict] = {}
guess_number_timeout_tasks: dict[str, asyncio.Task] = {}


def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _name_from_event(event):
    sender = getattr(event, "sender", None)
    if sender:
        return sender.card or sender.nickname or str(event.get_user_id())
    return str(event.get_user_id())


def _build_range_text(low: int, high: int) -> str:
    return f"{low} ~ {high}"


def _clear_session(user_id: str):
    guess_number_sessions.pop(user_id, None)
    t = guess_number_timeout_tasks.pop(user_id, None)
    if t:
        t.cancel()


async def _start_guess_timeout(bot: Bot, event, user_id: str):
    # 先取消旧任务
    if user_id in guess_number_timeout_tasks:
        guess_number_timeout_tasks[user_id].cancel()

    async def _task():
        await asyncio.sleep(GUESS_TIMEOUT)
        game = guess_number_sessions.get(user_id)
        if not game:
            return
        if game.get("status") != "playing":
            return

        answer = game["answer"]
        tries = game["tries"]
        _clear_session(user_id)

        await handle_send(
            bot, event,
            f"⏰ 猜数字超时（{GUESS_TIMEOUT}秒无操作），本局已结束。\n"
            f"答案是：{answer}\n"
            f"你共猜了：{tries} 次",
            md_type="娱乐",
            k1="再来一局", v1="开始猜数字",
            k2="小游戏帮助", v2="小游戏帮助",
            k3="娱乐帮助", v3="娱乐帮助"
        )

    guess_number_timeout_tasks[user_id] = asyncio.create_task(_task())


# =========================
# 命令
# =========================
guess_number_start_cmd = on_command("开始猜数字", priority=5, block=True)
guess_number_guess_cmd = on_command("猜", aliases={"猜数字"}, priority=5, block=True)
guess_number_info_cmd = on_command("猜数字信息", priority=5, block=True)
guess_number_end_cmd = on_command("结束猜数字", priority=5, block=True)
guess_number_help_cmd = on_command("猜数字帮助", priority=5, block=True)


@guess_number_start_cmd.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    user_id = str(event.get_user_id())
    user_name = _name_from_event(event)

    # 若已有进行中，直接提示
    old = guess_number_sessions.get(user_id)
    if old and old.get("status") == "playing":
        await handle_send(
            bot, event,
            f"你已经有一局猜数字在进行中啦！\n"
            f"当前范围：{_build_range_text(old['low'], old['high'])}\n"
            f"已猜次数：{old['tries']}\n"
            f"请继续发送：猜 数字（如：猜 50）",
            md_type="娱乐",
            k1="猜数字信息", v1="猜数字信息",
            k2="结束本局", v2="结束猜数字",
            k3="猜数字帮助", v3="猜数字帮助"
        )
        return

    answer = random.randint(GUESS_MIN, GUESS_MAX)
    guess_number_sessions[user_id] = {
        "user_id": user_id,
        "user_name": user_name,
        "answer": answer,
        "low": GUESS_MIN,
        "high": GUESS_MAX,
        "tries": 0,
        "status": "playing",
        "create_time": _now_str(),
        "last_action_time": _now_str(),
    }

    await _start_guess_timeout(bot, event, user_id)

    await handle_send(
        bot, event,
        f"🎯 猜数字开始！\n"
        f"我已经想好了一个 {GUESS_MIN}~{GUESS_MAX} 的整数。\n"
        f"请发送：猜 数字（例如：猜 50）",
        md_type="娱乐",
        k1="猜 50", v1="猜 50",
        k2="猜数字信息", v2="猜数字信息",
        k3="结束猜数字", v3="结束猜数字"
    )


@guess_number_guess_cmd.handle(parameterless=[Cooldown(cd_time=0.6)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    user_id = str(event.get_user_id())
    game = guess_number_sessions.get(user_id)

    if not game or game.get("status") != "playing":
        await handle_send(
            bot, event,
            "你当前没有进行中的猜数字游戏。\n发送【开始猜数字】即可开始。",
            md_type="娱乐",
            k1="开始猜数字", v1="开始猜数字",
            k2="猜数字帮助", v2="猜数字帮助",
            k3="小游戏帮助", v3="小游戏帮助"
        )
        return

    raw = args.extract_plain_text().strip()
    if not raw:
        await handle_send(
            bot, event,
            "请输入要猜的数字，例如：猜 66",
            md_type="娱乐",
            k1="示例", v1="猜 66",
            k2="猜数字信息", v2="猜数字信息",
            k3="结束猜数字", v3="结束猜数字"
        )
        return

    # 兼容“猜数字 50”这种（alias 命中后 args 可能是“50”）
    # 这里只取第一个可解析整数
    token = raw.split()[0]
    if not token.lstrip("-").isdigit():
        await handle_send(
            bot, event,
            "格式错误，请发送：猜 数字（例如：猜 66）",
            md_type="娱乐",
            k1="示例", v1="猜 66",
            k2="猜数字帮助", v2="猜数字帮助",
            k3="结束猜数字", v3="结束猜数字"
        )
        return

    num = int(token)
    if num < GUESS_MIN or num > GUESS_MAX:
        await handle_send(
            bot, event,
            f"你输入的数字超出范围，请输入 {GUESS_MIN}~{GUESS_MAX} 之间的整数。",
            md_type="娱乐",
            k1="猜 50", v1="猜 50",
            k2="猜数字信息", v2="猜数字信息",
            k3="结束猜数字", v3="结束猜数字"
        )
        return

    # 每次猜测刷新超时
    await _start_guess_timeout(bot, event, user_id)

    game["tries"] += 1
    game["last_action_time"] = _now_str()

    ans = game["answer"]
    if num == ans:
        tries = game["tries"]
        _clear_session(user_id)

        await handle_send(
            bot, event,
            f"🎉 恭喜你猜对了！答案就是 {ans}\n"
            f"总猜测次数：{tries} 次",
            md_type="娱乐",
            k1="再来一局", v1="开始猜数字",
            k2="小游戏帮助", v2="小游戏帮助",
            k3="娱乐帮助", v3="娱乐帮助"
        )
        return

    if num < ans:
        # 收紧下界
        if num >= game["low"]:
            game["low"] = max(game["low"], num + 1)

        await handle_send(
            bot, event,
            f"📉 猜小了！\n"
            f"当前有效范围：{_build_range_text(game['low'], game['high'])}\n"
            f"已猜次数：{game['tries']}",
            md_type="娱乐",
            k1="猜数字信息", v1="猜数字信息",
            k2="继续猜", v2=f"猜 {(game['low'] + game['high']) // 2}",
            k3="结束猜数字", v3="结束猜数字"
        )
        return

    # num > ans
    if num <= game["high"]:
        game["high"] = min(game["high"], num - 1)

    await handle_send(
        bot, event,
        f"📈 猜大了！\n"
        f"当前有效范围：{_build_range_text(game['low'], game['high'])}\n"
        f"已猜次数：{game['tries']}",
        md_type="娱乐",
        k1="猜数字信息", v1="猜数字信息",
        k2="继续猜", v2=f"猜 {(game['low'] + game['high']) // 2}",
        k3="结束猜数字", v3="结束猜数字"
    )


@guess_number_info_cmd.handle(parameterless=[Cooldown(cd_time=0.8)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    user_id = str(event.get_user_id())
    game = guess_number_sessions.get(user_id)

    if not game or game.get("status") != "playing":
        await handle_send(
            bot, event,
            "你当前没有进行中的猜数字游戏。",
            md_type="娱乐",
            k1="开始猜数字", v1="开始猜数字",
            k2="猜数字帮助", v2="猜数字帮助",
            k3="小游戏帮助", v3="小游戏帮助"
        )
        return

    await handle_send(
        bot, event,
        f"🎯 猜数字信息\n"
        f"玩家：{game['user_name']}\n"
        f"范围：{_build_range_text(game['low'], game['high'])}\n"
        f"已猜次数：{game['tries']}\n"
        f"创建时间：{game['create_time']}\n"
        f"最后操作：{game['last_action_time']}",
        md_type="娱乐",
        k1="继续猜", v1=f"猜 {(game['low'] + game['high']) // 2}",
        k2="结束猜数字", v2="结束猜数字",
        k3="猜数字帮助", v3="猜数字帮助"
    )


@guess_number_end_cmd.handle(parameterless=[Cooldown(cd_time=0.8)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    user_id = str(event.get_user_id())
    game = guess_number_sessions.get(user_id)

    if not game or game.get("status") != "playing":
        await handle_send(
            bot, event,
            "你当前没有进行中的猜数字游戏。",
            md_type="娱乐",
            k1="开始猜数字", v1="开始猜数字",
            k2="猜数字帮助", v2="猜数字帮助",
            k3="小游戏帮助", v3="小游戏帮助"
        )
        return

    answer = game["answer"]
    tries = game["tries"]
    _clear_session(user_id)

    await handle_send(
        bot, event,
        f"已结束当前猜数字。\n"
        f"答案是：{answer}\n"
        f"你共猜了：{tries} 次",
        md_type="娱乐",
        k1="再来一局", v1="开始猜数字",
        k2="小游戏帮助", v2="小游戏帮助",
        k3="娱乐帮助", v3="娱乐帮助"
    )


@guess_number_help_cmd.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await handle_send(
        bot, event,
        "猜数字帮助：\n"
        "1）开始猜数字\n"
        "2）猜 50（在 1~100 中猜）\n"
        "3）猜数字信息（查看当前范围与次数）\n"
        "4）结束猜数字\n\n"
        f"规则：系统随机一个 {GUESS_MIN}~{GUESS_MAX} 的整数，"
        "你根据“猜大了/猜小了”提示逐步逼近答案。",
        md_type="娱乐",
        k1="开始猜数字", v1="开始猜数字",
        k2="示例猜测", v2="猜 50",
        k3="小游戏帮助", v3="小游戏帮助"
    )