import asyncio

from ....features.entertainment.guess_application import NUMBER_SESSION_TIMEOUT
from ....infrastructure.clock import SystemClock
from ....infrastructure.random_source import SystemRandom

from ...on_compat import on_command
from nonebot.params import CommandArg

from ..command import *
from ..io_runtime import run_blocking_io
from ..room_store import entertainment_application
from .game_utils import event_display_name


# =========================
# 配置
# =========================
GUESS_MIN = 1
GUESS_MAX = 100
GUESS_TIMEOUT = NUMBER_SESSION_TIMEOUT


# =========================
# Timeout tasks are process-local; durable game state belongs to the feature repository.
guess_number_timeout_tasks: dict[str, asyncio.Task] = {}
guess_number_timeout_tokens: dict[str, str] = {}
runtime_clock = SystemClock()
runtime_random = SystemRandom()


def _build_range_text(low: int, high: int) -> str:
    return f"{low} ~ {high}"


def _clear_timeout(user_id: str):
    t = guess_number_timeout_tasks.pop(user_id, None)
    guess_number_timeout_tokens.pop(user_id, None)
    if t and t is not asyncio.current_task():
        t.cancel()


def _clock_snapshot() -> tuple[float, str]:
    now = runtime_clock.now()
    return now.timestamp(), now.strftime("%Y-%m-%d %H:%M:%S")


async def _send_timeout(bot: Bot, event, game: dict):
    await handle_send(
        bot, event,
        f"【猜数字超时】\n"
        f"{GUESS_TIMEOUT} 秒无操作，本局已结束。\n"
        f"答案：{game['answer']}\n"
        f"猜测次数：{game['tries']} 次",
        md_type="娱乐",
        k1="再来一局", v1="开始猜数字",
        k2="小游戏帮助", v2="小游戏帮助",
        k3="娱乐帮助", v3="娱乐帮助"
    )


async def _start_guess_timeout(bot: Bot, event, user_id: str, session: dict):
    token = str(session["session_token"])
    old_task = guess_number_timeout_tasks.get(user_id)
    if old_task and not old_task.done() and guess_number_timeout_tokens.get(user_id) == token:
        return
    _clear_timeout(user_id)

    async def _task():
        delay = max(float(session["expires_at"]) - runtime_clock.now().timestamp(), 0.0)
        await asyncio.sleep(delay)
        expired = await run_blocking_io(
            entertainment_application.guess_sessions.expire,
            "number",
            user_id,
            token,
            now_epoch=runtime_clock.now().timestamp(),
            timeout=5,
        )
        if expired["status"] != "expired":
            return
        _clear_timeout(user_id)
        await _send_timeout(bot, event, expired["session"])

    task = asyncio.create_task(_task())
    guess_number_timeout_tasks[user_id] = task
    guess_number_timeout_tokens[user_id] = token


# =========================
# 命令
# =========================
guess_number_start_cmd = on_command("开始猜数字", priority=5, block=True)
guess_number_guess_cmd = on_command("猜", aliases={"猜数字"}, priority=5, block=True)
guess_number_info_cmd = on_command("猜数字信息", priority=5, block=True)
guess_number_end_cmd = on_command("结束猜数字", priority=5, block=True)
guess_number_help_cmd = on_command("猜数字帮助", priority=5, block=True)


async def _read_session(user_id: str):
    now_epoch, _ = _clock_snapshot()
    return await run_blocking_io(
        entertainment_application.guess_sessions.read,
        "number",
        user_id,
        now_epoch=now_epoch,
        timeout=5,
    )


async def _send_in_progress(bot: Bot, event, game: dict):
    await handle_send(
        bot, event,
        f"【猜数字进行中】\n"
        f"当前范围：{_build_range_text(game['low'], game['high'])}\n"
        f"已猜次数：{game['tries']}\n"
        f"继续发送：猜 数字（如：猜 50）",
        md_type="娱乐",
        k1="猜数字信息", v1="猜数字信息",
        k2="结束本局", v2="结束猜数字",
        k3="猜数字帮助", v3="猜数字帮助"
    )


@guess_number_start_cmd.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    user_id = str(event.get_user_id())
    user_name = event_display_name(event)

    old = await _read_session(user_id)
    if old["status"] == "active":
        await _start_guess_timeout(bot, event, user_id, old)
        await _send_in_progress(bot, event, old["session"])
        return
    if old["status"] == "expired":
        _clear_timeout(user_id)
        await _send_timeout(bot, event, old["session"])

    answer = runtime_random.randint(GUESS_MIN, GUESS_MAX)
    now_epoch, now_display = _clock_snapshot()
    started = await run_blocking_io(
        entertainment_application.guess_sessions.start_number,
        user_id=user_id,
        user_name=user_name,
        answer=answer,
        create_time=now_display,
        last_action_time=now_display,
        now_epoch=now_epoch,
        timeout=5,
    )
    if started["status"] == "existing":
        await _start_guess_timeout(bot, event, user_id, started)
        await _send_in_progress(bot, event, started["session"])
        return
    await _start_guess_timeout(bot, event, user_id, started)

    await handle_send(
        bot, event,
        f"【猜数字开始】\n"
        f"范围：{GUESS_MIN}~{GUESS_MAX} 的整数\n"
        f"操作：猜 数字（例如：猜 50）",
        md_type="娱乐",
        k1="猜 50", v1="猜 50",
        k2="猜数字信息", v2="猜数字信息",
        k3="结束猜数字", v3="结束猜数字"
    )


@guess_number_guess_cmd.handle(parameterless=[Cooldown(cd_time=0.6)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    user_id = str(event.get_user_id())
    current = await _read_session(user_id)

    if current["status"] == "expired":
        _clear_timeout(user_id)
        await _send_timeout(bot, event, current["session"])
        return
    if current["status"] != "active":
        await handle_send(
            bot, event,
            "你当前没有进行中的猜数字游戏。\n发送：开始猜数字",
            md_type="娱乐",
            k1="开始猜数字", v1="开始猜数字",
            k2="猜数字帮助", v2="猜数字帮助",
            k3="小游戏帮助", v3="小游戏帮助"
        )
        return
    game = current["session"]
    await _start_guess_timeout(bot, event, user_id, current)

    raw = args.extract_plain_text().strip()
    if not raw:
        await handle_send(
            bot, event,
            "请输入要猜的数字。\n示例：猜 66",
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
            "格式错误。\n请发送：猜 数字（例如：猜 66）",
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
            f"你输入的数字超出范围。\n范围：{GUESS_MIN}~{GUESS_MAX}",
            md_type="娱乐",
            k1="猜 50", v1="猜 50",
            k2="猜数字信息", v2="猜数字信息",
            k3="结束猜数字", v3="结束猜数字"
        )
        return

    now_epoch, now_display = _clock_snapshot()
    result = await run_blocking_io(
        entertainment_application.guess_sessions.guess_number,
        user_id,
        num,
        last_action_time=now_display,
        now_epoch=now_epoch,
        timeout=5,
    )
    if result["status"] == "expired":
        _clear_timeout(user_id)
        await _send_timeout(bot, event, result["session"])
        return
    if result["status"] == "missing":
        _clear_timeout(user_id)
        await handle_send(
            bot, event,
            "你当前没有进行中的猜数字游戏。\n发送：开始猜数字",
            md_type="娱乐",
            k1="开始猜数字", v1="开始猜数字",
            k2="猜数字帮助", v2="猜数字帮助",
            k3="小游戏帮助", v3="小游戏帮助"
        )
        return
    game = result["session"]
    if result["status"] == "finished":
        _clear_timeout(user_id)

        await handle_send(
            bot, event,
            f"【猜数字结束】\n"
            f"结果：猜对了\n"
            f"答案：{game['answer']}\n"
            f"总猜测次数：{game['tries']} 次",
            md_type="娱乐",
            k1="再来一局", v1="开始猜数字",
            k2="小游戏帮助", v2="小游戏帮助",
            k3="娱乐帮助", v3="娱乐帮助"
        )
        return
    await _start_guess_timeout(bot, event, user_id, result)

    if result["outcome"] == "too_low":
        await handle_send(
            bot, event,
            f"【猜数字提示】\n"
            f"结果：猜小了\n"
            f"当前有效范围：{_build_range_text(game['low'], game['high'])}\n"
            f"已猜次数：{game['tries']}",
            md_type="娱乐",
            k1="猜数字信息", v1="猜数字信息",
            k2="继续猜", v2=f"猜 {(game['low'] + game['high']) // 2}",
            k3="结束猜数字", v3="结束猜数字"
        )
        return

    await handle_send(
        bot, event,
        f"【猜数字提示】\n"
        f"结果：猜大了\n"
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
    current = await _read_session(user_id)

    if current["status"] == "expired":
        _clear_timeout(user_id)
        await _send_timeout(bot, event, current["session"])
        return
    if current["status"] != "active":
        await handle_send(
            bot, event,
            "你当前没有进行中的猜数字游戏。\n发送：开始猜数字",
            md_type="娱乐",
            k1="开始猜数字", v1="开始猜数字",
            k2="猜数字帮助", v2="猜数字帮助",
            k3="小游戏帮助", v3="小游戏帮助"
        )
        return
    game = current["session"]
    await _start_guess_timeout(bot, event, user_id, current)

    await handle_send(
        bot, event,
        f"【猜数字信息】\n"
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
    now_epoch, _ = _clock_snapshot()
    result = await run_blocking_io(
        entertainment_application.guess_sessions.end,
        "number",
        user_id,
        now_epoch=now_epoch,
        timeout=5,
    )

    if result["status"] == "expired":
        _clear_timeout(user_id)
        await _send_timeout(bot, event, result["session"])
        return
    if result["status"] != "finished":
        _clear_timeout(user_id)
        await handle_send(
            bot, event,
            "你当前没有进行中的猜数字游戏。\n发送：开始猜数字",
            md_type="娱乐",
            k1="开始猜数字", v1="开始猜数字",
            k2="猜数字帮助", v2="猜数字帮助",
            k3="小游戏帮助", v3="小游戏帮助"
        )
        return

    game = result["session"]
    _clear_timeout(user_id)

    await handle_send(
        bot, event,
        f"【猜数字结束】\n"
        f"答案：{game['answer']}\n"
        f"猜测次数：{game['tries']} 次",
        md_type="娱乐",
        k1="再来一局", v1="开始猜数字",
        k2="小游戏帮助", v2="小游戏帮助",
        k3="娱乐帮助", v3="娱乐帮助"
    )


@guess_number_help_cmd.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await send_help_message(
        bot, event,
        "**猜数字帮助**\n\n"
        "**指令**\n"
        "- `开始猜数字`\n"
        f"- `猜 50`（在 {GUESS_MIN}~{GUESS_MAX} 中猜）\n"
        "- `猜数字信息`\n"
        "- `结束猜数字`\n\n"
        f"> 规则：系统随机一个 {GUESS_MIN}~{GUESS_MAX} 的整数，"
        "你根据“猜大了/猜小了”提示逐步逼近答案。",
        k1="开始猜数字", v1="开始猜数字",
        k2="示例猜测", v2="猜 50",
        k3="小游戏帮助", v3="小游戏帮助"
    )
