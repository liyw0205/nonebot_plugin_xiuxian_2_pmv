import asyncio

from ....features.entertainment.guess_application import PUZZLE_SESSION_TIMEOUT
from ....infrastructure.clock import SystemClock
from ....infrastructure.random_source import SystemRandom

from nonebot.params import CommandArg

from ..command import *
from ..io_runtime import run_blocking_io
from ..room_store import entertainment_application
from .game_utils import event_display_name


PUZZLE_TIMEOUT = PUZZLE_SESSION_TIMEOUT
DEFAULT_DIFFICULTY = "简单"
DIFFICULTIES = {
    "简单": ("简单", 4),
    "4": ("简单", 4),
    "4位": ("简单", 4),
    "4位数": ("简单", 4),
    "四位": ("简单", 4),
    "普通": ("普通", 7),
    "中等": ("普通", 7),
    "7": ("普通", 7),
    "7位": ("普通", 7),
    "7位数": ("普通", 7),
    "七位": ("普通", 7),
    "困难": ("困难", 9),
    "9": ("困难", 9),
    "9位": ("困难", 9),
    "9位数": ("困难", 9),
    "九位": ("困难", 9),
}

START_TOKENS = {"开始", "开局", "新局", "重开"}
END_TOKENS = {"结束", "答案", "查看答案", "看答案", "放弃"}
HELP_TOKENS = {"帮助", "规则", "help", "?"}
STATUS_TOKENS = {"状态", "信息", "进度"}

FULLWIDTH_DIGIT_TABLE = str.maketrans("０１２３４５６７８９", "0123456789")

# Timeout tasks are process-local; durable game state belongs to the feature repository.
guess_puzzle_timeout_tasks: dict[str, asyncio.Task] = {}
guess_puzzle_timeout_tokens: dict[str, str] = {}
runtime_clock = SystemClock()
runtime_random = SystemRandom()


def _normalize_digits(text: str) -> str:
    return (text or "").strip().translate(FULLWIDTH_DIGIT_TABLE)


def _parse_difficulty(text: str) -> tuple[str, int] | None:
    raw = (text or "").strip().lower()
    return DIFFICULTIES.get(raw)


def _difficulty_hint() -> str:
    return "简单=4位，普通=7位，困难=9位"


def _make_answer(digits: int, random_source=None) -> str:
    source = random_source or runtime_random
    first = source.choice("123456789")
    rest = "".join(source.choice("0123456789") for _ in range(digits - 1))
    return first + rest


def _example_guess(digits: int) -> str:
    base = "123456789"
    if digits <= len(base):
        return base[:digits]
    return base + "0" * (digits - len(base))


def _correct_count(answer: str, guess: str) -> int:
    return sum(1 for a, g in zip(answer, guess) if a == g)


def _encourage(correct: int, digits: int, random_source=None) -> str:
    source = random_source or runtime_random
    if correct == 0:
        choices = [
            "这一手还没撞上，但信息已经到手了，换个组合继续压。",
            "暂时空枪，别急，先把明显不顺的方向排掉。",
            "没有命中也有价值，下一手可以更大胆一点。",
        ]
    elif correct < digits // 2:
        choices = [
            "已经摸到一点门路了，继续试探，别让节奏断掉。",
            "有命中位，方向不是全错，稳住继续推。",
            "有进展，这局可以慢慢收网。",
        ]
    else:
        choices = [
            "很接近了，答案已经在你手边晃了。",
            "这一手很漂亮，再压一轮就可能破局。",
            "命中不少，保持这个思路继续收缩。",
        ]
    return source.choice(choices)


def _clear_timeout(user_id: str) -> None:
    task = guess_puzzle_timeout_tasks.pop(user_id, None)
    guess_puzzle_timeout_tokens.pop(user_id, None)
    if task and task is not asyncio.current_task():
        task.cancel()


def _clock_snapshot() -> tuple[float, str]:
    now = runtime_clock.now()
    return now.timestamp(), now.strftime("%Y-%m-%d %H:%M:%S")


async def _send_timeout(bot: Bot, event, game: dict) -> None:
    difficulty = game["difficulty"]
    await handle_send(
        bot,
        event,
        f"【猜数谜超时】\n"
        f"本局已结束。\n"
        f"答案：{game['answer']}\n"
        f"尝试次数：{game['tries']} 次\n"
        f"提示：下局可以先固定几位做排除。",
        md_type="娱乐",
        k1="再来一局",
        v1=f"开始猜数谜 {difficulty}",
        k2="换困难",
        v2="开始猜数谜 困难",
        k3="帮助",
        v3="猜数谜帮助",
    )


async def _start_timeout(bot: Bot, event, user_id: str, session: dict) -> None:
    token = str(session["session_token"])
    old_task = guess_puzzle_timeout_tasks.get(user_id)
    if old_task and not old_task.done() and guess_puzzle_timeout_tokens.get(user_id) == token:
        return
    _clear_timeout(user_id)

    async def _task():
        delay = max(float(session["expires_at"]) - runtime_clock.now().timestamp(), 0.0)
        await asyncio.sleep(delay)
        expired = await run_blocking_io(
            entertainment_application.guess_sessions.expire,
            "puzzle",
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
    guess_puzzle_timeout_tasks[user_id] = task
    guess_puzzle_timeout_tokens[user_id] = token


async def _read_session(user_id: str):
    now_epoch, _ = _clock_snapshot()
    return await run_blocking_io(
        entertainment_application.guess_sessions.read,
        "puzzle",
        user_id,
        now_epoch=now_epoch,
        timeout=5,
    )


async def _send_in_progress(bot: Bot, event, game: dict) -> None:
    digits = game["digits"]
    await handle_send(
        bot,
        event,
        f"【猜数谜进行中】\n"
        f"难度：{game['difficulty']}（{digits}位）\n"
        f"已尝试：{game['tries']} 次\n"
        f"继续发送：猜数谜 {_example_guess(digits)}\n"
        f"想放弃可发送：猜数谜 答案",
        md_type="娱乐",
        k1="继续猜",
        v1=f"猜数谜 {_example_guess(digits)}",
        k2="答案",
        v2="猜数谜 答案",
        k3="帮助",
        v3="猜数谜帮助",
    )


async def _send_help(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent) -> None:
    await send_help_message(
        bot,
        event,
        "**猜数谜帮助**\n\n"
        "**指令**\n"
        "- `开始猜数谜 [简单|普通|困难]`\n"
        "- `猜数谜 2223`\n"
        "- `猜数谜 状态`\n"
        "- `猜数谜 答案` / `猜数谜 结束`\n\n"
        f"**难度**\n{_difficulty_hint()}。\n\n"
        "> 规则：系统生成对应位数的随机数。每次猜测后，只告诉你猜对了几位；"
        "不会告诉具体位置，也不会告诉具体数字。全部猜对后自动结束。",
        k1="简单",
        v1="开始猜数谜 简单",
        k2="普通",
        v2="开始猜数谜 普通",
        k3="困难",
        v3="开始猜数谜 困难",
        k4="小游戏",
        v4="小游戏帮助",
    )


async def _start_game(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    difficulty_text: str = "",
) -> None:
    user_id = str(event.get_user_id())
    old = await _read_session(user_id)
    if old["status"] == "active":
        await _start_timeout(bot, event, user_id, old)
        await _send_in_progress(bot, event, old["session"])
        return
    if old["status"] == "expired":
        _clear_timeout(user_id)
        await _send_timeout(bot, event, old["session"])

    parsed = _parse_difficulty(difficulty_text) if difficulty_text else _parse_difficulty(DEFAULT_DIFFICULTY)
    if not parsed:
        await handle_send(
            bot,
            event,
            f"【猜数谜】\n难度格式不对。\n可选：{_difficulty_hint()}。",
            md_type="娱乐",
            k1="简单",
            v1="开始猜数谜 简单",
            k2="普通",
            v2="开始猜数谜 普通",
            k3="困难",
            v3="开始猜数谜 困难",
        )
        return

    difficulty, digits = parsed
    answer = _make_answer(digits, runtime_random)
    now_epoch, now_display = _clock_snapshot()
    started = await run_blocking_io(
        entertainment_application.guess_sessions.start_puzzle,
        user_id=user_id,
        user_name=event_display_name(event),
        answer=answer,
        difficulty=difficulty,
        digits=digits,
        create_time=now_display,
        last_action_time=now_display,
        now_epoch=now_epoch,
        timeout=5,
    )
    if started["status"] == "existing":
        await _start_timeout(bot, event, user_id, started)
        await _send_in_progress(bot, event, started["session"])
        return
    await _start_timeout(bot, event, user_id, started)

    await handle_send(
        bot,
        event,
        f"【猜数谜开始】\n"
        f"难度：{difficulty}（{digits}位）\n"
        f"操作：猜数谜 {_example_guess(digits)}\n"
        f"> 只提示猜对几位，不提示具体位置和数字。",
        md_type="娱乐",
        k1="试一手",
        v1=f"猜数谜 {_example_guess(digits)}",
        k2="答案",
        v2="猜数谜 答案",
        k3="帮助",
        v3="猜数谜帮助",
    )


async def _show_status(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent) -> None:
    user_id = str(event.get_user_id())
    current = await _read_session(user_id)
    if current["status"] == "expired":
        _clear_timeout(user_id)
        await _send_timeout(bot, event, current["session"])
        return
    if current["status"] != "active":
        await handle_send(
            bot,
            event,
            "你当前没有进行中的猜数谜。\n发送：开始猜数谜 简单/普通/困难",
            md_type="娱乐",
            k1="简单",
            v1="开始猜数谜 简单",
            k2="普通",
            v2="开始猜数谜 普通",
            k3="困难",
            v3="开始猜数谜 困难",
        )
        return

    game = current["session"]
    await _start_timeout(bot, event, user_id, current)
    digits = game["digits"]
    await handle_send(
        bot,
        event,
        f"【猜数谜状态】\n"
        f"玩家：{game['user_name']}\n"
        f"难度：{game['difficulty']}（{digits}位）\n"
        f"已尝试：{game['tries']} 次\n"
        f"创建时间：{game['create_time']}\n"
        f"最后操作：{game['last_action_time']}",
        md_type="娱乐",
        k1="继续猜",
        v1=f"猜数谜 {_example_guess(digits)}",
        k2="答案",
        v2="猜数谜 答案",
        k3="帮助",
        v3="猜数谜帮助",
    )


async def _reveal_answer(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent) -> None:
    user_id = str(event.get_user_id())
    now_epoch, _ = _clock_snapshot()
    result = await run_blocking_io(
        entertainment_application.guess_sessions.end,
        "puzzle",
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
            bot,
            event,
            "你当前没有进行中的猜数谜。\n发送：开始猜数谜 简单",
            md_type="娱乐",
            k1="开始简单",
            v1="开始猜数谜 简单",
            k2="开始普通",
            v2="开始猜数谜 普通",
            k3="帮助",
            v3="猜数谜帮助",
        )
        return

    game = result["session"]
    answer = game["answer"]
    tries = game["tries"]
    difficulty = game["difficulty"]
    _clear_timeout(user_id)

    await handle_send(
        bot,
        event,
        f"【猜数谜结束】\n"
        f"答案：{answer}\n"
        f"尝试次数：{tries} 次\n"
        f"提示：下局可以换个开局数继续试。",
        md_type="娱乐",
        k1="再来一局",
        v1=f"开始猜数谜 {difficulty}",
        k2="换困难",
        v2="开始猜数谜 困难",
        k3="帮助",
        v3="猜数谜帮助",
    )


async def _handle_guess(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    raw_guess: str,
) -> None:
    user_id = str(event.get_user_id())
    current = await _read_session(user_id)
    if current["status"] == "expired":
        _clear_timeout(user_id)
        await _send_timeout(bot, event, current["session"])
        return
    if current["status"] != "active":
        await handle_send(
            bot,
            event,
            "你当前没有进行中的猜数谜。\n发送：开始猜数谜 简单/普通/困难",
            md_type="娱乐",
            k1="简单",
            v1="开始猜数谜 简单",
            k2="普通",
            v2="开始猜数谜 普通",
            k3="困难",
            v3="开始猜数谜 困难",
        )
        return
    game = current["session"]
    await _start_timeout(bot, event, user_id, current)

    guess = _normalize_digits(raw_guess)
    digits = game["digits"]
    if not guess.isdigit():
        await handle_send(
            bot,
            event,
            f"【猜数谜】\n格式不对，请发送 {digits} 位数字。\n示例：猜数谜 {_example_guess(digits)}",
            md_type="娱乐",
            k1="示例",
            v1=f"猜数谜 {_example_guess(digits)}",
            k2="状态",
            v2="猜数谜 状态",
            k3="答案",
            v3="猜数谜 答案",
        )
        return

    if len(guess) != digits:
        await handle_send(
            bot,
            event,
            f"【猜数谜】\n"
            f"本局是 {digits} 位数，请输入正好 {digits} 位。\n"
            f"示例：猜数谜 {_example_guess(digits)}",
            md_type="娱乐",
            k1="示例",
            v1=f"猜数谜 {_example_guess(digits)}",
            k2="状态",
            v2="猜数谜 状态",
            k3="答案",
            v3="猜数谜 答案",
        )
        return

    now_epoch, now_display = _clock_snapshot()
    result = await run_blocking_io(
        entertainment_application.guess_sessions.guess_puzzle,
        user_id,
        guess,
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
            bot,
            event,
            "你当前没有进行中的猜数谜。\n发送：开始猜数谜 简单/普通/困难",
            md_type="娱乐",
            k1="简单",
            v1="开始猜数谜 简单",
            k2="普通",
            v2="开始猜数谜 普通",
            k3="困难",
            v3="开始猜数谜 困难",
        )
        return
    game = result["session"]
    correct = int(result["outcome"].split(":", 1)[1])
    tries = int(game["tries"])
    if result["status"] == "finished":
        difficulty = game["difficulty"]
        answer = game["answer"]
        _clear_timeout(user_id)
        await handle_send(
            bot,
            event,
            f"【猜数谜结束】\n"
            f"结果：全部猜对\n"
            f"答案：{answer}\n"
            f"总尝试次数：{tries} 次\n"
            f"提示：下一局可以挑战更高难度。",
            md_type="娱乐",
            k1="再来一局",
            v1=f"开始猜数谜 {difficulty}",
            k2="挑战困难",
            v2="开始猜数谜 困难",
            k3="小游戏",
            v3="小游戏帮助",
        )
        return
    await _start_timeout(bot, event, user_id, result)

    await handle_send(
        bot,
        event,
        f"【猜数谜提示】\n"
        f"本次猜测：对了 {correct} 位\n"
        f"已尝试：{tries} 次\n"
        f"{_encourage(correct, digits, runtime_random)}",
        md_type="娱乐",
        k1="继续猜",
        v1=f"猜数谜 {_example_guess(digits)}",
        k2="状态",
        v2="猜数谜 状态",
        k3="答案",
        v3="猜数谜 答案",
    )


guess_puzzle_cmd = on_command("猜数谜", aliases={"猜数迷"}, priority=5, block=True)
guess_puzzle_start_cmd = on_command("开始猜数谜", aliases={"开始猜数迷"}, priority=5, block=True)
guess_puzzle_help_cmd = on_command("猜数谜帮助", aliases={"猜数迷帮助"}, priority=5, block=True)


@guess_puzzle_cmd.handle(parameterless=[Cooldown(cd_time=0.6)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    raw = args.extract_plain_text().strip()
    if not raw:
        await _show_status(bot, event)
        return

    parts = raw.split(maxsplit=1)
    action = parts[0].strip().rstrip(":：")
    rest = parts[1].strip() if len(parts) > 1 else ""

    if action in HELP_TOKENS:
        await _send_help(bot, event)
        return
    if action in START_TOKENS:
        await _start_game(bot, event, rest)
        return
    if action in END_TOKENS:
        await _reveal_answer(bot, event)
        return
    if action in STATUS_TOKENS:
        await _show_status(bot, event)
        return
    if _parse_difficulty(raw):
        await _start_game(bot, event, raw)
        return

    await _handle_guess(bot, event, raw)


@guess_puzzle_start_cmd.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    await _start_game(bot, event, args.extract_plain_text().strip())


@guess_puzzle_help_cmd.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await _send_help(bot, event)
