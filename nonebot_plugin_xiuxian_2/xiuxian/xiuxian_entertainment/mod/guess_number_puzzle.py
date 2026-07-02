import asyncio
import random
from datetime import datetime

from nonebot.params import CommandArg

from ..command import *


PUZZLE_TIMEOUT = 900
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

guess_puzzle_sessions: dict[str, dict] = {}
guess_puzzle_timeout_tasks: dict[str, asyncio.Task] = {}


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _name_from_event(event) -> str:
    sender = getattr(event, "sender", None)
    if sender:
        return sender.card or sender.nickname or str(event.get_user_id())
    return str(event.get_user_id())


def _normalize_digits(text: str) -> str:
    return (text or "").strip().translate(FULLWIDTH_DIGIT_TABLE)


def _parse_difficulty(text: str) -> tuple[str, int] | None:
    raw = (text or "").strip().lower()
    return DIFFICULTIES.get(raw)


def _difficulty_hint() -> str:
    return "简单=4位，普通=7位，困难=9位"


def _make_answer(digits: int) -> str:
    first = random.choice("123456789")
    rest = "".join(random.choice("0123456789") for _ in range(digits - 1))
    return first + rest


def _example_guess(digits: int) -> str:
    base = "123456789"
    if digits <= len(base):
        return base[:digits]
    return base + "0" * (digits - len(base))


def _correct_count(answer: str, guess: str) -> int:
    return sum(1 for a, g in zip(answer, guess) if a == g)


def _encourage(correct: int, digits: int) -> str:
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
    return random.choice(choices)


def _clear_session(user_id: str) -> None:
    guess_puzzle_sessions.pop(user_id, None)
    task = guess_puzzle_timeout_tasks.pop(user_id, None)
    if task:
        task.cancel()


async def _start_timeout(bot: Bot, event, user_id: str) -> None:
    old_task = guess_puzzle_timeout_tasks.get(user_id)
    if old_task:
        old_task.cancel()

    async def _task():
        await asyncio.sleep(PUZZLE_TIMEOUT)
        game = guess_puzzle_sessions.get(user_id)
        if not game or game.get("status") != "playing":
            return

        answer = game["answer"]
        tries = game["tries"]
        difficulty = game["difficulty"]
        _clear_session(user_id)

        await handle_send(
            bot,
            event,
            f"【猜数谜超时】\n"
            f"本局已结束。\n"
            f"答案：{answer}\n"
            f"尝试次数：{tries} 次\n"
            f"提示：下局可以先固定几位做排除。",
            md_type="娱乐",
            k1="再来一局",
            v1=f"开始猜数谜 {difficulty}",
            k2="换困难",
            v2="开始猜数谜 困难",
            k3="帮助",
            v3="猜数谜帮助",
        )

    guess_puzzle_timeout_tasks[user_id] = asyncio.create_task(_task())


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
    old = guess_puzzle_sessions.get(user_id)
    if old and old.get("status") == "playing":
        digits = old["digits"]
        await handle_send(
            bot,
            event,
            f"【猜数谜进行中】\n"
            f"难度：{old['difficulty']}（{digits}位）\n"
            f"已尝试：{old['tries']} 次\n"
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
        return

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
    answer = _make_answer(digits)
    guess_puzzle_sessions[user_id] = {
        "user_id": user_id,
        "user_name": _name_from_event(event),
        "answer": answer,
        "difficulty": difficulty,
        "digits": digits,
        "tries": 0,
        "status": "playing",
        "create_time": _now_str(),
        "last_action_time": _now_str(),
    }
    await _start_timeout(bot, event, user_id)

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
    game = guess_puzzle_sessions.get(user_id)
    if not game or game.get("status") != "playing":
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
    game = guess_puzzle_sessions.get(user_id)
    if not game or game.get("status") != "playing":
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

    answer = game["answer"]
    tries = game["tries"]
    difficulty = game["difficulty"]
    _clear_session(user_id)

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
    game = guess_puzzle_sessions.get(user_id)
    if not game or game.get("status") != "playing":
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

    await _start_timeout(bot, event, user_id)
    game["tries"] += 1
    game["last_action_time"] = _now_str()

    answer = game["answer"]
    correct = _correct_count(answer, guess)
    tries = game["tries"]
    if correct == digits:
        difficulty = game["difficulty"]
        _clear_session(user_id)
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

    await handle_send(
        bot,
        event,
        f"【猜数谜提示】\n"
        f"本次猜测：对了 {correct} 位\n"
        f"已尝试：{tries} 次\n"
        f"{_encourage(correct, digits)}",
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
