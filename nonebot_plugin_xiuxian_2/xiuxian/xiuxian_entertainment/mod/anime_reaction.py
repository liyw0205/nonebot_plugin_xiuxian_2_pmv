import asyncio
import random
import requests
from io import BytesIO

from nonebot.params import CommandArg

from ..command import *


NEKOS_API_BASE = "https://nekos.best/api/v2"
NEKOS_USER_AGENT = (
    "nonebot_plugin_xiuxian_2_pmv/1.0 "
    "(https://github.com/MyXiaoNan/nonebot_plugin_xiuxian_2_pmv)"
)

IMAGE_CATEGORIES = {
    "猫娘": "neko",
    "neko": "neko",
    "老婆": "waifu",
    "waifu": "waifu",
    "狐娘": "kitsune",
    "kitsune": "kitsune",
    "老公": "husbando",
    "husbando": "husbando",
}
DEFAULT_IMAGE_CATEGORIES = ("neko", "waifu", "kitsune")

ACTION_CATEGORIES = {
    "抱抱": ("hug", "抱抱送达。"),
    "贴贴": ("cuddle", "贴贴成功。"),
    "摸摸": ("pat", "轻轻摸了摸。"),
    "拍头": ("pat", "拍头安抚成功。"),
    "亲亲": ("kiss", "亲亲送达。"),
    "戳戳": ("poke", "戳了一下。"),
    "击掌": ("highfive", "击掌成功。"),
    "咬": ("bite", "咬了一口。"),
    "挥手": ("wave", "挥手打招呼。"),
}


def _fetch_nekos_sync(category: str) -> tuple[BytesIO, str]:
    resp = requests.get(
        f"{NEKOS_API_BASE}/{category}",
        headers={"User-Agent": NEKOS_USER_AGENT},
        timeout=15,
    )
    resp.raise_for_status()
    result = resp.json()
    if not isinstance(result, dict):
        raise ValueError("接口返回不是JSON对象")

    items = result.get("results")
    if not isinstance(items, list) or not items:
        raise ValueError("接口未返回图片")

    item = items[0]
    if not isinstance(item, dict):
        raise ValueError("接口图片数据异常")

    media_url = str(item.get("url") or "").strip()
    if not media_url:
        raise ValueError("接口未返回图片地址")

    source = str(item.get("anime_name") or item.get("artist_name") or item.get("source_url") or "").strip()
    media_resp = requests.get(
        media_url,
        headers={"User-Agent": NEKOS_USER_AGENT},
        timeout=20,
    )
    media_resp.raise_for_status()
    return BytesIO(media_resp.content), source


async def _fetch_nekos(category: str) -> tuple[BytesIO, str]:
    return await asyncio.to_thread(_fetch_nekos_sync, category)


def _pick_image_category(raw: str) -> tuple[str, str]:
    text = (raw or "").strip().lower()
    if text in IMAGE_CATEGORIES:
        return IMAGE_CATEGORIES[text], text
    category = random.choice(DEFAULT_IMAGE_CATEGORIES)
    return category, "随机"


def _pick_action(raw_message: str, args_text: str) -> tuple[str, str, str]:
    combined = f"{raw_message} {args_text}"
    for command, (category, title) in ACTION_CATEGORIES.items():
        if command in combined:
            return category, command, title

    token = args_text.strip().split(maxsplit=1)[0] if args_text.strip() else ""
    if token in ACTION_CATEGORIES:
        category, title = ACTION_CATEGORIES[token]
        return category, token, title

    return "hug", "抱抱", "抱抱送达。"


async def _send_nekos_image(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    media_data: BytesIO,
    text: str,
    retry_command: str,
) -> None:
    try:
        media_data.seek(0)
        await handle_pic_msg_send(bot, event, media_data, text)
    except Exception as e:
        await handle_send(
            bot,
            event,
            f"发送二次元图片失败：{e}",
            md_type="娱乐",
            k1="重试",
            v1=retry_command,
            k2="摸鱼日报",
            v2="摸鱼日报",
            k3="帮助",
            v3="娱乐帮助",
        )


random_anime_cmd = on_command(
    "随机二次元",
    aliases={"随机猫娘", "猫娘图", "二次元图", "随机老婆", "随机狐娘", "随机老公"},
    priority=5,
    block=True,
)
anime_action_cmd = on_command(
    "动漫互动",
    aliases=set(ACTION_CATEGORIES.keys()),
    priority=5,
    block=True,
)
anime_reaction_help_cmd = on_command(
    "二次元帮助",
    aliases={"动漫互动帮助", "随机二次元帮助"},
    priority=5,
    block=True,
)


@random_anime_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def random_anime_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    raw = args.extract_plain_text().strip()
    raw_message = event.get_plaintext().strip()
    category, label = _pick_image_category(raw)
    if not raw:
        if "狐娘" in raw_message:
            category, label = "kitsune", "狐娘"
        elif "老公" in raw_message:
            category, label = "husbando", "老公"
        elif "老婆" in raw_message:
            category, label = "waifu", "老婆"
        elif "猫娘" in raw_message:
            category, label = "neko", "猫娘"

    try:
        media_data, source = await _fetch_nekos(category)
    except Exception as e:
        await handle_send(
            bot,
            event,
            f"获取随机二次元失败：{e}",
            md_type="娱乐",
            k1="再试一次",
            v1="随机二次元",
            k2="摸鱼日报",
            v2="摸鱼日报",
            k3="帮助",
            v3="娱乐帮助",
        )
        await random_anime_cmd.finish()

    text = f"随机二次元：{label}"
    if source:
        text += f"\n来源：{source}"
    await _send_nekos_image(bot, event, media_data, text, "随机二次元")
    await random_anime_cmd.finish()


@anime_action_cmd.handle(parameterless=[Cooldown(cd_time=4)])
async def anime_action_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    args_text = args.extract_plain_text().strip()
    raw_message = event.get_plaintext().strip()
    category, command, title = _pick_action(raw_message, args_text)

    try:
        media_data, source = await _fetch_nekos(category)
    except Exception as e:
        await handle_send(
            bot,
            event,
            f"获取动漫互动失败：{e}",
            md_type="娱乐",
            k1="抱抱",
            v1="抱抱",
            k2="摸摸",
            v2="摸摸",
            k3="帮助",
            v3="二次元帮助",
        )
        await anime_action_cmd.finish()

    text = title
    if source:
        text += f"\n来源：{source}"
    await _send_nekos_image(bot, event, media_data, text, command)
    await anime_action_cmd.finish()


@anime_reaction_help_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def anime_reaction_help_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await send_help_message(
        bot,
        event,
        "【二次元图片与互动】\n"
        "随机图片：\n"
        "- 随机二次元\n"
        "- 随机猫娘 / 随机老婆 / 随机狐娘 / 随机老公\n\n"
        "动作 GIF：\n"
        "- 抱抱 / 贴贴 / 摸摸 / 拍头\n"
        "- 亲亲 / 戳戳 / 击掌 / 挥手",
        k1="随机二次元",
        v1="随机二次元",
        k2="抱抱",
        v2="抱抱",
        k3="摸鱼日报",
        v3="摸鱼日报",
        k4="娱乐帮助",
        v4="娱乐帮助",
    )
    await anime_reaction_help_cmd.finish()
