import time
from urllib.parse import quote

from nonebot.params import CommandArg

from ..command import *


CATAAS_BASE = "https://cataas.com"
CAT_TEXT_MAX_LEN = 40


def _random_cat_url() -> str:
    return f"{CATAAS_BASE}/cat?ts={int(time.time())}"


def _cat_says_url(text: str) -> str:
    encoded = quote(text, safe="")
    return (
        f"{CATAAS_BASE}/cat/says/{encoded}"
        "?fontSize=40&fontColor=white&position=center"
        f"&ts={int(time.time())}"
    )


random_cat_cmd = on_command(
    "随机猫猫",
    aliases={"随机猫图", "猫猫", "猫图"},
    priority=5,
    block=True,
)
cat_says_cmd = on_command(
    "猫猫说",
    aliases={"猫说", "猫猫说话"},
    priority=5,
    block=True,
)
cat_help_cmd = on_command(
    "猫猫帮助",
    aliases={"猫图帮助", "猫猫说帮助"},
    priority=5,
    block=True,
)


@random_cat_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def random_cat_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    try:
        await handle_pic_msg_send(bot, event, _random_cat_url(), "随机猫猫已送达。")
    except Exception as e:
        await handle_send(
            bot,
            event,
            f"获取随机猫猫失败：{e}",
            md_type="娱乐",
            k1="重试",
            v1="随机猫猫",
            k2="猫猫说",
            v2="猫猫说 今天也要修仙",
            k3="娱乐帮助",
            v3="娱乐帮助",
        )
    await random_cat_cmd.finish()


@cat_says_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def cat_says_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    text = args.extract_plain_text().strip()
    if not text or text in {"帮助", "help", "?"}:
        await _send_cat_help(bot, event)
        await cat_says_cmd.finish()

    text = text[:CAT_TEXT_MAX_LEN]
    try:
        await handle_pic_msg_send(bot, event, _cat_says_url(text), f"猫猫说：{text}")
    except Exception as e:
        await handle_send(
            bot,
            event,
            f"生成猫猫说话失败：{e}",
            md_type="娱乐",
            k1="示例",
            v1="猫猫说 今天也要修仙",
            k2="随机猫猫",
            v2="随机猫猫",
            k3="娱乐帮助",
            v3="娱乐帮助",
        )
    await cat_says_cmd.finish()


async def _send_cat_help(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await send_help_message(
        bot,
        event,
        "【猫猫图片】\n"
        "用法：\n"
        "- 随机猫猫\n"
        "- 猫猫说 今天也要修仙\n\n"
        f"猫猫说话最多取前 {CAT_TEXT_MAX_LEN} 个字符。",
        k1="随机猫猫",
        v1="随机猫猫",
        k2="猫猫说",
        v2="猫猫说 今天也要修仙",
        k3="宝可梦",
        v3="宝可梦盲盒",
        k4="娱乐帮助",
        v4="娱乐帮助",
    )


@cat_help_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def cat_help_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await _send_cat_help(bot, event)
    await cat_help_cmd.finish()
