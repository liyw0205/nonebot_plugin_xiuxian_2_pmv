from ..command import *
import random

random_voice_cmd = on_command(
    "随机语音",
    aliases={"语音", "随机绿茶语音", "随机御姐撒娇语音", "绿茶语音", "御姐语音"},
    priority=5,
    block=True
)


@random_voice_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def random_voice_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """随机语音"""

    voice_type = random.choice(["绿茶", "御姐"])
    api_url = (
        "https://api.pearktrue.cn/api/greentea/"
        if voice_type == "绿茶"
        else "https://api.pearktrue.cn/api/yujie/"
    )

    try:
        result = get_json_api(api_url, params={"type": "mp3"}, timeout=15)
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取随机语音失败：{e}",
            md_type="娱乐",
            k1="再试一次", v1="随机语音",
            k2="随机点歌", v2="随机点歌",
            k3="帮助", v3="娱乐帮助"
        )
        await random_voice_cmd.finish()

    audiopath = result.get("audiopath")
    msg = result.get("msg", "接口异常")

    if not audiopath:
        await handle_send(
            bot, event,
            f"获取随机语音失败：{msg}",
            md_type="娱乐",
            k1="再试一次", v1="随机语音",
            k2="随机点歌", v2="随机点歌",
            k3="帮助", v3="娱乐帮助"
        )
        await random_voice_cmd.finish()

    try:
        await handle_send(
            bot, event,
            " ",
            md_type="娱乐",
            k1="再来一条", v1="随机语音",
            k2="随机点歌", v2="随机点歌",
            k3="帮助", v3="娱乐帮助"
        )
        await handle_audio_send(bot, event, audiopath)
    except Exception as e:
        logger.warning(f"随机语音 发送失败：{e}")
        await handle_send(
            bot, event,
            f"随机语音发送失败：{e}",
            md_type="娱乐",
            k1="再试一次", v1="随机语音",
            k2="随机点歌", v2="随机点歌",
            k3="帮助", v3="娱乐帮助"
        )

    await random_voice_cmd.finish()