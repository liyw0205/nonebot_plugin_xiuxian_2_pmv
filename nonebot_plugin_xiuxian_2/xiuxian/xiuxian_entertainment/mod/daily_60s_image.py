from ..command import *

daily_60s_image_cmd = on_command(
    "每日60S图片",
    aliases={"60S图片", "60s图片"},
    priority=5,
    block=True
)


@daily_60s_image_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def daily_60s_image_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """每日60S 图片版"""
    api_url = "https://api.pearapi.ai/api/60s/image/"

    try:
        image_url = await get_media_url_api(api_url, timeout=20)
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取每日60S图片失败：{e}",
            md_type="娱乐",
            k1="重试", v1="每日60S图片",
            k2="60S读世界", v2="60S读世界",
            k3="帮助", v3="娱乐帮助"
        )
        await daily_60s_image_cmd.finish()

    try:
        await send_entertainment_image_result(
            bot,
            event,
            image_url,
            "每日60S图片",
            title="每日60S图片",
            buttons=[("刷新", "每日60S图片"), ("60S读世界", "60S读世界")],
        )
    except Exception as e:
        await handle_send(
            bot, event,
            f"每日60S图片发送失败：{e}",
            md_type="娱乐",
            k1="重试", v1="每日60S图片",
            k2="60S读世界", v2="60S读世界",
            k3="帮助", v3="娱乐帮助"
        )

    await daily_60s_image_cmd.finish()
