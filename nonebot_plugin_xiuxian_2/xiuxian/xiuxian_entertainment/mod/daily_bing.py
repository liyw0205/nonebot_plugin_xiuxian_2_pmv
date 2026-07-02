from ..command import *

daily_bing_cmd = on_command("每日Bing图", priority=5, block=True)


@daily_bing_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def daily_bing_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """每日Bing图"""
    api_url = "https://api.pearapi.ai/api/bing/"

    try:
        image_url = await get_media_url_api(api_url, timeout=20)
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取每日Bing图失败：{e}",
            md_type="娱乐",
            k1="重试", v1="每日Bing图",
            k2="随机一言", v2="随机一言",
            k3="帮助", v3="娱乐帮助"
        )
        await daily_bing_cmd.finish()

    try:
        await send_entertainment_image_result(
            bot,
            event,
            image_url,
            "每日Bing图",
            title="每日Bing图",
            buttons=[("刷新", "每日Bing图"), ("娱乐帮助", "娱乐帮助")],
        )
    except Exception as e:
        await handle_send(
            bot, event,
            f"每日Bing图发送失败：{e}",
            md_type="娱乐",
            k1="重试", v1="每日Bing图",
            k2="随机一言", v2="随机一言",
            k3="帮助", v3="娱乐帮助"
        )

    await daily_bing_cmd.finish()
