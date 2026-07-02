from ..command import *

hot_rank_image_cmd = on_command(
    "热榜图片",
    aliases={"百度热榜图片", "微博热榜图片"},
    priority=5,
    block=True
)


@hot_rank_image_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def hot_rank_image_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """热榜60S 图片版"""
    api_url = "https://api.pearapi.ai/api/60s/image/hot/"

    raw_msg = str(event.message)
    rank_type = "baidu"
    title = "微博热榜图片" if "微博热榜图片" in raw_msg else "百度热榜图片"

    if "微博热榜图片" in raw_msg:
        rank_type = "weibo"

    try:
        image_url = await get_media_url_api(api_url, params={"type": rank_type}, timeout=20)
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取热榜图片失败：{e}",
            md_type="娱乐",
            k1="百度图", v1="百度热榜图片",
            k2="微博图", v2="微博热榜图片",
            k3="帮助", v3="娱乐帮助"
        )
        await hot_rank_image_cmd.finish()

    try:
        await send_entertainment_image_result(
            bot,
            event,
            image_url,
            title,
            title=title,
            buttons=[("百度图", "百度热榜图片"), ("微博图", "微博热榜图片"), ("娱乐帮助", "娱乐帮助")],
        )
    except Exception as e:
        await handle_send(
            bot, event,
            f"热榜图片发送失败：{e}",
            md_type="娱乐",
            k1="百度图", v1="百度热榜图片",
            k2="微博图", v2="微博热榜图片",
            k3="帮助", v3="娱乐帮助"
        )

    await hot_rank_image_cmd.finish()
