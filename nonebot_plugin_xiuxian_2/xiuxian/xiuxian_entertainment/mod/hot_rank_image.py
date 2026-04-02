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
    config = XiuConfig()
    api_url = "https://api.pearktrue.cn/api/60s/image/hot/"

    raw_msg = str(event.message)
    rank_type = "baidu"
    title = "🖼️ 百度热榜图片"

    if "微博热榜图片" in raw_msg:
        rank_type = "weibo"
        title = "🖼️ 微博热榜图片"

    try:
        image_url = get_media_url_api(api_url, params={"type": rank_type}, timeout=20)
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

    if config.markdown_status:
        if config.markdown_id:
            try:
                msg_param = {
                    "key": "t1",
                    "values": [
                        "[点击刷新](mqqapi://aio/inlinecmd?command=热榜图片&enter=false&reply=false)\r![",
                        f"img #1080px #1920px]({image_url})\r",
                        f"[{title}"
                    ]
                }
                await handle_send_md(
                    bot,
                    event,
                    " ",
                    markdown_id=config.markdown_id,
                    msg_param=msg_param,
                    at_msg=None
                )
            except Exception as e:
                logger.warning(f"热榜图片 模板MD发送失败：{e}")
            await hot_rank_image_cmd.finish()
        elif not is_channel_event(event):
            try:
                md_msg = (
                    f"## {title}\r"
                    f"![img #1080px #1920px]({image_url})\r"
                    f"[刷新](mqqapi://aio/inlinecmd?command=热榜图片&enter=false&reply=false)"
                )
                await bot.send(event=event, message=MessageSegment.markdown(bot, md_msg))
            except Exception as e:
                logger.warning(f"热榜图片 原生MD发送失败：{e}")
                await hot_rank_image_cmd.finish()
    try:
        await handle_pic_msg_send(bot, event, image_url, title)
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