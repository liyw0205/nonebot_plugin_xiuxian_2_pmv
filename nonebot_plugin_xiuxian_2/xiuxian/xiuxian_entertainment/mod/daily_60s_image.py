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
    config = XiuConfig()
    api_url = "https://api.pearktrue.cn/api/60s/image/"

    try:
        image_url = get_media_url_api(api_url, timeout=20)
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

    title = "📰 每日60S图片"

    if config.markdown_status:
        if config.markdown_id:
            try:
                msg_param = {
                    "key": "t1",
                    "values": [
                        "[点击刷新](mqqapi://aio/inlinecmd?command=每日60S图片&enter=false&reply=false)\r![",
                        f"img #1080px #1920px]({image_url})\r",
                        "📰 [每日60S图片"
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
                logger.warning(f"每日60S图片 模板MD发送失败：{e}")
            await daily_60s_image_cmd.finish()

        elif not is_channel_event(event):
            try:
                md_msg = (
                    f"## 📰 每日60S图片\r"
                    f"![img #1080px #1920px]({image_url})\r"
                    f"[刷新](mqqapi://aio/inlinecmd?command=每日60S图片&enter=false&reply=false)"
                )
                await bot.send(event=event, message=MessageSegment.markdown(bot, md_msg))
            except Exception as e:
                logger.warning(f"每日60S图片 原生MD发送失败：{e}")
            await daily_60s_image_cmd.finish()

    try:
        await handle_pic_msg_send(bot, event, image_url, title)
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