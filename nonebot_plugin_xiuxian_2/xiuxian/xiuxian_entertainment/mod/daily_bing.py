from ..command import *

daily_bing_cmd = on_command("每日Bing图", priority=5, block=True)


@daily_bing_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def daily_bing_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """每日Bing图"""
    config = XiuConfig()
    api_url = "https://api.pearktrue.cn/api/bing/"

    try:
        image_url = get_media_url_api(api_url, timeout=20)
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

    text_msg = "🌄 每日Bing图"

    if config.markdown_status:
        if config.markdown_id:
            try:
                msg_param = {
                    "key": "t1",
                    "values": [
                        "点击刷新](mqqapi://aio/inlinecmd?command=每日Bing图&enter=false&reply=false)\r![",
                        f"img #1280px #720px]({image_url})\r",
                        "🌄 [每日Bing图",
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
                logger.warning(f"每日Bing图 模板MD发送失败：{e}")
            await daily_bing_cmd.finish()

        elif not is_channel_event(event):
            try:
                md_msg = (
                    f"## 🌄 每日Bing图\r"
                    f"![img #1280px #720px]({image_url})\r"
                    f"[刷新](mqqapi://aio/inlinecmd?command=每日Bing图&enter=false&reply=false)"
                )
                await bot.send(event=event, message=MessageSegment.markdown(bot, md_msg))
            except Exception as e:
                logger.warning(f"每日Bing图 原生MD发送失败：{e}")
            await daily_bing_cmd.finish()
    try:
        await handle_pic_msg_send(bot, event, image_url, text_msg)
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