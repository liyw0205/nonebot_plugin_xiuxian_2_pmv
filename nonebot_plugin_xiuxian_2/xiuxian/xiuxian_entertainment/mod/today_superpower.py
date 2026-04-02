from ..command import *
today_superpower_cmd = on_command("今日超能力", priority=5, block=True)


@today_superpower_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def today_superpower_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """今日超能力"""
    config = XiuConfig()

    api_url = "https://api.pearktrue.cn/api/superpower"

    try:
        resp = requests.get(api_url, timeout=15)
        resp.raise_for_status()
        result = resp.json()
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取今日超能力失败：{e}",
            md_type="娱乐",
            k1="再试一次", v1="今日超能力",
            k2="今日老婆", v2="今日老婆",
            k3="帮助", v3="娱乐帮助"
        )
        await today_superpower_cmd.finish()

    if not isinstance(result, dict) or result.get("code") != 200:
        msg = result.get("msg", "接口异常") if isinstance(result, dict) else "接口异常"
        await handle_send(
            bot, event,
            f"获取今日超能力失败：{msg}",
            md_type="娱乐",
            k1="再试一次", v1="今日超能力",
            k2="今日老婆", v2="今日老婆",
            k3="帮助", v3="娱乐帮助"
        )
        await today_superpower_cmd.finish()

    data = result.get("data", {})
    superpower = data.get("superpower", "未知超能力")
    disadvantage = data.get("disadvantage", "暂无副作用说明")
    image_url = data.get("image_url")

    text_msg = (
        f"🦸 今日超能力 🦸\n"
        f"超能力：{superpower}\n"
        f"但是：{disadvantage}"
    )

    if config.markdown_status:
        if config.markdown_id:
            try:
                if image_url:
                    msg_param = {
                        "key": "t1",
                        "values": [
                            "](mqqapi://aio/inlinecmd?command=今日超能力&enter=false&reply=false)\r![",
                            "img #800px #800px](" + image_url + ")\r",
                            f"🦸 今日超能力：[{superpower}\r但是：{disadvantage}"
                        ]
                    }
                else:
                    msg_param = {
                        "key": "t1",
                        "values": [
                            f"🦸 今日超能力：[{superpower}\r但是：{disadvantage}"
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
                logger.warning(f"今日超能力 模板MD发送失败：{e}")
                await handle_send(
                    bot, event,
                    "今日超能力发送失败：模板 Markdown 发送异常",
                    md_type="娱乐",
                    k1="再试一次", v1="今日超能力",
                    k2="今日老婆", v2="今日老婆",
                    k3="帮助", v3="娱乐帮助"
                )
            await today_superpower_cmd.finish()

        else:
            if not is_channel_event(event):
                try:
                    if image_url:
                        md_msg = (
                            f"## 🦸 今日超能力\r"
                            f"![img #800px #800px]({image_url})\r"
                            f"超能力：{superpower}\r"
                            f"但是：{disadvantage}\r\r"
                            f"[再来一次](mqqapi://aio/inlinecmd?command=今日超能力&enter=false&reply=false)"
                        )
                    else:
                        md_msg = (
                            f"## 🦸 今日超能力\r"
                            f"超能力：{superpower}\r"
                            f"但是：{disadvantage}\r\r"
                            f"[再来一次](mqqapi://aio/inlinecmd?command=今日超能力&enter=false&reply=false)"
                        )

                    await bot.send(event=event, message=MessageSegment.markdown(bot, md_msg))
                except Exception as e:
                    logger.warning(f"今日超能力 原生MD发送失败：{e}")
                    await handle_send(
                        bot, event,
                        "今日超能力发送失败：原生 Markdown 发送异常",
                        md_type="娱乐",
                        k1="再试一次", v1="今日超能力",
                        k2="今日老婆", v2="今日老婆",
                        k3="帮助", v3="娱乐帮助"
                    )
                await today_superpower_cmd.finish()

    try:
        if image_url:
            await handle_pic_msg_send(bot, event, image_url, text_msg)
        else:
            await handle_send(
                bot, event,
                text_msg,
                md_type="娱乐",
                k1="再试一次", v1="今日超能力",
                k2="今日老婆", v2="今日老婆",
                k3="帮助", v3="娱乐帮助"
            )
    except Exception as e:
        logger.warning(f"今日超能力 普通图文发送失败：{e}")
        await handle_send(
            bot, event,
            f"今日超能力发送失败：{e}",
            md_type="娱乐",
            k1="再试一次", v1="今日超能力",
            k2="今日老婆", v2="今日老婆",
            k3="帮助", v3="娱乐帮助"
        )

    await today_superpower_cmd.finish()