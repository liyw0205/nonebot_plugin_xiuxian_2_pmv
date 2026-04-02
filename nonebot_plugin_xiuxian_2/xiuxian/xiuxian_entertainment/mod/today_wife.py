from ..command import *
today_wife_cmd = on_command("今日老婆", priority=5, block=True)


def _safe_int(value, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


@today_wife_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def today_wife_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """今日老婆"""
    config = XiuConfig()

    api_url = "https://api.pearktrue.cn/api/today_wife"

    try:
        resp = requests.get(api_url, timeout=15)
        resp.raise_for_status()
        result = resp.json()
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取今日老婆失败：{e}",
            md_type="娱乐",
            k1="再试一次", v1="今日老婆",
            k2="超能力", v2="今日超能力",
            k3="帮助", v3="娱乐帮助"
        )
        await today_wife_cmd.finish()

    if not isinstance(result, dict) or result.get("code") != 200:
        msg = result.get("msg", "接口异常") if isinstance(result, dict) else "接口异常"
        await handle_send(
            bot, event,
            f"获取今日老婆失败：{msg}",
            md_type="娱乐",
            k1="再试一次", v1="今日老婆",
            k2="超能力", v2="今日超能力",
            k3="帮助", v3="娱乐帮助"
        )
        await today_wife_cmd.finish()

    data = result.get("data", {})
    image_url = data.get("image_url")
    role_name = data.get("role_name", "未知角色")
    width = _safe_int(data.get("width"), 800)
    height = _safe_int(data.get("height"), 1200)

    if not image_url:
        await handle_send(
            bot, event,
            "获取今日老婆失败：接口未返回图片地址",
            md_type="娱乐",
            k1="再试一次", v1="今日老婆",
            k2="超能力", v2="今日超能力",
            k3="帮助", v3="娱乐帮助"
        )
        await today_wife_cmd.finish()

    text_msg = (
        f"💕 今日老婆 💕\n"
        f"名字：{role_name}\n"
        f"尺寸：{width} × {height}"
    )

    if config.markdown_status:
        if config.markdown_id:
            try:
                msg_param = {
                    "key": "t1",
                    "values": [
                        "](mqqapi://aio/inlinecmd?command=今日老婆&enter=false&reply=false)\r![",
                        f"img #{width}px #{height}px]({image_url})\r",
                        f"💕 今日老婆：[{role_name}"
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
                logger.warning(f"今日老婆 模板MD发送失败：{e}")
                await handle_send(
                    bot, event,
                    "今日老婆发送失败：模板 Markdown 发送异常",
                    md_type="娱乐",
                    k1="再试一次", v1="今日老婆",
                    k2="超能力", v2="今日超能力",
                    k3="帮助", v3="娱乐帮助"
                )
            await today_wife_cmd.finish()

        else:
            if not is_channel_event(event):
                try:
                    md_msg = (
                        f"## 💕 今日老婆\r"
                        f"![img #{width}px #{height}px]({image_url})\r"
                        f"名字：{role_name}\r"
                        f"尺寸：{width} × {height}\r\r"
                        f"[再来一张](mqqapi://aio/inlinecmd?command=今日老婆&enter=false&reply=false) | "
                        f"[查看原图](mqqapi://aio/inlinecmd?command={image_url}&enter=false&reply=false)"
                    )
                    await bot.send(event=event, message=MessageSegment.markdown(bot, md_msg))
                except Exception as e:
                    logger.warning(f"今日老婆 原生MD发送失败：{e}")
                    await handle_send(
                        bot, event,
                        "今日老婆发送失败：原生 Markdown 发送异常",
                        md_type="娱乐",
                        k1="再试一次", v1="今日老婆",
                        k2="超能力", v2="今日超能力",
                        k3="帮助", v3="娱乐帮助"
                    )
                await today_wife_cmd.finish()

    try:
        await handle_pic_msg_send(bot, event, image_url, text_msg)
    except Exception as e:
        logger.warning(f"今日老婆 普通图文发送失败：{e}")
        await handle_send(
            bot, event,
            f"今日老婆发送失败：{e}",
            md_type="娱乐",
            k1="再试一次", v1="今日老婆",
            k2="超能力", v2="今日超能力",
            k3="帮助", v3="娱乐帮助"
        )

    await today_wife_cmd.finish()