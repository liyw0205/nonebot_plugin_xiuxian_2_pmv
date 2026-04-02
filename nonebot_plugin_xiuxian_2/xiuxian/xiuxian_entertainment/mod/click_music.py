from ..command import *
click_music_cmd = on_command("随机点歌", priority=5, block=True)

@click_music_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def click_music_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """随机点歌"""
    config = XiuConfig()

    api_url = "https://api.pearktrue.cn/api/click_music"

    try:
        resp = requests.get(api_url, timeout=15)
        resp.raise_for_status()
        result = resp.json()
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取随机点歌失败：{e}",
            md_type="娱乐",
            k1="再试一次", v1="随机点歌",
            k2="今日老婆", v2="今日老婆",
            k3="帮助", v3="娱乐帮助"
        )
        await click_music_cmd.finish()

    if not isinstance(result, dict) or result.get("code") != 200:
        msg = result.get("msg", "接口异常") if isinstance(result, dict) else "接口异常"
        await handle_send(
            bot, event,
            f"获取随机点歌失败：{msg}",
            md_type="娱乐",
            k1="再试一次", v1="随机点歌",
            k2="今日超能力", v2="今日超能力",
            k3="帮助", v3="娱乐帮助"
        )
        await click_music_cmd.finish()

    data = result.get("data", {})
    avatar_url = data.get("avatar_url")
    nickname = data.get("nickname", "未知用户")
    lyrics = data.get("lyrics", "暂无歌词").replace('\n', '\r')
    audiosrc = data.get("audiosrc")

    text_msg = (
        f"🎵 随机点歌 · 唱鸭 🎵\n"
        f"演唱者：{nickname}\n"
        f"歌词：\n{lyrics}"
    )

    if config.markdown_status:
        if config.markdown_id:
            try:
                if avatar_url:
                    msg_param = {
                    "key": "t1",
                    "values": [
                        "](mqqapi://aio/inlinecmd?command=随机点歌&enter=false&reply=false)\r![",
                        f"img #300px #300px]({avatar_url})\r",
                        f"歌词：\r\r> {lyrics}\r\r[",
                        f"随机点歌](mqqapi://aio/inlinecmd?command=随机点歌&enter=false&reply=false)\r",
                        f"[{nickname}",
                    ]
                }
                else:
                    msg_param = {
                    "key": "t1",
                    "values": [
                        f"](mqqapi://aio/inlinecmd?command=随机点歌&enter=false&reply=false)\r"
                        f"歌词：\r\r> {lyrics}\r\r[",
                        f"随机点歌](mqqapi://aio/inlinecmd?command=随机点歌&enter=false&reply=false)\r",
                        f"> [{nickname}",
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

                # 模板MD发完后，补发音频
                if audiosrc:
                    await handle_audio_send(bot, event, audiosrc)

            except Exception as e:
                logger.warning(f"随机点歌 模板MD发送失败：{e}")
                await handle_send(
                    bot, event,
                    "随机点歌发送失败：模板 Markdown 发送异常",
                    md_type="娱乐",
                    k1="再试一次", v1="随机点歌",
                    k2="今日老婆", v2="今日老婆",
                    k3="帮助", v3="娱乐帮助"
                )
            await click_music_cmd.finish()

        else:
            if not is_channel_event(event):
                try:
                    if avatar_url:
                        md_msg = (
                            f"## 🎵 随机点歌 · 唱鸭\r"
                            f"![img #300px #300px]({avatar_url})\r"
                            f"演唱者：{nickname}\r\r"
                            f"歌词：\r\r> {lyrics}\r\r"
                            f"[再来一首](mqqapi://aio/inlinecmd?command=随机点歌&enter=false&reply=false)"
                        )
                    else:
                        md_msg = (
                            f"## 🎵 随机点歌 · 唱鸭\r"
                            f"演唱者：{nickname}\r\r"
                            f"歌词：\r\r> {lyrics}\r\r"
                            f"[再来一首](mqqapi://aio/inlinecmd?command=随机点歌&enter=false&reply=false)"
                        )

                    await bot.send(event=event, message=MessageSegment.markdown(bot, md_msg))

                    # 原生MD发完后，补发音频
                    if audiosrc:
                        await handle_audio_send(bot, event, audiosrc)

                except Exception as e:
                    logger.warning(f"随机点歌 原生MD发送失败：{e}")
                    await handle_send(
                        bot, event,
                        "随机点歌发送失败：原生 Markdown 发送异常",
                        md_type="娱乐",
                        k1="再试一次", v1="随机点歌",
                        k2="今日超能力", v2="今日超能力",
                        k3="帮助", v3="娱乐帮助"
                    )
                await click_music_cmd.finish()

    try:
        if avatar_url:
            await handle_pic_msg_send(bot, event, avatar_url, text_msg)
        else:
            await handle_send(
                bot, event,
                text_msg,
                md_type="娱乐",
                k1="再来一首", v1="随机点歌",
                k2="今日老婆", v2="今日老婆",
                k3="帮助", v3="娱乐帮助"
            )

        # 普通模式下发送真正的音频
        if audiosrc:
            await handle_audio_send(bot, event, audiosrc)

    except Exception as e:
        logger.warning(f"随机点歌 普通图文发送失败：{e}")
        await handle_send(
            bot, event,
            f"随机点歌发送失败：{e}",
            md_type="娱乐",
            k1="再试一次", v1="随机点歌",
            k2="今日老婆", v2="今日老婆",
            k3="帮助", v3="娱乐帮助"
        )

    await click_music_cmd.finish()