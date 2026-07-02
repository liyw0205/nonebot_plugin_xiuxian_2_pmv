from ..command import *
from ...xiuxian_utils.utils import escape_markdown_text

click_music_cmd = on_command("随机点歌", priority=5, block=True)


def _build_click_music_markdown(nickname: str, lyric_lines: list[str]) -> str:
    lyric_block = "\r".join(f"> {escape_markdown_text(line)}" for line in lyric_lines) or "> 暂无歌词"
    return (
        "**随机点歌**\r\r"
        f"> **演唱者**：{escape_markdown_text(nickname)}\r\r"
        "**歌词**\r"
        f"{lyric_block}\r\r"
        "[再来一首](mqqapi://aio/inlinecmd?command=随机点歌&enter=false&reply=false)"
    )


@click_music_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def click_music_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """随机点歌"""
    config = XiuConfig()

    api_url = "https://api.pearapi.ai/api/click_music"

    try:
        result = await get_json_api(api_url, timeout=15)
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
    lyrics = str(data.get("lyrics") or "暂无歌词").replace('\n', '\r')
    lyric_lines = [line.strip() for line in lyrics.split('\r') if line.strip()]
    plain_lyrics = "\n".join(lyric_lines) or "暂无歌词"
    audiosrc = data.get("audiosrc")

    text_msg = (
        "【随机点歌】\n"
        f"演唱者：{nickname}\n"
        f"歌词：\n{plain_lyrics}"
    )

    if config.markdown_status:
        try:
            await handle_send(
                bot,
                event,
                _build_click_music_markdown(nickname, lyric_lines),
                native_markdown=True,
                fallback_msg=text_msg,
                keyboard_rows=[
                    [("再来一首", "随机点歌"), ("娱乐帮助", "娱乐帮助")]
                ],
                at_msg=False,
            )

            if avatar_url:
                try:
                    await bot.send(event=event, message=MessageSegment.image(bot, avatar_url))
                except Exception as e:
                    logger.warning(f"随机点歌头像发送失败：{e}")

            if audiosrc:
                try:
                    await handle_audio_send(bot, event, audiosrc)
                except Exception as e:
                    logger.warning(f"随机点歌音频发送失败：{e}")
                    await handle_send(
                        bot,
                        event,
                        f"随机点歌音频发送失败：{e}",
                        md_type="娱乐",
                        k1="再试一次", v1="随机点歌",
                        k2="娱乐帮助", v2="娱乐帮助",
                    )

            return

        except Exception as e:
            logger.warning(f"随机点歌 Markdown发送失败：{e}")

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
