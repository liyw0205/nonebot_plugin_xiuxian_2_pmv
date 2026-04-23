from ..command import *

random_girl_video_cmd = on_command(
    "随机小姐姐",
    aliases={"小姐姐", "随机美女视频"},
    priority=5,
    block=True
)


@random_girl_video_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def random_girl_video_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """随机小姐姐视频"""
    config = XiuConfig()
    api_url = "https://api.yujn.cn/api/xjj.php?type=json"

    try:
        result = get_json_api(api_url, timeout=20)
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取随机小姐姐失败：{e}",
            md_type="娱乐",
            k1="再试一次", v1="随机小姐姐",
            k2="随机语音", v2="随机语音",
            k3="娱乐帮助", v3="娱乐帮助"
        )
        await random_girl_video_cmd.finish()

    if not isinstance(result, dict) or result.get("code") != 200:
        msg = result.get("tips", "接口异常") if isinstance(result, dict) else "接口异常"
        await handle_send(
            bot, event,
            f"获取随机小姐姐失败：{msg}",
            md_type="娱乐",
            k1="再试一次", v1="随机小姐姐",
            k2="今日老婆", v2="今日老婆",
            k3="娱乐帮助", v3="娱乐帮助"
        )
        await random_girl_video_cmd.finish()

    video_url = str(result.get("data", "")).strip()
    video_count = result.get("video_count", "未知")

    if not video_url:
        await handle_send(
            bot, event,
            "接口未返回视频地址，请稍后重试。",
            md_type="娱乐",
            k1="重试", v1="随机小姐姐",
            k2="随机点歌", v2="随机点歌",
            k3="娱乐帮助", v3="娱乐帮助"
        )
        await random_girl_video_cmd.finish()

    text_msg = f"💃 随机小姐姐"

    # ===== Markdown模式 =====
    if config.markdown_status:
        # 模板MD
        if config.markdown_id:
            try:
                msg_param = {
                    "key": "t1",
                    "values": [
                        "](mqqapi://aio/inlinecmd?command=随机小姐姐&enter=false&reply=false)\r",
                        f"💃 随机小姐姐\r[",
                        "再来一个](mqqapi://aio/inlinecmd?command=随机小姐姐&enter=false&reply=false)\r",
                        "[随机小姐姐"
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

                # MD文本后补发视频
                await bot.send(event=event, message=MessageSegment.video(bot, video_url))
            except Exception as e:
                logger.warning(f"随机小姐姐 模板MD发送失败：{e}")
            await random_girl_video_cmd.finish()

        # 原生MD（频道不走原生MD）
        elif not is_channel_event(event):
            try:
                md_msg = (
                    f"## 💃 随机小姐姐\r"
                    f"[再来一个](mqqapi://aio/inlinecmd?command=随机小姐姐&enter=false&reply=false)"
                )
                await bot.send(event=event, message=MessageSegment.markdown(bot, md_msg))
                await bot.send(event=event, message=MessageSegment.video(bot, video_url))
            except Exception as e:
                logger.warning(f"随机小姐姐 原生MD发送失败：{e}")
            await random_girl_video_cmd.finish()

    # ===== 普通模式回退 =====
    try:
        await handle_send(
            bot, event,
            text_msg,
            md_type="娱乐",
            k1="再来一个", v1="随机小姐姐",
            k2="随机点歌", v2="随机点歌",
            k3="娱乐帮助", v3="娱乐帮助"
        )
        await bot.send(event=event, message=MessageSegment.video(bot, video_url))
    except Exception as e:
        await handle_send(
            bot, event,
            f"随机小姐姐发送失败：{e}",
            md_type="娱乐",
            k1="再试一次", v1="随机小姐姐",
            k2="今日老婆", v2="今日老婆",
            k3="娱乐帮助", v3="娱乐帮助"
        )

    await random_girl_video_cmd.finish()