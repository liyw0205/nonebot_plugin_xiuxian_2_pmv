from ..command import *

hajimi_cmd = on_command(
    "哈基米",
    aliases={"随机哈基米", "哈基米音乐", "随机哈基米音乐"},
    priority=5,
    block=True
)

@hajimi_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def hajimi_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """随机哈基米音乐"""
    
    api_url = "https://api.tangdouz.com/zzz/hjm.php"
    
    try:
        # 直接获取音频URL（文本形式）
        audio_url = get_text_api(api_url, timeout=15)
        if not audio_url or not audio_url.startswith(("http://", "https://")):
            raise ValueError("获取到的音频链接无效")
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取哈基米音乐失败：{e}",
            md_type="娱乐",
            k1="再试一次", v1="哈基米",
            k2="随机语音", v2="随机语音",
            k3="帮助", v3="娱乐帮助"
        )
        await hajimi_cmd.finish()
    
    try:
        await handle_audio_send(bot, event, audio_url)
    except Exception as e:
        logger.warning(f"哈基米音乐发送失败：{e}")
        await handle_send(
            bot, event,
            f"哈基米音乐发送失败：{e}",
            md_type="娱乐",
            k1="再试一次", v1="哈基米",
            k2="随机语音", v2="随机语音",
            k3="帮助", v3="娱乐帮助"
        )
    
    await hajimi_cmd.finish()