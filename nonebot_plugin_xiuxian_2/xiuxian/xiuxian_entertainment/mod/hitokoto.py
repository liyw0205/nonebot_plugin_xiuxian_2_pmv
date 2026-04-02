from ..command import *

hitokoto_cmd = on_command("随机一言", priority=5, block=True)


@hitokoto_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def hitokoto_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """随机一言"""
    api_url = "https://api.pearktrue.cn/api/hitokoto/"

    try:
        text = get_text_api(api_url, timeout=15)
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取随机一言失败：{e}",
            md_type="娱乐",
            k1="重试", v1="随机一言",
            k2="舔狗日记", v2="舔狗日记",
            k3="帮助", v3="娱乐帮助"
        )
        await hitokoto_cmd.finish()

    if not text:
        await handle_send(
            bot, event,
            "获取随机一言失败：接口返回为空",
            md_type="娱乐",
            k1="重试", v1="随机一言",
            k2="舔狗日记", v2="舔狗日记",
            k3="帮助", v3="娱乐帮助"
        )
        await hitokoto_cmd.finish()

    msg = f"💬 随机一言\n{text}"

    await handle_send(
        bot, event, msg,
        md_type="娱乐",
        k1="再来一句", v1="随机一言",
        k2="舔狗日记", v2="舔狗日记",
        k3="帮助", v3="娱乐帮助"
    )
    await hitokoto_cmd.finish()