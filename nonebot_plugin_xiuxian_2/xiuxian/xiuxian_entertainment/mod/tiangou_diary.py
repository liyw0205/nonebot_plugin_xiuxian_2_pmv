from ..command import *

tiangou_diary_cmd = on_command("舔狗日记", priority=5, block=True)


@tiangou_diary_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def tiangou_diary_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """舔狗日记"""
    api_url = "https://api.pearktrue.cn/api/jdyl/tiangou.php"

    try:
        text = get_text_api(api_url, timeout=15)
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取舔狗日记失败：{e}",
            md_type="娱乐",
            k1="重试", v1="舔狗日记",
            k2="随机一言", v2="随机一言",
            k3="帮助", v3="娱乐帮助"
        )
        await tiangou_diary_cmd.finish()

    if not text:
        await handle_send(
            bot, event,
            "获取舔狗日记失败：接口返回为空",
            md_type="娱乐",
            k1="重试", v1="舔狗日记",
            k2="随机一言", v2="随机一言",
            k3="帮助", v3="娱乐帮助"
        )
        await tiangou_diary_cmd.finish()

    msg = f"🐶 舔狗日记\n{text}"

    await handle_send(
        bot, event, msg,
        md_type="娱乐",
        k1="再来一篇", v1="舔狗日记",
        k2="随机一言", v2="随机一言",
        k3="帮助", v3="娱乐帮助"
    )
    await tiangou_diary_cmd.finish()