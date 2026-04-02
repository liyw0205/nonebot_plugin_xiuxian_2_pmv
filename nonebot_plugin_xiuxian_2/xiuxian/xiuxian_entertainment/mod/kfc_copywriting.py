from ..command import *

kfc_copywriting_cmd = on_command("肯德基文案", aliases={"疯狂星期四", "KFC文案"}, priority=5, block=True)


@kfc_copywriting_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def kfc_copywriting_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """肯德基文案"""
    api_url = "https://api.pearktrue.cn/api/kfc"

    try:
        result = get_json_api(api_url, params={"type": "json"}, timeout=15)
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取肯德基文案失败：{e}",
            md_type="娱乐",
            k1="重试", v1="肯德基文案",
            k2="搞笑段子", v2="搞笑段子",
            k3="帮助", v3="娱乐帮助"
        )
        await kfc_copywriting_cmd.finish()

    text = result.get("text")
    msg = result.get("msg", "接口异常")
    code = result.get("code")

    if str(code) not in {"200", "0"} and not text:
        await handle_send(
            bot, event,
            f"获取肯德基文案失败：{msg}",
            md_type="娱乐",
            k1="重试", v1="肯德基文案",
            k2="搞笑段子", v2="搞笑段子",
            k3="帮助", v3="娱乐帮助"
        )
        await kfc_copywriting_cmd.finish()

    text_msg = f"🍗 肯德基疯狂星期四文案\n{text or msg}"

    await handle_send(
        bot, event,
        text_msg,
        md_type="娱乐",
        k1="再来一条", v1="肯德基文案",
        k2="搞笑段子", v2="搞笑段子",
        k3="帮助", v3="娱乐帮助"
    )
    await kfc_copywriting_cmd.finish()