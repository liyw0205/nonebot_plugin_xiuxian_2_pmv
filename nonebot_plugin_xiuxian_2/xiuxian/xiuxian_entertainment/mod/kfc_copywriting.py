from ..command import *

kfc_copywriting_cmd = on_command("肯德基文案", aliases={"疯狂星期四", "KFC文案"}, priority=5, block=True)


@kfc_copywriting_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def kfc_copywriting_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """肯德基文案"""
    api_url = "https://api.pearapi.ai/api/kfc"

    try:
        result = await get_json_api(api_url, params={"type": "json"}, timeout=15)
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

    text = extract_api_text(result, "text", "kfc")
    msg = extract_api_message(result)

    if not api_code_success(result) and not text:
        await handle_send(
            bot, event,
            f"获取肯德基文案失败：{msg}",
            md_type="娱乐",
            k1="重试", v1="肯德基文案",
            k2="搞笑段子", v2="搞笑段子",
            k3="帮助", v3="娱乐帮助"
        )
        await kfc_copywriting_cmd.finish()

    if not text:
        await handle_send(
            bot, event,
            f"获取肯德基文案失败：接口未返回文案",
            md_type="娱乐",
            k1="重试", v1="肯德基文案",
            k2="搞笑段子", v2="搞笑段子",
            k3="帮助", v3="娱乐帮助"
        )
        await kfc_copywriting_cmd.finish()

    await handle_send(
        bot, event,
        text,
        md_type="娱乐",
        k1="再来一条", v1="肯德基文案",
        k2="搞笑段子", v2="搞笑段子",
        k3="帮助", v3="娱乐帮助"
    )
    await kfc_copywriting_cmd.finish()
