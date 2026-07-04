from ..command import *

random_duanzi_cmd = on_command("搞笑段子", aliases={"随机段子"}, priority=5, block=True)


@random_duanzi_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def random_duanzi_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """搞笑段子"""
    api_url = "https://api.pearapi.ai/api/random/duanzi/"

    try:
        result = await get_json_api(api_url, params={"type": "json"}, timeout=15)
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取搞笑段子失败：{e}",
            md_type="娱乐",
            k1="重试", v1="搞笑段子",
            k2="肯德基文案", v2="肯德基文案",
            k3="帮助", v3="娱乐帮助"
        )
        await random_duanzi_cmd.finish()

    duanzi = extract_api_text(result, "duanzi", "text", "content")
    msg = extract_api_message(result)

    if not api_code_success(result) and not duanzi:
        await handle_send(
            bot, event,
            f"获取搞笑段子失败：{msg}",
            md_type="娱乐",
            k1="重试", v1="搞笑段子",
            k2="肯德基文案", v2="肯德基文案",
            k3="帮助", v3="娱乐帮助"
        )
        await random_duanzi_cmd.finish()

    if not duanzi:
        await handle_send(
            bot, event,
            "获取搞笑段子失败：接口未返回段子内容",
            md_type="娱乐",
            k1="重试", v1="搞笑段子",
            k2="肯德基文案", v2="肯德基文案",
            k3="帮助", v3="娱乐帮助"
        )
        await random_duanzi_cmd.finish()

    text_msg = duanzi

    await handle_send(
        bot, event,
        text_msg,
        md_type="娱乐",
        k1="再来一条", v1="搞笑段子",
        k2="肯德基文案", v2="肯德基文案",
        k3="帮助", v3="娱乐帮助"
    )
    await random_duanzi_cmd.finish()
