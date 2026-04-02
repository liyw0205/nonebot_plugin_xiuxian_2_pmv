from ..command import *

ruozhiba_qa_cmd = on_command("弱智吧问答", aliases={"弱智吧"}, priority=5, block=True)


@ruozhiba_qa_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def ruozhiba_qa_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """弱智吧问答"""
    api_url = "https://api.pearktrue.cn/api/ruozhiba/"

    try:
        result = get_json_api(api_url, timeout=15)
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取弱智吧问答失败：{e}",
            md_type="娱乐",
            k1="重试", v1="弱智吧问答",
            k2="脑筋急转弯", v2="脑筋急转弯",
            k3="帮助", v3="娱乐帮助"
        )
        await ruozhiba_qa_cmd.finish()

    data = result.get("data", {})
    if not isinstance(data, dict):
        data = {}

    instruction = data.get("instruction") or result.get("instruction")
    output = data.get("output") or result.get("output")
    msg = result.get("msg", "接口异常")
    code = result.get("code")

    if str(code) not in {"200", "0"} and not instruction:
        await handle_send(
            bot, event,
            f"获取弱智吧问答失败：{msg}",
            md_type="娱乐",
            k1="重试", v1="弱智吧问答",
            k2="脑筋急转弯", v2="脑筋急转弯",
            k3="帮助", v3="娱乐帮助"
        )
        await ruozhiba_qa_cmd.finish()

    text_msg = (
        f"🤪 弱智吧问答\n"
        f"问题：{instruction or '暂无问题'}\n"
        f"回答：{output or '暂无回答'}"
    )

    await handle_send(
        bot, event,
        text_msg,
        md_type="娱乐",
        k1="再来一条", v1="弱智吧问答",
        k2="脑筋急转弯", v2="脑筋急转弯",
        k3="帮助", v3="娱乐帮助"
    )
    await ruozhiba_qa_cmd.finish()