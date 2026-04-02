from ..command import *

brainteasers_cmd = on_command("脑筋急转弯", priority=5, block=True)


@brainteasers_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def brainteasers_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """脑筋急转弯"""
    api_url = "https://api.pearktrue.cn/api/brainteasers/"

    try:
        result = get_json_api(api_url, timeout=15)
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取脑筋急转弯失败：{e}",
            md_type="娱乐",
            k1="重试", v1="脑筋急转弯",
            k2="弱智吧问答", v2="弱智吧问答",
            k3="帮助", v3="娱乐帮助"
        )
        await brainteasers_cmd.finish()

    data = result.get("data", {})
    if not isinstance(data, dict):
        data = {}

    question = data.get("question") or result.get("question")
    answer = data.get("answer") or result.get("answer")
    msg = result.get("msg", "接口异常")
    code = result.get("code")

    if str(code) not in {"200", "0"} and not question:
        await handle_send(
            bot, event,
            f"获取脑筋急转弯失败：{msg}",
            md_type="娱乐",
            k1="重试", v1="脑筋急转弯",
            k2="弱智吧问答", v2="弱智吧问答",
            k3="帮助", v3="娱乐帮助"
        )
        await brainteasers_cmd.finish()

    text_msg = (
        f"🧠 脑筋急转弯\n"
        f"题目：{question or '暂无题目'}\n"
        f"答案：{answer or '暂无答案'}"
    )

    await handle_send(
        bot, event,
        text_msg,
        md_type="娱乐",
        k1="再来一题", v1="脑筋急转弯",
        k2="弱智吧问答", v2="弱智吧问答",
        k3="帮助", v3="娱乐帮助"
    )
    await brainteasers_cmd.finish()