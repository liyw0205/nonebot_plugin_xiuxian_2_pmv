from ..command import *

history_today_cmd = on_command("历史上的今天", priority=5, block=True)


@history_today_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def history_today_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """历史上的今天"""
    api_url = "https://api.pearktrue.cn/api/lsjt?type=json"

    try:
        result = get_json_api(api_url, timeout=15)
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取历史上的今天失败：{e}",
            md_type="娱乐",
            k1="重试", v1="历史上的今天",
            k2="脑筋急转弯", v2="脑筋急转弯",
            k3="帮助", v3="娱乐帮助"
        )
        await history_today_cmd.finish()

    code = result.get("code")
    msg = result.get("msg", "接口异常")
    time_text = result.get("time", "未知时间")
    data = result.get("data", [])

    content = "\n".join(f"{idx + 1}. {item}" for idx, item in enumerate(data) if item)

    if str(code) not in {"200", "0"} and not content:
        await handle_send(
            bot, event,
            f"获取历史上的今天失败：{msg}",
            md_type="娱乐",
            k1="重试", v1="历史上的今天",
            k2="脑筋急转弯", v2="脑筋急转弯",
            k3="帮助", v3="娱乐帮助"
        )
        await history_today_cmd.finish()

    text_msg = (
        f"📅 历史上的今天\n"
        f"时间：{time_text}\n"
        f"内容：\n{content or msg}"
    )

    await handle_send(
        bot, event,
        text_msg,
        md_type="娱乐",
        k1="再来一次", v1="历史上的今天",
        k2="脑筋急转弯", v2="脑筋急转弯",
        k3="帮助", v3="娱乐帮助"
    )
    await history_today_cmd.finish()