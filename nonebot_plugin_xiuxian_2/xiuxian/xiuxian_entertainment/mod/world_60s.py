from ..command import *

world_60s_cmd = on_command(
    "60S读世界",
    aliases={"每日60S", "60秒读世界"},
    priority=5,
    block=True
)


@world_60s_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def world_60s_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """60S读世界"""
    api_url = "https://api.pearktrue.cn/api/60s/"

    try:
        result = get_json_api(api_url, timeout=15)
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取60S读世界失败：{e}",
            md_type="娱乐",
            k1="重试", v1="60S读世界",
            k2="每日60S图片", v2="每日60S图片",
            k3="帮助", v3="娱乐帮助"
        )
        await world_60s_cmd.finish()

    code = result.get("code")
    msg = result.get("msg", "接口异常")
    data = result.get("data", [])
    api_source = result.get("api_source", "")

    if str(code) not in {"200", "0"} and not data:
        await handle_send(
            bot, event,
            f"获取60S读世界失败：{msg}",
            md_type="娱乐",
            k1="重试", v1="60S读世界",
            k2="每日60S图片", v2="每日60S图片",
            k3="帮助", v3="娱乐帮助"
        )
        await world_60s_cmd.finish()

    if isinstance(data, list):
        content = "\n".join([f"{idx + 1}. {item}" for idx, item in enumerate(data)])
    elif isinstance(data, str):
        content = data
    else:
        content = str(data)

    text_msg = f"📰 60S读世界\n{content}"
    if api_source:
        text_msg += f"\n\n来源：{api_source}"

    await handle_send(
        bot, event,
        text_msg,
        md_type="娱乐",
        k1="再看一次", v1="60S读世界",
        k2="60S图片", v2="每日60S图片",
        k3="帮助", v3="娱乐帮助"
    )
    await world_60s_cmd.finish()