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
    api_url = "https://api.pearapi.ai/api/60s/"

    try:
        result = await get_json_api(api_url, timeout=15)
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

    msg = extract_api_message(result)
    data = result.get("data", [])
    api_source = normalize_api_text(result.get("api_source"))

    if not api_code_success(result) and not data:
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
        items = [normalize_api_text(item) for item in data]
        content = "\n".join(f"{idx + 1}. {item}" for idx, item in enumerate(items) if item)
    elif isinstance(data, str):
        content = normalize_api_text(data)
    else:
        content = normalize_api_text(data)

    if not content:
        await handle_send(
            bot, event,
            "获取60S读世界失败：接口未返回新闻内容",
            md_type="娱乐",
            k1="重试", v1="60S读世界",
            k2="每日60S图片", v2="每日60S图片",
            k3="帮助", v3="娱乐帮助"
        )
        await world_60s_cmd.finish()

    text_msg = content
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
