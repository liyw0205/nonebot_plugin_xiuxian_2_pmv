from ..command import *
today_superpower_cmd = on_command("今日超能力", priority=5, block=True)


@today_superpower_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def today_superpower_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """今日超能力"""
    api_url = "https://api.pearapi.ai/api/superpower"

    try:
        result = await get_json_api(api_url, timeout=15)
    except Exception as e:
        await handle_send(
            bot, event,
            f"获取今日超能力失败：{e}",
            md_type="娱乐",
            k1="再试一次", v1="今日超能力",
            k2="今日老婆", v2="今日老婆",
            k3="帮助", v3="娱乐帮助"
        )
        await today_superpower_cmd.finish()

    if not api_code_success(result):
        msg = extract_api_message(result)
        await handle_send(
            bot, event,
            f"获取今日超能力失败：{msg}",
            md_type="娱乐",
            k1="再试一次", v1="今日超能力",
            k2="今日老婆", v2="今日老婆",
            k3="帮助", v3="娱乐帮助"
        )
        await today_superpower_cmd.finish()

    data = result.get("data", {})
    if not isinstance(data, dict):
        data = {}
    superpower = normalize_api_text(data.get("superpower")) or "未知超能力"
    disadvantage = normalize_api_text(data.get("disadvantage")) or "暂无副作用说明"
    image_url = data.get("image_url")

    text_msg = (
        f"超能力：{superpower}\n"
        f"但是：{disadvantage}"
    )

    try:
        if image_url:
            await send_entertainment_image_result(
                bot,
                event,
                image_url,
                text_msg,
                title="今日超能力",
                buttons=[("再试一次", "今日超能力"), ("今日老婆", "今日老婆"), ("娱乐帮助", "娱乐帮助")],
            )
        else:
            await handle_send(
                bot, event,
                text_msg,
                md_type="娱乐",
                k1="再试一次", v1="今日超能力",
                k2="今日老婆", v2="今日老婆",
                k3="帮助", v3="娱乐帮助"
            )
    except Exception as e:
        logger.warning(f"今日超能力 普通图文发送失败：{e}")
        await handle_send(
            bot, event,
            f"今日超能力发送失败：{e}",
            md_type="娱乐",
            k1="再试一次", v1="今日超能力",
            k2="今日老婆", v2="今日老婆",
            k3="帮助", v3="娱乐帮助"
        )

    await today_superpower_cmd.finish()
