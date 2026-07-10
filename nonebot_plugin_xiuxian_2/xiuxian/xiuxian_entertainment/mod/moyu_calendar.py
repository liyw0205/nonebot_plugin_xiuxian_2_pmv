from datetime import datetime

from ..command import *


MOYU_IMAGE_API = "https://api.52vmy.cn/api/wl/moyu"
MOYU_TEXT_API = "https://60s.viki.moe/v2/moyu"

moyu_calendar_cmd = on_command(
    "摸鱼日报",
    aliases={"摸鱼日历", "今日摸鱼", "摸鱼人日历"},
    priority=5,
    block=True,
)


def _moyu_image_url() -> str:
    cache_key = datetime.now().strftime("%Y%m%d")
    return f"{MOYU_IMAGE_API}?t={cache_key}"


async def _moyu_text_fallback() -> str:
    result = await get_json_api(MOYU_TEXT_API, timeout=15)
    data = result.get("data", {})
    if not isinstance(data, dict):
        data = {}

    date_info = data.get("date", {})
    progress = data.get("progress", {})
    countdown = data.get("countdown", {})
    next_holiday = data.get("nextHoliday") or {}
    next_weekend = data.get("nextWeekend") or {}

    if not isinstance(date_info, dict):
        date_info = {}
    if not isinstance(progress, dict):
        progress = {}
    if not isinstance(countdown, dict):
        countdown = {}
    if not isinstance(next_holiday, dict):
        next_holiday = {}
    if not isinstance(next_weekend, dict):
        next_weekend = {}

    year_progress = progress.get("year", {}) if isinstance(progress.get("year"), dict) else {}
    month_progress = progress.get("month", {}) if isinstance(progress.get("month"), dict) else {}
    week_progress = progress.get("week", {}) if isinstance(progress.get("week"), dict) else {}

    lines = [
        "【摸鱼日报】",
        f"日期：{date_info.get('gregorian', '今日')} {date_info.get('weekday', '')}".rstrip(),
        f"本周进度：{week_progress.get('percentage', '?')}%",
        f"本月进度：{month_progress.get('percentage', '?')}%",
        f"今年进度：{year_progress.get('percentage', '?')}%",
    ]
    if next_weekend:
        lines.append(f"距离周末：{next_weekend.get('daysUntil', '?')} 天")
    if next_holiday:
        lines.append(f"下个节日：{next_holiday.get('name', '未知')}，还有 {next_holiday.get('until', '?')} 天")
    if countdown:
        lines.append(f"距离周五：{countdown.get('toFriday', '?')} 天")
    quote = str(data.get("moyuQuote") or "").strip()
    if quote:
        lines.append(f"摸鱼语录：{quote}")
    return "\n".join(lines)


@moyu_calendar_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def moyu_calendar_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    image_url = _moyu_image_url()
    try:
        await send_entertainment_media(
            bot, event, MessageSegment.image(bot, image_url), media_type="图片"
        )
    except Exception as e:
        logger.warning(f"摸鱼日报图片发送失败：{e}")
        try:
            text_msg = await _moyu_text_fallback()
        except Exception as fallback_error:
            await handle_send(
                bot,
                event,
                f"获取摸鱼日报失败：图片源 {e}；文本源 {fallback_error}",
                md_type="娱乐",
                k1="重试",
                v1="摸鱼日报",
                k2="60S读世界",
                v2="60S读世界",
                k3="帮助",
                v3="娱乐帮助",
            )
            await moyu_calendar_cmd.finish()

        await handle_send(
            bot,
            event,
            text_msg,
            md_type="娱乐",
            k1="重试图片",
            v1="摸鱼日报",
            k2="60S读世界",
            v2="60S读世界",
            k3="帮助",
            v3="娱乐帮助",
        )
        await moyu_calendar_cmd.finish()

    await moyu_calendar_cmd.finish()
