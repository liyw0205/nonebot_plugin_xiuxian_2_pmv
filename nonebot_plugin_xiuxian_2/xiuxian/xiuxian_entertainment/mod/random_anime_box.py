from ..command import *


JIKAN_RANDOM_ANIME_API = "https://api.jikan.moe/v4/random/anime"
BLOCKED_RATINGS = ("Rx", "Hentai")

anime_box_cmd = on_command(
    "番剧盲盒",
    aliases={"随机番剧", "动漫盲盒", "随机动漫"},
    priority=5,
    block=True,
)
anime_box_help_cmd = on_command("番剧盲盒帮助", aliases={"随机番剧帮助"}, priority=5, block=True)


def _trim_text(text: str, limit: int = 220) -> str:
    value = " ".join(str(text or "").split())
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "…"


def _anime_image(data: dict) -> str:
    images = data.get("images", {}) if isinstance(data, dict) else {}
    jpg = images.get("jpg", {}) if isinstance(images, dict) else {}
    webp = images.get("webp", {}) if isinstance(images, dict) else {}
    return (
        jpg.get("large_image_url")
        or jpg.get("image_url")
        or webp.get("large_image_url")
        or webp.get("image_url")
        or ""
    )


def _anime_title(data: dict) -> str:
    title = data.get("title") or data.get("title_english") or data.get("title_japanese") or "未知番剧"
    jp_title = data.get("title_japanese")
    if jp_title and jp_title != title:
        return f"{title}\n日文：{jp_title}"
    return str(title)


def _anime_genres(data: dict) -> str:
    names = []
    for item in (data.get("genres") or []) + (data.get("themes") or []):
        if isinstance(item, dict) and item.get("name"):
            names.append(str(item["name"]))
    return " / ".join(names[:5]) or "未知"


def _anime_text(data: dict) -> str:
    lines = [
        "【番剧盲盒】",
        f"标题：{_anime_title(data)}",
        f"类型：{data.get('type') or '未知'}",
        f"集数：{data.get('episodes') or '未知'}",
        f"状态：{data.get('status') or '未知'}",
        f"评分：{data.get('score') or '暂无'}",
        f"年份：{data.get('year') or '未知'}",
        f"题材：{_anime_genres(data)}",
    ]
    rating = data.get("rating")
    if rating:
        lines.append(f"分级：{rating}")
    synopsis = _trim_text(data.get("synopsis") or "")
    if synopsis:
        lines.append(f"简介：{synopsis}")
    url = data.get("url")
    if url:
        lines.append(f"链接：{url}")
    return "\n".join(lines)


async def _fetch_random_anime() -> dict:
    last_data = {}
    for _ in range(3):
        result = await get_json_api(JIKAN_RANDOM_ANIME_API, timeout=15)
        data = result.get("data", {})
        if not isinstance(data, dict):
            continue
        last_data = data
        rating = str(data.get("rating") or "")
        if not any(mark in rating for mark in BLOCKED_RATINGS):
            return data
    return last_data


@anime_box_cmd.handle(parameterless=[Cooldown(cd_time=8)])
async def anime_box_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    try:
        data = await _fetch_random_anime()
        if not data:
            raise ValueError("接口未返回番剧数据")
        image_url = _anime_image(data)
        text = _anime_text(data)
        if image_url:
            await handle_pic_msg_send(bot, event, image_url, text)
        else:
            await handle_send(bot, event, text, md_type="娱乐", k1="再抽", v1="番剧盲盒", k2="帮助", v2="番剧盲盒帮助")
    except Exception as e:
        await handle_send(
            bot,
            event,
            f"打开番剧盲盒失败：{e}",
            md_type="娱乐",
            k1="再抽一次",
            v1="番剧盲盒",
            k2="今日番剧",
            v2="今日番剧",
            k3="帮助",
            v3="番剧盲盒帮助",
        )
    await anime_box_cmd.finish()


@anime_box_help_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def anime_box_help_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await send_help_message(
        bot,
        event,
        "**番剧盲盒**\n\n"
        "**用法**\n"
        "- 番剧盲盒\n"
        "- 随机番剧\n\n"
        "> 随机抽取一部番剧，展示封面、标题、评分、题材和简介。",
        k1="抽一部",
        v1="番剧盲盒",
        k2="今日番剧",
        v2="今日番剧",
        k3="番剧周表",
        v3="番剧周表",
        k4="娱乐帮助",
        v4="娱乐帮助",
    )
    await anime_box_help_cmd.finish()
