import asyncio
import time
from collections.abc import Awaitable, Callable
from ....infrastructure.random_source import SystemRandom
from ..command import *

runtime_random = SystemRandom()

random_girl_video_cmd = on_command(
    "随机小姐姐",
    aliases={"小姐姐", "随机美女视频"},
    priority=5,
    block=True,
)

YUJN_API = "https://api.yujn.cn/api/xjj.php?type=json"

# openapi.dwo.cc：GET 返回 video/mp4，MessageSegment 使用接口 URL 即可
DWO_VIDEO_APIS: dict[str, str] = {
    "dwo_xjj": "https://openapi.dwo.cc/api/xjj",
    "dwo_fh_mvsp": "https://openapi.dwo.cc/api/fh_mvsp",
    "dwo_52vmy": "https://openapi.dwo.cc/api/52vmy",
    "dwo_fh_bssp": "https://openapi.dwo.cc/api/fh_bssp",
}


async def _fetch_video_from_yujn(timeout: float = 8) -> str:
    result = await get_json_api(YUJN_API, timeout=timeout)
    if not api_code_success(result):
        msg = extract_api_message(result)
        raise ValueError(msg)
    video_url = normalize_api_text(result.get("data"))
    if not video_url:
        raise ValueError("接口未返回视频地址")
    return video_url


async def _fetch_video_from_dwo_direct(api_url: str, timeout: float = 8) -> str:
    """GET 直链：响应为 video/mp4，最终 URL 一般为 api_url 本身"""
    video_url = await get_media_url_api(api_url, timeout=timeout)
    video_url = str(video_url).strip()
    if not video_url:
        raise ValueError("接口未返回视频地址")
    return video_url


def _make_dwo_fetcher(
    name: str, api_url: str
) -> tuple[str, Callable[[float], Awaitable[str]]]:
    async def _fetch(timeout: float) -> str:
        return await _fetch_video_from_dwo_direct(api_url, timeout=timeout)

    return name, _fetch


async def _fetch_random_girl_video() -> tuple[str, str]:
    """
    多源负载均衡：随机打乱后依次尝试，任一成功即返回。
    返回 (video_url, source_name)
    """
    providers: list[tuple[str, Callable[[float], Awaitable[str]]]] = [
        ("yujn", lambda timeout: _fetch_video_from_yujn(timeout=timeout)),
    ]
    for name, url in DWO_VIDEO_APIS.items():
        providers.append(_make_dwo_fetcher(name, url))

    runtime_random.shuffle(providers)

    errors: list[str] = []
    deadline = time.monotonic() + 30.0
    for name, fetcher in providers:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            errors.append("总查询时间超过 30 秒")
            break
        try:
            video_url = await asyncio.wait_for(
                fetcher(min(8.0, remaining)), timeout=remaining
            )
            return video_url, name
        except Exception as e:
            errors.append(f"{name}: {e}")
            logger.warning(f"随机小姐姐 {name} 源失败：{e}")

    raise ValueError("；".join(errors) if errors else "全部视频源不可用")


async def _send_random_girl_video(bot: Bot, event, video_url: str):
    config = XiuConfig()
    text_msg = "随机小姐姐"

    if config.markdown_status:
        try:
            await handle_send(
                bot,
                event,
                "**随机小姐姐**",
                native_markdown=True,
                fallback_msg=text_msg,
                keyboard_rows=[
                    [("再来一个", "随机小姐姐"), ("娱乐帮助", "娱乐帮助")]
                ],
                at_msg=False,
            )
            await send_entertainment_media(
                bot, event, MessageSegment.video(bot, video_url), media_type="视频"
            )
            return
        except Exception as e:
            logger.warning(f"随机小姐姐 Markdown发送失败：{e}")

    await handle_send(
        bot,
        event,
        text_msg,
        md_type="娱乐",
        k1="再来一个",
        v1="随机小姐姐",
        k2="随机点歌",
        v2="随机点歌",
        k3="娱乐帮助",
        v3="娱乐帮助",
    )
    await send_entertainment_media(
        bot, event, MessageSegment.video(bot, video_url), media_type="视频"
    )


@random_girl_video_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def random_girl_video_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """随机小姐姐视频"""
    try:
        video_url, _source = await _fetch_random_girl_video()
        await _send_random_girl_video(bot, event, video_url)
    except Exception as e:
        await handle_send(
            bot,
            event,
            f"获取随机小姐姐失败：{e}",
            md_type="娱乐",
            k1="再试一次",
            v1="随机小姐姐",
            k2="今日老婆",
            v2="今日老婆",
            k3="娱乐帮助",
            v3="娱乐帮助",
        )

    await random_girl_video_cmd.finish()
