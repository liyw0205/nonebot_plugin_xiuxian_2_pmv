import random
from collections.abc import Awaitable, Callable
from ..command import *

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


async def _fetch_video_from_yujn() -> str:
    result = await get_json_api(YUJN_API, timeout=20)
    if not isinstance(result, dict) or result.get("code") != 200:
        msg = result.get("tips", "接口异常") if isinstance(result, dict) else "接口异常"
        raise ValueError(msg)
    video_url = str(result.get("data", "")).strip()
    if not video_url:
        raise ValueError("接口未返回视频地址")
    return video_url


async def _fetch_video_from_dwo_direct(api_url: str) -> str:
    """GET 直链：响应为 video/mp4，最终 URL 一般为 api_url 本身"""
    video_url = await get_media_url_api(api_url, timeout=30)
    video_url = str(video_url).strip()
    if not video_url:
        raise ValueError("接口未返回视频地址")
    return video_url


def _make_dwo_fetcher(name: str, api_url: str) -> tuple[str, Callable[[], Awaitable[str]]]:
    async def _fetch() -> str:
        return await _fetch_video_from_dwo_direct(api_url)

    return name, _fetch


async def _fetch_random_girl_video() -> tuple[str, str]:
    """
    多源负载均衡：随机打乱后依次尝试，任一成功即返回。
    返回 (video_url, source_name)
    """
    providers: list[tuple[str, Callable[[], Awaitable[str]]]] = [
        ("yujn", _fetch_video_from_yujn),
    ]
    for name, url in DWO_VIDEO_APIS.items():
        providers.append(_make_dwo_fetcher(name, url))

    random.shuffle(providers)

    errors: list[str] = []
    for name, fetcher in providers:
        try:
            return await fetcher(), name
        except Exception as e:
            errors.append(f"{name}: {e}")
            logger.warning(f"随机小姐姐 {name} 源失败：{e}")

    raise ValueError("；".join(errors) if errors else "全部视频源不可用")


async def _send_random_girl_video(bot: Bot, event, video_url: str):
    config = XiuConfig()
    text_msg = ""

    if config.markdown_status:
        if config.markdown_id:
            try:
                msg_param = {
                    "key": "t1",
                    "values": [
                        "](mqqapi://aio/inlinecmd?command=随机小姐姐&enter=false&reply=false)\r",
                        "[",
                        "再来一个](mqqapi://aio/inlinecmd?command=随机小姐姐&enter=false&reply=false)\r",
                    ],
                }
                await handle_send_md(
                    bot,
                    event,
                    " ",
                    markdown_id=config.markdown_id,
                    msg_param=msg_param,
                    at_msg=None,
                )
                await bot.send(event=event, message=MessageSegment.video(bot, video_url))
            except Exception as e:
                logger.warning(f"随机小姐姐 模板MD发送失败：{e}")
            return

        if not is_channel_event(event):
            try:
                md_msg = (
                    "[再来一个](mqqapi://aio/inlinecmd?command=随机小姐姐&enter=false&reply=false)"
                )
                await bot.send(event=event, message=MessageSegment.markdown(bot, md_msg))
                await bot.send(event=event, message=MessageSegment.video(bot, video_url))
            except Exception as e:
                logger.warning(f"随机小姐姐 原生MD发送失败：{e}")
            return

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
    await bot.send(event=event, message=MessageSegment.video(bot, video_url))


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