import json
import requests
import random
from urllib.parse import quote
from nonebot.log import logger
from nonebot import on_command
from nonebot.permission import SUPERUSER

from ..adapter_compat import (
    Bot,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
    is_channel_event,
    Message,
)

from ..xiuxian_utils.utils import (
    handle_send,
    handle_send_md,
    handle_pic_msg_send,
    generate_command,
)

from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.lay_out import Cooldown


def get_json_api(api_url: str, params: dict | None = None, timeout: int = 15) -> dict:
    """
    通用 JSON 接口请求
    - 优先 resp.json()
    - 失败时兼容 text -> json.loads
    - 失败抛异常给上层处理
    """
    resp = requests.get(api_url, params=params, timeout=timeout)
    resp.raise_for_status()
    try:
        result = resp.json()
    except Exception:
        result = json.loads(resp.text)

    if not isinstance(result, dict):
        raise ValueError("接口返回不是JSON对象")
    return result


def get_text_api(api_url: str, params: dict | None = None, timeout: int = 15) -> str:
    """
    通用文本接口请求
    """
    resp = requests.get(api_url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.text.strip()


def get_media_url_api(api_url: str, params: dict | None = None, timeout: int = 20) -> str:
    """
    通用媒体接口请求
    - 如果返回 JSON，则尝试从常见字段里找 URL
    - 如果不是 JSON，则使用 resp.url
    """
    resp = requests.get(api_url, params=params, timeout=timeout, allow_redirects=True)
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "")
    if "application/json" in content_type:
        try:
            result = resp.json()
        except Exception:
            result = json.loads(resp.text)

        if isinstance(result, dict):
            media_url = (
                result.get("url")
                or result.get("image")
                or result.get("image_url")
                or result.get("data")
            )
            if media_url:
                return str(media_url)

        raise ValueError("接口未返回媒体地址")

    return str(resp.url)


async def handle_audio_send(bot: Bot, event, audio_url: str):
    """
    发送音频消息，失败时抛出异常给上层处理
    """
    if not audio_url:
        return
    seg = MessageSegment.audio(bot, audio_url)
    await bot.send(event=event, message=seg)