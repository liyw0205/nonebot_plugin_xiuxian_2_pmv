import asyncio

from nonebot.adapters import Event as BaseEvent
from nonebot.log import logger
from nonebot.matcher import Matcher
from nonebot.params import EventPlainText
from nonebot.rule import Rule

from ..adapter_compat import Bot, GroupMessageEvent, PrivateMessageEvent
from ..on_compat import on_message
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.lay_out import Cooldown
from ..xiuxian_utils.http_proxy import http_client
from ..xiuxian_utils.utils import handle_pic_msg_send, handle_send


def get_random_acg_pic_url(timeout: int = 5) -> str | None:
    """获取默认回复随机图片地址。"""
    api_url = "https://v2.xxapi.cn/api/randomAcgPic"
    params = {
        "type": "pc",
        "return": "json",
    }

    try:
        data = http_client.get_json(api_url, params=params, timeout=timeout)

        if str(data.get("code")) != "200":
            logger.warning(
                f"默认回复图片接口请求失败: code={data.get('code')} msg={data.get('msg')}"
            )
            return None

        image_url = data.get("data")
        if image_url and isinstance(image_url, str):
            return image_url.strip()

        logger.warning("默认回复图片接口未返回有效图片地址")
        return None

    except Exception as e:
        logger.warning(f"获取默认回复随机图片失败: {e}")
        return None


async def get_random_acg_pic_url_async(timeout: int = 3) -> str | None:
    return await asyncio.to_thread(get_random_acg_pic_url, timeout)


def _fallback_rule() -> Rule:
    async def _checker(event: BaseEvent, text: str = EventPlainText()) -> bool:
        if not XiuConfig().empty_fallback or not XiuConfig().empty_msg:
            return False
        if not isinstance(event, (GroupMessageEvent, PrivateMessageEvent)):
            return False
        # 全量群：表情/闲聊也会进消息事件，不要刷默认回复
        try:
            from ..xiuxian_config import JsonConfig

            group_id = str(getattr(event, "group_id", "") or "").strip()
            if group_id and JsonConfig().is_full_message_group(group_id):
                return False
            # QQ 全量消息事件：即便尚未标记，也不用默认回复刷屏
            event_name = " ".join(
                str(x)
                for x in (
                    getattr(event, "__type__", None),
                    getattr(event, "type", None),
                )
                if x is not None
            )
            try:
                event_name = f"{event_name} {event.get_event_name()}"
            except Exception:
                pass
            if "GROUP_MESSAGE_CREATE" in event_name.upper():
                return False
        except Exception:
            pass
        return True

    return Rule(_checker)


empty_fallback = on_message(priority=999, block=False, rule=_fallback_rule())


@empty_fallback.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_empty_fallback(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    matcher: Matcher,
):
    config = XiuConfig()
    text_msg = config.empty_msg
    image_url = None
    if config.empty_fallback_image:
        image_url = await get_random_acg_pic_url_async(timeout=3)

    if image_url:
        try:
            await handle_pic_msg_send(bot, event, image_url, text_msg)
            await matcher.finish()
        except Exception as e:
            logger.warning(f"默认回复图文发送失败，准备降级纯文字: {e}")

    try:
        await handle_send(bot, event, text_msg)
    except Exception as e:
        logger.warning(f"默认回复纯文字发送失败: {e}")

    await matcher.finish()
