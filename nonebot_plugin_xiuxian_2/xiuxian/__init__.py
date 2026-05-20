#!usr/bin/env python3
# -*- coding: utf-8 -*-
from .xiuxian_utils.download_xiuxian_data import download_xiuxian_data
from nonebot.plugin import PluginMetadata
from nonebot.log import logger
from nonebot.message import event_preprocessor, IgnoredException
from .adapter_compat import (
    Bot,
    GroupMessageEvent,
    PrivateMessageEvent,
    get_group_id,
    patch_context
)
from nonebot import get_driver
from .xiuxian_config import XiuConfig
from pathlib import Path
from pkgutil import iter_modules
from nonebot.log import logger
from nonebot import require, load_all_plugins, get_plugin_by_module_name
from .xiuxian_utils.config import config as _config
from .broadcast_manager import auto_patch_broadcast_for_event

DRIVER = get_driver()

try:
    NICKNAME: str = list(DRIVER.config.nickname)[0]
except Exception as e:
    logger.opt(colors=True).info(f"<red>缺少超级用户配置文件，{e}!</red>")
    logger.opt(colors=True).info(f"<red>请去.env.dev文件中设置超级用户QQ号以及nickname!</red>")
    NICKNAME = 'bot'

try:
    download_xiuxian_data()
except Exception as e:
    logger.opt(colors=True).info(f"<red>下载配置文件失败，修仙插件无法加载，{e}!</red>")
    raise ImportError

put_bot = XiuConfig().put_bot
shield_group = XiuConfig().shield_group
response_group = XiuConfig().response_group
shield_private = XiuConfig().shield_private
try:
    put_bot_ = put_bot[0]
except:
    logger.opt(colors=True).info(f"<green>修仙插件没有配置put_bot,如果有多个qq和nb链接,请务必配置put_bot,具体介绍参考【风控帮助】！</green>")

require('nonebot_plugin_apscheduler')

if get_plugin_by_module_name("xiuxian"):
    logger.opt(colors=True).info(f"<green>推荐直接加载 xiuxian 仓库文件夹</green>")
    load_all_plugins(
        [
            f"xiuxian.{module.name}"
            for module in iter_modules([str(Path(__file__).parent)])
            if module.ispkg
            and (
                (name := module.name[11:]) == "meta"
                or name not in _config.disabled_plugins
            )
        ],
        [],
    )

__plugin_meta__ = PluginMetadata(
    name='修仙模拟器',
    description='',
    usage=(
        "必死之境机逢仙缘，修仙之路波澜壮阔！\n"
        " 输入 < 修仙帮助 > 获取仙界信息"
    ),
    extra={
        "show": True,
        "priority": 15
    }
)


def _safe_str(value):
    return "" if value is None else str(value)


def _get_event_name(event):
    names = []
    for attr in ("__type__", "type"):
        value = getattr(event, attr, None)
        if value is not None:
            names.append(_safe_str(value))
    try:
        names.append(_safe_str(event.get_event_name()))
    except Exception:
        pass
    return " ".join(names)


def _is_qq_group_message_create(event):
    return "GROUP_MESSAGE_CREATE" in _get_event_name(event)


def _get_bot_self_ids(bot):
    ids = set()

    for value in (
        getattr(bot, "self_id", None),
        getattr(getattr(bot, "bot_info", None), "id", None),
    ):
        if value:
            ids.add(_safe_str(value))

    try:
        config = XiuConfig()
    except Exception:
        config = None

    for attr in ("bot_uin", "bot_uid"):
        value = getattr(config, attr, None)
        if value:
            ids.add(_safe_str(value))

    try:
        self_info = getattr(bot, "self_info", None)
    except Exception:
        self_info = None

    for attr in ("id", "user_id", "user_openid", "member_openid", "union_openid", "union_user_account"):
        value = getattr(self_info, attr, None)
        if value:
            ids.add(_safe_str(value))

    return ids


def _get_event_to_me_mention_ids(event):
    ids = set()
    for mention in getattr(event, "mentions", None) or []:
        if isinstance(mention, dict):
            is_you = mention.get("is_you", False)
            values = [mention.get(key) for key in ("id", "user_id", "member_openid", "openid")]
        else:
            is_you = getattr(mention, "is_you", False)
            values = [getattr(mention, key, None) for key in ("id", "user_id", "member_openid", "openid")]
        if not is_you:
            continue
        ids.update(_safe_str(value) for value in values if value)
    return ids


def _is_self_mention_segment(segment, bot, event, *, allow_qq_group_fallback=False):
    seg_type = getattr(segment, "type", "")
    data = getattr(segment, "data", {}) or {}

    if seg_type == "group_mention_user":
        return bool(data.get("is_you"))

    if seg_type != "mention_user":
        return False

    user_id = _safe_str(data.get("user_id"))
    if not user_id:
        return False

    if user_id in _get_bot_self_ids(bot):
        return True

    to_me_mention_ids = _get_event_to_me_mention_ids(event)
    if user_id in to_me_mention_ids:
        return True

    if getattr(event, "to_me", False) and _is_qq_group_message_create(event):
        return True

    return False


def _is_other_bot_mention(segment):
    if getattr(segment, "type", "") != "group_mention_user":
        return False

    data = getattr(segment, "data", {}) or {}
    return bool(data.get("bot")) and not bool(data.get("is_you"))


def _has_self_mention(bot, event, message):
    for segment in message:
        if _is_self_mention_segment(segment, bot, event):
            return True
    return False


def _is_other_bot_at_message(bot, event):
    if not _is_qq_group_message_create(event):
        return False

    try:
        message = event.get_message()
    except Exception:
        return False

    if _has_self_mention(bot, event, message):
        return False

    if any(_is_other_bot_mention(segment) for segment in message):
        return True

    for mention in getattr(event, "mentions", None) or []:
        if isinstance(mention, dict):
            is_bot = mention.get("bot", False)
            is_you = mention.get("is_you", False)
        else:
            is_bot = getattr(mention, "bot", False)
            is_you = getattr(mention, "is_you", False)
        if is_bot and not is_you:
            return True

    return False


def _strip_left_blank_text(message):
    while message and getattr(message[0], "type", "") == "text":
        text = _safe_str((getattr(message[0], "data", {}) or {}).get("text"))
        text = text.lstrip("\xa0").lstrip()
        if text:
            message[0].data["text"] = text
            return
        del message[0]


def _strip_right_blank_text(message):
    while message and getattr(message[-1], "type", "") == "text":
        text = _safe_str((getattr(message[-1], "data", {}) or {}).get("text"))
        text = text.rstrip("\xa0").rstrip()
        if text:
            message[-1].data["text"] = text
            return
        del message[-1]


def _refresh_event_text_cache(event, message):
    try:
        plain_text = message.extract_plain_text()
    except Exception:
        plain_text = ""

    for attr in ("raw_message", "plaintext"):
        if hasattr(event, attr):
            try:
                setattr(event, attr, plain_text)
            except Exception:
                pass

    if hasattr(event, "message"):
        try:
            setattr(event, "message", message)
        except Exception:
            pass


def _normalize_qq_group_at_message(bot, event):
    if not _is_qq_group_message_create(event):
        return False

    try:
        message = event.get_message()
    except Exception:
        return False

    removed = False

    if message and _is_self_mention_segment(
        message[0],
        bot,
        event,
        allow_qq_group_fallback=True,
    ):
        del message[0]
        _strip_left_blank_text(message)
        removed = True

    if not removed:
        _strip_right_blank_text(message)
        if message and _is_self_mention_segment(message[-1], bot, event):
            del message[-1]
            _strip_right_blank_text(message)
            removed = True

    if removed:
        try:
            event.to_me = True
        except Exception:
            pass
        _refresh_event_text_cache(event, message)
    return removed


@event_preprocessor
async def do_something(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    if _is_other_bot_at_message(bot, event):
        raise IgnoredException("消息艾特了其他机器人,已忽略")

    qq_group_at_removed = _normalize_qq_group_at_message(bot, event)
    bot, event = patch_context(bot, event)

    if _is_other_bot_at_message(bot, event):
        raise IgnoredException("消息艾特了其他机器人,已忽略")

    qq_group_at_removed = _normalize_qq_group_at_message(bot, event) or qq_group_at_removed
    if qq_group_at_removed:
        try:
            event.to_me = True
        except Exception:
            pass

    global put_bot

    if not put_bot:
        pass
    else:
        if str(bot.self_id) in put_bot:
            # 私聊处理
            if isinstance(event, PrivateMessageEvent):
                if shield_private:
                    raise IgnoredException("私聊功能已屏蔽,已忽略")

                # 私聊没被屏蔽，允许补发
                try:
                    await auto_patch_broadcast_for_event(bot, event)
                except Exception as e:
                    logger.warning(f"[广播] 私聊补发失败: {e}")

                return

            # 群聊处理
            if response_group:
                if str(get_group_id(event)) in shield_group:
                    pass
                else:
                    raise IgnoredException("不为响应群消息,已忽略")
            else:
                if str(get_group_id(event)) in shield_group:
                    raise IgnoredException("为屏蔽群消息,已忽略")
                else:
                    pass

            # 群聊没被屏蔽，允许补发
            try:
                await auto_patch_broadcast_for_event(bot, event)
            except Exception as e:
                logger.warning(f"[广播] 群聊补发失败: {e}")
