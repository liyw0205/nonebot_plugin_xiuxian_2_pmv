#!usr/bin/env python3
# -*- coding: utf-8 -*-
import time
from collections import deque

from .xiuxian_utils.download_xiuxian_data import download_xiuxian_data
from .xiuxian_utils.ensure_dependencies import ensure_plugin_dependencies
from nonebot.plugin import PluginMetadata
from nonebot.log import logger
from nonebot.message import event_preprocessor, IgnoredException
from .adapter_compat import (
    Bot,
    GroupMessageEvent,
    PrivateMessageEvent,
    get_group_id,
    patch_bot_inplace,
    patch_context
)
from nonebot import get_driver
from .xiuxian_config import XiuConfig
from pathlib import Path
from pkgutil import iter_modules
from nonebot.log import logger
from nonebot import require, load_all_plugins, get_plugin_by_module_name
from .xiuxian_utils.config import config as _config
from .xiuxian_utils import db_backend
from .broadcast_manager import auto_patch_broadcast_for_event

DRIVER = get_driver()

try:
    NICKNAME: str = list(DRIVER.config.nickname)[0]
except Exception as e:
    logger.opt(colors=True).info(f"<red>缺少超级用户配置文件，{e}!</red>")
    logger.opt(colors=True).info(f"<red>请去.env.dev文件中设置超级用户QQ号以及nickname!</red>")
    NICKNAME = 'bot'

try:
    ensure_plugin_dependencies()
except Exception as e:
    logger.opt(colors=True).warning(f"<yellow>修仙插件依赖自检异常（将继续尝试加载）：{e}</yellow>")

try:
    download_xiuxian_data()
except Exception as e:
    logger.opt(colors=True).info(f"<red>下载配置文件失败，修仙插件无法加载，{e}!</red>")
    raise ImportError

try:
    db_backend.initialize_backend()
except Exception as e:
    logger.opt(colors=True).error(f"<red>数据库后端初始化失败，修仙插件无法加载，{e}!</red>")
    raise


def _run_startup_database_maintenance():
    """启动时整理本地 SQLite 结构。"""
    if not db_backend.is_backend_initialized():
        logger.warning("SQLite 后端未初始化，跳过启动数据库维护。")
        return

    try:
        from .xiuxian_utils.pet_system import migrate_pet_storage_once

        migrated = migrate_pet_storage_once()
        if migrated:
            logger.info(f"宠物数据库自动整理完成：{migrated} 条")
    except Exception as e:
        logger.warning(f"宠物数据库自动整理失败：{e}")


_run_startup_database_maintenance()


put_bot = XiuConfig().put_bot
shield_group = XiuConfig().shield_group
response_group = XiuConfig().response_group
shield_private = XiuConfig().shield_private
try:
    put_bot_ = put_bot[0]
except Exception:
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
        "修仙帮助：查看仙界信息"
    ),
    extra={
        "show": True,
        "priority": 15
    }
)


def _safe_str(value):
    return "" if value is None else str(value)


_RATE_LIMIT_CONFIG = XiuConfig()
_CONFIG_MISSING = object()


def _get_config_raw(name: str, default):
    try:
        value = getattr(DRIVER.config, name, _CONFIG_MISSING)
        if value is not _CONFIG_MISSING and value is not None:
            return value
    except Exception:
        pass

    try:
        return getattr(_RATE_LIMIT_CONFIG, name, default)
    except Exception:
        return default


def _get_config_int(name: str, default: int) -> int:
    try:
        return int(float(_get_config_raw(name, default)))
    except Exception:
        return default


def _get_config_str(name: str, default: str) -> str:
    try:
        return str(_get_config_raw(name, default))
    except Exception:
        return default


USER_COMMAND_RATE_WINDOW_SECONDS = _get_config_int("xiuxian_user_command_rate_window", 60)
USER_COMMAND_RATE_LIMIT = _get_config_int("xiuxian_user_command_rate_limit", 1000)
USER_COMMAND_RATE_LOG_INTERVAL_SECONDS = _get_config_int("xiuxian_user_command_rate_log_interval", 10)
USER_COMMAND_RATE_CACHE_CLEAN_INTERVAL_SECONDS = _get_config_int("xiuxian_user_command_rate_cache_clean_interval", 60)
GLOBAL_COMMAND_RATE_WINDOW_SECONDS = _get_config_int("xiuxian_global_command_rate_window", 1)
GLOBAL_COMMAND_RATE_LIMIT = _get_config_int("xiuxian_global_command_rate_limit", 1000)
GLOBAL_COMMAND_RATE_LOG_INTERVAL_SECONDS = _get_config_int("xiuxian_global_command_rate_log_interval", 5)
GLOBAL_COMMAND_OVERLOAD_NOTICE = _get_config_str("xiuxian_global_command_overload_notice", "当前命令较多，已进入繁忙保护，请稍后再试。")
GLOBAL_COMMAND_OVERLOAD_NOTICE_INTERVAL_SECONDS = _get_config_int("xiuxian_global_command_overload_notice_interval", 30)
GLOBAL_COMMAND_OVERLOAD_NOTICE_RATE_WINDOW_SECONDS = _get_config_int("xiuxian_global_command_overload_notice_rate_window", 1)
GLOBAL_COMMAND_OVERLOAD_NOTICE_RATE_LIMIT = _get_config_int("xiuxian_global_command_overload_notice_rate_limit", 5)
_user_command_rate_hits: dict[str, deque[float]] = {}
_user_command_rate_last_log: dict[str, float] = {}
_user_command_rate_last_cleanup = 0.0
_global_command_rate_hits: deque[float] = deque()
_global_command_rate_last_log = 0.0
_global_overload_notice_hits: deque[float] = deque()
_global_overload_notice_last_sent: dict[str, float] = {}


def _get_rate_limit_user_id(event):
    try:
        return _safe_str(event.get_user_id())
    except Exception:
        pass

    for attr in ("user_id", "operator_id"):
        value = getattr(event, attr, None)
        if value:
            return _safe_str(value)

    author = getattr(event, "author", None)
    if author is not None:
        for attr in ("user_openid", "member_openid", "id"):
            value = getattr(author, attr, None)
            if value:
                return _safe_str(value)

    return ""


def _get_rate_limit_plain_text(event) -> str:
    for attr in ("raw_message", "plaintext", "content"):
        value = getattr(event, attr, None)
        if value:
            return _safe_str(value).strip()

    try:
        message = event.get_message()
        if hasattr(message, "extract_plain_text"):
            return _safe_str(message.extract_plain_text()).strip()
        return _safe_str(message).strip()
    except Exception:
        pass

    return ""


def _is_command_attempt_text(text: str) -> bool:
    text = _safe_str(text).strip()
    if not text:
        return False

    command_start = getattr(DRIVER.config, "command_start", None)
    if command_start is None:
        command_start = {""}

    if isinstance(command_start, str):
        starts = {command_start}
    else:
        starts = {_safe_str(item) for item in command_start}
    if "" in starts:
        # 本项目默认 COMMAND_START = [""]，任意文本都可能进入 on_command 匹配。
        return True

    return any(text.startswith(prefix) for prefix in starts if prefix)


def _cleanup_user_command_rate_cache(now: float):
    global _user_command_rate_last_cleanup

    if now - _user_command_rate_last_cleanup < USER_COMMAND_RATE_CACHE_CLEAN_INTERVAL_SECONDS:
        return

    cutoff = now - USER_COMMAND_RATE_WINDOW_SECONDS
    for user_id, hits in list(_user_command_rate_hits.items()):
        while hits and hits[0] <= cutoff:
            hits.popleft()

        if not hits:
            _user_command_rate_hits.pop(user_id, None)
            _user_command_rate_last_log.pop(user_id, None)

    _user_command_rate_last_cleanup = now


def _cleanup_global_overload_notice_cache(now: float):
    cutoff = now - GLOBAL_COMMAND_OVERLOAD_NOTICE_RATE_WINDOW_SECONDS
    while _global_overload_notice_hits and _global_overload_notice_hits[0] <= cutoff:
        _global_overload_notice_hits.popleft()

    expire_before = now - GLOBAL_COMMAND_OVERLOAD_NOTICE_INTERVAL_SECONDS
    for key, last_sent in list(_global_overload_notice_last_sent.items()):
        if last_sent <= expire_before:
            _global_overload_notice_last_sent.pop(key, None)


def _check_user_command_rate_limit(user_id: str, now: float):
    if not user_id:
        return

    _cleanup_user_command_rate_cache(now)

    cutoff = now - USER_COMMAND_RATE_WINDOW_SECONDS
    hits = _user_command_rate_hits.setdefault(user_id, deque())

    while hits and hits[0] <= cutoff:
        hits.popleft()

    if len(hits) >= USER_COMMAND_RATE_LIMIT:
        last_log = _user_command_rate_last_log.get(user_id, 0.0)
        if now - last_log >= USER_COMMAND_RATE_LOG_INTERVAL_SECONDS:
            logger.warning(
                f"[用户命令限流] user_id={user_id} 在 "
                f"{int(USER_COMMAND_RATE_WINDOW_SECONDS)}s 内超过 "
                f"{USER_COMMAND_RATE_LIMIT} 条命令，已忽略后续命令"
            )
            _user_command_rate_last_log[user_id] = now
        raise IgnoredException("用户命令触发限流")


def _record_user_command_rate_hit(user_id: str, now: float):
    if user_id:
        _user_command_rate_hits.setdefault(user_id, deque()).append(now)


def _is_global_command_rate_limited(now: float):
    global _global_command_rate_last_log

    cutoff = now - GLOBAL_COMMAND_RATE_WINDOW_SECONDS
    while _global_command_rate_hits and _global_command_rate_hits[0] <= cutoff:
        _global_command_rate_hits.popleft()

    if len(_global_command_rate_hits) >= GLOBAL_COMMAND_RATE_LIMIT:
        if now - _global_command_rate_last_log >= GLOBAL_COMMAND_RATE_LOG_INTERVAL_SECONDS:
            logger.warning(
                f"[全局命令入口限流] 最近 "
                f"{GLOBAL_COMMAND_RATE_WINDOW_SECONDS:g}s 内超过 "
                f"{GLOBAL_COMMAND_RATE_LIMIT} 条命令，已在 matcher 前忽略超量事件"
            )
            _global_command_rate_last_log = now
        return True

    _global_command_rate_hits.append(now)
    return False


def _get_global_overload_notice_key(event, user_id: str) -> str:
    try:
        group_id = _safe_str(get_group_id(event))
    except Exception:
        group_id = _safe_str(getattr(event, "group_id", ""))

    if group_id:
        return f"group:{group_id}"

    if user_id:
        return f"user:{user_id}"

    try:
        return f"session:{event.get_session_id()}"
    except Exception:
        return ""


def _can_send_global_overload_notice(event, user_id: str, now: float) -> bool:
    if not GLOBAL_COMMAND_OVERLOAD_NOTICE or GLOBAL_COMMAND_OVERLOAD_NOTICE_RATE_LIMIT <= 0:
        return False

    _cleanup_global_overload_notice_cache(now)
    if len(_global_overload_notice_hits) >= GLOBAL_COMMAND_OVERLOAD_NOTICE_RATE_LIMIT:
        return False

    key = _get_global_overload_notice_key(event, user_id)
    if key:
        last_sent = _global_overload_notice_last_sent.get(key, 0.0)
        if now - last_sent < GLOBAL_COMMAND_OVERLOAD_NOTICE_INTERVAL_SECONDS:
            return False
        _global_overload_notice_last_sent[key] = now

    _global_overload_notice_hits.append(now)
    return True


async def _send_global_overload_notice(bot: Bot, event, user_id: str, now: float):
    if not _can_send_global_overload_notice(event, user_id, now):
        return

    try:
        bot = patch_bot_inplace(bot)
        await bot.send(event=event, message=GLOBAL_COMMAND_OVERLOAD_NOTICE)
    except Exception as e:
        logger.debug(f"[全局命令入口限流] 繁忙提示发送失败: {e}")


async def _check_command_ingress_rate_limit(bot: Bot, event):
    plain_text = _get_rate_limit_plain_text(event)
    if not _is_command_attempt_text(plain_text):
        return

    user_id = _get_rate_limit_user_id(event)
    now = time.monotonic()

    _check_user_command_rate_limit(user_id, now)
    if _is_global_command_rate_limited(now):
        await _send_global_overload_notice(bot, event, user_id, now)
        raise IgnoredException("全局命令入口过载")

    _record_user_command_rate_hit(user_id, now)


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
    await _check_command_ingress_rate_limit(bot, event)

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
