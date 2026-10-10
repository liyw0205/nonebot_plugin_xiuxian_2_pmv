import os
import random
import time
import threading
from dataclasses import dataclass
from weakref import WeakSet
from nonebot.log import logger
from nonebot.rule import Rule
from nonebot import get_driver
from nonebot import get_bots, get_bot, require
from enum import IntEnum, auto
from asyncio import get_running_loop
from typing import Dict
from nonebot.matcher import Matcher
from nonebot.params import Depends
from ..adapter_compat import (
    Bot,
    GROUP,
    Message,
    MessageEvent,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
    patch_context
)
from ..messaging.delivery import delivery_service
from ..xiuxian_config import XiuConfig, JsonConfig
from ...bootstrap.legacy import register_legacy_shutdown
from .utils import check_user, consume_player_stamina, get_msg_pic, get_user_profile, handle_send, recover_player_stamina


ADMIN_IDS = get_driver().config.superusers
limit_all_message = require("nonebot_plugin_apscheduler").scheduler
limit_all_stamina = require("nonebot_plugin_apscheduler").scheduler

_LIMIT_ALL_DATA_MAX_KEYS = 100000
_RATE_LIMIT_KEY_MAX_LENGTH = 512
_RATE_LIMIT_BLOCKED = -1
limit_all_data: Dict[str, int] = {}
limit_num = 99999
_limit_all_data_lock = threading.Lock()
_limit_all_capacity_warned = False


@limit_all_message.scheduled_job(
    "interval",
    minutes=1,
    id="reset_message_rate_limits",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=30,
)
def limit_all_message_():
    global _limit_all_capacity_warned
    with _limit_all_data_lock:
        limit_all_data.clear()
        _limit_all_capacity_warned = False
    logger.opt(colors=True).success(f"<green>已重置消息字典！</green>")

@limit_all_stamina.scheduled_job(
    "interval",
    minutes=1,
    id="recover_user_stamina",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=30,
)
def limit_all_stamina_():
    # 恢复体力
    started_at = time.monotonic()
    try:
        result = recover_player_stamina(
            XiuConfig().max_stamina,
            XiuConfig().stamina_recovery_points,
            batch_size=max(1, int(os.getenv("XIUXIAN_STAMINA_RECOVERY_BATCH_SIZE", "1000"))),
        )
        updated = int(result.get("updated", 0) or 0)
    except Exception as e:
        logger.opt(exception=e).warning("体力恢复定时任务执行失败，当前批次已回滚，后续恢复已停止")
        return

    elapsed = time.monotonic() - started_at
    if elapsed >= 10:
        logger.warning(f"体力恢复定时任务耗时过长：{elapsed:.2f}s，更新用户数：{updated}")

def limit_all_run(user_id: str):
    global _limit_all_capacity_warned
    user_id = str(user_id)
    if len(user_id) > _RATE_LIMIT_KEY_MAX_LENGTH:
        return False

    should_warn = False
    with _limit_all_data_lock:
        count = limit_all_data.get(user_id)
        if count == _RATE_LIMIT_BLOCKED:
            return False
        if count is None:
            if len(limit_all_data) >= _LIMIT_ALL_DATA_MAX_KEYS:
                if not _limit_all_capacity_warned:
                    _limit_all_capacity_warned = True
                    should_warn = True
                result = False
            else:
                count = 0
                result = None
        else:
            result = None

        if count is not None:
            count += 1
            if count > limit_num:
                limit_all_data[user_id] = _RATE_LIMIT_BLOCKED
                result = True
            else:
                limit_all_data[user_id] = count

    if should_warn:
        logger.warning(
            "消息限流用户表达到容量上限；新用户将在下次重置前被静默拒绝"
        )
    return result


def format_time(seconds: int) -> str:
    """将秒数转换为更大的时间单位"""
    from .periods import format_duration_compact

    return format_duration_compact(seconds).replace("分", "分钟")
    

def get_random_chat_notice():
    return random.choice([
        "慢...慢一..点❤，还有{}，让我再歇会！",
        "冷静一下，还有{}，让我再歇会！",
        "让我歇口气，还有{}，马上就好~",
        "耐心一点哦，还有{}就可以继续啦~",
        "别急嘛~还有{}，让我喘口气~",
        "稍等一下下啦，还有{}就好啦！",
        "时间还没到，还有{}，歇会歇会~~"
    ])

bu_ji_notice = random.choice(["别急！","急也没用!","让我先急!"])


def _event_type_name(event) -> str:
    names: list[str] = []
    for attr in ("__type__", "type"):
        value = getattr(event, attr, None)
        if value is not None:
            names.append(str(value))
    try:
        names.append(str(event.get_event_name()))
    except Exception:
        pass
    return " ".join(names).upper()


def _is_full_message_event(event) -> bool:
    """QQ 全量消息事件（非艾特专用通道）。"""
    return "GROUP_MESSAGE_CREATE" in _event_type_name(event)


def _is_explicit_command_intent(event, matcher: Matcher | None = None) -> bool:
    """
    是否像“真的在打指令”，用于全量群冷却提示：
    - 私聊：始终视为指令意图
    - 群聊：@机器人 / 非全量事件 / 有命令主键且带文本
    纯表情、空文本闲聊：不算
    """
    if isinstance(event, PrivateMessageEvent):
        return True

    # 明确艾特机器人：保留提示
    if bool(getattr(event, "to_me", False)):
        return True

    # 非全量消息事件（如 GROUP_AT_MESSAGE_CREATE）通常就是指令通道
    if not _is_full_message_event(event):
        return True

    # 全量消息：必须有可识别文本，且 matcher 本身是命令路由
    plain = ""
    try:
        plain = str(event.get_plaintext() if hasattr(event, "get_plaintext") else "").strip()
    except Exception:
        plain = ""
    if not plain:
        try:
            msg = event.get_message()
            if hasattr(msg, "extract_plain_text"):
                plain = str(msg.extract_plain_text() or "").strip()
        except Exception:
            plain = ""
    if not plain:
        # 纯表情/图片/空消息：不提示
        return False

    # 有文本 + 已进入带 Cooldown 的命令 handler：视为指令意图
    # （matcher 能命中说明不是完全无关闲聊）
    if matcher is not None:
        try:
            from ..on_compat import _PRIMARY_COMMAND_NAMES  # type: ignore

            primary = _PRIMARY_COMMAND_NAMES.get(type(matcher)) or _PRIMARY_COMMAND_NAMES.get(matcher)
            if primary:
                return True
        except Exception:
            pass
        cmds = getattr(matcher, "commands", None) or set()
        if cmds:
            return True

    return False


def _should_silence_full_group_notice(conf: JsonConfig, group_id: str | None, event, matcher: Matcher | None = None) -> bool:
    """
    全量群冷却/限流提示策略：
    - 非全量群：不静默
    - 全量群 + 明确指令意图（艾特/命令）：不静默，正常提示
    - 全量群 + 闲聊/表情：静默
    """
    if not group_id or not conf.is_full_message_group(group_id):
        return False
    return not _is_explicit_command_intent(event, matcher)


class CooldownIsolateLevel(IntEnum):
    """命令冷却的隔离级别"""

    GLOBAL = auto()
    GROUP = auto()
    USER = auto()
    GROUP_USER = auto()


class _CooldownKeyBudget:
    def __init__(self, max_keys: int):
        self.max_keys = max_keys
        self._active_keys = 0
        self._lock = threading.Lock()
        self._capacity_warned = False

    def reserve(self) -> bool:
        with self._lock:
            if self._active_keys >= self.max_keys:
                return False
            self._active_keys += 1
            return True

    def release(self) -> None:
        with self._lock:
            if self._active_keys > 0:
                self._active_keys -= 1

    def should_warn_capacity(self) -> bool:
        with self._lock:
            if self._capacity_warned:
                return False
            self._capacity_warned = True
            return True

    @property
    def active_keys(self) -> int:
        with self._lock:
            return self._active_keys


_COOLDOWN_MAX_ACTIVE_KEYS = 65536
_cooldown_key_budget = _CooldownKeyBudget(_COOLDOWN_MAX_ACTIVE_KEYS)


@dataclass(slots=True)
class _CooldownState:
    remaining: int
    started_at: int | None = None


class _CooldownRuntime:
    """Own active cooldown timers so lifecycle shutdown can release closures."""

    def __init__(self) -> None:
        self.running: Dict[str, _CooldownState] = {}
        self.handles: set[object] = set()

    def clear(self) -> None:
        for handle in tuple(self.handles):
            cancel = getattr(handle, "cancel", None)
            if callable(cancel):
                cancel()
        self.handles.clear()
        released = len(self.running)
        self.running.clear()
        for _ in range(released):
            _cooldown_key_budget.release()


_cooldown_runtimes: WeakSet[_CooldownRuntime] = WeakSet()


def _shutdown_cooldown_runtimes() -> None:
    for runtime in tuple(_cooldown_runtimes):
        runtime.clear()


register_legacy_shutdown(_shutdown_cooldown_runtimes)

def Cooldown(
        cd_time: float = 0.5,
        isolate_level: CooldownIsolateLevel = CooldownIsolateLevel.USER,
        parallel: int = 1,
        stamina_cost: int = 0,
        stamina_operation_prefix: str | None = None,
) -> None:
    """依赖注入形式的命令冷却

    用法:
        ```python
        @matcher.handle(parameterless=[Cooldown(cooldown=11.4514, ...)])
        async def handle_command(matcher: Matcher, message: Message):
            ...
        ```

    参数:
        cd_time: 命令冷却间隔
        isolate_level: 命令冷却的隔离级别, 参考 `CooldownIsolateLevel`
        parallel: 并行执行的命令数量
        stamina_cost: 每次执行命令消耗的体力值
        stamina_operation_prefix: 可选的稳定事件前缀，用于重放时避免重复扣体力
    """
    if not isinstance(isolate_level, CooldownIsolateLevel):
        raise ValueError(
            f"invalid isolate level: {isolate_level!r}, "
            "isolate level must use provided enumerate value."
        )
    runtime = _CooldownRuntime()
    _cooldown_runtimes.add(runtime)
    running = runtime.running

    def increase(key: str, state: _CooldownState):
        if running.get(key) is not state:
            return
        state.remaining += 1
        if state.remaining >= parallel:
            del running[key]
            _cooldown_key_budget.release()
        return

    async def dependency(bot: Bot, matcher: Matcher, event: MessageEvent | PrivateMessageEvent):
        bot, event = patch_context(bot, event)
        if XiuConfig().at_response:
            if not event.to_me:
                logger.opt(colors=True).success(f"<green>不为艾特命令,已忽略！</green>")
                await matcher.finish()
        is_private = isinstance(event, PrivateMessageEvent)
        user_id = str(event.get_user_id())
        group_id = str(event.group_id) if not is_private else None
        conf = JsonConfig()
        conf_data = conf.read_data()

        # 娱乐模块：不受修仙开关限制
        plugin_name = str(getattr(matcher, "plugin_name", "") or "")
        module_name = str(getattr(matcher, "module_name", "") or getattr(matcher, "module", "") or "")
        is_entertainment = (
            "xiuxian_entertainment" in plugin_name
            or "xiuxian_entertainment" in module_name
        )

        # 修仙帮助：关闭时仅提示开启命令；其他修仙指令静默
        is_xiuxian_help = False
        try:
            from ..on_compat import _PRIMARY_COMMAND_NAMES  # type: ignore

            primary = _PRIMARY_COMMAND_NAMES.get(type(matcher)) or _PRIMARY_COMMAND_NAMES.get(matcher)
            if primary in {"修仙帮助", "修仙菜单"}:
                is_xiuxian_help = True
        except Exception:
            primary = None
        if not is_xiuxian_help:
            cmds = getattr(matcher, "commands", None) or set()
            is_xiuxian_help = any(
                (isinstance(c, tuple) and c and str(c[0]) in {"修仙帮助", "修仙菜单"})
                or str(c) in {"修仙帮助", "修仙菜单"}
                for c in cmds
            )
        if not is_xiuxian_help and primary:
            is_xiuxian_help = str(primary) in {"修仙帮助", "修仙菜单"}

        limit_type = limit_all_run(str(event.get_user_id()))
        if limit_type is True:
            # 全量群：闲聊/表情不刷“别急”；正常艾特/指令仍提示
            if _should_silence_full_group_notice(conf, group_id, event, matcher):
                await matcher.finish()
            bot = await assign_bot_group(group_id=group_id)
            await delivery_service.reply(bot, event, bu_ji_notice)
            await matcher.finish()
        elif limit_type is False:
            await matcher.finish()
        else:
            pass

        loop = get_running_loop()

        if isolate_level is CooldownIsolateLevel.GROUP:
            key = str(
                event.group_id
                if isinstance(event, GroupMessageEvent)
                else event.user_id,
            )
        elif isolate_level is CooldownIsolateLevel.USER:
            key = str(event.user_id)
        elif isolate_level is CooldownIsolateLevel.GROUP_USER:
            key = (
                f"{event.group_id}_{event.user_id}"
                if isinstance(event, GroupMessageEvent)
                else str(event.user_id)
            )
        else:
            key = CooldownIsolateLevel.GLOBAL.name

        if len(key) > _RATE_LIMIT_KEY_MAX_LENGTH:
            await matcher.finish()

        # 修仙开关：默认开启；禁用列表里的群仅限制修仙，不限制娱乐
        if (
            not is_private
            and not is_entertainment
            and group_id
            and conf.is_group_xiuxian_disabled(group_id)
        ):
            if is_xiuxian_help:
                bot = await assign_bot_group(group_id=group_id)
                await handle_send(
                    bot,
                    event,
                    "本群修仙功能已关闭。\n开启命令：【启用修仙功能】",
                    md_type="修仙",
                    k1="开启修仙",
                    v1="启用修仙功能",
                    k2="娱乐帮助",
                    v2="娱乐帮助",
                )
            await matcher.finish()

        if is_private:
            if is_private and not conf_data.get("private", True) and not is_entertainment:
                if is_xiuxian_help:
                    await delivery_service.reply(
                        bot,
                        event,
                        "私聊修仙功能未启用，请联系管理员在群聊中发送「启用私聊功能」！",
                    )
                await matcher.finish()

        if XiuConfig().admin_debug:
            if event.get_user_id() not in bot.config.superusers:
                await matcher.finish()
        if user_id in ADMIN_IDS:
            return
        if stamina_cost > 0:
            stamina_user_id = user_id
            user_data = None
            checked_user = False
            try:
                checked_user = True
                is_user, active_user_data, check_msg = check_user(event)
                if active_user_data:
                    user_data = active_user_data
                    stamina_user_id = str(active_user_data.get("user_id", user_id))
                    if not is_user:
                        await handle_send(bot, event, check_msg)
                        await matcher.finish()
            except Exception as e:
                checked_user = False
                logger.warning(f"获取当前体力身份失败，回退到真实ID {user_id}: {e}")

            if user_data is None and not checked_user:
                user_data = get_user_profile(stamina_user_id)

            if user_data:
                current_stamina = int(user_data.get("user_stamina") or 0)
                message_id = str(
                    getattr(event, "message_id", "") or getattr(event, "id", "") or ""
                ).strip()
                stamina_operation_id = (
                    f"{str(stamina_operation_prefix).strip()}:{stamina_user_id}:{message_id}"
                    if stamina_operation_prefix and message_id
                    else None
                )
                if current_stamina < stamina_cost and not stamina_operation_id:
                    msg = "你没有足够的体力，请等待体力恢复后再试！"
                    await handle_send(bot, event, msg)
                    await matcher.finish()
                stamina_result = consume_player_stamina(
                    stamina_user_id,
                    stamina_cost,
                    expected_stamina=current_stamina,
                    operation_id=stamina_operation_id,
                )
                if stamina_result.get("status") not in {"applied", "duplicate"}:
                    if stamina_result.get("status") == "stamina_insufficient":
                        msg = "你没有足够的体力，请等待体力恢复后再试！"
                    else:
                        msg = "体力状态已更新，请稍后重试。"
                    await handle_send(bot, event, msg)
                    await matcher.finish()
        if cd_time <= 0:
            return

        state = running.get(key)
        if state is None:
            if not _cooldown_key_budget.reserve():
                if _cooldown_key_budget.should_warn_capacity():
                    logger.warning(
                        "命令冷却状态达到进程级容量上限；新 key 将被静默拒绝"
                    )
                await matcher.finish()
            state = _CooldownState(remaining=parallel)
            running[key] = state

        if state.remaining <= 0:
            if cd_time >= 1.5:
                # 全量群：闲聊/表情静默；正常艾特/指令保留冷却提示
                if _should_silence_full_group_notice(conf, group_id, event, matcher):
                    await matcher.finish()
                if state.started_at is None:
                    await matcher.finish()
                time = int(cd_time - (loop.time() - state.started_at))
                if time <= 1:
                    time = 1
                formatted_time = format_time(time)
                msg = get_random_chat_notice().format(formatted_time)
                await handle_send(bot, event, msg)
                await matcher.finish()
            else:
                await matcher.finish()
        else:
            state.started_at = int(loop.time())
            state.remaining -= 1
            timer_ref: list[object | None] = [None]

            def on_timer() -> None:
                timer_handle = timer_ref[0]
                if timer_handle is not None:
                    runtime.handles.discard(timer_handle)
                increase(key, state)

            timer_handle = loop.call_later(cd_time, on_timer)
            # TimerHandle is intentionally kept only until it fires or shutdown;
            # this lets shutdown cancel callbacks that still capture event data.
            if timer_handle is not None:
                runtime.handles.add(timer_handle)
            timer_ref[0] = timer_handle
        return

    return Depends(dependency)


put_bot = XiuConfig().put_bot
main_bot = XiuConfig().main_bo
layout_bot_dict = XiuConfig().layout_bot_dict


async def check_bot(bot: Bot) -> bool:  # 检测bot实例是否为主qq
    if str(bot.self_id) in put_bot:
        return True
    else:
        return False


def check_rule_bot() -> Rule:  # 对传入的消息检测，是主qq传入的消息就响应，其他的不响应
    async def _check_bot_(bot: Bot, event: GroupMessageEvent) -> bool:
        if str(bot.self_id) in put_bot:
            if str(event.get_user_id()) in main_bot:
                return False
            else:
                return True
        else:
            return False

    return Rule(_check_bot_)


async def range_bot(bot: Bot, event: GroupMessageEvent):  # 随机一个qq发送消息
    group_id = str(event.group_id)
    bot_list = list(get_bots().keys())
    try:
        bot = get_bots()[random.choice(bot_list)]
    except KeyError:
        pass
    return bot, group_id


async def assign_bot(bot=None, event=None):  # 按字典分配对应qq发送消息
    is_private = isinstance(event, PrivateMessageEvent)
    group_id = str(event.group_id) if not is_private else None
    try:
        bot_id = layout_bot_dict[group_id]
        if type(bot_id) is str:
            bot = get_bots()[bot_id]
        elif type(bot_id) is list:
            bot = get_bots()[random.choice(bot_id)]
        else:
            bot = bot
    except Exception:
        bot = bot
    return bot, group_id


async def assign_bot_group(group_id):  # 只导入群号，按字典分配对应qq发送消息
    group_id = str(group_id)
    try:
        bot_id = layout_bot_dict[group_id]
        if type(bot_id) is str:
            bot = get_bots()[bot_id]
        elif type(bot_id) is list:
            bot = get_bots()[random.choice(bot_id)]
        else:
            bot = get_bots()[put_bot[0]]
    except KeyError:
        bot = None
    except Exception as e:
        logger.opt(colors=True).error(f"<red>错误: {e}</red>")

    if bot is None:
        try:
            bot = get_bot()
        except ValueError:
            logger.opt(colors=True).error(f"<red>未找到对应的bot实例,请检查实现端链接状况！</red>")
            bot = None

    return bot
