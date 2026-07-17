import random
import time
from nonebot.log import logger
from nonebot.rule import Rule
from nonebot import get_driver
from nonebot import get_bots, get_bot, require
from enum import IntEnum, auto
from collections import defaultdict
from asyncio import get_running_loop
from typing import DefaultDict, Dict, Any
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
from .xiuxian2_handle import XiuxianDateManage
from .utils import get_msg_pic, check_user, handle_send


sql_message = XiuxianDateManage()
ADMIN_IDS = get_driver().config.superusers
limit_all_message = require("nonebot_plugin_apscheduler").scheduler
limit_all_stamina = require("nonebot_plugin_apscheduler").scheduler

limit_all_data: Dict[str, Any] = {}
limit_num = 99999

@limit_all_message.scheduled_job(
    "interval",
    minutes=1,
    id="reset_message_rate_limits",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=30,
)
def limit_all_message_():
    # 重置消息字典
    global limit_all_data
    limit_all_data  = {}
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
        updated = sql_message.update_all_users_stamina(
            XiuConfig().max_stamina,
            XiuConfig().stamina_recovery_points,
        )
    except Exception as e:
        logger.opt(exception=e).warning("体力恢复定时任务执行失败，已回滚本轮恢复")
        return

    elapsed = time.monotonic() - started_at
    if elapsed >= 10:
        logger.warning(f"体力恢复定时任务耗时过长：{elapsed:.2f}s，更新用户数：{updated}")

def limit_all_run(user_id: str):
    global limit_all_data
    user_id = str(user_id)
    num = None
    tip = None
    try:
        num = limit_all_data[user_id]["num"]
        tip = limit_all_data[user_id]["tip"]
    except Exception:
        limit_all_data[user_id] = {"num": 0,
                                   "tip" : False}
        num = 0
        tip = False
    num += 1    
    if num > limit_num and tip == False:
        tip = True
        limit_all_data[user_id]["num"] = num
        limit_all_data[user_id]["tip"] = tip
        return True
    if num > limit_num and tip == True:
        limit_all_data[user_id]["num"] = num
        return False
    else:
        limit_all_data[user_id]["num"] = num
        return None


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

def Cooldown(
        cd_time: float = 0.5,
        isolate_level: CooldownIsolateLevel = CooldownIsolateLevel.USER,
        parallel: int = 1,
        stamina_cost: int = 0
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
    """
    if not isinstance(isolate_level, CooldownIsolateLevel):
        raise ValueError(
            f"invalid isolate level: {isolate_level!r}, "
            "isolate level must use provided enumerate value."
        )
    running: DefaultDict[str, int] = defaultdict(lambda: parallel)
    time_sy: Dict[str, int] = {}
    

    def increase(key: str, value: int = 1):
        running[key] += value
        if running[key] >= parallel:
            del running[key]
            del time_sy[key]
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
                user_data = sql_message.get_user_info_with_id(stamina_user_id)

            if user_data:
                current_stamina = int(user_data.get("user_stamina") or 0)
                if current_stamina < stamina_cost:
                    msg = "你没有足够的体力，请等待体力恢复后再试！"
                    await handle_send(bot, event, msg)
                    await matcher.finish()
                sql_message.update_user_stamina(stamina_user_id, stamina_cost, 2)  # 减少体力
        if cd_time <= 0:
            return
        if running[key] <= 0:
            if cd_time >= 1.5:
                # 全量群：闲聊/表情静默；正常艾特/指令保留冷却提示
                if _should_silence_full_group_notice(conf, group_id, event, matcher):
                    await matcher.finish()
                time = int(cd_time - (loop.time() - time_sy[key]))
                if time <= 1:
                    time = 1
                formatted_time = format_time(time)
                msg = get_random_chat_notice().format(formatted_time)
                await handle_send(bot, event, msg)
                await matcher.finish()
            else:
                await matcher.finish()
        else:
            time_sy[key] = int(loop.time())
            running[key] -= 1
            loop.call_later(cd_time, lambda: increase(key))
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
