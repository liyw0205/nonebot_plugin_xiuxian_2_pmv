from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from nonebot.log import logger

from ...xiuxian.adapter_compat import MessageSegment
from ...xiuxian.qq_compat.lifecycle import apply_lifecycle_event, is_lifecycle_event
from ...xiuxian.xiuxian_config import XiuConfig
from ...xiuxian.xiuxian_utils.message_markdown import strip_md_command_links
from ...xiuxian.xiuxian_utils.utils import handle_send


async def _assign_default_bot(**kwargs):
    from ...xiuxian.xiuxian_utils.lay_out import assign_bot

    return await assign_bot(**kwargs)


@dataclass(frozen=True)
class GroupLifecycleNoticeDecision:
    action: str = ""
    group_id: str = ""
    finish_matcher: bool = False


def _event_type_name(event: Any) -> str:
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


def _extract_group_id(event: Any) -> str:
    for key in ("group_openid", "group_id", "groupId"):
        value = getattr(event, key, None)
        if value:
            return str(value)
    return ""


def _extract_event_id(event: Any) -> str:
    for key in ("event_id", "id"):
        value = getattr(event, key, None)
        if value:
            return str(value)
    return ""


def _legacy_action(event: Any) -> str:
    name = _event_type_name(event)
    if "GROUP_ADD_ROBOT" in name or "BOT_JOIN" in name:
        return "bot_join_group"
    if "GROUP_DEL_ROBOT" in name or "BOT_LEAVE" in name:
        return "bot_leave_group"
    if any(
        token in name
        for token in (
            "GROUP_MEMBER_ADD",
            "GROUPINCREASE",
            "GROUP_INCREASE",
            "MEMBER_JOIN",
            "NOTICE.GROUP_INCREASE",
        )
    ):
        return "member_join_group"
    return ""


class GroupLifecycleNoticeApplication:
    """Owns group lifecycle classification, policy and welcome delivery."""

    def __init__(
        self,
        admin_config: Any,
        *,
        settings_provider: Callable[[], Any] = XiuConfig,
        lifecycle_applier: Callable[[Any, Any], Any] = apply_lifecycle_event,
        assign_bot_fn: Callable[..., Any] = _assign_default_bot,
        message_segment: Any = MessageSegment,
        send_fallback: Callable[..., Any] = handle_send,
        strip_links: Callable[[str], str] = strip_md_command_links,
    ) -> None:
        self.admin_config = admin_config
        self.settings_provider = settings_provider
        self.lifecycle_applier = lifecycle_applier
        self.assign_bot = assign_bot_fn
        self.message_segment = message_segment
        self.send_fallback = send_fallback
        self.strip_links = strip_links

    def _group_allowed(self, bot: Any, group_id: str, settings: Any) -> bool:
        gid = str(group_id or "").strip()
        if not gid:
            return False
        put_bot = [str(item) for item in (getattr(settings, "put_bot", None) or [])]
        if put_bot and str(getattr(bot, "self_id", "") or "") not in put_bot:
            return False
        shield = {str(item) for item in (getattr(settings, "shield_group", None) or [])}
        if bool(getattr(settings, "response_group", False)):
            return gid in shield
        return gid not in shield

    def _welcome_enabled(self, group_id: str, settings: Any) -> bool:
        if not bool(getattr(settings, "group_welcome", True)):
            return False
        data = self.admin_config.repository.read_data()
        disabled = {str(item) for item in data.get("welcome_disabled_groups", [])}
        return group_id not in disabled

    @staticmethod
    def _member_welcome_text(settings: Any) -> str:
        return (getattr(settings, "group_welcome_msg", None) or "").strip() or "欢迎道友入群！"

    @staticmethod
    def _bot_join_text(settings: Any) -> str:
        msg = (getattr(settings, "group_bot_join_msg", None) or "").strip()
        if msg:
            return msg
        return (
            "必死之境机逢仙缘，修仙之路波澜壮阔！\n"
            "> 发送：\n"
            "【我要修仙】\n> 踏入修仙界\n"
            "【修仙帮助】\n> 查看玩法\n"
            "【娱乐帮助】\n> 查看娱乐功能。"
        )

    @staticmethod
    def _welcome_buttons() -> list[list[tuple[str, str]]]:
        return [
            [("我要修仙", "我要修仙"), ("修仙帮助", "修仙帮助"), ("娱乐帮助", "娱乐帮助")],
            [("关闭欢迎", "关闭进群欢迎")],
        ]

    @staticmethod
    def _welcome_md_text(msg: str) -> str:
        body = (msg or " ").replace("\n", "\r")
        links = (
            "[我要修仙](mqqapi://aio/inlinecmd?command=我要修仙&enter=false&reply=false)"
            " | [修仙帮助](mqqapi://aio/inlinecmd?command=修仙帮助&enter=false&reply=false)"
            " | [娱乐帮助](mqqapi://aio/inlinecmd?command=娱乐帮助&enter=false&reply=false)"
            " | [关闭欢迎](mqqapi://aio/inlinecmd?command=关闭进群欢迎&enter=false&reply=false)"
        )
        return f"{body}\r\r---\r\r{links}"

    async def _send_lifecycle_message(
        self, bot: Any, event: Any, group_id: str, message: Any, *, kind: str
    ) -> bool:
        event_id = _extract_event_id(event)
        try:
            send_to_group = getattr(bot, "send_to_group", None)
            if callable(send_to_group):
                kwargs = {"group_openid": group_id, "message": message}
                if event_id:
                    kwargs["event_id"] = event_id
                await send_to_group(**kwargs)
                logger.info(
                    f"[{kind}] 已发送 group={group_id} via=send_to_group "
                    f"event_id={bool(event_id)}"
                )
                return True
        except Exception as exc:
            logger.warning(f"[{kind}] send_to_group 失败 group={group_id}: {exc}")
        try:
            send = getattr(bot, "send", None)
            if callable(send):
                await send(event, message)
                logger.info(f"[{kind}] 已发送 group={group_id} via=bot.send")
                return True
        except Exception as exc:
            logger.debug(f"[{kind}] bot.send 不可用 group={group_id}: {exc}")
        return False

    async def _send_notice(
        self, bot: Any, event: Any, group_id: str, msg: str, *, kind: str, settings: Any
    ) -> None:
        if not msg or not group_id:
            return
        try:
            bot, _ = await self.assign_bot(bot=bot, event=event)
        except Exception:
            pass

        md_on = bool(getattr(settings, "markdown_status", False))
        plain = self.strip_links(msg)
        if md_on:
            try:
                md_body = msg.replace("\n", "\r")
                if bool(getattr(settings, "markdown_button_status", False)):
                    message = self.message_segment.markdown_keyboard(
                        bot, md_body, self._welcome_buttons()
                    )
                else:
                    message = self.message_segment.markdown(bot, self._welcome_md_text(msg))
                if await self._send_lifecycle_message(
                    bot, event, group_id, message, kind=f"{kind}/md"
                ):
                    return
            except Exception as exc:
                logger.warning(f"[{kind}] 构造/发送 MD 失败 group={group_id}: {exc}")
            try:
                await self.send_fallback(
                    bot,
                    event,
                    msg,
                    md_type="修仙",
                    k1="我要修仙",
                    v1="我要修仙",
                    k2="修仙帮助",
                    v2="修仙帮助",
                    k3="娱乐帮助",
                    v3="娱乐帮助",
                    k4="关闭欢迎",
                    v4="关闭进群欢迎",
                    at_msg=False,
                )
                logger.info(f"[{kind}] 已发送 group={group_id} via=handle_send(md)")
                return
            except Exception as exc:
                logger.warning(f"[{kind}] handle_send(md) 失败 group={group_id}: {exc}")

        if await self._send_lifecycle_message(
            bot, event, group_id, plain, kind=f"{kind}/plain"
        ):
            return
        try:
            await self.send_fallback(bot, event, plain, at_msg=False)
            logger.info(f"[{kind}] 已发送 group={group_id} via=handle_send(plain)")
        except Exception as exc:
            logger.warning(f"[{kind}] 发送失败 group={group_id}: {exc}")
            return

    async def handle(self, bot: Any, event: Any) -> GroupLifecycleNoticeDecision:
        settings = self.settings_provider()
        group_id = _extract_group_id(event)
        if is_lifecycle_event(event):
            # The event preprocessor may already have applied this lifecycle
            # event. Reuse its result so state counters and transitions happen
            # once per delivered event.
            result = getattr(event, "xiuxian_lifecycle_result", None)
            if result is None:
                result = self.lifecycle_applier(bot, event)
            action = result.context.action
            group_id = result.context.group_id or group_id
        else:
            action = _legacy_action(event)

        if not group_id:
            return GroupLifecycleNoticeDecision(action=action)

        if action == "bot_leave_group":
            if self.admin_config.set_full_message_group(group_id, enabled=False):
                logger.info(f"[全量群] bot退群取消标记 group={group_id}")
            return GroupLifecycleNoticeDecision(action=action, group_id=group_id)

        if action not in {"bot_join_group", "member_join_group"}:
            return GroupLifecycleNoticeDecision(action=action, group_id=group_id)
        if not self._group_allowed(bot, group_id, settings):
            logger.debug(f"[进群欢迎] 非响应群，已忽略 group={group_id} action={action}")
            return GroupLifecycleNoticeDecision(action=action, group_id=group_id)
        if not self._welcome_enabled(group_id, settings):
            return GroupLifecycleNoticeDecision(action=action, group_id=group_id)

        bot_join = action == "bot_join_group"
        kind = "Bot入驻" if bot_join else "成员欢迎"
        logger.info(
            f"[{kind}] group={group_id} event={_event_type_name(event)}"
        )
        await self._send_notice(
            bot,
            event,
            group_id,
            (
                self._bot_join_text(settings)
                if bot_join
                else self._member_welcome_text(settings)
            ),
            kind=kind,
            settings=settings,
        )
        return GroupLifecycleNoticeDecision(
            action=action,
            group_id=group_id,
            finish_matcher=True,
        )


__all__ = ["GroupLifecycleNoticeApplication", "GroupLifecycleNoticeDecision"]
