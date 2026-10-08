from nonebot.adapters import Event as BaseEvent
from nonebot.matcher import Matcher
from nonebot.params import EventPlainText
from nonebot.rule import Rule

from ..adapter_compat import Bot, GroupMessageEvent, PrivateMessageEvent
from ..on_compat import on_message
from ..xiuxian_utils.lay_out import Cooldown
from ...features.fallback.application import empty_fallback_application


def _fallback_rule() -> Rule:
    async def _checker(event: BaseEvent, text: str = EventPlainText()) -> bool:
        return empty_fallback_application.should_respond(event, text)

    return Rule(_checker)


empty_fallback = on_message(priority=999, block=False, rule=_fallback_rule())


@empty_fallback.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_empty_fallback(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    matcher: Matcher,
):
    await empty_fallback_application.respond(bot, event)
    await matcher.finish()
