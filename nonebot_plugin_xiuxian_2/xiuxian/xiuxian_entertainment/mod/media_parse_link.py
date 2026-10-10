"""链接解析：指令 + 正则（文案内嵌短链）+ 自动提链。"""
from ..command import *
from ..room_store import entertainment_application
from ....features.entertainment.media_parser_messages import (
    ANY_HTTP_RE,
    COMMAND_WITH_URL_RE,
    EMBEDDED_SHARE_MATCH_RE,
)

link_parse_cmd = on_command(
    "链接解析",
    aliases={"视频解析", "解析视频", "解析链接", "流媒体解析"},
    priority=5,
    block=True,
)


@link_parse_cmd.handle(parameterless=[Cooldown(cd_time=8)])
async def link_parse_cmd_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    args: Message = CommandArg(),
):
    arg_text = args.extract_plain_text().strip()
    if arg_text:
        await fun_media_send_parse_result(bot, event, arg_text)
        await link_parse_cmd.finish()
        return

    plain = strip_fun_media_parse_command_prefix(
        event.get_message().extract_plain_text()
    )
    if not plain:
        await handle_send(
            bot,
            event,
            "【链接解析】\n"
            "用法：链接解析 <分享链接>\n"
            "也可以直接发送含分享短链的整段文案，链接不必单独一行。",
            md_type="娱乐",
            k1="娱乐帮助",
            v1="娱乐帮助",
        )
        await link_parse_cmd.finish()
        return
    await fun_media_send_parse_result(bot, event, plain)
    await link_parse_cmd.finish()


# 正则：消息任意位置含「解析指令 + 链接」（链接后还可有其它字）
media_parse_cmd_url = on_regex(
    COMMAND_WITH_URL_RE.pattern,
    flags=re.I,
    priority=6,
    block=True,
)


@media_parse_cmd_url.handle(parameterless=[Cooldown(cd_time=8)])
async def media_parse_cmd_url_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    args: Tuple[Any, ...] = RegexGroup(),
):
    await entertainment_application.media_parser_messages.handle_command_url(bot, event)
    await media_parse_cmd_url.finish()


# 正则：整段文案内嵌分享短链（前后可有任意文字，不要求整条只有链接）
media_share_link_regex = on_regex(
    EMBEDDED_SHARE_MATCH_RE.pattern,
    flags=re.I | re.S,
    priority=10,
    block=True,
)


@media_share_link_regex.handle(parameterless=[Cooldown(cd_time=10)])
async def media_share_link_regex_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
):
    await entertainment_application.media_parser_messages.handle_embedded_share(
        bot, event
    )
    await media_share_link_regex.finish()


# 正则兜底：其它 http(s) 域名（仍从整段文案 search，非整句匹配）
media_any_http_regex = on_regex(
    rf".*({ANY_HTTP_RE.pattern}).*",
    flags=re.I | re.S,
    priority=11,
    block=True,
)


@media_any_http_regex.handle(parameterless=[Cooldown(cd_time=10)])
async def media_any_http_regex_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
):
    await entertainment_application.media_parser_messages.handle_any_http(bot, event)
    await media_any_http_regex.finish()
