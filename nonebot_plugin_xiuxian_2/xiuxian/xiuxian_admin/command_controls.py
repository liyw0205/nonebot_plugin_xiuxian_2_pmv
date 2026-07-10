try:
    import ujson as json
except ImportError:
    import json
import re
from urllib.parse import quote

from nonebot.log import logger
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER

from ..adapter_compat import Bot, GroupMessageEvent, Message, MessageSegment, PrivateMessageEvent
from ..command_disable import apply_disable_targets, format_command_list_page
from ..messaging.delivery import delivery_service
from ..on_compat import on_command, rebuild_on_compat_index
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.lay_out import Cooldown, assign_bot
from ..xiuxian_utils.utils import build_pagination_buttons, handle_send, send_help_message


cmd_disable = on_command("指令禁用", permission=SUPERUSER, priority=5, block=True)
cmd_enable = on_command("指令解禁", permission=SUPERUSER, priority=5, block=True)
cmd_list = on_command("指令列表", permission=SUPERUSER, priority=5, block=True)
all_apply_cmd = on_command("全量申请", priority=5, block=True)


@cmd_disable.handle(parameterless=[Cooldown(cd_time=0)])
async def cmd_disable_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    args: Message = CommandArg(),
):
    bot, _ = await assign_bot(bot=bot, event=event)
    raw = args.extract_plain_text().strip()
    changed, errors = apply_disable_targets(raw, disabled=True)
    rebuild_on_compat_index()
    lines: list[str] = []
    if changed:
        lines.append(f"已禁用：{', '.join(changed)}")
    if errors:
        lines.append("未处理：" + "；".join(errors))
    if not lines:
        lines.append("用法：指令禁用 测试,灵石,xiuxian_arena")
    await handle_send(bot, event, "\n".join(lines))
    await cmd_disable.finish()


@cmd_enable.handle(parameterless=[Cooldown(cd_time=0)])
async def cmd_enable_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    args: Message = CommandArg(),
):
    bot, _ = await assign_bot(bot=bot, event=event)
    raw = args.extract_plain_text().strip()
    changed, errors = apply_disable_targets(raw, disabled=False)
    rebuild_on_compat_index()
    lines: list[str] = []
    if changed:
        lines.append(f"已解禁：{', '.join(changed)}")
    if errors:
        lines.append("未处理：" + "；".join(errors))
    if not lines:
        lines.append("用法：指令解禁 测试,灵石,xiuxian_arena")
    await handle_send(bot, event, "\n".join(lines))
    await cmd_enable.finish()


def _parse_command_list_args(raw: str) -> tuple[bool, int, str]:
    text = (raw or "").strip()
    if not text:
        return False, 1, ""
    tokens = text.split()
    only_disabled = False
    page = 1
    filter_parts: list[str] = []
    for token in tokens:
        if token == "禁用":
            only_disabled = True
            continue
        if re.fullmatch(r"\d+", token):
            page = max(int(token), 1)
            continue
        filter_parts.append(token)
    raw_filter = " ".join(filter_parts).replace(" ", ",")
    return only_disabled, page, raw_filter


@cmd_list.handle(parameterless=[Cooldown(cd_time=0)])
async def cmd_list_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    args: Message = CommandArg(),
):
    bot, _ = await assign_bot(bot=bot, event=event)
    raw = args.extract_plain_text().strip()
    only_disabled, page, raw_filter = _parse_command_list_args(raw)
    msg, page, total_pages = format_command_list_page(
        raw_filter,
        only_disabled=only_disabled,
        page=page,
        per_page=30,
    )
    list_cmd = "指令列表 禁用" if only_disabled else "指令列表"
    if raw_filter.strip():
        list_cmd = f"{list_cmd} {raw_filter.strip().replace(',', ' ')}"
    button_kwargs = build_pagination_buttons(list_cmd, page, total_pages)
    await send_help_message(bot, event, msg, **button_kwargs)
    await cmd_list.finish()


@all_apply_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def all_apply_cmd_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    args: Message = CommandArg(),
):
    bot, _ = await assign_bot(bot=bot, event=event)

    group_id_str = args.extract_plain_text().strip()
    if not group_id_str or not group_id_str.isdigit():
        await handle_send(bot, event, f"用法：全量申请 [目标群号]\n示例：全量申请 {XiuConfig().qqq}")
        return

    config = XiuConfig()
    bot_uin = getattr(config, "bot_uin", 0)
    bot_uid = getattr(config, "bot_uid", "")

    if not bot_uin or not bot_uid:
        await handle_send(bot, event, "错误：请先在 xiu 配置中设置 bot_uin 和 bot_uid！")
        return

    info_dict = {
        "page_name": "ai_group_service_agreement_pop_page",
        "groupCode": int(group_id_str),
        "botUin": int(bot_uin),
        "botUid": str(bot_uid),
        "screen": 1,
    }
    info_json = json.dumps(info_dict, separators=(",", ":"))
    encoded_info = quote(info_json)
    target_url = f"https://club.vip.qq.com/transfer?open_kuikly_info={encoded_info}"

    md_text = (
        "**全量申请授权**\n"
        "> 请群主点击下方按钮授权\n"
        "> 需要更新 QQ 到最新版（9.2.90 及以上）"
    )
    rows = [[("群主大大请点击这里同意申请", target_url)]]

    try:
        msg = MessageSegment.markdown_keyboard(
            bot,
            md_text,
            rows,
            permission_type=3,
            specify_role_ids=["4"],
        )
        await delivery_service.reply(bot, event, msg)
    except Exception as e:
        logger.warning(f"全量申请：自定义键盘发送失败，尝试降级原生 MD: {e}")
        try:
            msg = MessageSegment.markdown(bot, md_text)
            await delivery_service.reply(bot, event, msg)
        except Exception as e2:
            logger.error(f"全量申请：Markdown 发送失败，降级纯文本: {e2}")
            fallback_msg = (
                "全量申请授权\n"
                "请群主点击下方链接完成授权。\n"
                "提示：需要更新 QQ 到最新版（9.2.90 及以上）。\n"
                f"授权链接：{target_url}"
            )
            await handle_send(bot, event, fallback_msg)
