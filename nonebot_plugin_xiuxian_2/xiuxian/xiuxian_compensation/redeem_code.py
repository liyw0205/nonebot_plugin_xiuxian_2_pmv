from datetime import datetime

from ..on_compat import on_command
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER

from ..adapter_compat import Bot, Message, MessageEvent, GroupMessageEvent, PrivateMessageEvent
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import check_user, handle_send, send_msg_handler, send_help_message

from .common import (
    DATA_CONFIG,
    create_reward_record,
    delete_record,
    clear_records,
    load_data,
    has_claimed,
    is_expired,
    is_not_started,
    format_reward_delivery,
    create_item_message,
    reward_claim_service,
)

config = DATA_CONFIG["兑换码"]


add_redeem_cmd = on_command("新增兑换码", permission=SUPERUSER, priority=5, block=True)
delete_redeem_cmd = on_command("删除兑换码", permission=SUPERUSER, priority=5, block=True)
clear_redeem_cmd = on_command("清空兑换码", permission=SUPERUSER, priority=5, block=True)
list_redeem_cmd = on_command("兑换码列表", permission=SUPERUSER, priority=5, block=True)
admin_help_redeem_cmd = on_command("兑换码管理", permission=SUPERUSER, priority=5, block=True)

redeem_cmd = on_command("兑换", priority=10, block=True)
help_redeem_cmd = on_command("兑换码帮助", priority=7, block=True)


@add_redeem_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)

    try:
        await create_reward_record(
            bot,
            event,
            config,
            args.extract_plain_text().strip(),
            is_redeem_code=True,
        )
    except Exception as e:
        await handle_send(bot, event, f"新增兑换码失败：{e}")


@delete_redeem_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)

    code = args.extract_plain_text().strip()

    if not code:
        await handle_send(bot, event, "请指定要删除的兑换码")
        return

    delete_record(code, config)
    await handle_send(bot, event, f"已删除兑换码 {code} 及其使用记录")


@clear_redeem_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)

    clear_records(config)
    await handle_send(bot, event, "已清空所有兑换码及使用记录")


@list_redeem_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent):
    """
    兑换码列表仅管理员可见。
    """
    await assign_bot(bot=bot, event=event)
    await send_redeem_code_list(bot, event)


@redeem_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    普通用户只能通过：兑换 [兑换码]
    """
    await assign_bot(bot=bot, event=event)

    is_user, user_info, msg = check_user(event)

    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    code = args.extract_plain_text().strip()

    if not code:
        await handle_send(bot, event, "请指定要兑换的兑换码")
        return

    data = load_data(config)
    redeem_info = data.get(code)

    if not redeem_info:
        if reward_claim_service.has_claimed(config["type_key"], code, user_id):
            await handle_send(bot, event, f"你已经使用过兑换码 {code}\n该兑换请求已经处理，无需重复提交。")
            return
        await handle_send(bot, event, "兑换码不存在")
        return

    if is_expired(redeem_info):
        if reward_claim_service.has_claimed(config["type_key"], code, user_id):
            await handle_send(bot, event, f"你已经使用过兑换码 {code}\n该兑换请求已经处理，无需重复提交。")
            return
        await handle_send(bot, event, "该兑换码已过期")
        return

    if is_not_started(redeem_info):
        await handle_send(
            bot,
            event,
            f"该兑换码尚未生效，生效时间：{redeem_info.get('start_time')}",
        )
        return

    usage_limit = redeem_info.get("usage_limit", 0)
    legacy_used_count = redeem_info.get("used_count", 0)
    # 先 claim：成功后 has_claimed/used_count 会挡住同事件重放。
    result = reward_claim_service.claim(
        config["type_key"],
        code,
        user_id,
        redeem_info["items"],
        usage_limit=usage_limit,
        legacy_used_count=legacy_used_count,
    )
    if result.status == "duplicate":
        reward_msg = format_reward_delivery(redeem_info["items"])
        await handle_send(
            bot,
            event,
            f"兑换成功\n"
            f"兑换码：{code}\n"
            f"奖励：\n" + "\n".join(f"- {line}" for line in reward_msg)
            + "\n该兑换请求已经处理，无需重复提交。",
        )
        return
    if result.status == "exhausted":
        await handle_send(bot, event, "该兑换码已被使用完")
        return
    if result.status != "claimed":
        await handle_send(bot, event, f"你已经使用过兑换码 {code}")
        return
    reward_msg = format_reward_delivery(redeem_info["items"])

    await handle_send(
        bot,
        event,
        f"兑换成功\n"
        f"兑换码：{code}\n"
        f"奖励：\n" + "\n".join(f"- {line}" for line in reward_msg),
    )


@help_redeem_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    await send_help_message(
        bot, event, REDEEM_HELP,
        k1="兑换", v1="兑换",
        k2="礼包", v2="礼包帮助",
        k3="补偿", v3="补偿帮助"
    )


@admin_help_redeem_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)
    await send_help_message(
        bot, event, REDEEM_ADMIN_HELP,
        k1="新增", v1="新增兑换码",
        k2="列表", v2="兑换码列表",
        k3="清空", v3="清空兑换码"
    )


async def send_redeem_code_list(bot: Bot, event: MessageEvent):
    data = load_data(config)

    if not data:
        await handle_send(bot, event, "当前没有兑换码")
        return

    current_time = datetime.now()

    valid = []
    not_started = []
    expired = []

    for code, info in data.items():
        if is_not_started(info):
            not_started.append((code, info))
        elif is_expired(info):
            expired.append((code, info))
        else:
            valid.append((code, info))

    lines = [
        "兑换码列表",
        f"更新时间：{current_time.strftime('%Y-%m-%d %H:%M:%S')}",
    ]

    def append_codes(title: str, codes: list):
        lines.append("")
        lines.append(f"【{title}】")

        if not codes:
            lines.append("暂无")
            return

        for code, info in codes:
            item_msg = create_item_message(info["items"])
            usage_limit = info.get("usage_limit", 0)
            used_count = reward_claim_service.get_used_count(
                config["type_key"], code, info.get("used_count", 0)
            )

            usage_text = "无限次" if usage_limit == 0 else f"{usage_limit}次"

            lines.extend([
                f"- 兑换码：{code}",
                f"内容：{', '.join(item_msg)}",
                f"使用情况：{used_count}/{usage_text}",
                f"有效期至：{info.get('expire_time')}",
                f"生效时间：{info.get('start_time')}",
                f"创建时间：{info.get('create_time')}",
            ])

    append_codes("有效", valid)
    append_codes("尚未生效", not_started)
    append_codes("过期", expired)

    await send_msg_handler(
        bot,
        event,
        "兑换码列表",
        bot.self_id,
        lines,
        title="兑换码列表",
    )


REDEEM_HELP = """
**兑换码帮助**
---
**指令**
- 兑换 [兑换码]

> 兑换码只能通过“兑换”命令使用。
> 每个用户对同一个兑换码只能使用一次。
> 可能有使用次数、生效时间和过期时间限制。
""".strip()


REDEEM_ADMIN_HELP = """
兑换码管理帮助

管理指令
- 新增兑换码 [兑换码] [物品数据] [使用上限] [有效期] [生效期]
- 删除兑换码 [兑换码]
- 清空兑换码
- 兑换码列表

示例
新增兑换码 XMAS2024 灵石x1000000,渡厄丹x1 100 30天 0

规则
- 使用上限为 0 表示无限次。
- 兑换码列表只有管理员可以查看。
- 普通用户不能领取兑换码，只能使用
> 兑换 [兑换码]
""".strip()
