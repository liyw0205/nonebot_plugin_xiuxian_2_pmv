from ..on_compat import on_command
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER

from ..adapter_compat import Bot, Message, MessageEvent, GroupMessageEvent, PrivateMessageEvent
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import handle_send, send_help_message

from .common import (
    DATA_CONFIG,
    create_reward_record,
    claim_normal_reward,
    delete_record,
    clear_records,
    list_normal_rewards,
)

config = DATA_CONFIG["补偿"]


add_compensation_cmd = on_command("新增补偿", permission=SUPERUSER, priority=5, block=True)
delete_compensation_cmd = on_command("删除补偿", permission=SUPERUSER, priority=5, block=True)
clear_compensation_cmd = on_command("清空补偿", permission=SUPERUSER, priority=5, block=True)

list_compensation_cmd = on_command("补偿列表", priority=5, block=True)
claim_compensation_cmd = on_command("领取补偿", priority=5, block=True)
help_compensation_cmd = on_command("补偿帮助", priority=7, block=True)
admin_help_compensation_cmd = on_command("补偿管理", permission=SUPERUSER, priority=5, block=True)


@add_compensation_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)

    try:
        await create_reward_record(
            bot,
            event,
            config,
            args.extract_plain_text().strip(),
            is_redeem_code=False,
        )
    except Exception as e:
        await handle_send(bot, event, f"新增补偿失败：{e}")


@delete_compensation_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)

    comp_id = args.extract_plain_text().strip()

    if not comp_id:
        await handle_send(bot, event, "请指定要删除的补偿ID")
        return

    result = delete_record(comp_id, config)
    if not result.succeeded:
        await handle_send(bot, event, "补偿定义已变化，请重新执行删除")
        return
    await handle_send(bot, event, f"已删除补偿 {comp_id} 及其领取记录")


@clear_compensation_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)

    result = clear_records(config)
    if not result.succeeded:
        await handle_send(bot, event, "补偿列表已变化，请重新执行清空")
        return
    await handle_send(bot, event, "已清空所有补偿及领取记录")


@list_compensation_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)

    await list_normal_rewards(bot, event, config)


@claim_compensation_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)

    comp_id = args.extract_plain_text().strip()

    if not comp_id:
        await handle_send(bot, event, "请指定要领取的补偿ID")
        return

    await claim_normal_reward(bot, event, config, comp_id)


@help_compensation_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    await send_help_message(
        bot, event, COMPENSATION_HELP,
        k1="领取", v1="领取补偿",
        k2="列表", v2="补偿列表",
        k3="礼包", v3="礼包帮助"
    )


@admin_help_compensation_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)
    await send_help_message(
        bot, event, COMPENSATION_ADMIN_HELP,
        k1="新增", v1="新增补偿",
        k2="删除", v2="删除补偿",
        k3="清空", v3="清空补偿"
    )


COMPENSATION_HELP = """
**补偿帮助**
---
**指令**
- 领取补偿 [补偿ID]
- 补偿列表

> 每个补偿每位用户只能领取一次。
> 补偿可能设置生效时间和过期时间。
""".strip()


COMPENSATION_ADMIN_HELP = """
补偿管理帮助

管理指令
- 新增补偿 [补偿ID] [物品数据] [原因] [有效期] [生效期]
- 删除补偿 [补偿ID]
- 清空补偿

示例
新增补偿 0 灵石x100000,渡厄丹x5 维护补偿 30天 0
""".strip()
