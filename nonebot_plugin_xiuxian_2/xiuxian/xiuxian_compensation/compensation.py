from nonebot import on_command
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER

from ..adapter_compat import Bot, Message, MessageEvent, GroupMessageEvent, PrivateMessageEvent
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import handle_send

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


@add_compensation_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
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


@delete_compensation_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)

    comp_id = args.extract_plain_text().strip()

    if not comp_id:
        await handle_send(bot, event, "请指定要删除的补偿ID")
        return

    delete_record(comp_id, config)
    await handle_send(bot, event, f"已删除补偿 {comp_id} 及其领取记录")


@clear_compensation_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)

    clear_records(config)
    await handle_send(bot, event, "已清空所有补偿及领取记录")


@list_compensation_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)

    await list_normal_rewards(bot, event, config)


@claim_compensation_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)

    comp_id = args.extract_plain_text().strip()

    if not comp_id:
        await handle_send(bot, event, "请指定要领取的补偿ID")
        return

    await claim_normal_reward(bot, event, config, comp_id)


@help_compensation_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    await handle_send(bot, event, COMPENSATION_HELP)


@admin_help_compensation_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)
    await handle_send(bot, event, COMPENSATION_ADMIN_HELP)


COMPENSATION_HELP = """
🛠️ 补偿系统帮助 🛠️
═════════════
【用户命令】
1. 领取补偿 [补偿ID]
2. 补偿列表

说明：
- 每个补偿每位用户只能领取一次。
- 补偿可以设置生效时间和过期时间。
""".strip()


COMPENSATION_ADMIN_HELP = """
👑 补偿管理帮助 👑
═════════════
【管理员命令】
1. 新增补偿 [补偿ID] [物品数据] [原因] [有效期] [生效期]
   示例：
   新增补偿 0 灵石x100000,渡厄丹x5 维护补偿 30天 0

2. 删除补偿 [补偿ID]

3. 清空补偿
""".strip()