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

config = DATA_CONFIG["礼包"]


add_gift_cmd = on_command("新增礼包", permission=SUPERUSER, priority=5, block=True)
delete_gift_cmd = on_command("删除礼包", permission=SUPERUSER, priority=5, block=True)
clear_gift_cmd = on_command("清空礼包", permission=SUPERUSER, priority=5, block=True)

list_gift_cmd = on_command("礼包列表", priority=5, block=True)
claim_gift_cmd = on_command("领取礼包", priority=5, block=True)
help_gift_cmd = on_command("礼包帮助", priority=7, block=True)
admin_help_gift_cmd = on_command("礼包管理", permission=SUPERUSER, priority=5, block=True)


@add_gift_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
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
        await handle_send(bot, event, f"新增礼包失败：{e}")


@delete_gift_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)

    gift_id = args.extract_plain_text().strip()

    if not gift_id:
        await handle_send(bot, event, "请指定要删除的礼包ID")
        return

    delete_record(gift_id, config)
    await handle_send(bot, event, f"已删除礼包 {gift_id} 及其领取记录")


@clear_gift_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)

    clear_records(config)
    await handle_send(bot, event, "已清空所有礼包及领取记录")


@list_gift_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)

    await list_normal_rewards(bot, event, config)


@claim_gift_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)

    gift_id = args.extract_plain_text().strip()

    if not gift_id:
        await handle_send(bot, event, "请指定要领取的礼包ID")
        return

    await claim_normal_reward(bot, event, config, gift_id)


@help_gift_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    await handle_send(bot, event, GIFT_HELP)


@admin_help_gift_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)
    await handle_send(bot, event, GIFT_ADMIN_HELP)


GIFT_HELP = """
🎁 礼包系统帮助 🎁
═════════════
【用户命令】
1. 领取礼包 [礼包ID]
2. 礼包列表

说明：
- 每个礼包每位用户只能领取一次。
- 礼包可以设置生效时间和过期时间。
""".strip()


GIFT_ADMIN_HELP = """
👑 礼包管理帮助 👑
═════════════
【管理员命令】
1. 新增礼包 [礼包ID] [物品数据] [原因] [有效期] [生效期]
   示例：
   新增礼包 0 灵石x100000,渡厄丹x1 新人礼包 无限 0

2. 删除礼包 [礼包ID]

3. 清空礼包
""".strip()