from typing import Any, Tuple

from ...paths import get_paths
from ...infrastructure.ids import UUIDGenerator
from ..on_compat import on_regex
from nonebot.log import logger
from nonebot.params import RegexGroup
from ..adapter_compat import (
    Bot,
    GroupMessageEvent,
    PrivateMessageEvent,
    GROUP,
    MessageSegment,
)
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from .bankconfig import get_config
from ..xiuxian_utils.utils import check_user, get_msg_pic, handle_send, send_help_message
from ..xiuxian_config import XiuConfig
from ...features.bank.application import BankApplication


config = get_config()
BANKLEVEL = config["BANKLEVEL"]
bank_application = BankApplication(get_paths().game_db)
runtime_ids = UUIDGenerator()
PLAYERSDATA = get_paths().players


bank = on_regex(
    r'^灵庄(存灵石|取灵石|升级会员|信息|结算)?(.*)?',
    priority=9,    
    block=True
)

__bank_help__ = """
**灵庄帮助**
---
**存取**
- 灵庄存灵石 [金额]
> 存入灵石获取利息
- 灵庄取灵石 [金额]
> 取出灵石（自动结算利息）

**会员**
- 灵庄升级会员
> 提升会员等级，增加利息倍率

**查询**
- 灵庄信息
> 查看余额和会员信息
- 灵庄结算
> 手动结算当前利息

> 利息按小时计算；会员等级越高收益越高；存取时会自动结算利息。
""".strip()


@bank.handle(parameterless=[Cooldown(cd_time=0)])
async def bank_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Tuple[Any, ...] = RegexGroup()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await bank.finish()
    mode = args[0]  # 存灵石、取灵石、升级会员、信息查看
    num = args[1]  # 数值
    if mode is None:
        msg = __bank_help__
        await send_help_message(bot, event, msg, k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
        await bank.finish()

    if mode == '存灵石' or mode == '取灵石':
        try:
            num = int(num)
            if num <= 0:
                msg = f"请输入正确的金额！"
                await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
                await bank.finish()
        except ValueError:
            msg = f"请输入正确的金额！"
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()
    user_id = user_info['user_id']

    if mode == '存灵石':  # 存灵石逻辑
        event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
        operation_id = f"bank-deposit:{event_id}:{user_id}" if event_id else f"bank-deposit:{user_id}:{runtime_ids.new_id()}"
        from ...features.bank.account_info_application import BankAccountInfoApplication
        from ...features.bank.account_application import BankDepositApplication
        from ...features.bank.clock import bank_clock

        from ...compatibility.legacy_bank_operation_receipts import LegacyBankOperationReceiptRepository

        prior = LegacyBankOperationReceiptRepository(get_paths().game_db).get_deposit_result(operation_id)
        if prior is not None:
            msg = (
                f"道友本次结息时间为：已处理，获得灵石：{prior['interest']}枚!\n"
                f"道友存入灵石{prior['deposited']}枚，当前所拥有灵石{prior['wallet_stone']}枚，灵庄存有灵石{prior['saved_stone']}枚\n"
                "该存款请求已经处理，无需重复提交。"
            )
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()

        migrated_account = BankAccountInfoApplication(get_paths().game_db).get_info(user_id=user_id)
        account_status = str(migrated_account.get("status"))
        if account_status not in {"ok", "account_missing"}:
            message = "未找到修仙数据，本次存款未结算。" if account_status == "user_missing" else "灵庄账户暂不可用。"
            await handle_send(bot, event, message, md_type="灵庄")
            await bank.finish()

        bank_level = str(migrated_account.get("bank_level", "1"))
        now = bank_clock().now()
        initial_account = None if account_status == "ok" else {
            "saved_stone": 0,
            "bank_level": "1",
            "updated_at": now.isoformat(),
        }
        result = BankDepositApplication(get_paths().game_db).deposit(
            operation_id=operation_id,
            user_id=user_id,
            amount=num,
            interest=0,
            limit=int(BANKLEVEL[bank_level]["savemax"]),
            bank_level=bank_level,
            settled_at=now.isoformat(),
            initial_account=initial_account,
        )
        messages = {
            "applied": f"灵庄存款成功：存入 {result['deposited']} 枚，当前存款 {result['saved_stone']} 枚。",
            "duplicate": "该存款请求已经处理，无需重复提交。",
            "stone_insufficient": "灵石不足，存款未结算。",
            "limit_exceeded": "超过灵庄存储上限，存款未结算。",
            "state_changed": "灵庄操作失败：账户当前状态已更新，本次未结算。",
            "operation_conflict": "请求冲突，存款未结算。",
            "user_missing": "未找到修仙数据。",
        }
        await handle_send(bot, event, messages.get(str(result.get("status")), "灵庄存款未结算。"), md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
        await bank.finish()

    elif mode == '取灵石':  # 取灵石逻辑
        event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "")).strip()
        operation_id = f"bank-withdrawal:{event_id}:{user_id}" if event_id else f"bank-withdrawal:{user_id}:{runtime_ids.new_id()}"
        from ...features.bank.account_info_application import BankAccountInfoApplication
        from ...features.bank.account_withdrawal_application import BankWithdrawalApplication
        from ...features.bank.clock import bank_clock

        from ...compatibility.legacy_bank_operation_receipts import LegacyBankOperationReceiptRepository

        prior = LegacyBankOperationReceiptRepository(get_paths().game_db).get_withdrawal_result(operation_id)
        if prior is not None:
            msg = (
                f"道友本次结息时间为：已处理，获得灵石：{prior['interest']}枚!\n"
                f"取出灵石{prior['withdrawn']}枚，当前所拥有灵石{prior['wallet_stone']}枚，灵庄存有灵石{prior['saved_stone']}枚!\n"
                "该取款请求已经处理，无需重复提交。"
            )
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()

        migrated_account = BankAccountInfoApplication(get_paths().game_db).get_info(user_id=user_id)
        account_status = str(migrated_account.get("status"))
        if account_status != "ok":
            message = "灵庄暂无存款，无法取款。" if account_status == "account_missing" else "未找到修仙数据，本次取款未结算。"
            await handle_send(bot, event, message, md_type="灵庄")
            await bank.finish()

        result = BankWithdrawalApplication(get_paths().game_db).withdraw(
            operation_id=operation_id,
            user_id=user_id,
            amount=num,
            interest=0,
            bank_level=str(migrated_account["bank_level"]),
            settled_at=bank_clock().now().isoformat(),
        )
        messages = {
            "applied": f"灵庄取款成功：取出 {result['withdrawn']} 枚，当前存款 {result['saved_stone']} 枚。",
            "duplicate": "该取款请求已经处理，无需重复提交。",
            "saved_stone_insufficient": "灵庄存款不足，取款未结算。",
            "state_changed": "灵庄操作失败：账户当前状态已更新，本次未结算。",
            "operation_conflict": "请求冲突，取款未结算。",
            "user_missing": "未找到修仙数据。",
        }
        await handle_send(bot, event, messages.get(str(result.get("status")), "灵庄取款未结算。"), md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
        await bank.finish()

    elif mode == '升级会员':  # 升级会员逻辑
        event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
        operation_id = f"bank-upgrade:{event_id}:{user_id}" if event_id else f"bank-upgrade:{user_id}:{runtime_ids.new_id()}"
        from ...features.bank.account_info_application import BankAccountInfoApplication
        from ...features.bank.account_upgrade_application import BankUpgradeApplication
        from ...features.bank.clock import bank_clock

        from ...compatibility.legacy_bank_operation_receipts import LegacyBankOperationReceiptRepository

        migrated_account = BankAccountInfoApplication(get_paths().game_db).get_info(user_id=user_id)
        account_status = str(migrated_account.get("status"))
        if account_status not in {"ok", "account_missing"}:
            message = "未找到修仙数据，本次会员升级未结算。" if account_status == "user_missing" else "灵庄账户暂不可用。"
            await handle_send(bot, event, message, md_type="灵庄")
            await bank.finish()

        initial_account = None
        if account_status == "account_missing":
            prior = LegacyBankOperationReceiptRepository(get_paths().game_db).get_upgrade_result(operation_id)
            if prior is not None:
                msg = (
                    f"道友成功升级灵庄会员等级，消耗灵石{prior['cost']}枚，当前为：{BANKLEVEL[prior['bank_level']]['level']}，"
                    f"灵庄可存有灵石上限{BANKLEVEL[prior['bank_level']]['savemax']}枚\n"
                    "该升级请求已经处理，无需重复提交。"
                )
                await handle_send(bot, event, msg, md_type="灵庄", k1="升级", v1="灵庄升级会员", k2="信息", v2="灵庄信息", k3="帮助", v3="灵庄帮助")
                await bank.finish()
            initial_account = {"saved_stone": 0, "bank_level": "1", "updated_at": ""}

        userlevel = str(migrated_account.get("bank_level", "1"))
        if userlevel == str(len(BANKLEVEL)):
            msg = f"道友已经是本灵庄最大的会员啦！"
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()

        stonecost = BANKLEVEL[f"{int(userlevel)}"]['levelup']
        next_level = f"{int(userlevel) + 1}"
        result = BankUpgradeApplication(get_paths().game_db).upgrade(
            operation_id=operation_id,
            user_id=user_id,
            expected_level=userlevel,
            next_level=next_level,
            cost=stonecost,
            settled_at=bank_clock().now().isoformat(),
            initial_account=initial_account,
        )
        status = str(result.get("status", "failed"))
        if status == "applied":
            msg = f"道友成功升级灵庄会员等级，消耗灵石{result['cost']}枚，当前为：{BANKLEVEL[result['bank_level']]['level']}，灵庄可存有灵石上限{BANKLEVEL[result['bank_level']]['savemax']}枚"
        elif status == "duplicate":
            msg = "该升级请求已经处理，无需重复提交。"
        elif status == "stone_insufficient":
            msg = "灵石不足，会员升级未结算。"
        elif status in {"state_changed", "operation_conflict"}:
            msg = "灵庄会员升级失败：账户当前状态已更新，本次未结算。"
        elif status == "user_missing":
            msg = "未找到修仙数据，本次会员升级未结算。"
        else:
            msg = "灵庄会员升级未完成，请刷新账户状态后重试。"

        await handle_send(bot, event, msg, md_type="灵庄", k1="升级", v1="灵庄升级会员", k2="信息", v2="灵庄信息", k3="帮助", v3="灵庄帮助")
        await bank.finish()

    elif mode == '信息':  # 查询灵庄信息
        from ...features.bank.account_info_application import BankAccountInfoApplication

        new_info = BankAccountInfoApplication(get_paths().game_db).get_info(user_id=user_id)
        if new_info.get("status") == "user_missing":
            await handle_send(bot, event, "未找到修仙数据。", md_type="我要修仙")
            await bank.finish()
        bank_level = str(new_info.get("bank_level", "1"))
        saved_stone = int(new_info.get("saved_stone", 0) or 0)
        updated_at = str(new_info.get("updated_at") or "尚未存入")
        msg = f'''**灵庄信息**
---
已存
> {saved_stone}灵石
存入时间
> {updated_at}
会员等级
> {BANKLEVEL[bank_level]['level']}
当前灵石
> {new_info['wallet_stone']}
存储上限
> {BANKLEVEL[bank_level]['savemax']}枚
'''
        await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="结算", v3="灵庄结算")
        await bank.finish()

    elif mode == '结算':
        event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "")).strip()
        operation_id = f"bank-interest:{event_id}:{user_id}" if event_id else f"bank-interest:{user_id}:{runtime_ids.new_id()}"
        from ...features.bank.account_info_application import BankAccountInfoApplication
        from ...features.bank.account_interest_application import BankInterestApplication
        from ...features.bank.interest_rules import calculate_interest
        from ...features.bank.clock import bank_clock

        from ...compatibility.legacy_bank_operation_receipts import LegacyBankOperationReceiptRepository

        migrated_account = BankAccountInfoApplication(get_paths().game_db).get_info(user_id=user_id)
        account_status = str(migrated_account.get("status"))
        if account_status not in {"ok", "account_missing"}:
            message = "未找到修仙数据，本次结息未处理。" if account_status == "user_missing" else "灵庄账户暂不可用。"
            await handle_send(bot, event, message, md_type="灵庄")
            await bank.finish()

        now = bank_clock().now()
        level = str(migrated_account.get("bank_level", "1"))
        saved_stone = int(migrated_account.get("saved_stone", 0) or 0)
        saved_at = str(migrated_account.get("updated_at") or now.isoformat())
        initial_account = None if account_status == "ok" else {
            "saved_stone": 0,
            "bank_level": "1",
            "updated_at": saved_at,
        }
        if account_status == "account_missing":
            prior = LegacyBankOperationReceiptRepository(get_paths().game_db).get_interest_result(operation_id)
            if prior is not None:
                msg = f"**灵庄结息**\n---\n✅ 结息成功\n获得灵石\n> {prior['interest']}枚\n该结息请求已经处理，无需重复提交。"
                await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
                await bank.finish()
        interest, hours = calculate_interest(
            saved_stone=saved_stone,
            saved_at=saved_at,
            settled_at=now,
            rate=float(BANKLEVEL[level]["interest"]),
        )
        result = BankInterestApplication(get_paths().game_db).settle_interest(
            operation_id=operation_id,
            user_id=user_id,
            interest=interest,
            bank_level=level,
            settled_at=now.strftime("%Y-%m-%d %H:%M:%S"),
            initial_account=initial_account,
        )
        status = str(result.get("status", "failed"))
        if status == "applied":
            msg = f"**灵庄结息**\n---\n✅ 结息成功\n结息时间\n> {hours}小时\n获得灵石\n> {result['interest']}枚"
        elif status == "duplicate":
            msg = "**灵庄结息**\n---\n✅ 结息成功\n该结息请求已经处理，无需重复提交。"
        elif status in {"state_changed", "operation_conflict"}:
            msg = "灵庄结息失败：账户当前状态已更新，本次未处理。"
        elif status == "user_missing":
            msg = "未找到修仙数据，本次结息未处理。"
        else:
            msg = "灵庄结息未完成，请刷新账户状态后重试。"
        await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
        await bank.finish()


def savef(user_id, data):
    from ...compatibility.legacy_bank_account_storage import savef as legacy_savef

    return legacy_savef(user_id, data)
