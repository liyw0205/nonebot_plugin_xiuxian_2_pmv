try:
    import ujson as json
except ImportError:
    import json
import os
import time
from typing import Any, Tuple

from ...paths import get_paths
from ...infrastructure.clock import SystemClock
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
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager
from datetime import datetime
from .bankconfig import get_config
from ..xiuxian_utils.utils import check_user, get_msg_pic, handle_send, send_help_message
from ..xiuxian_config import XiuConfig
from .transaction_service import (
    BankDepositService,
    BankWithdrawalService,
    BankUpgradeService,
    BankInterestService,
)
from ...features.bank.application import BankApplication
from ...features.bank.repository import LegacyBankRepository

config = get_config()
BANKLEVEL = config["BANKLEVEL"]
sql_message = XiuxianDateManage()  # sql类
player_data_manager = PlayerDataManager()
_bank_deposit_service_instance = None
_bank_withdrawal_service_instance = None
_bank_upgrade_service_instance = None
_bank_interest_service_instance = None
bank_application = BankApplication(
    get_paths().game_db,
    get_paths().player_db,
    repository=LegacyBankRepository(get_paths().game_db, get_paths().player_db),
)
runtime_clock = SystemClock()
runtime_ids = UUIDGenerator()
PLAYERSDATA = get_paths().players


def _bank_deposit_service():
    global _bank_deposit_service_instance
    if _bank_deposit_service_instance is None:
        _bank_deposit_service_instance = BankDepositService(get_paths().game_db, get_paths().player_db)
    return _bank_deposit_service_instance


def _bank_withdrawal_service():
    global _bank_withdrawal_service_instance
    if _bank_withdrawal_service_instance is None:
        _bank_withdrawal_service_instance = BankWithdrawalService(get_paths().game_db, get_paths().player_db)
    return _bank_withdrawal_service_instance


def _bank_upgrade_service():
    global _bank_upgrade_service_instance
    if _bank_upgrade_service_instance is None:
        _bank_upgrade_service_instance = BankUpgradeService(get_paths().game_db, get_paths().player_db)
    return _bank_upgrade_service_instance


def _bank_interest_service():
    global _bank_interest_service_instance
    if _bank_interest_service_instance is None:
        _bank_interest_service_instance = BankInterestService(get_paths().game_db, get_paths().player_db)
    return _bank_interest_service_instance

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
    try:
        bankinfo = readf(user_id)
    except Exception:
        bankinfo = {
            'savestone': 0,
            'savetime': str(runtime_clock.now().strftime('%Y-%m-%d %H:%M:%S')),
            'banklevel': '1',
        }

    if mode == '存灵石':  # 存灵石逻辑
        event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
        operation_id = f"bank-deposit:{event_id}:{user_id}" if event_id else f"bank-deposit:{user_id}:{runtime_ids.new_id()}"
        from ...features.bank.account_info_application import BankAccountInfoApplication
        from ...features.bank.account_application import BankDepositApplication
        from ...features.bank.clock import bank_clock

        migrated_account = BankAccountInfoApplication(get_paths().game_db).get_info(user_id=user_id)
        if migrated_account.get("status") == "ok":
            result = BankDepositApplication(get_paths().game_db).deposit(
                operation_id=operation_id,
                user_id=user_id,
                amount=num,
                interest=0,
                limit=int(BANKLEVEL[migrated_account["bank_level"]]["savemax"]),
                bank_level=migrated_account["bank_level"],
                settled_at=bank_clock().now().isoformat(),
            )
            messages = {
                "applied": f"新灵庄存款成功：存入 {result['deposited']} 枚，当前存款 {result['saved_stone']} 枚。",
                "duplicate": "该存款请求已经处理，无需重复提交。",
                "stone_insufficient": "灵石不足，存款未结算。",
                "limit_exceeded": "超过灵庄存储上限，存款未结算。",
                "operation_conflict": "请求冲突，存款未结算。",
                "user_missing": "未找到修仙数据。",
            }
            await handle_send(bot, event, messages.get(str(result.get("status")), "新存款未结算。"), md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()
        # 先回放：成功后余额/额度变化会挡住“灵石不足/额度不足”前置检查。
        prior = _bank_deposit_service().get_result(operation_id)
        if prior is not None and prior.succeeded:
            msg = (
                f"道友本次结息时间为：已处理，获得灵石：{prior.interest}枚!\n"
                f"道友存入灵石{prior.deposited}枚，当前所拥有灵石{prior.wallet_stone}枚，灵庄存有灵石{prior.saved_stone}枚\n"
                "该存款请求已经处理，无需重复提交。"
            )
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()

        if int(user_info['stone']) < num:
            msg = f"道友所拥有的灵石为{user_info['stone']}枚，金额不足，请重新输入！"
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()

        max = BANKLEVEL[bankinfo['banklevel']]['savemax']
        nowmax = max - bankinfo['savestone']

        if num > nowmax:
            msg = f"道友当前灵庄会员等级为{BANKLEVEL[bankinfo['banklevel']]['level']}，可存储的最大灵石为{max}枚,当前已存{bankinfo['savestone']}枚灵石，可以继续存{nowmax}枚灵石！"
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()

        expected_saved_stone = bankinfo['savestone']
        expected_saved_at = bankinfo['savetime']
        bankinfo, give_stone, timedeff = get_give_stone(bankinfo)
        # Legacy facade call: bank_deposit_service.deposit(...)
        deposit_outcome = bank_application.deposit(
            operation_id=operation_id, user_id=user_id, amount=num,
            expected_saved_stone=expected_saved_stone, expected_saved_at=expected_saved_at,
            bank_level=bankinfo['banklevel'], interest=give_stone,
            settled_at=bankinfo['savetime'], save_limit=max,
        )
        deposit_data = deposit_outcome.data or {}
        deposit_status = str(deposit_data.get("status", "failed"))
        deposit_interest = int(deposit_data.get("interest", 0) or 0)
        deposit_amount = int(deposit_data.get("deposited", 0) or 0)
        deposit_wallet = int(deposit_data.get("wallet_stone", 0) or 0)
        deposit_saved = int(deposit_data.get("saved_stone", 0) or 0)
        if deposit_status == "duplicate":
            msg = (
                f"道友本次结息时间为：{timedeff}小时，获得灵石：{deposit_interest}枚!\n"
                f"道友存入灵石{deposit_amount}枚，当前所拥有灵石{deposit_wallet}枚，灵庄存有灵石{deposit_saved}枚\n"
                "该存款请求已经处理，无需重复提交。"
            )
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()
        if deposit_status == "stone_insufficient":
            msg = "灵石不足，存款未结算。"
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()
        if deposit_status == "limit_exceeded":
            msg = "超过灵庄存储上限，存款未结算。"
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()
        if deposit_status == "state_changed":
            msg = "灵庄操作失败：账户当前状态已更新，本次未结算。"
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()
        if deposit_status == "user_missing":
            await handle_send(bot, event, "未找到修仙数据，本次存款未结算。", md_type="我要修仙")
            await bank.finish()
        msg = f"**灵庄存入**\n---\n✅ 存款成功\n结息时间\n> {timedeff}小时\n获得灵石\n> {deposit_interest}枚\n本次存入\n> {deposit_amount}枚\n当前灵石\n> {deposit_wallet}枚\n灵庄存款\n> {deposit_saved}枚"
        await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
        await bank.finish()

    elif mode == '取灵石':  # 取灵石逻辑
        event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "")).strip()
        operation_id = f"bank-withdrawal:{event_id}:{user_id}" if event_id else f"bank-withdrawal:{user_id}:{runtime_ids.new_id()}"
        from ...features.bank.account_info_application import BankAccountInfoApplication
        from ...features.bank.account_withdrawal_application import BankWithdrawalApplication
        from ...features.bank.clock import bank_clock

        migrated_account = BankAccountInfoApplication(get_paths().game_db).get_info(user_id=user_id)
        if migrated_account.get("status") == "ok":
            result = BankWithdrawalApplication(get_paths().game_db).withdraw(
                operation_id=operation_id,
                user_id=user_id,
                amount=num,
                interest=0,
                bank_level=migrated_account["bank_level"],
                settled_at=bank_clock().now().isoformat(),
            )
            messages = {
                "applied": f"新灵庄取款成功：取出 {result['withdrawn']} 枚，当前存款 {result['saved_stone']} 枚。",
                "duplicate": "该取款请求已经处理，无需重复提交。",
                "saved_stone_insufficient": "灵庄存款不足，取款未结算。",
                "operation_conflict": "请求冲突，取款未结算。",
                "user_missing": "未找到修仙数据。",
            }
            await handle_send(bot, event, messages.get(str(result.get("status")), "新取款未结算。"), md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()
        prior = _bank_withdrawal_service().get_result(operation_id)
        if prior is not None and prior.succeeded:
            msg = (
                f"道友本次结息时间为：已处理，获得灵石：{prior.interest}枚!\n"
                f"取出灵石{prior.withdrawn}枚，当前所拥有灵石{prior.wallet_stone}枚，灵庄存有灵石{prior.saved_stone}枚!\n"
                "该取款请求已经处理，无需重复提交。"
            )
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()

        if int(bankinfo['savestone']) < num:
            msg = f"道友当前灵庄所存有的灵石为{bankinfo['savestone']}枚，金额不足，请重新输入！"
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()

        expected_saved_stone = bankinfo['savestone']
        expected_saved_at = bankinfo['savetime']
        bankinfo, give_stone, timedeff = get_give_stone(bankinfo)
        # Legacy facade call: bank_withdrawal_service.withdraw(...)
        withdrawal_outcome = bank_application.withdraw(
            operation_id=operation_id, user_id=user_id, amount=num,
            expected_saved_stone=expected_saved_stone, expected_saved_at=expected_saved_at,
            bank_level=bankinfo['banklevel'], interest=give_stone, settled_at=bankinfo['savetime'],
        )
        withdrawal_data = withdrawal_outcome.data or {}
        withdrawal_status = str(withdrawal_data.get("status", "failed"))
        withdrawal_interest = int(withdrawal_data.get("interest", 0) or 0)
        withdrawal_amount = int(withdrawal_data.get("withdrawn", 0) or 0)
        withdrawal_wallet = int(withdrawal_data.get("wallet_stone", 0) or 0)
        withdrawal_saved = int(withdrawal_data.get("saved_stone", 0) or 0)
        if withdrawal_status == "duplicate":
            msg = (
                f"道友本次结息时间为：{timedeff}小时，获得灵石：{withdrawal_interest}枚!\n"
                f"取出灵石{withdrawal_amount}枚，当前所拥有灵石{withdrawal_wallet}枚，灵庄存有灵石{withdrawal_saved}枚!\n"
                "该取款请求已经处理，无需重复提交。"
            )
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()
        if withdrawal_status == "saved_stone_insufficient":
            msg = "灵庄存款不足，取款未结算。"
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()
        if withdrawal_status == "state_changed":
            msg = "灵庄操作失败：账户当前状态已更新，本次未结算。"
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()
        if withdrawal_status == "user_missing":
            await handle_send(bot, event, "未找到修仙数据，本次取款未结算。", md_type="我要修仙")
            await bank.finish()
        msg = f"**灵庄取出**\n---\n✅ 取款成功\n结息时间\n> {timedeff}小时\n获得灵石\n> {withdrawal_interest}枚\n本次取出\n> {withdrawal_amount}枚\n当前灵石\n> {withdrawal_wallet}枚\n灵庄存款\n> {withdrawal_saved}枚"
        await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
        await bank.finish()

    elif mode == '升级会员':  # 升级会员逻辑
        event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
        operation_id = f"bank-upgrade:{event_id}:{user_id}" if event_id else f"bank-upgrade:{user_id}:{runtime_ids.new_id()}"
        from ...features.bank.account_info_application import BankAccountInfoApplication
        from ...features.bank.account_upgrade_application import BankUpgradeApplication
        from ...features.bank.clock import bank_clock

        migrated_account = BankAccountInfoApplication(get_paths().game_db).get_info(user_id=user_id)
        if migrated_account.get("status") == "ok":
            userlevel = str(migrated_account["bank_level"])
            if userlevel == str(len(BANKLEVEL)):
                await handle_send(bot, event, "道友已经是本灵庄最大的会员啦！", md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
                await bank.finish()
            next_level = f"{int(userlevel) + 1}"
            stonecost = BANKLEVEL[userlevel]["levelup"]
            result = BankUpgradeApplication(get_paths().game_db).upgrade(
                operation_id=operation_id,
                user_id=user_id,
                expected_level=userlevel,
                next_level=next_level,
                cost=stonecost,
                settled_at=bank_clock().now().isoformat(),
            )
            messages = {
                "applied": f"道友成功升级灵庄会员等级，消耗灵石{result['cost']}枚，当前为：{BANKLEVEL[result['bank_level']]['level']}，灵庄可存有灵石上限{BANKLEVEL[result['bank_level']]['savemax']}枚",
                "duplicate": "该升级请求已经处理，无需重复提交。",
                "stone_insufficient": "灵石不足，会员升级未结算。",
                "state_changed": "灵庄会员升级失败：账户当前状态已更新。",
                "operation_conflict": "请求冲突，会员升级未结算。",
                "user_missing": "未找到修仙数据。",
            }
            await handle_send(bot, event, messages.get(str(result.get("status")), "会员升级未结算。"), md_type="灵庄", k1="升级", v1="灵庄升级会员", k2="信息", v2="灵庄信息", k3="帮助", v3="灵庄帮助")
            await bank.finish()
        prior = _bank_upgrade_service().get_result(operation_id)
        if prior is not None and prior.succeeded:
            msg = (
                f"道友成功升级灵庄会员等级，消耗灵石{prior.cost}枚，当前为：{BANKLEVEL[prior.bank_level]['level']}，"
                f"灵庄可存有灵石上限{BANKLEVEL[prior.bank_level]['savemax']}枚\n"
                "该升级请求已经处理，无需重复提交。"
            )
            await handle_send(bot, event, msg, md_type="灵庄", k1="升级", v1="灵庄升级会员", k2="信息", v2="灵庄信息", k3="帮助", v3="灵庄帮助")
            await bank.finish()

        userlevel = bankinfo["banklevel"]
        if userlevel == str(len(BANKLEVEL)):
            msg = f"道友已经是本灵庄最大的会员啦！"
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()

        stonecost = BANKLEVEL[f"{int(userlevel)}"]['levelup']
        if int(user_info['stone']) < stonecost:
            msg = f"道友所拥有的灵石为{user_info['stone']}枚，当前升级会员等级需求灵石{stonecost}枚金额不足，请重新输入！"
            await handle_send(bot, event, msg, md_type="灵庄", k1="升级", v1="灵庄升级会员", k2="信息", v2="灵庄信息", k3="帮助", v3="灵庄帮助")
            await bank.finish()

        next_level = f"{int(userlevel) + 1}"
        # Legacy facade call: bank_upgrade_service.upgrade(...)
        upgrade_outcome = bank_application.upgrade(
            operation_id=operation_id, user_id=user_id, expected_level=userlevel,
            next_level=next_level, cost=stonecost,
        )
        upgrade_data = upgrade_outcome.data or {}
        upgrade_status = str(upgrade_data.get("status", "failed"))
        upgrade_cost = int(upgrade_data.get("cost", 0) or 0)
        upgrade_level = str(upgrade_data.get("bank_level", userlevel))
        if upgrade_status == "duplicate":
            msg = (
                f"道友成功升级灵庄会员等级，消耗灵石{upgrade_cost}枚，当前为：{BANKLEVEL[upgrade_level]['level']}，"
                f"灵庄可存有灵石上限{BANKLEVEL[upgrade_level]['savemax']}枚\n"
                "该升级请求已经处理，无需重复提交。"
            )
            await handle_send(bot, event, msg, md_type="灵庄", k1="升级", v1="灵庄升级会员", k2="信息", v2="灵庄信息", k3="帮助", v3="灵庄帮助")
            await bank.finish()
        if upgrade_status == "stone_insufficient":
            msg = "灵石不足，会员升级未结算。"
            await handle_send(bot, event, msg, md_type="灵庄", k1="升级", v1="灵庄升级会员", k2="信息", v2="灵庄信息", k3="帮助", v3="灵庄帮助")
            await bank.finish()
        if upgrade_status == "state_changed":
            msg = "灵庄会员升级失败：账户当前状态已更新，本次未结算，请重新【灵庄】查看后再试。"
            await handle_send(bot, event, msg, md_type="灵庄", k1="升级", v1="灵庄升级会员", k2="信息", v2="灵庄信息", k3="帮助", v3="灵庄帮助")
            await bank.finish()
        if upgrade_status == "user_missing":
            await handle_send(bot, event, "未找到修仙数据，本次会员升级未结算。", md_type="我要修仙")
            await bank.finish()
        msg = f"道友成功升级灵庄会员等级，消耗灵石{upgrade_cost}枚，当前为：{BANKLEVEL[upgrade_level]['level']}，灵庄可存有灵石上限{BANKLEVEL[upgrade_level]['savemax']}枚"

        await handle_send(bot, event, msg, md_type="灵庄", k1="升级", v1="灵庄升级会员", k2="信息", v2="灵庄信息", k3="帮助", v3="灵庄帮助")
        await bank.finish()

    elif mode == '信息':  # 查询灵庄信息
        from ...features.bank.account_info_application import BankAccountInfoApplication

        new_info = BankAccountInfoApplication(get_paths().game_db).get_info(user_id=user_id)
        if new_info.get("status") == "ok":
            msg = f'''**灵庄信息**
---
已存
> {new_info['saved_stone']}灵石
存入时间
> {new_info['updated_at']}
会员等级
> {BANKLEVEL[new_info['bank_level']]['level']}
当前灵石
> {new_info['wallet_stone']}
存储上限
> {BANKLEVEL[new_info['bank_level']]['savemax']}枚
'''
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="结算", v3="灵庄结算")
            await bank.finish()
        msg = f'''**灵庄信息**
---
已存
> {bankinfo['savestone']}灵石
存入时间
> {bankinfo['savetime']}
会员等级
> {BANKLEVEL[bankinfo['banklevel']]['level']}
当前灵石
> {user_info['stone']}
存储上限
> {BANKLEVEL[bankinfo['banklevel']]['savemax']}枚
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

        migrated_account = BankAccountInfoApplication(get_paths().game_db).get_info(user_id=user_id)
        if migrated_account.get("status") == "ok":
            now = bank_clock().now()
            level = str(migrated_account["bank_level"])
            interest, hours = calculate_interest(
                saved_stone=int(migrated_account["saved_stone"]),
                saved_at=str(migrated_account["updated_at"]),
                settled_at=now,
                rate=float(BANKLEVEL[level]["interest"]),
            )
            result = BankInterestApplication(get_paths().game_db).settle_interest(
                operation_id=operation_id,
                user_id=user_id,
                interest=interest,
                bank_level=level,
                settled_at=now.strftime("%Y-%m-%d %H:%M:%S"),
            )
            status = str(result.get("status"))
            if status == "duplicate":
                msg = "**灵庄结息**\n---\n✅ 结息成功\n该结息请求已经处理，无需重复提交。"
            elif status == "applied":
                msg = f"**灵庄结息**\n---\n✅ 结息成功\n结息时间\n> {hours}小时\n获得灵石\n> {result['interest']}枚"
            elif status == "state_changed":
                msg = "⚠️ 灵庄结息失败：账户当前状态已更新，本次未处理。"
            else:
                msg = "灵庄结息未完成。"
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()
        prior = _bank_interest_service().get_result(operation_id)
        if prior is not None and prior.succeeded:
            msg = f"**灵庄结息**\n---\n✅ 结息成功\n获得灵石\n> {prior.interest}枚\n该结息请求已经处理，无需重复提交。"
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()

        expected_saved_stone = bankinfo['savestone']
        expected_saved_at = bankinfo['savetime']
        bankinfo, give_stone, timedeff = get_give_stone(bankinfo)
        # Legacy facade call: bank_interest_service.settle(...)
        settlement_outcome = bank_application.settle_interest(
            operation_id=operation_id, user_id=user_id,
            expected_saved_stone=expected_saved_stone, expected_saved_at=expected_saved_at,
            bank_level=bankinfo['banklevel'], interest=give_stone, settled_at=bankinfo['savetime'],
        )
        settlement_data = settlement_outcome.data or {}
        settlement_status = str(settlement_data.get("status", "failed"))
        settlement_interest = int(settlement_data.get("interest", 0) or 0)
        if settlement_status == "duplicate":
            msg = f"**灵庄结息**\n---\n✅ 结息成功\n结息时间\n> {timedeff}小时\n获得灵石\n> {settlement_interest}枚\n该结息请求已经处理，无需重复提交。"
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()
        if settlement_status == "state_changed":
            msg = "⚠️ 灵庄结息失败：账户当前状态已更新，本次未处理，请重新【灵庄】查看后再试。"
            await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
            await bank.finish()
        if settlement_status == "user_missing":
            await handle_send(bot, event, "❌ 未找到修仙数据，本次结息未处理。", md_type="我要修仙")
            await bank.finish()
        msg = f"**灵庄结息**\n---\n✅ 结息成功\n结息时间\n> {timedeff}小时\n获得灵石\n> {settlement_interest}枚"
        await handle_send(bot, event, msg, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
        await bank.finish()


def get_give_stone(bankinfo):
    """获取利息：利息=give_stone,结算时间=timedeff"""
    from ...features.bank.interest_rules import calculate_interest

    savetime = bankinfo['savetime']  # str
    nowtime = runtime_clock.now().strftime('%Y-%m-%d %H:%M:%S')  # str
    give_stone, timedeff = calculate_interest(
        saved_stone=bankinfo['savestone'],
        saved_at=savetime,
        settled_at=datetime.strptime(nowtime, '%Y-%m-%d %H:%M:%S'),
        rate=BANKLEVEL[bankinfo['banklevel']]['interest'],
    )
    bankinfo['savetime'] = nowtime

    return bankinfo, give_stone, timedeff


def readf(user_id):
    """从动态数据库读取灵庄信息（兼容默认值）"""
    user_id = str(user_id)
    bank_data = player_data_manager.get_fields(user_id, "bankinfo")
    if not bank_data:
        return {
            "savestone": 0,
            "savetime": str(runtime_clock.now().strftime('%Y-%m-%d %H:%M:%S')),
            "banklevel": "1",
        }

    # 兼容缺失字段
    savestone = bank_data.get("savestone", 0)
    savetime = bank_data.get("savetime", str(runtime_clock.now().strftime('%Y-%m-%d %H:%M:%S')))
    banklevel = str(bank_data.get("banklevel", "1"))

    try:
        savestone = int(savestone)
    except Exception:
        savestone = 0

    return {
        "savestone": savestone,
        "savetime": str(savetime),
        "banklevel": banklevel,
    }


def savef(user_id, data):
    """保存灵庄信息到动态数据库"""
    user_id = str(user_id)
    player_data_manager.update_or_write_data(user_id, "bankinfo", "savestone", int(data.get("savestone", 0)), data_type="INTEGER")
    player_data_manager.update_or_write_data(user_id, "bankinfo", "savetime", str(data.get("savetime", runtime_clock.now().strftime('%Y-%m-%d %H:%M:%S'))), data_type="TEXT")
    player_data_manager.update_or_write_data(user_id, "bankinfo", "banklevel", str(data.get("banklevel", "1")), data_type="TEXT")
    return True
