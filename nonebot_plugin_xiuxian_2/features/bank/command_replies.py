from __future__ import annotations

from typing import Any, Mapping


def render_bank_reply(result: Mapping[str, Any], bank_levels: Mapping[str, Any]) -> str:
    action = str(result.get("action", ""))
    status = str(result.get("status", "failed"))
    if action == "info" and status in {"ok", "account_missing"}:
        level = str(result.get("bank_level", "1"))
        config = bank_levels.get(level, {})
        return (
            "**灵庄信息**\n---\n"
            f"已存\n> {int(result.get('saved_stone', 0))}灵石\n"
            f"存入时间\n> {result.get('updated_at') or '尚未存入'}\n"
            f"会员等级\n> {config.get('level', level)}\n"
            f"当前灵石\n> {int(result.get('wallet_stone', 0))}\n"
            f"存储上限\n> {config.get('savemax', '暂不可用')}枚"
        )
    if status not in {"applied", "duplicate"}:
        return {
            "invalid_argument": "请输入正确的金额或指令！本次未结算。",
            "invalid_amount": "请输入正确的金额！本次未结算。",
            "invalid_mode": "灵庄指令无效，本次未结算。",
            "stone_insufficient": "灵石不足，本次灵庄操作未结算。",
            "saved_stone_insufficient": "灵庄存款不足，取款未结算。",
            "limit_exceeded": "超过灵庄存储上限，存款未结算。",
            "state_changed": "灵庄账户当前状态已更新，本次未结算，请重试。",
            "operation_conflict": "请求冲突，本次灵庄操作未结算。",
            "user_missing": "未找到修仙数据，本次灵庄操作未结算。",
            "account_missing": "灵庄暂无存款，无法取款。",
            "account_invalid": "灵庄账户暂不可用，本次未结算。",
            "schema_missing": "灵庄服务尚未就绪，本次未结算。",
            "receipt_invalid": "灵庄回执异常，本次未结算，请联系管理员核查。",
            "max_level": "道友已经是本灵庄最大的会员啦！",
            "needs_reconcile": "灵庄历史回执需要核查，本次未结算。",
        }.get(status, "灵庄操作未完成，请稍后重试。")

    replay = "\n该请求已经处理，无需重复提交。" if status == "duplicate" else ""
    if action == "deposit":
        message = (
            f"灵庄存款成功：存入 {result['deposited']} 枚，当前存款 {result['saved_stone']} 枚。"
            f"\n本次结息获得灵石 {result.get('interest', 0)} 枚，当前灵石 {result['wallet_stone']} 枚。"
        )
    elif action == "withdrawal":
        message = (
            f"灵庄取款成功：取出 {result['withdrawn']} 枚，当前存款 {result['saved_stone']} 枚。"
            f"\n本次结息获得灵石 {result.get('interest', 0)} 枚，当前灵石 {result['wallet_stone']} 枚。"
        )
    elif action == "upgrade":
        level = str(result['bank_level'])
        config = bank_levels.get(level, {})
        message = f"道友成功升级灵庄会员等级，消耗灵石{result['cost']}枚，当前为：{config.get('level', level)}。"
        if "savemax" in config:
            message += f"灵庄可存有灵石上限{config['savemax']}枚。"
    elif action == "interest":
        message = f"**灵庄结息**\n---\n结息成功\n获得灵石\n> {result['interest']}枚"
        if result.get("interest_hours") is not None:
            message += f"\n结息时间\n> {result['interest_hours']}小时"
    else:
        return "灵庄操作未完成，请稍后重试。"
    return message + replay


__all__ = ["render_bank_reply"]
