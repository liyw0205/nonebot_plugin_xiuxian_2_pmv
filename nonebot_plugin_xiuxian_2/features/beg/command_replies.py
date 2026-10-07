from __future__ import annotations

from typing import Any, Mapping


def render_beg_reply(result: Mapping[str, Any]) -> str:
    action = str(result.get("action", ""))
    status = str(result.get("status", "failed"))
    if action == "help" and status in {"ok", "help"}:
        return (
            "**仙途奇缘帮助**\n---\n"
            "**仙途奇缘**\n- 仙途奇缘\n> 每日领取一次随机灵石\n"
            f"> 修为低于{result['max_level']}\n"
            f"> 角色创建不超过{result['max_age_days']}天\n"
            "> 未加入宗门或拥有符合条件的特殊灵根\n\n"
            "**新手礼包**\n- 新手礼包\n"
            f"> 灵石、功法、装备等基础资源，限领一次（创建角色{result['max_age_days']}天内）\n\n"
            f"> 当前时间：{result['current_time']}"
        )
    if status not in {"applied", "duplicate"}:
        if status == "already_claimed":
            return "您已经领取过新手礼包了！" if action == "novice_claim" else "今日机缘已经领取，请明日再来。"
        if status == "expired":
            if action == "novice_claim" and result.get("max_age_days") is not None:
                return f"新手礼包仅限创建角色{result['max_age_days']}天内领取！"
            return "道友已经过了新手期，本次无法领取。"
        return {
            "ineligible_sect": "道友已有宗门庇佑，又何必来此寻求机缘呢？",
            "ineligible_root": "道友已是轮回大能，又何必来此寻求机缘呢？",
            "ineligible_level": "当前修为已超过新手机缘范围。",
            "inventory_full": "背包空间不足，无法领取新手礼包。",
            "operation_conflict": "领取失败：请求冲突，请勿重复提交。",
            "state_changed": "领取未结算：角色当前状态已更新，请重试。",
            "user_missing": "未找到角色信息，无法领取。",
            "schema_missing": "机缘服务尚未就绪，本次未结算。",
            "receipt_invalid": "机缘回执异常，本次未结算，请联系管理员核查。",
            "invalid_argument": "领取请求无效，本次未结算。",
            "config_invalid": "机缘配置暂不可用，本次未结算。",
            "profile_invalid": "角色数据暂不可用，本次未结算。",
            "operation_pending": "该机缘请求正在处理中，请稍后查询结果。",
            "operation_failed": "该机缘请求未完成，请联系管理员核查。",
        }.get(status, "机缘领取未完成，请稍后重试。")

    replayed = bool(result.get("replayed")) or status == "duplicate"
    if action == "daily_settle":
        message = f"你获得了 {result['stone_reward']} 枚灵石。"
    elif action == "novice_claim":
        lines = ["**新手礼包**", "---", "已发放" if replayed else "领取成功", f"获得灵石 {result['stone']} 枚"]
        for reward in result.get("rewards", ()):
            lines.append(f"获得 {reward['name']} x{reward['amount']}")
        message = "\n".join(lines)
    else:
        return "机缘领取未完成，请稍后重试。"
    if replayed:
        message += "\n该请求已经处理，无需重复提交。"
    return message


__all__ = ["render_beg_reply"]
