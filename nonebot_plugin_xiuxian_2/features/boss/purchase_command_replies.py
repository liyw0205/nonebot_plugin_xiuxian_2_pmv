from __future__ import annotations

from typing import Any, Mapping


def render_boss_purchase_reply(result: Mapping[str, Any]) -> str:
    """Render a purchase result without consulting the live shop or inventory."""
    status = str(result.get("status", "failed"))
    if status not in {"applied", "duplicate"}:
        return {
            "invalid_argument": "请输入正确的商品编号和数量。",
            "integral_insufficient": "兑换失败：世界积分不足。",
            "limit_reached": "该商品已到限购数量，无法继续兑换。",
            "inventory_full": "该商品持有数量已达上限。",
            "state_changed": "兑换未完成：活动进度已更新，请重新兑换。",
            "user_missing": "未找到道友数据，世界BOSS兑换失败！",
            "schema_missing": "世界积分数据未就绪，请稍后重试。",
            "receipt_invalid": "世界BOSS兑换回执异常，本次未结算，请联系管理员核查。",
            "config_invalid": "世界积分商店数据暂不可用，本次未结算。",
            "operation_conflict": "兑换请求冲突，本次未结算。",
            "operation_pending": "兑换请求正在处理中，请稍后查询结果。",
            "operation_failed": "兑换请求未完成，请稍后核查结果。",
        }.get(status, "兑换未完成：活动进度已更新，请重新兑换。")
    quantity = result.get("quantity")
    if type(quantity) is not int or quantity <= 0:
        return "世界BOSS兑换回执异常，本次未结算，请联系管理员核查。"
    item_name = result.get("item_name")
    if not isinstance(item_name, str) or not item_name.strip():
        return "世界BOSS兑换回执异常，本次未结算，请联系管理员核查。"
    message = f"道友成功兑换获得：{item_name}{quantity}个"
    if status == "duplicate" or result.get("replayed"):
        message += "\n该兑换请求已经处理，无需重复提交。"
    return message


__all__ = ["render_boss_purchase_reply"]
