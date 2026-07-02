import random
from datetime import datetime
from typing import Any, Dict, Optional

from ..xiuxian_utils.utils import log_message, update_statistics_value


def _trade_economy_context(action: str, trace_id: str | None = None, **detail):
    return {
        "source": "trade",
        "action": action,
        "trace_id": trace_id or f"trade:{action}:{datetime.now().strftime('%Y%m%d%H%M%S%f')}:{random.randint(1000, 9999)}",
        "detail": detail,
    }


def record_trade_event(user_id: str, title: str, detail: str, stats: Optional[Dict[str, int]] = None):
    """记录交易相关日志和核心统计。"""
    if str(user_id) == "0":
        return
    log_message(str(user_id), f"[{title}] {detail}")
    if not stats:
        return
    for key, increment in stats.items():
        if increment:
            update_statistics_value(str(user_id), key, increment=increment)


def get_item_trade_rank(item_info: Optional[Dict[str, Any]]) -> Optional[int]:
    if not item_info or "rank" not in item_info:
        return None
    try:
        return int(item_info["rank"])
    except (TypeError, ValueError):
        return None


def get_trade_forbid_reason(goods_id: Any, item_info: Optional[Dict[str, Any]], action: str = "交易") -> Optional[str]:
    item_name = item_info.get("name", f"ID:{goods_id}") if item_info else f"ID:{goods_id}"
    trade_rank = get_item_trade_rank(item_info)
    if trade_rank is not None and trade_rank <= 0:
        return f"{item_name}是不可交易的珍贵物品！"
    return None
