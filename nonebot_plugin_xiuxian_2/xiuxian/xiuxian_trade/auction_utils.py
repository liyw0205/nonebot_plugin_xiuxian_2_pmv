from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from nonebot.log import logger

from ..adapter_compat import GroupMessageEvent, PrivateMessageEvent
from ..xiuxian_utils.utils import check_user, number_to
from . import auction_config


auction_repository: Any = None


def bind_auction_repository(repository: Any) -> None:
    global auction_repository
    auction_repository = repository


def get_auction_status() -> Dict[str, Any]:
    """获取拍卖状态（是否活跃，开始/结束时间）"""
    # 从内存配置中读取 auction_status 字段
    status_dict = auction_config.get_auction_status_config()

    # 辅助函数：将 YYYYMMDDhhmmss 格式字符串转换为 datetime 对象
    def parse_time_str(time_str: str) -> Optional[datetime]:
        if time_str:
            try:
                return datetime.strptime(time_str, "%Y%m%d%H%M%S")
            except ValueError:
                logger.error(f"无法解析拍卖时间字符串: {time_str}")
        return None

    # 确保返回的字典包含所有预期字段，并处理时间字符串到datetime对象的转换
    return {
        "active": status_dict.get("active", False),
        "start_time": parse_time_str(status_dict.get("start_time", "")),
        "end_time": parse_time_str(status_dict.get("end_time", "")),
        "last_display_refresh_time": parse_time_str(status_dict.get("last_display_refresh_time", "")),
        "items_count": status_dict.get("items_count", 0)
    }


def set_auction_status(active: bool, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None, last_display_refresh_time: Optional[datetime] = None, items_count: int = 0):
    """
    更新拍卖状态。
    时间参数应为 datetime 对象或 None。
    """
    def format_time_to_str(dt: Optional[datetime]) -> str:
        return dt.strftime("%Y%m%d%H%M%S") if dt else ""

    status = {
        "active": active,
        "start_time": format_time_to_str(start_time), # 存储为 YYYYMMDDhhmmss 格式字符串
        "end_time": format_time_to_str(end_time),     # 存储为 YYYYMMDDhhmmss 格式字符串
        "last_display_refresh_time": format_time_to_str(last_display_refresh_time), # 存储为 YYYYMMDDhhmmss 格式字符串
        "items_count": items_count
    }
    auction_config.set_auction_config_value("auction_status", status)
    auction_config.persist_auction_status(status)


def _restore_auction_status_from_disk() -> bool:
    """启动时把落盘场次写回内存（不再写盘）。"""
    persisted = auction_config.load_persisted_auction_status()
    if not persisted:
        return False
    cfg = auction_config.get_auction_config()
    cfg["auction_status"] = persisted
    auction_config.save_config(cfg)
    return True


def _safe_auction_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_auction_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _format_auction_duration(total_seconds: float) -> str:
    seconds = max(int(total_seconds), 0)
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    if days:
        return f"{days}天{hours}小时{minutes}分"
    if hours:
        return f"{hours}小时{minutes}分"
    if minutes:
        return f"{minutes}分{seconds}秒"
    return f"{seconds}秒"


def _format_auction_datetime(dt: Optional[datetime], now: Optional[datetime] = None) -> str:
    if not dt:
        return "未知"
    now = now or datetime.now()
    if dt.date() == now.date():
        return f"今日 {dt.strftime('%H:%M')}"
    if dt.date() == (now + timedelta(days=1)).date():
        return f"明日 {dt.strftime('%H:%M')}"
    if dt.year == now.year:
        return dt.strftime("%m-%d %H:%M")
    return dt.strftime("%Y-%m-%d %H:%M")


def _get_next_auction_start(schedule: Dict[str, Any], now: Optional[datetime] = None) -> Optional[datetime]:
    if not schedule.get("enabled", True):
        return None
    now = now or datetime.now()
    start_hour = min(max(_safe_auction_int(schedule.get("start_hour"), 17), 0), 23)
    start_minute = min(max(_safe_auction_int(schedule.get("start_minute"), 0), 0), 59)
    next_start = now.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
    if next_start <= now:
        next_start += timedelta(days=1)
    return next_start


def _auction_bid_count(item: Dict[str, Any]) -> int:
    bids = item.get("bids") or {}
    return len(bids) if isinstance(bids, dict) else 0


def _auction_last_bid_time(item: Dict[str, Any]) -> float:
    bid_times = item.get("bid_times") or {}
    if isinstance(bid_times, dict) and bid_times:
        return max(_safe_auction_float(value) for value in bid_times.values())
    return _safe_auction_float(item.get("last_bid_time"))


def _format_hot_auction_items(current_auctions: List[Dict[str, Any]], limit: int = 5) -> List[str]:
    if not current_auctions:
        return ["暂无热门拍品：当前没有进行中的拍品。"]

    ranked_items = sorted(
        current_auctions,
        key=lambda item: (
            _auction_bid_count(item),
            _safe_auction_int(item.get("current_price"), _safe_auction_int(item.get("start_price"))),
            _auction_last_bid_time(item),
        ),
        reverse=True,
    )[:limit]

    lines = []
    for index, item in enumerate(ranked_items, 1):
        current_price = _safe_auction_int(item.get("current_price"), _safe_auction_int(item.get("start_price")))
        bid_count = _auction_bid_count(item)
        bid_text = f"{bid_count}次出价" if bid_count else "暂无出价"
        seller_name = "系统" if item.get("is_system") else str(item.get("seller_name") or "未知")
        lines.append(
            f"{index}. {item.get('name', '未知拍品')}（ID:{item.get('id', '未知')}） "
            f"当前价:{number_to(current_price)}灵石，{bid_text}，卖家:{seller_name}"
        )
    return lines


def _format_recent_auction_deals(limit: int = 5) -> List[str]:
    try:
        history_records = auction_repository.get_auction_history() or []
    except Exception as e:
        logger.warning(f"读取拍卖成交记录失败: {e}")
        return ["最近成交记录暂不可用。"]

    deal_records = [
        record for record in history_records
        if record.get("status") == "成交" and record.get("final_price") is not None
    ]
    if not deal_records:
        return ["暂无成交记录。"]

    deal_records.sort(key=lambda record: _safe_auction_float(record.get("end_time")), reverse=True)
    lines = []
    for index, record in enumerate(deal_records[:limit], 1):
        end_timestamp = _safe_auction_float(record.get("end_time"))
        end_time_text = datetime.fromtimestamp(end_timestamp).strftime("%m-%d %H:%M") if end_timestamp else "时间未知"
        winner_name = record.get("winner_name") or record.get("winner_id") or "未知"
        seller_name = record.get("seller_name") or record.get("seller_id") or "未知"
        final_price = _safe_auction_int(record.get("final_price"))
        lines.append(
            f"{index}. {record.get('item_name', '未知拍品')} "
            f"{number_to(final_price)}灵石，买家:{winner_name}，卖家:{seller_name}，{end_time_text}"
        )
    return lines


def _format_user_auction_quota(
    event: GroupMessageEvent | PrivateMessageEvent,
    current_auctions: List[Dict[str, Any]],
    max_user_items: int,
    active: bool = False,
) -> List[str]:
    is_user, user_info, _ = check_user(event)
    if not is_user or not user_info:
        return ["我的上架: 未加入修仙或当前不可用，暂无个人额度信息。"]

    user_id = str(user_info["user_id"])
    waiting_items = trade_manager.get_player_auction_items(user_id) or []
    waiting_count = len(waiting_items)
    current_count = sum(
        1
        for item in current_auctions
        if str(item.get("seller_id")) == user_id and not item.get("is_system")
    )
    remaining_count = max(max_user_items - waiting_count, 0)
    remaining_text = f"{remaining_count}件"
    if active:
        remaining_text = f"0件（拍卖进行中暂不可上架，结束后可用{remaining_count}件）"
    return [
        f"我的上架: 本场{current_count}件，等待区{waiting_count}/{max_user_items}件",
        f"剩余额度: {remaining_text}"
    ]
