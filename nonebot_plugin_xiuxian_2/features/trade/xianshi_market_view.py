"""Pure rendering for Xianshi market quotes and the player's watch list."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from .xianshi_market_application import XianshiWatchOverview
from .xianshi_market_repository import XianshiMarketQuote
from .xianshi_watch_repository import XianshiWatchResult


NumberFormatter = Callable[[int], str]


def _plain_number(value: int) -> str:
    return str(int(value))


def _money_text(value: Any, number: NumberFormatter) -> str:
    return f"{number(int(value or 0))}灵石"


def _time_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "时间未知"
    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text[:19], pattern).strftime("%m-%d %H:%M")
        except ValueError:
            continue
    return "时间未知"


def render_market_quote(
    name: str,
    quote: XianshiMarketQuote,
    *,
    watched: bool,
    number: NumberFormatter = _plain_number,
) -> str:
    """Render one item's 7-day trade summary and current listings."""

    lines = [f"【仙肆行情】{name}"]
    if not quote.trades_ready and not quote.listings_ready:
        lines.append("行情数据暂不可用（数据库未迁移或不可读）。")
    else:
        if not quote.trades_ready:
            lines.append("成交数据暂不可用。")
        elif quote.trade_count <= 0:
            lines.append("近7天没有成交记录。")
        else:
            lines.append(
                f"近7天成交：{quote.trade_count} 笔 / {quote.traded_quantity} 件"
            )
            lines.append(
                f"成交单价：最低 {_money_text(quote.min_unit_price, number)}｜"
                f"最高 {_money_text(quote.max_unit_price, number)}｜"
                f"加权均价 {_money_text(quote.avg_unit_price, number)}"
            )
            lines.append(
                f"最近成交：{_time_text(quote.last_traded_at)}"
                f"（单价 {_money_text(quote.last_unit_price, number)}）"
            )

        if not quote.listings_ready:
            lines.append("在售数据暂不可用。")
        elif quote.listing_count <= 0:
            lines.append("当前没有在售。")
        else:
            lines.append(
                f"当前在售：{quote.listing_count} 个摊位 / {quote.listing_quantity} 件"
            )
            lines.append(
                f"最低在售单价：{_money_text(quote.min_listing_price, number)}"
            )

    if watched:
        lines.append(f"已关注：发送【仙肆关注 取消 {name}】可取消")
    else:
        lines.append(f"未关注：发送【仙肆关注 {name}】加入关注")
    return "\n".join(lines)


def _watch_row(index: int, name: str, quote: XianshiMarketQuote, number: NumberFormatter) -> str:
    if not quote.trades_ready:
        trade_text = "成交不可用"
    elif quote.trade_count <= 0:
        trade_text = "近7天无成交"
    else:
        trade_text = f"近7天 {quote.trade_count} 笔，均价 {_money_text(quote.avg_unit_price, number)}"

    if not quote.listings_ready:
        listing_text = "在售不可用"
    elif quote.listing_count <= 0:
        listing_text = "当前无在售"
    else:
        listing_text = (
            f"在售 {quote.listing_count} 摊 {quote.listing_quantity} 件，"
            f"最低 {_money_text(quote.min_listing_price, number)}"
        )
    return f"{index}. {name}｜{trade_text}｜{listing_text}"


def render_watchlist(
    overview: XianshiWatchOverview,
    *,
    number: NumberFormatter = _plain_number,
) -> str:
    """Render the player's watched items with a per-item market summary."""

    if not overview.names:
        return (
            "你还没有关注任何仙肆物品。\n"
            f"发送【仙肆关注 物品名】添加关注（最多 {overview.limit} 个）。"
        )
    lines = [f"【我的仙肆关注】{len(overview.names)}/{overview.limit}"]
    for index, name in enumerate(overview.names, start=1):
        lines.append(_watch_row(index, name, overview.quote_for(name), number))
    lines.append(
        "提示：仙肆关注 物品名 添加｜仙肆关注 取消 物品名 取消｜"
        "仙肆行情 物品名 查看详情"
    )
    return "\n".join(lines)


def render_watch_result(
    result: XianshiWatchResult,
    *,
    limit: int = 10,
) -> str:
    """Render the outcome of an add/remove watch command."""

    name = result.goods_name or "该物品"
    if result.status == "added":
        return (
            f"已关注【{name}】，当前 {result.watch_count}/{limit}。\n"
            f"发送【仙肆行情 {name}】查看行情。"
        )
    if result.status == "duplicate":
        return f"【{name}】已在关注列表中，无需重复添加（当前 {result.watch_count}/{limit}）。"
    if result.status == "removed":
        return f"已取消关注【{name}】，当前 {result.watch_count}/{limit}。"
    if result.status == "absent":
        return f"【{name}】不在关注列表中。"
    if result.status == "limit_reached":
        return f"关注列表已满（最多 {limit} 个），请先取消一个再添加。"
    if result.status == "invalid_name":
        return "物品名无效：请使用 1-24 个可见字符，且不要包含换行。"
    if result.status == "invalid_user":
        return "缺少玩家身份，无法操作关注列表。"
    if result.status == "operation_conflict":
        return "该操作号已用于其他关注操作，请重新发送。"
    if result.status == "schema_missing":
        return "关注功能尚未初始化（缺少 trade.014 迁移），请联系管理员。"
    return "关注操作未完成，请稍后重试。"


__all__ = ["render_market_quote", "render_watch_result", "render_watchlist"]
