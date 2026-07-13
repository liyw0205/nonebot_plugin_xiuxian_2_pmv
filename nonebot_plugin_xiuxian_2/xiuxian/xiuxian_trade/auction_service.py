import random
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from nonebot.log import logger

from ..adapter_compat import Bot
from ..xiuxian_utils.game_events import safe_record_game_event
from ..xiuxian_utils.utils import number_to
from . import auction_config
from .auction_utils import (
    get_auction_status,
    set_auction_status,
)
from .trade_utils import _trade_economy_context, record_trade_event


_items: Any = None
_sql_message: Any = None
_trade_manager: Any = None
_auction_repository: Any = None
_auction_session_service: Any = None


def bind_auction_service_dependencies(
    *, items: Any, sql_message: Any, trade_manager: Any, auction_repository: Any,
    auction_session_service: Any
) -> None:
    global _items, _sql_message, _trade_manager, _auction_repository, _auction_session_service
    _items = items
    _sql_message = sql_message
    _trade_manager = trade_manager
    _auction_repository = auction_repository
    _auction_session_service = auction_session_service


def _auction_dependencies() -> tuple[Any, Any, Any, Any, Any]:
    if (
        _items is None
        or _sql_message is None
        or _trade_manager is None
        or _auction_repository is None
        or _auction_session_service is None
    ):
        raise RuntimeError("auction service dependencies are not bound")
    return _items, _sql_message, _trade_manager, _auction_repository, _auction_session_service


def start_auction_process(bot: Optional[Bot], operation_id: str | None = None) -> bool: # bot参数可能为None
    """
    启动拍卖流程。
    从玩家上架区和系统配置中生成拍卖品，并存入当前拍卖表。
    """
    _, _, _, _, session_service = _auction_dependencies()
    operation_id = operation_id or f"auction-start:{time.time_ns()}"
    previous = session_service.get_start_operation(operation_id)
    if previous is not None:
        start_dt = datetime.fromtimestamp(previous.start_time)
        end_dt = datetime.fromtimestamp(previous.end_time)
        set_auction_status(
            active=True, start_time=start_dt, end_time=end_dt,
            last_display_refresh_time=start_dt, items_count=previous.items_count,
        )
        return True
    system_items_config = auction_config.get_system_items() # 从内置配置获取系统物品

    schedule_config = auction_config.get_auction_schedule()

    # 随机选择5个系统拍卖品
    selected_system_items_names = random.sample(list(system_items_config.keys()), min(5, len(system_items_config)))
    selected_system_items = [
        {"item_id": system_items_config[name]["id"],
         "name": name,
         "start_price": system_items_config[name]["start_price"]
        } for name in selected_system_items_names
    ]
    now_dt = datetime.now()
    end_time_dt = now_dt + timedelta(hours=schedule_config["duration_hours"])
    session_id = f"auction:{now_dt.strftime('%Y%m%d%H%M%S')}:{operation_id[-12:]}"
    result = session_service.start(
        operation_id, session_id, start_time=now_dt.timestamp(),
        end_time=end_time_dt.timestamp(), system_items=selected_system_items,
    )
    if not result.succeeded:
        logger.warning(f"拍卖开启失败：{result.status}")
        return False
    start_dt = datetime.fromtimestamp(result.start_time)
    end_dt = datetime.fromtimestamp(result.end_time)
    set_auction_status(
        active=True, start_time=start_dt, end_time=end_dt,
        last_display_refresh_time=start_dt, items_count=result.items_count,
    )
    current_date = datetime.now().strftime('%Y-%m-%d')
    auction_config.set_auction_config_value("schedule", current_date, "last_auto_start_date")
    logger.info(f"拍卖已开启，共 {result.items_count} 件物品参与拍卖！")
    return True


async def end_auction_process(
    bot: Optional[Bot], operation_id: str | None = None
) -> List[Dict[str, Any]]: # bot参数可能为None
    """Atomically settle every item in the active database auction session."""
    items, _, _, auction_repository, session_service = _auction_dependencies()
    current_auctions = auction_repository.get_current_auction()
    if not current_auctions:
        return []
    session = session_service.get_active_session()
    if session is None:
        raise RuntimeError("auction items exist without an active database session")
    item_types = {}
    for item in current_auctions:
        info = items.get_data_by_item_id(item["item_id"])
        if info:
            item_types[int(item["item_id"])] = str(info["type"])
    result = session_service.finish(
        operation_id or f"auction-finish:{session['session_id']}",
        session["session_id"], end_time=time.time(),
        fee_rate=auction_config.get_auction_rules()["fee_rate"],
        item_types=item_types,
    )
    if not result.succeeded:
        raise ValueError(f"auction session settlement blocked: status={result.status}")
    auction_results = [dict(record) for record in result.results]
    for settlement in auction_results:
        trace_id = f"trade:auction:{settlement['auction_id']}"
        if settlement["final_price"] is not None:
            record_trade_event(
                settlement["winner_id"], "拍卖成交",
                f"拍得{settlement['item_name']}，成交价{number_to(settlement['final_price'])}灵石，拍卖ID:{settlement['auction_id']}",
                {"拍卖成交次数": 1, "拍卖消费灵石": settlement["final_price"]},
            )
            safe_record_game_event(
                settlement["winner_id"], "trade_buy", 1,
                {
                    "source": "auction", "action": "auction_buy",
                    "stone_delta": -int(settlement["final_price"]),
                    "item_delta": [{"id": settlement["item_id"], "name": settlement["item_name"], "amount": 1}],
                    "detail": {"auction_id": settlement["auction_id"], "seller_id": settlement["seller_id"], "final_price": settlement["final_price"]},
                    "trace_id": trace_id,
                },
            )
            if settlement["seller_id"] != "0":
                record_trade_event(
                    settlement["seller_id"], "拍卖成交",
                    f"售出{settlement['item_name']}，成交价{number_to(settlement['final_price'])}灵石，手续费{number_to(settlement['fee'])}灵石，收入{number_to(settlement['seller_earnings'])}灵石",
                    {"拍卖售出次数": 1, "拍卖收入灵石": settlement["seller_earnings"], "拍卖手续费消耗": settlement["fee"]},
                )
                safe_record_game_event(
                    settlement["seller_id"], "trade_sell", 1,
                    {
                        "source": "auction", "action": "auction_sell",
                        "stone_delta": int(settlement["seller_earnings"]),
                        "item_delta": [{"id": settlement["item_id"], "name": settlement["item_name"], "amount": -1}],
                        "detail": {"auction_id": settlement["auction_id"], "winner_id": settlement["winner_id"], "final_price": settlement["final_price"], "fee": settlement["fee"]},
                        "trace_id": trace_id,
                    },
                )
        elif settlement["seller_id"] != "0":
            record_trade_event(
                settlement["seller_id"], "拍卖流拍",
                f"{settlement['item_name']}流拍，已退回背包，拍卖ID:{settlement['auction_id']}",
                {"拍卖流拍次数": 1},
            )
    set_auction_status(
        active=False, start_time=None, end_time=None,
        last_display_refresh_time=None, items_count=0,
    )
    auction_config.clear_persisted_auction_status()
    logger.info("拍卖已结束，结算完成！")
    return auction_results


async def reconcile_auction_after_restart() -> None:
    """
    重启后对账：有落盘场次且未到结束时间 → 继续本场；否则库内遗留拍品 → 收尾结算。
    不向群里发公告。
    """
    _, _, _, auction_repository, session_service = _auction_dependencies()
    current_auctions = auction_repository.get_current_auction()
    if not current_auctions:
        return
    session = session_service.get_active_session()
    if session is None:
        raise RuntimeError("auction items exist without an active database session")
    now_dt = datetime.now()
    item_count = len(current_auctions)
    start_dt = datetime.fromtimestamp(session["start_time"])
    end_dt = datetime.fromtimestamp(session["end_time"])
    if now_dt >= end_dt:
        logger.info(
            f"拍卖重启后对账：已过结束时间（{end_dt.strftime('%m-%d %H:%M')}），"
            f"开始收尾，拍品 {item_count} 件。"
        )
        await end_auction_process(None)
        return
    set_auction_status(
        active=True, start_time=start_dt, end_time=end_dt,
        last_display_refresh_time=start_dt, items_count=item_count,
    )
    left_min = max(int((end_dt - now_dt).total_seconds()) // 60, 0)
    logger.info(
        f"拍卖重启后继续本场，预计 {end_dt.strftime('%H:%M')} 结束，"
        f"剩余约 {left_min} 分钟，拍品 {item_count} 件。"
    )


async def place_auction_bid(bot: Bot, user_id: str, user_name: str, auction_id: str, bid_price: int):
    """
    用户参与拍卖竞拍。
    """
    _, sql_message, _, auction_repository, _ = _auction_dependencies()
    auction_current_status = get_auction_status()
    if not auction_current_status["active"]:
        return False, "拍卖当前未开启！"

    item = auction_repository.get_current_auction(auction_id) # 获取单个拍卖品详情
    if not item:
        return False, "无效的拍卖品ID！"

    auction_rules = auction_config.get_auction_rules()
    ABSOLUTE_MIN_INCREMENT = auction_rules["min_bid_increment"]  # 绝对最低加价金额
    min_increment_percent = auction_rules["min_increment_percent"] # 最低加价百分比

    # 检查是否是首次出价
    if not item["bids"]:
        # 首次出价必须 >= 起拍价
        if bid_price < item["start_price"]:
            return False, (
                f"首次出价不得低于起拍价！\n"
                f"起拍价: {number_to(item['start_price'])}灵石\n"
                f"你的出价: {number_to(bid_price)}灵石"
            )
    else:
        # 非首次出价，需要满足最低加价规则
        required_min_increment = max(
            int(item["current_price"] * min_increment_percent),
            ABSOLUTE_MIN_INCREMENT
        )
        required_min_bid = item["current_price"] + required_min_increment

        if bid_price < required_min_bid:
            return False, (
                f"每次加价不得少于当前价格的 {int(min_increment_percent*100)}% 或 {number_to(ABSOLUTE_MIN_INCREMENT)}灵石！\n"
                f"当前最高价: {number_to(item['current_price'])}灵石\n"
                f"最低出价: {number_to(required_min_bid)}灵石\n"
                f"你的出价: {number_to(bid_price)}灵石"
            )

    # 不能自己竞拍自己的物品
    if item["seller_id"] == user_id:
        return False, "不能竞拍自己上架的物品！"

    # 获取用户当前灵石
    user_info = sql_message.get_user_info_with_id(user_id)
    if not user_info:
        return False, "用户信息获取失败！"

    user_id = str(user_id)
    old_current_price = int(item["current_price"])
    old_bids = {str(k): int(v) for k, v in item["bids"].items()}
    prev_winner_id = None
    prev_price = 0
    if old_bids:
        prev_winner_id, prev_price = max(old_bids.items(), key=lambda x: x[1])
        prev_winner_id = str(prev_winner_id)
        prev_price = int(prev_price)

    debit_amount = bid_price - old_bids.get(user_id, 0)
    if debit_amount <= 0:
        return False, "出价必须高于当前已锁定出价！"

    if user_info['stone'] < debit_amount:
        return False, f"灵石不足！当前拥有 {number_to(user_info['stone'])} 灵石，需要补足 {number_to(debit_amount)} 灵石"

    trace_id = f"trade:auction:{auction_id}"
    operation_id = f"auction-bid:{auction_id}:{user_id}:{bid_price}:{old_current_price}"
    bid_result = auction_repository.place_auction_bid(
        operation_id, auction_id, user_id, bid_price,
        old_current_price, old_bids, time.time(),
    )
    if bid_result.status == "stone_insufficient":
        return False, "灵石不足，竞拍失败！"
    if bid_result.status in {"state_changed", "bid_too_low"}:
        return False, "当前拍卖价格已变化，请重新出价！"
    if not bid_result.succeeded:
        return False, "竞拍状态发生变化，请重新查看拍卖！"
    record_trade_event(
        user_id,
        "拍卖竞拍",
        f"竞拍{item['name']}，出价{number_to(bid_price)}灵石，拍卖ID:{auction_id}",
        {"拍卖出价次数": 1, "拍卖出价灵石": bid_price}
    )

    # 构造返回消息
    msg_list = [
        f"【竞拍成功】",
        f"物品: {item['name']}",
        f"出价: {number_to(bid_price)}灵石",
        f"当前最高价: {number_to(bid_price)}灵石"
    ]

    if prev_winner_id and prev_winner_id != user_id:
        prev_winner_info = sql_message.get_user_info_with_id(prev_winner_id)
        prev_winner_name = prev_winner_info["user_name"] if prev_winner_info else str(prev_winner_id)
        msg_list.append(f"已退还 {prev_winner_name} 的 {number_to(prev_price)} 灵石")

    # 计算下次最低加价
    next_min_increment = max(int(bid_price * min_increment_percent), ABSOLUTE_MIN_INCREMENT)
    msg_list.append(f"\n下次最低加价: {number_to(next_min_increment)}灵石")

    return True, "\n".join(msg_list)
