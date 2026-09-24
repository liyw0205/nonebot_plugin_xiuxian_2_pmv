from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from threading import RLock
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
)
from .trade_utils import _trade_economy_context, record_trade_event
from datetime import datetime
from typing import Any
from ...infrastructure.clock import SystemClock
from ...infrastructure.random_source import SystemRandom
from ...infrastructure.ids import UUIDGenerator

runtime_clock = SystemClock()
runtime_random = SystemRandom()
runtime_ids = UUIDGenerator()

_items: Any = None
_sql_message: Any = None
_trade_manager: Any = None
_auction_repository: Any = None
_auction_session_service: Any = None
_auction_bid_application: Any = None
_auction_session_start_application: Any = None
_auction_settlement_application: Any = None
_auction_query_application: Any = None


def _resolve_dependency(dependency: Any) -> Any:
    return dependency() if callable(dependency) else dependency

def bind_auction_service_dependencies(
    *, items: Any, sql_message: Any, trade_manager: Any, auction_repository: Any,
    auction_session_service: Any, auction_bid_application: Any = None,
    auction_session_start_application: Any = None,
    auction_settlement_application: Any = None,
    auction_query_application: Any = None,
) -> None:
    global _items, _sql_message, _trade_manager, _auction_repository, _auction_session_service
    global _auction_bid_application, _auction_session_start_application, _auction_settlement_application
    global _auction_query_application
    _items = items
    _sql_message = sql_message
    _trade_manager = trade_manager
    _auction_repository = auction_repository
    _auction_session_service = auction_session_service
    _auction_bid_application = auction_bid_application
    _auction_session_start_application = auction_session_start_application
    _auction_settlement_application = auction_settlement_application
    _auction_query_application = auction_query_application

def _auction_dependencies() -> tuple[Any, Any, Any, Any, Any]:
    if (
        _items is None
        or _sql_message is None
        or _trade_manager is None
        or _auction_repository is None
        or _auction_session_service is None
    ):
        raise RuntimeError("auction service dependencies are not bound")
    sql_message = _resolve_dependency(_sql_message)
    trade_manager = _resolve_dependency(_trade_manager)
    session_service = _resolve_dependency(_auction_session_service)
    if session_service is None:
        raise RuntimeError("auction session service is not bound")
    if sql_message is None or trade_manager is None:
        raise RuntimeError("auction data dependencies are not bound")
    return _items, sql_message, trade_manager, _auction_repository, session_service


def _auction_query() -> Any:
    application = _resolve_dependency(_auction_query_application)
    if application is None:
        raise RuntimeError("auction query application is not bound")
    return application

def start_auction_process(bot: Optional[Bot], operation_id: str | None = None) -> bool: # bot参数可能为None
    """
    启动拍卖流程。
    从玩家上架区和系统配置中生成拍卖品，并存入当前拍卖表。
    """
    _, _, _, _, session_service = _auction_dependencies()
    operation_id = operation_id or f"auction-start:{runtime_ids.new_id()}"
    system_items_config = auction_config.get_system_items() # 从内置配置获取系统物品
    schedule_config = auction_config.get_auction_schedule()
    start_application = (
        _resolve_dependency(_auction_session_start_application)
        if _auction_session_start_application is not None
        else None
    )
    if start_application is not None:
        result = start_application.start(
            operation_id,
            system_items_config=system_items_config,
            duration_hours=schedule_config["duration_hours"],
        )
    else:
        previous = session_service.get_start_operation(operation_id)
        if previous is not None:
            active_session = session_service.get_active_session()
            return bool(active_session and active_session["session_id"] == previous.session_id)
        selected_names = runtime_random.sample(
            list(system_items_config), min(5, len(system_items_config))
        )
        selected_system_items = [
            {
                "item_id": system_items_config[name]["id"],
                "name": name,
                "start_price": system_items_config[name]["start_price"],
            }
            for name in selected_names
        ]
        now = runtime_clock.now()
        end_time = now + timedelta(hours=schedule_config["duration_hours"])
        session_id = f"auction:{now.strftime('%Y%m%d%H%M%S')}:{operation_id[-12:]}"
        result = session_service.start(
            operation_id,
            session_id,
            start_time=now.timestamp(),
            end_time=end_time.timestamp(),
            system_items=selected_system_items,
        )
    if not result.succeeded:
        logger.warning(f"拍卖开启失败：{result.status}")
        return False
    current_date = runtime_clock.now().strftime('%Y-%m-%d')
    auction_config.set_auction_config_value("schedule", current_date, "last_auto_start_date")
    logger.info(f"拍卖已开启，共 {result.items_count} 件物品参与拍卖！")
    return True

async def end_auction_process(
    bot: Optional[Bot], operation_id: str | None = None
) -> List[Dict[str, Any]]: # bot参数可能为None
    """Atomically settle every item in the active database auction session."""
    items, _, _, _, session_service = _auction_dependencies()
    current_auctions = _auction_query().get_current_auction()
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
    stable_operation_id = operation_id or f"auction-finish:{session['session_id']}"
    end_time = runtime_clock.now().timestamp()
    fee_rate = auction_config.get_auction_rules()["fee_rate"]
    settlement_application = (
        _resolve_dependency(_auction_settlement_application)
        if _auction_settlement_application is not None
        else None
    )
    settlement_replayed = False
    if settlement_application is not None:
        outcome = settlement_application.settle_active(
            operation_id=stable_operation_id,
            end_time=end_time,
            fee_rate=fee_rate,
            item_types=item_types,
        )
        if not outcome.ok:
            raise ValueError(
                f"auction session settlement blocked: status={outcome.code}"
            )
        auction_results = [dict(record) for record in (outcome.data or {}).get("results", ())]
        settlement_replayed = outcome.replayed
    else:
        result = session_service.finish(
            stable_operation_id,
            session["session_id"],
            end_time=end_time,
            fee_rate=fee_rate,
            item_types=item_types,
        )
        if not result.succeeded:
            raise ValueError(f"auction session settlement blocked: status={result.status}")
        auction_results = [dict(record) for record in result.results]
        settlement_replayed = result.status == "duplicate"
    if settlement_application is not None:
        logger.info("拍卖已结束，结算及副作用事件已提交！")
        return auction_results
    for settlement in auction_results:
        if settlement_replayed:
            continue
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
    logger.info("拍卖已结束，结算完成！")
    return auction_results

async def reconcile_auction_after_restart() -> None:
    """
    重启后对账：数据库场次未到结束时间则继续本场，否则收尾结算。
    不向群里发公告。
    """
    _, _, _, _, session_service = _auction_dependencies()
    current_auctions = _auction_query().get_current_auction()
    if not current_auctions:
        return
    session = session_service.get_active_session()
    if session is None:
        raise RuntimeError("auction items exist without an active database session")
    now_dt = runtime_clock.now()
    end_dt = datetime.fromtimestamp(session["end_time"], tz=now_dt.tzinfo)
    item_count = len(current_auctions)
    if now_dt >= end_dt:
        logger.info(
            f"拍卖重启后对账：已过结束时间（{end_dt.strftime('%m-%d %H:%M')}），"
            f"开始收尾，拍品 {item_count} 件。"
        )
        await end_auction_process(None)
        return
    left_min = max(int((end_dt - now_dt).total_seconds()) // 60, 0)
    logger.info(
        f"拍卖重启后继续本场，预计 {end_dt.strftime('%H:%M')} 结束，"
        f"剩余约 {left_min} 分钟，拍品 {item_count} 件。"
    )

async def place_auction_bid(bot: Bot, user_id: str, user_name: str, auction_id: str, bid_price: int):
    """
    用户参与拍卖竞拍。
    """
    _, sql_message, _, _, _ = _auction_dependencies()
    auction_current_status = get_auction_status()
    if not auction_current_status["active"]:
        return False, "拍卖尚未开启。"

    item = _auction_query().get_current_auction(auction_id)
    if not item:
        return False, "未找到该拍品，编号有误或已结拍。"

    auction_rules = auction_config.get_auction_rules()
    ABSOLUTE_MIN_INCREMENT = auction_rules["min_bid_increment"]
    min_increment_percent = auction_rules["min_increment_percent"]

    if not item["bids"]:
        if bid_price < item["start_price"]:
            return False, (
                f"首次出价不得低于起拍价。\n"
                f"起拍价：{number_to(item['start_price'])}灵石\n"
                f"本次出价：{number_to(bid_price)}灵石"
            )
    else:
        required_min_increment = max(
            int(item["current_price"] * min_increment_percent),
            ABSOLUTE_MIN_INCREMENT
        )
        required_min_bid = item["current_price"] + required_min_increment
        if bid_price < required_min_bid:
            return False, (
                f"加价不足。\n"
                f"当前价：{number_to(item['current_price'])}灵石\n"
                f"最低出价：{number_to(required_min_bid)}灵石\n"
                f"（加价不少于现价的{int(min_increment_percent*100)}%，或{number_to(ABSOLUTE_MIN_INCREMENT)}灵石）"
            )

    if item["seller_id"] == user_id:
        return False, "不可竞拍自身上架之物。"

    user_info = sql_message.get_user_info_with_id(user_id)
    if not user_info:
        return False, "未能读取道友修仙信息，请稍后再试。"

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
        return False, "出价须高于道友当前已锁定之价。"

    if user_info['stone'] < debit_amount:
        return False, (
            f"灵石不足。\n"
            f"当前灵石：{number_to(user_info['stone'])}\n"
            f"尚需补足：{number_to(debit_amount)}"
        )

    trace_id = f"trade:auction:{auction_id}"
    operation_id = f"auction-bid:{auction_id}:{user_id}:{bid_price}:{old_current_price}"
    bid_application = (
        _resolve_dependency(_auction_bid_application)
        if _auction_bid_application is not None
        else None
    )
    if bid_application is not None:
        outcome = bid_application.place_bid(
            operation_id=operation_id,
            auction_id=auction_id,
            bidder_id=user_id,
            bid_price=bid_price,
            expected_price=old_current_price,
            expected_bids=old_bids,
            bid_time=runtime_clock.now().timestamp(),
            item_name=str(item.get("name", auction_id)),
        )
        bid_status = str((outcome.data or {}).get("status", outcome.code or outcome.status))
        bid_replayed = bool(outcome.replayed)
        bid_result = None
    else:
        bid_result = auction_repository.place_auction_bid(
            operation_id, auction_id, user_id, bid_price,
            old_current_price, old_bids, time.time(),
        )
        bid_status = bid_result.status
        bid_replayed = bid_status == "duplicate"
    if bid_status == "stone_insufficient":
        return False, "灵石不足，竞拍未成立。"
    if bid_status == "bid_too_low":
        return False, "出价已低于当前价，请先【拍卖查看】确认后再出。"
    if bid_status == "state_changed":
        return False, "拍品价格已更新，请重新查看后再出价。"
    if bid_status == "auction_missing":
        return False, "该拍品已结拍或不存在。"
    if bid_status == "self_bid":
        return False, "不可竞拍自身上架之物。"
    if bid_status not in {"bid", "duplicate"}:
        return False, "竞拍未成立，请刷新列表后重试。"
    if bid_application is None and not bid_replayed:
        record_trade_event(
            user_id,
            "拍卖竞拍",
            f"竞拍{item['name']}，出价{number_to(bid_price)}灵石，拍卖ID:{auction_id}",
            {"拍卖出价次数": 1, "拍卖出价灵石": bid_price},
        )
    msg_list = [
        f"【竞拍成功】",
        f"拍品：{item['name']}",
        f"出价：{number_to(bid_price)}灵石",
        f"当前价：{number_to(bid_price)}灵石",
    ]

    if prev_winner_id and prev_winner_id != user_id:
        prev_winner_info = sql_message.get_user_info_with_id(prev_winner_id)
        prev_winner_name = prev_winner_info["user_name"] if prev_winner_info else str(prev_winner_id)
        msg_list.append(f"已退还{prev_winner_name}锁定灵石{number_to(prev_price)}")

    next_min_increment = max(int(bid_price * min_increment_percent), ABSOLUTE_MIN_INCREMENT)
    msg_list.append(f"下次最低加价：{number_to(next_min_increment)}灵石")

    return True, "\n".join(msg_list)

@dataclass(frozen=True)
class AuctionQueueResult:
    status: str
    action: str
    user_id: str
    item_id: int
    item_name: str = ""
    start_price: int = 0
    user_name: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"completed", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "completed"

class AuctionQueueService:
    """Compatibility facade for feature-owned auction queue transactions."""

    def __init__(
        self,
        game_database: str | Path,
        trade_database: str | Path,
        max_goods_num: int,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._trade_database = Path(trade_database)
        self._max_goods_num = max(int(max_goods_num), 1)
        from ...features.auction.queue_repository import AuctionQueueSqlRepository

        self._repository = AuctionQueueSqlRepository(
            self._game_database, self._trade_database, self._max_goods_num
        )

    @staticmethod
    def _compat_result(result):
        if result is None:
            return None
        return AuctionQueueResult(
            result.status,
            result.action,
            result.user_id,
            result.item_id,
            result.item_name,
            result.start_price,
            result.user_name,
        )

    def get_operation(self, operation_id, action, user_id, item_id):
        return self._compat_result(
            self._repository.get_operation(operation_id, action, user_id, item_id)
        )

    def enqueue(
        self,
        operation_id,
        user_id,
        item_id,
        item_name,
        start_price,
        user_name,
        *,
        max_user_items: int,
    ) -> AuctionQueueResult:
        return self._compat_result(
            self._repository.enqueue(
                operation_id,
                user_id,
                item_id,
                item_name,
                start_price,
                user_name,
                max_user_items=max_user_items,
            )
        )

    def dequeue(self, operation_id, user_id, item_id, item_type) -> AuctionQueueResult:
        return self._compat_result(
            self._repository.dequeue(operation_id, user_id, item_id, item_type)
        )

from ...compatibility.legacy_trade_auction_sessions import (
    AuctionSessionFinishResult,
    AuctionSessionService,
    AuctionSessionStartResult,
)

__all__ = [
    "AuctionQueueResult",
    "AuctionQueueService",
    "AuctionSessionStartResult",
    "AuctionSessionFinishResult",
    "AuctionSessionService",
]
