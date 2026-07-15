from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from uuid import uuid4
from .repository import TradeRepository, XianshiPurchase
from ..xiuxian_utils import db_backend
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
import hashlib
from typing import Any
from ..xiuxian_utils.numeric_bind import as_int_like, number_count

class XianshiPurchaseService:
    def __init__(self, repository: TradeRepository) -> None:
        self._repository = repository

    def purchase(
        self,
        buyer_id,
        listing_id,
        quantity,
        *,
        operation_id: str | None = None,
        stamina_operation_id: str | None = None,
        stamina_cost: int = 0,
    ) -> XianshiPurchase:
        operation_id = operation_id or f"xianshi:{listing_id}:{buyer_id}:{uuid4().hex}"
        return self._repository.purchase_xianshi_item(
            operation_id,
            str(buyer_id),
            str(listing_id),
            quantity,
            stamina_operation_id=stamina_operation_id,
            stamina_cost=stamina_cost,
        )

@dataclass(frozen=True)
class GuishiStoneResult:
    status: str
    operation_type: str
    user_id: str
    amount: int
    fee: int = 0
    actual_amount: int = 0
    stored_balance: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"completed", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "completed"

class GuishiStoneService:
    """Move stones between a player and the Guishi account atomically."""

    def __init__(
        self,
        game_database: str | Path,
        trade_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._trade_database = Path(trade_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS guishi_stone_operations (
                operation_id TEXT PRIMARY KEY,
                operation_type TEXT NOT NULL,
                user_id TEXT NOT NULL,
                amount INTEGER NOT NULL,
                fee INTEGER NOT NULL,
                actual_amount INTEGER NOT NULL,
                stored_balance INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS guishi_trade.guishi_info (
                user_id TEXT PRIMARY KEY,
                stored_stone INTEGER DEFAULT 0,
                items TEXT DEFAULT '{}'
            )
            """
        )

    @staticmethod
    def _fee_for_balance(stored_balance: int, amount: int) -> int:
        fee_rate = 0.2
        if stored_balance > 10_000_000_000:
            excess = stored_balance - 10_000_000_000
            fee_rate += (excess // 10_000_000_000) * 0.05
        return int(amount * min(fee_rate, 0.8))

    @staticmethod
    def _result_from_row(status: str, row) -> GuishiStoneResult:
        operation_type, user_id, amount, fee, actual_amount, balance = row
        return GuishiStoneResult(
            status,
            str(operation_type),
            str(user_id),
            as_int_like(amount),
            int(fee),
            int(actual_amount),
            int(balance),
        )

    def _execute(self, operation_id, operation_type, user_id, amount) -> GuishiStoneResult:
        operation_id = str(operation_id).strip()
        operation_type = str(operation_type)
        user_id = str(user_id)
        amount = as_int_like(amount)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if operation_type not in {"deposit", "withdraw"}:
            raise ValueError("unsupported operation_type")
        if amount <= 0:
            raise ValueError("amount must be positive")

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS guishi_trade", (str(self._trade_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT operation_type, user_id, amount, fee, actual_amount, "
                    "stored_balance FROM guishi_stone_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != operation_type or str(previous[1]) != user_id:
                        return GuishiStoneResult(
                            "state_changed", operation_type, user_id, amount
                        )
                    return self._result_from_row("duplicate", previous)

                player = conn.execute(
                    "SELECT COALESCE(stone, 0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if player is None:
                    conn.rollback()
                    return GuishiStoneResult("player_missing", operation_type, user_id, amount)

                stored = conn.execute(
                    "SELECT COALESCE(stored_stone, 0) FROM guishi_trade.guishi_info "
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                stored_balance = int(stored[0]) if stored is not None else 0

                if operation_type == "deposit":
                    charged = conn.execute(
                        "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(%s AS REAL) "
                        "WHERE user_id=%s AND COALESCE(stone, 0)>=%s",
                        (amount, user_id, amount),
                    )
                    if charged.rowcount != 1:
                        conn.rollback()
                        return GuishiStoneResult(
                            "stone_insufficient", operation_type, user_id, amount,
                            stored_balance=stored_balance,
                        )
                    new_balance = stored_balance + amount
                    fee = 0
                    actual_amount = amount
                else:
                    if stored_balance < amount:
                        conn.rollback()
                        return GuishiStoneResult(
                            "stored_insufficient", operation_type, user_id, amount,
                            stored_balance=stored_balance,
                        )
                    fee = self._fee_for_balance(stored_balance, amount)
                    actual_amount = amount - fee
                    new_balance = stored_balance - amount
                    conn.execute(
                        "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s",
                        (actual_amount, user_id),
                    )

                conn.execute(
                    """
                    INSERT INTO guishi_trade.guishi_info (user_id, stored_stone, items)
                    VALUES (%s, %s, '{}')
                    ON CONFLICT (user_id) DO UPDATE SET stored_stone=EXCLUDED.stored_stone
                    """,
                    (user_id, new_balance),
                )
                conn.execute(
                    "INSERT INTO guishi_stone_operations "
                    "(operation_id, operation_type, user_id, amount, fee, "
                    "actual_amount, stored_balance) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        operation_type,
                        user_id,
                        amount,
                        fee,
                        actual_amount,
                        new_balance,
                    ),
                )
                conn.commit()
                return GuishiStoneResult(
                    "completed",
                    operation_type,
                    user_id,
                    amount,
                    fee,
                    actual_amount,
                    new_balance,
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE guishi_trade")

    def deposit(self, operation_id, user_id, amount) -> GuishiStoneResult:
        return self._execute(operation_id, "deposit", user_id, amount)

    def withdraw(self, operation_id, user_id, amount) -> GuishiStoneResult:
        return self._execute(operation_id, "withdraw", user_id, amount)

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
        active_session = session_service.get_active_session()
        return bool(
            active_session
            and active_session["session_id"] == previous.session_id
        )
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
    logger.info("拍卖已结束，结算完成！")
    return auction_results

async def reconcile_auction_after_restart() -> None:
    """
    重启后对账：数据库场次未到结束时间则继续本场，否则收尾结算。
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
    end_dt = datetime.fromtimestamp(session["end_time"])
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
    """Atomically move items between inventory and the auction waiting queue."""

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
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auction_queue_operations (
                operation_id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                user_id TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                item_name TEXT NOT NULL,
                start_price INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auction_trade.auction_player_upload (
                user_id TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                item_name TEXT NOT NULL,
                start_price INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                PRIMARY KEY (user_id, item_id)
            )
            """
        )

    @staticmethod
    def _result(status: str, action: str, row) -> AuctionQueueResult:
        user_id, item_id, item_name, start_price, user_name = row
        return AuctionQueueResult(
            status,
            action,
            str(user_id),
            int(item_id),
            str(item_name),
            int(start_price),
            str(user_name),
        )

    def _connect(self):
        conn = db_backend.connect(self._game_database)
        conn.execute("ATTACH DATABASE %s AS auction_trade", (str(self._trade_database),))
        return conn

    def _previous(self, conn, operation_id, action, user_id, item_id):
        row = conn.execute(
            "SELECT action, user_id, item_id, item_name, start_price, user_name "
            "FROM auction_queue_operations WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        if row is None:
            return None
        previous_action, previous_user, previous_item, name, price, user_name = row
        if (
            str(previous_action) != action
            or str(previous_user) != user_id
            or int(previous_item) != item_id
        ):
            return AuctionQueueResult("state_changed", action, user_id, item_id)
        return AuctionQueueResult(
            "duplicate", action, user_id, item_id, str(name), int(price), str(user_name)
        )

    def get_operation(self, operation_id, action, user_id, item_id):
        operation_id = str(operation_id).strip()
        action = str(action)
        user_id = str(user_id)
        item_id = int(item_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("auction_queue_operations",),
            ).fetchone()
            if exists is None:
                return None
            return self._previous(conn, operation_id, action, user_id, item_id)

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
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        item_name = str(item_name)
        start_price = int(start_price)
        user_name = str(user_name)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if start_price <= 0:
            raise ValueError("start_price must be positive")

        with self._lock, closing(self._connect()) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = self._previous(
                    conn, operation_id, "enqueue", user_id, item_id
                )
                if previous is not None:
                    conn.rollback()
                    return previous
                queued = conn.execute(
                    "SELECT COUNT(*) FROM auction_trade.auction_player_upload "
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()[0]
                if int(queued) >= max(int(max_user_items), 1):
                    conn.rollback()
                    return AuctionQueueResult("limit_reached", "enqueue", user_id, item_id)
                if conn.execute(
                    "SELECT 1 FROM auction_trade.auction_player_upload "
                    "WHERE user_id=%s AND item_id=%s",
                    (user_id, item_id),
                ).fetchone():
                    conn.rollback()
                    return AuctionQueueResult("already_queued", "enqueue", user_id, item_id)

                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                consumed = conn.execute(
                    """
                    UPDATE back
                    SET goods_num=goods_num-1,
                        bind_num=LEAST(COALESCE(bind_num, 0), goods_num-1),
                        update_time=%s
                    WHERE user_id=%s AND goods_id=%s
                      AND COALESCE(goods_num, 0)-COALESCE(bind_num, 0)
                          -COALESCE(state, 0) >= 1
                    """,
                    (now, user_id, item_id),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return AuctionQueueResult("stock_insufficient", "enqueue", user_id, item_id)
                conn.execute(
                    "INSERT INTO auction_trade.auction_player_upload "
                    "(user_id, item_id, item_name, start_price, user_name) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (user_id, item_id, item_name, start_price, user_name),
                )
                conn.execute(
                    "INSERT INTO auction_queue_operations "
                    "(operation_id, action, user_id, item_id, item_name, "
                    "start_price, user_name) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        "enqueue",
                        user_id,
                        item_id,
                        item_name,
                        start_price,
                        user_name,
                    ),
                )
                conn.commit()
                return AuctionQueueResult(
                    "completed", "enqueue", user_id, item_id,
                    item_name, start_price, user_name
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE auction_trade")

    def dequeue(self, operation_id, user_id, item_id, item_type) -> AuctionQueueResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        item_type = str(item_type)
        if not operation_id:
            raise ValueError("operation_id must not be empty")

        with self._lock, closing(self._connect()) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = self._previous(
                    conn, operation_id, "dequeue", user_id, item_id
                )
                if previous is not None:
                    conn.rollback()
                    return previous
                row = conn.execute(
                    "SELECT user_id, item_id, item_name, start_price, user_name "
                    "FROM auction_trade.auction_player_upload "
                    "WHERE user_id=%s AND item_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return AuctionQueueResult("queue_missing", "dequeue", user_id, item_id)
                inventory = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back "
                    "WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if inventory is not None and int(inventory[0]) >= self._max_goods_num:
                    conn.rollback()
                    return AuctionQueueResult("inventory_full", "dequeue", user_id, item_id)
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                conn.execute(
                    """
                    INSERT INTO back (
                        user_id, goods_id, goods_name, goods_type,
                        goods_num, create_time, update_time, bind_num
                    ) VALUES (%s, %s, %s, %s, 1, %s, %s, 1)
                    ON CONFLICT (user_id, goods_id) DO UPDATE
                    SET goods_num=COALESCE(back.goods_num, 0)+1,
                        bind_num=COALESCE(back.bind_num, 0)+1,
                        update_time=EXCLUDED.update_time
                    """,
                    (user_id, item_id, str(row[2]), item_type, now, now),
                )
                conn.execute(
                    "DELETE FROM auction_trade.auction_player_upload "
                    "WHERE user_id=%s AND item_id=%s",
                    (user_id, item_id),
                )
                conn.execute(
                    "INSERT INTO auction_queue_operations "
                    "(operation_id, action, user_id, item_id, item_name, "
                    "start_price, user_name) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, "dequeue", *row),
                )
                conn.commit()
                return self._result("completed", "dequeue", row)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE auction_trade")

@dataclass(frozen=True)
class AuctionSessionStartResult:
    status: str
    operation_id: str
    session_id: str = ""
    start_time: float = 0.0
    end_time: float = 0.0
    items_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"started", "duplicate"}

@dataclass(frozen=True)
class AuctionSessionFinishResult:
    status: str
    operation_id: str
    session_id: str = ""
    results: tuple[dict[str, Any], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate"}

class AuctionSessionService:
    """Use the game database as the durable truth for an auction session."""

    def __init__(
        self,
        game_database: str | Path,
        trade_database: str | Path,
        max_goods_num: int | None = None,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._trade_database = Path(trade_database)
        self._max_goods_num = max(int(max_goods_num or 1), 1)
        self._lock = lock or RLock()

    def _connect(self):
        conn = db_backend.connect(self._game_database)
        conn.execute("ATTACH DATABASE %s AS auction_trade", (str(self._trade_database),))
        return conn

    @staticmethod
    def _ensure_schema(conn, *, include_trade: bool = False) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auction_sessions (
                session_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                start_time REAL NOT NULL,
                end_time REAL NOT NULL,
                items_count INTEGER NOT NULL,
                start_operation_id TEXT NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS auction_one_active_session
            ON auction_sessions(status) WHERE status='active'
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auction_session_operations (
                operation_id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                payload TEXT NOT NULL,
                result TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(auction_sessions)").fetchall()
        }
        if "finish_operation_id" not in columns:
            conn.execute("ALTER TABLE auction_sessions ADD COLUMN finish_operation_id TEXT")
        if "settled_at" not in columns:
            conn.execute("ALTER TABLE auction_sessions ADD COLUMN settled_at REAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auction_current (
                id TEXT PRIMARY KEY, item_id INTEGER NOT NULL, name TEXT NOT NULL,
                start_price INTEGER NOT NULL, current_price INTEGER NOT NULL,
                seller_id TEXT NOT NULL, seller_name TEXT NOT NULL,
                bids TEXT DEFAULT '{}', bid_times TEXT DEFAULT '{}',
                is_system INTEGER DEFAULT 0, last_bid_time REAL DEFAULT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auction_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT, auction_id TEXT NOT NULL,
                item_id INTEGER NOT NULL, item_name TEXT NOT NULL,
                start_price INTEGER NOT NULL, final_price INTEGER,
                seller_id TEXT NOT NULL, seller_name TEXT NOT NULL,
                winner_id TEXT, winner_name TEXT, status TEXT NOT NULL,
                fee INTEGER, seller_earnings INTEGER,
                start_time REAL NOT NULL, end_time REAL NOT NULL
            )
            """
        )
        if include_trade:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS auction_trade.auction_player_upload (
                    user_id TEXT NOT NULL, item_id INTEGER NOT NULL,
                    item_name TEXT NOT NULL, start_price INTEGER NOT NULL,
                    user_name TEXT NOT NULL, PRIMARY KEY (user_id, item_id)
                )
                """
            )

    @staticmethod
    def _payload(value: dict[str, Any]) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _read_operation(conn, operation_id: str, action: str, payload: str):
        row = conn.execute(
            "SELECT action, payload, result FROM auction_session_operations WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        if row is None:
            return None
        if str(row[0]) != action or str(row[1]) != payload:
            return "state_changed", None
        return "duplicate", json.loads(row[2])

    @staticmethod
    def _auction_id(session_id: str, index: int) -> str:
        digest = hashlib.sha256(f"{session_id}:{index}".encode("utf-8")).hexdigest()
        return digest[:8]

    def get_active_session(self) -> dict[str, Any] | None:
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            self._ensure_schema(conn)
            row = conn.execute(
                "SELECT session_id, start_time, end_time, items_count "
                "FROM auction_sessions WHERE status='active'"
            ).fetchone()
            if row is None:
                return None
            return {
                "session_id": str(row[0]),
                "start_time": float(row[1]),
                "end_time": float(row[2]),
                "items_count": int(row[3]),
            }

    def get_start_operation(self, operation_id: str) -> AuctionSessionStartResult | None:
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            self._ensure_schema(conn)
            row = conn.execute(
                "SELECT result FROM auction_session_operations "
                "WHERE operation_id=%s AND action='start'", (str(operation_id),)
            ).fetchone()
            if row is None:
                return None
            value = json.loads(row[0])
            return AuctionSessionStartResult(
                "duplicate", str(operation_id), str(value["session_id"]),
                float(value["start_time"]), float(value["end_time"]),
                int(value["items_count"]),
            )

    def start(
        self,
        operation_id: str,
        session_id: str,
        *,
        start_time: float,
        end_time: float,
        system_items: list[dict[str, Any]],
    ) -> AuctionSessionStartResult:
        operation_id = str(operation_id).strip()
        session_id = str(session_id).strip()
        if not operation_id or not session_id:
            raise ValueError("operation_id and session_id must not be empty")
        normalized_system = [
            {
                "item_id": int(item["item_id"]),
                "name": str(item["name"]),
                "start_price": int(item["start_price"]),
            }
            for item in system_items
        ]
        payload = self._payload(
            {
                "session_id": session_id,
                "start_time": float(start_time),
                "end_time": float(end_time),
                "system_items": normalized_system,
            }
        )
        with self._lock, closing(self._connect()) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn, include_trade=True)
                previous = self._read_operation(conn, operation_id, "start", payload)
                if previous is not None:
                    conn.rollback()
                    if previous[0] == "state_changed":
                        return AuctionSessionStartResult("state_changed", operation_id)
                    value = previous[1]
                    return AuctionSessionStartResult(
                        "duplicate", operation_id, str(value["session_id"]),
                        float(value["start_time"]), float(value["end_time"]),
                        int(value["items_count"]),
                    )
                if conn.execute(
                    "SELECT 1 FROM auction_sessions WHERE status='active'"
                ).fetchone() or conn.execute("SELECT 1 FROM auction_current LIMIT 1").fetchone():
                    conn.rollback()
                    return AuctionSessionStartResult("already_active", operation_id)

                queue = conn.execute(
                    "SELECT user_id, item_id, item_name, start_price, user_name "
                    "FROM auction_trade.auction_player_upload ORDER BY user_id, item_id"
                ).fetchall()
                all_items = list(normalized_system)
                all_items.extend(
                    {
                        "item_id": int(row[1]), "name": str(row[2]),
                        "start_price": int(row[3]), "seller_id": str(row[0]),
                        "seller_name": str(row[4]),
                    }
                    for row in queue
                )
                if not all_items:
                    conn.rollback()
                    return AuctionSessionStartResult("empty", operation_id)

                for index, item in enumerate(all_items):
                    conn.execute(
                        """
                        INSERT INTO auction_current (
                            id, item_id, name, start_price, current_price,
                            seller_id, seller_name, bids, bid_times,
                            is_system, last_bid_time
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,'{}','{}',%s,%s)
                        """,
                        (
                            self._auction_id(session_id, index), item["item_id"], item["name"],
                            item["start_price"], item["start_price"],
                            item.get("seller_id", "0"), item.get("seller_name", "系统"),
                            0 if "seller_id" in item else 1, float(start_time),
                        ),
                    )
                conn.execute("DELETE FROM auction_trade.auction_player_upload")
                count = len(all_items)
                conn.execute(
                    "INSERT INTO auction_sessions (session_id,status,start_time,end_time,items_count,"
                    "start_operation_id,created_at) VALUES (%s,'active',%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                    (session_id, float(start_time), float(end_time), count, operation_id),
                )
                result = {
                    "session_id": session_id, "start_time": float(start_time),
                    "end_time": float(end_time), "items_count": count,
                }
                conn.execute(
                    "INSERT INTO auction_session_operations (operation_id,action,payload,result) VALUES (%s,'start',%s,%s)",
                    (operation_id, payload, self._payload(result)),
                )
                conn.commit()
                return AuctionSessionStartResult(
                    "started", operation_id, session_id, float(start_time), float(end_time), count
                )
            except Exception:
                conn.rollback()
                raise

    def finish(
        self,
        operation_id: str,
        session_id: str,
        *,
        end_time: float,
        fee_rate: float,
        item_types: dict[int, str],
    ) -> AuctionSessionFinishResult:
        operation_id = str(operation_id).strip()
        session_id = str(session_id).strip()
        normalized_types = {str(int(key)): str(value) for key, value in item_types.items()}
        payload = self._payload(
            {"session_id": session_id, "fee_rate": float(fee_rate),
             "item_types": normalized_types}
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = self._read_operation(conn, operation_id, "finish", payload)
                if previous is not None:
                    conn.rollback()
                    if previous[0] == "state_changed":
                        return AuctionSessionFinishResult("state_changed", operation_id, session_id)
                    return AuctionSessionFinishResult(
                        "duplicate", operation_id, session_id, tuple(previous[1]["results"])
                    )
                session = conn.execute(
                    "SELECT start_time FROM auction_sessions WHERE session_id=%s AND status='active'",
                    (session_id,),
                ).fetchone()
                if session is None:
                    conn.rollback()
                    return AuctionSessionFinishResult("not_active", operation_id, session_id)
                rows = conn.execute(
                    "SELECT id,item_id,name,start_price,seller_id,seller_name,bids,is_system,last_bid_time "
                    "FROM auction_current ORDER BY id"
                ).fetchall()
                results: list[dict[str, Any]] = []
                for row in rows:
                    auction_id, item_id, name = str(row[0]), int(row[1]), str(row[2])
                    seller_id, seller_name = str(row[4]), str(row[5])
                    is_system = bool(row[7])
                    bids_value = json.loads(row[6] or "{}")
                    bids = {str(key): int(value) for key, value in bids_value.items()}
                    winner_id = final_price = winner_name = None
                    fee = earnings = 0
                    status = "流拍"
                    item_type = normalized_types.get(str(item_id))
                    if (bids or not is_system) and not item_type:
                        conn.rollback()
                        return AuctionSessionFinishResult("item_missing", operation_id, session_id)
                    if bids:
                        winner_id, final_price = max(bids.items(), key=lambda entry: entry[1])
                        winner = conn.execute(
                            "SELECT user_name FROM user_xiuxian WHERE user_id=%s", (winner_id,)
                        ).fetchone()
                        seller = True if is_system else conn.execute(
                            "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (seller_id,)
                        ).fetchone()
                        if winner is None or not seller:
                            conn.rollback()
                            return AuctionSessionFinishResult("participant_missing", operation_id, session_id)
                        winner_name = str(winner[0] or winner_id)
                        if self._inventory_full(conn, winner_id, item_id):
                            conn.rollback()
                            return AuctionSessionFinishResult("inventory_full", operation_id, session_id)
                        self._grant_item(conn, winner_id, item_id, name, item_type)
                        fee = 0 if is_system else int(final_price * float(fee_rate))
                        earnings = 0 if is_system else final_price - fee
                        if not is_system:
                            conn.execute(
                                "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s",
                                (earnings, seller_id),
                            )
                        for bidder_id, locked in bids.items():
                            if bidder_id != winner_id and conn.execute(
                                "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s",
                                (locked, bidder_id),
                            ).rowcount != 1:
                                conn.rollback()
                                return AuctionSessionFinishResult("participant_missing", operation_id, session_id)
                        status = "成交"
                    elif not is_system:
                        if not conn.execute(
                            "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (seller_id,)
                        ).fetchone():
                            conn.rollback()
                            return AuctionSessionFinishResult("participant_missing", operation_id, session_id)
                        if self._inventory_full(conn, seller_id, item_id):
                            conn.rollback()
                            return AuctionSessionFinishResult("inventory_full", operation_id, session_id)
                        self._grant_item(conn, seller_id, item_id, name, item_type)

                    record = {
                        "auction_id": auction_id, "item_id": item_id, "item_name": name,
                        "start_price": int(row[3]), "final_price": final_price,
                        "seller_id": seller_id, "seller_name": seller_name,
                        "winner_id": winner_id, "winner_name": winner_name,
                        "status": status, "fee": fee, "seller_earnings": earnings,
                        "start_time": float(row[8] or session[0]), "end_time": float(end_time),
                    }
                    conn.execute(
                        """
                        INSERT INTO auction_history (
                            auction_id,item_id,item_name,start_price,final_price,
                            seller_id,seller_name,winner_id,winner_name,status,fee,
                            seller_earnings,start_time,end_time
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        tuple(record[key] for key in (
                            "auction_id", "item_id", "item_name", "start_price", "final_price",
                            "seller_id", "seller_name", "winner_id", "winner_name", "status", "fee",
                            "seller_earnings", "start_time", "end_time",
                        )),
                    )
                    results.append(record)
                conn.execute("DELETE FROM auction_current")
                conn.execute(
                    "UPDATE auction_sessions SET status='settled',finish_operation_id=%s,settled_at=%s "
                    "WHERE session_id=%s AND status='active'",
                    (operation_id, float(end_time), session_id),
                )
                result_value = {"session_id": session_id, "results": results}
                conn.execute(
                    "INSERT INTO auction_session_operations (operation_id,action,payload,result) "
                    "VALUES (%s,'finish',%s,%s)",
                    (operation_id, payload, self._payload(result_value)),
                )
                conn.commit()
                return AuctionSessionFinishResult(
                    "settled", operation_id, session_id, tuple(results)
                )
            except Exception:
                conn.rollback()
                raise

    def _inventory_full(self, conn, user_id: str, item_id: int) -> bool:
        row = conn.execute(
            "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
            (user_id, item_id),
        ).fetchone()
        return bool(row and int(row[0] or 0) >= self._max_goods_num)

    @staticmethod
    def _grant_item(conn, user_id: str, item_id: int, name: str, item_type: str) -> None:
        conn.execute(
            """
            INSERT INTO back (
                user_id,goods_id,goods_name,goods_type,goods_num,
                create_time,update_time,bind_num
            ) VALUES (%s,%s,%s,%s,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,1)
            ON CONFLICT (user_id,goods_id) DO UPDATE SET
                goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,
                goods_num=COALESCE(back.goods_num,0)+1,
                bind_num=COALESCE(back.bind_num,0)+1,
                update_time=CURRENT_TIMESTAMP
            """,
            (user_id, item_id, name, item_type),
        )

__all__ = [
    "XianshiPurchaseService",
    "GuishiStoneResult",
    "GuishiStoneService",
    "AuctionQueueResult",
    "AuctionQueueService",
    "AuctionSessionStartResult",
    "AuctionSessionFinishResult",
    "AuctionSessionService",
]
