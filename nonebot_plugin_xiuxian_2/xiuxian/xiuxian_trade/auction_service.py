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
    _restore_auction_status_from_disk,
    get_auction_status,
    set_auction_status,
)
from .trade_utils import _trade_economy_context, record_trade_event


_items: Any = None
_sql_message: Any = None
_trade_manager: Any = None
_auction_repository: Any = None


def bind_auction_service_dependencies(
    *, items: Any, sql_message: Any, trade_manager: Any, auction_repository: Any
) -> None:
    global _items, _sql_message, _trade_manager, _auction_repository
    _items = items
    _sql_message = sql_message
    _trade_manager = trade_manager
    _auction_repository = auction_repository


def _auction_dependencies() -> tuple[Any, Any, Any, Any]:
    if (
        _items is None
        or _sql_message is None
        or _trade_manager is None
        or _auction_repository is None
    ):
        raise RuntimeError("auction service dependencies are not bound")
    return _items, _sql_message, _trade_manager, _auction_repository


def start_auction_process(bot: Optional[Bot]) -> bool: # bot参数可能为None
    """
    启动拍卖流程。
    从玩家上架区和系统配置中生成拍卖品，并存入当前拍卖表。
    """
    _, _, trade_manager, auction_repository = _auction_dependencies()
    auction_current_status = get_auction_status()
    if auction_current_status["active"]:
        logger.warning("拍卖已在运行中，无法重复开启！")
        return False

    player_auctions = trade_manager.get_player_auction_items() # 从数据库获取玩家上架物品
    system_items_config = auction_config.get_system_items() # 从内置配置获取系统物品

    schedule_config = auction_config.get_auction_schedule()

    # 随机选择5个系统拍卖品
    selected_system_items_names = random.sample(list(system_items_config.keys()), min(5, len(system_items_config)))
    selected_system_items = [
        {"id": trade_manager.generate_unique_id("auction_current"), # 生成唯一的拍卖ID
         "item_id": system_items_config[name]["id"],
         "name": name,
         "start_price": system_items_config[name]["start_price"],
         "current_price": system_items_config[name]["start_price"],
         "seller_id": 0,  # 系统卖方ID
         "seller_name": "系统",
         "bids": {},
         "bid_times": {},
         "is_system": True,
         "last_bid_time": datetime.now().timestamp() # 记录拍卖开始时间，作为拍卖品的基准时间 (仍使用timestamp方便竞价逻辑)
        } for name in selected_system_items_names
    ]

    # 添加玩家拍卖品
    player_auction_items: List[Dict[str, Any]] = []
    for player_item in player_auctions:
        player_auction_items.append({
            "id": trade_manager.generate_unique_id("auction_current"), # 生成唯一的拍卖ID
            "item_id": player_item["item_id"],
            "name": player_item["item_name"], # 玩家上架的是 item_name
            "start_price": player_item["start_price"],
            "current_price": player_item["start_price"],
            "seller_id": player_item["user_id"],
            "seller_name": player_item["user_name"],
            "bids": {},
            "bid_times": {},
            "is_system": False,
            "last_bid_time": datetime.now().timestamp() # 记录拍卖开始时间，作为拍卖品的基准时间
        })

    # 合并所有拍卖品
    all_auction_items = selected_system_items + player_auction_items

    if not all_auction_items:
        logger.warning("没有可供拍卖的物品！")
        return False

    # 将所有拍卖品存入数据库
    auction_repository.set_current_auction(all_auction_items)

    # 清空玩家上架等待区
    trade_manager.clear_player_auctions()

    # 更新拍卖状态
    now_dt = datetime.now()
    end_time_dt = now_dt + timedelta(hours=schedule_config["duration_hours"])
    set_auction_status(active=True, start_time=now_dt, end_time=end_time_dt, last_display_refresh_time=now_dt, items_count=len(all_auction_items))

    # 更新调度器的last_auto_start_date，防止当日重复自动开启
    current_date = datetime.now().strftime('%Y-%m-%d')
    auction_config.set_auction_config_value("schedule", current_date, "last_auto_start_date")

    logger.info(f"拍卖已开启，共 {len(all_auction_items)} 件物品参与拍卖！")
    return True


async def end_auction_process(bot: Optional[Bot]) -> List[Dict[str, Any]]: # bot参数可能为None
    """
    结束拍卖流程。
    结算所有当前拍卖品，将物品发给买家，灵石发给卖家，并记录到拍卖历史。
    """
    items, sql_message, _, auction_repository = _auction_dependencies()
    current_auctions = auction_repository.get_current_auction() # 从数据库获取当前拍卖品
    if not current_auctions:
        return []

    auction_results: List[Dict[str, Any]] = []
    auction_rules = auction_config.get_auction_rules()

    for item in current_auctions:
        winner_id = None
        winner_name = None
        if item["bids"]:
            winner_id, _ = max(item["bids"].items(), key=lambda entry: entry[1])
            winner_info = sql_message.get_user_info_with_id(winner_id)
            winner_name = winner_info["user_name"] if winner_info else str(winner_id)
        item_info = items.get_data_by_item_id(item["item_id"])
        settlement = auction_repository.settle_auction_item(
            item["id"],
            item_type=item_info["type"] if item_info else None,
            winner_name=winner_name,
            fee_rate=auction_rules["fee_rate"],
            end_time=time.time(),
        )
        if not settlement.applied:
            raise ValueError(
                f"auction settlement blocked: auction_id={item['id']} "
                f"status={settlement.status}"
            )
        result_record = settlement.as_history_record()
        trace_id = f"trade:auction:{settlement.auction_id}"

        if settlement.final_price is not None:
            record_trade_event(
                settlement.winner_id,
                "拍卖成交",
                f"拍得{settlement.item_name}，成交价{number_to(settlement.final_price)}灵石，拍卖ID:{settlement.auction_id}",
                {"拍卖成交次数": 1, "拍卖消费灵石": settlement.final_price}
            )
            safe_record_game_event(
                settlement.winner_id,
                "trade_buy",
                1,
                {
                    "source": "auction",
                    "action": "auction_buy",
                    "stone_delta": -int(settlement.final_price),
                    "item_delta": [
                        {
                            "id": settlement.item_id,
                            "name": settlement.item_name,
                            "amount": 1,
                        }
                    ],
                    "detail": {
                        "auction_id": settlement.auction_id,
                        "seller_id": settlement.seller_id,
                        "final_price": settlement.final_price,
                    },
                    "trace_id": trace_id,
                },
            )
            if not settlement.is_system:
                record_trade_event(
                    settlement.seller_id,
                    "拍卖成交",
                    f"售出{settlement.item_name}，成交价{number_to(settlement.final_price)}灵石，手续费{number_to(settlement.fee)}灵石，收入{number_to(settlement.seller_earnings)}灵石",
                    {"拍卖售出次数": 1, "拍卖收入灵石": settlement.seller_earnings, "拍卖手续费消耗": settlement.fee}
                )
                safe_record_game_event(
                    settlement.seller_id,
                    "trade_sell",
                    1,
                    {
                        "source": "auction",
                        "action": "auction_sell",
                        "stone_delta": int(settlement.seller_earnings),
                        "item_delta": [
                            {
                                "id": settlement.item_id,
                                "name": settlement.item_name,
                                "amount": -1,
                            }
                        ],
                        "detail": {
                            "auction_id": settlement.auction_id,
                            "winner_id": settlement.winner_id,
                            "final_price": settlement.final_price,
                            "fee": settlement.fee,
                        },
                        "trace_id": trace_id,
                    },
                )
        elif not settlement.is_system and item_info:
            record_trade_event(
                settlement.seller_id,
                "拍卖流拍",
                f"{settlement.item_name}流拍，已退回背包，拍卖ID:{settlement.auction_id}",
                {"拍卖流拍次数": 1},
            )

        auction_results.append(result_record)

    remaining = auction_repository.get_current_auction()
    if remaining:
        raise RuntimeError(f"auction settlement incomplete: remaining={len(remaining)}")

    # 更新拍卖状态为不活跃，时间置空
    set_auction_status(active=False, start_time=None, end_time=None, last_display_refresh_time=None, items_count=0)
    auction_config.clear_persisted_auction_status()
    logger.info("拍卖已结束，结算完成！")
    return auction_results


async def reconcile_auction_after_restart() -> None:
    """
    重启后对账：有落盘场次且未到结束时间 → 继续本场；否则库内遗留拍品 → 收尾结算。
    不向群里发公告。
    """
    _, _, _, auction_repository = _auction_dependencies()
    current_auctions = auction_repository.get_current_auction()
    if not current_auctions:
        return

    _restore_auction_status_from_disk()
    status = get_auction_status()
    now_dt = datetime.now()
    item_count = len(current_auctions)
    end_dt = status["end_time"]
    start_dt = status["start_time"]
    active = status["active"]

    if active and end_dt is not None:
        if now_dt >= end_dt:
            logger.info(
                f"拍卖重启后对账：已过结束时间（{end_dt.strftime('%m-%d %H:%M')}），"
                f"开始收尾，拍品 {item_count} 件。"
            )
            await end_auction_process(None)
            return
        refresh = status.get("last_display_refresh_time") or start_dt
        set_auction_status(
            active=True,
            start_time=start_dt,
            end_time=end_dt,
            last_display_refresh_time=refresh,
            items_count=item_count,
        )
        left_min = max(int((end_dt - now_dt).total_seconds()) // 60, 0)
        logger.info(
            f"拍卖重启后继续本场，预计 {end_dt.strftime('%H:%M')} 结束，"
            f"剩余约 {left_min} 分钟，拍品 {item_count} 件。"
        )
        return

    logger.warning(
        f"拍卖库里有 {item_count} 件未收尾拍品，场次记录对不上，按遗留数据结算。"
    )
    await end_auction_process(None)


async def place_auction_bid(bot: Bot, user_id: str, user_name: str, auction_id: str, bid_price: int):
    """
    用户参与拍卖竞拍。
    """
    _, sql_message, _, auction_repository = _auction_dependencies()
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
