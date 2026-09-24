from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from ...core.errors import ValidationError
from ...infrastructure.clock import SystemClock
from ...infrastructure.ids import UUIDGenerator
from ...infrastructure.random_source import SystemRandom
from ..auction.queue_application import AuctionQueueApplication
from ..auction.session_start_application import AuctionSessionStartApplication
from ..auction.settlement import AuctionSettlementApplication
from .guishi_cancel_repository import GuishiOrderCancelSqlRepository
from .guishi_deposit_repository import GuishiDepositSqlRepository
from .guishi_baitan_repository import GuishiBaitanSqlRepository
from .guishi_stone_rules import withdrawal_is_open
from .guishi_withdraw_repository import GuishiWithdrawSqlRepository
from .guishi_qiugou_repository import GuishiQiugouSqlRepository
from .guishi_match_repository import GuishiOrderMatchSqlRepository
from .guishi_expired_repository import GuishiExpiredOrderSqlRepository
from .guishi_take_repository import GuishiStoredItemTakeSqlRepository
from .xianshi_listing_repository import XianshiListingSqlRepository
from .xianshi_plan_listing_repository import XianshiPlanListingSqlRepository
from .xianshi_removal_repository import XianshiRemovalSqlRepository
from .xianshi_purchase_repository import XianshiPurchaseSqlRepository
from .repository import TradeFeatureRepository



class TradeApplication(LegacyApplication):
    def __init__(
        self,
        game_database: str | Path,
        trade_database: str | Path,
        *,
        repository: TradeFeatureRepository | None = None,
        clock: Any | None = None,
        ids: Any | None = None,
        random_source: Any | None = None,
        auction_queue: Any | None = None,
        auction_session_start: Any | None = None,
        auction_settlement: Any | None = None,
        auction_max_goods_num: int = 1000,
        auction_max_user_items: int = 3,
    ) -> None:
        self.game_database = str(game_database)
        self.trade_database = str(trade_database)
        self.clock = clock or SystemClock()
        self.ids = ids or UUIDGenerator()
        self.random_source = random_source or SystemRandom()
        self.auction_max_user_items = max(int(auction_max_user_items), 1)
        self.auction_queue = auction_queue or AuctionQueueApplication(
            self.game_database,
            self.trade_database,
            auction_max_goods_num,
            clock=self.clock,
        )
        self.auction_session_start = auction_session_start or AuctionSessionStartApplication(
            self.game_database,
            self.trade_database,
            clock=self.clock,
            random_source=self.random_source,
        )
        self.auction_settlement = auction_settlement or AuctionSettlementApplication(
            self.game_database
        )
        self.guishi_deposit_repository = GuishiDepositSqlRepository(
            self.game_database, self.trade_database
        )
        self.guishi_baitan_repository = GuishiBaitanSqlRepository(
            self.game_database, self.trade_database, clock=self.clock
        )
        self.guishi_order_cancel_repository = GuishiOrderCancelSqlRepository(
            self.game_database, self.trade_database, clock=self.clock
        )
        self.guishi_withdraw_repository = GuishiWithdrawSqlRepository(
            self.game_database, self.trade_database
        )
        self.guishi_qiugou_repository = GuishiQiugouSqlRepository(self.trade_database)
        self.guishi_order_match_repository = GuishiOrderMatchSqlRepository(self.trade_database)
        self.guishi_expired_order_repository = GuishiExpiredOrderSqlRepository(
            self.game_database, self.trade_database, clock=self.clock
        )
        self.guishi_stored_item_take_repository = GuishiStoredItemTakeSqlRepository(
            self.game_database, self.trade_database, clock=self.clock
        )
        self.xianshi_listing_repository = XianshiListingSqlRepository(
            self.game_database, clock=self.clock, ids=self.ids
        )
        self.xianshi_plan_listing_repository = XianshiPlanListingSqlRepository(
            self.game_database, clock=self.clock, ids=self.ids
        )
        self.xianshi_removal_repository = XianshiRemovalSqlRepository(
            self.game_database, clock=self.clock
        )
        self.xianshi_purchase_repository = XianshiPurchaseSqlRepository(
            self.game_database, clock=self.clock
        )
        super().__init__(game_database, repository=repository, feature="trade")

    def xianshi_list_items(
        self,
        *,
        operation_id: str,
        seller_id: str,
        goods_id: int,
        name: str,
        goods_type: str,
        price: int,
        quantity: int,
        stamina_cost: int = 0,
    ):
        return self.xianshi_listing_repository.list_items(
            operation_id,
            seller_id,
            goods_id,
            name,
            goods_type,
            price,
            quantity,
            stamina_cost=stamina_cost,
        )

    def xianshi_list_system_item(
        self,
        *,
        operation_id: str,
        goods_id: int,
        name: str,
        goods_type: str,
        price: int,
        quantity: int,
    ):
        return self.xianshi_listing_repository.list_system_item(
            operation_id, goods_id, name, goods_type, price, quantity
        )

    def xianshi_list_plan(
        self,
        *,
        operation_id: str,
        seller_id: str,
        listing_plan: list[dict[str, Any]],
        stamina_cost: int,
    ):
        return self.xianshi_plan_listing_repository.list_plan(
            operation_id,
            seller_id,
            listing_plan,
            stamina_cost=stamina_cost,
        )

    def xianshi_remove_listing(
        self, *, operation_id: str, listing_id: str, max_goods_num: int
    ):
        return self.xianshi_removal_repository.remove_listing(
            operation_id, listing_id, max_goods_num=max_goods_num
        )

    def xianshi_remove_by_name(
        self,
        *,
        operation_id: str,
        seller_id: str,
        item_name: str,
        quantity: int,
        max_goods_num: int,
    ):
        return self.xianshi_removal_repository.remove_by_name(
            operation_id,
            seller_id,
            item_name,
            quantity,
            max_goods_num=max_goods_num,
        )

    def xianshi_clear_all(self, *, operation_id: str, max_goods_num: int):
        return self.xianshi_removal_repository.clear_all(
            operation_id, max_goods_num=max_goods_num
        )

    @staticmethod
    def _take_action_arguments(
        action: str,
        kwargs: dict[str, Any],
        allowed: set[str],
        *,
        required: set[str] | None = None,
    ) -> dict[str, Any]:
        unexpected = set(kwargs) - allowed
        if unexpected:
            raise TypeError(f"unexpected {action} arguments: {', '.join(sorted(unexpected))}")
        missing = (required or set()) - set(kwargs)
        if missing:
            raise TypeError(f"missing {action} arguments: {', '.join(sorted(missing))}")
        return kwargs

    def _invoke_auction_action(
        self, action: str, operation_id: str, user_id: str, kwargs: dict[str, Any]
    ):
        if action == "enqueue":
            values = self._take_action_arguments(
                action,
                kwargs,
                {"item_id", "item_name", "start_price", "user_name"},
                required={"item_id", "item_name", "start_price", "user_name"},
            )
            return self.auction_queue.enqueue(
                operation_id,
                user_id,
                values["item_id"],
                values["item_name"],
                values["start_price"],
                values["user_name"],
                max_user_items=self.auction_max_user_items,
            )
        if action == "dequeue":
            values = self._take_action_arguments(
                action, kwargs, {"item_id", "item_type"}, required={"item_id", "item_type"}
            )
            return self.auction_queue.dequeue(
                operation_id, user_id, values["item_id"], values["item_type"]
            )
        if action == "session_start":
            values = self._take_action_arguments(
                action,
                kwargs,
                {"system_items_config", "duration_hours", "system_item_count"},
                required={"system_items_config", "duration_hours"},
            )
            return self.auction_session_start.start(
                operation_id,
                system_items_config=values["system_items_config"],
                duration_hours=values["duration_hours"],
                system_item_count=values.get("system_item_count", 5),
            )
        raise ValueError(f"unsupported trade action: {action}")

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        if action == "session_finish" and self.repository is None:
            values = self._take_action_arguments(
                action, dict(kwargs), {"end_time", "fee_rate", "item_types"}
            )
            if not str(user_id).strip():
                raise ValidationError("user_id is required")
            return self.auction_settlement.settle_active(
                operation_id=operation_id,
                end_time=values.get("end_time", self.clock.now().timestamp()),
                fee_rate=values.get("fee_rate", 0.2),
                item_types=values.get("item_types", {}),
            )

        def invoke():
            if self.repository is not None:
                return self.repository.invoke(action, operation_id, user_id, **kwargs)
            return self._invoke_auction_action(action, operation_id, user_id, dict(kwargs))

        return self._execute(
            operation_id=operation_id,
            user_id=user_id,
            action=f"trade.{action}",
            payload={"user_id": user_id, **kwargs},
            call=invoke,
        )

    def deposit(self, *, operation_id: str, user_id: str, amount: int, **kwargs: Any):
        if self.repository is not None:
            return self._action(
                "deposit",
                operation_id=operation_id,
                user_id=user_id,
                amount=amount,
                **kwargs,
            )
        return self._execute(
            operation_id=operation_id,
            user_id=user_id,
            action="trade.deposit",
            payload={"user_id": user_id, "amount": amount},
            call=lambda: self.guishi_deposit(
                operation_id=operation_id, user_id=user_id, amount=amount
            ),
        )

    def guishi_deposit(self, *, operation_id: str, user_id: str, amount: int):
        return self.guishi_deposit_repository.deposit(
            operation_id=operation_id, user_id=user_id, amount=amount
        )

    def withdraw(
        self, *, operation_id: str, user_id: str, amount: int, **kwargs: Any
    ):
        if self.repository is not None:
            return self._action(
                "withdraw",
                operation_id=operation_id,
                user_id=user_id,
                amount=amount,
                **kwargs,
            )
        return self._execute(
            operation_id=operation_id,
            user_id=user_id,
            action="trade.withdraw",
            payload={"user_id": user_id, "amount": amount},
            call=lambda: self.guishi_withdraw(
                operation_id=operation_id, user_id=user_id, amount=amount
            ),
        )

    def guishi_withdraw(self, *, operation_id: str, user_id: str, amount: int):
        return self.guishi_withdraw_repository.withdraw(
            operation_id=operation_id,
            user_id=user_id,
            amount=amount,
            withdrawal_open=withdrawal_is_open(self.clock.now()),
        )

    def guishi_qiugou(
        self,
        *,
        operation_id: str,
        user_id: str,
        item_id: int,
        item_name: str,
        price: int,
        quantity: int,
        max_orders: int,
    ):
        return self.guishi_qiugou_repository.create(
            operation_id=operation_id,
            user_id=user_id,
            item_id=item_id,
            item_name=item_name,
            price=price,
            quantity=quantity,
            max_orders=max_orders,
        )

    def guishi_baitan(
        self,
        *,
        operation_id: str,
        user_id: str,
        item_id: int,
        item_name: str,
        price: int,
        quantity: int,
        max_orders: int,
    ):
        return self.guishi_baitan_repository.create(
            operation_id=operation_id,
            user_id=user_id,
            item_id=item_id,
            item_name=item_name,
            price=price,
            quantity=quantity,
            max_orders=max_orders,
        )

    def guishi_cancel_qiugou(
        self,
        *,
        operation_id: str,
        user_id: str,
        order_id: str,
    ):
        return self.guishi_order_cancel_repository.cancel_qiugou(
            operation_id=operation_id,
            user_id=user_id,
            order_id=order_id,
        )

    def guishi_cancel_baitan(
        self,
        *,
        operation_id: str,
        user_id: str,
        order_id: str,
        goods_type: str,
        max_goods_num: int,
    ):
        return self.guishi_order_cancel_repository.cancel_baitan(
            operation_id=operation_id,
            user_id=user_id,
            order_id=order_id,
            goods_type=goods_type,
            max_goods_num=max_goods_num,
        )

    def guishi_match(
        self,
        *,
        operation_id: str,
        qiugou_order_id: str,
        baitan_order_id: str,
    ):
        return self.guishi_order_match_repository.match(
            operation_id=operation_id,
            qiugou_order_id=qiugou_order_id,
            baitan_order_id=baitan_order_id,
        )

    def guishi_clear_expired_baitan(
        self,
        *,
        operation_id: str,
        order_id: str,
        goods_type: str,
        max_goods_num: int,
        expected_user_id: str | None = None,
    ):
        return self.guishi_expired_order_repository.clear_baitan(
            operation_id=operation_id,
            order_id=order_id,
            goods_type=goods_type,
            max_goods_num=max_goods_num,
            expected_user_id=expected_user_id,
        )

    def guishi_take_stored_item(
        self,
        *,
        operation_id: str,
        user_id: str,
        goods_id: int,
        item_name: str,
        goods_type: str,
        max_goods_num: int,
    ):
        return self.guishi_stored_item_take_repository.take(
            operation_id=operation_id,
            user_id=user_id,
            goods_id=goods_id,
            item_name=item_name,
            goods_type=goods_type,
            max_goods_num=max_goods_num,
        )

    def enqueue(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("enqueue", operation_id=operation_id, user_id=user_id, **kwargs)
    def dequeue(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("dequeue", operation_id=operation_id, user_id=user_id, **kwargs)
    def session_start(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("session_start", operation_id=operation_id, user_id=user_id, **kwargs)
    def session_finish(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("session_finish", operation_id=operation_id, user_id=user_id, **kwargs)
    def purchase(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self.repository is not None:
            return self._action("purchase", operation_id=operation_id, user_id=user_id, **kwargs)
        max_goods_num = int(kwargs.pop("max_goods_num", 1) or 1)
        listing_id = kwargs.pop("listing_id", None)
        quantity = kwargs.pop("quantity", None)
        if listing_id is None or quantity is None:
            raise ValidationError("listing_id and quantity are required")
        return self._execute(
            operation_id=operation_id,
            user_id=user_id,
            action="trade.purchase",
            payload={"user_id": user_id, "listing_id": listing_id, "quantity": quantity, **kwargs},
            call=lambda: self.xianshi_purchase_repository.purchase(
                operation_id, user_id, listing_id, quantity,
                max_goods_num=max_goods_num,
                stamina_operation_id=kwargs.get("stamina_operation_id"),
                stamina_cost=int(kwargs.get("stamina_cost", 0) or 0),
            ),
        )


__all__ = ["TradeApplication"]
