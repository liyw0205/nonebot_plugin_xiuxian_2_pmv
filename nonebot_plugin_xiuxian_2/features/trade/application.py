from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from ...core.errors import ValidationError
from ...infrastructure.clock import SystemClock
from ...infrastructure.ids import UUIDGenerator
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
    ) -> None:
        self.game_database = str(game_database)
        self.trade_database = str(trade_database)
        self.clock = clock or SystemClock()
        self.ids = ids or UUIDGenerator()
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
    ):
        return self.xianshi_listing_repository.list_items(
            operation_id, seller_id, goods_id, name, goods_type, price, quantity
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

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action=f"trade.{action}", payload={"user_id": user_id, **kwargs}, call=lambda: self.repository.invoke(action, operation_id, user_id, **kwargs))

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
        from ...xiuxian.xiuxian_trade.repository import TradeRepository

        return self._execute(
            operation_id=operation_id,
            user_id=user_id,
            action="trade.purchase",
            payload={"user_id": user_id, "listing_id": listing_id, "quantity": quantity, **kwargs},
            call=lambda: TradeRepository(self.game_database, max_goods_num=max_goods_num).purchase_xianshi_item(
                operation_id, user_id, listing_id, quantity,
                stamina_operation_id=kwargs.get("stamina_operation_id"),
                stamina_cost=int(kwargs.get("stamina_cost", 0) or 0),
            ),
        )


__all__ = ["TradeApplication"]
