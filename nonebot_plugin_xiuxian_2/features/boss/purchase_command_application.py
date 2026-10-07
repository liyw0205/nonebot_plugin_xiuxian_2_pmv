from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from ...core.errors import DomainError
from ...infrastructure.clock import SystemClock
from .application import BossApplication
from .integral_application import BossIntegralApplication
from .purchase_command_repository import BossPurchaseCommandRepository
from .repository import BossPurchaseSqlRepository


class BossPurchaseCommandApplication:
    """Own the world-boss exchange command from request to durable outcome."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        config_provider,
        item_provider,
        *,
        application=None,
        repository=None,
        integral=None,
        max_goods_num_provider=None,
        clock=None,
    ) -> None:
        self.game_database = Path(game_database)
        self.player_database = Path(player_database)
        self.config_provider = config_provider
        self.item_provider = item_provider
        self.clock = clock or SystemClock()
        self.repository = repository or BossPurchaseCommandRepository(self.game_database)
        writer_repository = BossPurchaseSqlRepository(
            self.game_database, self.player_database, self.game_database, clock=self.clock
        )
        self.application = application or BossApplication(
            self.game_database, self.player_database, activity_database=self.game_database,
            repository=writer_repository, clock=self.clock,
        )
        self.integral = integral or BossIntegralApplication(self.player_database)
        self.max_goods_num_provider = max_goods_num_provider or (lambda: 1000)

    @staticmethod
    def _invalid(status: str = "invalid_argument") -> dict[str, Any]:
        return {"action": "purchase", "status": status, "replayed": False}

    @staticmethod
    def _integer(value, *, minimum=0) -> int:
        if isinstance(value, bool):
            raise ValueError("boolean is not an amount")
        number = int(value)
        if number < minimum or (not isinstance(value, str) and number != value):
            raise ValueError("invalid integer")
        return number

    @staticmethod
    def _field(source: Any, name: str, default=None):
        if isinstance(source, Mapping):
            return source.get(name, default)
        return getattr(source, name, default)

    def _item(self, item_id: int):
        item = None
        if callable(self.item_provider):
            try:
                direct = self.item_provider(item_id)
            except TypeError:
                direct = None
            if isinstance(direct, Mapping) and {"name", "type"} <= set(direct):
                item = direct
                provider = None
            else:
                provider = self.item_provider()
        else:
            provider = self.item_provider
            item = None
        if item is None:
            getter = getattr(provider, "get_data_by_item_id", None)
            item = getter(item_id) if getter else (provider.get(str(item_id)) if isinstance(provider, Mapping) else None)
        if not isinstance(item, Mapping):
            return None
        name = str(item.get("name", "")).strip()
        kind = str(item.get("type", "")).strip()
        if not name or not kind:
            return None
        return {"name": name, "type": kind}

    def _config(self, item_id: int):
        config = self.config_provider() if callable(self.config_provider) else self.config_provider
        shop = self._field(config, "世界积分商品", {})
        if not isinstance(shop, Mapping):
            raise ValueError("invalid boss shop")
        raw = shop.get(str(item_id), shop.get(item_id))
        if not isinstance(raw, Mapping):
            return None
        cost = self._integer(raw.get("cost"), minimum=0)
        weekly_limit = self._integer(raw.get("weekly_limit", 1), minimum=1)
        return cost, weekly_limit

    def execute(self, *, operation_id, user_id, item_id, quantity) -> dict[str, Any]:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        try:
            item_id = self._integer(item_id, minimum=1)
            quantity = self._integer(quantity, minimum=1)
        except (TypeError, ValueError, OverflowError):
            return self._invalid()
        if not operation_id or not user_id or len(operation_id) > 255 or len(user_id) > 255:
            return self._invalid()

        try:
            previous = self.repository.receipt(
                operation_id=operation_id, user_id=user_id,
                item_id=item_id, quantity=quantity,
            )
        except TypeError:
            previous = self.repository.receipt(operation_id, user_id)
        except RuntimeError as exc:
            if "schema_missing" in str(exc):
                return self._invalid("schema_missing")
            raise
        if previous is not None:
            return {**dict(previous), "action": "purchase"}

        try:
            profile = self.repository.profile(user_id)
        except (AttributeError, RuntimeError) as exc:
            if "schema_missing" in str(exc):
                return self._invalid("schema_missing")
            raise
        if profile is None:
            return self._invalid("user_missing")

        try:
            item = self._item(item_id)
            pricing = self._config(item_id)
            if item is None or pricing is None:
                return self._invalid("invalid_argument")
            unit_cost, weekly_limit = pricing
            weekly = self.application.weekly_purchases(user_id, self.clock.now().date())
            if weekly is None:
                return self._invalid("schema_missing")
            purchased = self._integer(weekly.get(str(item_id), 0) or 0, minimum=0)
            remaining = weekly_limit - purchased
            effective_quantity = min(quantity, remaining) if remaining > 0 else quantity
            snapshot = self.integral.get_integral(user_id)
            integral_status = str(self._field(snapshot, "status", "ok"))
            if integral_status == "schema_missing":
                return self._invalid("schema_missing")
            expected_integral = self._integer(self._field(snapshot, "integral", 0), minimum=0)
            max_goods_num = self._integer(self.max_goods_num_provider(), minimum=0)
        except (AttributeError, KeyError, TypeError, ValueError, OverflowError):
            return self._invalid("config_invalid")

        try:
            outcome = self.application.purchase(
                operation_id=operation_id,
                user_id=user_id,
                item_id=item_id,
                item_name=item["name"],
                item_type=item["type"],
                quantity=effective_quantity,
                unit_cost=unit_cost,
                weekly_limit=weekly_limit,
                expected_integral=expected_integral,
                expected_weekly_purchases=weekly,
                max_goods_num=max_goods_num,
                today=self.clock.now().date(),
                requested_quantity=quantity,
            )
        except DomainError as exc:
            code = getattr(exc, "code", None) or "operation_conflict"
            return {"action": "purchase", "status": str(code), "replayed": False, "item_id": item_id, "item_name": item["name"]}
        data = dict(getattr(outcome, "data", None) or {})
        status = "duplicate" if getattr(outcome, "ok", False) and getattr(outcome, "replayed", False) else data.get("status", getattr(outcome, "code", None) or getattr(outcome, "status", "failed"))
        return {
            "action": "purchase", "status": status, "replayed": bool(getattr(outcome, "replayed", False)),
            "item_id": item_id, "item_name": item["name"], **data,
        }


__all__ = ["BossPurchaseCommandApplication"]
