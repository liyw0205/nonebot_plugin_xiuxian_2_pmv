from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class HarvestReward:
    """One harvested reagent, shaped for the command adapter."""

    goods_id: int
    goods_name: str
    goods_type: str
    quantity: int


@dataclass(frozen=True)
class PuppetHarvestResult:
    status: str
    user_id: str
    rewards: tuple[HarvestReward, ...] = ()
    stone_cost: int = 0
    remaining_hours: float = 0.0

    @property
    def harvested(self) -> bool:
        return self.status in {"harvested", "duplicate"}


RewardFactory = Callable[[str, int], Iterable[Any]]


def normalize_now(value: datetime) -> datetime:
    """Normalize a harvest instant into the naive frame the stamps are stored in.

    ``mix_elixir_info."收取时间"`` is written as ``%Y-%m-%d %H:%M:%S`` with no
    offset, while the runtime clock (``SystemClock``) returns an AWARE UTC
    instant.  Subtracting one from the other raises ``TypeError``; an aware
    instant is therefore converted to UTC and stripped of its tzinfo here, and
    a naive instant is passed through unchanged so existing naive callers keep
    working.
    """
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def parse_harvest_time(value: Any) -> datetime | None:
    """Parse a stored ``收取时间``; ``None`` means the value cannot be trusted."""
    if isinstance(value, datetime):
        return normalize_now(value)
    if value in (None, "", 0, "0"):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    return None


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_reward(raw: Any) -> HarvestReward:
    """Accept both factory shapes: a 4-tuple and a ``PuppetHarvestReward`` DTO."""
    if isinstance(raw, HarvestReward):
        return raw
    if isinstance(raw, Mapping):
        return HarvestReward(
            as_int(raw.get("goods_id")),
            str(raw.get("goods_name", "")),
            str(raw.get("goods_type", "")),
            as_int(raw.get("quantity")),
        )
    if getattr(raw, "goods_id", None) is not None:
        return HarvestReward(
            as_int(getattr(raw, "goods_id")),
            str(getattr(raw, "goods_name", "")),
            str(getattr(raw, "goods_type", "")),
            as_int(getattr(raw, "quantity", 0)),
        )
    item_id, name, item_type, quantity = raw
    return HarvestReward(as_int(item_id), str(name), str(item_type), as_int(quantity))


class PuppetHarvestSqlRepository:
    """Harvest a mature 灵田 atomically across the game and player databases.

    Column contract: ``mix_elixir_info`` holds
    ``收取时间/收取等级/灵田数量/药材速度/灵田傀儡`` — ``灵田傀儡`` is the puppet
    LEVEL (``0`` means "not owned"), and there is no ``傀儡等级`` column.  The
    enable switch lives in ``user_xiuxian.puppet_status``.  Maturity, reward
    quantity and cost all follow the compatibility implementation and the
    ``我的灵田`` display so the three cannot drift apart.
    """

    def __init__(self, game_database: str | Path, player_database: str | Path, *, max_goods_num: int) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.max_goods_num = int(max_goods_num)

    def _duplicate(self, previous: Mapping[str, Any]) -> PuppetHarvestResult:
        stored = json.loads(str(previous["rewards_json"]))
        rewards = tuple(
            HarvestReward(
                as_int(item.get("goods_id")),
                str(item.get("goods_name", "")),
                str(item.get("goods_type", "")),
                as_int(item.get("quantity")),
            )
            for item in stored
        )
        return PuppetHarvestResult(
            "duplicate",
            str(previous["user_id"]),
            rewards=rewards,
            stone_cost=as_int(previous["stone_cost"]),
        )

    def harvest(
        self,
        user_id: str,
        *,
        operation_id: str,
        now: datetime,
        time_cost_hours: float,
        speed_base: float,
        harvest_costs: Mapping[int, int],
        harvest_bonus: int,
        reward_factory: RewardFactory,
        max_goods_num: int | None = None,
    ) -> PuppetHarvestResult:
        user_id, operation_id = str(user_id), str(operation_id).strip()
        capacity = self.max_goods_num if max_goods_num is None else int(max_goods_num)
        moment = normalize_now(now)
        harvested_at = moment.strftime("%Y-%m-%d %H:%M:%S")

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.execute("ATTACH DATABASE ? AS player_data", (self.player_database,))
            uow.execute(
                "CREATE TABLE IF NOT EXISTS puppet_harvest_operations("
                "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,"
                "rewards_json TEXT NOT NULL,stone_cost INTEGER NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            if operation_id:
                previous = uow.query_one(
                    "SELECT user_id,rewards_json,stone_cost FROM puppet_harvest_operations "
                    "WHERE operation_id=?",
                    (operation_id,),
                )
                if previous is not None:
                    return self._duplicate(previous)

            user = uow.query_one(
                "SELECT level,stone,blessed_spot_flag,puppet_status FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
            if user is None:
                return PuppetHarvestResult("user_missing", user_id)
            if as_int(user["blessed_spot_flag"]) == 0:
                return PuppetHarvestResult("blessed_spot_missing", user_id)
            if as_int(user["puppet_status"]) != 1:
                return PuppetHarvestResult("puppet_disabled", user_id)

            elixir = uow.query_one(
                'SELECT "收取时间" AS last_time,"收取等级" AS harvest_level,'
                '"灵田数量" AS field_count,"药材速度" AS herb_speed,"灵田傀儡" AS puppet_level '
                "FROM player_data.mix_elixir_info WHERE user_id=?",
                (user_id,),
            )
            if elixir is None:
                return PuppetHarvestResult("player_info_missing", user_id)

            last_time = parse_harvest_time(elixir["last_time"])
            if last_time is None:
                return PuppetHarvestResult("invalid_harvest_time", user_id)

            interval = max(
                float(time_cost_hours) * (1 - float(speed_base) * as_int(elixir["herb_speed"])),
                0.0,
            )
            elapsed = (moment - last_time).total_seconds() / 3600
            if elapsed < interval:
                return PuppetHarvestResult(
                    "not_ready", user_id, remaining_hours=max(interval - elapsed, 0.0)
                )

            puppet_level = as_int(elixir["puppet_level"])
            configured_cost = harvest_costs.get(puppet_level)
            if configured_cost is None:
                return PuppetHarvestResult("invalid_puppet_level", user_id)
            stone_cost = max(as_int(configured_cost), 0)

            if as_int(user["stone"]) < stone_cost:
                # Same as the compatibility implementation: an unfunded puppet
                # switches itself off rather than retrying every hour forever.
                uow.execute(
                    "UPDATE user_xiuxian SET puppet_status=0 WHERE user_id=? AND puppet_status=1",
                    (user_id,),
                )
                return PuppetHarvestResult("stone_insufficient", user_id, stone_cost=stone_cost)

            quantity = max(
                as_int(elixir["field_count"])
                + as_int(elixir["harvest_level"])
                + as_int(harvest_bonus),
                0,
            )
            rewards = tuple(
                reward
                for reward in (
                    _coerce_reward(raw)
                    for raw in reward_factory(str(user["level"] or ""), quantity)
                )
                if reward.quantity > 0
            )
            if not rewards:
                return PuppetHarvestResult("reward_missing", user_id, stone_cost=stone_cost)

            for reward in rewards:
                existing = uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num FROM back "
                    "WHERE user_id=? AND goods_id=?",
                    (user_id, reward.goods_id),
                )
                current = as_int(existing["goods_num"]) if existing is not None else 0
                if current + reward.quantity > capacity:
                    return PuppetHarvestResult("inventory_full", user_id, stone_cost=stone_cost)

            changed = uow.execute(
                'UPDATE player_data.mix_elixir_info SET "收取时间"=? '
                "WHERE user_id=? AND COALESCE(\"收取时间\",'')=?",
                (harvested_at, user_id, str(elixir["last_time"] if elixir["last_time"] is not None else "")),
            )
            if changed.rowcount != 1:
                return PuppetHarvestResult("harvest_time_changed", user_id, stone_cost=stone_cost)

            deducted = uow.execute(
                "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(? AS REAL) "
                "WHERE user_id=? AND stone>=? AND puppet_status=1",
                (stone_cost, user_id, stone_cost),
            )
            if deducted.rowcount != 1:
                raise RuntimeError("puppet harvest stone target changed")

            for reward in rewards:
                existing = uow.query_one(
                    "SELECT goods_num FROM back WHERE user_id=? AND goods_id=?",
                    (user_id, reward.goods_id),
                )
                if existing is None:
                    uow.execute(
                        "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) "
                        "VALUES(?,?,?,?,?,0)",
                        (
                            user_id,
                            reward.goods_id,
                            reward.goods_name,
                            reward.goods_type,
                            reward.quantity,
                        ),
                    )
                else:
                    uow.execute(
                        "UPDATE back SET goods_num=COALESCE(goods_num,0)+?,goods_name=?,goods_type=? "
                        "WHERE user_id=? AND goods_id=?",
                        (
                            reward.quantity,
                            reward.goods_name,
                            reward.goods_type,
                            user_id,
                            reward.goods_id,
                        ),
                    )

            if operation_id:
                payload = json.dumps([user_id], ensure_ascii=True, separators=(",", ":"))
                rewards_json = json.dumps(
                    [
                        {
                            "goods_id": reward.goods_id,
                            "goods_name": reward.goods_name,
                            "goods_type": reward.goods_type,
                            "quantity": reward.quantity,
                        }
                        for reward in rewards
                    ],
                    ensure_ascii=True,
                    separators=(",", ":"),
                )
                uow.execute(
                    "INSERT INTO puppet_harvest_operations"
                    "(operation_id,user_id,payload,rewards_json,stone_cost) VALUES(?,?,?,?,?)",
                    (operation_id, user_id, payload, rewards_json, stone_cost),
                )

            return PuppetHarvestResult(
                "harvested", user_id, rewards=rewards, stone_cost=stone_cost
            )


__all__ = [
    "HarvestReward",
    "PuppetHarvestResult",
    "PuppetHarvestSqlRepository",
    "as_int",
    "normalize_now",
    "parse_harvest_time",
]
