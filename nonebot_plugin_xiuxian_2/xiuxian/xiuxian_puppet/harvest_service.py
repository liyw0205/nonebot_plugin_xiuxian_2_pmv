from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Callable, Iterable

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PuppetHarvestReward:
    goods_id: int
    goods_name: str
    goods_type: str
    quantity: int


@dataclass(frozen=True)
class PuppetHarvest:
    status: str
    user_id: str
    rewards: tuple[PuppetHarvestReward, ...] = ()
    stone_cost: int = 0
    remaining_hours: float = 0.0

    @property
    def harvested(self) -> bool:
        return self.status == "harvested"


RewardFactory = Callable[[str, int], Iterable[PuppetHarvestReward]]


class PuppetHarvestService:
    """Apply puppet harvesting across the game and player databases in one transaction."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        max_goods_num: int,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._max_goods_num = max(1, int(max_goods_num))
        self._lock = lock or RLock()

    @staticmethod
    def _parse_time(value) -> datetime | None:
        if value in (None, "", 0, "0"):
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(str(value), fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _as_int(value, default: int = 0) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _aggregate_rewards(
        rewards: Iterable[PuppetHarvestReward],
    ) -> tuple[PuppetHarvestReward, ...]:
        grouped: dict[int, PuppetHarvestReward] = {}
        for reward in rewards:
            goods_id = int(reward.goods_id)
            quantity = int(reward.quantity)
            if quantity <= 0:
                continue
            existing = grouped.get(goods_id)
            if existing is None:
                grouped[goods_id] = PuppetHarvestReward(
                    goods_id,
                    str(reward.goods_name),
                    str(reward.goods_type),
                    quantity,
                )
                continue
            if (
                existing.goods_name != str(reward.goods_name)
                or existing.goods_type != str(reward.goods_type)
            ):
                raise ValueError("conflicting reward metadata")
            grouped[goods_id] = PuppetHarvestReward(
                goods_id,
                existing.goods_name,
                existing.goods_type,
                existing.quantity + quantity,
            )
        return tuple(grouped.values())

    def harvest(
        self,
        user_id,
        *,
        now: datetime,
        time_cost_hours: float,
        speed_base: float,
        harvest_costs: dict[int, int],
        harvest_bonus: int,
        reward_factory: RewardFactory,
    ) -> PuppetHarvest:
        user_id = str(user_id)
        now_text = now.strftime("%Y-%m-%d %H:%M:%S")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "ATTACH DATABASE %s AS player_data", (str(self._player_database),)
            )
            try:
                conn.execute("BEGIN IMMEDIATE")
                user = conn.execute(
                    """
                    SELECT level, stone, blessed_spot_flag, puppet_status
                    FROM user_xiuxian WHERE user_id=%s
                    """,
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return PuppetHarvest("user_missing", user_id)
                if self._as_int(user[2]) == 0:
                    conn.rollback()
                    return PuppetHarvest("blessed_spot_missing", user_id)
                if self._as_int(user[3]) != 1:
                    conn.rollback()
                    return PuppetHarvest("puppet_disabled", user_id)

                elixir = conn.execute(
                    """
                    SELECT "收取时间", "收取等级", "灵田数量", "药材速度", "灵田傀儡"
                    FROM player_data."mix_elixir_info" WHERE user_id=%s
                    """,
                    (user_id,),
                ).fetchone()
                if elixir is None:
                    conn.rollback()
                    return PuppetHarvest("player_info_missing", user_id)

                last_time = self._parse_time(elixir[0])
                if last_time is None:
                    conn.rollback()
                    return PuppetHarvest("invalid_harvest_time", user_id)
                interval = float(time_cost_hours) * (
                    1 - float(speed_base) * self._as_int(elixir[3])
                )
                interval = max(interval, 0.0)
                elapsed = (now - last_time).total_seconds() / 3600
                if elapsed < interval:
                    conn.rollback()
                    return PuppetHarvest(
                        "not_ready", user_id, remaining_hours=max(interval - elapsed, 0.0)
                    )

                puppet_level = self._as_int(elixir[4])
                harvest_cost = harvest_costs.get(puppet_level)
                if harvest_cost is None:
                    conn.rollback()
                    return PuppetHarvest("invalid_puppet_level", user_id)
                harvest_cost = max(int(harvest_cost), 0)
                if self._as_int(user[1]) < harvest_cost:
                    conn.execute(
                        "UPDATE user_xiuxian SET puppet_status=0 WHERE user_id=%s",
                        (user_id,),
                    )
                    conn.commit()
                    return PuppetHarvest("stone_insufficient", user_id, stone_cost=harvest_cost)

                quantity = max(
                    self._as_int(elixir[2])
                    + self._as_int(elixir[1])
                    + int(harvest_bonus),
                    0,
                )
                rewards = self._aggregate_rewards(reward_factory(str(user[0]), quantity))
                if not rewards:
                    conn.rollback()
                    return PuppetHarvest("reward_missing", user_id)

                for reward in rewards:
                    inventory = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back "
                        "WHERE user_id=%s AND goods_id=%s",
                        (user_id, reward.goods_id),
                    ).fetchone()
                    current = self._as_int(inventory[0]) if inventory else 0
                    if current + reward.quantity > self._max_goods_num:
                        conn.rollback()
                        return PuppetHarvest("inventory_full", user_id, rewards=rewards)

                updated_time = conn.execute(
                    """
                    UPDATE player_data."mix_elixir_info" SET "收取时间"=%s
                    WHERE user_id=%s AND "收取时间"=%s
                    """,
                    (now_text, user_id, elixir[0]),
                )
                if updated_time.rowcount != 1:
                    conn.rollback()
                    return PuppetHarvest("harvest_time_changed", user_id)
                deducted = conn.execute(
                    """
                    UPDATE user_xiuxian SET stone=stone-%s
                    WHERE user_id=%s AND stone >= %s AND puppet_status=1
                    """,
                    (harvest_cost, user_id, harvest_cost),
                )
                if deducted.rowcount != 1:
                    conn.rollback()
                    return PuppetHarvest("stone_changed", user_id)

                for reward in rewards:
                    conn.execute(
                        """
                        INSERT INTO back (
                            user_id, goods_id, goods_name, goods_type, goods_num,
                            create_time, update_time, bind_num
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, 0)
                        ON CONFLICT (user_id, goods_id) DO UPDATE
                        SET goods_name=EXCLUDED.goods_name,
                            goods_type=EXCLUDED.goods_type,
                            goods_num=COALESCE(back.goods_num, 0)+EXCLUDED.goods_num,
                            update_time=EXCLUDED.update_time
                        """,
                        (
                            user_id,
                            reward.goods_id,
                            reward.goods_name,
                            reward.goods_type,
                            reward.quantity,
                            now_text,
                            now_text,
                        ),
                    )
                conn.commit()
                return PuppetHarvest(
                    "harvested", user_id, rewards=rewards, stone_cost=harvest_cost
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")
