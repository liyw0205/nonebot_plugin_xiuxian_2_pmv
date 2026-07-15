from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from datetime import datetime
from typing import Callable, Iterable
import json

from ..xiuxian_utils import db_backend

@dataclass(frozen=True)
class PuppetOperation:
    status: str
    user_id: str
    action: str
    previous_level: int = 0
    current_level: int = 0
    stone_cost: int = 0

    @property
    def applied(self) -> bool:
        return self.status in {"purchased", "upgraded"}

    @property
    def succeeded(self) -> bool:
        return self.status in {"purchased", "upgraded", "duplicate"}

class PuppetOperationService:
    """Apply puppet purchases and upgrades across game and player data atomically."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS puppet_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                action TEXT NOT NULL,
                previous_level INTEGER NOT NULL,
                current_level INTEGER NOT NULL,
                stone_cost INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _as_int(value: object) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def get_result(self, operation_id: str) -> PuppetOperation | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            self._ensure_operations(conn)
            previous = conn.execute(
                "SELECT user_id, action, previous_level, current_level, stone_cost "
                "FROM puppet_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return PuppetOperation(
                "duplicate",
                str(previous[0]),
                str(previous[1]),
                self._as_int(previous[2]),
                self._as_int(previous[3]),
                self._as_int(previous[4]),
            )

    def purchase(self, operation_id, user_id, stone_cost: int) -> PuppetOperation:
        return self._apply(
            operation_id,
            user_id,
            action="purchase",
            costs={0: int(stone_cost)},
            max_level=1,
        )

    def upgrade(
        self,
        operation_id,
        user_id,
        upgrade_costs: dict[int, int],
        *,
        max_level: int,
    ) -> PuppetOperation:
        return self._apply(
            operation_id,
            user_id,
            action="upgrade",
            costs={int(level): int(cost) for level, cost in upgrade_costs.items()},
            max_level=int(max_level),
        )

    def _apply(
        self,
        operation_id,
        user_id,
        *,
        action: str,
        costs: dict[int, int],
        max_level: int,
    ) -> PuppetOperation:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "ATTACH DATABASE %s AS player_data", (str(self._player_database),)
            )
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    """
                    SELECT action, previous_level, current_level, stone_cost
                    FROM puppet_operations WHERE operation_id=%s
                    """,
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return PuppetOperation(
                        "duplicate",
                        user_id,
                        str(previous[0]),
                        self._as_int(previous[1]),
                        self._as_int(previous[2]),
                        self._as_int(previous[3]),
                    )

                user = conn.execute(
                    "SELECT stone, blessed_spot_flag FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return PuppetOperation("user_missing", user_id, action)
                if self._as_int(user[1]) == 0:
                    conn.rollback()
                    return PuppetOperation("blessed_spot_missing", user_id, action)

                player = conn.execute(
                    """
                    SELECT "灵田傀儡" FROM player_data."mix_elixir_info"
                    WHERE user_id=%s
                    """,
                    (user_id,),
                ).fetchone()
                if player is None:
                    conn.rollback()
                    return PuppetOperation("player_info_missing", user_id, action)
                previous_level = self._as_int(player[0])

                if action == "purchase":
                    if previous_level > 0:
                        conn.rollback()
                        return PuppetOperation(
                            "already_owned", user_id, action, previous_level, previous_level
                        )
                    current_level = 1
                else:
                    if previous_level <= 0:
                        conn.rollback()
                        return PuppetOperation("puppet_missing", user_id, action)
                    if previous_level >= max_level:
                        conn.rollback()
                        return PuppetOperation(
                            "max_level", user_id, action, previous_level, previous_level
                        )
                    current_level = previous_level + 1

                stone_cost = costs.get(previous_level)
                if stone_cost is None or stone_cost < 0:
                    conn.rollback()
                    return PuppetOperation(
                        "invalid_puppet_level", user_id, action, previous_level, previous_level
                    )
                if self._as_int(user[0]) < stone_cost:
                    conn.rollback()
                    return PuppetOperation(
                        "stone_insufficient",
                        user_id,
                        action,
                        previous_level,
                        previous_level,
                        stone_cost,
                    )

                deducted = conn.execute(
                    """
                    UPDATE user_xiuxian SET stone=CAST(stone AS INTEGER)-%s
                    WHERE user_id=%s AND CAST(COALESCE(stone, 0) AS INTEGER) >= %s
                    """,
                    (stone_cost, user_id, stone_cost),
                )
                if deducted.rowcount != 1:
                    conn.rollback()
                    return PuppetOperation(
                        "stone_changed",
                        user_id,
                        action,
                        previous_level,
                        previous_level,
                        stone_cost,
                    )
                updated = conn.execute(
                    """
                    UPDATE player_data."mix_elixir_info" SET "灵田傀儡"=%s
                    WHERE user_id=%s AND CAST(COALESCE("灵田傀儡", 0) AS INTEGER)=%s
                    """,
                    (str(current_level), user_id, previous_level),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    return PuppetOperation(
                        "puppet_level_changed",
                        user_id,
                        action,
                        previous_level,
                        previous_level,
                        stone_cost,
                    )
                conn.execute(
                    """
                    INSERT INTO puppet_operations (
                        operation_id, user_id, action, previous_level, current_level,
                        stone_cost
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        operation_id,
                        user_id,
                        action,
                        previous_level,
                        current_level,
                        stone_cost,
                    ),
                )
                conn.commit()
                return PuppetOperation(
                    "purchased" if action == "purchase" else "upgraded",
                    user_id,
                    action,
                    previous_level,
                    current_level,
                    stone_cost,
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

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
        return self.status in {"harvested", "duplicate"}

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

    def get_result(self, operation_id: str) -> PuppetHarvest | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS puppet_harvest_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, payload TEXT NOT NULL, "
                "rewards_json TEXT NOT NULL, stone_cost INTEGER NOT NULL, "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT user_id, rewards_json, stone_cost FROM puppet_harvest_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            rewards = tuple(
                PuppetHarvestReward(**item) for item in json.loads(str(previous[1]))
            )
            return PuppetHarvest(
                "duplicate", str(previous[0]), rewards=rewards, stone_cost=int(previous[2] or 0)
            )

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
        operation_id: str | None = None,
    ) -> PuppetHarvest:
        user_id = str(user_id)
        operation_id = str(operation_id or "").strip()
        now_text = now.strftime("%Y-%m-%d %H:%M:%S")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "ATTACH DATABASE %s AS player_data", (str(self._player_database),)
            )
            try:
                conn.execute("BEGIN IMMEDIATE")
                if operation_id:
                    conn.execute(
                        "CREATE TABLE IF NOT EXISTS puppet_harvest_operations ("
                        "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, payload TEXT NOT NULL, "
                        "rewards_json TEXT NOT NULL, stone_cost INTEGER NOT NULL, "
                        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                    )
                    previous = conn.execute(
                        "SELECT user_id, rewards_json, stone_cost FROM puppet_harvest_operations WHERE operation_id=%s",
                        (operation_id,),
                    ).fetchone()
                    if previous is not None:
                        conn.rollback()
                        if str(previous[0]) != user_id:
                            return PuppetHarvest("state_changed", user_id)
                        rewards = tuple(
                            PuppetHarvestReward(**item) for item in json.loads(str(previous[1]))
                        )
                        return PuppetHarvest(
                            "duplicate", user_id, rewards=rewards, stone_cost=int(previous[2] or 0)
                        )
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
                    UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(%s AS REAL)
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
                if operation_id:
                    rewards_json = json.dumps(
                        [
                            {
                                "goods_id": r.goods_id,
                                "goods_name": r.goods_name,
                                "goods_type": r.goods_type,
                                "quantity": r.quantity,
                            }
                            for r in rewards
                        ],
                        ensure_ascii=True,
                        separators=(",", ":"),
                    )
                    # Request identity only.
                    payload = json.dumps([user_id], ensure_ascii=True, separators=(",", ":"))
                    conn.execute(
                        "INSERT INTO puppet_harvest_operations "
                        "(operation_id, user_id, payload, rewards_json, stone_cost) VALUES (%s,%s,%s,%s,%s)",
                        (operation_id, user_id, payload, rewards_json, harvest_cost),
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

__all__ = [
    "PuppetOperation",
    "PuppetOperationService",
    "PuppetHarvestReward",
    "PuppetHarvest",
    "PuppetHarvestService",
]
