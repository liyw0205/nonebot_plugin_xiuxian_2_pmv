from __future__ import annotations

import json
from contextlib import closing
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PackageReward:
    item_id: int | None
    name: str
    item_type: str | None
    quantity: int


@dataclass(frozen=True)
class PackageOpenResult:
    status: str
    user_id: str
    package_id: int
    quantity: int
    rewards: tuple[PackageReward, ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PackageRewardService:
    """Consume packages and grant main-database rewards atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS package_reward_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                package_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                rewards_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _decode_rewards(payload: str) -> tuple[PackageReward, ...]:
        return tuple(PackageReward(**entry) for entry in json.loads(payload))

    def apply(
        self,
        operation_id,
        user_id,
        package_id,
        quantity,
        rewards,
        *,
        max_goods_num,
    ) -> PackageOpenResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        package_id = int(package_id)
        quantity = int(quantity)
        max_goods_num = int(max_goods_num)
        if quantity <= 0 or max_goods_num <= 0:
            raise ValueError("quantity and max_goods_num must be positive")
        normalized = tuple(
            reward if isinstance(reward, PackageReward) else PackageReward(
                None if reward[0] is None else int(reward[0]),
                str(reward[1]),
                None if reward[2] is None else str(reward[2]),
                int(reward[3]),
            )
            for reward in rewards
        )
        if not normalized or any(
            reward.quantity == 0
            or (reward.name != "灵石" and reward.quantity < 0)
            for reward in normalized
        ):
            raise ValueError("item rewards must be positive and stone rewards non-zero")
        if any(reward.name != "灵石" and reward.item_id is None for reward in normalized):
            raise ValueError("item rewards require item_id")
        if any(reward.name == "灵石" and reward.item_id is not None for reward in normalized):
            raise ValueError("stone rewards must not contain item_id")

        def result(status, result_quantity=quantity, result_rewards=normalized):
            return PackageOpenResult(
                status, user_id, package_id, int(result_quantity), tuple(result_rewards)
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT user_id, package_id, quantity, rewards_json "
                    "FROM package_reward_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if (
                        str(previous[0]) != user_id
                        or int(previous[1]) != package_id
                        or int(previous[2]) != quantity
                    ):
                        return result("state_changed")
                    return result(
                        "duplicate", previous[2], self._decode_rewards(previous[3])
                    )

                user = conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                package = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, package_id),
                ).fetchone()
                if package is None or int(package[0] or 0) < quantity:
                    conn.rollback()
                    return result("item_insufficient")

                columns = set(conn.column_names("back"))
                bind_update = ""
                params: list[object] = [quantity]
                if "bind_num" in columns:
                    bind_update = ", bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s)"
                    params.append(quantity)
                consumed = conn.execute(
                    f"UPDATE back SET goods_num=goods_num-%s{bind_update} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, package_id, quantity),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")

                stone_delta = sum(
                    reward.quantity for reward in normalized if reward.name == "灵石"
                )
                if stone_delta:
                    stone_update = conn.execute(
                        "UPDATE user_xiuxian SET stone=stone+%s "
                        "WHERE user_id=%s AND stone+%s>=0",
                        (stone_delta, user_id, stone_delta),
                    )
                    if stone_update.rowcount != 1:
                        conn.rollback()
                        return result("stone_insufficient")

                item_totals: dict[int, int] = {}
                for reward in normalized:
                    if reward.name != "灵石":
                        item_totals[reward.item_id] = (
                            item_totals.get(reward.item_id, 0) + reward.quantity
                        )
                for item_id, reward_quantity in item_totals.items():
                    current = conn.execute(
                        "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    current_quantity = int(current[0] or 0) if current else 0
                    if current_quantity + reward_quantity > max_goods_num:
                        conn.rollback()
                        return result("inventory_full")

                for reward in normalized:
                    if reward.name == "灵石":
                        continue
                    conn.execute(
                        "INSERT INTO back (user_id, goods_id, goods_name, goods_type, "
                        "goods_num, bind_num) VALUES (%s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                        "goods_num=MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s), "
                        "bind_num=MIN(COALESCE(back.bind_num, 0)+EXCLUDED.goods_num, "
                        "MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s))",
                        (
                            user_id, reward.item_id, reward.name, reward.item_type,
                            reward.quantity, reward.quantity,
                            max_goods_num, max_goods_num,
                        ),
                    )

                payload = json.dumps(
                    [asdict(reward) for reward in normalized], ensure_ascii=False
                )
                conn.execute(
                    "INSERT INTO package_reward_operations "
                    "(operation_id, user_id, package_id, quantity, rewards_json) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, package_id, quantity, payload),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise


__all__ = ["PackageOpenResult", "PackageReward", "PackageRewardService"]
