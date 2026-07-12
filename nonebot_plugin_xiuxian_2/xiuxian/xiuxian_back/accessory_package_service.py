from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .package_reward_service import PackageReward


@dataclass(frozen=True)
class AccessoryPackageResult:
    status: str
    user_id: str
    package_id: int
    quantity: int
    rewards: tuple[PackageReward, ...]
    accessories: tuple[dict, ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class AccessoryPackageService:
    """Grant mixed package rewards in one attached-database transaction."""

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
    def _ensure_schema(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS accessory_package_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                package_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                rewards_json TEXT NOT NULL,
                accessories_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.player_accessory "
            "(user_id TEXT PRIMARY KEY, equipped TEXT, bag TEXT)"
        )
        columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA player_data.table_info(player_accessory)"
            ).fetchall()
        }
        if "equipped" not in columns:
            conn.execute("ALTER TABLE player_data.player_accessory ADD COLUMN equipped TEXT")
        if "bag" not in columns:
            conn.execute("ALTER TABLE player_data.player_accessory ADD COLUMN bag TEXT")

    @staticmethod
    def _decode_rewards(payload: str) -> tuple[PackageReward, ...]:
        return tuple(PackageReward(**entry) for entry in json.loads(payload))

    @staticmethod
    def _load_list(payload) -> list:
        if not payload:
            return []
        value = json.loads(payload) if isinstance(payload, str) else payload
        return value if isinstance(value, list) else []

    @staticmethod
    def _load_equipped(payload) -> dict:
        if not payload:
            return {}
        value = json.loads(payload) if isinstance(payload, str) else payload
        return value if isinstance(value, dict) else {}

    def apply(
        self,
        operation_id,
        user_id,
        package_id,
        quantity,
        rewards,
        accessories,
        *,
        max_goods_num,
        accessory_limit,
    ) -> AccessoryPackageResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        package_id = int(package_id)
        quantity = int(quantity)
        max_goods_num = int(max_goods_num)
        accessory_limit = int(accessory_limit)
        normalized_rewards = tuple(rewards)
        normalized_accessories = tuple(dict(item) for item in accessories)
        if quantity <= 0 or max_goods_num <= 0 or accessory_limit <= 0:
            raise ValueError("quantities and limits must be positive")
        if not normalized_accessories:
            raise ValueError("accessories must not be empty")
        if any(not str(item.get("uid", "")).strip() for item in normalized_accessories):
            raise ValueError("accessory uid must not be empty")
        if len({str(item["uid"]) for item in normalized_accessories}) != len(normalized_accessories):
            raise ValueError("accessory uid must be unique")

        def result(status, result_quantity=quantity, result_rewards=normalized_rewards,
                   result_accessories=normalized_accessories):
            return AccessoryPackageResult(
                status, user_id, package_id, int(result_quantity),
                tuple(result_rewards), tuple(result_accessories),
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT user_id, package_id, quantity, rewards_json, accessories_json "
                    "FROM accessory_package_operations WHERE operation_id=%s",
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
                        "duplicate", previous[2], self._decode_rewards(previous[3]),
                        tuple(json.loads(previous[4])),
                    )

                if conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return result("user_missing")
                package = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, package_id),
                ).fetchone()
                if package is None or int(package[0] or 0) < quantity:
                    conn.rollback()
                    return result("item_insufficient")

                accessory_row = conn.execute(
                    "SELECT equipped, bag FROM player_data.player_accessory WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                equipped = self._load_equipped(accessory_row[0]) if accessory_row else {}
                bag = self._load_list(accessory_row[1]) if accessory_row else []
                equipped_count = sum(1 for item in equipped.values() if item)
                if len(bag) + equipped_count + len(normalized_accessories) > accessory_limit:
                    conn.rollback()
                    return result("accessory_full")

                item_totals: dict[int, int] = {}
                stone_delta = 0
                for reward in normalized_rewards:
                    if reward.name == "灵石":
                        stone_delta += reward.quantity
                    else:
                        item_totals[reward.item_id] = item_totals.get(reward.item_id, 0) + reward.quantity
                for item_id, reward_quantity in item_totals.items():
                    current = conn.execute(
                        "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(current[0] or 0) if current else 0) + reward_quantity > max_goods_num:
                        conn.rollback()
                        return result("inventory_full")

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

                if stone_delta:
                    updated = conn.execute(
                        "UPDATE user_xiuxian SET stone=stone+%s "
                        "WHERE user_id=%s AND stone+%s>=0",
                        (stone_delta, user_id, stone_delta),
                    )
                    if updated.rowcount != 1:
                        conn.rollback()
                        return result("stone_insufficient")

                for reward in normalized_rewards:
                    if reward.name == "灵石":
                        continue
                    conn.execute(
                        "INSERT INTO back (user_id, goods_id, goods_name, goods_type, "
                        "goods_num, bind_num) VALUES (%s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                        "goods_num=COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, "
                        "bind_num=MIN(COALESCE(back.bind_num, 0)+EXCLUDED.goods_num, "
                        "COALESCE(back.goods_num, 0)+EXCLUDED.goods_num)",
                        (user_id, reward.item_id, reward.name, reward.item_type,
                         reward.quantity, reward.quantity),
                    )

                bag.extend(normalized_accessories)
                equipped_json = json.dumps(equipped, ensure_ascii=False)
                bag_json = json.dumps(bag, ensure_ascii=False)
                conn.execute(
                    "INSERT INTO player_data.player_accessory (user_id, equipped, bag) "
                    "VALUES (%s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET "
                    "equipped=EXCLUDED.equipped, bag=EXCLUDED.bag",
                    (user_id, equipped_json, bag_json),
                )
                rewards_json = json.dumps(
                    [reward.__dict__ for reward in normalized_rewards], ensure_ascii=False
                )
                accessories_json = json.dumps(normalized_accessories, ensure_ascii=False)
                conn.execute(
                    "INSERT INTO accessory_package_operations "
                    "(operation_id, user_id, package_id, quantity, rewards_json, accessories_json) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, package_id, quantity, rewards_json, accessories_json),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")


__all__ = ["AccessoryPackageResult", "AccessoryPackageService"]
