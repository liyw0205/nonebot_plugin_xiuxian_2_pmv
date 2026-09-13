from __future__ import annotations

import json
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from ..package_reward.domain import PackageReward
from .domain import AccessoryReward
from .schemas import AccessoryPackageRequest


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


class AccessoryPackageGameRepository:
    """Owns the game-db half of the cross-database protocol."""

    def ensure_schema(self, uow: DatabaseUnitOfWork) -> None:
        uow.execute(
            """
            CREATE TABLE IF NOT EXISTS accessory_package_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                package_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                rewards_json TEXT NOT NULL,
                accessories_json TEXT NOT NULL,
                before_json TEXT NOT NULL,
                status TEXT NOT NULL,
                error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(accessory_package_operations)")}
        additions = {
            "before_json": "TEXT NOT NULL DEFAULT '{}'",
            "status": "TEXT NOT NULL DEFAULT 'legacy'",
            "error": "TEXT NOT NULL DEFAULT ''",
            "updated_at": "TEXT NOT NULL DEFAULT ''",
            "max_goods_num": "INTEGER NOT NULL DEFAULT 0",
            "accessory_limit": "INTEGER NOT NULL DEFAULT 0",
        }
        for name, declaration in additions.items():
            if name not in columns:
                uow.execute(f"ALTER TABLE accessory_package_operations ADD COLUMN {name} {declaration}")

    def get(self, uow: DatabaseUnitOfWork, operation_id: str) -> dict[str, Any] | None:
        self.ensure_schema(uow)
        return uow.query_one("SELECT * FROM accessory_package_operations WHERE operation_id = ?", (operation_id,))

    def snapshot(self, uow: DatabaseUnitOfWork, request: AccessoryPackageRequest) -> dict[str, Any]:
        game_tables = {
            str(row["name"])
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "user_xiuxian" not in game_tables:
            return {"stone": None, "package": None, "rewards": {}}
        columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")} if "back" in game_tables else set()
        bind_column = "bind_num" if "bind_num" in columns else "0 AS bind_num"
        user = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id = ?", (request.user_id,))
        package = (
            uow.query_one(
                f"SELECT goods_name, goods_type, goods_num, {bind_column} FROM back WHERE user_id = ? AND goods_id = ?",
                (request.user_id, request.package_id),
            )
            if "back" in game_tables
            else None
        )
        rewards: dict[str, dict[str, Any] | None] = {}
        for reward in request.rewards:
            if reward.name == "灵石" or reward.item_id is None:
                continue
            key = str(reward.item_id)
            if key not in rewards:
                rewards[key] = (
                    uow.query_one(
                        f"SELECT goods_name, goods_type, goods_num, {bind_column} FROM back WHERE user_id = ? AND goods_id = ?",
                        (request.user_id, reward.item_id),
                    )
                    if "back" in game_tables
                    else None
                )
        return {
            "stone": None if user is None else user.get("stone"),
            "package": None if package is None else dict(package),
            "rewards": rewards,
        }

    def prepare(self, uow: DatabaseUnitOfWork, request: AccessoryPackageRequest) -> tuple[str, dict[str, Any]]:
        existing = self.get(uow, request.operation_id)
        if existing is not None:
            if existing["status"] == "compensated":
                uow.execute("DELETE FROM accessory_package_operations WHERE operation_id = ?", (request.operation_id,))
            elif existing["status"] == "pending_accessory":
                return "applied", json.loads(existing["before_json"])
            elif existing["status"] == "applied":
                return "state_changed", json.loads(existing["before_json"])
        before = self.snapshot(uow, request)
        if before["stone"] is None:
            return "user_missing", before
        package = before["package"]
        if package is None or int(package.get("goods_num") or 0) < request.quantity:
            return "item_insufficient", before

        totals: dict[int, int] = {}
        stone_delta = 0
        for reward in request.rewards:
            if reward.name == "灵石":
                stone_delta += reward.quantity
            elif reward.item_id is not None:
                totals[reward.item_id] = totals.get(reward.item_id, 0) + reward.quantity
        if int(before["stone"] or 0) + stone_delta < 0:
            return "stone_insufficient", before
        for item_id, amount in totals.items():
            row = before["rewards"].get(str(item_id))
            if (int(row.get("goods_num") or 0) if row else 0) + amount > request.max_goods_num:
                return "inventory_full", before

        columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
        if "bind_num" in columns:
            changed = uow.execute(
                "UPDATE back SET goods_num = goods_num - ?, bind_num = MIN(COALESCE(bind_num, 0), goods_num - ?) "
                "WHERE user_id = ? AND goods_id = ? AND goods_num >= ?",
                (request.quantity, request.quantity, request.user_id, request.package_id, request.quantity),
            )
        else:
            changed = uow.execute(
                "UPDATE back SET goods_num = goods_num - ? WHERE user_id = ? AND goods_id = ? AND goods_num >= ?",
                (request.quantity, request.user_id, request.package_id, request.quantity),
            )
        if changed.rowcount != 1:
            return "state_changed", before
        if stone_delta:
            changed = uow.execute(
                "UPDATE user_xiuxian SET stone = CAST(stone AS REAL) + ? WHERE user_id = ? AND CAST(stone AS REAL) + ? >= 0",
                (stone_delta, request.user_id, stone_delta),
            )
            if changed.rowcount != 1:
                return "stone_insufficient", before
        for reward in request.rewards:
            if reward.name == "灵石" or reward.item_id is None:
                continue
            if "bind_num" in columns:
                uow.execute(
                    "INSERT INTO back(user_id, goods_id, goods_name, goods_type, goods_num, bind_num) VALUES (?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(user_id, goods_id) DO UPDATE SET goods_name=excluded.goods_name, goods_type=excluded.goods_type, "
                    "goods_num=COALESCE(back.goods_num, 0)+excluded.goods_num, bind_num=MIN(COALESCE(back.bind_num, 0)+excluded.goods_num, ?)",
                    (request.user_id, reward.item_id, reward.name, reward.item_type, reward.quantity, reward.quantity, request.max_goods_num),
                )
            else:
                uow.execute(
                    "INSERT INTO back(user_id, goods_id, goods_name, goods_type, goods_num) VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT(user_id, goods_id) DO UPDATE SET goods_name=excluded.goods_name, goods_type=excluded.goods_type, "
                    "goods_num=COALESCE(back.goods_num, 0)+excluded.goods_num",
                    (request.user_id, reward.item_id, reward.name, reward.item_type, reward.quantity),
                )
        self.ensure_schema(uow)
        uow.execute(
            "INSERT INTO accessory_package_operations(operation_id,user_id,package_id,quantity,rewards_json,accessories_json,before_json,status,max_goods_num,accessory_limit) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'pending_accessory', ?, ?)",
            (
                request.operation_id,
                request.user_id,
                request.package_id,
                request.quantity,
                _json([item.to_dict() for item in request.rewards]),
                _json([item.to_dict() for item in request.accessories]),
                _json(before),
                request.max_goods_num,
                request.accessory_limit,
            ),
        )
        return "applied", before

    def compensate(self, uow: DatabaseUnitOfWork, operation_id: str, error: str = "") -> bool:
        row = self.get(uow, operation_id)
        if row is None or row["status"] in {"compensated", "failed"}:
            return row is not None
        before = json.loads(row["before_json"])
        user_id, package_id = str(row["user_id"]), int(row["package_id"])
        package = before.get("package")
        if package is None:
            return False
        uow.execute(
            "UPDATE user_xiuxian SET stone = ? WHERE user_id = ?",
            (before.get("stone"), user_id),
        )
        self._restore_item(uow, user_id, package_id, package)
        for item_id, state in (before.get("rewards") or {}).items():
            self._restore_item(uow, user_id, int(item_id), state)
        uow.execute(
            "UPDATE accessory_package_operations SET status='compensated', error=?, updated_at=CURRENT_TIMESTAMP WHERE operation_id=?",
            (str(error), operation_id),
        )
        return True

    @staticmethod
    def _restore_item(uow: DatabaseUnitOfWork, user_id: str, item_id: int, state: dict[str, Any] | None) -> None:
        if state is None:
            uow.execute("DELETE FROM back WHERE user_id = ? AND goods_id = ?", (user_id, item_id))
            return
        columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
        if "bind_num" in columns:
            uow.execute(
                "UPDATE back SET goods_name=?, goods_type=?, goods_num=?, bind_num=? WHERE user_id=? AND goods_id=?",
                (state.get("goods_name"), state.get("goods_type"), state.get("goods_num"), state.get("bind_num"), user_id, item_id),
            )
        else:
            uow.execute(
                "UPDATE back SET goods_name=?, goods_type=?, goods_num=? WHERE user_id=? AND goods_id=?",
                (state.get("goods_name"), state.get("goods_type"), state.get("goods_num"), user_id, item_id),
            )

    def finalize(self, uow: DatabaseUnitOfWork, operation_id: str) -> None:
        self.ensure_schema(uow)
        uow.execute("UPDATE accessory_package_operations SET status='applied', updated_at=CURRENT_TIMESTAMP WHERE operation_id=?", (operation_id,))


class AccessoryPackagePlayerRepository:
    """Owns the player-db half and makes accessory writes idempotent."""

    def ensure_schema(self, uow: DatabaseUnitOfWork) -> None:
        uow.execute(
            """
            CREATE TABLE IF NOT EXISTS accessory_package_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                accessories_json TEXT NOT NULL,
                before_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        uow.execute("CREATE TABLE IF NOT EXISTS player_accessory (user_id TEXT PRIMARY KEY, equipped TEXT, bag TEXT)")
        columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(player_accessory)")}
        if "equipped" not in columns:
            uow.execute("ALTER TABLE player_accessory ADD COLUMN equipped TEXT")
        if "bag" not in columns:
            uow.execute("ALTER TABLE player_accessory ADD COLUMN bag TEXT")

    def apply(self, uow: DatabaseUnitOfWork, *, operation_id: str, user_id: str, accessories: tuple[AccessoryReward, ...], limit: int) -> str:
        self.ensure_schema(uow)
        previous = uow.query_one("SELECT status FROM accessory_package_operations WHERE operation_id=?", (operation_id,))
        if previous is not None:
            return "applied" if previous["status"] == "applied" else "state_changed"
        row = uow.query_one("SELECT equipped, bag FROM player_accessory WHERE user_id=?", (user_id,))
        equipped = json.loads(row["equipped"]) if row and row.get("equipped") else {}
        bag = json.loads(row["bag"]) if row and row.get("bag") else []
        if not isinstance(equipped, dict):
            equipped = {}
        if not isinstance(bag, list):
            bag = []
        before = {"equipped": equipped, "bag": bag}
        if len(bag) + sum(1 for value in equipped.values() if value) + len(accessories) > limit:
            return "accessory_full"
        bag.extend(item.to_dict() for item in accessories)
        encoded = _json(bag)
        uow.execute(
            "INSERT INTO player_accessory(user_id,equipped,bag) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET equipped=excluded.equipped, bag=excluded.bag",
            (user_id, _json(equipped), encoded),
        )
        uow.execute(
            "INSERT INTO accessory_package_operations(operation_id,user_id,accessories_json,before_json,status) VALUES (?, ?, ?, ?, 'applied')",
            (operation_id, user_id, _json([item.to_dict() for item in accessories]), _json(before)),
        )
        return "applied"


__all__ = ["AccessoryPackageGameRepository", "AccessoryPackagePlayerRepository"]
