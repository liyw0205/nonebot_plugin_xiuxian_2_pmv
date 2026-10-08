from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class ActivityBossRankClaimResult:
    status: str
    name: str = ""
    rank: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


class ActivityBossRankClaimRepository:
    """Grant a frozen rank-tier reward in game_db and project its claim marker."""

    def __init__(
        self,
        game_database: str | Path,
        activity_database: str | Path,
        *,
        clock: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.activity_database = str(activity_database)
        self.clock = clock or SystemClock()
        self._same_database = (
            Path(self.game_database).resolve() == Path(self.activity_database).resolve()
        )

    @staticmethod
    def _assert_schema(uow: DatabaseUnitOfWork) -> None:
        required = {
            "activity_boss_rank_claim_operations": {
                "operation_id", "payload", "request_json", "result_json", "result_status", "status", "updated_at",
            },
            "activity_boss_rank_claim_reservations": {
                "activity_key", "user_id", "tier_key", "operation_id",
            },
        }
        for table, columns in required.items():
            actual = {str(row["name"]) for row in uow.query_all(f"PRAGMA table_info({table})")}
            if not columns.issubset(actual):
                raise RuntimeError(f"activity_reward.010 schema_missing: {table}")

    def assert_schema_ready(self) -> None:
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            self._assert_schema(uow)

    @staticmethod
    def _normalize(
        operation_id: str,
        user_id: str,
        activity_key: str,
        tiers: Any,
        max_goods_num: int,
    ) -> tuple[str, str, dict[str, Any]]:
        user_id, activity_key = str(user_id).strip(), str(activity_key).strip()
        max_goods_num = int(max_goods_num)
        normalized = []
        seen = set()
        for raw in tiers:
            rank_min, rank_max = int(raw["rank_min"]), int(raw["rank_max"])
            if rank_min <= 0 or rank_max < rank_min:
                raise ValueError("valid activity boss rank ranges are required")
            name = str(raw.get("name") or "排行奖励")
            reward = str(raw.get("reward") or "")
            reward_items = [dict(item) for item in raw.get("reward_items") or ()]
            identity = (rank_min, rank_max)
            if identity in seen:
                raise ValueError("unique activity boss rank ranges are required")
            seen.add(identity)
            normalized.append({
                "rank_min": rank_min,
                "rank_max": rank_max,
                "name": name,
                "reward": reward,
                "reward_items": reward_items,
            })
        if not user_id or not activity_key or max_goods_num < 0:
            raise ValueError("valid activity boss rank claim is required")
        payload = _json([
            user_id,
            activity_key,
            [[row["rank_min"], row["rank_max"], row["name"], row["reward"], row["reward_items"]] for row in normalized],
        ])
        request = {
            "user_id": user_id,
            "activity_key": activity_key,
            "tiers": normalized,
            "max_goods_num": max_goods_num,
        }
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        return operation_id, payload, request

    @staticmethod
    def _decode(row: Mapping[str, Any], *, replay: bool = False) -> ActivityBossRankClaimResult:
        data = json.loads(str(row["result_json"] or "{}"))
        status = str(row["result_status"])
        if replay and status == "applied":
            status = "duplicate"
        return ActivityBossRankClaimResult(status, str(data.get("name") or ""), int(data.get("rank", 0)))

    def get_result(
        self,
        operation_id: str,
        user_id: str | None = None,
        *,
        payload: str | None = None,
    ) -> ActivityBossRankClaimResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            self._assert_schema(uow)
            row = uow.query_one(
                "SELECT payload,request_json,result_json,result_status,status "
                "FROM activity_boss_rank_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
            if row is None:
                return None
            request = json.loads(str(row["request_json"]))
            if user_id is not None and str(request["user_id"]) != str(user_id):
                return ActivityBossRankClaimResult("operation_conflict")
            if payload is not None and str(row["payload"]) != payload:
                return ActivityBossRankClaimResult("operation_conflict")
            if row["status"] not in {"applied", "rejected"}:
                return None
            return self._decode(row, replay=True)

    def pending_status(self, operation_id: str, user_id: str | None = None) -> str | None:
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            self._assert_schema(uow)
            row = uow.query_one(
                "SELECT request_json,status FROM activity_boss_rank_claim_operations WHERE operation_id=?",
                (str(operation_id).strip(),),
            )
        if row is None:
            return None
        request = json.loads(str(row["request_json"]))
        if user_id is not None and str(request["user_id"]) != str(user_id):
            return "operation_conflict"
        status = str(row["status"])
        return status if status in {"started", "granted"} else None

    def validate_claim(self, operation_id, user_id, activity_key, tiers, max_goods_num):
        resolved_id, payload, _ = self._normalize(operation_id, user_id, activity_key, tiers, max_goods_num)
        return resolved_id, payload

    @staticmethod
    def _rank_snapshot(uow: DatabaseUnitOfWork, request: Mapping[str, Any]) -> tuple[int, str, dict[str, Any]] | tuple[int, str, None]:
        ordered = [str(row["user_id"]) for row in uow.query_all(
            "SELECT user_id FROM activity_boss_damage WHERE activity_key=? ORDER BY total_damage DESC",
            (request["activity_key"],),
        )]
        user_id = str(request["user_id"])
        if user_id not in ordered:
            return 0, "not_participant", None
        rank = ordered.index(user_id) + 1
        tier = next((row for row in request["tiers"] if int(row["rank_min"]) <= rank <= int(row["rank_max"])), None)
        if tier is None:
            return rank, "not_eligible", None
        tier_key = f"{tier['rank_min']}-{tier['rank_max']}"
        claimed = uow.query_one(
            "SELECT 1 AS present FROM activity_boss_rank_claim "
            "WHERE activity_key=? AND user_id=? AND tier_key=?",
            (request["activity_key"], user_id, tier_key),
        )
        if claimed is not None:
            return rank, "already_claimed", None
        snapshot = {
            "user_id": user_id,
            "activity_key": str(request["activity_key"]),
            "rank": rank,
            "tier_key": tier_key,
            "name": str(tier["name"]),
            "reward": str(tier["reward"]),
            "reward_items": [dict(item) for item in tier["reward_items"]],
            "max_goods_num": int(request["max_goods_num"]),
        }
        return rank, "", snapshot

    def _legacy_rank_still_claimable(self, uow: DatabaseUnitOfWork, request: Mapping[str, Any]) -> str:
        rank, status, snapshot = self._rank_snapshot(uow, request)
        if rank != int(request["rank"]):
            return "state_changed"
        if status:
            return "already_claimed" if status == "already_claimed" else "state_changed"
        if (
            snapshot["tier_key"] != request["tier_key"]
            or snapshot["name"] != request["name"]
            or snapshot["reward"] != request["reward"]
            or snapshot["reward_items"] != request["reward_items"]
        ):
            return "state_changed"
        return ""

    def prepare_operation(self, uow, operation_id, user_id, activity_key, tiers, max_goods_num):
        operation_id, payload, request = self._normalize(operation_id, user_id, activity_key, tiers, max_goods_num)
        self._assert_schema(uow)
        operation = uow.query_one(
            "SELECT payload,request_json,result_json,result_status,status "
            "FROM activity_boss_rank_claim_operations WHERE operation_id=?",
            (operation_id,),
        )
        if operation is not None:
            if str(operation["status"]) in {"started", "granted"}:
                saved = json.loads(str(operation["request_json"]))
                if (str(saved["user_id"]), str(saved["activity_key"])) != (str(user_id), str(activity_key)):
                    return ActivityBossRankClaimResult("operation_conflict"), False
                if str(operation["payload"]) != payload:
                    return ActivityBossRankClaimResult("operation_conflict"), False
                request = saved
            elif str(operation["payload"]) != payload:
                return ActivityBossRankClaimResult("operation_conflict"), False
            else:
                return self._decode(operation, replay=True), False
        else:
            uow.execute(
                "INSERT INTO activity_boss_rank_claim_operations"
                "(operation_id,payload,request_json,result_json,result_status,status) "
                "VALUES(?,?,?,'{}','started','started')",
                (operation_id, payload, _json(request)),
            )
            if self._same_database:
                rank, status, snapshot = self._rank_snapshot(uow, request)
            else:
                with DatabaseUnitOfWork(self.activity_database, read_only=True) as legacy:
                    rank, status, snapshot = self._rank_snapshot(legacy, request)
            if status:
                return self._reject(uow, operation_id, status, rank), False
            request = {**request, **snapshot}
            identity = (request["activity_key"], request["user_id"], request["tier_key"])
            uow.execute(
                "INSERT OR IGNORE INTO activity_boss_rank_claim_reservations"
                "(activity_key,user_id,tier_key,operation_id) VALUES(?,?,?,?)",
                (*identity, operation_id),
            )
            owner = uow.query_one(
                "SELECT operation_id FROM activity_boss_rank_claim_reservations "
                "WHERE activity_key=? AND user_id=? AND tier_key=?",
                identity,
            )
            if owner is None or str(owner["operation_id"]) != operation_id:
                return self._reject(uow, operation_id, "claim_in_progress", rank), False
            uow.execute(
                "UPDATE activity_boss_rank_claim_operations SET request_json=? WHERE operation_id=?",
                (_json(request), operation_id),
            )
        return None, operation is None

    @staticmethod
    def _reject(uow, operation_id: str, status: str, rank: int = 0) -> ActivityBossRankClaimResult:
        changed = uow.execute(
            "UPDATE activity_boss_rank_claim_operations SET status='rejected',result_status=?,"
            "result_json=?,updated_at=CURRENT_TIMESTAMP WHERE operation_id=? AND status='started'",
            (status, _json({"rank": int(rank)}), operation_id),
        )
        if changed.rowcount != 1:
            raise RuntimeError("cannot reject a non-started activity boss rank operation")
        uow.execute("DELETE FROM activity_boss_rank_claim_reservations WHERE operation_id=?", (operation_id,))
        return ActivityBossRankClaimResult(status, rank=int(rank))

    @staticmethod
    def _aggregate_rewards(request: Mapping[str, Any]) -> tuple[int, list[dict[str, Any]]]:
        stone = 0
        items: dict[int, dict[str, Any]] = {}
        for raw_item in request["reward_items"]:
            item = dict(raw_item)
            quantity = int(item.get("quantity", 0))
            if quantity <= 0:
                raise ValueError("activity boss rank reward quantities must be positive")
            if str(item.get("type")) == "stone":
                stone += quantity
                continue
            item_id = int(item.get("id", 0))
            name, item_type = str(item.get("name") or ""), str(item.get("type") or "")
            if item_id <= 0 or not name or not item_type:
                raise ValueError("complete activity boss rank item rewards are required")
            existing = items.get(item_id)
            if existing is not None and (existing["name"], existing["type"]) != (name, item_type):
                raise ValueError("conflicting activity boss rank reward metadata")
            if existing is None:
                existing = {"id": item_id, "name": name, "type": item_type, "quantity": 0}
                items[item_id] = existing
            existing["quantity"] += quantity
        return stone, [items[key] for key in sorted(items)]

    def _apply_started_operation(
        self,
        game: DatabaseUnitOfWork,
        operation_id: str,
        request: Mapping[str, Any],
    ) -> bool:
        """Apply assets and mark an operation granted on an already-open UoW."""
        stone, items = self._aggregate_rewards(request)
        operation = game.query_one(
            "SELECT status FROM activity_boss_rank_claim_operations WHERE operation_id=?",
            (operation_id,),
        )
        if operation is None or str(operation["status"]) != "started":
            raise RuntimeError("activity boss rank operation state changed")
        owner = game.query_one(
            "SELECT operation_id FROM activity_boss_rank_claim_reservations "
            "WHERE activity_key=? AND user_id=? AND tier_key=?",
            (request["activity_key"], request["user_id"], request["tier_key"]),
        )
        if owner is None or str(owner["operation_id"]) != operation_id:
            raise RuntimeError("activity boss rank reservation is missing or changed")
        if game.query_one("SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (request["user_id"],)) is None:
            self._reject(game, operation_id, "user_missing", int(request["rank"]))
            return False
        for item in items:
            row = game.query_one(
                "SELECT COALESCE(goods_num,0) AS quantity FROM back WHERE user_id=? AND goods_id=?",
                (request["user_id"], item["id"]),
            )
            if (int(row["quantity"]) if row else 0) + int(item["quantity"]) > int(request["max_goods_num"]):
                self._reject(game, operation_id, "inventory_full", int(request["rank"]))
                return False
        now = self.clock.now().isoformat()
        if stone:
            changed = game.execute(
                "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+? WHERE user_id=?",
                (stone, request["user_id"]),
            )
            if changed.rowcount != 1:
                self._reject(game, operation_id, "user_missing", int(request["rank"]))
                return False
        columns = {str(row["name"]) for row in game.query_all("PRAGMA table_info(back)")}
        for item in items:
            existing = game.query_one(
                "SELECT COALESCE(goods_num,0) AS quantity FROM back WHERE user_id=? AND goods_id=?",
                (request["user_id"], item["id"]),
            )
            if existing is None:
                names = ["user_id", "goods_id", "goods_name", "goods_type", "goods_num"]
                values: list[Any] = [request["user_id"], item["id"], item["name"], item["type"], item["quantity"]]
                for field in ("create_time", "update_time"):
                    if field in columns:
                        names.append(field)
                        values.append(now)
                if "bind_num" in columns:
                    names.append("bind_num")
                    values.append(item["quantity"])
                game.execute(
                    f"INSERT INTO back({','.join(names)}) VALUES({','.join('?' for _ in values)})",
                    tuple(values),
                )
            else:
                assignments = "goods_name=?,goods_type=?,goods_num=COALESCE(goods_num,0)+?"
                values = [item["name"], item["type"], item["quantity"]]
                if "update_time" in columns:
                    assignments += ",update_time=?"
                    values.append(now)
                if "bind_num" in columns:
                    assignments += ",bind_num=COALESCE(bind_num,0)+?"
                    values.append(item["quantity"])
                values.extend((request["user_id"], item["id"]))
                game.execute(f"UPDATE back SET {assignments} WHERE user_id=? AND goods_id=?", tuple(values))
        game.execute(
            "UPDATE activity_boss_rank_claim_operations SET status='granted',result_status='applied',"
            "result_json=?,updated_at=? WHERE operation_id=? AND status='started'",
            (_json({"name": request["name"], "rank": request["rank"]}), now, operation_id),
        )
        return True

    def claim(self, operation_id: str, user_id: str, activity_key: str, tiers: Any, max_goods_num: int, *, newly_prepared: bool = False) -> ActivityBossRankClaimResult:
        operation_id = str(operation_id).strip()
        _, payload, request = self._normalize(operation_id, user_id, activity_key, tiers, max_goods_num)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as game:
            self._assert_schema(game)
            operation = game.query_one(
                "SELECT payload,request_json,result_json,result_status,status "
                "FROM activity_boss_rank_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
            if operation is None:
                raise RuntimeError("activity boss rank operation must be prepared before claiming")
            if str(operation["status"]) in {"started", "granted"}:
                saved = json.loads(str(operation["request_json"]))
                if (str(saved["user_id"]), str(saved["activity_key"])) != (str(user_id), str(activity_key)):
                    return ActivityBossRankClaimResult("operation_conflict")
                request = saved
            elif str(operation["payload"]) != payload:
                return ActivityBossRankClaimResult("operation_conflict")
            elif str(operation["status"]) in {"applied", "rejected"}:
                return self._decode(operation, replay=True)
            status = str(operation["status"])

        if status == "granted":
            self._finalize_legacy_state(request)
            with DatabaseUnitOfWork(self.game_database, immediate=True) as game:
                game.execute(
                    "UPDATE activity_boss_rank_claim_operations SET status='applied',updated_at=CURRENT_TIMESTAMP "
                    "WHERE operation_id=? AND status='granted'",
                    (operation_id,),
                )
        elif status == "started":
            if self._same_database:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as game:
                    changed = self._legacy_rank_still_claimable(game, request)
                    if changed:
                        return self._reject(game, operation_id, changed, int(request["rank"]))
                    if self._apply_started_operation(game, operation_id, request):
                        self._finalize_legacy_state(request, game)
            else:
                with DatabaseUnitOfWork(self.activity_database, immediate=True) as legacy:
                    changed = self._legacy_rank_still_claimable(legacy, request)
                    if changed:
                        with DatabaseUnitOfWork(self.game_database, immediate=True) as game:
                            result = self._reject(game, operation_id, changed, int(request["rank"]))
                        return result
                    with DatabaseUnitOfWork(self.game_database, immediate=True) as game:
                        applied = self._apply_started_operation(game, operation_id, request)
                    if applied:
                        self._finalize_legacy_state(request, legacy)
        with DatabaseUnitOfWork(self.game_database, read_only=True) as game:
            row = game.query_one(
                "SELECT result_json,result_status,status FROM activity_boss_rank_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
        if row is None:
            raise RuntimeError("activity boss rank operation disappeared")
        return self._decode(row, replay=not newly_prepared and row["status"] == "applied")

    def _finalize_legacy_state(self, request: Mapping[str, Any], uow: DatabaseUnitOfWork | None = None) -> None:
        if uow is None:
            with DatabaseUnitOfWork(self.activity_database, immediate=True) as legacy:
                self._finalize_legacy_state(request, legacy)
            return
        uow.execute(
            "INSERT OR IGNORE INTO activity_boss_rank_claim"
            "(activity_key,user_id,tier_key,create_time) VALUES(?,?,?,?)",
            (request["activity_key"], request["user_id"], request["tier_key"], self.clock.now().isoformat()),
        )

    def reconcile(self, operation_id: str) -> ActivityBossRankClaimResult:
        previous = self.get_result(operation_id)
        if previous is not None:
            return previous
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            self._assert_schema(uow)
            row = uow.query_one(
                "SELECT request_json FROM activity_boss_rank_claim_operations WHERE operation_id=?",
                (str(operation_id),),
            )
        if row is None:
            raise RuntimeError("activity boss rank operation is missing")
        request = json.loads(str(row["request_json"]))
        return self.claim(
            str(operation_id), request["user_id"], request["activity_key"], request["tiers"], request["max_goods_num"]
        )


__all__ = ["ActivityBossRankClaimRepository", "ActivityBossRankClaimResult"]
