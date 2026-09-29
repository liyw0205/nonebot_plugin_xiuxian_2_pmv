from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class ActivityBossMilestoneClaimResult:
    status: str
    names: tuple[str, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


class ActivityBossMilestoneClaimRepository:
    """Grant milestone rewards in game_db and retry the activity-state projection."""

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

    @staticmethod
    def _assert_schema(uow: DatabaseUnitOfWork) -> None:
        required = {
            "activity_boss_milestone_claim_operations": {
                "operation_id", "payload", "request_json", "result_json", "result_status", "status", "updated_at",
            },
            "activity_boss_milestone_claim_reservations": {
                "activity_key", "user_id", "milestone_key", "operation_id",
            },
        }
        for table, columns in required.items():
            actual = {str(row["name"]) for row in uow.query_all(f"PRAGMA table_info({table})")}
            if not columns.issubset(actual):
                raise RuntimeError(f"activity_reward.008 schema_missing: {table}")

    def assert_schema_ready(self) -> None:
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            self._assert_schema(uow)

    @staticmethod
    def _normalize(
        operation_id: str,
        user_id: str,
        activity_key: str,
        milestones: Any,
        max_goods_num: int,
    ) -> tuple[str, str, dict[str, Any]]:
        user_id, activity_key = str(user_id).strip(), str(activity_key).strip()
        max_goods_num = int(max_goods_num)
        rows = []
        seen = set()
        stone = 0
        items: dict[int, dict[str, Any]] = {}
        for raw in milestones:
            key = str(raw.get("key") or "").strip()
            if not key or key in seen:
                raise ValueError("unique activity boss milestones are required")
            seen.add(key)
            name = str(raw.get("name") or key)
            reward = str(raw.get("reward") or "")
            reward_items = []
            for raw_item in raw.get("reward_items") or ():
                item = dict(raw_item)
                quantity = int(item.get("quantity", 0))
                if quantity <= 0:
                    raise ValueError("activity boss reward quantities must be positive")
                if str(item.get("type")) == "stone":
                    stone += quantity
                    reward_items.append({"type": "stone", "quantity": quantity})
                    continue
                item_id = int(item.get("id", 0))
                item_name = str(item.get("name") or "")
                item_type = str(item.get("type") or "")
                if item_id <= 0 or not item_name or not item_type:
                    raise ValueError("complete activity boss item rewards are required")
                current = items.get(item_id)
                if current is not None and (current["name"], current["type"]) != (item_name, item_type):
                    raise ValueError("conflicting activity boss reward metadata")
                if current is None:
                    current = {"id": item_id, "name": item_name, "type": item_type, "quantity": 0}
                    items[item_id] = current
                current["quantity"] += quantity
                reward_items.append({
                    "id": item_id,
                    "name": item_name,
                    "type": item_type,
                    "quantity": quantity,
                })
            rows.append({
                "key": key,
                "name": name,
                "reward": reward,
                "reward_items": reward_items,
            })
        if not user_id or not activity_key or max_goods_num < 0:
            raise ValueError("valid activity boss milestone claim is required")
        legacy_rows = [[row["key"], row["name"], row["reward"]] for row in rows]
        payload = _json([user_id, activity_key, legacy_rows])
        request = {
            "user_id": user_id,
            "activity_key": activity_key,
            "milestones": rows,
            "names": [row["name"] for row in rows],
            "stone": stone,
            "items": [items[item_id] for item_id in sorted(items)],
            "max_goods_num": max_goods_num,
        }
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        return operation_id, payload, request

    @staticmethod
    def _decode(row: Mapping[str, Any], *, replay: bool = False) -> ActivityBossMilestoneClaimResult:
        data = json.loads(str(row["result_json"] or "{}"))
        status = str(row["result_status"])
        if replay and status == "applied":
            status = "duplicate"
        return ActivityBossMilestoneClaimResult(status, tuple(str(name) for name in data.get("names", ())))

    def get_result(
        self,
        operation_id: str,
        user_id: str | None = None,
        *,
        payload: str | None = None,
    ) -> ActivityBossMilestoneClaimResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            self._assert_schema(uow)
            row = uow.query_one(
                "SELECT payload,request_json,result_json,result_status,status "
                "FROM activity_boss_milestone_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
            if row is None:
                return None
            request = json.loads(str(row["request_json"]))
            if user_id is not None and str(request["user_id"]) != str(user_id):
                return ActivityBossMilestoneClaimResult("operation_conflict")
            if payload is not None and str(row["payload"]) != payload:
                return ActivityBossMilestoneClaimResult("operation_conflict")
            if row["status"] not in {"applied", "rejected"}:
                return None
            return self._decode(row, replay=True)

    def pending_status(self, operation_id: str, user_id: str | None = None) -> str | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            self._assert_schema(uow)
            row = uow.query_one(
                "SELECT request_json,status FROM activity_boss_milestone_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
            if row is None:
                return None
            request = json.loads(str(row["request_json"]))
            if user_id is not None and str(request["user_id"]) != str(user_id):
                return "operation_conflict"
            status = str(row["status"])
            return status if status in {"started", "granted"} else None

    def validate_claim(self, operation_id, user_id, activity_key, milestones, max_goods_num):
        resolved_id, payload, _ = self._normalize(
            operation_id, user_id, activity_key, milestones, max_goods_num
        )
        return resolved_id, payload

    def prepare_operation(self, uow, operation_id, user_id, activity_key, milestones, max_goods_num):
        operation_id, payload, request = self._normalize(
            operation_id, user_id, activity_key, milestones, max_goods_num
        )
        self._assert_schema(uow)
        operation = uow.query_one(
            "SELECT payload,request_json,result_json,result_status,status "
            "FROM activity_boss_milestone_claim_operations WHERE operation_id=?",
            (operation_id,),
        )
        status = "started"
        if operation is not None:
            status = str(operation["status"])
            if status in {"started", "granted"}:
                saved = json.loads(str(operation["request_json"]))
                if (str(saved["user_id"]), str(saved["activity_key"])) != (str(user_id), str(activity_key)):
                    return ActivityBossMilestoneClaimResult("operation_conflict"), False
                if str(operation["payload"]) != payload:
                    return ActivityBossMilestoneClaimResult("operation_conflict"), False
                request = saved
            elif str(operation["payload"]) != payload:
                return ActivityBossMilestoneClaimResult("operation_conflict"), False
            else:
                return self._decode(operation, replay=True), False
        else:
            uow.execute(
                "INSERT INTO activity_boss_milestone_claim_operations"
                "(operation_id,payload,request_json,result_json,result_status,status) "
                "VALUES(?,?,?,'{}','started','started')",
                (operation_id, payload, _json(request)),
            )
            selected, rejection = self._select_claimable(request)
            if rejection is not None:
                return self._reject(uow, operation_id, rejection), False
            # Keep the original payload for replay checks, but freeze only claimable rewards.
            request = selected
            uow.execute(
                "UPDATE activity_boss_milestone_claim_operations SET request_json=? WHERE operation_id=?",
                (_json(request), operation_id),
            )
        for milestone in request["milestones"]:
            identity = (request["activity_key"], request["user_id"], milestone["key"])
            uow.execute(
                "INSERT OR IGNORE INTO activity_boss_milestone_claim_reservations"
                "(activity_key,user_id,milestone_key,operation_id) VALUES(?,?,?,?)",
                (*identity, operation_id),
            )
            owner = uow.query_one(
                "SELECT operation_id FROM activity_boss_milestone_claim_reservations "
                "WHERE activity_key=? AND user_id=? AND milestone_key=?",
                identity,
            )
            if owner is None or str(owner["operation_id"]) != operation_id:
                return self._reject(uow, operation_id, "claim_in_progress"), False
        return None, operation is None

    def _legacy_claimable(self, request: Mapping[str, Any]) -> tuple[bool, bool]:
        with DatabaseUnitOfWork(self.activity_database, read_only=True) as uow:
            unlocked_rows = uow.query_all(
                "SELECT milestone_key FROM activity_boss_milestone WHERE activity_key=?",
                (request["activity_key"],),
            )
            unlocked = {str(row["milestone_key"]) for row in unlocked_rows}
            if not unlocked:
                return False, False
            claimed_rows = uow.query_all(
                "SELECT milestone_key FROM activity_boss_milestone_claim "
                "WHERE activity_key=? AND user_id=?",
                (request["activity_key"], request["user_id"]),
            )
            claimed = {str(row["milestone_key"]) for row in claimed_rows}
            if any(
                str(milestone["key"]) not in unlocked or str(milestone["key"]) in claimed
                for milestone in request["milestones"]
            ):
                return False, True
        return True, True

    def _select_claimable(
        self, request: Mapping[str, Any]
    ) -> tuple[dict[str, Any], str | None]:
        with DatabaseUnitOfWork(self.activity_database, read_only=True) as uow:
            unlocked_rows = uow.query_all(
                "SELECT milestone_key FROM activity_boss_milestone WHERE activity_key=?",
                (request["activity_key"],),
            )
            unlocked = {str(row["milestone_key"]) for row in unlocked_rows}
            if not unlocked:
                return dict(request), "not_unlocked"
            claimed_rows = uow.query_all(
                "SELECT milestone_key FROM activity_boss_milestone_claim "
                "WHERE activity_key=? AND user_id=?",
                (request["activity_key"], request["user_id"]),
            )
            claimed = {str(row["milestone_key"]) for row in claimed_rows}
        selected = [
            milestone for milestone in request["milestones"]
            if str(milestone["key"]) in unlocked and str(milestone["key"]) not in claimed
        ]
        if not selected:
            return dict(request), "already_claimed"
        _, _, selected_request = self._normalize(
            "selection",
            str(request["user_id"]),
            str(request["activity_key"]),
            selected,
            int(request["max_goods_num"]),
        )
        return selected_request, None

    @staticmethod
    def _reject(uow, operation_id: str, status: str) -> ActivityBossMilestoneClaimResult:
        changed = uow.execute(
            "UPDATE activity_boss_milestone_claim_operations SET status='rejected',result_status=?,"
            "result_json='{}',updated_at=CURRENT_TIMESTAMP WHERE operation_id=? AND status='started'",
            (status, operation_id),
        )
        if changed.rowcount != 1:
            raise RuntimeError("cannot reject a non-started activity boss milestone operation")
        uow.execute(
            "DELETE FROM activity_boss_milestone_claim_reservations WHERE operation_id=?",
            (operation_id,),
        )
        return ActivityBossMilestoneClaimResult(status)

    def claim(
        self,
        operation_id: str,
        user_id: str,
        activity_key: str,
        milestones: Any,
        max_goods_num: int,
        *,
        newly_prepared: bool = False,
    ) -> ActivityBossMilestoneClaimResult:
        operation_id = str(operation_id).strip()
        _, payload, request = self._normalize(
            operation_id, user_id, activity_key, milestones, max_goods_num
        )
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            self._assert_schema(uow)
            operation = uow.query_one(
                "SELECT payload,request_json,result_json,result_status,status "
                "FROM activity_boss_milestone_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
            if operation is None:
                raise RuntimeError("activity boss milestone operation must be prepared before claiming")
            if str(operation["status"]) in {"started", "granted"}:
                saved = json.loads(str(operation["request_json"]))
                if (str(saved["user_id"]), str(saved["activity_key"])) != (str(user_id), str(activity_key)):
                    return ActivityBossMilestoneClaimResult("operation_conflict")
                payload, request = str(operation["payload"]), saved
            elif str(operation["payload"]) != payload:
                return ActivityBossMilestoneClaimResult("operation_conflict")
            elif str(operation["status"]) in {"applied", "rejected"}:
                return self._decode(operation, replay=True)
            for milestone in request["milestones"]:
                identity = (request["activity_key"], request["user_id"], milestone["key"])
                owner = uow.query_one(
                    "SELECT operation_id FROM activity_boss_milestone_claim_reservations "
                    "WHERE activity_key=? AND user_id=? AND milestone_key=?",
                    identity,
                )
                if owner is None or str(owner["operation_id"]) != operation_id:
                    raise RuntimeError("activity boss milestone reservation is missing or changed")
            status = str(operation["status"])
        created = newly_prepared

        if status == "started":
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                operation = uow.query_one(
                    "SELECT status FROM activity_boss_milestone_claim_operations WHERE operation_id=?",
                    (operation_id,),
                )
                if operation is None:
                    raise RuntimeError("activity boss milestone operation is missing")
                if operation["status"] == "started":
                    claimable, unlocked = self._legacy_claimable(request)
                    if not claimable:
                        return self._reject(uow, operation_id, "state_changed" if unlocked else "not_unlocked")
                    if uow.query_one("SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (request["user_id"],)) is None:
                        return self._reject(uow, operation_id, "user_missing")
                    for item in request["items"]:
                        row = uow.query_one(
                            "SELECT COALESCE(goods_num,0) AS quantity FROM back WHERE user_id=? AND goods_id=?",
                            (request["user_id"], item["id"]),
                        )
                        if (int(row["quantity"]) if row else 0) + int(item["quantity"]) > int(request["max_goods_num"]):
                            return self._reject(uow, operation_id, "inventory_full")
                    now = self.clock.now().isoformat()
                    if int(request["stone"]):
                        changed = uow.execute(
                            "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+? WHERE user_id=?",
                            (int(request["stone"]), request["user_id"]),
                        )
                        if changed.rowcount != 1:
                            return self._reject(uow, operation_id, "user_missing")
                    columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
                    for item in request["items"]:
                        existing = uow.query_one(
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
                            uow.execute(
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
                            uow.execute(
                                f"UPDATE back SET {assignments} WHERE user_id=? AND goods_id=?",
                                tuple(values),
                            )
                    uow.execute(
                        "UPDATE activity_boss_milestone_claim_operations SET status='granted',result_status='applied',"
                        "result_json=?,updated_at=? WHERE operation_id=? AND status='started'",
                        (_json({"names": request["names"]}), now, operation_id),
                    )
                    status = "granted"
                else:
                    status = str(operation["status"])

        if status == "granted":
            self._finalize_legacy_state(request)
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                uow.execute(
                    "UPDATE activity_boss_milestone_claim_operations SET status='applied',updated_at=CURRENT_TIMESTAMP "
                    "WHERE operation_id=? AND status='granted'",
                    (operation_id,),
                )
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            final = uow.query_one(
                "SELECT result_json,result_status,status FROM activity_boss_milestone_claim_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
        if final is None:
            raise RuntimeError("activity boss milestone operation disappeared")
        return self._decode(final, replay=not created and final["status"] == "applied")

    def reconcile(self, operation_id: str) -> ActivityBossMilestoneClaimResult:
        previous = self.get_result(operation_id)
        if previous is not None:
            return previous
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            self._assert_schema(uow)
            row = uow.query_one(
                "SELECT request_json FROM activity_boss_milestone_claim_operations WHERE operation_id=?",
                (str(operation_id),),
            )
        if row is None:
            raise RuntimeError("activity boss milestone operation is missing")
        request = json.loads(str(row["request_json"]))
        return self.claim(
            str(operation_id),
            request["user_id"],
            request["activity_key"],
            request["milestones"],
            request["max_goods_num"],
        )

    def _finalize_legacy_state(self, request: Mapping[str, Any]) -> None:
        now = self.clock.now().isoformat()
        keys = [str(row["key"]) for row in request["milestones"]]
        with DatabaseUnitOfWork(self.activity_database, immediate=True) as uow:
            existing = uow.query_all(
                "SELECT milestone_key FROM activity_boss_milestone_claim "
                "WHERE activity_key=? AND user_id=? "
                f"AND milestone_key IN ({','.join('?' for _ in keys)})",
                (request["activity_key"], request["user_id"], *keys),
            )
            existing_keys = {str(row["milestone_key"]) for row in existing}
            if existing_keys and existing_keys != set(keys):
                raise RuntimeError("activity boss milestone projection is partially committed")
            for key in keys:
                if key in existing_keys:
                    continue
                uow.execute(
                    "INSERT INTO activity_boss_milestone_claim"
                    "(activity_key,user_id,milestone_key,create_time) VALUES(?,?,?,?)",
                    (request["activity_key"], request["user_id"], key, now),
                )


__all__ = ["ActivityBossMilestoneClaimRepository", "ActivityBossMilestoneClaimResult"]
