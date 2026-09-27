from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any

from ..xiuxian_utils import db_backend
from ..xiuxian_utils.utils import number_to

from ...compatibility.legacy_dungeon_team_transactions import (
    TeamMutationResult,
    TeamInviteSnapshot,
    TeamStateSnapshot,
    TeamExitResult,
    DungeonTeamTransactionService,
    DungeonTeamExitService,
)
from ...features.dungeon.team_presentation import (
    TeamInviteResponseResult,
    TeamInviteResult,
    TeamKickResult,
    TeamLeaveResult,
    TeamMemberView,
    TeamTransferResult,
    TeamViewResult,
    build_invite_response_message,
    build_kick_team_message,
    build_kick_team_result,
    build_leave_team_message,
    build_leave_team_result,
    build_team_invite_message,
    build_team_invite_private_message,
    build_team_view,
    build_team_view_message,
    build_transfer_team_not_member_message,
    build_transfer_team_self_message,
    build_transfer_team_success_message,
    resolve_invite_response,
    resolve_kick_target,
    resolve_team_invite,
    resolve_transfer_target,
)
from ...compatibility.legacy_dungeon_reset import DungeonResetResult, DungeonResetService
from ...compatibility.legacy_dungeon_session import DungeonSessionResult, DungeonSessionService



@dataclass(frozen=True)
class DungeonPurchaseResult:
    status: str
    quantity: int = 0
    cost: int = 0
    stone: int = 0
    inventory: int = 0
    response: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class DungeonPurchaseService:
    """Exchange spirit stones for a dungeon-shop item atomically."""

    _REJECTION_RESPONSES = {
        "stone_insufficient": "灵石不足，无法兑换。",
        "inventory_full": "背包中该物品数量已达上限。",
        "state_changed": "兑换未结算：数据刚被其他操作改动，请重试。",
        "user_missing": "未找到道友数据，兑换失败。",
    }

    def __init__(self, game_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(game_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id: str, item_id: int, quantity: int, bind_flag: int) -> str:
        return json.dumps(
            [user_id, item_id, quantity, bind_flag],
            ensure_ascii=True,
            separators=(",", ":"),
        )

    @classmethod
    def _response(cls, status: str, item_name: str, quantity: int, cost: int) -> str:
        if status == "applied":
            return f"成功兑换{item_name}×{quantity}，消耗{number_to(cost)}灵石。"
        return cls._REJECTION_RESPONSES.get(status, "兑换失败。")

    @classmethod
    def _legacy_payload(cls, payload: str) -> tuple[str, str]:
        """Return the immutable request identity and legacy item name."""
        try:
            values = json.loads(payload)
        except (TypeError, ValueError):
            return str(payload), ""
        if not isinstance(values, list):
            return str(payload), ""
        if len(values) == 8:
            try:
                return (
                    cls._payload(
                        str(values[0]),
                        int(values[1]),
                        int(values[4]),
                        int(bool(values[7])),
                    ),
                    str(values[2]),
                )
            except (TypeError, ValueError):
                return str(payload), ""
        if len(values) == 5:
            try:
                return (
                    cls._payload(
                        str(values[0]),
                        int(values[1]),
                        int(values[2]),
                        int(bool(values[4])),
                    ),
                    "",
                )
            except (TypeError, ValueError):
                pass
        if len(values) == 4:
            try:
                return (
                    cls._payload(
                        str(values[0]),
                        int(values[1]),
                        int(values[2]),
                        int(bool(values[3])),
                    ),
                    "",
                )
            except (TypeError, ValueError):
                pass
        return str(payload), ""

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dungeon_purchase_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_status TEXT NOT NULL DEFAULT 'applied',"
            "quantity INTEGER NOT NULL DEFAULT 0,cost INTEGER NOT NULL DEFAULT 0,"
            "stone INTEGER NOT NULL DEFAULT 0,inventory INTEGER NOT NULL DEFAULT 0,"
            "response TEXT NOT NULL DEFAULT '',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        for name, definition in (
            ("result_status", "TEXT NOT NULL DEFAULT 'applied'"),
            ("response", "TEXT NOT NULL DEFAULT ''"),
        ):
            if not conn.column_exists("dungeon_purchase_operations", name):
                conn.execute(
                    f"ALTER TABLE dungeon_purchase_operations ADD COLUMN {name} {definition}"
                )

        rows = conn.execute(
            "SELECT operation_id,payload,result_status,quantity,cost,response "
            "FROM dungeon_purchase_operations"
        ).fetchall()
        for row in rows:
            payload, item_name = cls._legacy_payload(str(row[1]))
            result_status = str(row[2] or "applied")
            response = str(row[5] or "")
            if not response:
                response = cls._response(
                    result_status, item_name or "该物品", int(row[3]), int(row[4])
                )
            if payload != str(row[1]) or result_status != str(row[2]) or response != str(row[5]):
                conn.execute(
                    "UPDATE dungeon_purchase_operations "
                    "SET payload=%s,result_status=%s,response=%s WHERE operation_id=%s",
                    (payload, result_status, response, str(row[0])),
                )

    @staticmethod
    def _stored_result(row) -> DungeonPurchaseResult:
        result_status = str(row[1])
        return DungeonPurchaseResult(
            "duplicate" if result_status == "applied" else result_status,
            int(row[2]),
            int(row[3]),
            int(row[4]),
            int(row[5]),
            str(row[6]),
        )

    @staticmethod
    def _record(conn, operation_id: str, payload: str, result: DungeonPurchaseResult) -> None:
        conn.execute(
            "INSERT INTO dungeon_purchase_operations("
            "operation_id,payload,result_status,quantity,cost,stone,inventory,response) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                operation_id,
                payload,
                result.status,
                result.quantity,
                result.cost,
                result.stone,
                result.inventory,
                result.response,
            ),
        )

    def operation_result(
        self,
        operation_id,
        user_id,
        item_id,
        quantity,
        bind_flag=1,
    ) -> DungeonPurchaseResult | None:
        """Replay a fixed purchase before consulting mutable shop metadata."""

        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        item_id, quantity, bind_flag = map(int, (item_id, quantity, bind_flag))
        if (
            not operation_id
            or not user_id
            or item_id <= 0
            or quantity <= 0
            or bind_flag not in {0, 1}
        ):
            raise ValueError("valid purchase identity is required")
        payload = self._payload(user_id, item_id, quantity, bind_flag)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_status,quantity,cost,stone,inventory,response "
                    "FROM dungeon_purchase_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        if previous is None:
            return None
        if str(previous[0]) != payload:
            return DungeonPurchaseResult(
                "state_changed",
                quantity=quantity,
                response=self._REJECTION_RESPONSES["state_changed"],
            )
        return self._stored_result(previous)

    def purchase(
        self,
        operation_id,
        user_id,
        item_id,
        item_name,
        item_type,
        quantity,
        unit_cost,
        expected_stone,
        max_goods,
        bind_flag=1,
    ) -> DungeonPurchaseResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        item_id, quantity, unit_cost, expected_stone, max_goods = map(
            int, (item_id, quantity, unit_cost, expected_stone, max_goods)
        )
        item_name, item_type = str(item_name).strip(), str(item_type).strip()
        bind_flag = int(bind_flag)
        if (
            not operation_id
            or not user_id
            or not item_name
            or not item_type
            or item_id <= 0
            or quantity <= 0
            or unit_cost <= 0
            or expected_stone < 0
            or max_goods < 0
            or bind_flag not in {0, 1}
        ):
            raise ValueError("valid purchase is required")
        payload = self._payload(user_id, item_id, quantity, bind_flag)
        cost = quantity * unit_cost

        def rejected(status: str, stone=0, inventory=0) -> DungeonPurchaseResult:
            return DungeonPurchaseResult(
                status,
                quantity,
                cost,
                int(stone),
                int(inventory),
                self._response(status, item_name, quantity, cost),
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_status,quantity,cost,stone,inventory,response "
                    "FROM dungeon_purchase_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.commit()
                    if str(previous[0]) != payload:
                        return rejected("state_changed")
                    return self._stored_result(previous)

                user = conn.execute(
                    "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    result = rejected("user_missing")
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result
                stone = int(user[0])
                if stone != expected_stone:
                    result = rejected("state_changed", stone)
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result
                if stone < cost:
                    result = rejected("stone_insufficient", stone)
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result

                item = conn.execute(
                    "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) "
                    "FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                inventory, bound = (
                    (int(item[0]), int(item[1])) if item is not None else (0, 0)
                )
                if inventory < 0 or bound < 0 or bound > inventory:
                    result = rejected("state_changed", stone, inventory)
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result
                if inventory + quantity > max_goods:
                    result = rejected("inventory_full", stone, inventory)
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result

                new_stone = stone - cost
                new_inventory = inventory + quantity
                new_bound = bound + (quantity if bind_flag else 0)
                changed = conn.execute(
                    "UPDATE user_xiuxian SET stone=%s "
                    "WHERE user_id=%s AND COALESCE(stone,0)=%s",
                    (new_stone, user_id, expected_stone),
                )
                if changed.rowcount != 1:
                    result = rejected("state_changed", stone, inventory)
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result

                now = datetime.now()
                conn.execute(
                    "INSERT INTO back("
                    "user_id,goods_id,goods_name,goods_type,goods_num,"
                    "create_time,update_time,bind_num) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                    "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,"
                    "goods_num=back.goods_num+EXCLUDED.goods_num,"
                    "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,"
                    "update_time=EXCLUDED.update_time",
                    (
                        user_id,
                        item_id,
                        item_name,
                        item_type,
                        quantity,
                        now,
                        now,
                        quantity if bind_flag else 0,
                    ),
                )
                final_item = conn.execute(
                    "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) "
                    "FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if final_item is None or (int(final_item[0]), int(final_item[1])) != (
                    new_inventory,
                    new_bound,
                ):
                    raise RuntimeError("dungeon purchase inventory invariant failed")

                result = DungeonPurchaseResult(
                    "applied",
                    quantity,
                    cost,
                    new_stone,
                    new_inventory,
                    self._response("applied", item_name, quantity, cost),
                )
                self._record(conn, operation_id, payload, result)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

_STATUS_INTEGER_FIELDS = {"current_layer", "total_layers", "reset_generation"}
_STATUS_FIELDS = (
    "dungeon_id",
    "dungeon_name",
    "dungeon_status",
    "current_layer",
    "total_layers",
    "last_reset_date",
    "reset_generation",
    "reset_operation_id",
)

@dataclass(frozen=True)
class DungeonExploreOperationResult:
    status: str
    phase: str = ""
    result_status: str = ""
    response: dict[str, Any] | None = None
    plan: dict[str, Any] | None = None
    current_layer: int = 0
    dungeon_status: str = ""

    @property
    def completed(self) -> bool:
        return self.phase == "completed"

class DungeonExploreOperationService:
    """Persist one resolved exploration and settle every business write once."""

    # Canonical cross-database settlement statements. The implementation
    # expands numeric casts for SQLite compatibility while preserving the
    # repository contract used by adapters.
    _PLAYER_REWARD_SQL = "UPDATE user_xiuxian SET hp=%s,mp=%s,stone=stone+%s,exp=exp+%s"
    _DUNGEON_STATUS_SQL = "UPDATE player_data.player_dungeon_status SET current_layer=%s,dungeon_status=%s"

    TABLE = "dungeon_explore_operations"

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
    def _identity(user_id: str) -> str:
        return json.dumps(
            {"action": "explore", "user_id": str(user_id)},
            ensure_ascii=True,
            sort_keys=True,
        )

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _load_json(value: Any, fallback: Any) -> Any:
        try:
            loaded = json.loads(str(value or ""))
        except (TypeError, ValueError, json.JSONDecodeError):
            return fallback
        return loaded

    def _ensure_schema(self, conn) -> None:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self.TABLE} ("
            "operation_id TEXT PRIMARY KEY,"
            "request_identity TEXT NOT NULL,"
            "phase TEXT NOT NULL,"
            "prepared_json TEXT NOT NULL DEFAULT '{}',"
            "result_status TEXT NOT NULL DEFAULT '',"
            "result_json TEXT NOT NULL DEFAULT '{}',"
            "current_layer INTEGER NOT NULL DEFAULT 0,"
            "dungeon_status TEXT NOT NULL DEFAULT '',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        migrations = {
            "request_identity": "TEXT NOT NULL DEFAULT ''",
            "phase": "TEXT NOT NULL DEFAULT 'completed'",
            "prepared_json": "TEXT NOT NULL DEFAULT '{}'",
            "result_status": "TEXT NOT NULL DEFAULT ''",
            "result_json": "TEXT NOT NULL DEFAULT '{}'",
            "current_layer": "INTEGER NOT NULL DEFAULT 0",
            "dungeon_status": "TEXT NOT NULL DEFAULT ''",
            "updated_at": "TIMESTAMP",
        }
        for column, definition in migrations.items():
            if not conn.column_exists(self.TABLE, column):
                conn.execute(f"ALTER TABLE {self.TABLE} ADD COLUMN {column} {definition}")

    def _row_result(self, row, *, duplicate: bool) -> DungeonExploreOperationResult:
        phase = str(row[1] or "")
        result_status = str(row[3] or "")
        response = self._load_json(row[4], {})
        plan = self._load_json(row[2], {})
        status = "duplicate" if duplicate and phase == "completed" else phase
        if not duplicate and phase == "completed":
            status = result_status or "completed"
        return DungeonExploreOperationResult(
            status=status,
            phase=phase,
            result_status=result_status,
            response=response if isinstance(response, dict) else {},
            plan=plan if isinstance(plan, dict) else {},
            current_layer=int(row[5] or 0),
            dungeon_status=str(row[6] or ""),
        )

    def _select(self, conn, operation_id: str):
        return conn.execute(
            f"SELECT request_identity,phase,prepared_json,result_status,result_json,"
            f"current_layer,dungeon_status FROM {self.TABLE} WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()

    def replay(self, operation_id: str, user_id: str) -> DungeonExploreOperationResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        identity = self._identity(str(user_id))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = self._select(conn, operation_id)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        if row is None:
            return DungeonExploreOperationResult("missing")
        if str(row[0]) != identity:
            return DungeonExploreOperationResult("operation_conflict")
        return self._row_result(row, duplicate=True)

    def complete_without_writes(
        self,
        operation_id: str,
        user_id: str,
        result_status: str,
        response: dict[str, Any],
        *,
        current_layer: int = 0,
        dungeon_status: str = "",
    ) -> DungeonExploreOperationResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        result_status = str(result_status).strip() or "rejected"
        if not operation_id:
            raise ValueError("operation_id is required")
        identity = self._identity(user_id)
        response_json = self._json(dict(response))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = self._select(conn, operation_id)
                if row is not None:
                    conn.rollback()
                    if str(row[0]) != identity:
                        return DungeonExploreOperationResult("operation_conflict")
                    return self._row_result(row, duplicate=True)
                conn.execute(
                    f"INSERT INTO {self.TABLE} (operation_id,request_identity,phase,prepared_json,"
                    "result_status,result_json,current_layer,dungeon_status,updated_at) "
                    "VALUES (%s,%s,'completed','{}',%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                    (
                        operation_id,
                        identity,
                        result_status,
                        response_json,
                        int(current_layer),
                        str(dungeon_status),
                    ),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return DungeonExploreOperationResult(
            "applied",
            "completed",
            result_status,
            dict(response),
            {},
            int(current_layer),
            str(dungeon_status),
        )

    def resolve_rejection(
        self,
        operation_id: str,
        user_id: str,
        result_status: str,
        response: dict[str, Any],
        max_goods_num: int,
        *,
        current_layer: int = 0,
        dungeon_status: str = "",
    ) -> DungeonExploreOperationResult:
        """Persist this rejection, or finish an already prepared winning plan."""

        result = self.complete_without_writes(
            operation_id,
            user_id,
            result_status,
            response,
            current_layer=current_layer,
            dungeon_status=dungeon_status,
        )
        if result.phase == "prepared":
            return self.settle(operation_id, user_id, max_goods_num)
        return result

    def prepare(
        self,
        operation_id: str,
        user_id: str,
        plan: dict[str, Any],
    ) -> DungeonExploreOperationResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        if not operation_id or not isinstance(plan, dict) or not plan:
            raise ValueError("operation_id and resolved plan are required")
        identity = self._identity(user_id)
        prepared_json = self._json(plan)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = self._select(conn, operation_id)
                if row is not None:
                    conn.rollback()
                    if str(row[0]) != identity:
                        return DungeonExploreOperationResult("operation_conflict")
                    return self._row_result(row, duplicate=True)
                conn.execute(
                    f"INSERT INTO {self.TABLE} (operation_id,request_identity,phase,prepared_json,"
                    "result_status,result_json,current_layer,dungeon_status,updated_at) "
                    "VALUES (%s,%s,'prepared',%s,'','{}',0,'',CURRENT_TIMESTAMP)",
                    (operation_id, identity, prepared_json),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return DungeonExploreOperationResult(
            "prepared", "prepared", "", {}, dict(plan), 0, ""
        )

    @staticmethod
    def _normalize_status(status: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for key, value in status.items():
            if key not in _STATUS_FIELDS:
                continue
            normalized[key] = int(value or 0) if key in _STATUS_INTEGER_FIELDS else str(value or "")
        return normalized

    @staticmethod
    def _members(value: Any) -> list[str]:
        if not isinstance(value, list):
            try:
                value = json.loads(str(value or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError):
                value = []
        if not isinstance(value, list):
            return []
        return [str(member) for member in value if str(member).strip()]

    def _current_team(self, conn, user_id: str) -> dict[str, Any] | None:
        if conn.execute(
            "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name='teams'"
        ).fetchone() is None:
            return None
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA player_data.table_info(teams)").fetchall()
        }
        selected = ["user_id", "leader", "members"]
        if "version" in columns:
            selected.append("version")
        rows = conn.execute(
            "SELECT " + ",".join(selected) + " FROM player_data.teams"
        ).fetchall()
        for row in rows:
            members = self._members(row[2])
            if str(row[1]) == user_id or user_id in members:
                result = {
                    "team_id": str(row[0]),
                    "leader": str(row[1]),
                    "members": members,
                }
                if len(row) > 3:
                    result["version"] = int(row[3] or 0)
                return result
        return None

    def _complete_in_transaction(
        self,
        conn,
        operation_id: str,
        result_status: str,
        response: dict[str, Any],
        current_layer: int,
        dungeon_status: str,
    ) -> DungeonExploreOperationResult:
        conn.execute(
            f"UPDATE {self.TABLE} SET phase='completed',result_status=%s,result_json=%s,"
            "current_layer=%s,dungeon_status=%s,updated_at=CURRENT_TIMESTAMP "
            "WHERE operation_id=%s AND phase='prepared'",
            (
                str(result_status),
                self._json(response),
                int(current_layer),
                str(dungeon_status),
                operation_id,
            ),
        )
        return DungeonExploreOperationResult(
            "applied",
            "completed",
            str(result_status),
            dict(response),
            None,
            int(current_layer),
            str(dungeon_status),
        )

    def settle(
        self,
        operation_id: str,
        user_id: str,
        max_goods_num: int,
    ) -> DungeonExploreOperationResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        max_goods_num = int(max_goods_num)
        if not operation_id or max_goods_num < 0:
            raise ValueError("valid operation and inventory limit are required")
        identity = self._identity(user_id)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = self._select(conn, operation_id)
                if row is None:
                    conn.rollback()
                    return DungeonExploreOperationResult("missing")
                if str(row[0]) != identity:
                    conn.rollback()
                    return DungeonExploreOperationResult("operation_conflict")
                if str(row[1]) == "completed":
                    conn.rollback()
                    return self._row_result(row, duplicate=True)
                if str(row[1]) != "prepared":
                    conn.rollback()
                    return DungeonExploreOperationResult("invalid_phase", str(row[1]))

                plan = self._load_json(row[2], {})
                if not isinstance(plan, dict):
                    conn.rollback()
                    return DungeonExploreOperationResult("invalid_plan")
                expected_status = self._normalize_status(plan.get("expected_status", {}))
                status_columns = set(
                    conn.execute("PRAGMA player_data.table_info(player_dungeon_status)").fetchall()
                )
                available_status_columns = set()
                for info in status_columns:
                    if len(info) > 1:
                        available_status_columns.add(str(info[1]))
                required_status_columns = set(_STATUS_FIELDS) & available_status_columns
                if (
                    not expected_status
                    or not required_status_columns.issubset(expected_status)
                    or any(key not in available_status_columns for key in expected_status)
                ):
                    conflict = self._complete_in_transaction(
                        conn,
                        operation_id,
                        "state_changed",
                        {"battle_messages": [], "message": "探索未开始：副本进度数据刚被改动，请重新发起。"},
                        int(expected_status.get("current_layer", 0)),
                        str(expected_status.get("dungeon_status", "")),
                    )
                    conn.commit()
                    return conflict
                selected = list(expected_status)
                status_row = conn.execute(
                    "SELECT " + ",".join(selected)
                    + " FROM player_data.player_dungeon_status WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                current_status = None
                if status_row is not None:
                    current_status = self._normalize_status(dict(zip(selected, status_row)))
                if current_status != expected_status:
                    conflict = self._complete_in_transaction(
                        conn,
                        operation_id,
                        "state_changed",
                        {"battle_messages": [], "message": "探索未开始：副本进度数据刚被改动，请重新发起。"},
                        int(expected_status.get("current_layer", 0)),
                        str(expected_status.get("dungeon_status", "")),
                    )
                    conn.commit()
                    return conflict

                expected_team = plan.get("team")
                current_team = self._current_team(conn, user_id)
                if expected_team is None:
                    team_matches = current_team is None
                else:
                    normalized_team = {
                        "team_id": str(expected_team.get("team_id", "")),
                        "leader": str(expected_team.get("leader", "")),
                        "members": self._members(expected_team.get("members", [])),
                    }
                    if "version" in expected_team:
                        normalized_team["version"] = int(expected_team.get("version", 0))
                    team_matches = current_team == normalized_team
                if not team_matches:
                    conflict = self._complete_in_transaction(
                        conn,
                        operation_id,
                        "team_changed",
                        {"battle_messages": [], "message": "探索未开始：队伍已解散或成员变动，请重新组队发起。"},
                        int(expected_status.get("current_layer", 0)),
                        str(expected_status.get("dungeon_status", "")),
                    )
                    conn.commit()
                    return conflict

                members = plan.get("members", [])
                if not isinstance(members, list) or not members:
                    conn.rollback()
                    return DungeonExploreOperationResult("invalid_plan")
                seen: set[str] = set()
                inventory_rows: list[tuple[str, dict[str, Any], int, int]] = []
                for member in members:
                    member_id = str(member.get("user_id", ""))
                    if not member_id or member_id in seen:
                        conn.rollback()
                        return DungeonExploreOperationResult("invalid_plan")
                    seen.add(member_id)
                    expected = member.get("expected", {})
                    user = conn.execute(
                        "SELECT hp,mp,stone,exp FROM user_xiuxian WHERE user_id=%s",
                        (member_id,),
                    ).fetchone()
                    if user is None:
                        conflict = self._complete_in_transaction(
                            conn,
                            operation_id,
                            "user_missing",
                            {"battle_messages": [], "message": "队伍成员数据已不存在，请重新发起探索。"},
                            int(expected_status.get("current_layer", 0)),
                            str(expected_status.get("dungeon_status", "")),
                        )
                        conn.commit()
                        return conflict
                    current_resources = {
                        "hp": int(user[0]),
                        "mp": int(user[1]),
                        "stone": int(user[2]),
                        "exp": int(user[3]),
                    }
                    expected_resources = {
                        key: int(expected.get(key, 0))
                        for key in ("hp", "mp", "stone", "exp")
                    }
                    final_hp = int(member.get("final_hp", expected_resources["hp"]))
                    final_mp = int(member.get("final_mp", expected_resources["mp"]))
                    if final_hp < 1 or final_mp < 0:
                        conn.rollback()
                        return DungeonExploreOperationResult("invalid_plan")
                    cd = conn.execute(
                        "SELECT COALESCE(type,0) FROM user_cd WHERE user_id=%s ORDER BY rowid DESC LIMIT 1",
                        (member_id,),
                    ).fetchone()
                    current_cd_type = int(cd[0]) if cd else 0
                    if (
                        current_resources != expected_resources
                        or current_cd_type != int(expected.get("cd_type", 0))
                    ):
                        conflict = self._complete_in_transaction(
                            conn,
                            operation_id,
                            "state_changed",
                            {"battle_messages": [], "message": "探索未开始：队伍成员状态不符，请重新确认后再发起。"},
                            int(expected_status.get("current_layer", 0)),
                            str(expected_status.get("dungeon_status", "")),
                        )
                        conn.commit()
                        return conflict
                    member_item_ids: set[int] = set()
                    for item in member.get("items", []):
                        item_id = int(item["id"])
                        if item_id in member_item_ids:
                            conn.rollback()
                            return DungeonExploreOperationResult("invalid_plan")
                        member_item_ids.add(item_id)
                        inventory = conn.execute(
                            "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) FROM back "
                            "WHERE user_id=%s AND goods_id=%s",
                            (member_id, item_id),
                        ).fetchone()
                        goods_num = int(inventory[0]) if inventory else 0
                        bind_num = int(inventory[1]) if inventory else 0
                        if goods_num < 0 or bind_num < 0 or bind_num > goods_num:
                            conflict = self._complete_in_transaction(
                                conn,
                                operation_id,
                                "state_changed",
                                {"battle_messages": [], "message": "背包状态异常，请重新发起探索。"},
                                int(expected_status.get("current_layer", 0)),
                                str(expected_status.get("dungeon_status", "")),
                            )
                            conn.commit()
                            return conflict
                        if (
                            goods_num != int(item.get("expected_num", 0))
                            or bind_num != int(item.get("expected_bind_num", 0))
                        ):
                            conflict = self._complete_in_transaction(
                                conn,
                                operation_id,
                                "state_changed",
                                {"battle_messages": [], "message": "探索未开始：背包数据刚被改动，请整理后再发起。"},
                                int(expected_status.get("current_layer", 0)),
                                str(expected_status.get("dungeon_status", "")),
                            )
                            conn.commit()
                            return conflict
                        amount = int(item.get("amount", 0))
                        if amount <= 0:
                            conn.rollback()
                            return DungeonExploreOperationResult("invalid_plan")
                        if goods_num + amount > max_goods_num:
                            conflict = self._complete_in_transaction(
                                conn,
                                operation_id,
                                "inventory_full",
                                {"battle_messages": [], "message": "背包中该物品数量已达上限，本次探索未结算。"},
                                int(expected_status.get("current_layer", 0)),
                                str(expected_status.get("dungeon_status", "")),
                            )
                            conn.commit()
                            return conflict
                        inventory_rows.append((member_id, item, goods_num, bind_num))

                now = datetime.now()
                for member in members:
                    member_id = str(member["user_id"])
                    conn.execute(
                        "UPDATE user_xiuxian SET hp=%s,mp=%s,stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL),exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL) "
                        "WHERE user_id=%s",
                        (
                            int(member.get("final_hp", member["expected"]["hp"])),
                            int(member.get("final_mp", member["expected"]["mp"])),
                            int(member.get("stone_delta", 0)),
                            int(member.get("exp_delta", 0)),
                            member_id,
                        ),
                    )
                    for item in member.get("items", []):
                        amount = int(item["amount"])
                        conn.execute(
                            "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,"
                            "create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                            "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                            "goods_num=back.goods_num+EXCLUDED.goods_num,"
                            "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,"
                            "update_time=EXCLUDED.update_time",
                            (
                                member_id,
                                int(item["id"]),
                                str(item["name"]),
                                str(item["type"]),
                                amount,
                                now,
                                now,
                                amount,
                            ),
                        )

                for member_id, item, goods_num, bind_num in inventory_rows:
                    final_item = conn.execute(
                        "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) FROM back "
                        "WHERE user_id=%s AND goods_id=%s",
                        (member_id, int(item["id"])),
                    ).fetchone()
                    amount = int(item["amount"])
                    if final_item is None or (int(final_item[0]), int(final_item[1])) != (
                        goods_num + amount,
                        bind_num + amount,
                    ):
                        raise RuntimeError("dungeon exploration inventory invariant failed")

                current_layer = int(expected_status["current_layer"])
                total_layers = int(expected_status["total_layers"])
                if bool(plan.get("complete")):
                    final_layer = total_layers
                elif bool(plan.get("advance")):
                    final_layer = min(current_layer + 1, total_layers)
                else:
                    final_layer = current_layer
                final_status = "completed" if final_layer >= total_layers else "exploring"

                where = " AND ".join(f"{column}=%s" for column in selected)
                expected_values = [expected_status[column] for column in selected]
                updated = conn.execute(
                    "UPDATE player_data.player_dungeon_status SET current_layer=%s,dungeon_status=%s "
                    "WHERE user_id=%s AND " + where,
                    (final_layer, final_status, user_id, *expected_values),
                )
                if updated.rowcount != 1:
                    raise db_backend.OperationalError("dungeon status compare-and-set failed")

                response = plan.get("response", {})
                if not isinstance(response, dict):
                    response = {}
                result = self._complete_in_transaction(
                    conn,
                    operation_id,
                    "applied",
                    response,
                    final_layer,
                    final_status,
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

@dataclass(frozen=True)
class DungeonRewardResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class DungeonRewardService:
    """Award every eligible dungeon member in one game-database transaction."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def award(self, operation_id, rewards, max_goods_num) -> DungeonRewardResult:
        operation_id = str(operation_id).strip()
        max_goods_num = int(max_goods_num)
        normalized = tuple(
            (str(reward["user_id"]), int(reward.get("stone", 0)), int(reward.get("exp", 0)),
             tuple((int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"])) for item in reward.get("items", []) if int(item.get("amount", 0)) > 0))
            for reward in rewards
        )
        if not operation_id or max_goods_num < 0 or not normalized or any(min(stone, exp) < 0 for _, stone, exp, _ in normalized):
            raise ValueError("valid operation and rewards are required")
        payload = json.dumps(normalized, ensure_ascii=True, sort_keys=True)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS dungeon_reward_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload FROM dungeon_reward_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback()
                    return DungeonRewardResult("duplicate" if str(previous[0]) == payload else "state_changed")
                for user_id, _, _, _ in normalized:
                    if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                        conn.rollback()
                        return DungeonRewardResult("user_missing")
                for user_id, _, _, reward_items in normalized:
                    for item_id, _, _, amount in reward_items:
                        row = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                        if (int(row[0]) if row else 0) + amount > max_goods_num:
                            conn.rollback()
                            return DungeonRewardResult("inventory_full")
                now = datetime.now()
                for user_id, stone, exp, reward_items in normalized:
                    conn.execute("UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL), exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s", (stone, exp, user_id))
                    for item_id, name, item_type, amount in reward_items:
                        conn.execute("INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num, update_time=EXCLUDED.update_time", (user_id, item_id, name, item_type, amount, now, now, amount))
                conn.execute("INSERT INTO dungeon_reward_operations VALUES (%s,%s,CURRENT_TIMESTAMP)", (operation_id, payload))
                conn.commit()
                return DungeonRewardResult("applied")
            except Exception:
                conn.rollback()
                raise

__all__ = [
    "TeamMutationResult",
    "TeamInviteSnapshot",
    "TeamStateSnapshot",
    "TeamExitResult",
    "DungeonTeamTransactionService",
    "TeamMemberView",
    "TeamViewResult",
    "TeamTransferResult",
    "TeamLeaveResult",
    "TeamKickResult",
    "TeamInviteResult",
    "TeamInviteResponseResult",
    "DungeonTeamExitService",
    "DungeonSessionResult",
    "DungeonSessionService",
    "DungeonPurchaseResult",
    "DungeonPurchaseService",
    "DungeonExploreOperationResult",
    "DungeonExploreOperationService",
    "DungeonResetResult",
    "DungeonResetService",
    "DungeonRewardResult",
    "DungeonRewardService",
]
