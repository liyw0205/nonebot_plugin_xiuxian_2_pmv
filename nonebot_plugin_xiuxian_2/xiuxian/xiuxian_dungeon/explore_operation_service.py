from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any

from ..xiuxian_utils import db_backend


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
                        {"battle_messages": [], "message": "副本状态已变化，请重新发起探索。"},
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
                        {"battle_messages": [], "message": "副本状态已变化，请重新发起探索。"},
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
                        {"battle_messages": [], "message": "队伍状态已变化，请重新发起探索。"},
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
                            {"battle_messages": [], "message": "队伍成员状态已变化，请重新发起探索。"},
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
                                {"battle_messages": [], "message": "背包状态已变化，请重新发起探索。"},
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
                        "UPDATE user_xiuxian SET hp=%s,mp=%s,stone=stone+%s,exp=exp+%s "
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


__all__ = ["DungeonExploreOperationResult", "DungeonExploreOperationService"]
