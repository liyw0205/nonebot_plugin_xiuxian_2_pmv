from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from datetime import datetime
from ..xiuxian_utils import db_backend
from datetime import datetime, timedelta
from .explore_schema import ensure_explore_status_schema, snapshot_value_matches

@dataclass(frozen=True)
class SeedPurchaseResult:
    status: str
    quantity: int
    cost: int
    stone: int
    inventory: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class SeedPurchaseService:
    """Purchase map seeds without splitting stone and inventory writes."""

    def __init__(self, game_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._lock = lock or RLock()

    def purchase(self, operation_id, user_id, item_id, item_name, quantity, unit_cost, expected_stone, max_goods_num) -> SeedPurchaseResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        item_id, quantity, unit_cost, expected_stone, max_goods_num = map(int, (item_id, quantity, unit_cost, expected_stone, max_goods_num))
        item_name = str(item_name)
        if not operation_id or quantity <= 0 or min(item_id, unit_cost, expected_stone, max_goods_num) < 0:
            raise ValueError("valid operation, seed and purchase state are required")
        payload = json.dumps([user_id, item_id, item_name, quantity, unit_cost, expected_stone, max_goods_num], ensure_ascii=True, sort_keys=True)

        def result(status, stone=expected_stone, inventory=0):
            succeeded = status in {"applied", "duplicate"}
            return SeedPurchaseResult(status, quantity if succeeded else 0, quantity * unit_cost if succeeded else 0, int(stone), int(inventory))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS map_seed_purchase_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, quantity INTEGER NOT NULL, "
                    "cost INTEGER NOT NULL, stone INTEGER NOT NULL, inventory INTEGER NOT NULL, "
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,quantity,cost,stone,inventory FROM map_seed_purchase_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return result("state_changed")
                    return SeedPurchaseResult("duplicate", *(int(value) for value in old[1:]))
                user = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                stone = int(user[0] or 0)
                if stone != expected_stone:
                    conn.rollback()
                    return result("state_changed")
                cost = quantity * unit_cost
                if stone < cost:
                    conn.rollback()
                    return result("stone_insufficient")
                item = conn.execute(
                    "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                inventory = int(item[0]) if item else 0
                if inventory + quantity > max_goods_num:
                    conn.rollback()
                    return result("inventory_full")
                stone -= cost
                inventory += quantity
                now = datetime.now()
                conn.execute("UPDATE user_xiuxian SET stone=%s WHERE user_id=%s", (stone, user_id))
                conn.execute(
                    "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=EXCLUDED.goods_name, "
                    "goods_type=EXCLUDED.goods_type, goods_num=back.goods_num+EXCLUDED.goods_num, "
                    "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num, update_time=EXCLUDED.update_time",
                    (user_id, item_id, item_name, "特殊物品", quantity, now, now, quantity),
                )
                conn.execute(
                    "INSERT INTO map_seed_purchase_operations (operation_id,payload,quantity,cost,stone,inventory) VALUES (%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, quantity, cost, stone, inventory),
                )
                conn.commit()
                return SeedPurchaseResult("applied", quantity, cost, stone, inventory)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class MapDongfuBuildResult:
    status: str
    stone: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MapDongfuBuildService:
    """Atomically spend stone and bind a dongfu to the current map node."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def build(self, operation_id, user_id, expected_stone, cost, expected_position, dongfu) -> MapDongfuBuildResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_stone, cost = map(int, (expected_stone, cost))
        position = {key: str(value) for key, value in dict(expected_position).items()}
        dongfu = {key: str(value) for key, value in dict(dongfu).items()}
        if (
            not operation_id
            or min(expected_stone, cost) < 0
            or not {"realm", "heaven", "node_id"}.issubset(position)
            or dongfu.get("built") != "1"
        ):
            raise ValueError("valid operation, position and dongfu are required")
        payload = json.dumps([user_id, expected_stone, cost, position, dongfu], ensure_ascii=True, sort_keys=True)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS map_dongfu_build_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,stone FROM map_dongfu_build_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return MapDongfuBuildResult("state_changed", expected_stone)
                    return MapDongfuBuildResult("duplicate", int(old[1]))

                user = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback()
                    return MapDongfuBuildResult("user_missing", expected_stone)
                stone = int(user[0] or 0)
                if stone != expected_stone:
                    conn.rollback()
                    return MapDongfuBuildResult("state_changed", stone)
                if stone < cost:
                    conn.rollback()
                    return MapDongfuBuildResult("stone_insufficient", stone)

                position_columns = self._columns(conn, "map_status")
                if not set(position).issubset(position_columns):
                    conn.rollback()
                    return MapDongfuBuildResult("state_changed", stone)
                current_position = conn.execute(
                    "SELECT " + ",".join(f'"{key}"' for key in position) + " FROM player_data.map_status WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if current_position is None or tuple(str(value) for value in current_position) != tuple(position.values()):
                    conn.rollback()
                    return MapDongfuBuildResult("state_changed", stone)

                columns = self._columns(conn, "dongfu_status")
                for key in dongfu:
                    if key not in columns:
                        conn.execute(f'ALTER TABLE player_data.dongfu_status ADD COLUMN "{key}" TEXT DEFAULT NULL')
                existing = conn.execute("SELECT built FROM player_data.dongfu_status WHERE user_id=%s", (user_id,)).fetchone()
                if existing is not None and int(existing[0] or 0) == 1:
                    conn.rollback()
                    return MapDongfuBuildResult("already_built", stone)

                remaining = stone - cost
                conn.execute("UPDATE user_xiuxian SET stone=%s WHERE user_id=%s", (remaining, user_id))
                conn.execute(
                    "INSERT INTO player_data.dongfu_status (user_id," + ",".join(f'"{key}"' for key in dongfu) + ") "
                    "VALUES (" + ",".join(["%s"] * (len(dongfu) + 1)) + ") ON CONFLICT(user_id) DO UPDATE SET "
                    + ",".join(f'"{key}"=EXCLUDED."{key}"' for key in dongfu),
                    (user_id, *dongfu.values()),
                )
                conn.execute(
                    "INSERT INTO map_dongfu_build_operations (operation_id,payload,stone) VALUES (%s,%s,%s)",
                    (operation_id, payload, remaining),
                )
                conn.commit()
                return MapDongfuBuildResult("applied", remaining)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

    @staticmethod
    def _columns(conn, table: str) -> set[str]:
        return {str(row[1]) for row in conn.execute(f"PRAGMA player_data.table_info({table})").fetchall()}

@dataclass(frozen=True)
class MapHomeReturnResult:
    status: str
    realm: str = ""
    heaven: str = ""
    node_id: str = ""
    node_name: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MapHomeReturnService:
    """Atomically return a player to their persisted dongfu location."""

    def __init__(
        self, player_database: str | Path, lock: RLock | None = None
    ) -> None:
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS map_home_return_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_status TEXT NOT NULL,realm TEXT NOT NULL DEFAULT '',"
            "heaven TEXT NOT NULL DEFAULT '',node_id TEXT NOT NULL DEFAULT '',"
            "node_name TEXT NOT NULL DEFAULT '',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _columns(conn, table: str) -> set[str]:
        return {
            str(row[1])
            for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }

    @staticmethod
    def _text(value) -> str:
        return "" if value is None else str(value)

    @staticmethod
    def _visited(value) -> list[str]:
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (TypeError, ValueError):
                value = []
        return [str(item) for item in value] if isinstance(value, list) else []

    @classmethod
    def _stored_result(cls, row) -> MapHomeReturnResult:
        status = "duplicate" if str(row[1]) == "applied" else str(row[1])
        return MapHomeReturnResult(
            status,
            cls._text(row[2]),
            cls._text(row[3]),
            cls._text(row[4]),
            cls._text(row[5]),
        )

    @staticmethod
    def _record(
        conn,
        operation_id: str,
        payload: str,
        status: str,
        realm: str = "",
        heaven: str = "",
        node_id: str = "",
        node_name: str = "",
    ) -> MapHomeReturnResult:
        conn.execute(
            "INSERT INTO map_home_return_operations("
            "operation_id,payload,result_status,realm,heaven,node_id,node_name) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s)",
            (
                operation_id,
                payload,
                status,
                realm,
                heaven,
                node_id,
                node_name,
            ),
        )
        return MapHomeReturnResult(
            status, realm, heaven, node_id, node_name
        )

    def return_home(self, operation_id, user_id) -> MapHomeReturnResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation and user are required")
        payload = json.dumps([user_id], ensure_ascii=True, separators=(",", ":"))

        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_status,realm,heaven,node_id,node_name "
                    "FROM map_home_return_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return MapHomeReturnResult("operation_conflict")
                    return self._stored_result(previous)

                if not conn.table_exists("dongfu_status"):
                    result = self._record(
                        conn, operation_id, payload, "dongfu_missing"
                    )
                    conn.commit()
                    return result
                dongfu_columns = self._columns(conn, "dongfu_status")
                required_dongfu = {"built", "realm", "heaven", "node_id"}
                if not required_dongfu.issubset(dongfu_columns):
                    result = self._record(
                        conn, operation_id, payload, "dongfu_invalid"
                    )
                    conn.commit()
                    return result
                select_columns = ["built", "realm", "heaven", "node_id"]
                if "node_name" in dongfu_columns:
                    select_columns.append("node_name")
                dongfu = conn.execute(
                    "SELECT "
                    + ",".join(f'"{column}"' for column in select_columns)
                    + " FROM dongfu_status WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if dongfu is None or int(dongfu[0] or 0) != 1:
                    result = self._record(
                        conn, operation_id, payload, "dongfu_missing"
                    )
                    conn.commit()
                    return result

                realm, heaven, node_id = (
                    self._text(dongfu[1]),
                    self._text(dongfu[2]),
                    self._text(dongfu[3]),
                )
                node_name = (
                    self._text(dongfu[4])
                    if len(select_columns) == 5
                    else node_id
                ) or node_id
                if not all((realm, heaven, node_id)):
                    result = self._record(
                        conn, operation_id, payload, "dongfu_invalid"
                    )
                    conn.commit()
                    return result

                if not conn.table_exists("map_status"):
                    result = self._record(
                        conn, operation_id, payload, "position_missing"
                    )
                    conn.commit()
                    return result
                map_columns = self._columns(conn, "map_status")
                required_map = {"realm", "heaven", "node_id", "visited_nodes"}
                if not required_map.issubset(map_columns):
                    result = self._record(
                        conn, operation_id, payload, "position_missing"
                    )
                    conn.commit()
                    return result
                current = conn.execute(
                    "SELECT realm,heaven,node_id,visited_nodes "
                    "FROM map_status WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if current is None:
                    result = self._record(
                        conn, operation_id, payload, "position_missing"
                    )
                    conn.commit()
                    return result

                visited = self._visited(current[3])
                if node_id not in visited:
                    visited.append(node_id)
                conn.execute(
                    "UPDATE map_status SET realm=%s,heaven=%s,node_id=%s,"
                    "visited_nodes=%s WHERE user_id=%s",
                    (
                        realm,
                        heaven,
                        node_id,
                        json.dumps(visited, ensure_ascii=False),
                        user_id,
                    ),
                )
                result = self._record(
                    conn,
                    operation_id,
                    payload,
                    "applied",
                    realm,
                    heaven,
                    node_id,
                    node_name,
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class MapMovementResult:
    status: str
    stamina: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MapMovementSettlementService:
    """Atomically move a player, record the visit, and consume stamina."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def move(self, operation_id, user_id, expected_position, target_position, expected_stamina, cost):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected = self._position(expected_position)
        target = self._position(target_position)
        expected_stamina, cost = int(expected_stamina), int(cost)
        if not operation_id or expected_stamina < 0 or cost <= 0 or expected == target:
            raise ValueError("valid operation, distinct positions and positive cost are required")
        payload = json.dumps(
            [user_id, expected, target, expected_stamina, cost], ensure_ascii=True, sort_keys=True
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS map_movement_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stamina INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,stamina FROM map_movement_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return MapMovementResult("state_changed", expected_stamina)
                    return MapMovementResult("duplicate", int(old[1]))

                user = conn.execute(
                    "SELECT user_stamina FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return MapMovementResult("user_missing", expected_stamina)
                stamina = int(user[0] or 0)
                if stamina != expected_stamina:
                    conn.rollback()
                    return MapMovementResult("state_changed", stamina)
                if stamina < cost:
                    conn.rollback()
                    return MapMovementResult("stamina_insufficient", stamina)

                row = conn.execute(
                    "SELECT realm,heaven,node_id,visited_nodes FROM player_data.map_status WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None or tuple(str(value) for value in row[:3]) != tuple(expected.values()):
                    conn.rollback()
                    return MapMovementResult("state_changed", stamina)
                visited = self._visited(row[3])
                if target["node_id"] not in visited:
                    visited.append(target["node_id"])

                remaining = stamina - cost
                conn.execute(
                    "UPDATE player_data.map_status SET realm=%s,heaven=%s,node_id=%s,visited_nodes=%s "
                    "WHERE user_id=%s",
                    (*target.values(), json.dumps(visited, ensure_ascii=False), user_id),
                )
                conn.execute(
                    "UPDATE user_xiuxian SET user_stamina=%s WHERE user_id=%s", (remaining, user_id)
                )
                conn.execute(
                    "INSERT INTO map_movement_operations (operation_id,payload,stamina) VALUES (%s,%s,%s)",
                    (operation_id, payload, remaining),
                )
                conn.commit()
                return MapMovementResult("applied", remaining)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

    @staticmethod
    def _position(value):
        value = dict(value)
        if not {"realm", "heaven", "node_id"}.issubset(value):
            raise ValueError("position requires realm, heaven and node_id")
        return {key: str(value[key]) for key in ("realm", "heaven", "node_id")}

    @staticmethod
    def _visited(value):
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                value = []
        return [str(item) for item in value] if isinstance(value, list) else []

@dataclass(frozen=True)
class MapInteractiveActionResult:
    status: str
    stamina: int = 0
    action: dict | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MapInteractiveActionService:
    """Persist the start and terminal states of timed map resource actions."""

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
    def _canonical(value) -> str:
        return json.dumps(
            value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )

    @staticmethod
    def _parse(value: str) -> dict:
        try:
            parsed = json.loads(str(value))
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def _datetime(value) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    @staticmethod
    def _ensure_start_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS map_interactive_start_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_status TEXT NOT NULL,stamina INTEGER NOT NULL DEFAULT 0,"
            "action_json TEXT NOT NULL DEFAULT '{}',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _ensure_player_schema(conn, *, attached: bool) -> None:
        prefix = "player_data." if attached else ""
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {prefix}map_interactive_actions("
            "user_id TEXT PRIMARY KEY,action_id TEXT NOT NULL UNIQUE,"
            "action_type TEXT NOT NULL,status TEXT NOT NULL,"
            "state_json TEXT NOT NULL,settlement_json TEXT NOT NULL DEFAULT '',"
            "ready_at TEXT NOT NULL,expires_at TEXT NOT NULL,"
            "cooldown_seconds INTEGER NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {prefix}map_interactive_terminal_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_status TEXT NOT NULL,action_json TEXT NOT NULL DEFAULT '{}',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {prefix}map_cooldown("
            "user_id TEXT PRIMARY KEY,gather_cd_until TEXT DEFAULT NULL)"
        )
        pragma = (
            "PRAGMA player_data.table_info(map_cooldown)"
            if attached
            else "PRAGMA table_info(map_cooldown)"
        )
        columns = {str(row[1]) for row in conn.execute(pragma).fetchall()}
        if "gather_cd_until" not in columns:
            conn.execute(
                f'ALTER TABLE {prefix}map_cooldown '
                'ADD COLUMN "gather_cd_until" TEXT DEFAULT NULL'
            )

    @staticmethod
    def _columns(conn, table: str, *, attached: bool) -> set[str]:
        pragma = (
            f"PRAGMA player_data.table_info({table})"
            if attached
            else f"PRAGMA table_info({table})"
        )
        return {str(row[1]) for row in conn.execute(pragma).fetchall()}

    @classmethod
    def _start_result(cls, row) -> MapInteractiveActionResult:
        status = "duplicate" if str(row[1]) == "applied" else str(row[1])
        return MapInteractiveActionResult(
            status, int(row[2] or 0), cls._parse(str(row[3]))
        )

    @staticmethod
    def _record_start(
        conn,
        operation_id: str,
        payload: str,
        status: str,
        stamina: int,
        action: dict | None = None,
    ) -> MapInteractiveActionResult:
        action = dict(action or {})
        conn.execute(
            "INSERT INTO map_interactive_start_operations("
            "operation_id,payload,result_status,stamina,action_json) "
            "VALUES(%s,%s,%s,%s,%s)",
            (
                operation_id,
                payload,
                status,
                stamina,
                MapInteractiveActionService._canonical(action),
            ),
        )
        return MapInteractiveActionResult(status, stamina, action)

    def replay_start(
        self, operation_id, user_id, action_type
    ) -> MapInteractiveActionResult | None:
        operation_id = str(operation_id).strip()
        identity = [str(user_id).strip(), str(action_type).strip()]
        if not operation_id or not all(identity):
            raise ValueError("operation, user and action are required")
        payload = self._canonical(identity)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("map_interactive_start_operations"):
                return None
            row = conn.execute(
                "SELECT payload,result_status,stamina,action_json "
                "FROM map_interactive_start_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
        if row is None:
            return None
        if str(row[0]) != payload:
            return MapInteractiveActionResult("operation_conflict")
        return self._start_result(row)

    def start(
        self,
        operation_id,
        user_id,
        action_type,
        expected_stamina,
        stamina_cost,
        expected_position,
        expected_daily,
        daily_limit,
        expected_cooldown,
        action,
    ) -> MapInteractiveActionResult:
        operation_id = str(operation_id).strip()
        user_id, action_type = str(user_id).strip(), str(action_type).strip()
        expected_stamina, stamina_cost, daily_limit = map(
            int, (expected_stamina, stamina_cost, daily_limit)
        )
        position = {
            key: str(value) for key, value in dict(expected_position).items()
        }
        daily = {key: str(value) for key, value in dict(expected_daily).items()}
        expected_cooldown = (
            "" if expected_cooldown is None else str(expected_cooldown)
        )
        action = dict(action)
        identity_payload = self._canonical([user_id, action_type])
        required_action = {
            "action_id",
            "action",
            "start_ts",
            "ready_ts",
            "expire_ts",
            "cooldown_sec",
        }
        if (
            not operation_id
            or not user_id
            or not action_type
            or min(expected_stamina, stamina_cost, daily_limit) < 0
            or not {"realm", "heaven", "node_id"}.issubset(position)
            or not daily.get("date")
            or not required_action.issubset(action)
            or str(action["action_id"]) != operation_id
            or str(action["action"]) != action_type
        ):
            raise ValueError("valid interactive action snapshots are required")
        action_json = self._canonical(action)
        started_at = str(action["start_ts"])

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute(
                    "ATTACH DATABASE %s AS player_data",
                    (str(self._player_database),),
                )
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_start_schema(conn)
                self._ensure_player_schema(conn, attached=True)
                previous = conn.execute(
                    "SELECT payload,result_status,stamina,action_json "
                    "FROM map_interactive_start_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != identity_payload:
                        return MapInteractiveActionResult("operation_conflict")
                    return self._start_result(previous)

                user = conn.execute(
                    "SELECT user_stamina FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "user_missing",
                        expected_stamina,
                    )
                    conn.commit()
                    return result
                stamina = int(user[0] or 0)
                if stamina != expected_stamina:
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "state_changed",
                        stamina,
                    )
                    conn.commit()
                    return result

                position_columns = self._columns(
                    conn, "map_status", attached=True
                )
                if not set(position).issubset(position_columns):
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "state_changed",
                        stamina,
                    )
                    conn.commit()
                    return result
                position_row = conn.execute(
                    "SELECT "
                    + ",".join(f'"{key}"' for key in position)
                    + " FROM player_data.map_status WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if position_row is None or tuple(
                    str(value) for value in position_row
                ) != tuple(position.values()):
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "state_changed",
                        stamina,
                    )
                    conn.commit()
                    return result

                daily_columns = self._columns(
                    conn, "map_daily_limit", attached=True
                )
                required_daily = {"date", "gather_count", "resource_total_count"}
                if not required_daily.issubset(daily_columns):
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "state_changed",
                        stamina,
                    )
                    conn.commit()
                    return result
                daily_row = conn.execute(
                    "SELECT date,gather_count,resource_total_count "
                    "FROM player_data.map_daily_limit WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                expected_daily_tuple = (
                    daily["date"],
                    daily.get("gather_count", "0"),
                    daily.get("resource_total_count", "0"),
                )
                if daily_row is None or tuple(
                    str(value) for value in daily_row
                ) != expected_daily_tuple:
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "state_changed",
                        stamina,
                    )
                    conn.commit()
                    return result
                if int(daily_row[1] or 0) >= daily_limit:
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "limit_reached",
                        stamina,
                    )
                    conn.commit()
                    return result

                cooldown_row = conn.execute(
                    "SELECT gather_cd_until FROM player_data.map_cooldown "
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                current_cooldown = (
                    ""
                    if cooldown_row is None or cooldown_row[0] is None
                    else str(cooldown_row[0])
                )
                if current_cooldown != expected_cooldown:
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "state_changed",
                        stamina,
                    )
                    conn.commit()
                    return result
                if current_cooldown and current_cooldown > started_at:
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "cooldown",
                        stamina,
                        {"cooldown_until": current_cooldown},
                    )
                    conn.commit()
                    return result

                active = conn.execute(
                    "SELECT action_id,state_json,expires_at,cooldown_seconds "
                    "FROM player_data.map_interactive_actions "
                    "WHERE user_id=%s AND status='active'",
                    (user_id,),
                ).fetchone()
                if active is not None:
                    expires_at = self._datetime(active[2])
                    start_time = self._datetime(started_at)
                    if (
                        expires_at is None
                        or start_time is None
                        or expires_at > start_time
                    ):
                        result = self._record_start(
                            conn,
                            operation_id,
                            identity_payload,
                            "already_running",
                            stamina,
                            self._parse(str(active[1])),
                        )
                        conn.commit()
                        return result
                    cooldown_until = (
                        start_time + timedelta(seconds=int(active[3] or 0))
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    conn.execute(
                        "UPDATE player_data.map_interactive_actions "
                        "SET status='expired',updated_at=%s WHERE user_id=%s",
                        (started_at, user_id),
                    )
                    conn.execute(
                        "INSERT INTO player_data.map_cooldown("
                        "user_id,gather_cd_until) VALUES(%s,%s) "
                        "ON CONFLICT(user_id) DO UPDATE SET "
                        "gather_cd_until=EXCLUDED.gather_cd_until",
                        (user_id, cooldown_until),
                    )
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "cooldown",
                        stamina,
                        {"cooldown_until": cooldown_until},
                    )
                    conn.commit()
                    return result

                if stamina < stamina_cost:
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "stamina_insufficient",
                        stamina,
                    )
                    conn.commit()
                    return result

                remaining = stamina - stamina_cost
                conn.execute(
                    "UPDATE user_xiuxian SET user_stamina=%s WHERE user_id=%s",
                    (remaining, user_id),
                )
                conn.execute(
                    "INSERT INTO player_data.map_interactive_actions("
                    "user_id,action_id,action_type,status,state_json,settlement_json,"
                    "ready_at,expires_at,cooldown_seconds,updated_at) "
                    "VALUES(%s,%s,%s,'active',%s,'',%s,%s,%s,%s) "
                    "ON CONFLICT(user_id) DO UPDATE SET "
                    "action_id=EXCLUDED.action_id,action_type=EXCLUDED.action_type,"
                    "status='active',state_json=EXCLUDED.state_json,"
                    "settlement_json='',ready_at=EXCLUDED.ready_at,"
                    "expires_at=EXCLUDED.expires_at,"
                    "cooldown_seconds=EXCLUDED.cooldown_seconds,"
                    "updated_at=EXCLUDED.updated_at",
                    (
                        user_id,
                        operation_id,
                        action_type,
                        action_json,
                        str(action["ready_ts"]),
                        str(action["expire_ts"]),
                        int(action["cooldown_sec"]),
                        started_at,
                    ),
                )
                result = self._record_start(
                    conn,
                    operation_id,
                    identity_payload,
                    "applied",
                    remaining,
                    action,
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

    def get_active(self, user_id) -> dict | None:
        user_id = str(user_id).strip()
        if not user_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            if not conn.table_exists("map_interactive_actions"):
                return None
            row = conn.execute(
                "SELECT action_id,state_json,settlement_json "
                "FROM map_interactive_actions "
                "WHERE user_id=%s AND status='active'",
                (user_id,),
            ).fetchone()
        if row is None:
            return None
        action = self._parse(str(row[1]))
        action["action_id"] = str(row[0])
        if row[2]:
            action["settlement"] = self._parse(str(row[2]))
        return action

    def save_settlement(
        self, user_id, action_id, settlement
    ) -> MapInteractiveActionResult:
        user_id, action_id = str(user_id).strip(), str(action_id).strip()
        if not user_id or not action_id or not isinstance(settlement, dict):
            raise ValueError("user, action and settlement are required")
        settlement_json = self._canonical(settlement)
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_player_schema(conn, attached=False)
                row = conn.execute(
                    "SELECT state_json,settlement_json FROM map_interactive_actions "
                    "WHERE user_id=%s AND action_id=%s AND status='active'",
                    (user_id, action_id),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return MapInteractiveActionResult("state_changed")
                action = self._parse(str(row[0]))
                action["action_id"] = action_id
                if row[1]:
                    action["settlement"] = self._parse(str(row[1]))
                    conn.rollback()
                    return MapInteractiveActionResult("duplicate", action=action)
                conn.execute(
                    "UPDATE map_interactive_actions SET settlement_json=%s,"
                    "updated_at=CURRENT_TIMESTAMP "
                    "WHERE user_id=%s AND action_id=%s AND status='active'",
                    (settlement_json, user_id, action_id),
                )
                conn.commit()
                action["settlement"] = settlement
                return MapInteractiveActionResult("applied", action=action)
            except Exception:
                conn.rollback()
                raise

    @classmethod
    def _terminal_result(cls, row) -> MapInteractiveActionResult:
        status = "duplicate" if str(row[1]) == "applied" else str(row[1])
        return MapInteractiveActionResult(status, action=cls._parse(str(row[2])))

    def finish_failure(
        self,
        operation_id,
        user_id,
        action_id,
        outcome,
        cooldown_until,
    ) -> MapInteractiveActionResult:
        operation_id = str(operation_id).strip()
        user_id, action_id = str(user_id).strip(), str(action_id).strip()
        outcome, cooldown_until = str(outcome).strip(), str(cooldown_until).strip()
        if (
            not operation_id
            or not user_id
            or not action_id
            or outcome not in {"expired", "failed", "invalid"}
            or not cooldown_until
        ):
            raise ValueError("valid terminal action is required")
        payload = self._canonical(
            [user_id, action_id, outcome, cooldown_until]
        )
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_player_schema(conn, attached=False)
                previous = conn.execute(
                    "SELECT payload,result_status,action_json "
                    "FROM map_interactive_terminal_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return MapInteractiveActionResult("operation_conflict")
                    return self._terminal_result(previous)

                row = conn.execute(
                    "SELECT state_json FROM map_interactive_actions "
                    "WHERE user_id=%s AND action_id=%s AND status='active'",
                    (user_id, action_id),
                ).fetchone()
                if row is None:
                    action = {}
                    result_status = "state_changed"
                else:
                    action = self._parse(str(row[0]))
                    action["action_id"] = action_id
                    conn.execute(
                        "UPDATE map_interactive_actions SET status=%s,"
                        "updated_at=CURRENT_TIMESTAMP "
                        "WHERE user_id=%s AND action_id=%s AND status='active'",
                        (outcome, user_id, action_id),
                    )
                    conn.execute(
                        "INSERT INTO map_cooldown(user_id,gather_cd_until) "
                        "VALUES(%s,%s) ON CONFLICT(user_id) DO UPDATE SET "
                        "gather_cd_until=EXCLUDED.gather_cd_until",
                        (user_id, cooldown_until),
                    )
                    result_status = "applied"
                conn.execute(
                    "INSERT INTO map_interactive_terminal_operations("
                    "operation_id,payload,result_status,action_json) "
                    "VALUES(%s,%s,%s,%s)",
                    (
                        operation_id,
                        payload,
                        result_status,
                        self._canonical(action),
                    ),
                )
                conn.commit()
                return MapInteractiveActionResult(
                    result_status, action=action
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class MapCombatLifecycleResult:
    status: str
    stamina: int = 0
    task: dict | None = None
    snapshot: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate", "pending"}

class MapCombatLifecycleService:
    """Persist node-combat cost, cooldown, and recoverable battle plans."""

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
    def _canonical(value) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _parse(value) -> dict:
        try:
            parsed = json.loads(str(value))
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def _identity(user_id: str) -> str:
        return json.dumps([user_id], ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def _ensure_start_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS map_combat_start_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_status TEXT NOT NULL,stamina INTEGER NOT NULL DEFAULT 0,"
            "task_json TEXT NOT NULL DEFAULT '{}',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _ensure_player_schema(conn, *, attached: bool) -> None:
        prefix = "player_data." if attached else ""
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {prefix}map_combat_settlement("
            "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL DEFAULT '')"
        )
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {prefix}map_cooldown("
            "user_id TEXT PRIMARY KEY,combat_cd_until TEXT DEFAULT NULL)"
        )
        pragma = (
            "PRAGMA player_data.table_info(map_cooldown)"
            if attached
            else "PRAGMA table_info(map_cooldown)"
        )
        cooldown_columns = {
            str(row[1]) for row in conn.execute(pragma).fetchall()
        }
        if "combat_cd_until" not in cooldown_columns:
            conn.execute(
                f'ALTER TABLE {prefix}map_cooldown '
                'ADD COLUMN "combat_cd_until" TEXT DEFAULT NULL'
            )

    @staticmethod
    def _columns(conn, table: str) -> set[str]:
        return {
            str(row[1])
            for row in conn.execute(
                f"PRAGMA player_data.table_info({table})"
            ).fetchall()
        }

    @classmethod
    def _start_result(cls, row) -> MapCombatLifecycleResult:
        status = "duplicate" if str(row[1]) == "applied" else str(row[1])
        task = cls._parse(row[3])
        return MapCombatLifecycleResult(
            status, int(row[2] or 0), task, cls._canonical(task) if task else ""
        )

    @classmethod
    def _record_start(
        cls,
        conn,
        operation_id: str,
        payload: str,
        status: str,
        stamina: int,
        task: dict | None = None,
    ) -> MapCombatLifecycleResult:
        task = dict(task or {})
        task_json = cls._canonical(task)
        conn.execute(
            "INSERT INTO map_combat_start_operations("
            "operation_id,payload,result_status,stamina,task_json) "
            "VALUES(%s,%s,%s,%s,%s)",
            (operation_id, payload, status, stamina, task_json),
        )
        return MapCombatLifecycleResult(
            status, stamina, task, task_json if task else ""
        )

    def replay_start(
        self, operation_id, user_id
    ) -> MapCombatLifecycleResult | None:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation and user are required")
        payload = self._identity(user_id)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("map_combat_start_operations"):
                return None
            row = conn.execute(
                "SELECT payload,result_status,stamina,task_json "
                "FROM map_combat_start_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
        if row is None:
            return None
        if str(row[0]) != payload:
            return MapCombatLifecycleResult("operation_conflict")
        return self._start_result(row)

    def get_pending(self, user_id) -> MapCombatLifecycleResult | None:
        user_id = str(user_id).strip()
        if not user_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            if not conn.table_exists("map_combat_settlement"):
                return None
            row = conn.execute(
                "SELECT snapshot FROM map_combat_settlement WHERE user_id=%s",
                (user_id,),
            ).fetchone()
        snapshot = "" if row is None or row[0] is None else str(row[0])
        if not snapshot:
            return None
        task = self._parse(snapshot)
        return MapCombatLifecycleResult("pending", task=task, snapshot=snapshot)

    def start(
        self,
        operation_id,
        user_id,
        expected_stamina,
        stamina_cost,
        expected_position,
        expected_daily,
        daily_limit,
        expected_cooldown,
        task,
    ) -> MapCombatLifecycleResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        expected_stamina, stamina_cost, daily_limit = map(
            int, (expected_stamina, stamina_cost, daily_limit)
        )
        position = {
            key: str(value) for key, value in dict(expected_position).items()
        }
        daily = {key: str(value) for key, value in dict(expected_daily).items()}
        expected_cooldown = (
            "" if expected_cooldown is None else str(expected_cooldown)
        )
        task = dict(task)
        required_task = {
            "task_id",
            "status",
            "started_at",
            "cooldown_until",
            "daily",
            "enemy",
            "node_name",
            "node_type",
        }
        if (
            not operation_id
            or not user_id
            or min(expected_stamina, stamina_cost, daily_limit) < 0
            or not {"realm", "heaven", "node_id"}.issubset(position)
            or not daily.get("date")
            or not required_task.issubset(task)
            or str(task["task_id"]) != operation_id
            or str(task["status"]) != "running"
            or dict(task["daily"]) != dict(expected_daily)
        ):
            raise ValueError("valid combat lifecycle snapshots are required")
        payload = self._identity(user_id)
        task_json = self._canonical(task)
        started_at = str(task["started_at"])

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute(
                    "ATTACH DATABASE %s AS player_data",
                    (str(self._player_database),),
                )
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_start_schema(conn)
                self._ensure_player_schema(conn, attached=True)
                previous = conn.execute(
                    "SELECT payload,result_status,stamina,task_json "
                    "FROM map_combat_start_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return MapCombatLifecycleResult("operation_conflict")
                    return self._start_result(previous)

                user = conn.execute(
                    "SELECT user_stamina FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    result = self._record_start(
                        conn, operation_id, payload, "user_missing", expected_stamina
                    )
                    conn.commit()
                    return result
                stamina = int(user[0] or 0)
                if stamina != expected_stamina:
                    result = self._record_start(
                        conn, operation_id, payload, "state_changed", stamina
                    )
                    conn.commit()
                    return result

                position_columns = self._columns(conn, "map_status")
                if not set(position).issubset(position_columns):
                    result = self._record_start(
                        conn, operation_id, payload, "state_changed", stamina
                    )
                    conn.commit()
                    return result
                position_row = conn.execute(
                    "SELECT "
                    + ",".join(f'"{key}"' for key in position)
                    + " FROM player_data.map_status WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if position_row is None or tuple(
                    str(value) for value in position_row
                ) != tuple(position.values()):
                    result = self._record_start(
                        conn, operation_id, payload, "state_changed", stamina
                    )
                    conn.commit()
                    return result

                daily_columns = self._columns(conn, "map_daily_limit")
                required_daily = {"date", "combat_count", "resource_total_count"}
                if not required_daily.issubset(daily_columns):
                    result = self._record_start(
                        conn, operation_id, payload, "state_changed", stamina
                    )
                    conn.commit()
                    return result
                daily_row = conn.execute(
                    "SELECT date,combat_count,resource_total_count "
                    "FROM player_data.map_daily_limit WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                expected_daily_tuple = (
                    daily["date"],
                    daily.get("combat_count", "0"),
                    daily.get("resource_total_count", "0"),
                )
                if daily_row is None or tuple(
                    str(value) for value in daily_row
                ) != expected_daily_tuple:
                    result = self._record_start(
                        conn, operation_id, payload, "state_changed", stamina
                    )
                    conn.commit()
                    return result
                if int(daily_row[1] or 0) >= daily_limit:
                    result = self._record_start(
                        conn, operation_id, payload, "limit_reached", stamina
                    )
                    conn.commit()
                    return result

                cooldown_row = conn.execute(
                    "SELECT combat_cd_until FROM player_data.map_cooldown "
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                current_cooldown = (
                    ""
                    if cooldown_row is None or cooldown_row[0] is None
                    else str(cooldown_row[0])
                )
                if current_cooldown != expected_cooldown:
                    result = self._record_start(
                        conn, operation_id, payload, "state_changed", stamina
                    )
                    conn.commit()
                    return result
                if current_cooldown and current_cooldown > started_at:
                    result = self._record_start(
                        conn,
                        operation_id,
                        payload,
                        "cooldown",
                        stamina,
                        {"cooldown_until": current_cooldown},
                    )
                    conn.commit()
                    return result

                pending = conn.execute(
                    "SELECT snapshot FROM player_data.map_combat_settlement "
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if pending is not None and str(pending[0] or ""):
                    pending_task = self._parse(pending[0])
                    result = self._record_start(
                        conn,
                        operation_id,
                        payload,
                        "already_running",
                        stamina,
                        pending_task,
                    )
                    conn.commit()
                    return result

                if stamina < stamina_cost:
                    result = self._record_start(
                        conn,
                        operation_id,
                        payload,
                        "stamina_insufficient",
                        stamina,
                    )
                    conn.commit()
                    return result

                remaining = stamina - stamina_cost
                conn.execute(
                    "UPDATE user_xiuxian SET user_stamina=%s WHERE user_id=%s",
                    (remaining, user_id),
                )
                conn.execute(
                    "INSERT INTO player_data.map_cooldown("
                    "user_id,combat_cd_until) VALUES(%s,%s) "
                    "ON CONFLICT(user_id) DO UPDATE SET "
                    "combat_cd_until=EXCLUDED.combat_cd_until",
                    (user_id, str(task["cooldown_until"])),
                )
                conn.execute(
                    "INSERT INTO player_data.map_combat_settlement(user_id,snapshot) "
                    "VALUES(%s,%s) ON CONFLICT(user_id) DO UPDATE SET "
                    "snapshot=EXCLUDED.snapshot",
                    (user_id, task_json),
                )
                result = self._record_start(
                    conn,
                    operation_id,
                    payload,
                    "applied",
                    remaining,
                    task,
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

    def save_plan(
        self, user_id, task_id, plan
    ) -> MapCombatLifecycleResult:
        user_id, task_id = str(user_id).strip(), str(task_id).strip()
        plan = dict(plan)
        if (
            not user_id
            or not task_id
            or str(plan.get("task_id", "")) != task_id
            or str(plan.get("status", "")) != "planned"
        ):
            raise ValueError("valid combat plan is required")
        plan_json = self._canonical(plan)
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_player_schema(conn, attached=False)
                row = conn.execute(
                    "SELECT snapshot FROM map_combat_settlement WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                current_json = "" if row is None or row[0] is None else str(row[0])
                current = self._parse(current_json)
                if str(current.get("task_id", "")) != task_id:
                    conn.rollback()
                    return MapCombatLifecycleResult("state_changed")
                if str(current.get("status", "")) == "planned":
                    conn.rollback()
                    return MapCombatLifecycleResult(
                        "duplicate", task=current, snapshot=current_json
                    )
                if str(current.get("status", "")) != "running":
                    conn.rollback()
                    return MapCombatLifecycleResult("state_changed")
                conn.execute(
                    "UPDATE map_combat_settlement SET snapshot=%s WHERE user_id=%s",
                    (plan_json, user_id),
                )
                conn.commit()
                return MapCombatLifecycleResult(
                    "applied", task=plan, snapshot=plan_json
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class MapCombatSettlementResult:
    status: str
    stone: int
    rewards: tuple[tuple[int, int], ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MapCombatSettlementService:
    """Commit a completed map combat, its counters and fixed rewards atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def settle(self, operation_id, user_id, expected_daily, snapshot, daily_limit, stone, items, max_goods_num):
        operation_id, user_id, snapshot = str(operation_id).strip(), str(user_id), str(snapshot)
        expected = {key: str(value) for key, value in dict(expected_daily).items()}
        daily_limit, stone, max_goods_num = map(int, (daily_limit, stone, max_goods_num))
        rewards = tuple(
            (int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"]))
            for item in items if int(item["amount"]) > 0
        )
        if not operation_id or not snapshot or min(daily_limit, stone, max_goods_num) < 0 or not expected.get("date"):
            raise ValueError("valid operation, daily state and combat snapshot are required")
        payload = json.dumps([user_id, expected, snapshot, daily_limit, stone, rewards, max_goods_num], ensure_ascii=True, sort_keys=True)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS map_combat_settlement_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER NOT NULL,"
                    "rewards TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,stone,rewards FROM map_combat_settlement_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return MapCombatSettlementResult("state_changed", 0, ())
                    return MapCombatSettlementResult("duplicate", int(old[1]), tuple(tuple(map(int, value)) for value in json.loads(str(old[2]))))
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return MapCombatSettlementResult("user_missing", 0, ())

                daily_columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(map_daily_limit)").fetchall()}
                snapshot_columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(map_combat_settlement)").fetchall()}
                if not {"date", "combat_count", "resource_total_count"}.issubset(daily_columns) or "snapshot" not in snapshot_columns:
                    conn.rollback()
                    return MapCombatSettlementResult("state_changed", 0, ())
                daily = conn.execute(
                    "SELECT date,combat_count,resource_total_count FROM player_data.map_daily_limit WHERE user_id=%s", (user_id,)
                ).fetchone()
                stored_snapshot = conn.execute(
                    "SELECT snapshot FROM player_data.map_combat_settlement WHERE user_id=%s", (user_id,)
                ).fetchone()
                if daily is None or tuple(str(value) for value in daily) != (
                    expected["date"], expected.get("combat_count", "0"), expected.get("resource_total_count", "0"),
                ) or stored_snapshot is None or str(stored_snapshot[0] or "") != snapshot:
                    conn.rollback()
                    return MapCombatSettlementResult("state_changed", 0, ())
                if int(daily[1] or 0) >= daily_limit:
                    conn.rollback()
                    return MapCombatSettlementResult("limit_reached", 0, ())

                totals: dict[int, int] = {}
                metadata: dict[int, tuple[str, str]] = {}
                for item_id, name, item_type, amount in rewards:
                    totals[item_id] = totals.get(item_id, 0) + amount
                    metadata[item_id] = (name, item_type)
                for item_id, amount in totals.items():
                    row = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                    if (int(row[0]) if row else 0) + amount > max_goods_num:
                        conn.rollback()
                        return MapCombatSettlementResult("inventory_full", 0, ())

                conn.execute(
                    "UPDATE player_data.map_daily_limit SET combat_count=%s,resource_total_count=%s WHERE user_id=%s",
                    (int(daily[1] or 0) + 1, int(daily[2] or 0) + 1, user_id),
                )
                conn.execute("UPDATE player_data.map_combat_settlement SET snapshot=%s WHERE user_id=%s", ("", user_id))
                if stone:
                    conn.execute("UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s", (stone, user_id))
                now = datetime.now()
                for item_id, amount in totals.items():
                    name, item_type = metadata[item_id]
                    conn.execute(
                        "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,update_time=EXCLUDED.update_time",
                        (user_id, item_id, name, item_type, amount, now, now, amount),
                    )
                compact = tuple(sorted(totals.items()))
                conn.execute(
                    "INSERT INTO map_combat_settlement_operations (operation_id,payload,stone,rewards) VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, stone, json.dumps(compact)),
                )
                conn.commit()
                return MapCombatSettlementResult("applied", stone, compact)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class MapDaoBattleResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MapDaoBattleSettlementService:
    """Atomically record both sides of a dao battle after position revalidation."""

    def __init__(self, player_database: str | Path, game_database: str | Path, lock: RLock | None = None) -> None:
        self._player_database = Path(player_database)
        self._game_database = Path(game_database)
        self._lock = lock or RLock()

    def settle(self, operation_id, challenger_id, target_id, expected_position, challenger_won):
        operation_id = str(operation_id).strip()
        challenger_id, target_id = str(challenger_id), str(target_id)
        position = self._position(expected_position)
        challenger_won = bool(challenger_won)
        if not operation_id or not challenger_id or challenger_id == target_id:
            raise ValueError("valid operation and distinct players are required")
        payload = json.dumps(
            [challenger_id, target_id, position, challenger_won], ensure_ascii=True, sort_keys=True
        )

        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS game_data", (str(self._game_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS map_dao_battle_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload FROM map_dao_battle_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    return MapDaoBattleResult("duplicate" if str(old[0]) == payload else "state_changed")

                placeholders = "%s,%s"
                users = conn.execute(
                    f"SELECT user_id FROM game_data.user_xiuxian WHERE user_id IN ({placeholders})",
                    (challenger_id, target_id),
                ).fetchall()
                if {str(row[0]) for row in users} != {challenger_id, target_id}:
                    conn.rollback()
                    return MapDaoBattleResult("user_missing")

                rows = conn.execute(
                    f"SELECT user_id,realm,heaven,node_id FROM map_status WHERE user_id IN ({placeholders})",
                    (challenger_id, target_id),
                ).fetchall()
                current = {str(row[0]): tuple(str(value) for value in row[1:]) for row in rows}
                expected = tuple(position.values())
                if current.get(challenger_id) != expected or current.get(target_id) != expected:
                    conn.rollback()
                    return MapDaoBattleResult("position_changed")

                conn.execute(
                    "CREATE TABLE IF NOT EXISTS dao_record ("
                    "user_id TEXT PRIMARY KEY,total INTEGER DEFAULT 0,win INTEGER DEFAULT 0,lose INTEGER DEFAULT 0)"
                )
                columns = set(conn.column_names("dao_record"))
                for column in ("total", "win", "lose"):
                    if column not in columns:
                        conn.execute(f'ALTER TABLE dao_record ADD COLUMN "{column}" INTEGER DEFAULT 0')

                self._increment(conn, challenger_id, challenger_won)
                self._increment(conn, target_id, not challenger_won)
                conn.execute(
                    "INSERT INTO map_dao_battle_operations (operation_id,payload) VALUES (%s,%s)",
                    (operation_id, payload),
                )
                conn.commit()
                return MapDaoBattleResult("applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE game_data")

    @staticmethod
    def _increment(conn, user_id: str, won: bool) -> None:
        conn.execute(
            "INSERT INTO dao_record (user_id,total,win,lose) VALUES (%s,1,%s,%s) "
            "ON CONFLICT(user_id) DO UPDATE SET total=COALESCE(dao_record.total,0)+1,"
            "win=COALESCE(dao_record.win,0)+EXCLUDED.win,"
            "lose=COALESCE(dao_record.lose,0)+EXCLUDED.lose",
            (user_id, int(won), int(not won)),
        )

    @staticmethod
    def _position(value):
        value = dict(value)
        if not {"realm", "heaven", "node_id"}.issubset(value):
            raise ValueError("position requires realm, heaven and node_id")
        return {key: str(value[key]) for key in ("realm", "heaven", "node_id")}

@dataclass(frozen=True)
class MapExploreStartResult:
    status: str
    stamina: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MapExploreStartService:
    """Atomically spend stamina and create a long-running exploration."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def start(
        self,
        operation_id,
        user_id,
        expected_stamina,
        stamina_cost,
        expected_position,
        expected_status,
        expected_daily,
        daily_limit,
        expected_cooldown,
        cooldown_until,
        new_status,
    ) -> MapExploreStartResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_stamina, stamina_cost, daily_limit = map(int, (expected_stamina, stamina_cost, daily_limit))
        position = {key: str(value) for key, value in dict(expected_position).items()}
        status = {key: str(value) for key, value in dict(expected_status).items()}
        daily = {key: str(value) for key, value in dict(expected_daily).items()}
        expected_cooldown = "" if expected_cooldown is None else str(expected_cooldown)
        cooldown_until = str(cooldown_until)
        new_status = {key: str(value) for key, value in dict(new_status).items()}
        if (
            not operation_id
            or min(expected_stamina, stamina_cost, daily_limit) < 0
            or not {"realm", "heaven", "node_id"}.issubset(position)
            or status.get("running", "0") != "0"
            or not daily.get("date")
            or new_status.get("running") != "1"
        ):
            raise ValueError("valid operation and exploration snapshots are required")
        payload = json.dumps(
            [
                user_id,
                expected_stamina,
                stamina_cost,
                position,
                status,
                daily,
                daily_limit,
                expected_cooldown,
                cooldown_until,
                new_status,
            ],
            ensure_ascii=True,
            sort_keys=True,
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                ensure_explore_status_schema(conn)
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS map_explore_start_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stamina INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,stamina FROM map_explore_start_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return MapExploreStartResult("state_changed", expected_stamina)
                    return MapExploreStartResult("duplicate", int(old[1]))

                user = conn.execute("SELECT user_stamina FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback()
                    return MapExploreStartResult("user_missing", expected_stamina)
                stamina = int(user[0] or 0)
                if stamina != expected_stamina:
                    conn.rollback()
                    return MapExploreStartResult("state_changed", stamina)
                if stamina < stamina_cost:
                    conn.rollback()
                    return MapExploreStartResult("stamina_insufficient", stamina)

                if not self._matches_row(conn, "map_status", user_id, position):
                    conn.rollback()
                    return MapExploreStartResult("state_changed", stamina)
                if not self._matches_row(conn, "map_explore_status", user_id, status):
                    running_row = conn.execute(
                        "SELECT running FROM player_data.map_explore_status WHERE user_id=%s", (user_id,)
                    ).fetchone()
                    conn.rollback()
                    if running_row is not None and str(running_row[0]) == "1":
                        return MapExploreStartResult("already_running", stamina)
                    return MapExploreStartResult("state_changed", stamina)
                if not self._matches_row(conn, "map_daily_limit", user_id, daily):
                    conn.rollback()
                    return MapExploreStartResult("state_changed", stamina)
                if int(daily.get("explore_count", 0)) >= daily_limit:
                    conn.rollback()
                    return MapExploreStartResult("limit_reached", stamina)

                cooldown_columns = self._columns(conn, "map_cooldown")
                if "explore_start_cd_until" not in cooldown_columns:
                    conn.execute('ALTER TABLE player_data.map_cooldown ADD COLUMN "explore_start_cd_until" TEXT DEFAULT NULL')
                cooldown_row = conn.execute(
                    'SELECT "explore_start_cd_until" FROM player_data.map_cooldown WHERE user_id=%s', (user_id,)
                ).fetchone()
                current_cooldown = "" if cooldown_row is None or cooldown_row[0] is None else str(cooldown_row[0])
                if current_cooldown != expected_cooldown:
                    conn.rollback()
                    return MapExploreStartResult("state_changed", stamina)

                explore_columns = self._columns(conn, "map_explore_status")
                if not set(new_status).issubset(explore_columns):
                    conn.rollback()
                    return MapExploreStartResult("state_changed", stamina)
                conn.execute("UPDATE user_xiuxian SET user_stamina=%s WHERE user_id=%s", (stamina - stamina_cost, user_id))
                conn.execute(
                    "UPDATE player_data.map_explore_status SET "
                    + ",".join(f'"{key}"=%s' for key in new_status)
                    + " WHERE user_id=%s",
                    (*new_status.values(), user_id),
                )
                conn.execute(
                    'INSERT INTO player_data.map_cooldown (user_id,"explore_start_cd_until") VALUES (%s,%s) '
                    'ON CONFLICT(user_id) DO UPDATE SET "explore_start_cd_until"=EXCLUDED."explore_start_cd_until"',
                    (user_id, cooldown_until),
                )
                remaining = stamina - stamina_cost
                conn.execute(
                    "INSERT INTO map_explore_start_operations (operation_id,payload,stamina) VALUES (%s,%s,%s)",
                    (operation_id, payload, remaining),
                )
                conn.commit()
                return MapExploreStartResult("applied", remaining)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

    @staticmethod
    def _columns(conn, table: str) -> set[str]:
        return {str(row[1]) for row in conn.execute(f"PRAGMA player_data.table_info({table})").fetchall()}

    @classmethod
    def _matches_row(cls, conn, table: str, user_id: str, expected: dict[str, str]) -> bool:
        columns = cls._columns(conn, table)
        if not expected or not set(expected).issubset(columns):
            return False
        row = conn.execute(
            "SELECT " + ",".join(f'"{key}"' for key in expected) + f" FROM player_data.{table} WHERE user_id=%s",
            (user_id,),
        ).fetchone()
        if row is None:
            return False
        return all(
            snapshot_value_matches(actual, wanted) if key == "settlement" else str(actual) == wanted
            for key, actual, wanted in zip(expected, row, expected.values())
        )

@dataclass(frozen=True)
class MapExploreSettlementResult:
    status: str
    stone: int
    rewards: tuple[tuple[int, int], ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MapExploreSettlementService:
    """Atomically clear a completed exploration and grant its fixed rewards."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def settle(self, operation_id, user_id, expected_state, expected_daily, daily_limit, stone, items, max_goods_num):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        state = {key: str(value) for key, value in dict(expected_state).items()}
        daily = {key: str(value) for key, value in dict(expected_daily).items()}
        daily_limit, stone, max_goods_num = map(int, (daily_limit, stone, max_goods_num))
        rewards = tuple(
            (int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"]))
            for item in items if int(item["amount"]) > 0
        )
        if not operation_id or min(daily_limit, stone, max_goods_num) < 0 or state.get("running") != "1" or not daily.get("date"):
            raise ValueError("valid operation, explore state and daily state are required")
        payload = json.dumps([user_id, state, daily, daily_limit, stone, rewards, max_goods_num], ensure_ascii=True, sort_keys=True)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                explore_columns = ensure_explore_status_schema(conn)
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS map_explore_settlement_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, stone INTEGER NOT NULL, "
                    "rewards TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,stone,rewards FROM map_explore_settlement_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return MapExploreSettlementResult("state_changed", 0, ())
                    return MapExploreSettlementResult("duplicate", int(old[1]), tuple(tuple(map(int, value)) for value in json.loads(str(old[2]))))
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return MapExploreSettlementResult("user_missing", 0, ())

                daily_columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(map_daily_limit)").fetchall()}
                if not set(state).issubset(explore_columns) or not {"date", "explore_count", "resource_total_count"}.issubset(daily_columns):
                    conn.rollback()
                    return MapExploreSettlementResult("state_changed", 0, ())
                status_row = conn.execute(
                    "SELECT " + ",".join(state) + " FROM player_data.map_explore_status WHERE user_id=%s", (user_id,)
                ).fetchone()
                if status_row is None or any(
                    not (snapshot_value_matches(actual, wanted) if key == "settlement" else str(actual) == wanted)
                    for key, actual, wanted in zip(state, status_row, state.values())
                ):
                    conn.rollback()
                    return MapExploreSettlementResult("state_changed", 0, ())
                daily_row = conn.execute(
                    "SELECT date,explore_count,resource_total_count FROM player_data.map_daily_limit WHERE user_id=%s", (user_id,)
                ).fetchone()
                if daily_row is None or tuple(str(value) for value in daily_row) != (
                    daily["date"], daily.get("explore_count", "0"), daily.get("resource_total_count", "0"),
                ):
                    conn.rollback()
                    return MapExploreSettlementResult("state_changed", 0, ())
                if int(daily_row[1] or 0) >= daily_limit:
                    conn.rollback()
                    return MapExploreSettlementResult("limit_reached", 0, ())

                totals: dict[int, int] = {}
                metadata: dict[int, tuple[str, str]] = {}
                for item_id, name, item_type, amount in rewards:
                    totals[item_id] = totals.get(item_id, 0) + amount
                    metadata[item_id] = (name, item_type)
                for item_id, amount in totals.items():
                    row = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                    if (int(row[0]) if row else 0) + amount > max_goods_num:
                        conn.rollback()
                        return MapExploreSettlementResult("inventory_full", 0, ())

                conn.execute(
                    "UPDATE player_data.map_daily_limit SET explore_count=%s,resource_total_count=%s WHERE user_id=%s",
                    (int(daily_row[1] or 0) + 1, int(daily_row[2] or 0) + 1, user_id),
                )
                clear_values = {
                    "running": 0, "node_type": "", "node_name": "", "start_time": "", "duration_min": 0,
                    "max_duration_min": 0, "interval_min": 0,
                }
                if "settlement" in explore_columns:
                    clear_values["settlement"] = ""
                clear_values = {key: value for key, value in clear_values.items() if key in explore_columns}
                conn.execute(
                    "UPDATE player_data.map_explore_status SET " + ",".join(f'\"{key}\"=%s' for key in clear_values) + " WHERE user_id=%s",
                    (*clear_values.values(), user_id),
                )
                if stone:
                    conn.execute("UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s", (stone, user_id))
                now = datetime.now()
                for item_id, amount in totals.items():
                    name, item_type = metadata[item_id]
                    conn.execute(
                        "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,"
                        "goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,update_time=EXCLUDED.update_time",
                        (user_id, item_id, name, item_type, amount, now, now, amount),
                    )
                compact_rewards = tuple(sorted(totals.items()))
                conn.execute(
                    "INSERT INTO map_explore_settlement_operations (operation_id,payload,stone,rewards) VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, stone, json.dumps(compact_rewards)),
                )
                conn.commit()
                return MapExploreSettlementResult("applied", stone, compact_rewards)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class MapMissionClaimResult:
    status: str
    stone: int
    rewards: tuple[tuple[int, int], ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MapMissionClaimService:
    """Atomically mark a completed map mission claimed and deliver its rewards."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def claim(self, operation_id, user_id, expected_mission, expected_daily, progress_key, stone, items, max_goods_num):
        operation_id, user_id, progress_key = str(operation_id).strip(), str(user_id), str(progress_key)
        mission = {key: str(value) for key, value in dict(expected_mission).items()}
        daily = {key: str(value) for key, value in dict(expected_daily).items()}
        stone, max_goods_num = map(int, (stone, max_goods_num))
        rewards = tuple(
            (int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"]))
            for item in items if int(item["amount"]) > 0
        )
        if not operation_id or min(stone, max_goods_num) < 0 or not mission.get("date") or not progress_key:
            raise ValueError("valid operation, mission, daily state and progress key are required")
        payload = json.dumps([user_id, mission, daily, progress_key, stone, rewards, max_goods_num], ensure_ascii=True, sort_keys=True)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS map_mission_claim_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, stone INTEGER NOT NULL, "
                    "rewards TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,stone,rewards FROM map_mission_claim_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return MapMissionClaimResult("state_changed", 0, ())
                    return MapMissionClaimResult("duplicate", int(old[1]), tuple(tuple(map(int, value)) for value in json.loads(str(old[2]))))
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return MapMissionClaimResult("user_missing", 0, ())

                mission_columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(map_mission)").fetchall()}
                daily_columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(map_daily_limit)").fetchall()}
                if not set(mission).issubset(mission_columns) or not {"date", progress_key}.issubset(daily_columns):
                    conn.rollback()
                    return MapMissionClaimResult("state_changed", 0, ())
                mission_row = conn.execute(
                    "SELECT " + ",".join(mission) + " FROM player_data.map_mission WHERE user_id=%s", (user_id,)
                ).fetchone()
                if mission_row is None or tuple(str(value) for value in mission_row) != tuple(mission.values()):
                    conn.rollback()
                    return MapMissionClaimResult("state_changed", 0, ())
                progress_row = conn.execute(
                    f'SELECT date,"{progress_key}" FROM player_data.map_daily_limit WHERE user_id=%s', (user_id,)
                ).fetchone()
                if progress_row is None or tuple(str(value) for value in progress_row) != (daily.get("date", ""), daily.get(progress_key, "0")):
                    conn.rollback()
                    return MapMissionClaimResult("state_changed", 0, ())
                if int(mission.get("claimed", "0")) != 0:
                    conn.rollback()
                    return MapMissionClaimResult("already_claimed", 0, ())
                if int(progress_row[1] or 0) < int(mission.get("target", "0")):
                    conn.rollback()
                    return MapMissionClaimResult("not_completed", 0, ())

                totals: dict[int, int] = {}
                metadata: dict[int, tuple[str, str]] = {}
                for item_id, name, item_type, amount in rewards:
                    totals[item_id] = totals.get(item_id, 0) + amount
                    metadata[item_id] = (name, item_type)
                for item_id, amount in totals.items():
                    row = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                    if (int(row[0]) if row else 0) + amount > max_goods_num:
                        conn.rollback()
                        return MapMissionClaimResult("inventory_full", 0, ())

                conn.execute("UPDATE player_data.map_mission SET claimed=%s WHERE user_id=%s", (1, user_id))
                if stone:
                    conn.execute("UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s", (stone, user_id))
                now = datetime.now()
                for item_id, amount in totals.items():
                    name, item_type = metadata[item_id]
                    conn.execute(
                        "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,"
                        "goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,update_time=EXCLUDED.update_time",
                        (user_id, item_id, name, item_type, amount, now, now, amount),
                    )
                compact_rewards = tuple(sorted(totals.items()))
                conn.execute(
                    "INSERT INTO map_mission_claim_operations (operation_id,payload,stone,rewards) VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, stone, json.dumps(compact_rewards)),
                )
                conn.commit()
                return MapMissionClaimResult("applied", stone, compact_rewards)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class MapResourceRewardResult:
    status: str
    stone: int
    rewards: tuple[tuple[int, int], ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MapResourceRewardService:
    """Commit one completed resource action with its daily counters and rewards."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def settle(
        self,
        operation_id,
        user_id,
        expected_daily,
        daily_limit,
        stone,
        items,
        max_goods_num,
        *,
        action_id=None,
        action_settlement=None,
        cooldown_until=None,
    ):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected = {key: str(value) for key, value in dict(expected_daily).items()}
        daily_limit, stone, max_goods_num = map(int, (daily_limit, stone, max_goods_num))
        rewards = tuple((int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"])) for item in items if int(item["amount"]) > 0)
        if not operation_id or min(daily_limit, stone, max_goods_num) < 0 or not expected.get("date"):
            raise ValueError("valid operation, daily state and rewards are required")
        lifecycle = None
        if any(value is not None for value in (action_id, action_settlement, cooldown_until)):
            if (
                not str(action_id or "").strip()
                or not isinstance(action_settlement, dict)
                or not str(cooldown_until or "").strip()
            ):
                raise ValueError("complete interactive action lifecycle is required")
            lifecycle = {
                "action_id": str(action_id).strip(),
                "settlement": json.dumps(
                    action_settlement,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "cooldown_until": str(cooldown_until).strip(),
            }
        payload_values = [user_id, expected, daily_limit, stone, rewards, max_goods_num]
        if lifecycle is not None:
            payload_values.append(lifecycle)
        payload = json.dumps(payload_values, ensure_ascii=True, sort_keys=True)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS map_resource_reward_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, stone INTEGER NOT NULL, "
                    "rewards TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,stone,rewards FROM map_resource_reward_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return MapResourceRewardResult("state_changed", 0, ())
                    return MapResourceRewardResult("duplicate", int(old[1]), tuple(tuple(map(int, value)) for value in json.loads(str(old[2]))))
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return MapResourceRewardResult("user_missing", 0, ())
                if lifecycle is not None:
                    action_columns = {
                        str(row[1])
                        for row in conn.execute(
                            "PRAGMA player_data.table_info(map_interactive_actions)"
                        ).fetchall()
                    }
                    required_action = {
                        "action_id", "status", "settlement_json"
                    }
                    if not required_action.issubset(action_columns):
                        conn.rollback()
                        return MapResourceRewardResult("state_changed", 0, ())
                    action_row = conn.execute(
                        "SELECT status,settlement_json "
                        "FROM player_data.map_interactive_actions "
                        "WHERE user_id=%s AND action_id=%s",
                        (user_id, lifecycle["action_id"]),
                    ).fetchone()
                    if (
                        action_row is None
                        or str(action_row[0]) != "active"
                        or str(action_row[1]) != lifecycle["settlement"]
                    ):
                        conn.rollback()
                        return MapResourceRewardResult("state_changed", 0, ())
                columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(map_daily_limit)").fetchall()}
                required = {"date", "gather_count", "resource_total_count"}
                if not required.issubset(columns):
                    conn.rollback()
                    return MapResourceRewardResult("state_changed", 0, ())
                daily = conn.execute(
                    "SELECT date,gather_count,resource_total_count FROM player_data.map_daily_limit WHERE user_id=%s", (user_id,)
                ).fetchone()
                if daily is None or tuple(str(value) for value in daily) != (
                    expected["date"], expected.get("gather_count", "0"), expected.get("resource_total_count", "0"),
                ):
                    conn.rollback()
                    return MapResourceRewardResult("state_changed", 0, ())
                if int(daily[1] or 0) >= daily_limit:
                    conn.rollback()
                    return MapResourceRewardResult("limit_reached", 0, ())
                totals: dict[int, int] = {}
                metadata: dict[int, tuple[str, str]] = {}
                for item_id, name, item_type, amount in rewards:
                    totals[item_id] = totals.get(item_id, 0) + amount
                    metadata[item_id] = (name, item_type)
                for item_id, amount in totals.items():
                    row = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                    if (int(row[0]) if row else 0) + amount > max_goods_num:
                        conn.rollback()
                        return MapResourceRewardResult("inventory_full", 0, ())
                conn.execute(
                    "UPDATE player_data.map_daily_limit SET gather_count=%s,resource_total_count=%s WHERE user_id=%s",
                    (int(daily[1] or 0) + 1, int(daily[2] or 0) + 1, user_id),
                )
                if stone:
                    conn.execute("UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s", (stone, user_id))
                now = datetime.now()
                for item_id, amount in totals.items():
                    name, item_type = metadata[item_id]
                    conn.execute(
                        "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,"
                        "goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,update_time=EXCLUDED.update_time",
                        (user_id, item_id, name, item_type, amount, now, now, amount),
                    )
                if lifecycle is not None:
                    conn.execute(
                        "UPDATE player_data.map_interactive_actions "
                        "SET status='completed',updated_at=%s "
                        "WHERE user_id=%s AND action_id=%s AND status='active'",
                        (now, user_id, lifecycle["action_id"]),
                    )
                    conn.execute(
                        "INSERT INTO player_data.map_cooldown("
                        "user_id,gather_cd_until) VALUES(%s,%s) "
                        "ON CONFLICT(user_id) DO UPDATE SET "
                        "gather_cd_until=EXCLUDED.gather_cd_until",
                        (user_id, lifecycle["cooldown_until"]),
                    )
                compact_rewards = tuple(sorted(totals.items()))
                conn.execute(
                    "INSERT INTO map_resource_reward_operations (operation_id,payload,stone,rewards) VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, stone, json.dumps(compact_rewards)),
                )
                conn.commit()
                return MapResourceRewardResult("applied", stone, compact_rewards)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

__all__ = [
    "SeedPurchaseResult",
    "SeedPurchaseService",
    "MapDongfuBuildResult",
    "MapDongfuBuildService",
    "MapHomeReturnResult",
    "MapHomeReturnService",
    "MapMovementResult",
    "MapMovementSettlementService",
    "MapInteractiveActionResult",
    "MapInteractiveActionService",
    "MapCombatLifecycleResult",
    "MapCombatLifecycleService",
    "MapCombatSettlementResult",
    "MapCombatSettlementService",
    "MapDaoBattleResult",
    "MapDaoBattleSettlementService",
    "MapExploreStartResult",
    "MapExploreStartService",
    "MapExploreSettlementResult",
    "MapExploreSettlementService",
    "MapMissionClaimResult",
    "MapMissionClaimService",
    "MapResourceRewardResult",
    "MapResourceRewardService",
]
