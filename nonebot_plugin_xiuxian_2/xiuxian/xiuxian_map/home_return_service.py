from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


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


__all__ = ["MapHomeReturnResult", "MapHomeReturnService"]
