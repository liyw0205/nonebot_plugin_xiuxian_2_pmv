from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from ...infrastructure.database import DatabaseUnitOfWork


MessagePresenter = Callable[[list[dict[str, Any]], sqlite3.Connection], list[dict[str, Any]]]
SessionPresenter = Callable[[list[dict[str, Any]], sqlite3.Connection], list[dict[str, Any]]]
_VALID_SCENES = ("group", "private", "channel_group", "channel_private")


class MessageHistoryRepository:
    """Read the operational message history without initializing its database."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    def list_messages(
        self,
        *,
        scene: str = "ALL",
        direction: str = "ALL",
        keyword: str = "",
        group_id: str = "",
        user_id: str = "",
        adapter: str = "",
        start: str = "",
        end: str = "",
        date: str = "",
        page: Any = 1,
        page_size: Any = 50,
        include_total: Any = True,
        presenter: MessagePresenter | None = None,
    ) -> dict[str, Any]:
        scene = str(scene or "").strip()
        direction = str(direction or "").strip()
        keyword = str(keyword or "").strip()
        group_id = str(group_id or "").strip()
        user_id = str(user_id or "").strip()
        adapter = str(adapter or "").strip()
        start = str(start or "").strip()
        end = str(end or "").strip()
        date = str(date or "").strip()
        page = max(1, int(page))
        page_size = min(max(int(page_size), 10), 300)
        include_total = self._enabled(include_total)
        offset = (page - 1) * page_size

        where: list[str] = []
        params: list[Any] = []
        if scene and scene != "ALL":
            where.append("scene = ?")
            params.append(scene)
        if direction and direction != "ALL":
            where.append("direction = ?")
            params.append(direction)
        if keyword:
            where.append(
                "(content LIKE ? OR username LIKE ? OR nickname LIKE ? "
                "OR group_name LIKE ? OR group_id LIKE ? OR user_id LIKE ?)"
            )
            like = f"%{keyword}%"
            params.extend([like] * 6)
        if group_id:
            where.append("group_id = ?")
            params.append(group_id)
        if user_id:
            where.append("user_id = ?")
            params.append(user_id)
        if adapter:
            where.append("adapter = ?")
            params.append(adapter)
        if start:
            where.append("created_at >= ?")
            params.append(start.replace("T", " "))
        if end:
            where.append("created_at <= ?")
            params.append(end.replace("T", " "))
        if date:
            where.append("substr(CAST(created_at AS TEXT), 1, 10) = ?")
            params.append(date)

        where_sql = " WHERE " + " AND ".join(where) if where else ""
        with self._uow() as uow:
            conn = self._connection(uow)
            total = None
            if include_total:
                total_row = uow.query_one(
                    f"SELECT COUNT(*) AS c FROM messages{where_sql}", params
                )
                total = int((total_row or {}).get("c") or 0)

            rows = uow.query_all(
                "SELECT * FROM messages"
                f"{where_sql} ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
                params + [page_size if include_total else page_size + 1, offset],
            )
            has_more = False
            if not include_total and len(rows) > page_size:
                has_more = True
                rows = rows[:page_size]
            elif include_total:
                has_more = offset + len(rows) < int(total or 0)
            rows = self._present(rows, conn, presenter)

        return {
            "success": True,
            "total": total if include_total else len(rows),
            "has_more": has_more,
            "page": page,
            "page_size": page_size,
            "rows": rows,
        }

    def dates(
        self,
        *,
        scene: str,
        target_id: str,
        adapter: str = "",
        include_counts: Any = True,
        today: str | None = None,
    ) -> dict[str, Any]:
        scene = str(scene or "").strip()
        target_id = str(target_id or "").strip()
        adapter = str(adapter or "").strip()
        if scene not in _VALID_SCENES:
            return {"success": False, "error": "无效 scene"}
        if not target_id:
            return {"success": False, "error": "缺少 target_id"}

        where = ["scene = ?"]
        params: list[Any] = [scene]
        if adapter:
            where.append("adapter = ?")
            params.append(adapter)
        if scene in ("group", "channel_group"):
            where.append("group_id = ?")
        else:
            where.append("user_id = ?")
        params.append(target_id)

        date_expr = "substr(CAST(created_at AS TEXT), 1, 10)"
        include_counts = self._enabled(include_counts)
        with self._uow() as uow:
            self._connection(uow)
            if include_counts:
                date_rows = uow.query_all(
                    f"SELECT {date_expr} AS d, COUNT(*) AS c FROM messages "
                    f"WHERE {' AND '.join(where)} GROUP BY {date_expr} "
                    "ORDER BY d DESC LIMIT 60",
                    params,
                )
            else:
                fetched = uow.query_all(
                    "SELECT created_at FROM messages "
                    f"WHERE {' AND '.join(where)} "
                    "ORDER BY created_at DESC, id DESC LIMIT 5000",
                    params,
                )
                seen_dates: set[str] = set()
                date_rows = []
                for row in fetched:
                    day = str(row.get("created_at") or "")[:10]
                    if not day or day in seen_dates:
                        continue
                    seen_dates.add(day)
                    date_rows.append({"d": day, "c": None})
                    if len(date_rows) >= 60:
                        break

        current_day = today or datetime.now().strftime("%Y-%m-%d")
        rows = []
        for row in date_rows:
            day = row["d"]
            if day == current_day:
                label = "今天"
            else:
                try:
                    label = datetime.strptime(day, "%Y-%m-%d").strftime("%m月%d日")
                except Exception:
                    label = day
            rows.append({"date": day, "label": label, "count": row["c"]})
        return {"success": True, "rows": rows}

    def sessions(
        self,
        *,
        scene: str = "group",
        adapter: str = "",
        presenter: SessionPresenter | None = None,
    ) -> dict[str, Any]:
        return self._sessions(scene=scene, adapter=adapter, presenter=presenter)

    def sessions_since(
        self,
        *,
        scene: str = "group",
        adapter: str = "",
        after_id: Any = 0,
        presenter: SessionPresenter | None = None,
    ) -> dict[str, Any]:
        try:
            after_id = max(0, int(after_id))
        except Exception:
            after_id = 0
        return self._sessions(
            scene=scene, adapter=adapter, after_id=after_id, presenter=presenter
        )

    def _sessions(
        self,
        *,
        scene: str,
        adapter: str,
        after_id: int | None = None,
        presenter: SessionPresenter | None = None,
    ) -> dict[str, Any]:
        scene = str(scene or "").strip()
        adapter = str(adapter or "").strip()
        if scene not in _VALID_SCENES:
            return {"success": False, "error": "无效 scene"}

        params: list[Any] = []
        latest_filter = ""
        if after_id is not None:
            latest_filter = "id > ? AND "
            params.append(after_id)
        params.append(scene)
        adapter_sql = ""
        if adapter:
            adapter_sql = " AND adapter = ? "
            params.append(adapter)

        if scene in ("group", "channel_group"):
            target_column = "group_id"
            title_sql = "COALESCE(NULLIF(m.group_name, ''), l.target_id)"
        else:
            target_column = "user_id"
            title_sql = "COALESCE(NULLIF(m.username, ''), NULLIF(m.nickname, ''), l.target_id)"

        order_sql = "m.id DESC" if after_id is not None else "m.created_at DESC, m.id DESC"
        with self._uow() as uow:
            conn = self._connection(uow)
            sql = (
                "WITH latest AS ("
                f"SELECT adapter, scene, {target_column} AS target_id, MAX(id) AS latest_id "
                f"FROM messages WHERE {latest_filter}scene = ? AND {target_column} IS NOT NULL "
                f"AND {target_column} != '' {adapter_sql}GROUP BY adapter, scene, {target_column}"
                f") SELECT l.adapter, l.scene, l.target_id, m.id AS last_row_id, {title_sql} "
                "AS title, m.bot_id AS bot_id, m.created_at AS last_time, "
                "m.content AS last_content, m.direction AS direction, m.username AS username, "
                "m.nickname AS nickname, m.avatar AS avatar, m.user_id AS user_id "
                "FROM latest l JOIN messages m ON m.id = l.latest_id "
                + f"ORDER BY {order_sql} LIMIT 300"
            )
            rows = uow.query_all(
                sql,
                params,
            )
            rows = self._present(rows, conn, presenter)
            last_row_id = max(
                ([after_id] if after_id is not None else [])
                + [int(row.get("last_row_id") or 0) for row in rows]
                or [0]
            )

        return {"success": True, "last_row_id": last_row_id, "rows": rows}

    def list_since(
        self,
        *,
        scene: str,
        target_id: str,
        adapter: str = "",
        date: str = "",
        last_row_id: Any = 0,
        presenter: MessagePresenter | None = None,
    ) -> dict[str, Any]:
        scene = str(scene or "").strip()
        target_id = str(target_id or "").strip()
        adapter = str(adapter or "").strip()
        date = str(date or "").strip()
        try:
            last_row_id = int(last_row_id)
        except Exception:
            last_row_id = 0
        if scene not in _VALID_SCENES:
            return {"success": False, "error": "无效 scene"}
        if not target_id:
            return {"success": False, "error": "缺少 target_id"}

        where = ["id > ?", "scene = ?"]
        params: list[Any] = [last_row_id, scene]
        if adapter:
            where.append("adapter = ?")
            params.append(adapter)
        if date:
            where.append("substr(CAST(created_at AS TEXT), 1, 10) = ?")
            params.append(date)
        where.append("group_id = ?" if scene in ("group", "channel_group") else "user_id = ?")
        params.append(target_id)

        with self._uow() as uow:
            conn = self._connection(uow)
            rows = uow.query_all(
                "SELECT * FROM messages WHERE " + " AND ".join(where) + " ORDER BY id ASC LIMIT 200",
                params,
            )
            rows = self._present(rows, conn, presenter)
            next_id = rows[-1]["id"] if rows else last_row_id
        return {"success": True, "rows": rows, "last_row_id": next_id}

    def list_before(
        self,
        *,
        scene: str,
        target_id: str,
        adapter: str = "",
        keyword: str = "",
        date: str = "",
        before_row_id: Any = 0,
        page_size: Any = 300,
        presenter: MessagePresenter | None = None,
    ) -> dict[str, Any]:
        scene = str(scene or "").strip()
        target_id = str(target_id or "").strip()
        adapter = str(adapter or "").strip()
        keyword = str(keyword or "").strip()
        date = str(date or "").strip()
        try:
            before_row_id = max(0, int(before_row_id))
        except Exception:
            before_row_id = 0
        try:
            page_size = min(max(int(page_size), 50), 300)
        except Exception:
            page_size = 300

        if scene not in _VALID_SCENES:
            return {"success": False, "error": "无效 scene"}
        if not target_id:
            return {"success": False, "error": "缺少 target_id"}
        if before_row_id <= 0:
            return {"success": True, "rows": [], "has_more": False}

        where = ["id < ?", "scene = ?"]
        params: list[Any] = [before_row_id, scene]
        if adapter:
            where.append("adapter = ?")
            params.append(adapter)
        if date:
            where.append("substr(CAST(created_at AS TEXT), 1, 10) = ?")
            params.append(date)
        if keyword:
            where.append(
                "(content LIKE ? OR username LIKE ? OR nickname LIKE ? "
                "OR group_name LIKE ? OR group_id LIKE ? OR user_id LIKE ?)"
            )
            like = f"%{keyword}%"
            params.extend([like] * 6)
        where.append("group_id = ?" if scene in ("group", "channel_group") else "user_id = ?")
        params.append(target_id)

        with self._uow() as uow:
            conn = self._connection(uow)
            fetched = uow.query_all(
                "SELECT * FROM messages WHERE " + " AND ".join(where) + " ORDER BY id DESC LIMIT ?",
                params + [page_size + 1],
            )
            has_more = len(fetched) > page_size
            rows = self._present(fetched[:page_size], conn, presenter)
            oldest_row_id = rows[-1]["id"] if rows else before_row_id
        return {
            "success": True,
            "rows": rows,
            "has_more": has_more,
            "oldest_row_id": oldest_row_id,
        }

    def _uow(self) -> DatabaseUnitOfWork:
        if not self.database.is_file():
            raise FileNotFoundError("消息日志数据库不存在")
        return DatabaseUnitOfWork(self.database, read_only=True)

    @staticmethod
    def _enabled(value: Any) -> bool:
        return str(value).strip().lower() not in ("0", "false", "no")

    @staticmethod
    def _connection(uow: DatabaseUnitOfWork) -> sqlite3.Connection:
        if uow.connection is None:
            raise RuntimeError("message history query requires an active UoW")
        if uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
            ("messages",),
        ) is None:
            raise RuntimeError("消息日志数据库缺少 messages 表")
        return uow.connection

    @staticmethod
    def _present(
        rows: list[dict[str, Any]],
        connection: sqlite3.Connection,
        presenter: MessagePresenter | SessionPresenter | None,
    ) -> list[dict[str, Any]]:
        if presenter is None:
            return rows
        return presenter(rows, connection)


__all__ = ["MessageHistoryRepository", "MessagePresenter", "SessionPresenter"]
