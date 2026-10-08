from __future__ import annotations

from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class MessageLogsRepository:
    MAX_CANDIDATES = 50

    def __init__(self, game_database: str | Path, message_database: str | Path) -> None:
        self.game_database = Path(game_database)
        self.message_database = Path(message_database)

    @staticmethod
    def _table_exists(uow: DatabaseUnitOfWork, table_name: str) -> bool:
        return uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ) is not None

    def xiuxian_user_candidates(self, query: str, limit: int) -> list[dict]:
        if not self.game_database.is_file():
            return []
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            if not self._table_exists(uow, "user_xiuxian"):
                return []
            if query:
                like = f"%{query}%"
                rows = uow.query_all(
                    "SELECT user_id,user_name,level,root_type FROM user_xiuxian "
                    "WHERE CAST(user_id AS TEXT)=? OR CAST(user_id AS TEXT) LIKE ? "
                    "OR COALESCE(user_name,'') LIKE ? "
                    "ORDER BY CASE WHEN CAST(user_id AS TEXT)=? THEN 0 "
                    "WHEN user_name=? THEN 1 ELSE 2 END,user_name ASC,user_id ASC LIMIT ?",
                    (query, like, like, query, query, limit),
                )
            else:
                rows = uow.query_all(
                    "SELECT user_id,user_name,level,root_type FROM user_xiuxian "
                    "WHERE user_id IS NOT NULL ORDER BY user_id ASC LIMIT ?",
                    (limit,),
                )
        return rows

    def _require_messages(self) -> None:
        if not self.message_database.is_file():
            raise FileNotFoundError("消息日志数据库不存在")

    def message_user_candidates(self, query: str, limit: int) -> list[dict]:
        self._require_messages()
        where = ["user_id IS NOT NULL", "user_id != ''"]
        params: list[object] = []
        if query:
            like = f"%{query}%"
            where.append("(user_id=? OR user_id LIKE ? OR username LIKE ? OR nickname LIKE ?)")
            params.extend((query, like, like, like))
        with DatabaseUnitOfWork(self.message_database, read_only=True) as uow:
            if not self._table_exists(uow, "messages"):
                raise RuntimeError("消息日志数据库缺少 messages 表")
            return uow.query_all(
                "SELECT user_id,MAX(id) AS last_row_id,MAX(created_at) AS last_time "
                "FROM messages WHERE " + " AND ".join(where) +
                " GROUP BY user_id ORDER BY last_row_id DESC LIMIT ?",
                params + [limit],
            )

    @staticmethod
    def _selected_values(user_ids: list[str]) -> tuple[str, list[str]]:
        placeholders = ",".join("(?)" for _ in user_ids)
        return placeholders, user_ids

    @staticmethod
    def _human_name(username, nickname, user_id: str) -> str:
        for value in (username, nickname):
            value = str(value or "").strip()
            if value and value.lower() != "bot" and value != user_id:
                return value
        return ""

    def enrich_users(self, bases: list[dict]) -> list[dict]:
        user_ids = list(dict.fromkeys(str(row.get("user_id") or "").strip() for row in bases))
        user_ids = [value for value in user_ids if value]
        if not user_ids:
            return [self._user_row(base, {}, {}, "") for base in bases]
        self._require_messages()

        placeholders, selected_params = self._selected_values(user_ids)
        selected = f"selected(user_id) AS (VALUES {placeholders})"
        with DatabaseUnitOfWork(self.message_database, read_only=True) as uow:
            if not self._table_exists(uow, "messages"):
                raise RuntimeError("消息日志数据库缺少 messages 表")

            summary_rows = uow.query_all(
                "WITH " + selected + ", relevant(user_id,id,direction,created_at) AS ("
                "SELECT selected.user_id,message.id,message.direction,message.created_at "
                "FROM selected JOIN messages AS message ON message.user_id=selected.user_id "
                "UNION "
                "SELECT selected.user_id,sent.id,sent.direction,sent.created_at "
                "FROM selected JOIN messages AS received ON received.user_id=selected.user_id "
                "AND received.direction='recv' AND COALESCE(received.message_id,'')!='' "
                "JOIN messages AS sent ON sent.direction='send' "
                "AND sent.source_message_id=received.message_id) "
                "SELECT user_id,COUNT(*) AS message_count,"
                "SUM(CASE WHEN direction='recv' THEN 1 ELSE 0 END) AS recv_count,"
                "SUM(CASE WHEN direction='send' THEN 1 ELSE 0 END) AS send_count,"
                "MAX(created_at) AS last_time,MAX(id) AS last_row_id FROM relevant GROUP BY user_id",
                selected_params,
            )
            profiles = uow.query_all(
                "WITH " + selected + ", ranked AS ("
                "SELECT message.user_id,message.adapter,message.bot_id,message.username,message.nickname,"
                "message.avatar,ROW_NUMBER() OVER(PARTITION BY message.user_id ORDER BY message.id DESC) AS rn "
                "FROM messages AS message JOIN selected USING(user_id) WHERE message.direction='recv') "
                "SELECT user_id,adapter,bot_id,username,nickname,avatar FROM ranked WHERE rn=1",
                selected_params,
            )
            nickname_rows = uow.query_all(
                "WITH " + selected + " SELECT nickname.user_id,nickname.username "
                "FROM user_nicknames AS nickname JOIN selected USING(user_id)",
                selected_params,
            ) if self._table_exists(uow, "user_nicknames") else []
            human_rows = uow.query_all(
                "WITH " + selected + ", ranked AS ("
                "SELECT message.user_id,message.scene,message.username,message.nickname,"
                "CASE WHEN message.scene IN ('group','channel_group') THEN 0 ELSE 1 END AS scene_rank,"
                "ROW_NUMBER() OVER(PARTITION BY message.user_id,"
                "CASE WHEN message.scene IN ('group','channel_group') THEN 0 ELSE 1 END "
                "ORDER BY message.created_at DESC,message.id DESC) AS rn "
                "FROM messages AS message JOIN selected USING(user_id) "
                "WHERE message.direction='recv' "
                "AND message.scene IN ('group','channel_group','private','channel_private') "
                "AND ((TRIM(COALESCE(message.username,''))!='' AND message.username!='Bot' "
                "AND message.username!=message.user_id) OR "
                "(TRIM(COALESCE(message.nickname,''))!='' AND message.nickname!='Bot' "
                "AND message.nickname!=message.user_id))) "
                "SELECT user_id,scene,username,nickname FROM ranked WHERE rn=1 "
                "ORDER BY user_id,scene_rank",
                selected_params,
            )

        summary_by_id = {str(row["user_id"]): row for row in summary_rows}
        profile_by_id = {str(row["user_id"]): row for row in profiles}
        cached_names = {str(row["user_id"]): str(row["username"] or "") for row in nickname_rows}
        history_names: dict[str, dict[int, str]] = {}
        for row in human_rows:
            user_id = str(row["user_id"])
            rank = 0 if row["scene"] in ("group", "channel_group") else 1
            name = self._human_name(row["username"], row["nickname"], user_id)
            if name:
                history_names.setdefault(user_id, {}).setdefault(rank, name)

        result = []
        for base in bases:
            user_id = str(base.get("user_id") or "").strip()
            profile = dict(profile_by_id.get(user_id, {}))
            summary = dict(summary_by_id.get(user_id, {}))
            human_name = self._human_name(cached_names.get(user_id), "", user_id)
            if not human_name:
                available = history_names.get(user_id, {})
                human_name = available.get(0) or available.get(1) or ""
            result.append(self._user_row(base, profile, summary, human_name))
        return result

    @staticmethod
    def _user_row(base: dict, profile: dict, summary: dict, human_name: str) -> dict:
        user_id = str(base.get("user_id") or "").strip()
        user_name = str(base.get("user_name") or "").strip()
        if not human_name:
            human_name = MessageLogsRepository._human_name(
                profile.get("username"), profile.get("nickname"), user_id
            )
        title = user_name or human_name or user_id or "未知用户"
        subtitle_bits = []
        if user_name:
            subtitle_bits.append(f"道号: {user_name}")
        if human_name and human_name != user_name:
            subtitle_bits.append(f"昵称: {human_name}")
        if user_id:
            subtitle_bits.append(f"ID: {user_id}")
        adapter = str(profile.get("adapter") or "")
        bot_id = str(profile.get("bot_id") or "")
        return {
            "user_id": user_id,
            "user_name": user_name,
            "title": title,
            "subtitle": " ｜ ".join(subtitle_bits),
            "level": base.get("level") or "",
            "root_type": base.get("root_type") or "",
            "adapter": adapter,
            "bot_id": bot_id,
            "avatar": str(profile.get("avatar") or ""),
            "avatar_text": str(title)[:1] if title else "人",
            "message_count": int(summary.get("message_count") or 0),
            "recv_count": int(summary.get("recv_count") or 0),
            "send_count": int(summary.get("send_count") or 0),
            "last_time": summary.get("last_time") or base.get("last_time") or "",
            "last_row_id": int(summary.get("last_row_id") or base.get("last_row_id") or 0),
        }

    def user_messages(
        self,
        user_id: str,
        *,
        scene: str,
        direction: str,
        keyword: str,
        adapter: str,
        start: str,
        end: str,
        page: int,
        page_size: int,
    ) -> dict:
        self._require_messages()
        where = [
            "(user_id=? OR (direction='send' AND COALESCE(source_message_id,'')!='' "
            "AND source_message_id IN (SELECT message_id FROM messages WHERE user_id=? "
            "AND direction='recv' AND COALESCE(message_id,'')!='')))"
        ]
        params: list[object] = [user_id, user_id]
        if scene and scene != "ALL":
            if scene not in ("group", "private", "channel_group", "channel_private"):
                return {"success": False, "error": "无效 scene"}
            where.append("scene=?")
            params.append(scene)
        if direction and direction != "ALL":
            if direction not in ("recv", "send"):
                return {"success": False, "error": "无效 direction"}
            where.append("direction=?")
            params.append(direction)
        if adapter:
            where.append("adapter=?")
            params.append(adapter)
        if keyword:
            where.append("(content LIKE ? OR username LIKE ? OR nickname LIKE ? OR group_name LIKE ? OR group_id LIKE ? OR user_id LIKE ?)")
            like = f"%{keyword}%"
            params.extend([like] * 6)
        if start:
            where.append("created_at>=?")
            params.append(start.replace("T", " "))
        if end:
            where.append("created_at<=?")
            params.append(end.replace("T", " "))
        where_sql = " WHERE " + " AND ".join(where)

        with DatabaseUnitOfWork(self.message_database, read_only=True) as uow:
            if not self._table_exists(uow, "messages"):
                raise RuntimeError("消息日志数据库缺少 messages 表")
            total_row = uow.query_one("SELECT COUNT(*) AS c FROM messages" + where_sql, params)
            total = int((total_row or {}).get("c") or 0)
            rows = uow.query_all(
                "SELECT * FROM messages" + where_sql +
                " ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?",
                params + [page_size, (page - 1) * page_size],
            )

        user = {}
        if self.game_database.is_file():
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                if self._table_exists(uow, "user_xiuxian"):
                    user = uow.query_one(
                        "SELECT user_id,user_name,level,root_type FROM user_xiuxian WHERE user_id=? LIMIT 1",
                        (user_id,),
                    ) or {}
        return {
            "success": True,
            "user": {
                "user_id": user_id,
                "user_name": user.get("user_name", ""),
                "level": user.get("level", ""),
                "root_type": user.get("root_type", ""),
            },
            "total": total,
            "page": page,
            "page_size": page_size,
            "rows": rows,
        }
