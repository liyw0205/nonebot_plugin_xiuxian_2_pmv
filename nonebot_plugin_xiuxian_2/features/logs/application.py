from __future__ import annotations

from pathlib import Path

from .file_repository import LogFileRepository
from .message_repository import MessageLogsRepository


class LogsApplication:
    def __init__(
        self,
        game_database: str | Path,
        message_database: str | Path,
        module_path: str | Path,
        *,
        project_dir: str | None = None,
        cwd: str | Path | None = None,
        home: str | Path | None = None,
        user_avatar_builder=None,
        year_provider=None,
    ) -> None:
        self.game_database = Path(game_database)
        self.message_database = Path(message_database)
        self.user_avatar_builder = user_avatar_builder
        self.message_repository = MessageLogsRepository(self.game_database, self.message_database)
        self.file_repository = LogFileRepository(
            self.game_database,
            module_path,
            project_dir=project_dir,
            cwd=cwd,
            home=home,
            year_provider=year_provider,
        )

    @staticmethod
    def _safe_int(value, default: int, minimum: int, maximum: int) -> int:
        try:
            return min(maximum, max(minimum, int(value)))
        except Exception:
            return default

    def users(self, query: str = "", limit=20) -> dict:
        query = str(query or "").strip()
        limit = self._safe_int(limit, 20, 5, 50)
        try:
            xiuxian_rows = self.message_repository.xiuxian_user_candidates(query, limit)
            message_rows = self.message_repository.message_user_candidates(query, limit * 2)
            ordered = xiuxian_rows + message_rows if query else message_rows + xiuxian_rows
            merged: dict[str, dict] = {}
            order: list[str] = []
            for row in ordered:
                user_id = str(row.get("user_id") or "").strip()
                if not user_id:
                    continue
                if user_id not in merged:
                    merged[user_id] = dict(row)
                    order.append(user_id)
                else:
                    merged[user_id].update({key: value for key, value in row.items() if value not in (None, "")})

            rows = self.message_repository.enrich_users([merged[user_id] for user_id in order[:limit]])
            if self.user_avatar_builder is not None:
                for row in rows:
                    row["avatar"] = self.user_avatar_builder(
                        row["adapter"], row["bot_id"], row["user_id"], row["avatar"]
                    )
            if query:
                rows.sort(key=lambda row: (
                    0 if row["user_id"] == query else 1,
                    0 if row["user_name"] == query else 1,
                    -int(row.get("message_count") or 0),
                    str(row.get("title") or ""),
                ))
            else:
                rows.sort(key=lambda row: int(row.get("last_row_id") or 0), reverse=True)
            return {"success": True, "rows": rows[:limit]}
        except Exception as exc:
            return {"success": False, "error": f"搜索用户失败：{exc}"}

    def user_messages(
        self,
        *,
        user_id: str,
        scene: str = "ALL",
        direction: str = "ALL",
        keyword: str = "",
        adapter: str = "",
        start: str = "",
        end: str = "",
        page=1,
        page_size=200,
    ) -> dict:
        user_id = str(user_id or "").strip()
        if not user_id:
            return {"success": False, "error": "缺少 user_id"}
        page = self._safe_int(page, 1, 1, 1_000_000)
        page_size = self._safe_int(page_size, 200, 20, 500)
        try:
            return self.message_repository.user_messages(
                user_id,
                scene=str(scene or "ALL").strip(),
                direction=str(direction or "ALL").strip(),
                keyword=str(keyword or "").strip(),
                adapter=str(adapter or "").strip(),
                start=str(start or "").strip(),
                end=str(end or "").strip(),
                page=page,
                page_size=page_size,
            )
        except Exception as exc:
            return {"success": False, "error": f"获取用户消息失败：{exc}"}

    def files(self) -> dict:
        try:
            return self.file_repository.files()
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def read(
        self,
        *,
        file: str,
        keyword: str = "",
        level: str = "ALL",
        start: str = "",
        end: str = "",
        page=1,
        page_size=200,
    ) -> dict:
        try:
            page = max(1, int(page))
        except Exception:
            page = 1
        try:
            page_size = min(max(int(page_size), 50), 1000)
        except Exception:
            page_size = 200
        try:
            return self.file_repository.read(
                str(file or "").strip(),
                str(keyword or "").strip(),
                str(level or "ALL").strip().upper(),
                str(start or "").strip(),
                str(end or "").strip(),
                page,
                page_size,
            )
        except Exception as exc:
            return {"success": False, "error": f"读取失败：{exc}"}

    def tail(
        self,
        *,
        file: str,
        offset=0,
        keyword: str = "",
        level: str = "ALL",
        start: str = "",
        end: str = "",
        ignore_unknown: bool = False,
        ignore_keywords: str = "",
    ) -> dict:
        try:
            offset = int(offset)
        except Exception:
            offset = 0
        try:
            return self.file_repository.tail(
                str(file or "").strip(),
                offset,
                str(keyword or "").strip(),
                str(level or "ALL").strip().upper(),
                str(start or "").strip(),
                str(end or "").strip(),
                bool(ignore_unknown),
                [value.strip() for value in str(ignore_keywords or "").split("|") if value.strip()],
            )
        except Exception as exc:
            return {"success": False, "error": f"tail失败：{exc}"}
