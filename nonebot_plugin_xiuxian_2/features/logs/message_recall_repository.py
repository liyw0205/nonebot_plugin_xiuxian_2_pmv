from __future__ import annotations

import sqlite3
from pathlib import Path


class MessageRecallRepository:
    """Mark existing message log rows as recalled."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    def mark_recalled(
        self,
        *,
        adapter: str,
        scene: str,
        message_id: str,
        row_id: str = "",
    ) -> int:
        if not self.database.is_file():
            raise FileNotFoundError("消息日志数据库不存在")

        # mode=rw prevents SQLite from silently creating a database between the
        # existence check and connection open.
        connection = sqlite3.connect(
            f"{self.database.resolve().as_uri()}?mode=rw", uri=True
        )
        try:
            if str(row_id or "").strip():
                cursor = connection.execute(
                    "UPDATE messages SET content=? WHERE id=?",
                    ("[该消息已撤回]", str(row_id).strip()),
                )
            else:
                cursor = connection.execute(
                    "UPDATE messages SET content=? "
                    "WHERE adapter=? AND scene=? AND message_id=?",
                    (
                        "[该消息已撤回]",
                        str(adapter or "").strip(),
                        str(scene or "").strip(),
                        str(message_id or "").strip(),
                    ),
                )
            connection.commit()
            return int(cursor.rowcount)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


__all__ = ["MessageRecallRepository"]
