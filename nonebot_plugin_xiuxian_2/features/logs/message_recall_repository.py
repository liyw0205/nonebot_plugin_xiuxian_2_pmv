from __future__ import annotations

from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


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

        # require_exists opens mode=rw, so a database deleted after the
        # existence check above is rejected instead of re-created.
        with DatabaseUnitOfWork(self.database, require_exists=True) as uow:
            if str(row_id or "").strip():
                cursor = uow.execute(
                    "UPDATE messages SET content=? WHERE id=?",
                    ("[该消息已撤回]", str(row_id).strip()),
                )
            else:
                cursor = uow.execute(
                    "UPDATE messages SET content=? "
                    "WHERE adapter=? AND scene=? AND message_id=?",
                    (
                        "[该消息已撤回]",
                        str(adapter or "").strip(),
                        str(scene or "").strip(),
                        str(message_id or "").strip(),
                    ),
                )
            return int(cursor.rowcount)


__all__ = ["MessageRecallRepository"]
