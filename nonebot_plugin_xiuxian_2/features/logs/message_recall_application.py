from __future__ import annotations

from typing import Any


class MessageRecallApplication:
    """Coordinate platform recall and its best-effort history marker."""

    def __init__(self, delivery: Any, repository: Any) -> None:
        self.delivery = delivery
        self.repository = repository

    async def revoke(
        self,
        bot: Any,
        *,
        adapter: str,
        scene: str,
        message_id: str,
        group_id: str = "",
        user_id: str = "",
        row_id: str = "",
    ) -> dict[str, Any]:
        await self.delivery.recall(
            bot,
            scene=scene,
            message_id=message_id,
            group_id=group_id,
            user_id=user_id,
        )

        try:
            affected_rows = self.repository.mark_recalled(
                adapter=adapter,
                scene=scene,
                message_id=message_id,
                row_id=row_id,
            )
        except Exception as exc:
            return {
                "success": True,
                "message": "撤回成功",
                "log_updated": False,
                "log_error": str(exc),
            }

        return {
            "success": True,
            "message": "撤回成功",
            "log_updated": affected_rows > 0,
            "log_error": "" if affected_rows > 0 else "未找到对应消息记录",
        }


__all__ = ["MessageRecallApplication"]
