from typing import Any, TypedDict


class TradeRequest(TypedDict, total=False):
    operation_id: str
    user_id: str
    payload: dict[str, Any]


__all__ = ["TradeRequest"]
