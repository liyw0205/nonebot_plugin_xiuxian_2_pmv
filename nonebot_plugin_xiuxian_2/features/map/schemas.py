from typing import Any, TypedDict


class MapRequest(TypedDict, total=False):
    operation_id: str
    user_id: str
    payload: dict[str, Any]


__all__ = ["MapRequest"]
