from typing import Any, TypedDict


class BaseRequest(TypedDict, total=False):
    operation_id: str
    user_id: str
    payload: dict[str, Any]


__all__ = ["BaseRequest"]
