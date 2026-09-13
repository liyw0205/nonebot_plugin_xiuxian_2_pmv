from __future__ import annotations

import uuid


class UUIDGenerator:
    def new_id(self) -> str:
        return uuid.uuid4().hex


__all__ = ["UUIDGenerator"]
