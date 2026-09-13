from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TitleTransactionResult:
    status: str
    title_id: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate", "already_equipped"}

    def to_dict(self) -> dict[str, str]:
        return {"status": self.status, "title_id": self.title_id}


__all__ = ["TitleTransactionResult"]
