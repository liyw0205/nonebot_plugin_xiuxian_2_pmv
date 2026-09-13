from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DailyFortuneRequest:
    user_id: str
    operation_id: str
    date: str | None = None

    def validate(self) -> None:
        if not self.user_id.strip():
            raise ValueError("user_id is required")
        if not self.operation_id.strip():
            raise ValueError("operation_id is required")


@dataclass(frozen=True)
class DailyFortune:
    user_id: str
    date: str
    score: int
    title: str
    message: str

    def to_dict(self) -> dict[str, object]:
        return {
            "user_id": self.user_id,
            "date": self.date,
            "score": self.score,
            "title": self.title,
            "message": self.message,
        }


__all__ = ["DailyFortune", "DailyFortuneRequest"]
