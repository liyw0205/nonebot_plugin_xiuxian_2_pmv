from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FairylandClaimRequest:
    operation_id: str
    user_id: str
    sect_id: str
    day: str
    level: int
    minutes: int

    def validate(self) -> None:
        if not self.operation_id or not self.user_id or not self.sect_id or not self.day:
            raise ValueError("operation_id, user_id, sect_id and day are required")
        if self.level <= 0 or self.minutes <= 0:
            raise ValueError("level and minutes must be positive")

    def payload(self) -> dict[str, object]:
        return {
            "user_id": self.user_id,
            "sect_id": self.sect_id,
            "day": self.day,
            "level": self.level,
            "minutes": self.minutes,
        }


__all__ = ["FairylandClaimRequest"]
