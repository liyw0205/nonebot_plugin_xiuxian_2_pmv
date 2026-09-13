from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class IllusionChoiceResult:
    status: str
    choice_count: int = 0
    stone: int = 0
    exp: int = 0
    item_id: int = 0
    item_name: str = ""
    item_type: str = ""
    selected_option: str = ""
    question_index: int = 0
    choice_index: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "choice_count": self.choice_count,
            "stone": self.stone,
            "exp": self.exp,
            "item_id": self.item_id,
            "item_name": self.item_name,
            "item_type": self.item_type,
            "selected_option": self.selected_option,
            "question_index": self.question_index,
            "choice_index": self.choice_index,
        }


def period_key(now: datetime | None = None) -> str:
    """Return the gameplay day, whose boundary is 08:00 local time."""
    value = now or datetime.now()
    boundary = value.replace(hour=8, minute=0, second=0, microsecond=0)
    day = value.date() if value >= boundary else value.date() - timedelta(days=1)
    return day.isoformat()


__all__ = ["IllusionChoiceResult", "period_key"]
