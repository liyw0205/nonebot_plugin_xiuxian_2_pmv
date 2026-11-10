"""Write rejection shared by activity redemption transaction owners."""

from ...core.errors import DomainError
from ...core.player_reasons import reason_text


class ActivityRedemptionRejected(DomainError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason, reason_text(reason), {"reason": reason, "subject": "actor"})


def require_redemption_write(cursor, reason: str, *, expected: int = 1) -> None:
    if cursor.rowcount != expected:
        raise ActivityRedemptionRejected(reason)
