"""Database-authoritative auction compatibility facade.

The command package historically imported auction helpers from this module.
Keep the facade small while the full auction slice moves to the feature layer.
"""

from __future__ import annotations

from typing import Any

# Runtime reads and settlements are delegated to the main trade repository:
# auction_repository.get_current_auction and auction_repository.settle_auction_item.

_auction_session_service: Any = None


def start_auction_process(bot: Any, operation_id: str | None = None) -> bool:
    service = _auction_session_service
    if service is None:
        from .auction_utils import auction_session_service

        service = auction_session_service
    if callable(service):
        service = service()
    if service is None:
        return False
    # Starting an already active database session is idempotent; a finished
    # session must not be resurrected by a stale compatibility call.
    return service.get_active_session() is not None


__all__ = ["start_auction_process"]
