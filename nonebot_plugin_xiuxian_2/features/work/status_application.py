from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping

from ...xiuxian.xiuxian_utils.cd_time import elapsed_minutes_from_cd_time, is_blank_cd_time
from .status_repository import WorkStatusSqlRepository, WorkStatusState


LegacyOfferReader = Callable[[str], dict[str, Any] | None]
LegacyOfferProjector = Callable[[str, Mapping[str, Any]], None]


class WorkStatusApplication:
    """Own status selection, SQL expiration CAS, and legacy projection callbacks."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: WorkStatusSqlRepository | None = None,
        legacy_offer_reader: LegacyOfferReader | None = None,
        legacy_projection_writer: LegacyOfferProjector | None = None,
    ) -> None:
        self.repository = repository or WorkStatusSqlRepository(database)
        self.legacy_offer_reader = legacy_offer_reader
        self.legacy_projection_writer = legacy_projection_writer

    def _legacy_offer(self, user_id: str) -> dict[str, Any] | None:
        if self.legacy_offer_reader is None:
            return None
        offer = self.legacy_offer_reader(str(user_id))
        return dict(offer) if isinstance(offer, Mapping) else None

    def _project(self, user_id: str, offer: Mapping[str, Any]) -> None:
        if self.legacy_projection_writer is not None:
            self.legacy_projection_writer(str(user_id), dict(offer))

    def get_offer(self, user_id: str) -> dict[str, Any] | None:
        """Read the canonical SQL offer, falling back only when it is absent."""
        user_id = str(user_id)
        offer = self.repository.get_offer(user_id)
        return offer if offer is not None else self._legacy_offer(user_id)

    @staticmethod
    def _active_status(
        cooldown: Mapping[str, Any], offer: Mapping[str, Any] | None, now: datetime
    ) -> int:
        if is_blank_cd_time(cooldown.get("create_time")):
            return 2
        task_name = str(cooldown.get("scheduled_time") or "")
        tasks = offer.get("tasks") if isinstance(offer, Mapping) else None
        task = tasks.get(task_name) if isinstance(tasks, Mapping) else None
        try:
            duration = int((task or {}).get("time", 0) or 0)
        except (TypeError, ValueError, AttributeError):
            duration = 0
        elapsed = elapsed_minutes_from_cd_time(
            cooldown.get("create_time"), now=now, on_error=0
        )
        return 1 if max(duration - elapsed, 0) > 0 else 2

    @staticmethod
    def _refresh_time(value: Any) -> datetime | None:
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(str(value), fmt)
            except (TypeError, ValueError):
                continue
        return None

    def get_user_work_status(
        self,
        user_id: str,
        *,
        now: datetime | None = None,
        expire_minutes: int = 30,
    ) -> tuple[int, dict[str, Any] | None]:
        user_id = str(user_id)
        current_time = now or datetime.now()
        state: WorkStatusState = self.repository.get_state(user_id)
        cooldown = state.cooldown
        cd_type = int((cooldown or {}).get("type", 0) or 0)

        if cooldown is not None and cd_type == 2:
            active_offer = state.active_snapshot or state.offer
            if active_offer is None:
                active_offer = self._legacy_offer(user_id)
            return self._active_status(cooldown, active_offer, current_time), cooldown

        offer = state.offer
        from_sql = state.has_sql_offer
        if offer is None:
            offer = self._legacy_offer(user_id)
        if offer is None:
            return 0, None

        if offer.get("status") == 1:
            refreshed_at = self._refresh_time(offer.get("refresh_time"))
            if refreshed_at is None:
                return 4, offer
            if current_time - refreshed_at > timedelta(minutes=expire_minutes):
                if from_sql:
                    result = self.repository.mark_offer_expired(
                        user_id,
                        offer,
                        current_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                    )
                    if isinstance(result.offer, dict):
                        offer = result.offer
                    else:
                        offer = {**offer, "status": 0}
                    self._project(user_id, offer)
                else:
                    offer = {**offer, "status": 0}
                    self._project(user_id, offer)

        return (3, offer) if offer.get("status") == 1 else (4, offer)


__all__ = ["WorkStatusApplication"]
