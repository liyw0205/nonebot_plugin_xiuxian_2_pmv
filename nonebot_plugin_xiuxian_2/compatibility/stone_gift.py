"""Compatibility facade for the legacy ``StoneGiftService`` API."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any
import warnings

from ..core.errors import DomainError
from ..features.stone_gift.application import StoneGiftApplication


@dataclass(frozen=True)
class StoneGiftResult:
    status: str
    sender_id: str
    recipient_id: str
    gross_amount: int
    net_amount: int
    fee_amount: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"transferred", "duplicate"}


class StoneGiftService:
    """Preserve message-handler result semantics while using the new use case."""

    def __init__(self, database: str | Path) -> None:
        self.application = StoneGiftApplication(str(database))
        enabled = os.environ.get("XIUXIAN_STONE_GIFT_ENABLED", "true").strip().lower()
        self._legacy = None
        if enabled in {"0", "false", "no", "off"}:
            from ..xiuxian.xiuxian_base.transaction_service import StoneGiftService as LegacyStoneGiftService

            self._legacy = LegacyStoneGiftService(database)

    @staticmethod
    def _warn() -> None:
        warnings.warn(
            "StoneGiftService is a compatibility facade; use StoneGiftApplication",
            DeprecationWarning,
            stacklevel=3,
        )
        from .commands import record_compatibility_hit

        record_compatibility_hit("stone_gift")

    @staticmethod
    def _result(status: str, sender_id: str, recipient_id: str, gross: int, net: int, fee: int) -> StoneGiftResult:
        return StoneGiftResult(status, str(sender_id), str(recipient_id), int(gross), int(net), int(fee))

    def get_operation(self, operation_id: str, sender_id: str, recipient_id: str) -> StoneGiftResult | None:
        self._warn()
        if self._legacy is not None:
            return self._legacy.get_operation(operation_id, sender_id, recipient_id)
        record = self.application.lookup(operation_id, sender_id=sender_id, recipient_id=recipient_id)
        if record is None:
            return None
        return self._result("duplicate", record.sender_id, record.recipient_id, record.gross_amount, record.net_amount, record.fee_amount)

    def transfer(
        self,
        operation_id: str,
        sender_id: str,
        recipient_id: str,
        gross_amount: int,
        *,
        fee_rate: float = 0.1,
        transfer_date: str | None = None,
        send_limit: int | None = None,
        receive_limit: int | None = None,
        send_used: int | None = None,
        receive_used: int | None = None,
    ) -> StoneGiftResult:
        self._warn()
        if self._legacy is not None:
            return self._legacy.transfer(operation_id, sender_id, recipient_id, gross_amount, fee_rate=fee_rate)
        # The historical facade treated an operation key as authoritative and
        # returned its original transfer even when a retried command carried a
        # different amount.  Keep that public contract here; the application
        # service itself still rejects request-hash conflicts for new callers.
        previous = self.application.lookup(operation_id)
        if previous is not None:
            if previous.sender_id != str(sender_id) or previous.recipient_id != str(recipient_id):
                raise ValueError("operation participants changed")
            return self._result(
                "duplicate",
                previous.sender_id,
                previous.recipient_id,
                previous.gross_amount,
                previous.net_amount,
                previous.fee_amount,
            )
        try:
            outcome = self.application.transfer(
                operation_id=operation_id,
                sender_id=sender_id,
                recipient_id=recipient_id,
                gross_amount=gross_amount,
                fee_rate=fee_rate,
                transfer_date=transfer_date,
                send_limit=send_limit,
                receive_limit=receive_limit,
                send_used=send_used,
                receive_used=receive_used,
            )
        except DomainError as exc:
            if exc.code == "validation_error":
                raise ValueError(exc.message) from exc
            raise
        data: Any = outcome.data or {}
        raw = data.get("stone_gift", {}) if isinstance(data, dict) else {}
        gross = int(raw.get("gross_amount", gross_amount) or 0)
        net = int(raw.get("net_amount", gross - int(gross * fee_rate)) or 0)
        fee = int(raw.get("fee_amount", gross - net) or 0)
        if outcome.ok:
            return self._result("duplicate" if outcome.status == "replayed" else "transferred", raw.get("sender_id", sender_id), raw.get("recipient_id", recipient_id), gross, net, fee)
        return self._result(outcome.code or outcome.status, sender_id, recipient_id, gross, net, fee)


__all__ = ["StoneGiftResult", "StoneGiftService"]
