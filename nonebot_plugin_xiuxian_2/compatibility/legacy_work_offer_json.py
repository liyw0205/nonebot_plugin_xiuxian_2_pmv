from __future__ import annotations

from typing import Any, Mapping


class LegacyWorkOfferJsonAdapter:
    """Explicit bridge for historical workinfo.json readers and projections."""

    def read(self, user_id: str) -> dict[str, Any] | None:
        from ..xiuxian.xiuxian_work.reward_data_source import read_legacy_json_offer

        return read_legacy_json_offer(str(user_id))

    def project(self, user_id: str, offer: Mapping[str, Any]) -> None:
        from ..xiuxian.xiuxian_work.reward_data_source import savef

        savef(str(user_id), dict(offer), sync_snapshot=False)

    def remove(self, user_id: str, *, delete_snapshot: bool = False) -> None:
        from ..xiuxian.xiuxian_work.reward_data_source import delete_work_file

        delete_work_file(str(user_id), delete_snapshot=delete_snapshot)


__all__ = ["LegacyWorkOfferJsonAdapter"]
