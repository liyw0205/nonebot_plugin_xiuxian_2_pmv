from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class AdapterCapabilities:
    platform: str
    features: frozenset[str]

    def supports(self, feature: str) -> bool:
        return str(feature) in self.features


class CapabilityRegistry:
    def __init__(self, capabilities: Iterable[AdapterCapabilities] = ()) -> None:
        self._items: dict[str, AdapterCapabilities] = {}
        for item in capabilities:
            self.register(item)

    def register(self, item: AdapterCapabilities) -> AdapterCapabilities:
        key = item.platform.casefold()
        previous = self._items.get(key)
        if previous is not None and previous != item:
            raise ValueError(f"conflicting adapter capability: {item.platform}")
        self._items[key] = item
        return item

    def get(self, platform: str) -> AdapterCapabilities:
        return self._items[str(platform).casefold()]

    def supports(self, platform: str, feature: str) -> bool:
        try:
            return self.get(platform).supports(feature)
        except KeyError:
            return False

    def export(self) -> dict[str, list[str]]:
        return {key: sorted(item.features) for key, item in self._items.items()}


DEFAULT_CAPABILITIES = CapabilityRegistry(
    (
        AdapterCapabilities("onebot", frozenset({"text", "image", "at", "reply", "group_member"})),
        AdapterCapabilities("qq", frozenset({"text", "image", "markdown", "keyboard", "reply", "group_member", "interaction_ack"})),
    )
)


__all__ = ["AdapterCapabilities", "CapabilityRegistry", "DEFAULT_CAPABILITIES"]
