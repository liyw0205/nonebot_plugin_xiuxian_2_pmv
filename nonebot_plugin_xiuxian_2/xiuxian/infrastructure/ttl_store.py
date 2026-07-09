from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from collections.abc import Callable
from typing import Generic, TypeVar


K = TypeVar("K")
V = TypeVar("V")


class TTLStore(Generic[K, V]):
    """带容量上限的进程内 TTL 存储，支持注入时钟以便测试。"""

    def __init__(
        self,
        *,
        ttl: float,
        max_size: int = 5000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl <= 0:
            raise ValueError("ttl 必须大于 0")
        if max_size <= 0:
            raise ValueError("max_size 必须大于 0")
        self.ttl = float(ttl)
        self.max_size = int(max_size)
        self._clock = clock
        self._items: OrderedDict[K, tuple[float, V]] = OrderedDict()
        self._lock = asyncio.Lock()

    def _purge(self, now: float) -> None:
        while self._items:
            key, (expires_at, _) = next(iter(self._items.items()))
            if expires_at > now:
                break
            self._items.pop(key, None)

    async def set(self, key: K, value: V, *, ttl: float | None = None) -> None:
        lifetime = self.ttl if ttl is None else float(ttl)
        if lifetime <= 0:
            raise ValueError("ttl 必须大于 0")
        async with self._lock:
            now = self._clock()
            self._purge(now)
            self._items.pop(key, None)
            self._items[key] = (now + lifetime, value)
            while len(self._items) > self.max_size:
                self._items.popitem(last=False)

    async def get(self, key: K, default: V | None = None) -> V | None:
        async with self._lock:
            now = self._clock()
            self._purge(now)
            item = self._items.get(key)
            if item is None:
                return default
            self._items.move_to_end(key)
            return item[1]

    async def pop(self, key: K, default: V | None = None) -> V | None:
        async with self._lock:
            self._purge(self._clock())
            item = self._items.pop(key, None)
            return item[1] if item else default

    async def add_if_absent(self, key: K, value: V, *, ttl: float | None = None) -> bool:
        lifetime = self.ttl if ttl is None else float(ttl)
        if lifetime <= 0:
            raise ValueError("ttl 必须大于 0")
        async with self._lock:
            now = self._clock()
            self._purge(now)
            if key in self._items:
                return False
            self._items[key] = (now + lifetime, value)
            while len(self._items) > self.max_size:
                self._items.popitem(last=False)
            return True

    async def clear(self) -> None:
        async with self._lock:
            self._items.clear()

    async def size(self) -> int:
        async with self._lock:
            self._purge(self._clock())
            return len(self._items)


__all__ = ["TTLStore"]
