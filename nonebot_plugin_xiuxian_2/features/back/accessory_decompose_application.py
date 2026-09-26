from __future__ import annotations

from pathlib import Path
from typing import Any

from .accessory_decompose_domain import AccessoryDecomposeChange
from .accessory_decompose_repository import AccessoryDecomposeSqlRepository


class AccessoryDecomposeApplication:
    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: AccessoryDecomposeSqlRepository | None = None,
    ) -> None:
        self.repository = repository or AccessoryDecomposeSqlRepository(
            game_database, player_database
        )

    def replay(self, operation_id: str) -> AccessoryDecomposeChange | None:
        return self.repository.replay(operation_id)

    def decompose(
        self,
        operation_id: str,
        user_id: str,
        uid: str,
        expected_accessory: dict[str, Any],
        stone_id: int,
        stone_name: str,
        stone_gain: int,
        max_goods_num: int,
    ) -> AccessoryDecomposeChange:
        return self.repository.decompose(
            operation_id,
            user_id,
            uid,
            expected_accessory,
            stone_id,
            stone_name,
            stone_gain,
            max_goods_num,
        )

    def batch_decompose(
        self,
        operation_id: str,
        user_id: str,
        expected_bag: list[dict[str, Any]],
        selected_uids: tuple[str, ...],
        stone_id: int,
        stone_name: str,
        total_gain: int,
        max_goods_num: int,
    ) -> AccessoryDecomposeChange:
        return self.repository.batch_decompose(
            operation_id,
            user_id,
            expected_bag,
            selected_uids,
            stone_id,
            stone_name,
            total_gain,
            max_goods_num,
        )


__all__ = ["AccessoryDecomposeApplication", "AccessoryDecomposeChange"]
