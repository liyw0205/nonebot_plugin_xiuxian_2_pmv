from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any


class LegacyMapBattleRunner:
    """Keep the shared legacy battle engine behind explicit map adapters."""

    def __init__(
        self,
        *,
        battle_engine: Callable[..., Awaitable[tuple[Any, str, dict[str, Any]]]],
        player_data_provider: Callable[[str], Mapping[str, Any]],
        boss_attribute_provider: Callable[[dict[str, Any], Any], Mapping[str, Any]],
        boss_buff_provider: Callable[..., Sequence[Any]],
        boss_skill_provider: Callable[..., Any],
        boss_status_updater: Callable[[dict[str, Any], Sequence[Mapping[str, Any]]], Any],
        player_status_updater: Callable[[Sequence[Mapping[str, Any]], Any], Any],
    ) -> None:
        self.battle_engine = battle_engine
        self.player_data_provider = player_data_provider
        self.boss_attribute_provider = boss_attribute_provider
        self.boss_buff_provider = boss_buff_provider
        self.boss_skill_provider = boss_skill_provider
        self.boss_status_updater = boss_status_updater
        self.player_status_updater = player_status_updater

    async def __call__(
        self,
        user_id: str,
        boss: dict[str, Any],
        *,
        bot_id: Any,
    ) -> tuple[Any, str, dict[str, Any]]:
        player_data = self.player_data_provider(user_id)
        return await self.battle_engine(
            user_id,
            boss,
            bot_id=bot_id,
            player_data=player_data,
            boss_attribute_provider=self.boss_attribute_provider,
            boss_buff_provider=self.boss_buff_provider,
            boss_skill_provider=self.boss_skill_provider,
            boss_status_updater=self.boss_status_updater,
            player_status_updater=self.player_status_updater,
        )


def build_legacy_map_battle_runner() -> LegacyMapBattleRunner:
    from ..xiuxian.xiuxian_utils.player_fight import (
        Boss_fight,
        generate_boss_buff,
        generate_boss_skill,
        get_boss_attributes,
        get_players_attributes,
        update_all_user_status,
        update_data_boss_status,
    )

    return LegacyMapBattleRunner(
        battle_engine=Boss_fight,
        player_data_provider=get_players_attributes,
        boss_attribute_provider=get_boss_attributes,
        boss_buff_provider=generate_boss_buff,
        boss_skill_provider=generate_boss_skill,
        boss_status_updater=update_data_boss_status,
        player_status_updater=update_all_user_status,
    )


__all__ = ["LegacyMapBattleRunner", "build_legacy_map_battle_runner"]
