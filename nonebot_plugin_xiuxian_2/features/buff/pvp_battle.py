"""Pure normal-PvP battle calculation adapter.

Persistence and asset mutation live in ``pvp_repository``.  This module keeps
the existing battle engine inputs isolated while that engine is migrated.
"""

from __future__ import annotations

from ...xiuxian.xiuxian_utils.fight_models import Entity
from ...xiuxian.xiuxian_utils.player_fight import BattleSystem, apply_player_buffs, get_players_attributes


def calculate_battle(challenger_id, opponent_id, bot_id=0):
    players = [get_players_attributes(challenger_id), get_players_attributes(opponent_id)]
    entities = []
    for team_id, player in enumerate(players):
        attributes = player["属性"]
        attributes["natal_data"] = player.get("本命法宝")
        entity = Entity(attributes, team_id=team_id)
        apply_player_buffs(entity, player)
        entities.append(entity)
    messages, winner, statuses = BattleSystem([entities[0]], [entities[1]], bot_id).run_battle()
    final = {}
    for item in statuses:
        for attributes in item.values():
            hp_multiplier = float(attributes.get("hp_multiplier", 1) or 1)
            mp_multiplier = float(attributes.get("mp_multiplier", 1) or 1)
            final[str(attributes["user_id"])] = (
                max(1, int(float(attributes.get("hp", 1)) / hp_multiplier)),
                max(1, int(float(attributes.get("mp", 1)) / mp_multiplier)),
            )
    winner_id = "" if winner == 2 else str((challenger_id, opponent_id)[winner])
    winner_name = "没有人" if winner == 2 else str(players[winner]["属性"]["nickname"])
    return messages, winner_id, winner_name, final


__all__ = ["calculate_battle"]
