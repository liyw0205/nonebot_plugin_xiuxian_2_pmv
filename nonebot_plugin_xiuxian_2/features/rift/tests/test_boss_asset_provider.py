from unittest.mock import patch

import nonebot

nonebot.init()

from ....xiuxian.xiuxian_utils import player_fight


def test_player_attribute_builder_uses_explicit_item_provider():
    class Buffs:
        BuffInfo = {"faqi_buff": 7}

        def __init__(self, _user_id):
            pass

    class Natal:
        def __init__(self, _user_id):
            pass

        def exists(self):
            return False

    final = {
        "user_id": "u",
        "nickname": "道友",
        "max_hp": 100,
        "current_hp": 90,
        "max_mp": 80,
        "current_mp": 70,
        "final_atk": 20,
        "exp": 100,
        "crit_rate": 0,
        "crit_damage": 1.5,
        "boss_damage_bonus": 0,
        "damage_reduction": 0,
        "armor_penetration": 0,
    }
    calls = []

    def lookup(item_id):
        calls.append(item_id)
        return {"name": "法器", "mp_buff": 3}

    with patch.object(player_fight, "UserBuffDate", Buffs), patch.object(
        player_fight, "get_final_attributes", return_value=final
    ), patch.object(player_fight, "NatalTreasure", Natal), patch.object(
        player_fight, "get_user_pet_for_battle", return_value=None
    ):
        result = player_fight.get_players_attributes("u", item_provider=lookup)

    assert calls == [7]
    assert result["法器"]["mp_buff"] == 3
