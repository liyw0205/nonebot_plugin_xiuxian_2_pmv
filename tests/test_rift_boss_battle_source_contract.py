from pathlib import Path


def test_rift_boss_handlers_use_feature_battle_application():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_rift/__init__.py"
    ).read_text(encoding="utf-8")
    event_handler = source[
        source.index("async def _roll_rift_event") : source.index(
            "async def _roll_rift_boss_event", source.index("async def _roll_rift_event")
        )
    ]
    token_handler = source[
        source.index("async def _roll_rift_boss_event") : source.index(
            "@complete_rift.handle", source.index("async def _roll_rift_boss_event")
        )
    ]
    assert "rift_application.roll_boss_battle(" in event_handler
    assert "rift_application.roll_boss_battle(" in token_handler
    assert "get_boss_battle_info(" not in event_handler
    assert "get_boss_battle_info(" not in token_handler


def test_rift_boss_asset_snapshot_is_an_explicit_engine_boundary():
    resolver = Path(
        "nonebot_plugin_xiuxian_2/features/rift/domain.py"
    ).read_text(encoding="utf-8")
    facade = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_rift/__init__.py"
    ).read_text(encoding="utf-8")
    battle = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/player_fight.py"
    ).read_text(encoding="utf-8")
    assert "class RiftBossBattleAssetProvider" in resolver
    assert "player_asset_provider=get_rift_battle_player_assets" in facade
    assert "player_data=None" in battle
    assert 'runner_kwargs["player_data"] = player_data' in resolver


def test_rift_boss_player_snapshot_wires_item_lookup_provider():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_rift/riftmake.py"
    ).read_text(encoding="utf-8")
    player_fight = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/player_fight.py"
    ).read_text(encoding="utf-8")
    assert "item_provider=items.get_data_by_item_id" in source
    assert "pet_provider=get_user_pet_for_battle" in source
    assert "attribute_provider=None" in player_fight
    assert "item_data = item_provider(item_id)" in player_fight
    assert "buffs[\"宠物\"] = pet_provider(user_id)" in player_fight
    assert "final_attr = attribute_provider(user_id, ratio=ratio, include_current=True)" in player_fight
    assert "attribute_provider=get_rift_battle_final_attributes" in source


def test_rift_boss_player_snapshot_wires_read_only_natal_provider():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_rift/riftmake.py"
    ).read_text(encoding="utf-8")
    player_fight = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/player_fight.py"
    ).read_text(encoding="utf-8")
    provider_start = source.index("def get_rift_battle_natal_data")
    provider = source[provider_start:source.index("async def get_boss_battle_info", provider_start)]
    assert "natal_provider=get_rift_battle_natal_data" in source
    assert "natal_provider=None" in player_fight
    assert "natal_data = natal_provider(user_id)" in player_fight
    assert "DatabaseUnitOfWork(database, read_only=True)" in provider
    assert "CREATE TABLE" not in provider
    assert "ALTER TABLE" not in provider


def test_rift_boss_final_attributes_wires_read_only_impart_provider():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_rift/riftmake.py"
    ).read_text(encoding="utf-8")
    attributes = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/xiuxian2_handle.py"
    ).read_text(encoding="utf-8")
    provider_start = source.index("def get_rift_battle_impart_data")
    provider = source[provider_start:source.index("def get_rift_battle_final_attributes", provider_start)]
    assert "impart_provider=get_rift_battle_impart_data" in source
    assert "def get_rift_battle_final_attributes" in source
    assert "impart_provider=None" in attributes
    assert "impart = impart_provider(user_id) or {}" in attributes
    assert "DatabaseUnitOfWork(database, read_only=True)" in provider
    assert "CREATE TABLE" not in provider
    assert "ALTER TABLE" not in provider
