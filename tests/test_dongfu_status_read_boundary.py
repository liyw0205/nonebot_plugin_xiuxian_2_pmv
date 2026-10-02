from pathlib import Path


SOURCE = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_dongfu/__init__.py"


def test_dongfu_status_read_uses_feature_projection_without_legacy_writeback():
    source = SOURCE.read_text(encoding="utf-8")
    start = source.index("def _get_dongfu")
    end = source.index("def _save_dongfu", start)
    helper = source[start:end]
    assert "dongfu_application.status(" in helper
    assert "_player_data_manager().get_fields" not in helper
    assert "update_or_write_data" not in helper


def test_dongfu_status_schema_is_player_startup_migration_only():
    from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database

    migrations = build_migrations()
    player = {item.version for item in migrations_for_database(migrations, "player_db")}
    game = {item.version for item in migrations_for_database(migrations, "game_db")}
    assert "map.017" in player
    assert "map.017" not in game
