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


def test_infiltration_eligibility_checks_do_not_write_legacy_projection():
    source = SOURCE.read_text(encoding="utf-8")
    for name, end in (("_can_infiltrate", "def _consume_infiltrate_count"), ("_can_intrude", "def _get_random_dongfu_target")):
        helper = source[source.index(f"def {name}"):source.index(end)]
        assert "_save_dongfu" not in helper


def test_my_dongfu_display_does_not_persist_derived_daily_counters():
    source = SOURCE.read_text(encoding="utf-8")
    start = source.index("@my_dongfu.handle")
    end = source.index("@dongfu_plant.handle", start)
    handler = source[start:end]
    assert "_reset_intrude_count_if_needed(d)" in handler
    assert "_reset_infiltrate_count_if_needed(d)" in handler
    assert "_reset_patrol_count_if_needed(d)" in handler
    assert "_save_dongfu" not in handler


def test_expansion_handler_does_not_write_legacy_projection_after_repository_commit():
    source = SOURCE.read_text(encoding="utf-8")
    start = source.index("@dongfu_expand.handle")
    end = source.index("@visit_friend.handle", start)
    assert "dongfu_application.expand(" in source[start:end]
    assert "_save_dongfu" not in source[start:end]
