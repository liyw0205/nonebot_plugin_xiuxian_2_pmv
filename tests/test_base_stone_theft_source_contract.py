from pathlib import Path


ROOT = Path(__file__).parents[1]
PACKAGE = ROOT / "nonebot_plugin_xiuxian_2"


def test_default_theft_handler_uses_application_before_random_resolution():
    source = (PACKAGE / "xiuxian/xiuxian_base/__init__.py").read_text(encoding="utf-8")
    handler = source[source.index("@steal_stone.handle"):source.index("@rob_stone.handle")]
    assert "base_application.get_stone_theft_result(" in handler
    assert "base_application.settle_stone_theft(" in handler
    assert handler.index("base_application.get_stone_theft_result(") < handler.index("random.randint(")
    assert "_stone_contest_service(" not in source
    assert "StoneContestService" not in source


def test_theft_repository_is_no_ddl_and_migration_is_game_only():
    repository = (PACKAGE / "features/base/theft_repository.py").read_text(encoding="utf-8")
    plugin = (PACKAGE / "plugin.py").read_text(encoding="utf-8")
    assert "immediate=True" in repository
    assert "CREATE TABLE" not in repository
    assert "ALTER TABLE" not in repository
    assert 'Migration("base.003", "stone_contest_operations", apply_base_stone_contest_operations)' in plugin


def test_base_manifest_tracks_latest_registered_migration():
    manifest = (PACKAGE / "features/base/manifest.py").read_text(encoding="utf-8")
    assert 'migration_version="base.003"' in manifest


def test_stone_contest_migration_routes_only_to_game_database():
    from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database

    migrations = build_migrations()
    version = "base.003"
    assert version in {migration.version for migration in migrations_for_database(migrations, "game_db")}
    for database_key in ("player_db", "trade_db", "impart_db", "message_db"):
        assert version not in {
            migration.version for migration in migrations_for_database(migrations, database_key)
        }
