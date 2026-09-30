from pathlib import Path


def test_base_rename_defaults_to_feature_owned_repository():
    source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/features/base/application.py").read_text(encoding="utf-8")
    assert "BaseRenameSqlRepository" in source
    assert "if self.repository is None" in source


def test_base_rename_replay_and_schema_are_feature_owned():
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2"
    facade = (root / "xiuxian/xiuxian_base/__init__.py").read_text(encoding="utf-8")
    repository = (root / "features/base/rename_repository.py").read_text(encoding="utf-8")
    assert "base_application.get_rename_result(" in facade
    assert "_player_rename_service" not in facade
    assert "CREATE TABLE" not in repository
    assert "ALTER TABLE" not in repository
    assert 'Migration("base.002", "player_rename_operations", apply_base_player_rename_operations)' in (
        root / "plugin.py"
    ).read_text(encoding="utf-8")


def test_base_rename_migration_routes_only_to_game_database():
    from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database

    migrations = build_migrations()
    version = "base.002"
    assert version in {migration.version for migration in migrations_for_database(migrations, "game_db")}
    for database_key in ("player_db", "trade_db", "impart_db", "message_db"):
        assert version not in {
            migration.version for migration in migrations_for_database(migrations, database_key)
        }
