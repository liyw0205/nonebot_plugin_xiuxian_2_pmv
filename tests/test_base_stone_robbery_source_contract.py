from pathlib import Path


ROOT = Path(__file__).parents[1]
PACKAGE = ROOT / "nonebot_plugin_xiuxian_2"


def test_default_robbery_handler_uses_application_before_fight():
    source = (PACKAGE / "xiuxian/xiuxian_base/__init__.py").read_text(encoding="utf-8")
    handler = source[source.index("@rob_stone.handle"):source.index("@view_logs.handle")]
    assert "base_application.get_stone_robbery_result(" in handler
    assert "base_application.settle_stone_robbery(" in handler
    assert handler.index("base_application.get_stone_robbery_result(") < handler.index("OtherSet().player_fight(")
    assert "_stone_robbery_service(" not in source
    assert "StoneRobberySettlementService" not in source


def test_robbery_repository_has_no_request_ddl_and_uses_attached_uow():
    repository = (PACKAGE / "features/base/robbery_repository.py").read_text(encoding="utf-8")
    assert "AttachedDatabaseUnitOfWork" in repository
    assert "immediate=True" in repository
    assert "read_only=True" in repository
    assert "CREATE TABLE" not in repository
    assert "ALTER TABLE" not in repository


def test_robbery_migrations_route_receipt_to_game_and_statistics_to_player():
    from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database

    migrations = build_migrations()
    assert "base.004" in {migration.version for migration in migrations_for_database(migrations, "game_db")}
    assert "base.005" in {migration.version for migration in migrations_for_database(migrations, "player_db")}
    for database_key in ("player_db", "trade_db", "impart_db", "message_db"):
        assert "base.004" not in {migration.version for migration in migrations_for_database(migrations, database_key)}
    for database_key in ("game_db", "trade_db", "impart_db", "message_db"):
        assert "base.005" not in {migration.version for migration in migrations_for_database(migrations, database_key)}
