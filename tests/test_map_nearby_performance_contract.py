from __future__ import annotations

import asyncio
from nonebot_plugin_xiuxian_2.features.map.nearby_display_query import select_nearby_display
from nonebot_plugin_xiuxian_2.features.map.nearby_display_repository import (
    MapNearbyDisplaySqlQueryRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


POSITION = {"realm": "realm", "heaven": "heaven", "node_id": "node"}


def _display(repository):
    class Random:
        def randint(self, low, high):
            return high

        def sample(self, values, count):
            return list(values)[:count]

    return asyncio.run(
        select_nearby_display(
            repository,
            **POSITION,
            exclude_user_id="actor",
            random_source=Random(),
        )
    )


def _make_nearby_databases(tmp_path):
    player, game = tmp_path / "player.db", tmp_path / "game.db"
    with DatabaseUnitOfWork(player) as uow:
        uow.execute(
            "CREATE TABLE map_status (user_id TEXT, realm TEXT, heaven TEXT, node_id TEXT)"
        )
        uow.executemany(
            "INSERT INTO map_status VALUES (?, 'realm', 'heaven', 'node')",
            [("actor",), ("first",), ("first",), ("second",), ("orphan",)],
        )
    with DatabaseUnitOfWork(game) as uow:
        uow.execute(
            "CREATE TABLE user_xiuxian (user_id TEXT, user_name TEXT, level TEXT, power)"
        )
        uow.executemany(
            "INSERT INTO user_xiuxian VALUES (?, ?, ?, ?)",
            [
                ("actor", "actor", "筑基", 1),
                ("first", "first", "金丹", 20),
                ("first", "later", "元婴", 99),
                ("second", "second", "元婴", 30),
            ],
        )
    return player, game


def test_optimized_nearby_query_matches_legacy_canonical_output(tmp_path):
    player, game = _make_nearby_databases(tmp_path)
    legacy = _display(MapNearbyDisplaySqlQueryRepository(player, game))
    optimized = _display(MapNearbyDisplaySqlQueryRepository(player, game, optimized=True))
    assert optimized == legacy == [
        {"user_id": "first", "user_name": "first", "level": "金丹", "power": 20},
        {"user_id": "second", "user_name": "second", "level": "元婴", "power": 30},
    ]


def test_optimized_nearby_rejects_invalid_power_without_partial_reply(tmp_path):
    player, game = _make_nearby_databases(tmp_path)
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("UPDATE user_xiuxian SET power=? WHERE user_id='first'", ("not-an-integer",))
    assert _display(MapNearbyDisplaySqlQueryRepository(player, game, optimized=True)) == []


def test_nearby_indexes_are_routed_and_idempotent(tmp_path):
    catalog = build_migrations()
    game_versions = {migration.version for migration in migrations_for_database(catalog, "game_db")}
    player_versions = {migration.version for migration in migrations_for_database(catalog, "player_db")}
    assert "map.019" in game_versions and "map.019" not in player_versions
    assert "map.020" in player_versions and "map.020" not in game_versions

    game = tmp_path / "game.db"
    player = tmp_path / "player.db"
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE user_xiuxian (user_id TEXT, user_name TEXT, level TEXT, power INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES ('u1', 'u1', '筑基', 1)")
        migration = next(item for item in catalog if item.version == "map.019")
        migration.apply(uow)
        migration.apply(uow)
        assert uow.query_one("SELECT COUNT(*) AS count FROM user_xiuxian")["count"] == 1
        assert uow.query_one(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='map_nearby_profile_text_uid'"
        ) is not None
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("CREATE TABLE map_status (user_id TEXT, realm TEXT, heaven TEXT, node_id TEXT)")
        uow.execute("INSERT INTO map_status VALUES ('u1', 'realm', 'heaven', 'node')")
        migration = next(item for item in catalog if item.version == "map.020")
        migration.apply(uow)
        migration.apply(uow)
        assert uow.query_one("SELECT COUNT(*) AS count FROM map_status")["count"] == 1
        assert uow.query_one(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='map_nearby_position_text_uid'"
        ) is not None


def test_nearby_index_explain_uses_both_expression_indexes(tmp_path):
    player, game = _make_nearby_databases(tmp_path)
    with DatabaseUnitOfWork(game) as uow:
        uow.execute(
            "CREATE INDEX map_nearby_profile_text_uid ON user_xiuxian "
            "(CAST(user_id AS TEXT))"
        )
    with DatabaseUnitOfWork(player) as uow:
        uow.execute(
            "CREATE INDEX map_nearby_position_text_uid ON map_status "
            "(realm, heaven, node_id, CAST(user_id AS TEXT))"
        )
        uow.attach_database(f"{game.resolve().as_uri()}?mode=ro", "game_data")
        rows = uow.query_all(
            "EXPLAIN QUERY PLAN SELECT CAST(map.user_id AS TEXT), profile.rowid "
            "FROM map_status AS map JOIN game_data.user_xiuxian AS profile "
            "ON CAST(profile.user_id AS TEXT)=CAST(map.user_id AS TEXT) "
            "WHERE map.realm=? AND map.heaven=? AND map.node_id=? "
            "AND CAST(map.user_id AS TEXT)<>? "
            "ORDER BY CAST(map.user_id AS TEXT) LIMIT 1",
            ("realm", "heaven", "node", "actor"),
        )
    detail = " ".join(str(row["detail"]) for row in rows)
    assert "map_nearby_position_text_uid" in detail
    assert "map_nearby_profile_text_uid" in detail
