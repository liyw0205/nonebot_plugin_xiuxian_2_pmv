from __future__ import annotations

import sqlite3
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.pet.application import PetApplication
from nonebot_plugin_xiuxian_2.features.pet.migrations import apply_pet_hatch
from nonebot_plugin_xiuxian_2.features.pet.repository import PetHatchSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema, build_migrations, migrations_for_database


def _prepare_databases(root: Path, *, hatch_schema: bool) -> tuple[Path, Path]:
    game = root / "game.sqlite3"
    player = root / "player.sqlite3"
    with DatabaseUnitOfWork(game) as uow:
        apply_platform_schema(uow)
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('u', 100)")
        if hatch_schema:
            apply_pet_hatch(uow)
    with DatabaseUnitOfWork(player) as uow:
        uow.execute(
            "CREATE TABLE player_pet(user_id TEXT PRIMARY KEY, active_uid TEXT, "
            "egg_pity_count INTEGER, egg_pity_no_mythic_count INTEGER, travel TEXT)"
        )
        uow.execute("INSERT INTO player_pet VALUES('u', '', 0, 0, NULL)")
        uow.execute(
            "CREATE TABLE player_pet_item(id TEXT, user_id TEXT, uid TEXT, is_active INTEGER, "
            "pet_id TEXT, stars INTEGER, exp INTEGER, total_exp INTEGER, skill_id TEXT, "
            "created_at INTEGER, updated_at INTEGER)"
        )
    return game, player


def _pet() -> dict[str, object]:
    return {"uid": "pet-1", "pet_id": "1", "stars": 1, "exp": 0, "total_exp": 0, "skill": {}}


def test_pet_hatch_application_uses_migrated_feature_repository(tmp_path: Path) -> None:
    game, player = _prepare_databases(tmp_path, hatch_schema=True)
    result = PetApplication(game, player).hatch(
        operation_id="hatch-1",
        user_id="u",
        expected_stone=100,
        cost=10,
        expected_meta=["", 0, 0, None],
        pets=[(_pet(), True)],
        updated_meta=["pet-1", 1, 0],
        bag_limit=10,
    )

    assert result.status == "applied"
    assert result.data["pets"][0][0]["uid"] == "pet-1"
    with DatabaseUnitOfWork(game) as uow:
        assert uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"] == 90


def test_pet_hatch_repository_does_not_create_missing_operation_table(tmp_path: Path) -> None:
    game, player = _prepare_databases(tmp_path, hatch_schema=False)
    repository = PetHatchSqlRepository(game, player)

    try:
        repository.hatch("missing-schema", "u", 100, 10, ["", 0, 0, None], [(_pet(), True)], ["pet-1", 1, 0], 10)
    except sqlite3.OperationalError:
        pass
    else:  # pragma: no cover - the repository must fail closed when migration is absent
        raise AssertionError("missing pet.002 schema was silently accepted")

    with DatabaseUnitOfWork(game) as uow:
        assert uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='pet_hatch_operations'"
        ) is None


def test_pet_hatch_migration_is_game_db_only() -> None:
    migrations = build_migrations()
    routed = {
        key: {migration.version for migration in migrations_for_database(migrations, key)}
        for key in ("game_db", "player_db", "trade_db")
    }
    assert "pet.002" in routed["game_db"]
    assert "pet.002" not in routed["player_db"]
    assert "pet.002" not in routed["trade_db"]


def test_pet_egg_handler_uses_application_hatch() -> None:
    source = Path("nonebot_plugin_xiuxian_2/xiuxian/xiuxian_pet/__init__.py").read_text(encoding="utf-8")
    start = source.index("@pet_egg.handle")
    handler = source[start : source.index("@pet_set_active.handle", start)]
    assert "pet_application.hatch(" in handler
    assert "_pet_hatch_service().hatch(" not in handler
