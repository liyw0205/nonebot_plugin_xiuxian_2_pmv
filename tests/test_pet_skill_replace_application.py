from __future__ import annotations

import sqlite3
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.pet.application import PetApplication
from nonebot_plugin_xiuxian_2.features.pet.migrations import apply_pet_skill_replace
from nonebot_plugin_xiuxian_2.features.pet.repository import PetSkillReplaceSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


def _prepare_player(root: Path, *, operation_schema: bool) -> Path:
    database = root / "player.sqlite3"
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(
            "CREATE TABLE player_pet_item(user_id TEXT, uid TEXT, skill_id TEXT, "
            "updated_at INTEGER, PRIMARY KEY(user_id, uid))"
        )
        uow.execute("INSERT INTO player_pet_item VALUES('u', 'pet-1', 'old', 0)")
        if operation_schema:
            apply_pet_skill_replace(uow)
    return database


def test_skill_replace_application_replays_and_updates_snapshot(tmp_path: Path) -> None:
    player = _prepare_player(tmp_path, operation_schema=True)
    application = PetApplication(tmp_path / "game.sqlite3", player)

    first = application.skill_replace(
        operation_id="replace-1",
        user_id="u",
        uid="pet-1",
        expected_skill_id="old",
        new_skill_id="new",
    )
    replay = application.skill_replace(
        operation_id="replace-1",
        user_id="u",
        uid="pet-1",
        expected_skill_id="old",
        new_skill_id="new",
    )

    assert (first.status, replay.status, replay.skill_id) == ("applied", "duplicate", "new")
    conflict = application.skill_replace(
        operation_id="replace-1",
        user_id="u",
        uid="pet-1",
        expected_skill_id="old",
        new_skill_id="newer",
    )
    assert conflict.status == "state_changed"
    with DatabaseUnitOfWork(player) as uow:
        assert uow.query_one("SELECT skill_id FROM player_pet_item WHERE user_id='u' AND uid='pet-1'")["skill_id"] == "new"


def test_skill_replace_repository_fails_closed_without_migration(tmp_path: Path) -> None:
    player = _prepare_player(tmp_path, operation_schema=False)
    repository = PetSkillReplaceSqlRepository(player)

    try:
        repository.replace("missing-schema", "u", "pet-1", "old", "new")
    except sqlite3.OperationalError:
        pass
    else:  # pragma: no cover - missing schema must be surfaced to startup/deployment
        raise AssertionError("missing pet.003 schema was silently accepted")

    with DatabaseUnitOfWork(player) as uow:
        assert uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' "
            "AND name='pet_skill_replace_operations'"
        ) is None


def test_pet_skill_replace_migration_is_player_db_only() -> None:
    migrations = build_migrations()
    routed = {
        key: {migration.version for migration in migrations_for_database(migrations, key)}
        for key in ("game_db", "player_db", "trade_db")
    }
    assert "pet.003" in routed["player_db"]
    assert "pet.003" not in routed["game_db"]
    assert "pet.003" not in routed["trade_db"]


def test_pet_replace_handler_uses_feature_application() -> None:
    source = Path("nonebot_plugin_xiuxian_2/xiuxian/xiuxian_pet/__init__.py").read_text(encoding="utf-8")
    start = source.index("@pet_replace_skill.handle")
    handler = source[start : source.index("@pet_keep_skill.handle", start)]
    assert "pet_application.skill_replace(" in handler
    assert "_pet_skill_replace_service().replace(" not in handler
