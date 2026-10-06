from __future__ import annotations

import asyncio
from pathlib import Path

from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.paths import configure_paths, get_paths
from nonebot_plugin_xiuxian_2.plugin import (
    build_lifecycle,
    build_migrations,
    migrations_for_database,
)
from tests.bootstrap import copy_static_data


def test_blackhouse_migration_is_in_the_game_only_startup_catalog():
    migrations = build_migrations()
    for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db"):
        versions = [item.version for item in migrations_for_database(migrations, key)]
        assert versions.count("legacy.admin.007") == (1 if key == "game_db" else 0)


def test_lifecycle_imports_blackhouse_once_into_game_database(tmp_path):
    previous_data = get_paths().data
    data = copy_static_data(Path(__file__).resolve().parents[1] / "data/xiuxian", tmp_path / "data")
    legacy = data / "blackhouse.json"
    original = b'{"users":{"legacy-id":{"name":"Guest","reason":"legacy"}}}'
    legacy.write_bytes(original)
    lifecycle, _, context = build_lifecycle(build_runtime_context(data_dir=data, legacy_startup=False))

    async def verify():
        try:
            state = await lifecycle.start()
            assert state.phase.value == "ready", state.error
            for spec in context.database.specs():
                with DatabaseUnitOfWork(spec.path, read_only=True) as uow:
                    tables = {row["name"] for row in uow.query_all(
                        "SELECT name FROM sqlite_master WHERE name LIKE 'admin_blackhouse_%'"
                    )}
                    if spec.key == "game_db":
                        assert tables == {
                            "admin_blackhouse_users", "admin_blackhouse_status_operations", "admin_blackhouse_imports",
                        }
                        assert uow.query_one("SELECT user_id,name,reason FROM admin_blackhouse_users") == {
                            "user_id": "legacy-id", "name": "Guest", "reason": "legacy",
                        }
                        assert uow.query_one(
                            "SELECT version FROM schema_migrations WHERE version='legacy.admin.007'"
                        ) is not None
                    else:
                        assert tables == set()
            assert legacy.read_bytes() == original
        finally:
            await lifecycle.shutdown()

    try:
        asyncio.run(verify())
    finally:
        configure_paths(previous_data)
