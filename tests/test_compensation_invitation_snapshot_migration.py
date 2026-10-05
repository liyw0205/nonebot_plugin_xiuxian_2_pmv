from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from nonebot_plugin_xiuxian_2.features.compensation.invitation_repository import (
    InvitationRewardClaimSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.compensation.migrations import (
    apply_compensation_invitation_definition_schema,
    apply_compensation_invitation_reward_schema,
    apply_compensation_invitation_snapshot_migration,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


def _prepared_database(database: Path) -> None:
    with db_backend.transaction(database) as conn:
        conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
        conn.execute("INSERT INTO user_xiuxian VALUES('u1',10)")
        conn.execute(
            "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
            "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
            "bind_num INTEGER,UNIQUE(user_id,goods_id))"
        )
    with DatabaseUnitOfWork(database) as uow:
        apply_compensation_invitation_reward_schema(uow)
        apply_compensation_invitation_definition_schema(uow)


def test_invitation_snapshots_import_once_with_source_hashes() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        database = root / "game.db"
        records = root / "records.json"
        claimed = root / "claimed.json"
        rewards = root / "rewards.json"
        records.write_text(json.dumps({"u1": ["a", "b"]}), encoding="utf-8")
        claimed.write_text(json.dumps({"u1": [1]}), encoding="utf-8")
        rewards.write_text(
            json.dumps({"1": [{"type": "stone", "id": "stone", "name": "灵石", "quantity": 5}]}),
            encoding="utf-8",
        )
        _prepared_database(database)

        with DatabaseUnitOfWork(database) as uow:
            apply_compensation_invitation_snapshot_migration(
                uow, records, claimed, rewards, "2026-10-05 12:00:00"
            )

        repository = InvitationRewardClaimSqlRepository(database)
        assert repository.invitation_count("u1") == 2
        assert repository.claimed_thresholds("u1") == {1}
        assert repository.reward_definitions()["1"][0]["quantity"] == 5
        with db_backend.connection(database) as conn:
            receipt = conn.execute(
                "SELECT records_sha256,claimed_sha256,rewards_sha256 FROM invitation_reward_migrations"
            ).fetchone()
        assert all(len(str(value)) == 64 for value in receipt)

        records.write_text(json.dumps({"u1": ["changed"]}), encoding="utf-8")
        with DatabaseUnitOfWork(database) as uow:
            apply_compensation_invitation_snapshot_migration(
                uow, records, claimed, rewards, "2026-10-06 12:00:00"
            )
        assert repository.invitation_count("u1") == 2


@pytest.mark.parametrize("payload", ["not-json", "[]", " " * (1024 * 1024 + 1)])
def test_invalid_invitation_snapshot_rolls_back_marker(payload: str) -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        database = root / "game.db"
        records = root / "records.json"
        claimed = root / "claimed.json"
        rewards = root / "rewards.json"
        records.write_text("{}", encoding="utf-8")
        claimed.write_text(payload, encoding="utf-8")
        rewards.write_text("{}", encoding="utf-8")
        _prepared_database(database)

        with pytest.raises(ValueError):
            with DatabaseUnitOfWork(database) as uow:
                apply_compensation_invitation_snapshot_migration(
                    uow, records, claimed, rewards
                )

        with db_backend.connection(database) as conn:
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='invitation_reward_migrations'"
            ).fetchone()
        assert table is None
