from __future__ import annotations

import sqlite3

import pytest

from nonebot_plugin_xiuxian_2.features.base.migrations import (
    apply_base_xiangyuan,
    apply_base_xiangyuan_player,
)
from nonebot_plugin_xiuxian_2.features.base.xiangyuan_repository import XiangyuanSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


@pytest.fixture
def xiangyuan(tmp_path):
    game, player = tmp_path / "game.db", tmp_path / "player.db"
    with DatabaseUnitOfWork(game, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)")
        uow.execute("INSERT INTO user_xiuxian VALUES ('giver', 1000)")
        uow.execute("INSERT INTO user_xiuxian VALUES ('receiver', 0)")
        uow.execute(
            "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
            "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,state INTEGER DEFAULT 0,"
            "UNIQUE(user_id,goods_id))"
        )
        uow.execute("INSERT INTO back VALUES ('giver',101,'符剑','装备',5,'','',0,0)")
        apply_base_xiangyuan(uow)
    with DatabaseUnitOfWork(player, immediate=True) as uow:
        apply_base_xiangyuan_player(uow)
    return game, player, XiangyuanSqlRepository(game, player)


def _create(repository, *, operation_id="gift", quantity=2, stone=100):
    return repository.create(
        operation_id, "group", "giver", "赠礼者", stone,
        [{"goods_id": 101, "name": "符剑", "type": "装备", "quantity": quantity}],
        2, 10,
    )


def _state(game):
    with sqlite3.connect(game) as conn:
        return (
            int(conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='giver'").fetchone()[0]),
            int(conn.execute("SELECT goods_num FROM back WHERE user_id='giver' AND goods_id=101").fetchone()[0]),
            int(conn.execute("SELECT COUNT(*) FROM xiangyuan_gifts").fetchone()[0]),
            int(conn.execute("SELECT next_gift_id FROM xiangyuan_groups WHERE group_id='group'").fetchone()[0]),
        )


def test_clear_all_refunds_remaining_assets_after_partial_claim(xiangyuan):
    game, _, repository = xiangyuan
    assert _create(repository).status == "applied"
    assert repository.claim("claim", "group", 1, "receiver", 30, [101], 10, 10).status == "applied"

    result = repository.clear_all(10)

    assert result == (1, 1, 70, 1)
    assert _state(game) == (970, 4, 0, 1)
    with sqlite3.connect(game) as conn:
        receiver = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id='receiver'").fetchone()[0]
        receiver_item = conn.execute(
            "SELECT goods_num FROM back WHERE user_id='receiver' AND goods_id=101"
        ).fetchone()[0]
        for table in ("xiangyuan_receivers", "xiangyuan_gift_items", "xiangyuan_gifts"):
            assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    assert (receiver, receiver_item) == (30, 1)
    assert repository.clear_all(10) == (0, 0, 0, 0)
    assert _state(game) == (970, 4, 0, 1)


def test_clear_all_rolls_back_every_refund_when_any_inventory_is_full(xiangyuan):
    game, _, repository = xiangyuan
    assert _create(repository, operation_id="gift-1", quantity=1).status == "applied"
    assert _create(repository, operation_id="gift-2", quantity=1).status == "applied"
    before = _state(game)

    with pytest.raises(ValueError, match="inventory_full"):
        repository.clear_all(4)

    assert _state(game) == before
    with sqlite3.connect(game) as conn:
        gift = conn.execute(
            "SELECT remaining_stone FROM xiangyuan_gifts WHERE group_id='group' AND gift_id=1"
        ).fetchone()
        remaining_item = conn.execute(
            "SELECT quantity FROM xiangyuan_gift_items WHERE group_id='group' AND gift_id=1 AND goods_id=101"
        ).fetchone()
    assert (gift[0], remaining_item[0]) == (100, 1)

    assert repository.clear_all(5) == (1, 2, 200, 2)
    assert _state(game) == (1000, 5, 0, 1)


def test_clear_all_empty_and_missing_projection_are_noops(xiangyuan, tmp_path):
    game, player, repository = xiangyuan
    assert repository.clear_all(10) == (0, 0, 0, 0)

    missing = XiangyuanSqlRepository(tmp_path / "missing-game.db", player)
    assert missing.clear_all(10) == (0, 0, 0, 0)
    assert not (tmp_path / "missing-game.db").exists()
    with sqlite3.connect(tmp_path / "missing-game.db"):
        pass
    assert missing.clear_all(10) == (0, 0, 0, 0)
    with sqlite3.connect(tmp_path / "missing-game.db") as conn:
        assert conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == []


@pytest.mark.parametrize("stone", [0, 100])
def test_clear_all_preserves_orphaned_gifts_instead_of_claiming_a_refund(xiangyuan, stone):
    game, _, repository = xiangyuan
    assert _create(repository, stone=stone).status == "applied"
    with sqlite3.connect(game) as conn:
        conn.execute("DELETE FROM user_xiuxian WHERE user_id='giver'")
    with pytest.raises(ValueError, match="user_missing"):
        repository.clear_all(10)
    with sqlite3.connect(game) as conn:
        assert conn.execute("SELECT remaining_stone FROM xiangyuan_gifts").fetchone()[0] == stone
        assert conn.execute("SELECT quantity FROM xiangyuan_gift_items").fetchone()[0] == 2
        assert conn.execute("SELECT goods_num FROM back WHERE user_id='giver'").fetchone()[0] == 3
