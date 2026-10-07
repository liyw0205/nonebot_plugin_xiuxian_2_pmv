from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ..command_application import BegCommandApplication
from .test_beg_application import BegApplicationTest


@pytest.fixture
def command(tmp_path):
    database = BegApplicationTest._database(str(tmp_path))
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,last_check_info_time TEXT)")
        uow.execute("INSERT INTO user_cd VALUES('u','old')")
    config = SimpleNamespace(beg_max_days=7, beg_max_level="limit", beg_lingshi_lower_limit=10,
                             beg_lingshi_upper_limit=30, max_goods_num=10)
    return BegCommandApplication(
        database, Mock(return_value=config), Mock(return_value={"练气境初期": {}, "limit": {}}),
        Mock(return_value={"name_1": "灵石", "amount_1": 500}),
        clock=SimpleNamespace(now=lambda: datetime(2026, 9, 13, 8, 30)),
        rng=SimpleNamespace(randint=lambda lower, upper: 25),
    )


def query(command, sql):
    with DatabaseUnitOfWork(command.repository.database, read_only=True) as uow:
        return uow.query_all(sql)


def test_daily_activity_uses_shared_owner_and_replay_does_not_touch_it(command):
    first = command.execute(action="daily_settle", operation_id="event", user_id="u")
    assert first["status"] == "applied"
    assert query(command, "SELECT last_check_info_time FROM user_cd") == [{"last_check_info_time": "2026-09-13 08:30:00"}]
    with patch.object(command.activity, "update_last_check_info_time", side_effect=AssertionError("activity on replay")):
        assert command.execute(action="daily_settle", operation_id="event", user_id="u")["status"] == "duplicate"


@pytest.mark.parametrize("gift", [None, {}, {"name_1": "item"}, {"name_1": "灵石", "amount_1": -1},
                                  {"name_1": "灵石", "amount_1": 1.5}])
def test_invalid_catalog_cannot_partially_grant_or_mark_claimed(command, gift):
    command.gift_provider.return_value = gift
    with patch.object(command.application, "execute", side_effect=AssertionError("invalid catalog reached writer")):
        assert command.execute(action="novice_claim", operation_id="bad", user_id="u")["status"] == "config_invalid"
    assert query(command, "SELECT stone,is_novice FROM user_xiuxian") == [{"stone": 100, "is_novice": 0}]
    assert query(command, "SELECT * FROM operation_ledger") == []


def test_catalog_repeated_items_are_aggregated_before_capacity_check(command):
    command.gift_provider.return_value = {
        "name_1": "book", "type_1": "功法", "buff_1": 123, "amount_1": 6,
        "name_2": "book", "type_2": "功法", "buff_2": 123, "amount_2": 6,
    }
    result = command.execute(action="novice_claim", operation_id="full", user_id="u")
    assert result["status"] == "inventory_full"
    assert query(command, "SELECT * FROM back") == []
    assert query(command, "SELECT stone,is_novice FROM user_xiuxian") == [{"stone": 100, "is_novice": 0}]


def test_two_commands_reading_same_profile_only_pay_one_daily_reward(command):
    original = command.repository.profile
    competing = BegCommandApplication(
        command.repository.database, command.config_provider, command.levels_provider,
        command.gift_provider, clock=command.clock, rng=command.rng,
    )

    def settle_after_read(user_id):
        snapshot = original(user_id)
        assert competing.execute(action="daily_settle", operation_id="winner", user_id=user_id)["status"] == "applied"
        return snapshot

    with patch.object(command.repository, "profile", side_effect=settle_after_read):
        result = command.execute(action="daily_settle", operation_id="stale", user_id="u")
    assert result["status"] == "state_changed"
    assert query(command, "SELECT stone,is_beg FROM user_xiuxian") == [{"stone": 125, "is_beg": 1}]
    assert query(command, "SELECT operation_id FROM beg_daily_reward_operations") == [{"operation_id": "winner"}]


def test_future_creation_time_is_rejected_before_activity_or_writer(command):
    command.clock = SimpleNamespace(now=lambda: datetime(2026, 9, 10))
    assert command.execute(action="daily_settle", operation_id="future", user_id="u")["status"] == "profile_invalid"
    assert query(command, "SELECT last_check_info_time FROM user_cd") == [{"last_check_info_time": "old"}]
    assert query(command, "SELECT * FROM operation_ledger") == []
