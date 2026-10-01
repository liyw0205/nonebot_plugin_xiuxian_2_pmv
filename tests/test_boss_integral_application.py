import sqlite3
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.boss.integral_application import BossIntegralApplication


def _create_schema(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE boss_limit(user_id TEXT, integral INTEGER NOT NULL)"
        )
        connection.execute("INSERT INTO boss_limit VALUES('u', 20)")
        connection.commit()


def test_boss_integral_grant_is_cas_and_uses_first_duplicate_row(tmp_path: Path) -> None:
    database = tmp_path / "player.sqlite3"
    _create_schema(database)
    with sqlite3.connect(database) as connection:
        connection.execute("INSERT INTO boss_limit VALUES('u', 900)")
        connection.commit()

    application = BossIntegralApplication(database)
    result = application.grant_integral("u", 5)
    assert (result.status, result.applied, result.value) == ("applied", 5, 25)
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT integral FROM boss_limit ORDER BY rowid"
        ).fetchall() == [(25,), (900,)]


def test_boss_integral_fails_closed_without_schema_or_user(tmp_path: Path) -> None:
    missing = tmp_path / "missing.sqlite3"
    assert BossIntegralApplication(missing).grant_integral("u", 1).status == "schema_missing"
    assert not missing.exists()

    database = tmp_path / "ready.sqlite3"
    _create_schema(database)
    assert BossIntegralApplication(database).grant_integral("missing", 1).status == "user_missing"


def test_reward_service_uses_boss_integral_application() -> None:
    source = Path(__file__).parents[1] / (
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/reward_service.py"
    )
    text = source.read_text(encoding="utf-8")
    assert "BossIntegralApplication" in text
    assert "self.boss_integral_application.grant_integral(" in text
    assert "player_data_manager.update_or_write_data" not in text
