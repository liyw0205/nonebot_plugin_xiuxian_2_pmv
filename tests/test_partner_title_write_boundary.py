import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.title.application import TitleApplication
from nonebot_plugin_xiuxian_2.features.title.migrations import apply_title_schema
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger


def _ready_title_database(database: Path) -> None:
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        OperationLedger().ensure_schema(uow)
        apply_title_schema(uow)


def test_partner_mentor_title_grant_uses_title_application_and_is_idempotent(tmp_path: Path) -> None:
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner

    database = tmp_path / "player.db"
    _ready_title_database(database)
    application = TitleApplication(database)
    with patch.object(partner, "_mentor_title_application_instance", application), patch.object(
        partner, "_get_mentor_title_by_id", return_value={"name": "百炼成师"}
    ):
        assert partner._grant_title_to_user("mentor", "30120") == (True, "已赠送称号【百炼成师】")
        assert partner._grant_title_to_user("mentor", "30120") == (
            False,
            "用户已拥有称号【百炼成师】",
        )

    with sqlite3.connect(database) as connection:
        assert json.loads(
            connection.execute("SELECT unlocked FROM title WHERE user_id='mentor'").fetchone()[0]
        ) == ["30120"]
        assert connection.execute(
            "SELECT COUNT(*) FROM operation_ledger WHERE operation_id=?",
            ("mentor-title:mentor:30120",),
        ).fetchone()[0] == 1


def test_partner_title_writer_no_longer_uses_player_data_manager() -> None:
    source = Path(__file__).parents[1] / (
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_buff/partner.py"
    )
    text = source.read_text(encoding="utf-8")
    assert "_mentor_title_application().grant(" in text
    assert "_player_data_manager().update_or_write_data(" not in text
