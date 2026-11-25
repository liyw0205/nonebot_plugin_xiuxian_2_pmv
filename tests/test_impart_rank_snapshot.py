from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import (
    process_read_cache,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import (
    XIUXIAN_IMPART_BUFF,
)


def test_public_impart_rank_is_a_shared_snapshot_but_core_rank_read_stays_live(tmp_path):
    process_read_cache.clear()
    impart_database = tmp_path / "xiuxian_impart.db"
    game_database = tmp_path / "xiuxian.db"
    impart_database.touch()
    game_database.touch()

    ranks = [{"user_id": "u1", "impart_lv": 10}]
    names = {"u1": "旧道号"}
    rank_reads = []
    name_reads = []

    manager = SimpleNamespace(
        database_path=tmp_path,
        db_file=impart_database,
    )

    def load_rank():
        rank_reads.append(1)
        return [dict(row) for row in ranks]

    manager._load_impart_rank = load_rank
    user_manager = SimpleNamespace(
        get_user_names_by_ids=lambda user_ids: name_reads.append(list(user_ids))
        or {user_id: names[user_id] for user_id in user_ids if user_id in names}
    )

    try:
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle._sql_message",
            return_value=user_manager,
        ):
            first = XIUXIAN_IMPART_BUFF.get_impart_rank_display(manager)
            names["u1"] = "新道号"
            ranks[0]["impart_lv"] = 11
            assert XIUXIAN_IMPART_BUFF.get_impart_rank_display(manager) == first
            assert first == [{"user_id": "u1", "impart_lv": 10, "user_name": "旧道号"}]
            assert rank_reads == [1]
            assert name_reads == [["u1"]]

            assert XIUXIAN_IMPART_BUFF.get_impart_rank(manager) == [
                {"user_id": "u1", "impart_lv": 11}
            ]
            assert len(rank_reads) == 2

            process_read_cache.invalidate_snapshot_database(impart_database)
            refreshed = XIUXIAN_IMPART_BUFF.get_impart_rank_display(manager)
            assert refreshed == [
                {"user_id": "u1", "impart_lv": 11, "user_name": "新道号"}
            ]
            assert len(rank_reads) == 3
            assert len(name_reads) == 2
    finally:
        process_read_cache.clear()
