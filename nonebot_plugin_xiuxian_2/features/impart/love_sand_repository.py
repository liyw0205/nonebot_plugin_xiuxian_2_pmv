from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork
from .migrations import LOVE_SAND_STATISTICS_COLUMNS


@dataclass(frozen=True)
class LoveSandResult:
    status: str
    gained: int = 0
    stone_num: int = 0
    item_remaining: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class LoveSandSqlRepository:
    def __init__(
        self,
        game_database: str | Path,
        impart_database: str | Path,
        player_database: str | Path,
    ) -> None:
        self.game_database = str(game_database)
        self.impart_database = str(impart_database)
        self.player_database = str(player_database)

    def apply(
        self,
        operation_id,
        user_id,
        item_id,
        quantity,
        gained,
        expected_item_count,
        expected_stone_num,
    ) -> LoveSandResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        item_id, quantity, gained, expected_item_count, expected_stone_num = map(
            int,
            (item_id, quantity, gained, expected_item_count, expected_stone_num),
        )
        if not operation_id or quantity <= 0 or gained < 0:
            raise ValueError("invalid love sand request")
        if not all(
            Path(database).is_file()
            for database in (self.game_database, self.impart_database, self.player_database)
        ):
            return LoveSandResult("schema_missing")

        payload = json.dumps([user_id, item_id, quantity], separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.impart_database, "impart_data")
            uow.attach_database(self.player_database, "player_data")
            operation_table = uow.query_one(
                "SELECT 1 AS present FROM sqlite_master "
                "WHERE type='table' AND name='love_sand_operations'"
            )
            player_columns = {
                str(row[1])
                for row in uow.execute("PRAGMA player_data.table_info(statistics)").fetchall()
            }
            if operation_table is None or not {
                "user_id",
                *LOVE_SAND_STATISTICS_COLUMNS,
            }.issubset(player_columns):
                return LoveSandResult("schema_missing")

            old = uow.query_one(
                "SELECT payload,gained,stone_num,item_remaining "
                "FROM love_sand_operations WHERE operation_id=?",
                (operation_id,),
            )
            if old is not None:
                status = "duplicate" if str(old["payload"]) == payload else "operation_conflict"
                return LoveSandResult(
                    status,
                    int(old["gained"]),
                    int(old["stone_num"]),
                    int(old["item_remaining"]),
                )

            item = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num,COALESCE(bind_num,0) AS bind_num "
                "FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            impart = uow.query_one(
                "SELECT stone_num FROM impart_data.xiuxian_impart WHERE user_id=?",
                (user_id,),
            )
            if (
                item is None
                or impart is None
                or int(item["goods_num"]) != expected_item_count
                or int(impart["stone_num"]) != expected_stone_num
            ):
                return LoveSandResult("state_changed")
            if expected_item_count < quantity:
                return LoveSandResult("item_missing")

            remaining = expected_item_count - quantity
            stone_num = expected_stone_num + gained
            consumed = uow.execute(
                "UPDATE back SET goods_num=?,bind_num=? "
                "WHERE user_id=? AND goods_id=? AND COALESCE(goods_num,0)=?",
                (
                    remaining,
                    min(max(0, int(item["bind_num"]) - quantity), remaining),
                    user_id,
                    item_id,
                    expected_item_count,
                ),
            )
            if consumed.rowcount != 1:
                return LoveSandResult("state_changed")
            uow.execute(
                "UPDATE impart_data.xiuxian_impart SET stone_num=? WHERE user_id=?",
                (stone_num, user_id),
            )

            stat_fields = ",".join(f'"{column}"' for column in LOVE_SAND_STATISTICS_COLUMNS)
            stat_updates = ",".join(
                f'"{column}"=COALESCE(statistics."{column}",0)+'
                f'EXCLUDED."{column}"'
                for column in LOVE_SAND_STATISTICS_COLUMNS
            )
            uow.execute(
                f"INSERT INTO player_data.statistics(user_id,{stat_fields}) VALUES(?,?,?) "
                f"ON CONFLICT(user_id) DO UPDATE SET {stat_updates}",
                (user_id, quantity, gained),
            )
            uow.execute(
                "INSERT INTO love_sand_operations "
                "(operation_id,payload,gained,stone_num,item_remaining) VALUES(?,?,?,?,?)",
                (operation_id, payload, gained, stone_num, remaining),
            )
            return LoveSandResult("applied", gained, stone_num, remaining)


__all__ = ["LoveSandSqlRepository", "LoveSandResult"]
