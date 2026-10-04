from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True, slots=True)
class BotOverviewSnapshot:
    total_users: int | None
    today_active_users: int | None
    yesterday_active_users: int | None
    last_7days_active_users: int | None
    total_items_quantity: int | float | None
    total_goods_quantity: int | None


class BotOverviewSqlRepository:
    """Read bot-wide counters without creating schema or retaining cached results."""

    def __init__(self, game_database: str | Path, trade_database: str | Path) -> None:
        self.game_database = Path(game_database)
        self.trade_database = Path(trade_database)

    @staticmethod
    def _table_columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {
            str(row["name"])
            for row in uow.query_all(f'PRAGMA table_info("{table}")')
        }

    @classmethod
    def _has_columns(
        cls,
        uow: DatabaseUnitOfWork,
        required: dict[str, set[str]],
    ) -> bool:
        for table, columns in required.items():
            actual_columns = cls._table_columns(uow, table)
            if not actual_columns or not columns <= actual_columns:
                return False
        return True

    @staticmethod
    def _scalar(uow: DatabaseUnitOfWork, sql: str, params: tuple = ()):
        row = uow.execute(sql, params).fetchone()
        return row[0] if row is not None else None

    def snapshot(self, *, now: datetime | None = None) -> BotOverviewSnapshot:
        current = now or datetime.now()
        game_stats = self._game_stats(current)
        trade_total = self._trade_total()
        if game_stats is None:
            game_stats = (None, None, None, None, None)
        return BotOverviewSnapshot(*game_stats, trade_total)

    def _game_stats(self, now: datetime) -> tuple[int, int, int, int, int | float] | None:
        if not self.game_database.is_file():
            return None
        required = {
            "user_xiuxian": set(),
            "user_cd": {"user_id", "create_time"},
            "back": {"goods_num"},
        }
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            if not self._has_columns(uow, required):
                return None
            today = now.strftime("%Y-%m-%d")
            yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
            seven_days_ago = (now - timedelta(days=6)).strftime("%Y-%m-%d")
            create_date = "substr(CAST(create_time AS TEXT), 1, 10)"
            total_users = self._scalar(uow, "SELECT COUNT(*) FROM user_xiuxian")
            active_today = self._scalar(
                uow,
                f"SELECT COUNT(DISTINCT user_id) FROM user_cd WHERE {create_date} = ?",
                (today,),
            )
            active_yesterday = self._scalar(
                uow,
                f"SELECT COUNT(DISTINCT user_id) FROM user_cd WHERE {create_date} = ?",
                (yesterday,),
            )
            active_last_week = self._scalar(
                uow,
                f"SELECT COUNT(DISTINCT user_id) FROM user_cd WHERE {create_date} >= ?",
                (seven_days_ago,),
            )
            total_items = self._scalar(uow, "SELECT SUM(goods_num) FROM back") or 0
        return (
            int(total_users or 0),
            int(active_today or 0),
            int(active_yesterday or 0),
            int(active_last_week or 0),
            total_items,
        )

    def _trade_total(self) -> int | None:
        if not self.trade_database.is_file():
            return None
        required = {
            "xianshi_item": {"user_id", "quantity"},
            "guishi_item": {"user_id", "item_type", "quantity"},
        }
        with DatabaseUnitOfWork(self.trade_database, read_only=True) as uow:
            if not self._has_columns(uow, required):
                return None
            row = uow.query_one(
                "SELECT SUM(quantity) AS total_quantity FROM ("
                "SELECT quantity FROM xianshi_item "
                "WHERE user_id != '0' AND quantity > 0 "
                "UNION ALL "
                "SELECT quantity FROM guishi_item "
                "WHERE user_id != '0' AND (item_type='baitan' OR item_type='摆摊') "
                "AND quantity > 0)"
            )
        return int((row or {}).get("total_quantity") or 0)


__all__ = ["BotOverviewSnapshot", "BotOverviewSqlRepository"]
