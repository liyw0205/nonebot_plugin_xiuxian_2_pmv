from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DongfuExpansion:
    status: str
    user_id: str
    previous_count: int = 0
    current_count: int = 0
    deed_cost: int = 0
    stone_cost: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"expanded", "duplicate"}


class DongfuExpansionService:
    """Expand dongfu plots while charging game and player assets atomically."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _as_int(value: object) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dongfu_expansion_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                previous_count INTEGER NOT NULL,
                current_count INTEGER NOT NULL,
                deed_cost INTEGER NOT NULL,
                stone_cost INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def expand(
        self,
        operation_id,
        user_id,
        *,
        deed_id: int,
        base_plot_count: int,
        max_plot_count: int,
        stone_cost_per_level: int,
    ) -> DongfuExpansion:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    """
                    SELECT previous_count, current_count, deed_cost, stone_cost
                    FROM dongfu_expansion_operations WHERE operation_id=%s
                    """,
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return DongfuExpansion(
                        "duplicate",
                        user_id,
                        self._as_int(previous[0]),
                        self._as_int(previous[1]),
                        self._as_int(previous[2]),
                        self._as_int(previous[3]),
                    )

                user = conn.execute(
                    "SELECT stone FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return DongfuExpansion("user_missing", user_id)

                dongfu = conn.execute(
                    'SELECT built, plot_count FROM player_data."dongfu_status" WHERE user_id=%s',
                    (user_id,),
                ).fetchone()
                if dongfu is None or self._as_int(dongfu[0]) != 1:
                    conn.rollback()
                    return DongfuExpansion("dongfu_missing", user_id)

                previous_count = max(base_plot_count, self._as_int(dongfu[1]))
                if previous_count >= max_plot_count:
                    conn.rollback()
                    return DongfuExpansion(
                        "max_plots", user_id, previous_count, previous_count
                    )

                current_count = previous_count + 1
                deed_cost = current_count - base_plot_count
                stone_cost = stone_cost_per_level * deed_cost
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, int(deed_id)),
                ).fetchone()
                if item is None or self._as_int(item[0]) < deed_cost:
                    conn.rollback()
                    return DongfuExpansion(
                        "deed_insufficient",
                        user_id,
                        previous_count,
                        previous_count,
                        deed_cost,
                        stone_cost,
                    )
                if self._as_int(user[0]) < stone_cost:
                    conn.rollback()
                    return DongfuExpansion(
                        "stone_insufficient",
                        user_id,
                        previous_count,
                        previous_count,
                        deed_cost,
                        stone_cost,
                    )

                deducted_item = conn.execute(
                    """
                    UPDATE back SET goods_num=goods_num-%s
                    WHERE user_id=%s AND goods_id=%s AND goods_num >= %s
                    """,
                    (deed_cost, user_id, int(deed_id), deed_cost),
                )
                deducted_stone = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone >= %s",
                    (stone_cost, user_id, stone_cost),
                )
                updated_dongfu = conn.execute(
                    """
                    UPDATE player_data."dongfu_status" SET plot_count=%s
                    WHERE user_id=%s AND CAST(plot_count AS INTEGER)=%s
                    """,
                    (str(current_count), user_id, previous_count),
                )
                if deducted_item.rowcount != 1:
                    conn.rollback()
                    return DongfuExpansion("deed_changed", user_id, previous_count, previous_count)
                if deducted_stone.rowcount != 1:
                    conn.rollback()
                    return DongfuExpansion("stone_changed", user_id, previous_count, previous_count)
                if updated_dongfu.rowcount != 1:
                    conn.rollback()
                    return DongfuExpansion("dongfu_changed", user_id, previous_count, previous_count)

                conn.execute(
                    """
                    INSERT INTO dongfu_expansion_operations (
                        operation_id, user_id, previous_count, current_count, deed_cost, stone_cost
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (operation_id, user_id, previous_count, current_count, deed_cost, stone_cost),
                )
                conn.commit()
                return DongfuExpansion(
                    "expanded", user_id, previous_count, current_count, deed_cost, stone_cost
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")


__all__ = ["DongfuExpansion", "DongfuExpansionService"]
