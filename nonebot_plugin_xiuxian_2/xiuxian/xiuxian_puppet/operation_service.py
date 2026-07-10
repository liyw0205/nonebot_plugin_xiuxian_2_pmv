from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PuppetOperation:
    status: str
    user_id: str
    action: str
    previous_level: int = 0
    current_level: int = 0
    stone_cost: int = 0

    @property
    def applied(self) -> bool:
        return self.status in {"purchased", "upgraded"}

    @property
    def succeeded(self) -> bool:
        return self.status in {"purchased", "upgraded", "duplicate"}


class PuppetOperationService:
    """Apply puppet purchases and upgrades across game and player data atomically."""

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
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS puppet_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                action TEXT NOT NULL,
                previous_level INTEGER NOT NULL,
                current_level INTEGER NOT NULL,
                stone_cost INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _as_int(value: object) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def purchase(self, operation_id, user_id, stone_cost: int) -> PuppetOperation:
        return self._apply(
            operation_id,
            user_id,
            action="purchase",
            costs={0: int(stone_cost)},
            max_level=1,
        )

    def upgrade(
        self,
        operation_id,
        user_id,
        upgrade_costs: dict[int, int],
        *,
        max_level: int,
    ) -> PuppetOperation:
        return self._apply(
            operation_id,
            user_id,
            action="upgrade",
            costs={int(level): int(cost) for level, cost in upgrade_costs.items()},
            max_level=int(max_level),
        )

    def _apply(
        self,
        operation_id,
        user_id,
        *,
        action: str,
        costs: dict[int, int],
        max_level: int,
    ) -> PuppetOperation:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "ATTACH DATABASE %s AS player_data", (str(self._player_database),)
            )
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    """
                    SELECT action, previous_level, current_level, stone_cost
                    FROM puppet_operations WHERE operation_id=%s
                    """,
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return PuppetOperation(
                        "duplicate",
                        user_id,
                        str(previous[0]),
                        self._as_int(previous[1]),
                        self._as_int(previous[2]),
                        self._as_int(previous[3]),
                    )

                user = conn.execute(
                    "SELECT stone, blessed_spot_flag FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return PuppetOperation("user_missing", user_id, action)
                if self._as_int(user[1]) == 0:
                    conn.rollback()
                    return PuppetOperation("blessed_spot_missing", user_id, action)

                player = conn.execute(
                    """
                    SELECT "灵田傀儡" FROM player_data."mix_elixir_info"
                    WHERE user_id=%s
                    """,
                    (user_id,),
                ).fetchone()
                if player is None:
                    conn.rollback()
                    return PuppetOperation("player_info_missing", user_id, action)
                previous_level = self._as_int(player[0])

                if action == "purchase":
                    if previous_level > 0:
                        conn.rollback()
                        return PuppetOperation(
                            "already_owned", user_id, action, previous_level, previous_level
                        )
                    current_level = 1
                else:
                    if previous_level <= 0:
                        conn.rollback()
                        return PuppetOperation("puppet_missing", user_id, action)
                    if previous_level >= max_level:
                        conn.rollback()
                        return PuppetOperation(
                            "max_level", user_id, action, previous_level, previous_level
                        )
                    current_level = previous_level + 1

                stone_cost = costs.get(previous_level)
                if stone_cost is None or stone_cost < 0:
                    conn.rollback()
                    return PuppetOperation(
                        "invalid_puppet_level", user_id, action, previous_level, previous_level
                    )
                if self._as_int(user[0]) < stone_cost:
                    conn.rollback()
                    return PuppetOperation(
                        "stone_insufficient",
                        user_id,
                        action,
                        previous_level,
                        previous_level,
                        stone_cost,
                    )

                deducted = conn.execute(
                    """
                    UPDATE user_xiuxian SET stone=stone-%s
                    WHERE user_id=%s AND stone >= %s
                    """,
                    (stone_cost, user_id, stone_cost),
                )
                if deducted.rowcount != 1:
                    conn.rollback()
                    return PuppetOperation(
                        "stone_changed",
                        user_id,
                        action,
                        previous_level,
                        previous_level,
                        stone_cost,
                    )
                updated = conn.execute(
                    """
                    UPDATE player_data."mix_elixir_info" SET "灵田傀儡"=%s
                    WHERE user_id=%s AND "灵田傀儡"=%s
                    """,
                    (str(current_level), user_id, str(previous_level)),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    return PuppetOperation(
                        "puppet_level_changed",
                        user_id,
                        action,
                        previous_level,
                        previous_level,
                        stone_cost,
                    )
                conn.execute(
                    """
                    INSERT INTO puppet_operations (
                        operation_id, user_id, action, previous_level, current_level,
                        stone_cost
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        operation_id,
                        user_id,
                        action,
                        previous_level,
                        current_level,
                        stone_cost,
                    ),
                )
                conn.commit()
                return PuppetOperation(
                    "purchased" if action == "purchase" else "upgraded",
                    user_id,
                    action,
                    previous_level,
                    current_level,
                    stone_cost,
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")


__all__ = ["PuppetOperation", "PuppetOperationService"]
