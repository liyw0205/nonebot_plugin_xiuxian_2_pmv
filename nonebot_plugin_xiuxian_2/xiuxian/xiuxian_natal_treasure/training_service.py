from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class NatalTrainingResult:
    status: str
    exp_added: int
    stone_cost: int
    level: int
    exp: int
    max_exp: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"trained", "duplicate"}


class NatalTrainingService:
    """Charge stones and train a natal treasure across attached databases."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> NatalTrainingResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS natal_training_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, requested_exp INTEGER NOT NULL, "
                "base_cost INTEGER NOT NULL, growth_rate REAL NOT NULL, max_level INTEGER NOT NULL, "
                "max_exp_base INTEGER NOT NULL, max_exp_growth INTEGER NOT NULL, exp_added INTEGER NOT NULL, "
                "stone_cost INTEGER NOT NULL, level INTEGER NOT NULL, exp INTEGER NOT NULL, max_exp INTEGER NOT NULL)"
            )
            previous = conn.execute(
                "SELECT exp_added, stone_cost, level, exp, max_exp FROM natal_training_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return NatalTrainingResult("duplicate", int(previous[0]), int(previous[1]), int(previous[2]), int(previous[3]), int(previous[4]))

    def train(self, operation_id, user_id, requested_exp, *, base_cost, growth_rate,
              max_level, max_exp_base, max_exp_growth) -> NatalTrainingResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        requested_exp = int(requested_exp)
        base_cost = int(base_cost)
        growth_rate = float(growth_rate)
        max_level = int(max_level)
        max_exp_base = int(max_exp_base)
        max_exp_growth = int(max_exp_growth)
        if not operation_id or requested_exp <= 0 or base_cost <= 0 or max_level <= 0:
            raise ValueError("valid operation and positive training parameters are required")

        def result(status, exp_added=0, stone_cost=0, level=0, exp=0, max_exp=0):
            return NatalTrainingResult(
                status, int(exp_added), int(stone_cost), int(level), int(exp), int(max_exp)
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS natal_training_operations ("
                    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, requested_exp INTEGER NOT NULL, "
                    "base_cost INTEGER NOT NULL, growth_rate REAL NOT NULL, max_level INTEGER NOT NULL, "
                    "max_exp_base INTEGER NOT NULL, max_exp_growth INTEGER NOT NULL, exp_added INTEGER NOT NULL, "
                    "stone_cost INTEGER NOT NULL, level INTEGER NOT NULL, exp INTEGER NOT NULL, max_exp INTEGER NOT NULL)"
                )
                conn.execute("CREATE TABLE IF NOT EXISTS player_data.natal_treasure (user_id TEXT PRIMARY KEY)")
                columns = {
                    str(row[1]) for row in conn.execute(
                        "PRAGMA player_data.table_info(natal_treasure)"
                    ).fetchall()
                }
                for field in ("form", "level", "exp", "max_exp"):
                    if field not in columns:
                        conn.execute(
                            f"ALTER TABLE player_data.natal_treasure ADD COLUMN {db_backend.quote_ident(field)} INTEGER"
                        )
                previous = conn.execute(
                    "SELECT user_id, requested_exp, base_cost, growth_rate, max_level, max_exp_base, "
                    "max_exp_growth, exp_added, stone_cost, level, exp, max_exp "
                    "FROM natal_training_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    # Request identity = user + requested_exp; costs/outcomes stored in op row.
                    if str(previous[0]) != user_id or int(previous[1]) != requested_exp:
                        return result("state_changed")
                    return result("duplicate", *previous[7:])

                user = conn.execute(
                    "SELECT COALESCE(stone, 0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                treasure = conn.execute(
                    "SELECT COALESCE(form, 0), COALESCE(level, 0), COALESCE(exp, 0), "
                    "COALESCE(max_exp, %s) FROM player_data.natal_treasure WHERE user_id=%s",
                    (max_exp_base, user_id),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                if treasure is None or int(treasure[0]) == 0:
                    conn.rollback()
                    return result("treasure_missing")
                level, current_exp = int(treasure[1]), int(treasure[2])
                current_max_exp = max_exp_base + level * max_exp_growth
                if level >= max_level:
                    conn.rollback()
                    return result("max_level", level=level, exp=current_exp, max_exp=current_max_exp)
                exp_added = min(requested_exp, max(0, current_max_exp - current_exp))
                if exp_added <= 0:
                    conn.rollback()
                    return result("exp_full", level=level, exp=current_exp, max_exp=current_max_exp)
                stone_cost = int(base_cost * (1 + level * growth_rate)) * exp_added
                if int(user[0]) < stone_cost:
                    conn.rollback()
                    return result("stone_insufficient", exp_added=exp_added, stone_cost=stone_cost,
                                  level=level, exp=current_exp, max_exp=current_max_exp)

                new_exp = current_exp + exp_added
                new_level = level
                new_max_exp = current_max_exp
                if new_exp >= current_max_exp:
                    new_level += 1
                    new_exp -= current_max_exp
                    if new_level < max_level:
                        new_max_exp = max_exp_base + new_level * max_exp_growth
                    else:
                        new_exp = current_max_exp
                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone>=%s",
                    (stone_cost, user_id, stone_cost),
                )
                updated = conn.execute(
                    "UPDATE player_data.natal_treasure SET level=%s, exp=%s, max_exp=%s WHERE user_id=%s",
                    (new_level, new_exp, new_max_exp, user_id),
                )
                if charged.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO natal_training_operations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, requested_exp, base_cost, growth_rate, max_level,
                     max_exp_base, max_exp_growth, exp_added, stone_cost, new_level, new_exp, new_max_exp),
                )
                conn.commit()
                return result("trained", exp_added, stone_cost, new_level, new_exp, new_max_exp)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")


__all__ = ["NatalTrainingResult", "NatalTrainingService"]
