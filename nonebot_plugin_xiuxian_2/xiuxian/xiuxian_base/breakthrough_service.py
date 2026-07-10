from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DirectBreakthroughResult:
    status: str
    user_id: str
    outcome: str
    from_level: str = ""
    to_level: str = ""
    exp_loss: int = 0

    @property
    def applied(self) -> bool:
        return self.status == "applied"


class BreakthroughService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS direct_breakthrough_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                outcome TEXT NOT NULL,
                from_level TEXT NOT NULL,
                to_level TEXT NOT NULL,
                exp_loss INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_tribulation_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tribulation_breakthrough_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                outcome TEXT NOT NULL,
                from_level TEXT NOT NULL,
                to_level TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                item_count INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply_failure(
        self,
        operation_id,
        user_id,
        expected_level,
        expected_exp,
        expected_hp,
        expected_mp,
        expected_rate,
        exp_loss,
        new_hp,
        new_mp,
        new_rate,
        *,
        occurred_at: datetime | None = None,
    ) -> DirectBreakthroughResult:
        return self._apply(
            operation_id,
            user_id,
            "failure",
            expected_level,
            expected_level,
            expected_exp,
            expected_hp,
            expected_mp,
            expected_rate,
            exp_loss=max(int(exp_loss), 0),
            new_hp=new_hp,
            new_mp=new_mp,
            new_rate=new_rate,
            occurred_at=occurred_at,
        )

    def apply_success(
        self,
        operation_id,
        user_id,
        expected_level,
        target_level,
        expected_exp,
        expected_hp,
        expected_mp,
        expected_rate,
        root_rate,
        level_spend,
        *,
        occurred_at: datetime | None = None,
    ) -> DirectBreakthroughResult:
        return self._apply(
            operation_id,
            user_id,
            "success",
            expected_level,
            target_level,
            expected_exp,
            expected_hp,
            expected_mp,
            expected_rate,
            root_rate=float(root_rate),
            level_spend=float(level_spend),
            occurred_at=occurred_at,
        )

    def apply_tribulation_failure(
        self,
        operation_id,
        user_id,
        expected_level,
        expected_exp,
        expected_hp,
        expected_mp,
        expected_rate,
        new_rate,
        item_id,
        *,
        occurred_at: datetime | None = None,
    ) -> DirectBreakthroughResult:
        return self._apply_tribulation(
            operation_id,
            user_id,
            "failure",
            expected_level,
            expected_level,
            expected_exp,
            expected_hp,
            expected_mp,
            expected_rate,
            item_id,
            new_rate=new_rate,
            occurred_at=occurred_at,
        )

    def apply_tribulation_success(
        self,
        operation_id,
        user_id,
        expected_level,
        target_level,
        expected_exp,
        expected_hp,
        expected_mp,
        expected_rate,
        root_rate,
        level_spend,
        item_id,
        *,
        occurred_at: datetime | None = None,
    ) -> DirectBreakthroughResult:
        return self._apply_tribulation(
            operation_id,
            user_id,
            "success",
            expected_level,
            target_level,
            expected_exp,
            expected_hp,
            expected_mp,
            expected_rate,
            item_id,
            root_rate=root_rate,
            level_spend=level_spend,
            occurred_at=occurred_at,
        )

    def _apply_tribulation(
        self,
        operation_id,
        user_id,
        outcome,
        expected_level,
        target_level,
        expected_exp,
        expected_hp,
        expected_mp,
        expected_rate,
        item_id,
        *,
        new_rate=0,
        root_rate=0.0,
        level_spend=0.0,
        occurred_at=None,
    ) -> DirectBreakthroughResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        expected_level = str(expected_level)
        target_level = str(target_level)
        expected_exp = int(expected_exp)
        expected_hp = int(expected_hp)
        expected_mp = int(expected_mp)
        expected_rate = int(expected_rate)
        item_id = int(item_id)
        occurred_at = occurred_at or datetime.now()

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_tribulation_operations(conn)
                previous = conn.execute(
                    "SELECT outcome, from_level, to_level FROM "
                    "tribulation_breakthrough_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return DirectBreakthroughResult(
                        "duplicate",
                        user_id,
                        str(previous[0]),
                        str(previous[1]),
                        str(previous[2]),
                    )

                user = conn.execute(
                    "SELECT level, exp, hp, mp, level_up_rate "
                    "FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return DirectBreakthroughResult("user_missing", user_id, outcome)
                if (
                    str(user[0]) != expected_level
                    or int(user[1] or 0) != expected_exp
                    or int(user[2] or 0) != expected_hp
                    or int(user[3] or 0) != expected_mp
                    or int(user[4] or 0) != expected_rate
                ):
                    conn.rollback()
                    return DirectBreakthroughResult(
                        "state_changed", user_id, outcome, str(user[0]), str(user[0])
                    )

                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None or int(item[0] or 0) < 1:
                    conn.rollback()
                    return DirectBreakthroughResult(
                        "item_missing", user_id, outcome, expected_level, target_level
                    )

                if outcome == "failure":
                    conn.execute(
                        "UPDATE user_xiuxian SET level_up_rate=%s, level_up_cd=%s "
                        "WHERE user_id=%s",
                        (int(new_rate), occurred_at, user_id),
                    )
                elif outcome == "success":
                    conn.execute(
                        "UPDATE user_xiuxian SET level=%s, power=ROUND(exp*%s*%s, 0), "
                        "level_up_cd=%s, level_up_rate=0, hp=exp/2, mp=exp, atk=exp/10 "
                        "WHERE user_id=%s",
                        (
                            target_level,
                            float(root_rate),
                            float(level_spend),
                            occurred_at,
                            user_id,
                        ),
                    )
                else:
                    raise ValueError(f"unsupported breakthrough outcome: {outcome}")

                consumed = conn.execute(
                    "UPDATE back SET update_time=%s, action_time=%s, "
                    "day_num=CASE WHEN goods_type='丹药' THEN COALESCE(day_num, 0)+1 "
                    "ELSE COALESCE(day_num, 0) END, "
                    "all_num=CASE WHEN goods_type='丹药' THEN COALESCE(all_num, 0)+1 "
                    "ELSE COALESCE(all_num, 0) END, goods_num=goods_num-1, "
                    "bind_num=MIN(COALESCE(bind_num, 0), goods_num-1) "
                    "WHERE user_id=%s AND goods_id=%s AND COALESCE(goods_num, 0)>=1",
                    (occurred_at, occurred_at, user_id, item_id),
                )
                if consumed.rowcount != 1:
                    raise db_backend.IntegrityError("tribulation item changed concurrently")
                conn.execute(
                    "INSERT INTO tribulation_breakthrough_operations "
                    "(operation_id, user_id, outcome, from_level, to_level, "
                    "item_id, item_count) VALUES (%s, %s, %s, %s, %s, %s, 1)",
                    (
                        operation_id,
                        user_id,
                        outcome,
                        expected_level,
                        target_level,
                        item_id,
                    ),
                )
                conn.commit()
                return DirectBreakthroughResult(
                    "applied", user_id, outcome, expected_level, target_level
                )
            except Exception:
                conn.rollback()
                raise

    def _apply(
        self,
        operation_id,
        user_id,
        outcome,
        expected_level,
        target_level,
        expected_exp,
        expected_hp,
        expected_mp,
        expected_rate,
        *,
        exp_loss=0,
        new_hp=0,
        new_mp=0,
        new_rate=0,
        root_rate=0.0,
        level_spend=0.0,
        occurred_at=None,
    ) -> DirectBreakthroughResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        expected_level = str(expected_level)
        target_level = str(target_level)
        expected_exp = int(expected_exp)
        expected_hp = int(expected_hp)
        expected_mp = int(expected_mp)
        expected_rate = int(expected_rate)
        occurred_at = occurred_at or datetime.now()

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT outcome, from_level, to_level, exp_loss "
                    "FROM direct_breakthrough_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return DirectBreakthroughResult(
                        "duplicate",
                        user_id,
                        str(previous[0]),
                        str(previous[1]),
                        str(previous[2]),
                        int(previous[3]),
                    )

                user = conn.execute(
                    "SELECT level, exp, hp, mp, level_up_rate "
                    "FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return DirectBreakthroughResult("user_missing", user_id, outcome)
                if (
                    str(user[0]) != expected_level
                    or int(user[1] or 0) != expected_exp
                    or int(user[2] or 0) != expected_hp
                    or int(user[3] or 0) != expected_mp
                    or int(user[4] or 0) != expected_rate
                ):
                    conn.rollback()
                    return DirectBreakthroughResult(
                        "state_changed", user_id, outcome, str(user[0]), str(user[0])
                    )

                if outcome == "failure":
                    conn.execute(
                        "UPDATE user_xiuxian SET exp=MAX(exp-%s, 0), hp=%s, mp=%s, "
                        "level_up_rate=%s, level_up_cd=%s WHERE user_id=%s",
                        (
                            int(exp_loss),
                            int(new_hp),
                            int(new_mp),
                            int(new_rate),
                            occurred_at,
                            user_id,
                        ),
                    )
                elif outcome == "success":
                    conn.execute(
                        "UPDATE user_xiuxian SET level=%s, power=ROUND(exp*%s*%s, 0), "
                        "level_up_cd=%s, level_up_rate=0, hp=exp/2, mp=exp, atk=exp/10 "
                        "WHERE user_id=%s",
                        (
                            target_level,
                            root_rate,
                            level_spend,
                            occurred_at,
                            user_id,
                        ),
                    )
                else:
                    raise ValueError(f"unsupported breakthrough outcome: {outcome}")

                conn.execute(
                    "INSERT INTO direct_breakthrough_operations "
                    "(operation_id, user_id, outcome, from_level, to_level, exp_loss) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        user_id,
                        outcome,
                        expected_level,
                        target_level,
                        int(exp_loss),
                    ),
                )
                conn.commit()
                return DirectBreakthroughResult(
                    "applied",
                    user_id,
                    outcome,
                    expected_level,
                    target_level,
                    int(exp_loss),
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["BreakthroughService", "DirectBreakthroughResult"]
