from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
import random
from typing import Callable
from ..xiuxian_utils import db_backend
from ..xiuxian_buff.relation_transaction_utils import increment_stat
from ...compatibility.legacy_base_lottery import (
    LotteryPoolSnapshot,
    LotterySettlementResult,
    LotterySettlementService,
    LotteryWinner,
)
from ...compatibility.legacy_base_player_rename import (
    PlayerRenameResult,
    PlayerRenameService,
)
from ...compatibility.legacy_base_stone_contest import (
    StoneContestResult,
    StoneContestService,
    StoneTheftResult,
)
from ...compatibility.legacy_base_stone_robbery import (
    StoneRobberyResult,
    StoneRobberySettlementService,
)
from ...compatibility.legacy_base_xiangyuan import (
    XiangyuanClaimResult,
    XiangyuanCreateResult,
    XiangyuanSettlementService,
)
from datetime import date, datetime
from datetime import datetime


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

@dataclass(frozen=True)
class ContinuousBreakthroughResult:
    status: str
    user_id: str
    from_level: str
    to_level: str
    attempts: int
    fail_count: int
    exp_loss: int

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

    @staticmethod
    def _ensure_continuous_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS continuous_breakthrough_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                from_level TEXT NOT NULL,
                to_level TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                fail_count INTEGER NOT NULL,
                exp_loss INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _ensure_continuous_tribulation_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS continuous_tribulation_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                from_level TEXT NOT NULL,
                to_level TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                fail_count INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                item_count INTEGER NOT NULL,
                exp_gain INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply_continuous_tribulation(
        self,
        operation_id,
        user_id,
        expected_level,
        expected_exp,
        expected_hp,
        expected_mp,
        expected_rate,
        final_level,
        final_exp,
        final_rate,
        attempts,
        fail_count,
        item_id,
        item_count,
        exp_gain,
        *,
        root_rate=0.0,
        level_spend=0.0,
        occurred_at: datetime | None = None,
    ) -> ContinuousBreakthroughResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        expected_level = str(expected_level)
        final_level = str(final_level)
        expected_exp = int(expected_exp)
        expected_hp = int(expected_hp)
        expected_mp = int(expected_mp)
        expected_rate = int(expected_rate)
        final_exp = max(int(final_exp), 0)
        final_rate = max(int(final_rate), 0)
        attempts = max(int(attempts), 0)
        fail_count = max(int(fail_count), 0)
        item_id = int(item_id)
        item_count = max(int(item_count), 0)
        exp_gain = max(int(exp_gain), 0)
        occurred_at = occurred_at or datetime.now()

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_continuous_tribulation_operations(conn)
                previous = conn.execute(
                    "SELECT from_level, to_level, attempts, fail_count, exp_gain "
                    "FROM continuous_tribulation_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return ContinuousBreakthroughResult(
                        "duplicate", user_id, str(previous[0]), str(previous[1]),
                        int(previous[2]), int(previous[3]), -int(previous[4]),
                    )

                user = conn.execute(
                    "SELECT level, exp, hp, mp, level_up_rate "
                    "FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return ContinuousBreakthroughResult(
                        "user_missing", user_id, expected_level, final_level,
                        attempts, fail_count, -exp_gain,
                    )
                if (
                    str(user[0]) != expected_level
                    or int(user[1] or 0) != expected_exp
                    or int(user[2] or 0) != expected_hp
                    or int(user[3] or 0) != expected_mp
                    or int(user[4] or 0) != expected_rate
                ):
                    conn.rollback()
                    return ContinuousBreakthroughResult(
                        "state_changed", user_id, str(user[0]), str(user[0]),
                        attempts, fail_count, -exp_gain,
                    )

                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item_count <= 0 or item is None or int(item[0] or 0) < item_count:
                    conn.rollback()
                    return ContinuousBreakthroughResult(
                        "item_missing", user_id, expected_level, final_level,
                        attempts, fail_count, -exp_gain,
                    )

                if final_level != expected_level:
                    conn.execute(
                        "UPDATE user_xiuxian SET level=%s, exp=%s, "
                        "power=ROUND(%s*%s*%s, 0), level_up_cd=%s, "
                        "level_up_rate=0, hp=%s/2, mp=%s, atk=%s/10 "
                        "WHERE user_id=%s",
                        (
                            final_level, final_exp, final_exp, float(root_rate),
                            float(level_spend), occurred_at, final_exp, final_exp,
                            final_exp, user_id,
                        ),
                    )
                else:
                    conn.execute(
                        "UPDATE user_xiuxian SET exp=%s, level_up_rate=%s, "
                        "level_up_cd=%s WHERE user_id=%s",
                        (final_exp, final_rate, occurred_at, user_id),
                    )

                consumed = conn.execute(
                    "UPDATE back SET update_time=%s, action_time=%s, "
                    "day_num=CASE WHEN goods_type='丹药' "
                    "THEN COALESCE(day_num, 0)+%s ELSE COALESCE(day_num, 0) END, "
                    "all_num=CASE WHEN goods_type='丹药' "
                    "THEN COALESCE(all_num, 0)+%s ELSE COALESCE(all_num, 0) END, "
                    "goods_num=goods_num-%s, "
                    "bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s) "
                    "WHERE user_id=%s AND goods_id=%s "
                    "AND COALESCE(goods_num, 0)>=%s",
                    (
                        occurred_at, occurred_at, item_count, item_count,
                        item_count, item_count, user_id, item_id, item_count,
                    ),
                )
                if consumed.rowcount != 1:
                    raise db_backend.IntegrityError(
                        "continuous tribulation items changed concurrently"
                    )
                conn.execute(
                    "INSERT INTO continuous_tribulation_operations "
                    "(operation_id, user_id, from_level, to_level, attempts, "
                    "fail_count, item_id, item_count, exp_gain) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id, user_id, expected_level, final_level,
                        attempts, fail_count, item_id, item_count, exp_gain,
                    ),
                )
                conn.commit()
                return ContinuousBreakthroughResult(
                    "applied", user_id, expected_level, final_level,
                    attempts, fail_count, -exp_gain,
                )
            except Exception:
                conn.rollback()
                raise

    def apply_continuous(
        self,
        operation_id,
        user_id,
        expected_level,
        expected_exp,
        expected_hp,
        expected_mp,
        expected_rate,
        final_level,
        final_exp,
        final_hp,
        final_mp,
        final_rate,
        attempts,
        fail_count,
        exp_loss,
        *,
        root_rate=0.0,
        level_spend=0.0,
        occurred_at: datetime | None = None,
    ) -> ContinuousBreakthroughResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        expected_level = str(expected_level)
        final_level = str(final_level)
        expected_exp = int(expected_exp)
        expected_hp = int(expected_hp)
        expected_mp = int(expected_mp)
        expected_rate = int(expected_rate)
        final_exp = max(int(final_exp), 0)
        final_hp = max(int(final_hp), 1)
        final_mp = max(int(final_mp), 1)
        final_rate = max(int(final_rate), 0)
        attempts = max(int(attempts), 0)
        fail_count = max(int(fail_count), 0)
        exp_loss = max(int(exp_loss), 0)
        occurred_at = occurred_at or datetime.now()

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_continuous_operations(conn)
                previous = conn.execute(
                    "SELECT from_level, to_level, attempts, fail_count, exp_loss "
                    "FROM continuous_breakthrough_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    return ContinuousBreakthroughResult(
                        "duplicate",
                        user_id,
                        str(previous[0]),
                        str(previous[1]),
                        int(previous[2]),
                        int(previous[3]),
                        int(previous[4]),
                    )

                user = conn.execute(
                    "SELECT level, exp, hp, mp, level_up_rate "
                    "FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return ContinuousBreakthroughResult(
                        "user_missing", user_id, expected_level, final_level,
                        attempts, fail_count, exp_loss,
                    )
                if (
                    str(user[0]) != expected_level
                    or int(user[1] or 0) != expected_exp
                    or int(user[2] or 0) != expected_hp
                    or int(user[3] or 0) != expected_mp
                    or int(user[4] or 0) != expected_rate
                ):
                    conn.rollback()
                    return ContinuousBreakthroughResult(
                        "state_changed", user_id, str(user[0]), str(user[0]),
                        attempts, fail_count, exp_loss,
                    )

                if final_level != expected_level:
                    conn.execute(
                        "UPDATE user_xiuxian SET level=%s, exp=%s, "
                        "power=ROUND(%s*%s*%s, 0), level_up_cd=%s, "
                        "level_up_rate=0, hp=%s/2, mp=%s, atk=%s/10 "
                        "WHERE user_id=%s",
                        (
                            final_level,
                            final_exp,
                            final_exp,
                            float(root_rate),
                            float(level_spend),
                            occurred_at,
                            final_exp,
                            final_exp,
                            final_exp,
                            user_id,
                        ),
                    )
                else:
                    conn.execute(
                        "UPDATE user_xiuxian SET exp=%s, hp=%s, mp=%s, "
                        "level_up_rate=%s, level_up_cd=%s WHERE user_id=%s",
                        (
                            final_exp,
                            final_hp,
                            final_mp,
                            final_rate,
                            occurred_at,
                            user_id,
                        ),
                    )
                conn.execute(
                    "INSERT INTO continuous_breakthrough_operations "
                    "(operation_id, user_id, from_level, to_level, attempts, "
                    "fail_count, exp_loss) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        user_id,
                        expected_level,
                        final_level,
                        attempts,
                        fail_count,
                        exp_loss,
                    ),
                )
                conn.commit()
                return ContinuousBreakthroughResult(
                    "applied", user_id, expected_level, final_level,
                    attempts, fail_count, exp_loss,
                )
            except Exception:
                conn.rollback()
                raise

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
        exp_gain=0,
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
            exp_gain=exp_gain,
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
        exp_gain=0,
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
            exp_gain=exp_gain,
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
        exp_gain=0,
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
        exp_gain = max(int(exp_gain), 0)
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
                        "UPDATE user_xiuxian SET exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL), level_up_rate=%s, level_up_cd=%s "
                        "WHERE user_id=%s",
                        (exp_gain, int(new_rate), occurred_at, user_id),
                    )
                elif outcome == "success":
                    conn.execute(
                        "UPDATE user_xiuxian SET level=%s, exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL), "
                        "power=ROUND((exp+%s)*%s*%s, 0), level_up_cd=%s, "
                        "level_up_rate=0, hp=(exp+%s)/2, mp=exp+%s, atk=(exp+%s)/10 "
                        "WHERE user_id=%s",
                        (
                            target_level,
                            exp_gain,
                            exp_gain,
                            float(root_rate),
                            float(level_spend),
                            occurred_at,
                            exp_gain,
                            exp_gain,
                            exp_gain,
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
                    # REAL floor-sub: high-realm overflow TEXT/REAL exp.
                    conn.execute(
                        "UPDATE user_xiuxian SET "
                        "exp=MAX(CAST(COALESCE(exp,0) AS REAL)-CAST(%s AS REAL), 0), "
                        "hp=%s, mp=%s, level_up_rate=%s, level_up_cd=%s WHERE user_id=%s",
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

@dataclass(frozen=True)
class OrdinaryTribulationResult:
    status: str
    successful: bool = False
    rate: int = 0
    item_used: bool = False
    user_id: str = ""
    target_level: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class OrdinaryTribulationService:
    """Commit one resolved ordinary tribulation without partial state."""

    def __init__(self, game_database, player_database, lock=None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _saved_result(payload, successful, rate, item_used, *, status="duplicate"):
        data = json.loads(str(payload))
        return OrdinaryTribulationResult(
            status,
            bool(successful),
            int(rate),
            bool(item_used),
            str(data[0]),
            str(data[4]),
        )

    def replay(self, operation_id, user_id):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("ordinary_tribulation_operations"):
                return None
            previous = conn.execute(
                "SELECT payload,successful,rate,item_used "
                "FROM ordinary_tribulation_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            saved = self._saved_result(*previous)
            if saved.user_id != user_id:
                return OrdinaryTribulationResult("operation_conflict")
            return saved

    def settle(
        self, operation_id, user_id, *, expected_level, expected_exp,
        expected_rate, target_level, successful, new_rate, occurred_at,
        power=0, consume_destiny_pill=False,
    ) -> OrdinaryTribulationResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_level, target_level = str(expected_level), str(target_level)
        expected_exp, expected_rate, new_rate, power = map(
            int, (expected_exp, expected_rate, new_rate, power)
        )
        successful, consume_destiny_pill = bool(successful), bool(consume_destiny_pill)
        occurred_at = str(occurred_at)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        payload = json.dumps(
            [user_id, expected_level, expected_exp, expected_rate, target_level,
             successful, new_rate, occurred_at, power, consume_destiny_pill],
            ensure_ascii=True, separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS ordinary_tribulation_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,successful INTEGER NOT NULL,"
                    "rate INTEGER NOT NULL,item_used INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,successful,rate,item_used FROM ordinary_tribulation_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    saved = self._saved_result(*previous)
                    if saved.user_id != user_id:
                        return OrdinaryTribulationResult("operation_conflict", False, 0, False)
                    return saved
                user = conn.execute(
                    "SELECT level,exp FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                state = conn.execute(
                    "SELECT current_rate FROM user_tribulation WHERE user_id=%s", (user_id,)
                ).fetchone()
                # 无渡劫状态行时，与 get_user_tribulation_info 默认值一致（base=30）
                actual_rate = int(state[0]) if state else 30
                if user is None or str(user[0]) != expected_level or int(user[1] or 0) != expected_exp or actual_rate != expected_rate:
                    conn.rollback()
                    return OrdinaryTribulationResult(
                        "state_changed", False, actual_rate, False, user_id, target_level
                    )
                if consume_destiny_pill:
                    changed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-1,bind_num=MIN(COALESCE(bind_num,0),goods_num-1),"
                        "day_num=COALESCE(day_num,0)+1,all_num=COALESCE(all_num,0)+1,update_time=%s,action_time=%s "
                        "WHERE user_id=%s AND goods_id=1996 AND goods_num>0",
                        (occurred_at, occurred_at, user_id),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return OrdinaryTribulationResult(
                            "item_missing", False, actual_rate, False, user_id, target_level
                        )
                if successful:
                    changed = conn.execute(
                        "UPDATE user_xiuxian SET level=%s,power=%s WHERE user_id=%s AND level=%s AND exp=%s",
                        (target_level, power, user_id, expected_level, expected_exp),
                    )
                    conn.execute("DELETE FROM user_tribulation WHERE user_id=%s", (user_id,))
                else:
                    # 失败：必须能写入/更新渡劫状态。无行时 INSERT，有行时 UPDATE。
                    # 旧逻辑只 UPDATE，首次渡劫失败（无 user_tribulation 行）会 rowcount=0 → 误报 state_changed
                    if state is None:
                        changed = conn.execute(
                            "INSERT INTO user_tribulation "
                            "(user_id, current_rate, heart_devil_count, last_time, next_level) "
                            "VALUES (%s, %s, 0, %s, %s)",
                            (user_id, new_rate, occurred_at, target_level),
                        )
                    else:
                        changed = conn.execute(
                            "UPDATE user_tribulation SET current_rate=%s,last_time=%s,next_level=%s "
                            "WHERE user_id=%s AND current_rate=%s",
                            (new_rate, occurred_at, target_level, user_id, expected_rate),
                        )
                if changed.rowcount != 1:
                    conn.rollback()
                    return OrdinaryTribulationResult(
                        "state_changed", False, actual_rate, False, user_id, target_level
                    )
                increment_stat(conn, user_id, "渡劫次数", 1)
                increment_stat(conn, user_id, "渡劫成功" if successful else "渡劫失败", 1)
                if consume_destiny_pill:
                    increment_stat(conn, user_id, "天命丹消耗", 1)
                conn.execute(
                    "INSERT INTO ordinary_tribulation_operations(operation_id,payload,successful,rate,item_used) VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, int(successful), new_rate, int(consume_destiny_pill)),
                )
                conn.commit()
                return OrdinaryTribulationResult(
                    "applied",
                    successful,
                    new_rate,
                    consume_destiny_pill,
                    user_id,
                    target_level,
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

@dataclass(frozen=True)
class DestinyTribulationResult:
    status: str
    target_level: str = ""
    user_id: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class DestinyTribulationService:
    """Consume the destiny pill and promote the player in one transaction."""

    def __init__(self, game_database, player_database, lock=None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _saved_result(payload, target_level, *, status="duplicate"):
        data = json.loads(str(payload))
        return DestinyTribulationResult(status, str(target_level), str(data[0]))

    def replay(self, operation_id, user_id):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("destiny_tribulation_operations"):
                return None
            previous = conn.execute(
                "SELECT payload,target_level FROM destiny_tribulation_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            saved = self._saved_result(*previous)
            if saved.user_id != user_id:
                return DestinyTribulationResult("operation_conflict")
            return saved

    def settle(self, operation_id, user_id, *, expected_level, expected_exp, target_level, power, occurred_at):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_level, target_level = str(expected_level), str(target_level)
        expected_exp, power, occurred_at = int(expected_exp), int(power), str(occurred_at)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        payload = json.dumps([user_id, expected_level, expected_exp, target_level, power, occurred_at], separators=(",", ":"))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS destiny_tribulation_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,target_level TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,target_level FROM destiny_tribulation_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous:
                    conn.rollback()
                    saved = self._saved_result(*previous)
                    if saved.user_id != user_id:
                        return DestinyTribulationResult("operation_conflict")
                    return saved
                user = conn.execute("SELECT level,exp FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None or str(user[0]) != expected_level or int(user[1] or 0) != expected_exp:
                    conn.rollback(); return DestinyTribulationResult("state_changed", target_level, user_id)
                consumed = conn.execute(
                    "UPDATE back SET goods_num=goods_num-1,bind_num=MIN(COALESCE(bind_num,0),goods_num-1),"
                    "day_num=COALESCE(day_num,0)+1,all_num=COALESCE(all_num,0)+1,update_time=%s,action_time=%s "
                    "WHERE user_id=%s AND goods_id=1997 AND goods_num>0", (occurred_at, occurred_at, user_id),
                )
                if consumed.rowcount != 1:
                    conn.rollback(); return DestinyTribulationResult("item_missing", target_level, user_id)
                changed = conn.execute(
                    "UPDATE user_xiuxian SET level=%s,power=%s WHERE user_id=%s AND level=%s AND exp=%s",
                    (target_level, power, user_id, expected_level, expected_exp),
                )
                if changed.rowcount != 1:
                    conn.rollback(); return DestinyTribulationResult("state_changed", target_level, user_id)
                conn.execute("DELETE FROM user_tribulation WHERE user_id=%s", (user_id,))
                increment_stat(conn, user_id, "渡劫次数", 1)
                increment_stat(conn, user_id, "渡劫成功", 1)
                increment_stat(conn, user_id, "天命渡劫丹消耗", 1)
                conn.execute("INSERT INTO destiny_tribulation_operations(operation_id,payload,target_level) VALUES(%s,%s,%s)", (operation_id, payload, target_level))
                conn.commit(); return DestinyTribulationResult("applied", target_level, user_id)
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass

@dataclass(frozen=True)
class HeartDevilTribulationResult:
    status: str
    successful: bool = False
    rate: int = 0
    heart_devil_count: int = 0
    item_used: bool = False
    user_id: str = ""
    devil_name: str = ""
    message: str = ""
    battle_messages: list = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class HeartDevilTribulationService:
    """Commit one already-resolved heart-devil encounter atomically."""

    def __init__(self, game_database, player_database, lock=None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _saved_result(
        payload, successful, rate, heart_devil_count, item_used, *, status="duplicate"
    ):
        data = json.loads(str(payload))
        return HeartDevilTribulationResult(
            status=status,
            successful=bool(successful),
            rate=int(rate),
            heart_devil_count=int(heart_devil_count),
            item_used=bool(item_used),
            user_id=str(data[0]),
            devil_name=str(data[6] or ""),
            message=str(data[8] or "") if len(data) > 8 else "",
            battle_messages=list(data[9] or []) if len(data) > 9 else [],
        )

    def replay(self, operation_id, user_id):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("heart_devil_tribulation_operations"):
                return None
            previous = conn.execute(
                "SELECT payload,successful,rate,heart_devil_count,item_used "
                "FROM heart_devil_tribulation_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            saved = self._saved_result(*previous)
            if saved.user_id != user_id:
                return HeartDevilTribulationResult("operation_conflict")
            return saved

    def settle(
        self, operation_id, user_id, *, expected_rate, expected_count,
        successful, new_rate, occurred_at, devil_name="", consume_destiny_pill=False,
        message="", battle_messages=None,
    ) -> HeartDevilTribulationResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_rate, expected_count, new_rate = map(int, (expected_rate, expected_count, new_rate))
        successful, consume_destiny_pill = bool(successful), bool(consume_destiny_pill)
        occurred_at, devil_name, message = (
            str(occurred_at), str(devil_name), str(message)
        )
        battle_messages = list(battle_messages or [])
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        payload = json.dumps(
            [
                user_id, expected_rate, expected_count, successful, new_rate,
                occurred_at, devil_name, consume_destiny_pill, message,
                battle_messages,
            ],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS heart_devil_tribulation_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,successful INTEGER NOT NULL,rate INTEGER NOT NULL,"
                    "heart_devil_count INTEGER NOT NULL,item_used INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,successful,rate,heart_devil_count,item_used FROM heart_devil_tribulation_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous:
                    conn.rollback()
                    saved = self._saved_result(*previous)
                    if saved.user_id != user_id:
                        return HeartDevilTribulationResult("operation_conflict", False, 0, 0, False)
                    return saved
                state = conn.execute("SELECT current_rate,heart_devil_count FROM user_tribulation WHERE user_id=%s", (user_id,)).fetchone()
                actual_rate, actual_count = (int(state[0]), int(state[1])) if state else (30, 0)
                if actual_rate != expected_rate or actual_count != expected_count:
                    conn.rollback(); return HeartDevilTribulationResult("state_changed", False, actual_rate, actual_count, False)
                if consume_destiny_pill:
                    consumed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-1,bind_num=MIN(COALESCE(bind_num,0),goods_num-1),"
                        "day_num=COALESCE(day_num,0)+1,all_num=COALESCE(all_num,0)+1,update_time=%s,action_time=%s "
                        "WHERE user_id=%s AND goods_id=1996 AND goods_num>0", (occurred_at, occurred_at, user_id),
                    )
                    if consumed.rowcount != 1:
                        conn.rollback(); return HeartDevilTribulationResult("item_missing", False, actual_rate, actual_count, False)
                new_count = expected_count + 1
                changed = conn.execute(
                    "UPDATE user_tribulation SET current_rate=%s,heart_devil_count=%s,last_time=%s "
                    "WHERE user_id=%s AND current_rate=%s AND heart_devil_count=%s",
                    (new_rate, new_count, occurred_at, user_id, expected_rate, expected_count),
                )
                if changed.rowcount == 0 and state is None:
                    conn.execute(
                        "INSERT INTO user_tribulation(user_id,current_rate,heart_devil_count,last_time) VALUES(%s,%s,%s,%s)",
                        (user_id, new_rate, new_count, occurred_at),
                    )
                elif changed.rowcount != 1:
                    conn.rollback(); return HeartDevilTribulationResult("state_changed", False, actual_rate, actual_count, False)
                increment_stat(conn, user_id, "心魔劫次数", 1)
                increment_stat(conn, user_id, "心魔劫成功" if successful else "心魔劫失败", 1)
                if consume_destiny_pill:
                    increment_stat(conn, user_id, "天命丹消耗", 1)
                conn.execute(
                    "INSERT INTO heart_devil_tribulation_operations(operation_id,payload,successful,rate,heart_devil_count,item_used) VALUES(%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, int(successful), new_rate, new_count, int(consume_destiny_pill)),
                )
                conn.commit()
                return HeartDevilTribulationResult(
                    "applied", successful, new_rate, new_count,
                    consume_destiny_pill, user_id, devil_name, message,
                    battle_messages,
                )
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass

@dataclass(frozen=True)
class PillFusionResult:
    status: str
    user_id: str
    source_item_id: int
    source_quantity: int
    target_item_id: int
    target_quantity: int
    successful: bool

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class PillFusionService:
    """Consume fusion materials and grant a pre-rolled result atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pill_fusion_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                source_item_id INTEGER NOT NULL,
                source_quantity INTEGER NOT NULL,
                target_item_id INTEGER NOT NULL,
                target_quantity INTEGER NOT NULL,
                successful INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply(
        self,
        operation_id,
        user_id,
        source_item_id,
        source_quantity,
        target_item_id,
        target_name,
        target_type,
        *,
        successful,
        target_quantity=1,
        max_goods_num,
    ) -> PillFusionResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        source_item_id = int(source_item_id)
        source_quantity = int(source_quantity)
        target_item_id = int(target_item_id)
        target_name = str(target_name)
        target_type = str(target_type)
        successful = bool(successful)
        target_quantity = int(target_quantity) if successful else 0
        max_goods_num = int(max_goods_num)
        if source_quantity <= 0 or max_goods_num <= 0:
            raise ValueError("source_quantity and max_goods_num must be positive")
        if successful and target_quantity <= 0:
            raise ValueError("target_quantity must be positive after successful fusion")
        if source_item_id == target_item_id:
            raise ValueError("source and target items must differ")

        def result(status: str, success=successful, reward_quantity=target_quantity):
            return PillFusionResult(
                status,
                user_id,
                source_item_id,
                source_quantity,
                target_item_id,
                int(reward_quantity),
                bool(success),
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT target_quantity, successful FROM pill_fusion_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", bool(previous[1]), previous[0])

                source = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, source_item_id),
                ).fetchone()
                if source is None or int(source[0] or 0) < source_quantity:
                    conn.rollback()
                    return result("item_insufficient")

                columns = set(conn.column_names("back"))
                updates = ["goods_num=goods_num-%s"]
                params: list[object] = [source_quantity]
                if "bind_num" in columns:
                    updates.append(
                        "bind_num=CASE WHEN goods_num-%s=0 THEN 0 "
                        "WHEN COALESCE(bind_num, 0)>=%s "
                        "THEN COALESCE(bind_num, 0)-%s "
                        "ELSE MIN(COALESCE(bind_num, 0), goods_num-%s) END"
                    )
                    params.extend((source_quantity,) * 4)
                consumed = conn.execute(
                    f"UPDATE back SET {', '.join(updates)} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, source_item_id, source_quantity),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")

                if successful:
                    conn.execute(
                        "INSERT INTO back (user_id, goods_id, goods_name, goods_type, "
                        "goods_num, bind_num) VALUES (%s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                        "goods_num=MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s), "
                        "bind_num=MIN(COALESCE(back.bind_num, 0)+EXCLUDED.goods_num, "
                        "MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s))",
                        (
                            user_id, target_item_id, target_name, target_type,
                            target_quantity, target_quantity, max_goods_num, max_goods_num,
                        ),
                    )
                conn.execute(
                    "INSERT INTO pill_fusion_operations "
                    "(operation_id, user_id, source_item_id, source_quantity, "
                    "target_item_id, target_quantity, successful) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id, user_id, source_item_id, source_quantity,
                        target_item_id, target_quantity, int(successful),
                    ),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class TribulationStateMigrationResult:
    status: str
    state: dict = field(default_factory=dict)

    @property
    def database_ready(self) -> bool:
        return self.status in {
            "applied",
            "duplicate",
            "database_authoritative",
        }

class TribulationStateMigrationService:
    """Import one legacy tribulation state without overwriting database state."""

    _STATE_FIELDS = (
        "current_rate",
        "heart_devil_count",
        "last_time",
        "next_level",
    )

    def __init__(self, game_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._lock = lock or RLock()

    @staticmethod
    def normalize(data, *, base_rate=30) -> dict:
        data = dict(data or {})
        try:
            current_rate = int(data.get("current_rate", base_rate))
        except (TypeError, ValueError):
            current_rate = int(base_rate)
        try:
            heart_devil_count = int(data.get("heart_devil_count", 0))
        except (TypeError, ValueError):
            heart_devil_count = 0
        return {
            "current_rate": current_rate,
            "heart_devil_count": heart_devil_count,
            "last_time": data.get("last_time") or None,
            "next_level": data.get("next_level") or None,
        }

    @classmethod
    def _row_state(cls, row, *, base_rate=30) -> dict:
        return cls.normalize(
            {
                "current_rate": row[0],
                "heart_devil_count": row[1],
                "last_time": row[2],
                "next_level": row[3],
            },
            base_rate=base_rate,
        )

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tribulation_state_migration_operations("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL UNIQUE,"
            "payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def migrate(self, operation_id, user_id, legacy_data, *, base_rate=30):
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id must not be empty")
        state = self.normalize(legacy_data, base_rate=base_rate)
        payload = json.dumps(
            state,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT user_id,payload FROM tribulation_state_migration_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    status = (
                        "duplicate"
                        if str(previous[0]) == user_id and str(previous[1]) == payload
                        else "operation_conflict"
                    )
                    saved = json.loads(str(previous[1]))
                    return TribulationStateMigrationResult(status, saved)

                current = conn.execute(
                    "SELECT current_rate,heart_devil_count,last_time,next_level "
                    "FROM user_tribulation WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if current is not None:
                    conn.rollback()
                    return TribulationStateMigrationResult(
                        "database_authoritative",
                        self._row_state(current, base_rate=base_rate),
                    )

                conn.execute(
                    "INSERT INTO user_tribulation("
                    "user_id,current_rate,heart_devil_count,last_time,next_level"
                    ") VALUES(%s,%s,%s,%s,%s)",
                    (
                        user_id,
                        state["current_rate"],
                        state["heart_devil_count"],
                        state["last_time"],
                        state["next_level"],
                    ),
                )
                conn.execute(
                    "INSERT INTO tribulation_state_migration_operations("
                    "operation_id,user_id,payload) VALUES(%s,%s,%s)",
                    (operation_id, user_id, payload),
                )
                conn.commit()
                return TribulationStateMigrationResult("applied", state)
            except Exception:
                conn.rollback()
                raise

__all__ = [
    "SignInResult",
    "SignInService",
    "PlayerRenameResult",
    "PlayerRenameService",
    "StoneGiftResult",
    "StoneGiftService",
    "StoneContestResult",
    "StoneTheftResult",
    "StoneContestService",
    "StoneRobberyResult",
    "StoneRobberySettlementService",
    "LotteryWinner",
    "LotteryPoolSnapshot",
    "LotterySettlementResult",
    "LotterySettlementService",
    "XiangyuanCreateResult",
    "XiangyuanClaimResult",
    "XiangyuanSettlementService",
    "DirectBreakthroughResult",
    "ContinuousBreakthroughResult",
    "BreakthroughService",
    "OrdinaryTribulationResult",
    "OrdinaryTribulationService",
    "DestinyTribulationResult",
    "DestinyTribulationService",
    "HeartDevilTribulationResult",
    "HeartDevilTribulationService",
    "PillFusionResult",
    "PillFusionService",
    "TribulationStateMigrationResult",
    "TribulationStateMigrationService",
]
