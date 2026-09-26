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
from ...compatibility.legacy_base_breakthrough import (
    BreakthroughService,
    ContinuousBreakthroughResult,
    DirectBreakthroughResult,
)
from ...compatibility.legacy_base_ordinary_tribulation import (
    OrdinaryTribulationResult,
    OrdinaryTribulationService,
)
from ...compatibility.legacy_base_destiny_tribulation import (
    DestinyTribulationResult,
    DestinyTribulationService,
)
from ...compatibility.legacy_base_heart_devil_tribulation import (
    HeartDevilTribulationResult,
    HeartDevilTribulationService,
)
from datetime import date, datetime
from datetime import datetime


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
