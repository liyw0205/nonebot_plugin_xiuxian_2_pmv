from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .closing_settlement_service import _increment_stat


@dataclass(frozen=True)
class ImpartBattleBatchResult:
    status: str
    challenger_pk_num: int = 0
    opponent_pk_num: int | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ImpartBattleBatchService:
    def __init__(self, impart_db, player_db, lock=None):
        self.impart_db = Path(impart_db)
        self.player_db = Path(player_db)
        self.lock = lock or RLock()

    def get_pk_num(self, user_id, legacy_pk_num):
        user_id, legacy_pk_num = str(user_id), int(legacy_pk_num)
        with self.lock, closing(db_backend.connect(self.player_db)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS impart_pk_state("
                "user_id TEXT PRIMARY KEY,pk_num INTEGER NOT NULL DEFAULT 7,"
                "win_num INTEGER NOT NULL DEFAULT 0)"
            )
            row = conn.execute("SELECT pk_num FROM impart_pk_state WHERE user_id=%s", (user_id,)).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO impart_pk_state(user_id,pk_num,win_num) VALUES(%s,%s,0)",
                    (user_id, legacy_pk_num),
                )
                conn.commit()
                return legacy_pk_num
            return int(row[0])

    def get_result(self, operation_id: str) -> ImpartBattleBatchResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self.lock, closing(db_backend.connect(self.impart_db)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS impart_battle_batch_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            old = conn.execute(
                "SELECT payload,result_json FROM impart_battle_batch_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if old is None:
                return None
            saved = json.loads(str(old[1]))
            return ImpartBattleBatchResult("duplicate", saved[0], saved[1])

    def settle(
        self,
        operation_id,
        challenger_id,
        expected_challenger_pk_num,
        challenger_wins,
        challenger_losses,
        challenger_stones,
        opponent_id=None,
        expected_opponent_pk_num=None,
        opponent_wins=0,
        opponent_losses=0,
        opponent_stones=0,
    ) -> ImpartBattleBatchResult:
        operation_id = str(operation_id).strip()
        challenger_id = str(challenger_id)
        opponent_id = None if opponent_id is None else str(opponent_id)
        values = tuple(
            int(value)
            for value in (
                expected_challenger_pk_num,
                challenger_wins,
                challenger_losses,
                challenger_stones,
                opponent_wins,
                opponent_losses,
                opponent_stones,
            )
        )
        expected_challenger_pk_num, challenger_wins, challenger_losses, challenger_stones, opponent_wins, opponent_losses, opponent_stones = values
        if expected_opponent_pk_num is not None:
            expected_opponent_pk_num = int(expected_opponent_pk_num)
        if (
            not operation_id
            or min(values) < 0
            or challenger_losses > expected_challenger_pk_num
            or (opponent_id is None) != (expected_opponent_pk_num is None)
            or (expected_opponent_pk_num is not None and opponent_losses > expected_opponent_pk_num)
        ):
            raise ValueError("invalid impart battle batch")
        # Request identity only — win/loss/stone rolls + pk snapshots are concurrency checks.
        payload = json.dumps(
            [challenger_id, opponent_id], ensure_ascii=False, separators=(",", ":"),
        )
        with self.lock, closing(db_backend.connect(self.impart_db)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player_db),))
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_battle_batch_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS player_data.impart_pk_state("
                    "user_id TEXT PRIMARY KEY,pk_num INTEGER NOT NULL DEFAULT 7,"
                    "win_num INTEGER NOT NULL DEFAULT 0)"
                )
                old = conn.execute(
                    "SELECT payload,result_json FROM impart_battle_batch_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return ImpartBattleBatchResult("operation_conflict")
                    saved = json.loads(str(old[1]))
                    return ImpartBattleBatchResult("duplicate", saved[0], saved[1])

                participants = [
                    (
                        challenger_id,
                        expected_challenger_pk_num,
                        challenger_wins,
                        challenger_losses,
                        challenger_stones,
                    )
                ]
                if opponent_id is not None:
                    participants.append(
                        (opponent_id, expected_opponent_pk_num, opponent_wins, opponent_losses, opponent_stones)
                    )
                remaining = []
                for user_id, expected_pk, wins, losses, stones in participants:
                    row = conn.execute(
                        "SELECT pk_num FROM player_data.impart_pk_state WHERE user_id=%s", (user_id,)
                    ).fetchone()
                    if row is None:
                        conn.execute(
                            "INSERT INTO player_data.impart_pk_state(user_id,pk_num,win_num) VALUES(%s,%s,0)",
                            (user_id, expected_pk),
                        )
                    elif int(row[0]) != expected_pk:
                        conn.rollback()
                        return ImpartBattleBatchResult("state_changed")
                    impart = conn.execute(
                        "SELECT 1 FROM xiuxian_impart WHERE user_id=%s", (user_id,)
                    ).fetchone()
                    if impart is None:
                        conn.rollback()
                        return ImpartBattleBatchResult("user_missing")
                    changed = conn.execute(
                        "UPDATE player_data.impart_pk_state SET pk_num=pk_num-%s,win_num=win_num+%s "
                        "WHERE user_id=%s AND pk_num=%s AND pk_num>=%s",
                        (losses, wins, user_id, expected_pk, losses),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return ImpartBattleBatchResult("state_changed")
                    conn.execute(
                        "UPDATE xiuxian_impart SET stone_num=COALESCE(stone_num,0)+%s WHERE user_id=%s",
                        (stones, user_id),
                    )
                    _increment_stat(conn, user_id, "虚神界对决次数", wins + losses)
                    _increment_stat(conn, user_id, "虚神界对决胜利", wins)
                    _increment_stat(conn, user_id, "虚神界对决失败", losses)
                    _increment_stat(conn, user_id, "思恋结晶获取", stones)
                    remaining.append(expected_pk - losses)

                saved = [remaining[0], remaining[1] if len(remaining) > 1 else None]
                conn.execute(
                    "INSERT INTO impart_battle_batch_operations(operation_id,payload,result_json) VALUES(%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, separators=(",", ":"))),
                )
                conn.commit()
                return ImpartBattleBatchResult("applied", saved[0], saved[1])
            except Exception:
                conn.rollback()
                raise


__all__ = ["ImpartBattleBatchResult", "ImpartBattleBatchService"]
