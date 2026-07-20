from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend

@dataclass(frozen=True)
class DufangBetResult:
    status: str
    cost: int
    wallet_stone: int
    bet_id: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class DufangBetService:
    """Charge an unseal wager and persist its pending state atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> DufangBetResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS dufang_bet_operations ("
                "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, cost INTEGER NOT NULL, "
                "wallet_stone INTEGER NOT NULL, bet_id TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT cost,wallet_stone,bet_id FROM dufang_bet_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return DufangBetResult("duplicate", int(previous[0]), int(previous[1]), str(previous[2]))

    def place(self, operation_id, user_id, cost, placed_at) -> DufangBetResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        cost = int(cost)
        placed_at = str(placed_at).strip()
        if not operation_id or cost <= 0 or not placed_at:
            raise ValueError("operation id, positive cost and placement time are required")
        # Request identity only; placed_at is placement metadata, not the key.
        payload = json.dumps([user_id, cost], ensure_ascii=True, separators=(",", ":"))

        def result(status, wallet_stone=0, bet_id=""):
            return DufangBetResult(status, cost if status in {"applied", "duplicate"} else 0, wallet_stone, bet_id)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS dufang_bets ("
                    "bet_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, cost INTEGER NOT NULL, "
                    "status TEXT NOT NULL, placed_at TEXT NOT NULL, settled_at TEXT)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS dufang_bet_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, cost INTEGER NOT NULL, "
                    "wallet_stone INTEGER NOT NULL, bet_id TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,cost,wallet_stone,bet_id FROM dufang_bet_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return DufangBetResult("duplicate", int(previous[1]), int(previous[2]), str(previous[3]))

                user = conn.execute(
                    "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                wallet_stone = int(user[0])
                if wallet_stone < cost:
                    conn.rollback()
                    return result("stone_insufficient", wallet_stone)

                conn.execute(
                    "CREATE TABLE IF NOT EXISTS player_data.unseal_data ("
                    "user_id TEXT PRIMARY KEY, count INTEGER, total_cost INTEGER, profit INTEGER, loss INTEGER, "
                    "shared_profit INTEGER, shared_loss INTEGER, received_profit INTEGER, received_loss INTEGER, last_update TEXT)"
                )
                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(%s AS REAL) "
                    "WHERE user_id=%s AND CAST(COALESCE(stone,0) AS REAL)>=CAST(%s AS REAL)",
                    (cost, user_id, cost),
                )
                if charged.rowcount != 1:
                    conn.rollback()
                    return result("state_changed", wallet_stone)
                conn.execute(
                    "INSERT INTO player_data.unseal_data (user_id,count,total_cost,last_update) VALUES (%s,1,%s,%s) "
                    "ON CONFLICT(user_id) DO UPDATE SET count=COALESCE(count,0)+1, "
                    "total_cost=COALESCE(total_cost,0)+EXCLUDED.total_cost,last_update=EXCLUDED.last_update",
                    (user_id, cost, placed_at),
                )
                conn.execute(
                    "INSERT INTO dufang_bets VALUES (%s,%s,%s,%s,%s,NULL)",
                    (operation_id, user_id, cost, "pending", placed_at),
                )
                wallet_stone -= cost
                conn.execute(
                    "INSERT INTO dufang_bet_operations VALUES (%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                    (operation_id, payload, cost, wallet_stone, operation_id),
                )
                conn.commit()
                return DufangBetResult("applied", cost, wallet_stone, operation_id)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class DufangPayoutResult:
    status: str
    wallet_stone: int
    gain: int
    loss: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class DufangPayoutService:
    """Settle one pending unseal wager and its statistics atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> DufangPayoutResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS dufang_payout_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,wallet_stone INTEGER NOT NULL,"
                "gain INTEGER NOT NULL,loss INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT wallet_stone,gain,loss FROM dufang_payout_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return DufangPayoutResult("duplicate", int(previous[0]), int(previous[1]), int(previous[2]))

    def settle(self, operation_id, bet_id, user_id, outcome, gain, requested_loss, settled_at) -> DufangPayoutResult:
        operation_id, bet_id, user_id = str(operation_id).strip(), str(bet_id).strip(), str(user_id)
        outcome, settled_at = str(outcome), str(settled_at)
        gain, requested_loss = int(gain), int(requested_loss)
        if not operation_id or not bet_id or outcome not in {"win", "loss"} or gain < 0 or requested_loss < 0:
            raise ValueError("valid operation, bet and payout values are required")
        # Request identity = bet + user; outcome/gain/loss stored as result.
        payload = json.dumps([bet_id, user_id], ensure_ascii=True, separators=(",", ":"))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS dufang_payout_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,wallet_stone INTEGER NOT NULL,gain INTEGER NOT NULL,loss INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload,wallet_stone,gain,loss FROM dufang_payout_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return DufangPayoutResult("state_changed", 0, 0, 0)
                    return DufangPayoutResult("duplicate", int(previous[1]), int(previous[2]), int(previous[3]))
                bet = conn.execute("SELECT user_id,status FROM dufang_bets WHERE bet_id=%s", (bet_id,)).fetchone()
                if bet is None or str(bet[0]) != user_id or str(bet[1]) != "pending":
                    conn.rollback()
                    return DufangPayoutResult("state_changed", 0, 0, 0)
                user = conn.execute("SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback()
                    return DufangPayoutResult("user_missing", 0, 0, 0)
                wallet = int(user[0])
                actual_gain = gain if outcome == "win" else 0
                actual_loss = min(requested_loss, wallet) if outcome == "loss" else 0
                wallet = wallet + actual_gain - actual_loss
                conn.execute("UPDATE user_xiuxian SET stone=%s WHERE user_id=%s", (wallet, user_id))
                field, amount = ("profit", actual_gain) if outcome == "win" else ("loss", actual_loss)
                conn.execute(f"UPDATE player_data.unseal_data SET {db_backend.quote_ident(field)}=COALESCE({db_backend.quote_ident(field)},0)+%s,last_update=%s WHERE user_id=%s", (amount, settled_at, user_id))
                if conn.execute("UPDATE dufang_bets SET status=%s,settled_at=%s WHERE bet_id=%s AND status=%s", (outcome, settled_at, bet_id, "pending")).rowcount != 1:
                    conn.rollback()
                    return DufangPayoutResult("state_changed", 0, 0, 0)
                conn.execute("INSERT INTO dufang_payout_operations VALUES (%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)", (operation_id, payload, wallet, actual_gain, actual_loss))
                conn.commit()
                return DufangPayoutResult("applied", wallet, actual_gain, actual_loss)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class DufangShareRecipientResult:
    user_id: str
    user_name: str
    status: str
    amount: int = 0
    wallet_stone: int = 0

@dataclass(frozen=True)
class DufangShareSettlementResult:
    status: str
    task_status: str = ""
    event_type: str = ""
    event_title: str = ""
    event_description: str = ""
    bonus_percent: int = 0
    total: int = 0
    completed: int = 0
    total_amount: int = 0
    recipients: tuple[DufangShareRecipientResult, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class DufangShareSettlementService:
    """Settle a frozen sharing batch with durable per-player progress."""

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
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dufang_share_operations("
            "operation_id TEXT PRIMARY KEY,source_id TEXT NOT NULL,event_type TEXT NOT NULL,"
            "event_title TEXT NOT NULL,event_description TEXT NOT NULL,effect_amount INTEGER NOT NULL,"
            "bonus_percent INTEGER NOT NULL,total INTEGER NOT NULL,completed INTEGER NOT NULL DEFAULT 0,"
            "total_amount INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL DEFAULT 'running',"
            "created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dufang_share_progress("
            "operation_id TEXT NOT NULL,ordinal INTEGER NOT NULL,target_id TEXT NOT NULL,"
            "target_name TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',"
            "actual_amount INTEGER NOT NULL DEFAULT 0,wallet_stone INTEGER NOT NULL DEFAULT 0,"
            "reason TEXT NOT NULL DEFAULT '',updated_at TEXT NOT NULL,"
            "PRIMARY KEY(operation_id,target_id),UNIQUE(operation_id,ordinal))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS economy_log("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
            "source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,"
            "exp_delta INTEGER NOT NULL DEFAULT 0,sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
            "sect_scale_delta INTEGER NOT NULL DEFAULT 0,sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
            "item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',"
            "trace_id TEXT,created_at TEXT NOT NULL)"
        )
        if not conn.column_exists("economy_log", "trace_id"):
            conn.execute("ALTER TABLE economy_log ADD COLUMN trace_id TEXT")

    @staticmethod
    def _ensure_player_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.unseal_data("
            "user_id TEXT PRIMARY KEY,count INTEGER,total_cost INTEGER,profit INTEGER,loss INTEGER,"
            "shared_profit INTEGER,shared_loss INTEGER,received_profit INTEGER,"
            "received_loss INTEGER,last_update TEXT)"
        )

    @staticmethod
    def _normalize_recipients(source_id, recipients):
        normalized = []
        seen = set()
        for target_id, target_name in recipients:
            target_id = str(target_id).strip()
            if not target_id or target_id == source_id or target_id in seen:
                raise ValueError("sharing recipients must be unique and different from the source")
            seen.add(target_id)
            normalized.append((target_id, str(target_name) or "未知道友"))
        if not normalized:
            raise ValueError("at least one sharing recipient is required")
        return tuple(normalized)

    @staticmethod
    def _result(conn, operation_id, status, applied_now=()):
        operation = conn.execute(
            "SELECT source_id,event_type,event_title,event_description,bonus_percent,total,"
            "completed,total_amount,status FROM dufang_share_operations WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        if operation is None:
            return DufangShareSettlementResult(status)
        applied_now = set(applied_now)
        recipients = []
        for row in conn.execute(
            "SELECT target_id,target_name,status,actual_amount,wallet_stone "
            "FROM dufang_share_progress WHERE operation_id=%s ORDER BY ordinal",
            (operation_id,),
        ).fetchall():
            persisted_status = str(row[2])
            recipient_status = persisted_status
            if persisted_status == "applied" and str(row[0]) not in applied_now:
                recipient_status = "duplicate"
            recipients.append(
                DufangShareRecipientResult(
                    str(row[0]),
                    str(row[1]),
                    recipient_status,
                    int(row[3]),
                    int(row[4]),
                )
            )
        return DufangShareSettlementResult(
            status,
            str(operation[8]),
            str(operation[1]),
            str(operation[2]),
            str(operation[3]),
            int(operation[4]),
            int(operation[5]),
            int(operation[6]),
            int(operation[7]),
            tuple(recipients),
        )

    def settle(
        self,
        operation_id,
        source_id,
        event_type,
        event_title,
        event_description,
        effect_amount,
        bonus_percent,
        recipients,
        occurred_at,
        *,
        chunk_size=100,
    ) -> DufangShareSettlementResult:
        operation_id = str(operation_id).strip()
        source_id = str(source_id).strip()
        event_type = str(event_type).strip()
        event_title = str(event_title)
        event_description = str(event_description)
        effect_amount = int(effect_amount)
        bonus_percent = int(bonus_percent)
        occurred_at = str(occurred_at).strip()
        chunk_size = max(1, int(chunk_size))
        if (
            not operation_id
            or not source_id
            or event_type not in {"profit", "loss"}
            or not event_title
            or effect_amount <= 0
            or not 0 <= bonus_percent <= 50
            or not occurred_at
        ):
            raise ValueError("invalid sharing settlement request")
        normalized_recipients = self._normalize_recipients(source_id, recipients)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                self._ensure_player_schema(conn)
                previous = conn.execute(
                    "SELECT source_id,status FROM dufang_share_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is None:
                    conn.execute(
                        "INSERT INTO dufang_share_operations("
                        "operation_id,source_id,event_type,event_title,event_description,effect_amount,"
                        "bonus_percent,total,created_at,updated_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (
                            operation_id,
                            source_id,
                            event_type,
                            event_title,
                            event_description,
                            effect_amount,
                            bonus_percent,
                            len(normalized_recipients),
                            occurred_at,
                            occurred_at,
                        ),
                    )
                    for ordinal, (target_id, target_name) in enumerate(normalized_recipients):
                        conn.execute(
                            "INSERT INTO dufang_share_progress("
                            "operation_id,ordinal,target_id,target_name,updated_at) VALUES(%s,%s,%s,%s,%s)",
                            (operation_id, ordinal, target_id, target_name, occurred_at),
                        )
                    conn.commit()
                else:
                    if str(previous[0]) != source_id:
                        result = self._result(conn, operation_id, "operation_conflict")
                        conn.rollback()
                        return result
                    if str(previous[1]) == "completed":
                        result = self._result(conn, operation_id, "duplicate")
                        conn.rollback()
                        return result
                    conn.commit()

                frozen = conn.execute(
                    "SELECT event_type,event_title,effect_amount FROM dufang_share_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                frozen_event_type = str(frozen[0])
                frozen_event_title = str(frozen[1])
                frozen_effect_amount = int(frozen[2])
                pending = conn.execute(
                    "SELECT target_id,target_name FROM dufang_share_progress "
                    "WHERE operation_id=%s AND status='pending' ORDER BY ordinal LIMIT %s",
                    (operation_id, chunk_size),
                ).fetchall()
                applied_now = set()

                for pending_row in pending:
                    target_id, target_name = str(pending_row[0]), str(pending_row[1])
                    conn.execute("BEGIN IMMEDIATE")
                    source = conn.execute(
                        "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (source_id,)
                    ).fetchone()
                    target = conn.execute(
                        "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s",
                        (target_id,),
                    ).fetchone()
                    if source is None or target is None:
                        reason = "source_missing" if source is None else "target_missing"
                        conn.execute(
                            "UPDATE dufang_share_progress SET status='skipped',reason=%s,updated_at=%s "
                            "WHERE operation_id=%s AND target_id=%s AND status='pending'",
                            (reason, occurred_at, operation_id, target_id),
                        )
                        conn.execute(
                            "UPDATE dufang_share_operations SET completed=completed+1,"
                            "status=CASE WHEN completed+1>=total THEN 'completed' ELSE 'running' END,"
                            "updated_at=%s WHERE operation_id=%s",
                            (occurred_at, operation_id),
                        )
                        conn.commit()
                        continue

                    previous_stone = int(target[0])
                    actual_amount = (
                        frozen_effect_amount
                        if frozen_event_type == "profit"
                        else min(frozen_effect_amount, previous_stone)
                    )
                    if actual_amount <= 0:
                        conn.execute(
                            "UPDATE dufang_share_progress SET status='skipped',reason='zero_balance',"
                            "wallet_stone=%s,updated_at=%s WHERE operation_id=%s AND target_id=%s "
                            "AND status='pending'",
                            (previous_stone, occurred_at, operation_id, target_id),
                        )
                        conn.execute(
                            "UPDATE dufang_share_operations SET completed=completed+1,"
                            "status=CASE WHEN completed+1>=total THEN 'completed' ELSE 'running' END,"
                            "updated_at=%s WHERE operation_id=%s",
                            (occurred_at, operation_id),
                        )
                        conn.commit()
                        continue

                    stone_delta = actual_amount if frozen_event_type == "profit" else -actual_amount
                    changed = conn.execute(
                        "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s",
                        (stone_delta, target_id),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        raise RuntimeError("sharing target state changed")
                    wallet_stone = previous_stone + stone_delta
                    received_field = (
                        "received_profit" if frozen_event_type == "profit" else "received_loss"
                    )
                    shared_field = (
                        "shared_profit" if frozen_event_type == "profit" else "shared_loss"
                    )
                    conn.execute(
                        f"INSERT INTO player_data.unseal_data(user_id,{received_field},last_update) "
                        f"VALUES(%s,%s,%s) ON CONFLICT(user_id) DO UPDATE SET {received_field}="
                        f"COALESCE({received_field},0)+EXCLUDED.{received_field},last_update=EXCLUDED.last_update",
                        (target_id, actual_amount, occurred_at),
                    )
                    conn.execute(
                        f"INSERT INTO player_data.unseal_data(user_id,{shared_field},last_update) "
                        f"VALUES(%s,%s,%s) ON CONFLICT(user_id) DO UPDATE SET {shared_field}="
                        f"COALESCE({shared_field},0)+EXCLUDED.{shared_field},last_update=EXCLUDED.last_update",
                        (source_id, actual_amount, occurred_at),
                    )
                    detail = json.dumps(
                        {
                            "source_id": source_id,
                            "event_title": frozen_event_title,
                            "requested_amount": frozen_effect_amount,
                            "previous_stone": previous_stone,
                            "final_stone": wallet_stone,
                        },
                        ensure_ascii=True,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    conn.execute(
                        "INSERT INTO economy_log("
                        "user_id,source,action,stone_delta,item_delta,detail,trace_id,created_at) "
                        "VALUES(%s,'dufang',%s,%s,'[]',%s,%s,%s)",
                        (
                            target_id,
                            f"dufang_share_{frozen_event_type}",
                            stone_delta,
                            detail,
                            operation_id,
                            occurred_at,
                        ),
                    )
                    conn.execute(
                        "UPDATE dufang_share_progress SET status='applied',actual_amount=%s,"
                        "wallet_stone=%s,updated_at=%s WHERE operation_id=%s AND target_id=%s "
                        "AND status='pending'",
                        (actual_amount, wallet_stone, occurred_at, operation_id, target_id),
                    )
                    conn.execute(
                        "UPDATE dufang_share_operations SET completed=completed+1,"
                        "total_amount=total_amount+%s,"
                        "status=CASE WHEN completed+1>=total THEN 'completed' ELSE 'running' END,"
                        "updated_at=%s WHERE operation_id=%s",
                        (actual_amount, occurred_at, operation_id),
                    )
                    conn.commit()
                    applied_now.add(target_id)

                result = self._result(
                    conn,
                    operation_id,
                    "applied" if pending else "duplicate",
                    applied_now,
                )
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

__all__ = [
    "DufangBetResult",
    "DufangBetService",
    "DufangPayoutResult",
    "DufangPayoutService",
    "DufangShareRecipientResult",
    "DufangShareSettlementResult",
    "DufangShareSettlementService",
]
