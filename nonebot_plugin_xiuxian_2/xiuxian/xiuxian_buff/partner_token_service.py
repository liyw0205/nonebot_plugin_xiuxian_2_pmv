from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PartnerTokenUseResult:
    status: str
    used_tokens: int = 0
    used_count: int = 0
    item_remaining: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PartnerTokenUseService:
    """Atomically consume tokens and persist the authoritative two-exp usage count."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(
        self,
        operation_id,
        user_id,
        item_id,
        *,
        requested_count,
        expected_item_count,
        expected_used_count,
    ) -> PartnerTokenUseResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        requested_count = int(requested_count)
        expected_item_count = int(expected_item_count)
        expected_used_count = int(expected_used_count)
        if not operation_id or requested_count <= 0 or expected_item_count < 0 or expected_used_count < 0:
            raise ValueError("invalid partner token operation")
        payload = json.dumps(
            [user_id, item_id, requested_count, expected_item_count, expected_used_count],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS partner_token_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,used_tokens INTEGER NOT NULL,"
                    "used_count INTEGER NOT NULL,item_remaining INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS player_data.partner_two_exp_usage ("
                    "user_id TEXT PRIMARY KEY,used_count INTEGER NOT NULL DEFAULT 0,"
                    "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,used_tokens,used_count,item_remaining FROM partner_token_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    status = "duplicate" if str(previous[0]) == payload else "operation_conflict"
                    return PartnerTokenUseResult(status, int(previous[1]), int(previous[2]), int(previous[3]))

                usage = conn.execute(
                    "SELECT used_count FROM player_data.partner_two_exp_usage WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if usage is not None and int(usage[0]) != expected_used_count:
                    conn.rollback()
                    return PartnerTokenUseResult("state_changed")
                item = conn.execute(
                    "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None:
                    conn.rollback()
                    return PartnerTokenUseResult("item_missing")
                if int(item[0]) != expected_item_count:
                    conn.rollback()
                    return PartnerTokenUseResult("state_changed")

                used_tokens = min(requested_count, expected_used_count, expected_item_count)
                if used_tokens <= 0:
                    conn.rollback()
                    return PartnerTokenUseResult("limit_full", 0, expected_used_count, expected_item_count)
                new_used_count = expected_used_count - used_tokens
                item_remaining = expected_item_count - used_tokens
                bind_remaining = min(max(0, int(item[1]) - used_tokens), item_remaining)
                changed = conn.execute(
                    "UPDATE back SET goods_num=%s,bind_num=%s WHERE user_id=%s AND goods_id=%s "
                    "AND COALESCE(goods_num,0)=%s",
                    (item_remaining, bind_remaining, user_id, item_id, expected_item_count),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return PartnerTokenUseResult("state_changed")
                if usage is None:
                    conn.execute(
                        "INSERT INTO player_data.partner_two_exp_usage(user_id,used_count) VALUES(%s,%s)",
                        (user_id, new_used_count),
                    )
                else:
                    changed = conn.execute(
                        "UPDATE player_data.partner_two_exp_usage SET used_count=%s "
                        "WHERE user_id=%s AND used_count=%s",
                        (new_used_count, user_id, expected_used_count),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return PartnerTokenUseResult("state_changed")
                conn.execute(
                    "INSERT INTO partner_token_operations(operation_id,payload,used_tokens,used_count,item_remaining) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, used_tokens, new_used_count, item_remaining),
                )
                conn.commit()
                return PartnerTokenUseResult("applied", used_tokens, new_used_count, item_remaining)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass


__all__ = ["PartnerTokenUseResult", "PartnerTokenUseService"]
