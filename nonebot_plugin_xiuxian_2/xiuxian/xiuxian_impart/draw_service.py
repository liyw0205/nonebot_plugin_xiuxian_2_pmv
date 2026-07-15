from __future__ import annotations

import json
from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ImpartDrawResult:
    status: str
    wish: int = 0
    draw_count: int = 0
    cards: tuple[str, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ImpartDrawService:
    def __init__(self, game_db: str | Path, impart_db: str | Path, lock: RLock | None = None):
        self.game = Path(game_db)
        self.impart = Path(impart_db)
        self.lock = lock or RLock()

    def get_result(self, operation_id: str) -> ImpartDrawResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self.lock, closing(db_backend.connect(self.game)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS impart_draw_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,wish INTEGER,draw_count INTEGER,"
                "cards_json TEXT DEFAULT '[]')"
            )
            # migrate older schema without cards_json
            cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(impart_draw_operations)").fetchall()}
            if "cards_json" not in cols:
                try:
                    conn.execute("ALTER TABLE impart_draw_operations ADD COLUMN cards_json TEXT DEFAULT '[]'")
                except Exception:
                    pass
            old = conn.execute(
                "SELECT wish,draw_count,COALESCE(cards_json,'[]') FROM impart_draw_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if old is None:
                return None
            try:
                cards = tuple(str(c) for c in json.loads(str(old[2] or "[]")))
            except Exception:
                cards = ()
            return ImpartDrawResult("duplicate", int(old[0]), int(old[1]), cards)

    def draw(
        self,
        operation_id,
        user_id,
        expected_stone,
        expected_wish,
        expected_count,
        cost,
        new_wish,
        pulls,
        cards,
    ):
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_stone, expected_wish, expected_count, cost, new_wish, pulls = map(
            int, (expected_stone, expected_wish, expected_count, cost, new_wish, pulls)
        )
        cards = tuple(map(str, cards))
        if not operation_id or cost <= 0 or pulls <= 0:
            raise ValueError("invalid draw request")
        # Request identity only; stone/wish/count/cards are concurrency/outcome.
        payload = json.dumps([user_id, cost, pulls], ensure_ascii=True, separators=(",", ":"))
        with self.lock, closing(db_backend.connect(self.game)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS impart_data", (str(self.impart),))
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_draw_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT,wish INTEGER,draw_count INTEGER,"
                    "cards_json TEXT DEFAULT '[]')"
                )
                cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(impart_draw_operations)").fetchall()}
                if "cards_json" not in cols:
                    try:
                        conn.execute("ALTER TABLE impart_draw_operations ADD COLUMN cards_json TEXT DEFAULT '[]'")
                    except Exception:
                        pass
                old = conn.execute(
                    "SELECT payload,wish,draw_count,COALESCE(cards_json,'[]') FROM impart_draw_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return ImpartDrawResult("state_changed")
                    try:
                        old_cards = tuple(str(c) for c in json.loads(str(old[3] or "[]")))
                    except Exception:
                        old_cards = ()
                    return ImpartDrawResult("duplicate", int(old[1]), int(old[2]), old_cards)
                user = conn.execute(
                    "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                state = conn.execute(
                    "SELECT wish,impart_num FROM impart_data.xiuxian_impart WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None or state is None:
                    conn.rollback()
                    return ImpartDrawResult("state_changed")
                if int(user[0]) != expected_stone or tuple(map(int, state)) != (expected_wish, expected_count):
                    conn.rollback()
                    return ImpartDrawResult("state_changed")
                if expected_stone < cost:
                    conn.rollback()
                    return ImpartDrawResult("stone_missing")
                if expected_count + pulls > 100:
                    conn.rollback()
                    return ImpartDrawResult("limit_exceeded")
                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS INTEGER)-%s "
                    "WHERE user_id=%s AND CAST(COALESCE(stone,0) AS INTEGER)>=%s",
                    (cost, user_id, cost),
                )
                if charged.rowcount != 1:
                    conn.rollback()
                    return ImpartDrawResult("state_changed")
                conn.execute(
                    "UPDATE impart_data.xiuxian_impart SET wish=%s,impart_num=impart_num+%s WHERE user_id=%s",
                    (new_wish, pulls, user_id),
                )
                for card_name, quantity in Counter(cards).items():
                    conn.execute(
                        "INSERT INTO impart_data.impart_cards(user_id,card_name,quantity) VALUES (%s,%s,%s) "
                        "ON CONFLICT(user_id,card_name) DO UPDATE SET "
                        "quantity=impart_cards.quantity+EXCLUDED.quantity",
                        (user_id, card_name, quantity),
                    )
                cards_json = json.dumps(list(cards), ensure_ascii=False, separators=(",", ":"))
                conn.execute(
                    "INSERT INTO impart_draw_operations(operation_id,payload,wish,draw_count,cards_json) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (operation_id, payload, new_wish, pulls, cards_json),
                )
                conn.commit()
                return ImpartDrawResult("applied", new_wish, pulls, cards)
            except Exception:
                conn.rollback()
                raise


__all__ = ["ImpartDrawResult", "ImpartDrawService"]
