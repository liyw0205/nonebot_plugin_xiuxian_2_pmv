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

from .card_bonus import refresh_card_bonuses

@dataclass(frozen=True)
class CardComposeResult:
    status: str
    source_quantity: int = 0
    target_quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class CardComposeService:
    """Consume duplicate cards and create one selected card atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> CardComposeResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS impart_card_compose_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                "source_quantity INTEGER NOT NULL,target_quantity INTEGER NOT NULL)"
            )
            old = conn.execute(
                "SELECT source_quantity,target_quantity FROM "
                "impart_card_compose_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if old is None:
                return None
            return CardComposeResult("duplicate", int(old[0]), int(old[1]))

    def compose(
        self,
        operation_id,
        user_id,
        source_card,
        target_card,
        expected_source_quantity,
        expected_target_quantity,
        cost=5,
        card_definitions=None,
    ) -> CardComposeResult:
        operation_id = str(operation_id).strip()
        user_id, source_card, target_card = map(str, (user_id, source_card, target_card))
        expected_source_quantity = int(expected_source_quantity)
        expected_target_quantity = int(expected_target_quantity)
        cost = int(cost)
        if not operation_id or not source_card or not target_card or cost <= 0:
            raise ValueError("invalid compose request")
        if source_card == target_card:
            return CardComposeResult("same_card")
        # Request identity only.
        payload = json.dumps(
            [user_id, source_card, target_card, cost],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_card_compose_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "source_quantity INTEGER NOT NULL,target_quantity INTEGER NOT NULL)"
                )
                old = conn.execute(
                    "SELECT payload,source_quantity,target_quantity FROM "
                    "impart_card_compose_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    status = "duplicate" if str(old[0]) == payload else "state_changed"
                    return CardComposeResult(status, int(old[1]), int(old[2]))
                source = conn.execute(
                    "SELECT quantity FROM impart_cards WHERE user_id=%s AND card_name=%s",
                    (user_id, source_card),
                ).fetchone()
                target = conn.execute(
                    "SELECT quantity FROM impart_cards WHERE user_id=%s AND card_name=%s",
                    (user_id, target_card),
                ).fetchone()
                source_quantity = int(source[0]) if source else 0
                target_quantity = int(target[0]) if target else 0
                if (source_quantity, target_quantity) != (
                    expected_source_quantity,
                    expected_target_quantity,
                ):
                    conn.rollback()
                    return CardComposeResult("state_changed", source_quantity, target_quantity)
                if source_quantity < cost:
                    conn.rollback()
                    return CardComposeResult("card_missing", source_quantity, target_quantity)
                consumed = conn.execute(
                    "UPDATE impart_cards SET quantity=quantity-%s WHERE user_id=%s "
                    "AND card_name=%s AND quantity=%s",
                    (cost, user_id, source_card, source_quantity),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return CardComposeResult("state_changed")
                conn.execute(
                    "DELETE FROM impart_cards WHERE user_id=%s AND card_name=%s AND quantity=0",
                    (user_id, source_card),
                )
                conn.execute(
                    "INSERT INTO impart_cards(user_id,card_name,quantity) VALUES (%s,%s,1) "
                    "ON CONFLICT(user_id,card_name) DO UPDATE SET quantity=impart_cards.quantity+1",
                    (user_id, target_card),
                )
                new_source, new_target = source_quantity - cost, target_quantity + 1
                if card_definitions is not None:
                    refresh_card_bonuses(conn, user_id, card_definitions)
                conn.execute(
                    "INSERT INTO impart_card_compose_operations VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, new_source, new_target),
                )
                conn.commit()
                return CardComposeResult("applied", new_source, new_target)
            except Exception:
                conn.rollback()
                raise

from .card_bonus import refresh_card_bonuses

@dataclass(frozen=True)
class CardDisassembleResult:
    status: str
    card_quantity: int = 0
    stone_quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class CardDisassembleService:
    """Convert duplicate cards into longing crystals atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> CardDisassembleResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS impart_card_disassemble_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                "card_quantity INTEGER NOT NULL,stone_quantity INTEGER NOT NULL)"
            )
            old = conn.execute(
                "SELECT card_quantity,stone_quantity FROM "
                "impart_card_disassemble_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if old is None:
                return None
            return CardDisassembleResult("duplicate", int(old[0]), int(old[1]))

    def disassemble(
        self,
        operation_id,
        user_id,
        card_name,
        quantity,
        expected_card_quantity,
        expected_stone_quantity,
        reward_per_card=2,
        card_definitions=None,
    ) -> CardDisassembleResult:
        operation_id = str(operation_id).strip()
        user_id, card_name = str(user_id), str(card_name)
        quantity, expected_card_quantity, expected_stone_quantity, reward_per_card = map(
            int, (quantity, expected_card_quantity, expected_stone_quantity, reward_per_card)
        )
        if not operation_id or not card_name or quantity <= 0 or reward_per_card <= 0:
            raise ValueError("invalid disassemble request")
        payload = json.dumps(
            [user_id, card_name, quantity, reward_per_card],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_card_disassemble_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "card_quantity INTEGER NOT NULL,stone_quantity INTEGER NOT NULL)"
                )
                old = conn.execute(
                    "SELECT payload,card_quantity,stone_quantity FROM "
                    "impart_card_disassemble_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    status = "duplicate" if str(old[0]) == payload else "state_changed"
                    return CardDisassembleResult(status, int(old[1]), int(old[2]))
                card = conn.execute(
                    "SELECT quantity FROM impart_cards WHERE user_id=%s AND card_name=%s",
                    (user_id, card_name),
                ).fetchone()
                state = conn.execute(
                    "SELECT stone_num FROM xiuxian_impart WHERE user_id=%s", (user_id,),
                ).fetchone()
                card_quantity = int(card[0]) if card else 0
                if state is None:
                    conn.rollback()
                    return CardDisassembleResult("user_missing", card_quantity)
                stone_quantity = int(state[0] or 0)
                if (card_quantity, stone_quantity) != (
                    expected_card_quantity,
                    expected_stone_quantity,
                ):
                    conn.rollback()
                    return CardDisassembleResult("state_changed", card_quantity, stone_quantity)
                # Keep one copy so an acquired card and its passive bonus cannot disappear.
                if card_quantity - quantity < 1:
                    conn.rollback()
                    return CardDisassembleResult("card_missing", card_quantity, stone_quantity)
                consumed = conn.execute(
                    "UPDATE impart_cards SET quantity=quantity-%s WHERE user_id=%s "
                    "AND card_name=%s AND quantity=%s",
                    (quantity, user_id, card_name, card_quantity),
                )
                rewarded = conn.execute(
                    "UPDATE xiuxian_impart SET stone_num=CAST(COALESCE(stone_num,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s "
                    "AND stone_num=%s",
                    (quantity * reward_per_card, user_id, stone_quantity),
                )
                if consumed.rowcount != 1 or rewarded.rowcount != 1:
                    conn.rollback()
                    return CardDisassembleResult("state_changed")
                new_card = card_quantity - quantity
                new_stone = stone_quantity + quantity * reward_per_card
                if card_definitions is not None:
                    refresh_card_bonuses(conn, user_id, card_definitions)
                conn.execute(
                    "INSERT INTO impart_card_disassemble_operations VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, new_card, new_stone),
                )
                conn.commit()
                return CardDisassembleResult("applied", new_card, new_stone)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class LoveSandUseResult:
    status: str
    gained: int = 0
    stone_num: int = 0
    item_remaining: int = 0

    @property
    def succeeded(self):
        return self.status in {"applied", "duplicate"}

class LoveSandUseService:
    def __init__(self, game_db, impart_db, player_db, lock=None):
        self.game_db, self.impart_db, self.player_db = map(Path, (game_db, impart_db, player_db))
        self.lock = lock or RLock()

    def get_result(self, operation_id: str) -> LoveSandUseResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self.lock, closing(db_backend.connect(self.game_db)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS love_sand_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,gained INTEGER NOT NULL,"
                "stone_num INTEGER NOT NULL,item_remaining INTEGER NOT NULL)"
            )
            old = conn.execute(
                "SELECT gained,stone_num,item_remaining FROM love_sand_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if old is None:
                return None
            return LoveSandUseResult("duplicate", int(old[0]), int(old[1]), int(old[2]))

    def apply(self, operation_id, user_id, item_id, quantity, gained, expected_item_count, expected_stone_num):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        item_id, quantity, gained, expected_item_count, expected_stone_num = map(
            int, (item_id, quantity, gained, expected_item_count, expected_stone_num)
        )
        if not operation_id or quantity <= 0 or gained < 0:
            raise ValueError("invalid love sand request")
        # Request identity only; expected_* and gained outcome not part of key.
        payload = json.dumps(
            [user_id, item_id, quantity],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self.lock, closing(db_backend.connect(self.game_db)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS impart_data", (str(self.impart_db),))
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player_db),))
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS love_sand_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,gained INTEGER NOT NULL,"
                    "stone_num INTEGER NOT NULL,item_remaining INTEGER NOT NULL)"
                )
                conn.execute("CREATE TABLE IF NOT EXISTS player_data.statistics(user_id TEXT PRIMARY KEY)")
                for column in ("思恋流沙使用", "思恋结晶获取"):
                    try:
                        conn.execute(
                            f'ALTER TABLE player_data.statistics ADD COLUMN "{column}" INTEGER DEFAULT 0'
                        )
                    except db_backend.Error:
                        pass
                old = conn.execute(
                    "SELECT payload,gained,stone_num,item_remaining FROM love_sand_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    return LoveSandUseResult(
                        "duplicate" if str(old[0]) == payload else "operation_conflict",
                        int(old[1]),
                        int(old[2]),
                        int(old[3]),
                    )
                item = conn.execute(
                    "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) FROM back "
                    "WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                impart = conn.execute(
                    "SELECT stone_num FROM impart_data.xiuxian_impart WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if (
                    not item
                    or not impart
                    or int(item[0]) != expected_item_count
                    or int(impart[0]) != expected_stone_num
                ):
                    conn.rollback()
                    return LoveSandUseResult("state_changed")
                if expected_item_count < quantity:
                    conn.rollback()
                    return LoveSandUseResult("item_missing")
                remaining, stone_num = expected_item_count - quantity, expected_stone_num + gained
                changed = conn.execute(
                    "UPDATE back SET goods_num=%s,bind_num=%s WHERE user_id=%s AND goods_id=%s "
                    "AND COALESCE(goods_num,0)=%s",
                    (
                        remaining,
                        min(max(0, int(item[1]) - quantity), remaining),
                        user_id,
                        item_id,
                        expected_item_count,
                    ),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return LoveSandUseResult("state_changed")
                conn.execute(
                    "UPDATE impart_data.xiuxian_impart SET stone_num=%s WHERE user_id=%s",
                    (stone_num, user_id),
                )
                conn.execute(
                    'INSERT INTO player_data.statistics(user_id,"思恋流沙使用","思恋结晶获取") '
                    "VALUES (%s,%s,%s) ON CONFLICT(user_id) DO UPDATE SET "
                    '"思恋流沙使用"=COALESCE(statistics."思恋流沙使用",0)+EXCLUDED."思恋流沙使用",'
                    '"思恋结晶获取"=COALESCE(statistics."思恋结晶获取",0)+EXCLUDED."思恋结晶获取"',
                    (user_id, quantity, gained),
                )
                conn.execute(
                    "INSERT INTO love_sand_operations VALUES (%s,%s,%s,%s,%s)",
                    (operation_id, payload, gained, stone_num, remaining),
                )
                conn.commit()
                return LoveSandUseResult("applied", gained, stone_num, remaining)
            except Exception:
                conn.rollback()
                raise

from .card_bonus import refresh_card_bonuses

@dataclass(frozen=True)
class ImpartPrayerSettlementResult:
    status: str
    cards: tuple[str, ...] = ()
    new_cards: tuple[str, ...] = ()
    card_counts: tuple[tuple[str, int], ...] = ()
    item_remaining: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ImpartPrayerSettlementService:
    """Consume wishing stones and persist the fixed card batch atomically."""

    def __init__(
        self,
        game_database: str | Path,
        impart_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._impart_database = Path(impart_database)
        self._lock = lock or RLock()

    @staticmethod
    def _identity(user_id: str, item_id: int, quantity: int) -> str:
        return json.dumps(
            [user_id, item_id, quantity],
            ensure_ascii=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _from_row(status: str, row) -> ImpartPrayerSettlementResult:
        return ImpartPrayerSettlementResult(
            status,
            tuple(str(card) for card in json.loads(str(row[1]))),
            tuple(str(card) for card in json.loads(str(row[2]))),
            tuple((str(name), int(count)) for name, count in json.loads(str(row[3]))),
            int(row[4]),
        )

    @staticmethod
    def _normalize_request(operation_id, user_id, item_id, quantity):
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        item_id = int(item_id)
        quantity = int(quantity)
        if not operation_id or not user_id or item_id <= 0 or quantity <= 0:
            raise ValueError("invalid impart prayer request")
        return operation_id, user_id, item_id, quantity

    def replay(self, operation_id, user_id, item_id, quantity):
        operation_id, user_id, item_id, quantity = self._normalize_request(
            operation_id, user_id, item_id, quantity
        )
        identity = self._identity(user_id, item_id, quantity)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("impart_prayer_operations"):
                return None
            previous = conn.execute(
                "SELECT identity_json,cards_json,new_cards_json,card_counts_json,item_remaining "
                "FROM impart_prayer_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            if str(previous[0]) != identity:
                return ImpartPrayerSettlementResult("operation_conflict")
            return self._from_row("duplicate", previous)

    def settle(
        self,
        operation_id,
        user_id,
        item_id,
        quantity,
        cards,
        card_definitions,
    ) -> ImpartPrayerSettlementResult:
        operation_id, user_id, item_id, quantity = self._normalize_request(
            operation_id, user_id, item_id, quantity
        )
        cards = tuple(str(card).strip() for card in cards)
        definitions = {str(name): dict(definition) for name, definition in dict(card_definitions).items()}
        if len(cards) != quantity or any(not card or card not in definitions for card in cards):
            raise ValueError("card batch must match the prayer quantity and definitions")
        identity = self._identity(user_id, item_id, quantity)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS impart_data", (str(self._impart_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_prayer_operations("
                    "operation_id TEXT PRIMARY KEY,identity_json TEXT NOT NULL,cards_json TEXT NOT NULL,"
                    "new_cards_json TEXT NOT NULL,card_counts_json TEXT NOT NULL,"
                    "item_remaining INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT identity_json,cards_json,new_cards_json,card_counts_json,item_remaining "
                    "FROM impart_prayer_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != identity:
                        return ImpartPrayerSettlementResult("operation_conflict")
                    return self._from_row("duplicate", previous)

                columns = set(conn.column_names("back"))
                bind_expression = ",COALESCE(bind_num,0)" if "bind_num" in columns else ",0"
                item = conn.execute(
                    f"SELECT COALESCE(goods_num,0){bind_expression} FROM back "
                    "WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None or int(item[0]) < quantity:
                    conn.rollback()
                    return ImpartPrayerSettlementResult("item_missing")
                impart_user = conn.execute(
                    "SELECT 1 FROM impart_data.xiuxian_impart WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if impart_user is None:
                    conn.rollback()
                    return ImpartPrayerSettlementResult("user_missing")

                item_count = int(item[0])
                item_remaining = item_count - quantity
                updates = ["goods_num=goods_num-%s"]
                update_params: list[object] = [quantity]
                if "bind_num" in columns:
                    updates.append(
                        "bind_num=CASE WHEN goods_num-%s=0 THEN 0 "
                        "WHEN COALESCE(bind_num,0)>=%s THEN COALESCE(bind_num,0)-%s "
                        "ELSE MIN(COALESCE(bind_num,0),goods_num-%s) END"
                    )
                    update_params.extend((quantity, quantity, quantity, quantity))
                consumed = conn.execute(
                    f"UPDATE back SET {', '.join(updates)} WHERE user_id=%s AND goods_id=%s "
                    "AND COALESCE(goods_num,0)>=%s",
                    (*update_params, user_id, item_id, quantity),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return ImpartPrayerSettlementResult("state_changed")

                existing = {
                    str(row[0]): int(row[1])
                    for row in conn.execute(
                        "SELECT card_name,quantity FROM impart_data.impart_cards WHERE user_id=%s",
                        (user_id,),
                    ).fetchall()
                }
                new_cards = tuple(dict.fromkeys(card for card in cards if card not in existing))
                increments = Counter(cards)
                for card_name, amount in increments.items():
                    conn.execute(
                        "INSERT INTO impart_data.impart_cards(user_id,card_name,quantity) "
                        "VALUES (%s,%s,%s) ON CONFLICT(user_id,card_name) DO UPDATE SET "
                        "quantity=impart_cards.quantity+EXCLUDED.quantity",
                        (user_id, card_name, amount),
                    )
                card_counts = tuple(
                    (card_name, existing.get(card_name, 0) + amount)
                    for card_name, amount in increments.items()
                )
                refresh_card_bonuses(conn, user_id, definitions)

                cards_json = json.dumps(cards, ensure_ascii=False, separators=(",", ":"))
                new_cards_json = json.dumps(new_cards, ensure_ascii=False, separators=(",", ":"))
                counts_json = json.dumps(card_counts, ensure_ascii=False, separators=(",", ":"))
                conn.execute(
                    "INSERT INTO impart_prayer_operations("
                    "operation_id,identity_json,cards_json,new_cards_json,card_counts_json,item_remaining"
                    ") VALUES (%s,%s,%s,%s,%s,%s)",
                    (
                        operation_id,
                        identity,
                        cards_json,
                        new_cards_json,
                        counts_json,
                        item_remaining,
                    ),
                )
                conn.commit()
                return ImpartPrayerSettlementResult(
                    "applied", cards, new_cards, card_counts, item_remaining
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE impart_data")

__all__ = [
    "ImpartDrawResult",
    "ImpartDrawService",
    "CardComposeResult",
    "CardComposeService",
    "CardDisassembleResult",
    "CardDisassembleService",
    "LoveSandUseResult",
    "LoveSandUseService",
    "ImpartPrayerSettlementResult",
    "ImpartPrayerSettlementService",
]
