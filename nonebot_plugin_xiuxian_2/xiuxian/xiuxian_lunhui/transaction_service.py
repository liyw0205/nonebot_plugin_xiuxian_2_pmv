from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend

@dataclass(frozen=True)
class CultivationResetResult:
    status: str
    reset_exp: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class CultivationResetService:
    def __init__(self, database: str | Path, lock: RLock | None = None):
        self._database = Path(database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> CultivationResetResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS cultivation_reset_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,reset_exp INTEGER NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload,reset_exp FROM cultivation_reset_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return CultivationResetResult("duplicate", int(previous[1]))

    def reset(self, operation_id: str, user_id: str, expected_level: str, expected_exp: int):
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_level = str(expected_level)
        expected_exp = int(expected_exp)
        if not operation_id or expected_exp < 0:
            raise ValueError("invalid cultivation reset operation")
        # Request identity only — level/exp are concurrency checks, not the request key.
        payload = json.dumps([user_id], ensure_ascii=False, separators=(",", ":"))

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS cultivation_reset_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,reset_exp INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,reset_exp FROM cultivation_reset_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return CultivationResetResult("operation_conflict")
                    return CultivationResetResult("duplicate", int(previous[1]))

                row = conn.execute(
                    "SELECT level,exp FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return CultivationResetResult("user_missing")
                if str(row[0]) != expected_level or int(row[1]) != expected_exp:
                    conn.rollback()
                    return CultivationResetResult("state_changed")
                if expected_level not in {"感气境初期", "感气境中期", "感气境圆满"}:
                    conn.rollback()
                    return CultivationResetResult("level_rejected")

                changed = conn.execute(
                    "UPDATE user_xiuxian SET level='江湖好手',level_up_rate=0,exp=100,"
                    "power=0,hp=50,mp=100,atk=10 WHERE user_id=%s AND level=%s AND exp=%s",
                    (user_id, expected_level, expected_exp),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return CultivationResetResult("state_changed")
                conn.execute(
                    "INSERT INTO cultivation_reset_operations(operation_id,payload,reset_exp) VALUES(%s,%s,%s)",
                    (operation_id, payload, expected_exp),
                )
                conn.commit()
                return CultivationResetResult("applied", expected_exp)
            except Exception:
                conn.rollback()
                raise

FIELDS = {
    "main_buff": ("retrieved_main", "main_buff"),
    "sub_buff": ("retrieved_sub", "sub_buff"),
    "sec_buff": ("retrieved_sec", "sec_buff"),
    "effect1_buff": ("retrieved_effect1", "effect1_buff"),
    "effect2_buff": ("retrieved_effect2", "effect2_buff"),
}

@dataclass(frozen=True)
class LunhuiRecallResult:
    status: str
    skill_id: int = 0

    @property
    def succeeded(self):
        return self.status in {"applied", "duplicate"}

class LunhuiRecallService:
    def __init__(self, game_db: str | Path, player_db: str | Path, lock: RLock | None = None):
        self.game = Path(game_db)
        self.player = Path(player_db)
        self.lock = lock or RLock()

    def get_result(self, operation_id) -> LunhuiRecallResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self.lock, closing(db_backend.connect(self.game)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS lunhui_recall_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,skill_id INTEGER)"
            )
            old = conn.execute(
                "SELECT payload,skill_id FROM lunhui_recall_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if old is None:
                return None
            return LunhuiRecallResult("duplicate", int(old[1] or 0))

    def recall(self, operation_id, user_id, skill_type, expected_skill_id):
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        skill_type = str(skill_type)
        expected_skill_id = int(expected_skill_id)
        # Request identity only — skill_id is outcome stored in op row.
        payload = json.dumps([user_id, skill_type], ensure_ascii=True, separators=(",", ":"))
        fields = FIELDS.get(skill_type)
        if not fields:
            return LunhuiRecallResult("invalid_type")
        retrieved_field, buff_field = fields
        with self.lock, closing(db_backend.connect(self.game)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player),))
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS lunhui_recall_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT,skill_id INTEGER)"
                )
                old = conn.execute(
                    "SELECT payload,skill_id FROM lunhui_recall_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return LunhuiRecallResult("state_changed")
                    return LunhuiRecallResult("duplicate", int(old[1] or 0))
                memory = conn.execute(
                    f"SELECT {skill_type},{retrieved_field} FROM player_data.reincarnation_memory WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if memory is None or int(memory[0] or 0) != expected_skill_id or int(memory[1] or 0):
                    conn.rollback()
                    return LunhuiRecallResult("state_changed")
                buff = conn.execute("SELECT 1 FROM BuffInfo WHERE user_id=%s", (user_id,)).fetchone()
                if buff is None:
                    conn.rollback()
                    return LunhuiRecallResult("state_changed")
                conn.execute(
                    f"UPDATE player_data.reincarnation_memory SET {retrieved_field}=1 WHERE user_id=%s",
                    (user_id,),
                )
                conn.execute(
                    f"UPDATE BuffInfo SET {buff_field}=%s WHERE user_id=%s",
                    (expected_skill_id, user_id),
                )
                conn.execute(
                    "INSERT INTO lunhui_recall_operations VALUES (%s,%s,%s)",
                    (operation_id, payload, expected_skill_id),
                )
                conn.commit()
                return LunhuiRecallResult("applied", expected_skill_id)
            except Exception:
                conn.rollback()
                raise

ROOTS = {
    6: ("轮回千次不灭，只为臻至巅峰", "轮回道果"),
    7: ("轮回万次不灭，只为超越巅峰", "真·轮回道果"),
    8: ("轮回无尽不灭，只为触及永恒之境", "永恒道果"),
    9: (None, "命运道果"),
}
MEMORY_FIELDS = ("main_buff", "sub_buff", "sec_buff", "effect1_buff", "effect2_buff")

@dataclass(frozen=True)
class LunhuiSettlementResult:
    status: str
    stone: int = 0
    root_level: int = 0
    wishing_stones: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

def _ensure_player_table(conn, table: str, fields: dict[str, str]) -> None:
    table_sql = db_backend.quote_ident(table)
    conn.execute(f"CREATE TABLE IF NOT EXISTS player_data.{table_sql} (user_id TEXT PRIMARY KEY)")
    columns = {
        str(row[1])
        for row in conn.execute(f"PRAGMA player_data.table_info({table_sql})").fetchall()
    }
    for field, data_type in fields.items():
        if field not in columns:
            conn.execute(
                f"ALTER TABLE player_data.{table_sql} ADD COLUMN "
                f"{db_backend.quote_ident(field)} {data_type}"
            )

def _set_player_fields(conn, table: str, user_id: str, values: dict[str, object]) -> None:
    _ensure_player_table(
        conn,
        table,
        {key: "TEXT" if key == "memory_level" else "INTEGER" for key in values},
    )
    table_sql = db_backend.quote_ident(table)
    changed = conn.execute(
        f"UPDATE player_data.{table_sql} SET "
        + ",".join(f"{db_backend.quote_ident(key)}=%s" for key in values)
        + " WHERE user_id=%s",
        tuple(values.values()) + (user_id,),
    )
    if changed.rowcount == 0:
        fields = ["user_id", *values]
        conn.execute(
            f"INSERT INTO player_data.{table_sql}("
            + ",".join(db_backend.quote_ident(field) for field in fields)
            + ") VALUES("
            + ",".join("%s" for _ in fields)
            + ")",
            (user_id, *values.values()),
        )

def _increment_stat(conn, user_id: str, field: str) -> None:
    _ensure_player_table(conn, "statistics", {field: "INTEGER"})
    field_sql = db_backend.quote_ident(field)
    changed = conn.execute(
        f"UPDATE player_data.statistics SET {field_sql}=COALESCE({field_sql},0)+1 WHERE user_id=%s",
        (user_id,),
    )
    if changed.rowcount == 0:
        conn.execute(
            f"INSERT INTO player_data.statistics(user_id,{field_sql}) VALUES(%s,1)",
            (user_id,),
        )

class LunhuiSettlementService:
    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path | None = None,
        impart_database: str | Path | None = None,
        lock: RLock | None = None,
    ):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database) if player_database else None
        self._impart_database = Path(impart_database) if impart_database else None
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> LunhuiSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS lunhui_settlement_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER NOT NULL,"
                "root_level INTEGER NOT NULL,wishing_stones INTEGER NOT NULL DEFAULT 0,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload,stone,root_level,wishing_stones FROM lunhui_settlement_operations "
                "WHERE operation_id=%s", (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return LunhuiSettlementResult(
                "duplicate", int(previous[1]), int(previous[2]), int(previous[3])
            )

    def settle(
        self,
        operation_id,
        user_id,
        expected_level,
        root_key,
        expected_root_type=None,
        reward_id=20025,
        reward_name="灵根改名卡",
        *,
        expected_exp=None,
        expected_stone=None,
        expected_root_level=None,
        expected_buffs=None,
        expected_impart_exp_day=None,
        expected_impart_stone=None,
        user_name="",
    ):
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_level = str(expected_level)
        root_key = int(root_key)
        reward_id = int(reward_id)
        expected_buffs = dict(expected_buffs or {})
        # Request identity only — level/stone/buffs/impart are concurrency checks; outcomes in op columns.
        payload = json.dumps(
            [user_id, root_key], ensure_ascii=False, separators=(",", ":"),
        )
        if not operation_id or root_key not in {0, 6, 7, 8, 9}:
            raise ValueError("invalid reincarnation settlement")

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached_player = attached_impart = False
            try:
                if self._player_database:
                    conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                    attached_player = True
                if self._impart_database:
                    conn.execute("ATTACH DATABASE %s AS impart_data", (str(self._impart_database),))
                    attached_impart = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS lunhui_settlement_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER NOT NULL,"
                    "root_level INTEGER NOT NULL,wishing_stones INTEGER NOT NULL DEFAULT 0,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,stone,root_level,wishing_stones FROM lunhui_settlement_operations "
                    "WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return LunhuiSettlementResult("operation_conflict")
                    return LunhuiSettlementResult("duplicate", int(previous[1]), int(previous[2]), int(previous[3]))

                row = conn.execute(
                    "SELECT level,exp,stone,root_type,root_level,user_name FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return LunhuiSettlementResult("user_missing")
                actual = [str(row[0]), int(row[1]), int(row[2]), str(row[3]), int(row[4] or 0)]
                expected = [
                    expected_level,
                    actual[1] if expected_exp is None else int(expected_exp),
                    actual[2] if expected_stone is None else int(expected_stone),
                    actual[3] if expected_root_type is None else str(expected_root_type),
                    actual[4] if expected_root_level is None else int(expected_root_level),
                ]
                if actual != expected:
                    conn.rollback()
                    return LunhuiSettlementResult("state_changed")

                buff_row = conn.execute(
                    "SELECT main_buff,sub_buff,sec_buff,effect1_buff,effect2_buff FROM BuffInfo WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                buffs = dict(zip(MEMORY_FIELDS, tuple(buff_row or (0, 0, 0, 0, 0))))
                if expected_buffs and {key: int(buffs[key] or 0) for key in MEMORY_FIELDS} != {
                    key: int(expected_buffs.get(key, 0)) for key in MEMORY_FIELDS
                }:
                    conn.rollback()
                    return LunhuiSettlementResult("state_changed")

                impart_exp_day = impart_stone = 0
                if attached_impart:
                    impart = conn.execute(
                        "SELECT COALESCE(exp_day,0),COALESCE(stone_num,0) FROM impart_data.xiuxian_impart WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()
                    impart_exp_day, impart_stone = tuple(impart or (0, 0))
                    if expected_impart_exp_day is not None and int(impart_exp_day) != int(expected_impart_exp_day):
                        conn.rollback(); return LunhuiSettlementResult("state_changed")
                    if expected_impart_stone is not None and int(impart_stone) != int(expected_impart_stone):
                        conn.rollback(); return LunhuiSettlementResult("state_changed")

                root_name, root_type = (str(row[3]), str(row[3])) if root_key == 0 else ROOTS[root_key]
                if root_key == 9:
                    root_name = f"轮回命主·{user_name or row[5] or user_id}"
                new_root_level = actual[4] + (1 if root_key in {0, 9} else 0)
                retained_stone = min(actual[2], 100_000_000)
                practices = "atkpractice=0,hppractice=0,mppractice=0," if root_key != 0 else ""
                conn.execute(
                    "UPDATE user_xiuxian SET level='江湖好手',exp=100,stone=%s,level_up_rate=0,"
                    f"{practices}root=%s,root_type=%s,root_level=%s,power=0,hp=50,mp=100,atk=10 WHERE user_id=%s",
                    (retained_stone, root_name, root_type, new_root_level, user_id),
                )
                conn.execute(
                    "UPDATE BuffInfo SET main_buff=0,sub_buff=0,sec_buff=0,effect1_buff=0,effect2_buff=0 WHERE user_id=%s",
                    (user_id,),
                )
                conn.execute("UPDATE back SET all_num=0 WHERE user_id=%s AND goods_type='丹药'", (user_id,))
                wishing_stones = int(impart_stone) // 100
                if attached_impart:
                    conn.execute(
                        "UPDATE impart_data.xiuxian_impart SET exp_day=0,stone_num=%s WHERE user_id=%s",
                        (0 if wishing_stones else int(impart_stone), user_id),
                    )
                for item_id, item_name, quantity in (
                    (reward_id, reward_name, 1),
                    (20005, "祈愿石", wishing_stones),
                ):
                    if quantity > 0:
                        conn.execute(
                            "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) "
                            "VALUES(%s,%s,%s,'特殊道具',%s,%s) ON CONFLICT(user_id,goods_id) "
                            "DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num,"
                            "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num",
                            (user_id, item_id, item_name, quantity, quantity),
                        )
                if attached_player:
                    memory = {key: int(buffs[key] or 0) for key in MEMORY_FIELDS}
                    memory.update({"memory_level": expected_level, **{f"retrieved_{key.split('_')[0]}": 0 for key in MEMORY_FIELDS}})
                    _set_player_fields(conn, "reincarnation_memory", user_id, memory)
                    _increment_stat(conn, user_id, "轮回次数")
                    _increment_stat(conn, user_id, "无限轮回次数" if root_key in {0, 9} else "普通轮回次数")
                conn.execute(
                    "INSERT INTO lunhui_settlement_operations(operation_id,payload,stone,root_level,wishing_stones) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, retained_stone, new_root_level, wishing_stones),
                )
                conn.commit()
                return LunhuiSettlementResult("applied", retained_stone, new_root_level, wishing_stones)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached_impart:
                    try: conn.execute("DETACH DATABASE impart_data")
                    except Exception: pass
                if attached_player:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass

__all__ = [
    "CultivationResetResult",
    "CultivationResetService",
    "LunhuiRecallResult",
    "LunhuiRecallService",
    "LunhuiSettlementResult",
    "LunhuiSettlementService",
]
