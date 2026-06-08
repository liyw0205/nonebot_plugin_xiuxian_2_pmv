try:
    import ujson as json
except ImportError:
    import json
import os
import zipfile
import random
import shutil
import string
import time
import queue
import copy
from datetime import datetime, timedelta
from pathlib import Path
import threading
from nonebot.log import logger
from . import db_backend
from .data_source import jsondata
from ..xiuxian_config import XiuConfig, convert_rank
# from .. import DRIVER
from nonebot import get_driver
from .download_xiuxian_data import UpdateManager
from .item_json import Items
from .xn_xiuxian_impart_config import config_impart

WORKDATA = Path() / "data" / "xiuxian" / "work"
DATABASE = Path() / "data" / "xiuxian"
SKILLPATHH = DATABASE / "功法"
WEAPONPATH = DATABASE / "装备"
xiuxian_num = "578043031" # 这里其实是修仙1作者的QQ号
impart_num = "123451234"
trade_num = "123451234"
player_num = "123451234"
current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')


_USER_CD_TEXT_COLUMNS = {"user_id", "create_time", "scheduled_time", "last_check_info_time"}
_BACK_TEXT_COLUMNS = {"user_id", "goods_name", "goods_type", "create_time", "update_time", "remake", "action_time"}
_USER_XIUXIAN_TEXT_COLUMNS = {
    "user_id",
    "root",
    "root_type",
    "level",
    "create_time",
    "user_name",
    "blessed_spot_name",
}


def _compat_column_definition(table_name: str, column_name: str) -> str:
    """Schema补字段兼容：时间/文本字段不能用 INTEGER DEFAULT 0。"""
    text_columns = {
        "user_cd": _USER_CD_TEXT_COLUMNS,
        "back": _BACK_TEXT_COLUMNS,
        "user_xiuxian": _USER_XIUXIAN_TEXT_COLUMNS,
    }
    if column_name in text_columns.get(table_name, set()):
        return "TEXT DEFAULT NULL"
    return "INTEGER DEFAULT 0"


def _quote_ident(name: str) -> str:
    return db_backend.quote_ident(str(name))


class XiuxianDateManage:
    global xiuxian_num
    _instance = {}
    _has_init = {}

    def __new__(cls):
        if cls._instance.get(xiuxian_num) is None:
            cls._instance[xiuxian_num] = super(XiuxianDateManage, cls).__new__(cls)
        return cls._instance[xiuxian_num]

    def __init__(self):
        if not self._has_init.get(xiuxian_num):
            self._has_init[xiuxian_num] = True
            self.database_path = DATABASE
            if not self.database_path.exists():
                self.database_path.mkdir(parents=True)
            self.database_path /= "xiuxian.db"
            self.conn = db_backend.connect(self.database_path, check_same_thread=False)
            self._conn_lock = threading.RLock()
            self.lock = self._conn_lock
            self._business_lock = threading.RLock()
            self._fast_conn_pool = queue.LifoQueue()
            self._fast_conn_count = 0
            self._fast_conn_lock = threading.Lock()
            self._fast_conn_max = max(1, int(os.getenv("XIUXIAN_FAST_DB_POOL_SIZE", "64")))
            self._read_cache = {}
            self._read_cache_lock = threading.RLock()
            self._read_cache_ttl = max(0.0, float(os.getenv("XIUXIAN_READ_CACHE_TTL", "2")))
            logger.opt(colors=True).info(f"<green>修仙数据库已连接！</green>")
            self._check_data()

    def close(self):
        with self._conn_lock:
            if getattr(self, "conn", None):
                self.conn.close()
                self.conn = None
                logger.opt(colors=True).info(f"<green>修仙数据库关闭！</green>")
        while hasattr(self, "_fast_conn_pool"):
            try:
                conn = self._fast_conn_pool.get_nowait()
            except queue.Empty:
                break
            try:
                conn.close()
            except Exception:
                pass

    def _check_data(self):
        """检查数据完整性"""
        with self._conn_lock:
            c = self.conn.cursor()
            self._normalize_legacy_buffinfo_table(c)

            for i in XiuConfig().sql_table:
                if i == "user_xiuxian":
                    try:
                        c.execute(f"select count(1) from {i}")
                    except db_backend.OperationalError:
                        c.execute("""CREATE TABLE "user_xiuxian" (
      "id" INTEGER PRIMARY KEY,
      "user_id" TEXT NOT NULL,
      "sect_id" INTEGER DEFAULT NULL,
      "sect_position" INTEGER DEFAULT NULL,
      "stone" integer DEFAULT 0,
      "root" TEXT,
      "root_type" TEXT,
      "root_level" integer DEFAULT 0,
      "level" TEXT,
      "power" integer DEFAULT 0,
      "create_time" integer,
      "is_sign" integer DEFAULT 0,
      "is_beg" integer DEFAULT 0,
      "is_novice" integer DEFAULT 0,
      "is_ban" integer DEFAULT 0,
      "exp" integer DEFAULT 0,
      "work_num" integer DEFAULT 5,
      "user_name" TEXT DEFAULT NULL,
      "level_up_cd" integer DEFAULT NULL,
      "level_up_rate" integer DEFAULT 0,
      "mixelixir_num" integer DEFAULT 0
    );""")
                elif i == "user_cd":
                    try:
                        c.execute(f"select count(1) from {i}")
                    except db_backend.OperationalError:
                        c.execute("""CREATE TABLE "user_cd" (
      "user_id" TEXT NOT NULL PRIMARY KEY,
      "type" integer DEFAULT 0,
      "create_time" TEXT DEFAULT NULL,
      "scheduled_time" TEXT,
      "last_check_info_time" TEXT DEFAULT NULL
    );""")
                elif i == "sects":
                    try:
                        c.execute(f"select count(1) from {i}")
                    except db_backend.OperationalError:
                        c.execute("""CREATE TABLE "sects" (
      "sect_id" INTEGER PRIMARY KEY,
      "sect_name" TEXT NOT NULL,
      "sect_owner" integer,
      "sect_scale" integer NOT NULL,
      "sect_used_stone" integer,
      "join_open" integer DEFAULT 1,
      "closed" integer DEFAULT 0,
      "combat_power" integer DEFAULT 0,
      "sect_fairyland" integer
    );""")
                elif i == "back":
                    try:
                        c.execute(f"select count(1) from {i}")
                    except db_backend.OperationalError:
                        c.execute("""CREATE TABLE "back" (
      "user_id" TEXT NOT NULL,
      "goods_id" INTEGER NOT NULL,
      "goods_name" TEXT,
      "goods_type" TEXT,
      "goods_num" INTEGER,
      "create_time" TEXT,
      "update_time" TEXT,
      "remake" TEXT,
      "day_num" INTEGER DEFAULT 0,
      "all_num" INTEGER DEFAULT 0,
      "action_time" TEXT,
      "state" INTEGER DEFAULT 0
    );""")
                elif i == "BuffInfo":
                    try:
                        c.execute(f"select count(1) from {i}")
                    except db_backend.OperationalError:
                        c.execute("""CREATE TABLE buffinfo (
      "id" INTEGER PRIMARY KEY,
      "user_id" TEXT DEFAULT 0,
      "main_buff" integer DEFAULT 0,
      "sec_buff" integer DEFAULT 0,
      "effect1_buff" integer DEFAULT 0,
      "effect2_buff" integer DEFAULT 0,
      "faqi_buff" integer DEFAULT 0,
      "fabao_weapon" integer DEFAULT 0,
      "sub_buff" integer DEFAULT 0
    );""")

            for i in XiuConfig().sql_user_xiuxian:
                try:
                    c.execute(f"select {i} from user_xiuxian")
                except db_backend.OperationalError:
                    column_def = _compat_column_definition("user_xiuxian", i)
                    sql = f"ALTER TABLE user_xiuxian ADD COLUMN {i} {column_def};"
                    c.execute(sql)

            for d in XiuConfig().sql_user_cd:
                try:
                    c.execute(f"select {d} from user_cd")
                except db_backend.OperationalError:
                    column_def = _compat_column_definition("user_cd", d)
                    sql = f"ALTER TABLE user_cd ADD COLUMN {d} {column_def};"
                    c.execute(sql)

            for s in XiuConfig().sql_sects:
                try:
                    c.execute(f"select {s} from sects")
                except db_backend.OperationalError:
                    sql = f"ALTER TABLE sects ADD COLUMN {s} INTEGER DEFAULT 0;"
                    c.execute(sql)

            for m in XiuConfig().sql_buff:
                try:
                    c.execute(f"select {m} from BuffInfo")
                except db_backend.OperationalError:
                    sql = f"ALTER TABLE BuffInfo ADD COLUMN {m} INTEGER DEFAULT 0;"
                    c.execute(sql)

            for b in XiuConfig().sql_back:
                try:
                    c.execute(f"select {b} from back")
                except db_backend.OperationalError:
                    column_def = _compat_column_definition("back", b)
                    sql = f"ALTER TABLE back ADD COLUMN {b} {column_def};"
                    c.execute(sql)

            for column in ("create_time", "scheduled_time", "last_check_info_time"):
                self._ensure_text_column(c, "user_cd", column)

            now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            c.execute(
                """UPDATE user_cd
                   SET create_time = NULL
                   WHERE trim(COALESCE(create_time, '')) IN ('', '0')
                """
            )
            c.execute(
                """UPDATE user_cd
                   SET scheduled_time = NULL
                   WHERE trim(COALESCE(scheduled_time, '')) IN ('', '0')
                """
            )
            c.execute(
                """UPDATE user_cd
                   SET last_check_info_time = %s
                   WHERE trim(COALESCE(last_check_info_time, '')) IN ('', '0')
                """,
                (now_time,)
            )
            c.execute("CREATE INDEX IF NOT EXISTS idx_user_xiuxian_user_id ON user_xiuxian(user_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_user_xiuxian_user_name ON user_xiuxian(user_name)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_user_xiuxian_user_stamina ON user_xiuxian(user_stamina)")
            self._ensure_back_unique_index(c)
            self._ensure_tribulation_table(c)

            self._commit_write()

    def _ensure_text_column(self, cur, table_name: str, column_name: str):
        try:
            columns = {row[1] for row in self.conn.table_info(table_name)}
            if column_name not in columns:
                cur.execute(
                    f"ALTER TABLE {_quote_ident(table_name)} "
                    f"ADD COLUMN {_quote_ident(column_name)} TEXT DEFAULT NULL"
                )
        except Exception as e:
            logger.warning(f"兼容 {table_name}.{column_name} 文本字段失败：{e}")

    def _normalize_legacy_buffinfo_table(self, cur):
        """兼容历史大小写不一致的 BuffInfo 表名。"""
        return

    @classmethod
    def close_dbs(cls):
        XiuxianDateManage().close()

    def _ensure_back_unique_index(self, cur):
        """合并历史重复背包行，并确保背包按用户+物品唯一。"""
        cur.execute("PRAGMA index_list(back)")
        if any(row[1] == "idx_back_user_goods_unique" for row in cur.fetchall()):
            return

        max_goods_num = int(XiuConfig().max_goods_num)
        cur.execute(
            """
            WITH normalized AS (
                SELECT
                    rowid AS rid,
                    user_id,
                    goods_id,
                    COALESCE(goods_num, 0) AS goods_num,
                    COALESCE(bind_num, 0) AS bind_num,
                    COALESCE(day_num, 0) AS day_num,
                    COALESCE(all_num, 0) AS all_num,
                    COALESCE(state, 0) AS state,
                    SUM(COALESCE(goods_num, 0)) OVER (PARTITION BY user_id, goods_id) AS total_goods_num,
                    SUM(COALESCE(bind_num, 0)) OVER (PARTITION BY user_id, goods_id) AS total_bind_num,
                    SUM(COALESCE(day_num, 0)) OVER (PARTITION BY user_id, goods_id) AS total_day_num,
                    SUM(COALESCE(all_num, 0)) OVER (PARTITION BY user_id, goods_id) AS total_all_num,
                    SUM(COALESCE(state, 0)) OVER (PARTITION BY user_id, goods_id) AS total_state,
                    FIRST_VALUE(goods_name) OVER (
                        PARTITION BY user_id, goods_id
                        ORDER BY (goods_name IS NOT NULL) DESC, update_time DESC, create_time DESC, rowid DESC
                    ) AS merged_goods_name,
                    FIRST_VALUE(goods_type) OVER (
                        PARTITION BY user_id, goods_id
                        ORDER BY (goods_type IS NOT NULL) DESC, update_time DESC, create_time DESC, rowid DESC
                    ) AS merged_goods_type,
                    MIN(create_time) OVER (PARTITION BY user_id, goods_id) AS merged_create_time,
                    MAX(update_time) OVER (PARTITION BY user_id, goods_id) AS merged_update_time,
                    MAX(action_time) OVER (PARTITION BY user_id, goods_id) AS merged_action_time,
                    FIRST_VALUE(remake) OVER (
                        PARTITION BY user_id, goods_id
                        ORDER BY (remake IS NOT NULL) DESC, update_time DESC, create_time DESC, rowid DESC
                    ) AS merged_remake,
                    ROW_NUMBER() OVER (
                        PARTITION BY user_id, goods_id
                        ORDER BY update_time DESC, create_time DESC, rowid DESC
                    ) AS rn,
                    COUNT(*) OVER (PARTITION BY user_id, goods_id) AS duplicate_count
                FROM back
            ),
            keepers AS (
                SELECT *
                FROM normalized
                WHERE rn = 1 AND duplicate_count > 1
            )
            UPDATE back AS b
            SET goods_name = COALESCE(k.merged_goods_name, b.goods_name),
                goods_type = COALESCE(k.merged_goods_type, b.goods_type),
                goods_num = LEAST(k.total_goods_num, %s),
                bind_num = LEAST(k.total_bind_num, LEAST(k.total_goods_num, %s)),
                day_num = k.total_day_num,
                all_num = k.total_all_num,
                state = LEAST(k.total_state, LEAST(k.total_goods_num, %s)),
                create_time = COALESCE(k.merged_create_time, b.create_time),
                update_time = COALESCE(k.merged_update_time, b.update_time),
                action_time = COALESCE(k.merged_action_time, b.action_time),
                remake = COALESCE(k.merged_remake, b.remake)
            FROM keepers AS k
            WHERE b.rowid = k.rid
            """,
            (max_goods_num, max_goods_num, max_goods_num),
        )
        cur.execute(
            """
            DELETE FROM back
            WHERE rowid IN (
                SELECT rid
                FROM (
                    SELECT rowid AS rid,
                       ROW_NUMBER() OVER (
                           PARTITION BY user_id, goods_id
                           ORDER BY update_time DESC, create_time DESC, rowid DESC
                       ) AS rn
                    FROM back
                ) AS ranked
                WHERE ranked.rn > 1
            )
            """
        )
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_back_user_goods_unique ON back(user_id, goods_id)")

    def _ensure_tribulation_table(self, cur):
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_tribulation (
                user_id TEXT PRIMARY KEY,
                current_rate INTEGER DEFAULT 30,
                heart_devil_count INTEGER DEFAULT 0,
                last_time TEXT DEFAULT NULL,
                next_level TEXT DEFAULT NULL
            )
            """
        )
        required_columns = {
            "current_rate": f"INTEGER DEFAULT {int(XiuConfig().tribulation_base_rate)}",
            "heart_devil_count": "INTEGER DEFAULT 0",
            "last_time": "TEXT DEFAULT NULL",
            "next_level": "TEXT DEFAULT NULL",
        }
        existing_columns = {row[1] for row in self.conn.table_info("user_tribulation")}
        for column, column_def in required_columns.items():
            if column not in existing_columns:
                cur.execute(
                    f"ALTER TABLE user_tribulation ADD COLUMN {_quote_ident(column)} {column_def}"
                )

    def _default_tribulation_info(self):
        return {
            "current_rate": int(XiuConfig().tribulation_base_rate),
            "heart_devil_count": 0,
            "last_time": None,
            "next_level": None,
        }

    def get_user_tribulation_info(self, user_id):
        """获取用户渡劫状态。"""
        default_data = self._default_tribulation_info()
        row = self._read_query(
            """
            SELECT current_rate, heart_devil_count, last_time, next_level
            FROM user_tribulation
            WHERE user_id = %s
            """,
            (str(user_id),),
            one=True,
            dict_row=True,
        )
        if not row:
            return default_data

        data = default_data.copy()
        for key in data:
            if row.get(key) is not None:
                data[key] = row[key]

        try:
            data["current_rate"] = int(data["current_rate"])
        except (TypeError, ValueError):
            data["current_rate"] = default_data["current_rate"]
        try:
            data["heart_devil_count"] = int(data["heart_devil_count"])
        except (TypeError, ValueError):
            data["heart_devil_count"] = default_data["heart_devil_count"]
        data["last_time"] = data["last_time"] or None
        data["next_level"] = data["next_level"] or None
        return data

    def has_user_tribulation_info(self, user_id):
        """判断用户是否已有渡劫状态。"""
        row = self._read_query(
            "SELECT 1 FROM user_tribulation WHERE user_id = %s LIMIT 1",
            (str(user_id),),
            one=True,
        )
        return row is not None

    def save_user_tribulation_info(self, user_id, data):
        """保存用户渡劫状态。"""
        default_data = self._default_tribulation_info()
        try:
            current_rate = int(data.get("current_rate", default_data["current_rate"]))
        except (TypeError, ValueError):
            current_rate = default_data["current_rate"]
        try:
            heart_devil_count = int(data.get("heart_devil_count", default_data["heart_devil_count"]))
        except (TypeError, ValueError):
            heart_devil_count = default_data["heart_devil_count"]
        last_time = data.get("last_time") or None
        next_level = data.get("next_level") or None

        with self._conn_lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                UPDATE user_tribulation
                SET current_rate = %s,
                    heart_devil_count = %s,
                    last_time = %s,
                    next_level = %s
                WHERE user_id = %s
                """,
                (current_rate, heart_devil_count, last_time, next_level, str(user_id)),
            )
            if cur.rowcount == 0:
                cur.execute(
                    """
                    INSERT INTO user_tribulation
                        (user_id, current_rate, heart_devil_count, last_time, next_level)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (str(user_id), current_rate, heart_devil_count, last_time, next_level),
                )
            self._commit_write()

    def clear_user_tribulation_info(self, user_id):
        """清空用户渡劫状态。"""
        with self._conn_lock:
            cur = self.conn.cursor()
            cur.execute("DELETE FROM user_tribulation WHERE user_id = %s", (str(user_id),))
            self._commit_write()

    def _create_user(self, user_id: str, root: str, type: str, power: str, create_time, user_name) -> None:
        """在数据库中创建用户并初始化"""
        with self._conn_lock:
            c = self.conn.cursor()
            sql = (
                "INSERT INTO user_xiuxian "
                "(user_id, stone, root, root_type, root_level, level, power, create_time, "
                "user_name, exp, hp, mp, atk, work_num, sect_id, sect_position, user_stamina, is_novice) "
                "VALUES (%s, 0, %s, %s, 0, '江湖好手', %s, %s, %s, 100, 50, 100, 10, 5, NULL, NULL, %s, 0)"
            )
            c.execute(sql, (user_id, root, type, power, create_time, user_name, XiuConfig().max_stamina))

    def _borrow_fast_conn(self):
        try:
            return self._fast_conn_pool.get_nowait()
        except queue.Empty:
            pass

        with self._fast_conn_lock:
            if self._fast_conn_count < self._fast_conn_max:
                self._fast_conn_count += 1
                try:
                    return db_backend.connect(self.database_path, check_same_thread=False)
                except Exception:
                    self._fast_conn_count -= 1
                    raise

        return self._fast_conn_pool.get()

    def _return_fast_conn(self, conn, reusable: bool = True):
        if not reusable:
            try:
                conn.close()
            except Exception:
                pass
            with self._fast_conn_lock:
                self._fast_conn_count = max(0, self._fast_conn_count - 1)
            return
        self._fast_conn_pool.put(conn)

    def _commit_write(self, conn=None):
        (conn or self.conn).commit()
        self._clear_read_cache()

    def _clear_read_cache(self):
        with self._read_cache_lock:
            self._read_cache.clear()

    def _get_read_cache(self, cache_key):
        if not cache_key or self._read_cache_ttl <= 0:
            return None, False
        now_ts = time.monotonic()
        with self._read_cache_lock:
            cached = self._read_cache.get(cache_key)
            if not cached:
                return None, False
            expires_at, value = cached
            if expires_at <= now_ts:
                self._read_cache.pop(cache_key, None)
                return None, False
            return copy.deepcopy(value), True

    def _set_read_cache(self, cache_key, value, ttl=None):
        if not cache_key:
            return value
        cache_ttl = self._read_cache_ttl if ttl is None else max(0.0, float(ttl))
        if cache_ttl <= 0:
            return value
        with self._read_cache_lock:
            self._read_cache[cache_key] = (time.monotonic() + cache_ttl, copy.deepcopy(value))
        return value

    def _read_query(self, sql, params=None, *, one=False, dict_row=False, cache_key=None, cache_ttl=None):
        cached_value, cached = self._get_read_cache(cache_key)
        if cached:
            return cached_value

        conn = None
        reusable = True
        try:
            conn = self._borrow_fast_conn()
            cur = conn.cursor()
            cur.execute(sql, params or ())
            if one:
                row = cur.fetchone()
                if row is None:
                    result = None
                elif dict_row:
                    result = dict(zip((column[0] for column in cur.description), row))
                else:
                    result = row
            else:
                rows = cur.fetchall()
                if dict_row:
                    columns = [column[0] for column in cur.description]
                    result = [dict(zip(columns, row)) for row in rows]
                else:
                    result = rows
            return self._set_read_cache(cache_key, result, cache_ttl)
        except Exception:
            reusable = False
            raise
        finally:
            if conn is not None:
                self._return_fast_conn(conn, reusable=reusable)

    def today_active_users(self):
        """获取今日活跃用户数（今天有操作记录的用户）"""
        today = datetime.now().strftime('%Y-%m-%d')
        create_date = db_backend.date_expression("create_time")
        sql = f"SELECT COUNT(DISTINCT user_id) FROM user_cd WHERE {create_date} = %s"
        result = self._read_query(sql, (today,), one=True, cache_key=("today_active_users", today), cache_ttl=10)
        return result[0] if result else 0

    def yesterday_active_users(self):
        """获取昨日活跃用户数（昨天有操作记录的用户）"""
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        create_date = db_backend.date_expression("create_time")
        sql = f"SELECT COUNT(DISTINCT user_id) FROM user_cd WHERE {create_date} = %s"
        result = self._read_query(sql, (yesterday,), one=True, cache_key=("yesterday_active_users", yesterday), cache_ttl=30)
        return result[0] if result else 0

    def last_7days_active_users(self):
        """获取近七日活跃用户数（最近7天内有操作记录的用户）"""
        seven_days_ago = (datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')
        create_date = db_backend.date_expression("create_time")
        sql = f"SELECT COUNT(DISTINCT user_id) FROM user_cd WHERE {create_date} >= %s"
        result = self._read_query(sql, (seven_days_ago,), one=True, cache_key=("last_7days_active_users", seven_days_ago), cache_ttl=30)
        return result[0] if result else 0

    def all_users(self):
        """获取全部用户数"""
        result = self._read_query("SELECT COUNT(*) FROM user_xiuxian", one=True, cache_key=("all_users",), cache_ttl=10)
        return result[0] if result else 0

    def get_user_count_by_level(self, level_name: str) -> int:
        """查询指定境界的人数"""
        result = self._read_query(
            "SELECT COUNT(*) FROM user_xiuxian WHERE level = %s",
            (level_name,),
            one=True,
            cache_key=("get_user_count_by_level", level_name),
            cache_ttl=10,
        )
        return result[0] if result else 0

    def get_top_users_by_level(self, level_name: str, limit: int = 10):
        """获取指定大境界或小境界修为最高的用户列表"""
        level_name = str(level_name or "").strip()
        if not level_name:
            return []

        try:
            limit = int(limit)
        except (TypeError, ValueError):
            limit = 10
        limit = max(1, min(limit, 50))

        _, rank_list = convert_rank('江湖好手')
        exact_match = level_name in rank_list
        if exact_match:
            sql = """
                SELECT * FROM user_xiuxian
                WHERE level = %s AND user_name IS NOT NULL
                ORDER BY exp DESC
                LIMIT %s
            """
            return self._read_query(sql, (level_name, limit), dict_row=True)

        possible_prefixes = [level_name]
        if not level_name.endswith("境"):
            possible_prefixes.append(f"{level_name}境")

        matched_levels = []
        for prefix in possible_prefixes:
            matched_levels = [rank for rank in rank_list if rank.startswith(prefix)]
            if matched_levels:
                break

        if not matched_levels:
            return []

        placeholders = ",".join(["%s"] * len(matched_levels))
        sql = f"""
            SELECT * FROM user_xiuxian
            WHERE level IN ({placeholders}) AND user_name IS NOT NULL
            ORDER BY exp DESC
            LIMIT %s
        """
        return self._read_query(sql, (*matched_levels, limit), dict_row=True)

    def total_items_quantity(self):
        """获取全部用户背包的物品数量总合"""
        result = self._read_query("SELECT SUM(goods_num) FROM back", one=True, cache_key=("total_items_quantity",), cache_ttl=10)
        return result[0] if result and result[0] is not None else 0

    def get_user_info_with_id(self, user_id):
        """根据USER_ID获取用户信息,不获取功法加成"""
        return self._read_query(
            "select * from user_xiuxian WHERE user_id=%s",
            (user_id,),
            one=True,
            dict_row=True,
        )

    def get_user_info_with_name(self, user_id):
        """根据user_name获取用户信息"""
        return self._read_query(
            "select * from user_xiuxian WHERE user_name=%s",
            (user_id,),
            one=True,
            dict_row=True,
        )

    def update_all_users_stamina(self, max_stamina, stamina):
        """体力未满用户更新体力值。"""
        conn = db_backend.connect(self.database_path, check_same_thread=False)
        total_updated = 0
        batch_size = max(1, int(os.getenv("XIUXIAN_STAMINA_RECOVERY_BATCH_SIZE", "1000")))

        try:
            while True:
                cur = conn.cursor()
                sql = """
                    WITH pending AS (
                        SELECT id
                        FROM user_xiuxian
                        WHERE user_stamina < %s
                        ORDER BY id
                        LIMIT %s
                    )
                    UPDATE user_xiuxian AS ux
                    SET user_stamina = LEAST(ux.user_stamina + %s, %s)
                    WHERE ux.id IN (SELECT id FROM pending)
                """
                cur.execute(sql, (max_stamina, batch_size, stamina, max_stamina))
                updated = max(cur.rowcount, 0)
                self._commit_write(conn)
                total_updated += updated
                if updated < batch_size:
                    return total_updated
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()

    def update_user_stamina(self, user_id, stamina_change, key):
        """更新用户体力值 1为增加，2为减少"""
        with self._conn_lock:
            cur = self.conn.cursor()
            max_stamina = XiuConfig().max_stamina

            if key == 1:
                cur.execute("SELECT user_stamina FROM user_xiuxian WHERE user_id=%s", (user_id,))
                current_stamina = cur.fetchone()[0]
                new_stamina = min(current_stamina + stamina_change, max_stamina)
                if current_stamina < max_stamina:
                    sql = "UPDATE user_xiuxian SET user_stamina=%s WHERE user_id=%s"
                    cur.execute(sql, (new_stamina, user_id))
                    self._commit_write()

            elif key == 2:
                sql = "UPDATE user_xiuxian SET user_stamina=GREATEST(user_stamina-%s, 0) WHERE user_id=%s"
                cur.execute(sql, (stamina_change, user_id))
                self._commit_write()

    def get_user_real_info(self, user_id):
        """根据USER_ID获取用户信息,获取功法加成"""
        conn = None
        reusable = True
        try:
            conn = self._borrow_fast_conn()
            cur = conn.cursor()
            cur.execute("select * from user_xiuxian WHERE user_id=%s", (user_id,))
            result = cur.fetchone()
            if result:
                return final_user_data(result, cur.description)
            return None
        except Exception:
            reusable = False
            raise
        finally:
            if conn is not None:
                self._return_fast_conn(conn, reusable=reusable)

    def get_player_data(self, user_id, boss=False):
        """根据USER_ID获取用户信息,获取属性"""
        player = {"user_id": None, "道号": None, "气血": None, "攻击": None, "真元": None, '会心': None, '防御': 0}
        userinfo = sql_message.get_user_real_info(user_id)
        user_weapon_data = UserBuffDate(userinfo['user_id']).get_user_weapon_data()

        impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
        boss_atk = impart_data['boss_atk'] if impart_data and impart_data['boss_atk'] is not None else 0
        impart_atk_per = impart_data['impart_atk_per'] if impart_data is not None else 0
        user_armor_data = UserBuffDate(userinfo['user_id']).get_user_armor_buff_data()
        user_main_data = UserBuffDate(userinfo['user_id']).get_user_main_buff_data()
        user1_sub_buff_data = UserBuffDate(userinfo['user_id']).get_user_sub_buff_data()
        integral_buff = user1_sub_buff_data['integral'] if user1_sub_buff_data is not None else 0
        exp_buff = user1_sub_buff_data['exp'] if user1_sub_buff_data is not None else 0

        if user_main_data != None:
            main_crit_buff = user_main_data['crit_buff']
        else:
            main_crit_buff = 0

        if user_armor_data != None:
            armor_crit_buff = user_armor_data['crit_buff']
        else:
            armor_crit_buff = 0

        if user_weapon_data != None:
            player['会心'] = int(((user_weapon_data['crit_buff']) + (armor_crit_buff) + (main_crit_buff)) * 100)
        else:
            player['会心'] = (armor_crit_buff + main_crit_buff) * 100

        player['user_id'] = userinfo['user_id']
        player['道号'] = userinfo['user_name']
        player['气血'] = userinfo['hp']
        if boss:
            player['攻击'] = int(userinfo['atk'] * (1 + boss_atk))
        else:
            player['攻击'] = int(userinfo['atk'])
        player['真元'] = userinfo['mp']
        player['exp'] = userinfo['exp']
        return player

    def get_sect_info(self, sect_id):
        """
        通过宗门编号获取宗门信息
        """
        return self._read_query(
            "select * from sects WHERE sect_id=%s",
            (sect_id,),
            one=True,
            dict_row=True,
        )

    def get_sect_owners(self):
        """获取所有宗主的 user_id"""
        result = self._read_query(
            "SELECT user_id FROM user_xiuxian WHERE sect_position = 0",
            cache_key=("get_sect_owners",),
            cache_ttl=10,
        )
        return [row[0] for row in result]

    def get_elders(self):
        """获取所有长老的 user_id"""
        result = self._read_query(
            "SELECT user_id FROM user_xiuxian WHERE sect_position = 2",
            cache_key=("get_elders",),
            cache_ttl=10,
        )
        return [row[0] for row in result]

    def create_user(self, user_id, *args):
        """校验用户是否存在"""
        with self._business_lock, self._conn_lock:
            cur = self.conn.cursor()
            sql = "SELECT 1 FROM user_xiuxian WHERE user_id=%s LIMIT 1"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if not result:
                self._create_user(user_id, args[0], args[1], args[2], args[3], args[4])
                self._commit_write()
                welcome_msg = f"欢迎进入修仙世界的，你的灵根为：{args[0]},类型是：{args[1]},你的战力为：{args[2]},当前境界：江湖好手"
                return True, welcome_msg
            else:
                return False, f"您已迈入修仙世界，输入【我的修仙信息】获取数据吧！"

    def create_user_fast(self, user_id, *args):
        """注册专用快路径：独立连接短事务，避免压测时被全局连接锁串行化。"""
        conn = None
        reusable = True
        try:
            conn = self._borrow_fast_conn()
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s LIMIT 1", (user_id,))
            if cur.fetchone():
                return False, f"您已迈入修仙世界，输入【我的修仙信息】获取数据吧！"
            cur.execute("SELECT 1 FROM user_xiuxian WHERE user_name=%s LIMIT 1", (args[4],))
            if cur.fetchone():
                return None, "道号已存在，请重试"

            sql = (
                "INSERT INTO user_xiuxian "
                "(user_id, stone, root, root_type, root_level, level, power, create_time, "
                "user_name, exp, hp, mp, atk, work_num, sect_id, sect_position, user_stamina, is_novice) "
                "VALUES (%s, 0, %s, %s, 0, '江湖好手', %s, %s, %s, 100, 50, 100, 10, 5, NULL, NULL, %s, 0)"
            )
            cur.execute(sql, (user_id, args[0], args[1], args[2], args[3], args[4], XiuConfig().max_stamina))
            conn.commit()
            welcome_msg = f"欢迎进入修仙世界的，你的灵根为：{args[0]},类型是：{args[1]},你的战力为：{args[2]},当前境界：江湖好手"
            return True, welcome_msg
        except Exception as exc:
            reusable = False
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.warning(f"注册快路径失败，回退普通注册: {exc}")
            return self.create_user(user_id, *args)
        finally:
            if conn is not None:
                self._return_fast_conn(conn, reusable=reusable)

    def get_user_info_with_id_fast(self, user_id):
        """独立连接查询用户，供高并发入口避开主连接锁。"""
        conn = None
        reusable = True
        try:
            conn = self._borrow_fast_conn()
            cur = conn.cursor()
            cur.execute("SELECT * FROM user_xiuxian WHERE user_id=%s", (user_id,))
            result = cur.fetchone()
            if result:
                columns = [column[0] for column in cur.description]
                return dict(zip(columns, result))
            return None
        except Exception as exc:
            reusable = False
            logger.warning(f"用户快查失败，回退普通查询: {exc}")
            return self.get_user_info_with_id(user_id)
        finally:
            if conn is not None:
                self._return_fast_conn(conn, reusable=reusable)

    def get_sign(self, user_id):
        """获取用户签到信息"""
        with self._business_lock, self._conn_lock:
            cur = self.conn.cursor()
            sql = "select is_sign from user_xiuxian WHERE user_id=%s"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if not result:
                return f"修仙界没有你的足迹，输入 我要修仙 加入修仙世界吧！"
            elif result[0] == 0:
                ls = random.randint(XiuConfig().sign_in_lingshi_lower_limit, XiuConfig().sign_in_lingshi_upper_limit)
                sql2 = f"UPDATE user_xiuxian SET is_sign=1,stone=stone+%s WHERE user_id=%s"
                cur.execute(sql2, (ls, user_id))
                self._commit_write()
                return f"签到成功，获取{ls}块灵石!"
            elif result[0] == 1:
                return f"贪心的人是不会有好运的！"

    def get_beg(self, user_id):
        """获取仙途奇缘信息"""
        with self._business_lock, self._conn_lock:
            cur = self.conn.cursor()
            sql = f"select is_beg from user_xiuxian WHERE user_id=%s"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if result[0] == 0:
                ls = random.randint(XiuConfig().beg_lingshi_lower_limit, XiuConfig().beg_lingshi_upper_limit)
                sql2 = f"UPDATE user_xiuxian SET is_beg=1,stone=stone+%s WHERE user_id=%s"
                cur.execute(sql2, (ls, user_id))
                self._commit_write()
                return ls
            elif result[0] == 1:
                return None

    def get_novice(self, user_id):
        """检查用户是否已领取新手礼包"""
        result = self._read_query("select is_novice from user_xiuxian WHERE user_id=%s", (user_id,), one=True)
        if result and result[0] == 0:
            return True
        if result and result[0] == 1:
            return None

    def save_novice(self, user_id):
        """标记用户已领取新手礼包"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET is_novice=1 WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self._commit_write()

    def novice_remake(self):
        """重置新手礼包"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET is_novice=0"
            cur = self.conn.cursor()
            cur.execute(sql)
            self._commit_write()

    def get_user_create_time(self, user_id):
        """获取用户创建时间"""
        result = self._read_query("SELECT create_time FROM user_xiuxian WHERE user_id=%s", (user_id,), one=True)
        if result and result[0]:
            return _safe_parse_dt(result[0])
        return None

    def ramaker(self, lg, type, user_id):
        """洗灵根"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE user_xiuxian SET root=%s,root_type=%s,stone=stone-%s WHERE user_id=%s"
            cur.execute(sql, (lg, type, XiuConfig().remake, user_id))
            self._commit_write()

            self.update_power2(user_id)
            return f"逆天之行，重获新生，新的灵根为：{lg}，类型为：{type}"

    def get_root_rate(self, name, user_id):
        """获取灵根倍率"""
        data = jsondata.root_data()
        if name == '命运道果':
            # 基础加成（永恒道果的7.0）
            base_rate = data['永恒道果']['type_speeds']
            # 命运道果的基础增量系数（2.0）
            current_step_bonus = data[name]['type_speeds']
            
            user_info = self.get_user_info_with_id(user_id)
            root_level = int((user_info or {}).get('root_level', 0))
            
            total_bonus = 0.0
            remaining_levels = root_level
            
            # 阶梯式计算循环
            while remaining_levels > 0:
                # 每个阶段计算5级
                levels_in_this_step = min(remaining_levels, 5)
                total_bonus += levels_in_this_step * current_step_bonus
                remaining_levels -= levels_in_this_step
                
                # 进入下一阶段，系数减少0.3，最低0.5
                current_step_bonus = round(max(0.5, current_step_bonus - 0.3), 2)
                
                # 如果系数已经到0.5了，剩下的等级可以直接批量计算，跳出循环
                if current_step_bonus <= 0.5:
                    total_bonus += remaining_levels * 0.5
                    break
            
            return base_rate + total_bonus
        else:
            return data[name]['type_speeds']

    def get_level_power(self, name):
        """获取境界倍率|exp"""
        data = jsondata.level_data()
        return data[name]['power']

    def get_level_cost(self, name):
        """获取炼体境界倍率"""
        data = jsondata.exercises_level_data()
        return data[name]['cost_exp'], data[name]['cost_stone']

    def update_power2(self, user_id) -> None:
        """更新战力"""
        with self._conn_lock:
            UserMessage = self.get_user_info_with_id(user_id)
            cur = self.conn.cursor()
            level = jsondata.level_data()
            root_rate = sql_message.get_root_rate(UserMessage['root_type'], user_id)
            sql = f"UPDATE user_xiuxian SET power=round(exp*%s*%s,0) WHERE user_id=%s"
            cur.execute(sql, (root_rate, level[UserMessage['level']]["spend"], user_id))
            self._commit_write()

    def update_ls(self, user_id, price, key):
        """更新灵石 1增加 2减少"""
        with self._conn_lock:
            cur = self.conn.cursor()
            price = abs(int(price))
            if key == 1:
                sql = "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s"
                cur.execute(sql, (price, user_id))
            elif key == 2:
                sql = "UPDATE user_xiuxian SET stone=GREATEST(stone-%s, 0) WHERE user_id=%s"
                cur.execute(sql, (price, user_id))
            self._commit_write()

    def try_update_ls(self, user_id, price, key):
        """更新灵石并返回是否成功；扣减时要求余额足够。"""
        with self._conn_lock:
            cur = self.conn.cursor()
            price = abs(int(price))
            if price <= 0:
                return True
            if key == 1:
                sql = "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s"
                cur.execute(sql, (price, user_id))
            elif key == 2:
                sql = "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND COALESCE(stone, 0) >= %s"
                cur.execute(sql, (price, user_id, price))
            else:
                return False
            success = cur.rowcount > 0
            self._commit_write()
            return success

    def update_exp(self, user_id, exp):
        """增加修为"""
        with self._conn_lock:
            exp = number_count(exp)
            sql = "UPDATE user_xiuxian SET exp=exp+%s WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (exp, user_id))
            self._commit_write()

    def update_j_exp(self, user_id, exp):
        """减少修为"""
        with self._conn_lock:
            exp = number_count(exp)
            sql = "UPDATE user_xiuxian SET exp=GREATEST(exp-%s, 0) WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (exp, user_id))
            self._commit_write()

    def update_root(self, user_id, key):
        """更新灵根"""
        with self._conn_lock:
            cur = self.conn.cursor()
            if int(key) == 1:
                sql = f"UPDATE user_xiuxian SET root=%s,root_type=%s WHERE user_id=%s"
                cur.execute(sql, ("全属性灵根", "混沌灵根", user_id))
                root_name = "混沌灵根"
                self._commit_write()

            elif int(key) == 2:
                sql = f"UPDATE user_xiuxian SET root=%s,root_type=%s WHERE user_id=%s"
                cur.execute(sql, ("融合万物灵根", "融合灵根", user_id))
                root_name = "融合灵根"
                self._commit_write()

            elif int(key) == 3:
                sql = f"UPDATE user_xiuxian SET root=%s,root_type=%s WHERE user_id=%s"
                cur.execute(sql, ("月灵根", "超灵根", user_id))
                root_name = "超灵根"
                self._commit_write()

            elif int(key) == 4:
                sql = f"UPDATE user_xiuxian SET root=%s,root_type=%s WHERE user_id=%s"
                cur.execute(sql, ("言灵灵根", "龙灵根", user_id))
                root_name = "龙灵根"
                self._commit_write()

            elif int(key) == 5:
                sql = f"UPDATE user_xiuxian SET root=%s,root_type=%s WHERE user_id=%s"
                cur.execute(sql, ("金灵根", "天灵根", user_id))
                root_name = "天灵根"
                self._commit_write()

            elif int(key) == 6:
                sql = f"UPDATE user_xiuxian SET root=%s,root_type=%s WHERE user_id=%s"
                cur.execute(sql, ("轮回千次不灭，只为臻至巅峰", "轮回道果", user_id))
                root_name = "轮回道果"
                self._commit_write()

            elif int(key) == 7:
                sql = f"UPDATE user_xiuxian SET root=%s,root_type=%s WHERE user_id=%s"
                cur.execute(sql, ("轮回万次不灭，只为超越巅峰", "真·轮回道果", user_id))
                root_name = "真·轮回道果"
                self._commit_write()

            elif int(key) == 8:
                sql = f"UPDATE user_xiuxian SET root=%s,root_type=%s WHERE user_id=%s"
                cur.execute(sql, ("轮回无尽不灭，只为触及永恒之境", "永恒道果", user_id))
                root_name = "永恒道果"
                self._commit_write()

            elif int(key) == 9:
                user_info = sql_message.get_user_info_with_id(user_id)
                sql = f"UPDATE user_xiuxian SET root=%s,root_type=%s WHERE user_id=%s"
                cur.execute(sql, (f"轮回命主·{user_info['user_name']}", "命运道果", user_id))
                root_name = "命运道果"
                self._commit_write()

            return root_name

    def update_root_name(self, user_id, root_name):
        """自定义灵根名称，不改变灵根类型和倍率。"""
        with self._conn_lock:
            sql = "UPDATE user_xiuxian SET root=%s WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (root_name, user_id))
            self._commit_write()
            self.update_power2(user_id)
            return f"灵根已改名为：{root_name}"

    def update_ls_all(self, price):
        """所有用户增加灵石"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE user_xiuxian SET stone=stone+%s"
            cur.execute(sql, (price,))
            self._commit_write()

    def get_exp_rank(self, user_id):
        """修为排行"""
        sql = "SELECT rank_value FROM (SELECT user_id, exp, dense_rank() OVER (ORDER BY exp DESC) AS rank_value FROM user_xiuxian) AS ranked_users WHERE user_id=%s"
        return self._read_query(sql, (user_id,), one=True)

    def get_stone_rank(self, user_id):
        """灵石排行"""
        sql = "SELECT rank_value FROM (SELECT user_id, stone, dense_rank() OVER (ORDER BY stone DESC) AS rank_value FROM user_xiuxian) AS ranked_users WHERE user_id=%s"
        return self._read_query(sql, (user_id,), one=True)

    def get_ls_rank(self):
        """灵石排行榜"""
        sql = f"SELECT user_id,stone FROM user_xiuxian  WHERE stone>0 ORDER BY stone DESC LIMIT 5"
        return self._read_query(sql)

    def sign_remake(self):
        """重置签到"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET is_sign=0"
            cur = self.conn.cursor()
            cur.execute(sql)
            self._commit_write()

    def beg_remake(self):
        """重置仙途奇缘"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET is_beg=0"
            cur = self.conn.cursor()
            cur.execute(sql)
            self._commit_write()

    def ban_user(self, user_id):
        """将用户关进小黑屋"""
        with self._conn_lock:
            cur = self.conn.cursor()
            cur.execute("SELECT is_ban FROM user_xiuxian WHERE user_id = %s", (user_id,))
            result = cur.fetchone()
            if not result:
                return False
            if result[0] == 1:
                return False
            sql = "UPDATE user_xiuxian SET is_ban = 1 WHERE user_id = %s"
            cur.execute(sql, (user_id,))
            self._commit_write()
            return True

    def unban_user(self, user_id):
        """解除用户小黑屋状态"""
        with self._conn_lock:
            cur = self.conn.cursor()
            cur.execute("SELECT is_ban FROM user_xiuxian WHERE user_id = %s", (user_id,))
            result = cur.fetchone()
            if not result:
                return False
            if result[0] == 0:
                return False
            sql = "UPDATE user_xiuxian SET is_ban = 0 WHERE user_id = %s"
            cur.execute(sql, (user_id,))
            self._commit_write()
            return True

    def update_mixelixir_num(self, user_id):
        """增加炼丹次数"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET mixelixir_num=mixelixir_num+1 WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self._commit_write()

    def update_user_name(self, user_id, user_name):
        """更新用户道号"""
        with self._conn_lock:
            cur = self.conn.cursor()
            get_name = f"select user_name from user_xiuxian WHERE user_name=%s"
            cur.execute(get_name, (user_name,))
            result = cur.fetchone()
            if result:
                return "已存在该道号！"
            else:
                sql = f"UPDATE user_xiuxian SET user_name=%s WHERE user_id=%s"
                cur.execute(sql, (user_name, user_id))
                self._commit_write()
                return '道友的道号更新成啦~'

    def updata_level_cd(self, user_id):
        """更新突破境界CD"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET level_up_cd=%s WHERE user_id=%s"
            cur = self.conn.cursor()
            now_time = datetime.now()
            cur.execute(sql, (now_time, user_id))
            self._commit_write()

    def update_last_check_info_time(self, user_id):
        """更新查看修仙信息时间"""
        with self._conn_lock:
            sql = "UPDATE user_cd SET last_check_info_time = %s WHERE user_id = %s"
            cur = self.conn.cursor()
            now_time = datetime.now()
            cur.execute(sql, (now_time, user_id))
            self._commit_write()

    def get_last_check_info_time(self, user_id):
        """获取最后一次查看修仙信息时间"""
        sql = "SELECT last_check_info_time FROM user_cd WHERE user_id = %s"
        result = self._read_query(sql, (user_id,), one=True)
        if result and result[0]:
            return _safe_parse_dt(result[0])
        return None

    def updata_level(self, user_id, level_name):
        """更新境界"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET level=%s WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (level_name, user_id))
            self._commit_write()

    def updata_root_level(self, user_id, level_num):
        """更新轮回等级"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET root_level=root_level+%s WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (level_num, user_id))
            self._commit_write()

    def get_user_cd(self, user_id):
        """获取用户操作CD"""
        with self._conn_lock:
            sql = f"SELECT * FROM user_cd  WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if result:
                columns = [column[0] for column in cur.description]
                user_cd_dict = dict(zip(columns, result))
                return user_cd_dict
            else:
                self.insert_user_cd(user_id)
                return None

    def insert_user_cd(self, user_id) -> None:
        """添加用户至CD表"""
        with self._conn_lock:
            sql = f"INSERT INTO user_cd (user_id) VALUES (%s)"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self._commit_write()

    def create_sect(self, user_id, sect_name) -> None:
        """创建宗门"""
        with self._conn_lock:
            sql = f"INSERT INTO sects(sect_name, sect_owner, sect_scale, sect_used_stone, join_open, closed, combat_power) VALUES (%s,%s,0,0,1,0,0)"
            cur = self.conn.cursor()
            cur.execute(sql, (sect_name, user_id))
            self._commit_write()

    def update_sect_name(self, sect_id, sect_name) -> None:
        """修改宗门名称"""
        with self._conn_lock:
            cur = self.conn.cursor()
            get_sect_name = f"select sect_name from sects WHERE sect_name=%s"
            cur.execute(get_sect_name, (sect_name,))
            result = cur.fetchone()
            if result:
                return False
            else:
                sql = f"UPDATE sects SET sect_name=%s WHERE sect_id=%s"
                cur.execute(sql, (sect_name, sect_id))
                self._commit_write()
                return True

    def get_sect_info_by_qq(self, user_id):
        """通过用户qq获取宗门信息"""
        return self._read_query(
            "select * from sects WHERE sect_owner=%s",
            (user_id,),
            one=True,
            dict_row=True,
        )

    def calculate_sect_combat_power(self, sect_id):
        """计算宗门战力"""
        members = self.get_all_users_by_sect_id(sect_id)
        total_power = 0
        for member in members:
            user_real_info = self.get_user_real_info(member['user_id'])
            if user_real_info and 'power' in user_real_info:
                total_power += user_real_info['power']
        return total_power

    def update_sect_combat_power(self, sect_id):
        """更新宗门战力"""
        with self._conn_lock:
            total_power = self.calculate_sect_combat_power(sect_id)
            sql = "UPDATE sects SET combat_power = %s WHERE sect_id = %s"
            cur = self.conn.cursor()
            cur.execute(sql, (total_power, sect_id))
            self._commit_write()
            return total_power

    def combat_power_top(self):
        """宗门战力排行榜"""
        sql = f"SELECT sect_id, sect_name, combat_power FROM sects WHERE sect_owner is NOT NULL ORDER BY combat_power DESC LIMIT 50"
        return self._read_query(sql)

    def get_sect_info_by_id(self, sect_id):
        """通过宗门id获取宗门信息"""
        return self._read_query(
            "select * from sects WHERE sect_id=%s",
            (sect_id,),
            one=True,
            dict_row=True,
        )

    def update_usr_sect(self, user_id, usr_sect_id, usr_sect_position):
        """更新用户信息表的宗门信息字段"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET sect_id=%s,sect_position=%s WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (usr_sect_id, usr_sect_position, user_id))
            self._commit_write()

    def update_sect_owner(self, user_id, sect_id):
        """更新宗门所有者"""
        with self._conn_lock:
            sql = f"UPDATE sects SET sect_owner=%s WHERE sect_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id, sect_id))
            self._commit_write()

    def get_highest_contrib_user_except_current(self, sect_id, current_owner_id):
        """获取指定宗门的贡献最高的人，排除当前宗主"""
        sql = """
        SELECT user_id
        FROM user_xiuxian
        WHERE sect_id = %s AND sect_position = 1 AND user_id != %s
        ORDER BY sect_contribution DESC
        LIMIT 1
        """
        return self._read_query(sql, (sect_id, current_owner_id), one=True)

    def get_highest_contrib_user(self, sect_id):
        """获取宗门中贡献最高的用户（不限职位）"""
        sql = """
        SELECT user_id 
        FROM user_xiuxian 
        WHERE sect_id = %s 
        ORDER BY sect_contribution DESC 
        LIMIT 1
        """
        return self._read_query(sql, (sect_id,), one=True)

    def update_sect_join_status(self, sect_id, status):
        """更新宗门加入状态"""
        with self._conn_lock:
            sql = f"UPDATE sects SET join_open=%s WHERE sect_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (status, sect_id))
            self._commit_write()

    def update_sect_closed_status(self, sect_id, status):
        """更新宗门封闭状态"""
        with self._conn_lock:
            sql = f"UPDATE sects SET closed=%s WHERE sect_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (status, sect_id))
            self._commit_write()

    def delete_sect(self, sect_id):
        """删除宗门并踢出所有成员"""
        with self._conn_lock:
            cur = self.conn.cursor()
            try:
                members_sql = "SELECT user_id FROM user_xiuxian WHERE sect_id = %s"
                cur.execute(members_sql, (sect_id,))
                members = cur.fetchall()

                if members:
                    update_sql = """
                        UPDATE user_xiuxian 
                        SET sect_id = NULL, sect_position = NULL, sect_contribution = 0 
                        WHERE sect_id = %s
                    """
                    cur.execute(update_sql, (sect_id,))
                    logger.opt(colors=True).info(f"<green>已踢出宗门 {sect_id} 的所有成员，共 {len(members)} 人</green>")

                delete_sql = "DELETE FROM sects WHERE sect_id = %s"
                cur.execute(delete_sql, (sect_id,))

                self._commit_write()
                logger.opt(colors=True).info(f"<green>宗门 {sect_id} 解散成功，已清理所有成员数据</green>")
                return True

            except Exception as e:
                self.conn.rollback()
                logger.error(f"解散宗门 {sect_id} 时发生错误: {str(e)}")
                return False

    def get_sect_name(self, sect_name):
        """通过宗门名称获取宗门ID"""
        result = self._read_query("SELECT sect_id FROM sects WHERE sect_name = %s", (sect_name,), one=True)
        if result:
            return result[0]
        return None

    def get_all_sect_id(self):
        """获取全部宗门id"""
        result = self._read_query("SELECT sect_id FROM sects")
        return result if result else None

    def get_all_user_id(self):
        """获取全部用户id"""
        result = self._read_query("SELECT user_id FROM user_xiuxian")
        if result:
            return [row[0] for row in result]
        return None

    def in_closing(self, user_id, the_type):
        """更新用户操作CD"""
        with self._conn_lock:
            now_time = None
            if the_type == 0:
                now_time = 0
            else:
                now_time = datetime.now()
            sql = "UPDATE user_cd SET type=%s,create_time=%s,scheduled_time=NULL WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (the_type, now_time, user_id))
            self._commit_write()

    def clear_user_type_if_match(self, user_id, the_type, create_time):
        """仅当用户仍处于同一轮状态时清理状态。"""
        with self._conn_lock:
            cur = self.conn.cursor()
            if create_time is None:
                sql = """
                    UPDATE user_cd
                    SET type=0, create_time=0, scheduled_time=NULL
                    WHERE user_id=%s AND type=%s AND create_time IS NULL
                """
                cur.execute(sql, (user_id, the_type))
            else:
                sql = """
                    UPDATE user_cd
                    SET type=0, create_time=0, scheduled_time=NULL
                    WHERE user_id=%s AND type=%s AND create_time=%s
                """
                cur.execute(sql, (user_id, the_type, create_time))
            success = cur.rowcount > 0
            self._commit_write()
            return success

    def del_exp_decimal(self, user_id, exp):
        """去浮点"""
        with self._conn_lock:
            sql = "UPDATE user_xiuxian SET exp=%s WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (int(exp), user_id))
            self._commit_write()

    def realm_top(self):
        """境界排行"""
        with self._conn_lock:
            rank_mapping = {rank: idx for idx, rank in enumerate(convert_rank('江湖好手')[1])}

            sql = """SELECT user_name, level, exp FROM user_xiuxian 
                WHERE user_name IS NOT NULL
                ORDER BY exp DESC, (CASE level """

            for level, value in sorted(rank_mapping.items(), key=lambda x: x[1], reverse=True):
                sql += f"WHEN '{level}' THEN '{value:02}' "

            sql += """ELSE level END) ASC LIMIT 50"""

            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            return result

    def stone_top(self):
        """这也是灵石排行榜"""
        sql = f"SELECT user_name,stone FROM user_xiuxian WHERE user_name is NOT NULL ORDER BY stone DESC LIMIT 50"
        return self._read_query(sql)

    def power_top(self):
        """战力排行榜"""
        sql = f"SELECT user_name,power FROM user_xiuxian WHERE user_name is NOT NULL ORDER BY power DESC LIMIT 50"
        return self._read_query(sql)

    def scale_top(self):
        """宗门建设度排行榜"""
        sql = f"SELECT sect_id, sect_name, sect_scale FROM sects WHERE sect_owner is NOT NULL ORDER BY sect_scale DESC"
        return self._read_query(sql)

    def root_top(self):
        """这是轮回排行榜"""
        sql = f"SELECT user_name,root_level FROM user_xiuxian WHERE user_name is NOT NULL ORDER BY root_level DESC LIMIT 50"
        return self._read_query(sql)

    def get_all_sects(self):
        """获取所有宗门信息"""
        sql = f"SELECT * FROM sects WHERE sect_owner is NOT NULL"
        return self._read_query(sql, dict_row=True)

    def get_all_sects_with_member_count(self):
        """获取所有宗门及其各个宗门成员数"""
        return self._read_query("""
            SELECT s.sect_id, s.sect_name, s.sect_scale, (SELECT user_name FROM user_xiuxian WHERE user_id = s.sect_owner) as user_name, COUNT(ux.user_id) as member_count
            FROM sects s
            LEFT JOIN user_xiuxian ux ON s.sect_id = ux.sect_id
            GROUP BY s.sect_id
        """)

    def update_user_is_beg(self, user_id, is_beg):
        """更新用户的最后奇缘时间"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = "UPDATE user_xiuxian SET is_beg=%s WHERE user_id=%s"
            cur.execute(sql, (is_beg, user_id))
            self._commit_write()

    def get_top1_user(self):
        """获取修为第一的用户"""
        return self._read_query(
            "select * from user_xiuxian ORDER BY exp DESC LIMIT 1",
            one=True,
            dict_row=True,
        )

    def get_realm_top1_user(self):
        """获取境界第一的用户"""
        with self._conn_lock:
            rank_mapping = {rank: idx for idx, rank in enumerate(convert_rank('江湖好手')[1])}

            sql = """SELECT user_name, level, exp FROM user_xiuxian 
                WHERE user_name IS NOT NULL
                ORDER BY exp DESC, (CASE level """

            for level, value in sorted(rank_mapping.items(), key=lambda x: x[1], reverse=True):
                sql += f"WHEN '{level}' THEN '{value:02}' "

            sql += """ELSE level END) ASC LIMIT 1"""

            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchone()
            if result:
                columns = [column[0] for column in cur.description]
                top1_dict = dict(zip(columns, result))
                return top1_dict
            else:
                return None

    def donate_update(self, sect_id, stone_num):
        """宗门捐献更新建设度及可用灵石"""
        with self._conn_lock:
            sql = f"UPDATE sects SET sect_used_stone=sect_used_stone+%s,sect_scale=sect_scale+%s WHERE sect_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (stone_num, stone_num * 1, sect_id))
            self._commit_write()

    def update_sect_used_stone(self, sect_id, sect_used_stone, key):
        """更新宗门灵石储备"""
        with self._conn_lock:
            cur = self.conn.cursor()
            if key == 1:
                sql = f"UPDATE sects SET sect_used_stone=sect_used_stone+%s WHERE sect_id=%s"
                cur.execute(sql, (sect_used_stone, sect_id))
                self._commit_write()
            elif key == 2:
                sql = f"UPDATE sects SET sect_used_stone=sect_used_stone-%s WHERE sect_id=%s"
                cur.execute(sql, (sect_used_stone, sect_id))
                self._commit_write()

    def update_sect_materials(self, sect_id, sect_materials, key):
        """更新资材"""
        with self._conn_lock:
            cur = self.conn.cursor()
            if key == 1:
                sql = f"UPDATE sects SET sect_materials=sect_materials+%s WHERE sect_id=%s"
                cur.execute(sql, (sect_materials, sect_id))
                self._commit_write()
            elif key == 2:
                sql = f"UPDATE sects SET sect_materials=sect_materials-%s WHERE sect_id=%s"
                cur.execute(sql, (sect_materials, sect_id))
                self._commit_write()

    def get_all_sects_id_scale(self):
        """获取所有宗门信息"""
        sql = f"SELECT sect_id, sect_scale, elixir_room_level FROM sects WHERE sect_owner is NOT NULL ORDER BY sect_scale DESC"
        return self._read_query(sql)

    def get_all_users_by_sect_id(self, sect_id):
        """获取宗门所有成员信息"""
        return self._read_query(
            "SELECT * FROM user_xiuxian WHERE sect_id = %s",
            (sect_id,),
            dict_row=True,
        )

    def do_work(self, user_id, the_type, sc_time=None):
        """更新用户操作CD"""
        with self._conn_lock:
            now_time = None
            if the_type == 1:
                now_time = datetime.now()
            elif the_type == 0:
                now_time = 0
            elif the_type == 2:
                now_time = datetime.now()
            elif the_type == 3:
                now_time = datetime.now()
            elif the_type == 4:
                now_time = datetime.now()

            sql = f"UPDATE user_cd SET type=%s,create_time=%s,scheduled_time=%s WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (the_type, now_time, sc_time, user_id))
            self._commit_write()

    def update_levelrate(self, user_id, rate):
        """更新突破成功率"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET level_up_rate=%s WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (rate, user_id))
            self._commit_write()

    def update_user_attribute(self, user_id, hp, mp, atk):
        """更新用户HP,MP,ATK信息"""
        with self._conn_lock:
            hp = number_count(hp)
            mp = number_count(mp)
            atk = number_count(atk)
            sql = f"UPDATE user_xiuxian SET hp=%s,mp=%s,atk=%s WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (hp, mp, atk, user_id))
            self._commit_write()

    def update_user_hp_mp(self, user_id, hp, mp):
        """更新用户HP,MP信息"""
        with self._conn_lock:
            hp = number_count(hp)
            mp = number_count(mp)
            sql = f"UPDATE user_xiuxian SET hp=%s,mp=%s WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (hp, mp, user_id))
            self._commit_write()

    def update_user_sect_contribution(self, user_id, sect_contribution):
        """更新用户宗门贡献度"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET sect_contribution=%s WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (sect_contribution, user_id))
            self._commit_write()

    def deduct_sect_contribution(self, user_id, contribution):
        """扣除用户宗门贡献度"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = "UPDATE user_xiuxian SET sect_contribution=sect_contribution-%s WHERE user_id=%s"
            cur.execute(sql, (contribution, user_id))
            self._commit_write()

    def update_user_hp(self, user_id):
        """重置用户hp,mp信息"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET hp=exp/2,mp=exp,atk=exp/10 WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self._commit_write()

    def restate(self, user_id=None):
        """重置所有用户状态或重置对应人状态"""
        with self._conn_lock:
            if user_id is None:
                sql = f"UPDATE user_xiuxian SET hp=exp/2,mp=exp,atk=exp/10"
                cur = self.conn.cursor()
                cur.execute(sql)
                self._commit_write()
            else:
                sql = f"UPDATE user_xiuxian SET hp=exp/2,mp=exp,atk=exp/10 WHERE user_id=%s"
                cur = self.conn.cursor()
                cur.execute(sql, (user_id,))
                self._commit_write()

    def get_back_msg(self, user_id):
        """获取用户背包信息"""
        result = self._read_query(
            "SELECT * FROM back WHERE user_id=%s and goods_num >= 1",
            (user_id,),
            dict_row=True,
        )
        return result if result else None

    def check_and_adjust_goods_quantity(self):
        """检查并调整背包表中的物品数量和物品名称"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = "SELECT user_id, goods_id, goods_num, bind_num, goods_name FROM back"
            cur.execute(sql)
            results = cur.fetchall()

            processed_goods = ""
            items = Items()
            for row in results:
                user_id, goods_id, goods_num, bind_num, goods_name = row
                if goods_num > XiuConfig().max_goods_num:
                    new_goods_num = XiuConfig().max_goods_num
                    sql_update = f"UPDATE back SET goods_num=%s WHERE user_id=%s AND goods_id=%s"
                    cur.execute(sql_update, (new_goods_num, user_id, goods_id))
                    logger.opt(colors=True).info(f"<green>用户 {user_id} 的物品 {goods_name} 的数量已调整为 {new_goods_num}</green>")
                    processed_goods += f"{user_id} 的 {goods_name} 数量异常{goods_num}\n"

                if bind_num > XiuConfig().max_goods_num:
                    new_bind_num = XiuConfig().max_goods_num
                    sql_update = f"UPDATE back SET bind_num=%s WHERE user_id=%s AND goods_id=%s"
                    cur.execute(sql_update, (new_bind_num, user_id, goods_id))
                    logger.opt(colors=True).info(f"<green>用户 {user_id} 的物品 {goods_name} 的绑定数量已调整为 {new_bind_num}</green>")
                    processed_goods += f"{user_id} 的 {goods_name} 绑定数量异常{bind_num}\n"

                try:
                    item_info = items.get_data_by_item_id(int(goods_id))
                except Exception:
                    item_info = None

                if item_info:
                    current_name = item_info.get("name")
                    if current_name and goods_name != current_name:
                        sql_update = "UPDATE back SET goods_name=%s WHERE user_id=%s AND goods_id=%s"
                        cur.execute(sql_update, (current_name, user_id, goods_id))
                        logger.opt(colors=True).info(
                            f"<green>用户 {user_id} 的物品ID {goods_id} 名称已由 {goods_name} 修正为 {current_name}</green>"
                        )
                        processed_goods += f"{user_id} 的物品ID {goods_id} 名称异常：{goods_name} -> {current_name}\n"

            self._commit_write()
            if not processed_goods:
                return "无"
            return processed_goods

    def goods_num(self, user_id, goods_id, num_type=None):
        """判断用户物品数量"""
        result = self._read_query(
            "SELECT goods_num, bind_num, state FROM back WHERE user_id=%s and goods_id=%s",
            (user_id, goods_id),
            one=True,
        )
        if result:
            goods_num = result[0]
            bind_num = result[1]
            state = result[2]
            if num_type == 'bind':
                return bind_num
            elif num_type == 'trade':
                return goods_num - bind_num - state
            else:
                return goods_num
        return 0

    def goods_max_num(self, goods_id):
        """返回物品的总数量"""
        result = self._read_query("SELECT SUM(goods_num) FROM back WHERE goods_id=%s", (goods_id,), one=True)
        if result and result[0] is not None:
            return result[0]
        return 0

    def unbind_item(self, user_id, goods_id, quantity=1):
        """解绑物品，减少绑定数量"""
        with self._business_lock, self._conn_lock:
            try:
                cur = self.conn.cursor()
                sql = "SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s"
                cur.execute(sql, (user_id, goods_id))
                result = cur.fetchone()

                if not result:
                    return False

                current_goods_num = result[0]
                current_bind_num = result[1]

                if current_bind_num < quantity:
                    return False

                new_bind_num = current_bind_num - quantity
                new_bind_num = max(0, new_bind_num)

                update_sql = "UPDATE back SET bind_num=%s, update_time=%s WHERE user_id=%s AND goods_id=%s"
                now_time = datetime.now()
                cur.execute(update_sql, (new_bind_num, now_time, user_id, goods_id))
                self._commit_write()

                return True

            except Exception as e:
                logger.error(f"解绑物品时发生错误: {str(e)}")
                self.conn.rollback()
                return False

    def get_all_user_exp(self, level):
        """查询所有对应大境界玩家的修为"""
        return self._read_query("SELECT exp FROM user_xiuxian WHERE level like %s", (f"{level}%",))

    def update_user_atkpractice(self, user_id, atkpractice):
        """更新用户攻击修炼等级"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET atkpractice={atkpractice} WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self._commit_write()

    def update_user_hppractice(self, user_id, hppractice):
        """更新用户元血修炼等级"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET hppractice={hppractice} WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self._commit_write()

    def update_user_mppractice(self, user_id, mppractice):
        """更新用户灵海修炼等级"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET mppractice={mppractice} WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self._commit_write()

    def update_user_sect_task(self, user_id, sect_task):
        """更新用户宗门任务次数"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET sect_task=sect_task+%s WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (sect_task, user_id))
            self._commit_write()

    def sect_task_reset(self):
        """重置宗门任务次数"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET sect_task=0"
            cur = self.conn.cursor()
            cur.execute(sql)
            self._commit_write()

    def update_sect_scale_and_used_stone(self, sect_id, sect_used_stone, sect_scale):
        """更新宗门灵石、建设度"""
        with self._conn_lock:
            sql = f"UPDATE sects SET sect_used_stone=%s,sect_scale=%s WHERE sect_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (sect_used_stone, sect_scale, sect_id))
            self._commit_write()

    def update_sect_elixir_room_level(self, sect_id, level):
        """更新宗门丹房等级"""
        with self._conn_lock:
            sql = f"UPDATE sects SET elixir_room_level=%s WHERE sect_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (level, sect_id))
            self._commit_write()

    def update_sect_fairyland(self, sect_id, level):
        """更新宗门炼体堂等级"""
        with self._conn_lock:
            sql = f"UPDATE sects SET sect_fairyland=%s WHERE sect_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (level, sect_id))
            self._commit_write()

    def update_user_sect_elixir_get_num(self, user_id):
        """更新用户每日领取丹药领取次数"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET sect_elixir_get=1 WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self._commit_write()

    def sect_elixir_get_num_reset(self):
        """重置宗门丹药领取次数"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET sect_elixir_get=0"
            cur = self.conn.cursor()
            cur.execute(sql)
            self._commit_write()

    def update_sect_mainbuff(self, sect_id, mainbuffid):
        """更新宗门当前的主修功法"""
        with self._conn_lock:
            sql = f"UPDATE sects SET mainbuff=%s WHERE sect_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (mainbuffid, sect_id))
            self._commit_write()

    def update_sect_secbuff(self, sect_id, secbuffid):
        """更新宗门当前的神通"""
        with self._conn_lock:
            sql = f"UPDATE sects SET secbuff=%s WHERE sect_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (secbuffid, sect_id))
            self._commit_write()

    def initialize_user_buff_info(self, user_id):
        """初始化用户buff信息"""
        with self._conn_lock:
            sql = f"INSERT INTO BuffInfo (user_id,main_buff,sec_buff,effect1_buff,effect2_buff,faqi_buff,fabao_weapon) VALUES (%s,0,0,0,0,0,0)"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self._commit_write()

    def get_user_buff_info(self, user_id):
        """获取用户buff信息"""
        return self._read_query(
            "select * from BuffInfo WHERE user_id =%s",
            (user_id,),
            one=True,
            dict_row=True,
        )

    def updata_user_main_buff(self, user_id, id):
        """更新用户主功法信息"""
        with self._conn_lock:
            sql = f"UPDATE BuffInfo SET main_buff = %s WHERE user_id = %s"
            cur = self.conn.cursor()
            cur.execute(sql, (id, user_id,))
            self._commit_write()

    def updata_user_sub_buff(self, user_id, id):
        """更新用户辅修功法信息"""
        with self._conn_lock:
            sql = f"UPDATE BuffInfo SET sub_buff = %s WHERE user_id = %s"
            cur = self.conn.cursor()
            cur.execute(sql, (id, user_id,))
            self._commit_write()

    def updata_user_sec_buff(self, user_id, id):
        """更新用户副功法信息"""
        with self._conn_lock:
            sql = f"UPDATE BuffInfo SET sec_buff = %s WHERE user_id = %s"
            cur = self.conn.cursor()
            cur.execute(sql, (id, user_id,))
            self._commit_write()

    def updata_user_effect1_buff(self, user_id, id):
        """更新用户身法信息"""
        with self._conn_lock:
            sql = f"UPDATE BuffInfo SET effect1_buff = %s WHERE user_id = %s"
            cur = self.conn.cursor()
            cur.execute(sql, (id, user_id,))
            self._commit_write()

    def updata_user_effect2_buff(self, user_id, id):
        """更新用户瞳术信息"""
        with self._conn_lock:
            sql = f"UPDATE BuffInfo SET effect2_buff = %s WHERE user_id = %s"
            cur = self.conn.cursor()
            cur.execute(sql, (id, user_id,))
            self._commit_write()

    def updata_user_faqi_buff(self, user_id, id):
        """更新用户法器信息"""
        with self._conn_lock:
            sql = f"UPDATE BuffInfo SET faqi_buff = %s WHERE user_id = %s"
            cur = self.conn.cursor()
            cur.execute(sql, (id, user_id,))
            self._commit_write()

    def updata_user_fabao_weapon(self, user_id, id):
        """更新用户法宝信息"""
        with self._conn_lock:
            sql = f"UPDATE BuffInfo SET fabao_weapon = %s WHERE user_id = %s"
            cur = self.conn.cursor()
            cur.execute(sql, (id, user_id,))
            self._commit_write()

    def updata_user_armor_buff(self, user_id, id):
        """更新用户防具信息"""
        with self._conn_lock:
            sql = f"UPDATE BuffInfo SET armor_buff = %s WHERE user_id = %s"
            cur = self.conn.cursor()
            cur.execute(sql, (id, user_id,))
            self._commit_write()

    def updata_user_atk_buff(self, user_id, buff):
        """更新用户永久攻击buff信息"""
        with self._conn_lock:
            sql = f"UPDATE BuffInfo SET atk_buff=atk_buff+%s WHERE user_id = %s"
            cur = self.conn.cursor()
            cur.execute(sql, (buff, user_id,))
            self._commit_write()

    def updata_user_blessed_spot(self, user_id, blessed_spot):
        """更新用户洞天福地等级"""
        with self._conn_lock:
            sql = f"UPDATE BuffInfo SET blessed_spot=%s WHERE user_id = %s"
            cur = self.conn.cursor()
            cur.execute(sql, (blessed_spot, user_id,))
            self._commit_write()

    def update_user_blessed_spot_flag(self, user_id):
        """更新用户洞天福地是否开启"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET blessed_spot_flag=1 WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self._commit_write()

    def update_user_blessed_spot_name(self, user_id, blessed_spot_name):
        """更新用户洞天福地的名字"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET blessed_spot_name=%s WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (blessed_spot_name, user_id,))
            self._commit_write()

    def day_num_reset(self):
        """重置丹药每日使用次数"""
        with self._conn_lock:
            sql = f"UPDATE back SET day_num=0 where goods_type='丹药'"
            cur = self.conn.cursor()
            cur.execute(sql)
            self._commit_write()

    def mixelixir_num_reset(self):
        """重置每日炼丹次数"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET mixelixir_num=0"
            cur = self.conn.cursor()
            cur.execute(sql)
            self._commit_write()

    def reset_work_num(self, count):
        """重置用户悬赏令刷新次数"""
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET work_num=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (count,))
            self._commit_write()

    def get_work_num(self, user_id):
        """获取用户悬赏令刷新次数"""
        result = self._read_query("SELECT work_num FROM user_xiuxian WHERE user_id=%s", (user_id,), one=True)
        if result:
            return result[0]
        return None

    def update_work_num(self, user_id, work_num):
        with self._conn_lock:
            sql = f"UPDATE user_xiuxian SET work_num=%s WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (work_num, user_id,))
            self._commit_write()

    def send_back(self, user_id, goods_id, goods_name, goods_type, goods_num, bind_flag=0):
        """插入物品至背包"""
        with self._conn_lock:
            now_time = datetime.now()
            max_goods_num = int(XiuConfig().max_goods_num)
            goods_num = min(abs(int(goods_num)), max_goods_num)
            bind_flag = 1 if int(bind_flag) == 1 else 0
            bind_num = goods_num if bind_flag == 1 else 0

            sql = """
                INSERT INTO back (
                    user_id, goods_id, goods_name, goods_type, goods_num,
                    create_time, update_time, bind_num
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, goods_id) DO UPDATE
                SET goods_name = EXCLUDED.goods_name,
                    goods_type = EXCLUDED.goods_type,
                    update_time = EXCLUDED.update_time,
                    goods_num = LEAST(COALESCE(back.goods_num, 0) + EXCLUDED.goods_num, %s),
                    bind_num = CASE
                        WHEN %s = 1 THEN
                            LEAST(
                                COALESCE(back.bind_num, 0) + EXCLUDED.goods_num,
                                LEAST(COALESCE(back.goods_num, 0) + EXCLUDED.goods_num, %s),
                                %s
                            )
                        ELSE
                            LEAST(
                                COALESCE(back.bind_num, 0),
                                LEAST(COALESCE(back.goods_num, 0) + EXCLUDED.goods_num, %s)
                            )
                    END
            """
            cur = self.conn.cursor()
            cur.execute(
                sql,
                (
                    str(user_id), int(goods_id), goods_name, goods_type, goods_num,
                    now_time, now_time, bind_num,
                    max_goods_num, bind_flag, max_goods_num, max_goods_num, max_goods_num,
                ),
            )
            self._commit_write()

    def update_back_j(self, user_id, goods_id, num=1, use_key=0):
        """使用物品"""
        with self._conn_lock:
            num = abs(int(num))
            user_id = str(user_id)
            goods_id = int(goods_id)
            if num <= 0:
                return

            now_time = datetime.now()
            sql_str = """
                UPDATE back
                SET update_time=%s,
                    action_time=%s,
                    day_num = CASE
                        WHEN goods_type = '丹药' AND %s = 1
                        THEN COALESCE(day_num, 0) + LEAST(COALESCE(goods_num, 0), %s)
                        ELSE COALESCE(day_num, 0)
                    END,
                    all_num = CASE
                        WHEN goods_type = '丹药' AND %s = 1
                        THEN COALESCE(all_num, 0) + LEAST(COALESCE(goods_num, 0), %s)
                        ELSE COALESCE(all_num, 0)
                    END,
                    goods_num = GREATEST(COALESCE(goods_num, 0) - %s, 0),
                    bind_num = CASE
                        WHEN GREATEST(COALESCE(goods_num, 0) - %s, 0) = 0 THEN 0
                        ELSE LEAST(
                            CASE
                                WHEN COALESCE(bind_num, 0) >= LEAST(COALESCE(goods_num, 0), %s)
                                THEN COALESCE(bind_num, 0) - LEAST(COALESCE(goods_num, 0), %s)
                                ELSE COALESCE(bind_num, 0)
                            END,
                            GREATEST(COALESCE(goods_num, 0) - %s, 0)
                        )
                    END
                WHERE user_id=%s AND goods_id=%s AND COALESCE(goods_num, 0) > 0
            """
            cur = self.conn.cursor()
            cur.execute(
                sql_str,
                (
                    now_time, now_time,
                    int(use_key), num,
                    int(use_key), num,
                    num, num, num, num, num,
                    user_id, goods_id,
                ),
            )
            self._commit_write()

    def spend_stone_and_consume_trade_items(self, user_id, stone_cost=0, consume_items=None):
        """原子扣除灵石和可交易物品，失败时不改变数据。"""
        user_id = str(user_id)
        stone_cost = abs(int(stone_cost or 0))
        consume_items = consume_items or []

        normalized_items = {}
        for goods_id, num in consume_items:
            goods_id = int(goods_id)
            num = abs(int(num))
            if num <= 0:
                continue
            normalized_items[goods_id] = normalized_items.get(goods_id, 0) + num

        if stone_cost <= 0 and not normalized_items:
            return True

        with self._business_lock, self._conn_lock:
            cur = self.conn.cursor()
            try:
                if stone_cost > 0:
                    cur.execute(
                        "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND COALESCE(stone, 0) >= %s",
                        (stone_cost, user_id, stone_cost),
                    )
                    if cur.rowcount <= 0:
                        self.conn.rollback()
                        return False

                now_time = datetime.now()
                for goods_id, num in normalized_items.items():
                    cur.execute(
                        """
                        UPDATE back
                        SET goods_num=COALESCE(goods_num, 0)-%s,
                            update_time=%s,
                            action_time=%s
                        WHERE user_id=%s
                          AND goods_id=%s
                          AND COALESCE(goods_num, 0)-COALESCE(bind_num, 0)-COALESCE(state, 0) >= %s
                        """,
                        (num, now_time, now_time, user_id, goods_id, num),
                    )
                    if cur.rowcount <= 0:
                        self.conn.rollback()
                        return False

                self._commit_write()
                return True
            except Exception:
                self.conn.rollback()
                raise

    def consume_trade_item(self, user_id, goods_id, num=1):
        """扣除非绑定且未装备的可交易物品。"""
        return self.spend_stone_and_consume_trade_items(user_id, 0, [(goods_id, num)])

    def alchemy_items(self, user_id, reward_stone=0, consume_items=None):
        """炼金扣除物品并增加灵石；允许扣绑定物品，但保留 state 占用数量。"""
        user_id = str(user_id)
        reward_stone = abs(int(reward_stone or 0))
        consume_items = consume_items or []

        normalized_items = {}
        for goods_id, num in consume_items:
            goods_id = int(goods_id)
            num = abs(int(num))
            if num <= 0:
                continue
            normalized_items[goods_id] = normalized_items.get(goods_id, 0) + num

        if reward_stone <= 0 or not normalized_items:
            return False

        with self._business_lock, self._conn_lock:
            cur = self.conn.cursor()
            try:
                now_time = datetime.now()
                for goods_id, num in normalized_items.items():
                    cur.execute(
                        """
                        UPDATE back
                        SET goods_num=COALESCE(goods_num, 0)-%s,
                            bind_num=LEAST(
                                COALESCE(bind_num, 0),
                                GREATEST(COALESCE(goods_num, 0)-%s, 0)
                            ),
                            update_time=%s,
                            action_time=%s
                        WHERE user_id=%s
                          AND goods_id=%s
                          AND COALESCE(goods_num, 0)-COALESCE(state, 0) >= %s
                        """,
                        (num, num, now_time, now_time, user_id, goods_id, num),
                    )
                    if cur.rowcount <= 0:
                        self.conn.rollback()
                        return False

                cur.execute(
                    "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                    (reward_stone, user_id),
                )
                if cur.rowcount <= 0:
                    self.conn.rollback()
                    return False

                self._commit_write()
                return True
            except Exception:
                self.conn.rollback()
                raise

    def get_item_by_good_id_and_user_id(self, user_id, goods_id):
        """根据物品id、用户id获取物品信息"""
        with self._conn_lock:
            sql = "select * from back WHERE user_id=%s and goods_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (str(user_id), int(goods_id)))
            result = cur.fetchone()
            if not result:
                return None

            columns = [column[0] for column in cur.description]
            item_dict = dict(zip(columns, result))
            return item_dict

    def update_back_equipment(self, sql_str, params=None):
        with self._conn_lock:
            cur = self.conn.cursor()
            if isinstance(sql_str, tuple) and params is None:
                sql_str, params = sql_str
            if params is not None:
                cur.execute(sql_str, params)
            else:
                cur.execute(sql_str)
            self._commit_write()

    def reset_user_drug_resistance(self, user_id):
        """重置用户耐药性"""
        with self._conn_lock:
            sql = "UPDATE back SET all_num=0 WHERE goods_type='丹药' AND user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (str(user_id),))
            self._commit_write()

    def _ensure_puppet_column(self):
        """确保 user_xiuxian 表中存在 puppet_status 字段，没有就创建"""
        with self._conn_lock:
            cur = self.conn.cursor()
            if not self.conn.column_exists("user_xiuxian", "puppet_status"):
                cur.execute("ALTER TABLE user_xiuxian ADD COLUMN puppet_status INTEGER DEFAULT 0")
                self._commit_write()

    def check_puppet_status(self, user_id):
        """查询灵田傀儡状态，没有字段会自动创建"""
        with self._conn_lock:
            self._ensure_puppet_column()
            sql = "SELECT puppet_status FROM user_xiuxian WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if not result:
                return 0
            return result[0]

    def set_puppet_status(self, user_id, status):
        """设置灵田傀儡状态 status: 0关闭 1开启"""
        with self._conn_lock:
            self._ensure_puppet_column()
            sql = "UPDATE user_xiuxian SET puppet_status=%s WHERE user_id=%s"
            cur = self.conn.cursor()
            cur.execute(sql, (status, user_id))
            self._commit_write()

    def get_all_enabled_puppets(self):
        """获取所有开启灵田傀儡的玩家 user_id 列表"""
        with self._conn_lock:
            self._ensure_puppet_column()
            sql = "SELECT user_id FROM user_xiuxian WHERE puppet_status=1"
            cur = self.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            return [r[0] for r in rows]


class XiuxianJsonDate:
    def __init__(self):
        self.root_jsonpath = DATABASE / "灵根.json"
        self.level_jsonpath = DATABASE / "突破概率.json"

    def beifen_linggen_get(self):
        with open(self.root_jsonpath, 'r', encoding='utf-8') as e:
            a = e.read()
            data = json.loads(a)
            lg = random.choice(data)
            return lg['name'], lg['type']

    def level_rate(self, level):
        with open(self.level_jsonpath, 'r', encoding='utf-8') as e:
            a = e.read()
            data = json.loads(a)
            return data[0][level]

    def linggen_get(self):
        """获取灵根信息"""
        data = jsondata.root_data()
        rate_dict = {}
        for i, v in data.items():
            rate_dict[i] = v["type_rate"]
        lgen = OtherSet().calculated(rate_dict)
        if data[lgen]["type_flag"]:
            flag = random.choice(data[lgen]["type_flag"])
            root = random.sample(data[lgen]["type_list"], flag)
            msg = ""
            for j in root:
                if j == root[-1]:
                    msg += j
                    break
                msg += (j + "、")

            return msg + '属性灵根', lgen
        else:
            root = random.choice(data[lgen]["type_list"])
            return root, lgen



class OtherSet(XiuConfig):

    def __init__(self):
        super().__init__()

    def set_closing_type(self, user_level):
        list_all = len(self.level) - 1
        now_index = self.level.index(user_level)
        if list_all == now_index:
            need_exp = 0.001
        else:
            is_updata_level = self.level[now_index + 1]
            need_exp = XiuxianDateManage().get_level_power(is_updata_level)
        return need_exp

    def get_type(self, user_exp, rate, user_level):
        list_all = len(self.level) - 1
        now_index = self.level.index(user_level)
        if list_all == now_index:
            return "道友已是最高境界，无法突破！"

        is_updata_level = self.level[now_index + 1]
        need_exp = XiuxianDateManage().get_level_power(is_updata_level)

        # 判断修为是否足够突破
        if user_exp >= need_exp:
            pass
        else:
            from .utils import number_to
            return f"道友的修为不足以突破！距离下次突破需要{number_to(need_exp - user_exp)}修为！突破境界为：{is_updata_level}"

        success_rate = True if random.randint(0, 100) < rate else False

        if success_rate:
            return [self.level[now_index + 1]]
        else:
            return '失败'

    def calculated(self, rate: dict) -> str:
        """
        根据概率计算，轮盘型
        :rate:格式{"数据名":"获取几率"}
        :return: 数据名
        """

        get_list = []  # 概率区间存放

        n = 1
        for name, value in rate.items():  # 生成数据区间
            value_rate = int(value)
            list_rate = [_i for _i in range(n, value_rate + n)]
            get_list.append(list_rate)
            n += value_rate

        now_n = n - 1
        get_random = random.randint(1, now_n)  # 抽取随机数

        index_num = None
        for list_r in get_list:
            if get_random in list_r:  # 判断随机在那个区间
                index_num = get_list.index(list_r)
                break

        return list(rate.keys())[index_num]

    def date_diff(self, new_time, old_time):
        """计算日期差"""
        if isinstance(new_time, datetime):
            pass
        else:
            new_time = datetime.strptime(new_time, '%Y-%m-%d %H:%M:%S.%f')

        if isinstance(old_time, datetime):
            pass
        else:
            old_time = datetime.strptime(old_time, '%Y-%m-%d %H:%M:%S.%f')

        day = (new_time - old_time).days
        sec = (new_time - old_time).seconds

        return (day * 24 * 60 * 60) + sec

    def get_power_rate(self, mind, other):
        power_rate = mind / (other + mind)
        if power_rate >= 0.8:
            return "道友偷窃小辈实属天道所不齿！"
        elif power_rate <= 0.05:
            return "道友请不要不自量力！"
        else:
            return int(power_rate * 100)

    def player_fight(self, player1: dict, player2: dict):
        """
        回合制战斗
        type_in : 1 为完整返回战斗过程（未加）
        2：只返回战斗结果
        数据示例：
        {"道号": None, "气血": None, "攻击": None, "真元": None, '会心':None}
        """
        msg1 = "{}发起攻击，造成了{}伤害\n"
        msg2 = "{}发起攻击，造成了{}伤害\n"

        play_list = []
        suc = None
        default_msg = {id(player1): msg1, id(player2): msg2}

        def get_player_speed(player: dict):
            if "速度" in player:
                return float(player.get("速度", 0) or 0)
            user_id = player.get("user_id")
            final_attr = get_final_attributes(user_id, include_current=True) if user_id else None
            return float(final_attr.get("speed", 0)) if final_attr else 0

        def calc_damage(attacker: dict, defender: dict):
            msg_tpl = default_msg[id(attacker)]
            attack = int(round(random.uniform(0.95, 1.05), 2) * attacker['攻击'])
            if random.randint(0, 100) <= attacker['会心']:
                attack = int(attack * attacker['爆伤'])
                msg_tpl = "{}发起会心一击，造成了{}伤害\n"
            damage = int(attack * (1 - defender['防御']))
            return msg_tpl, max(0, damage)

        speed_tiebreaker = {id(player1): random.random(), id(player2): random.random()}
        player1["速度"] = get_player_speed(player1)
        player2["速度"] = get_player_speed(player2)
        if player1['气血'] <= 0:
            player1['气血'] = 1
        if player2['气血'] <= 0:
            player2['气血'] = 1
        while True:
            order = sorted(
                (player1, player2),
                key=lambda p: (get_player_speed(p), speed_tiebreaker[id(p)]),
                reverse=True
            )

            for attacker in order:
                defender = player2 if attacker is player1 else player1
                if attacker['气血'] <= 0 or defender['气血'] <= 0:
                    continue

                msg_tpl, damage = calc_damage(attacker, defender)
                play_list.append(msg_tpl.format(attacker['道号'], damage))
                defender['气血'] -= damage
                play_list.append(f"{defender['道号']}剩余血量{defender['气血']}")
                XiuxianDateManage().update_user_hp_mp(defender['user_id'], defender['气血'], defender['真元'])

                if defender['气血'] <= 0:
                    play_list.append(f"{attacker['道号']}胜利")
                    suc = f"{attacker['道号']}"
                    XiuxianDateManage().update_user_hp_mp(defender['user_id'], 1, defender['真元'])
                    break

            if suc:
                break

        return play_list, suc

    def send_hp_mp(self, user_id, hp, mp):
        user_msg = XiuxianDateManage().get_user_info_with_id(user_id)
        max_hp = int(user_msg['exp'] / 2)
        max_mp = int(user_msg['exp'])

        msg = []
        hp_mp = []
        from .utils import number_to
        if user_msg['hp'] < max_hp:
            if user_msg['hp'] + hp < max_hp:
                new_hp = user_msg['hp'] + hp
                msg.append(f',回复气血：{number_to(hp)}')
            else:
                new_hp = max_hp
                msg.append(',气血已回满！')
        else:
            new_hp = user_msg['hp']
            msg.append('')

        if user_msg['mp'] < max_mp:
            if user_msg['mp'] + mp < max_mp:
                new_mp = user_msg['mp'] + mp
                msg.append(f',回复真元：{number_to(mp)}')
            else:
                new_mp = max_mp
                msg.append(',真元已回满！')
        else:
            new_mp = user_msg['mp']
            msg.append('')

        hp_mp.append(new_hp)
        hp_mp.append(new_mp)
        hp_mp.append(user_msg['exp'])

        return msg, hp_mp

# 这里是交易数据部分
class TradeDataManager:
    global trade_num
    _instance = {}
    _has_init = {}

    def __new__(cls):
        if cls._instance.get(trade_num) is None:
            cls._instance[trade_num] = super(TradeDataManager, cls).__new__(cls)
        return cls._instance[trade_num]

    def __init__(self):
        if not self._has_init.get(trade_num):
            self._has_init[trade_num] = True
            self.database_path = DATABASE
            self.trade_db_path = self.database_path / "trade.db"
            self.conn = db_backend.connect(self.trade_db_path, check_same_thread=False)
            self._conn_lock = threading.RLock()
            self.lock = self._conn_lock
            self._business_lock = threading.RLock()
            self._check_data()

    def _commit_write(self, conn=None):
        (conn or self.conn).commit()

    def _check_data(self):
        """检查数据完整性"""
        with self._conn_lock:
            c = self.conn.cursor()

            # 仙肆商品表
            c.execute("""
                CREATE TABLE IF NOT EXISTS xianshi_item (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    goods_id INTEGER,
                    name TEXT,
                    type TEXT,
                    price INTEGER,
                    quantity INTEGER
                )
            """)

            # 鬼市订单表（item_type: qiugou/baitan；兼容历史中文）
            c.execute("""
                CREATE TABLE IF NOT EXISTS guishi_item (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    item_id INTEGER,
                    item_name TEXT,
                    item_type TEXT,
                    price INTEGER,
                    quantity INTEGER,
                    filled_quantity INTEGER DEFAULT 0
                )
            """)

            # 鬼市账户（灵石+暂存物品）
            c.execute("""
                CREATE TABLE IF NOT EXISTS guishi_info (
                    user_id TEXT PRIMARY KEY,
                    stored_stone INTEGER DEFAULT 0,
                    items TEXT DEFAULT '{}'
                )
            """)

            # 玩家拍卖等待区
            c.execute("""
                CREATE TABLE IF NOT EXISTS auction_player_upload (
                    user_id TEXT NOT NULL,
                    item_id INTEGER NOT NULL,
                    item_name TEXT NOT NULL,
                    start_price INTEGER NOT NULL,
                    user_name TEXT NOT NULL,
                    PRIMARY KEY (user_id, item_id)
                )
            """)

            # 当前拍卖表
            c.execute("""
                CREATE TABLE IF NOT EXISTS auction_current (
                    id TEXT PRIMARY KEY,
                    item_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    start_price INTEGER NOT NULL,
                    current_price INTEGER NOT NULL,
                    seller_id TEXT NOT NULL,
                    seller_name TEXT NOT NULL,
                    bids TEXT DEFAULT '{}',
                    bid_times TEXT DEFAULT '{}',
                    is_system INTEGER DEFAULT 0,
                    last_bid_time REAL DEFAULT NULL
                )
            """)

            # 拍卖历史
            c.execute("""
                CREATE TABLE IF NOT EXISTS auction_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    auction_id TEXT NOT NULL,
                    item_id INTEGER NOT NULL,
                    item_name TEXT NOT NULL,
                    start_price INTEGER NOT NULL,
                    final_price INTEGER,
                    seller_id TEXT NOT NULL,
                    seller_name TEXT NOT NULL,
                    winner_id TEXT,
                    winner_name TEXT,
                    status TEXT NOT NULL,
                    fee INTEGER,
                    seller_earnings INTEGER,
                    start_time REAL NOT NULL,
                    end_time REAL NOT NULL
                )
            """)
            self._commit_write()

    def total_goods_quantity(self):
        """获取全部仙肆物品总数（不含系统无限）"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = """
                SELECT SUM(quantity) AS total_quantity
                FROM (
                    SELECT quantity FROM xianshi_item WHERE user_id != '0' AND quantity > 0
                    UNION ALL
                    SELECT quantity FROM guishi_item WHERE user_id != '0' AND (item_type='baitan' OR item_type='摆摊') AND quantity > 0
                )
            """
            cur.execute(sql)
            result = cur.fetchone()
            return int(result[0]) if result and result[0] is not None else 0

    def generate_unique_id(self, table_name):
        """生成唯一ID（字符串）"""
        with self._conn_lock:
            cur = self.conn.cursor()
            for _ in range(200):
                first = str(random.randint(1, 9))
                rest = ''.join(random.choices(string.digits, k=random.randint(7, 11)))
                uid = first + rest
                cur.execute(f"SELECT 1 FROM {table_name} WHERE id = %s", (uid,))
                if not cur.fetchone():
                    return uid
            return f"{int(time.time() * 1000)}{random.randint(100, 999)}"

    # ======== 仙肆 ========

    def add_xianshi_item(self, user_id, goods_id, name, type, price, quantity):
        with self._conn_lock:
            unique_id = self.generate_unique_id("xianshi_item")
            sql = """
                INSERT INTO xianshi_item (id, user_id, goods_id, name, type, price, quantity)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            self.conn.execute(
                sql,
                (str(unique_id), str(user_id), int(goods_id), str(name), str(type), int(price), int(quantity))
            )
            self._commit_write()

    def remove_xianshi_item(self, item_id, quantity=1):
        """
        删除仙肆物品：
        - quantity == -1 视为系统无限库存，不删除
        - 库存小于等于购买数量时删除记录
        - 库存大于购买数量时减少对应数量
        """
        with self._conn_lock:
            quantity = abs(int(quantity))
            if quantity <= 0:
                return True
            cur = self.conn.cursor()
            cur.execute("SELECT quantity FROM xianshi_item WHERE id = %s", (str(item_id),))
            row = cur.fetchone()
            if not row:
                return False

            qty = int(row[0])
            if qty == -1:
                return True
            if qty < quantity:
                return False
            if qty <= quantity:
                self.conn.execute("DELETE FROM xianshi_item WHERE id = %s", (str(item_id),))
            else:
                self.conn.execute("UPDATE xianshi_item SET quantity=%s WHERE id=%s", (qty - quantity, str(item_id)))
            self._commit_write()
            return True

    def restore_xianshi_item(self, user_id, goods_id, name, type, price, quantity):
        """购买流程失败时回补仙肆库存。"""
        with self._conn_lock:
            quantity = abs(int(quantity))
            if quantity <= 0:
                return
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT id, quantity FROM xianshi_item
                WHERE user_id=%s AND goods_id=%s AND name=%s AND type=%s AND price=%s
                LIMIT 1
                """,
                (str(user_id), int(goods_id), str(name), str(type), int(price)),
            )
            row = cur.fetchone()
            if row:
                item_id, old_quantity = row
                if int(old_quantity) != -1:
                    cur.execute(
                        "UPDATE xianshi_item SET quantity=quantity+%s WHERE id=%s",
                        (quantity, str(item_id)),
                    )
            else:
                unique_id = self.generate_unique_id("xianshi_item")
                cur.execute(
                    """
                    INSERT INTO xianshi_item (id, user_id, goods_id, name, type, price, quantity)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (str(unique_id), str(user_id), int(goods_id), str(name), str(type), int(price), quantity),
                )
            self._commit_write()

    def remove_xianshi_all_item(self, item_id):
        with self._conn_lock:
            self.conn.execute("DELETE FROM xianshi_item WHERE id = %s", (str(item_id),))
            self._commit_write()

    def get_xianshi_items(self, user_id=None, type=None, id=None, name=None):
        with self._conn_lock:
            conditions = []
            params = []

            if user_id is not None:
                conditions.append("user_id = %s")
                params.append(str(user_id))
            if type:
                conditions.append("type = %s")
                params.append(str(type))
            if id:
                conditions.append("id = %s")
                params.append(str(id))
            if name:
                conditions.append("name = %s")
                params.append(str(name))

            q = "SELECT * FROM xianshi_item"
            if conditions:
                q += " WHERE " + " AND ".join(conditions)

            cur = self.conn.cursor()
            cur.execute(q, params)
            rows = cur.fetchall()
            if not rows:
                return None
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in rows]

    # ======== 鬼市 ========

    def add_guishi_order(self, user_id, item_id, item_name, item_type, price, quantity):
        with self._conn_lock:
            uid = self.generate_unique_id("guishi_item")
            sql = """
                INSERT INTO guishi_item (id, user_id, item_id, item_name, item_type, price, quantity)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            self.conn.execute(sql, (
                str(uid), str(user_id), int(item_id), str(item_name), str(item_type), int(price), int(quantity)
            ))
            self._commit_write()
            return uid

    def remove_guishi_order(self, order_id):
        with self._conn_lock:
            self.conn.execute("DELETE FROM guishi_item WHERE id = %s", (str(order_id),))
            self._commit_write()

    def increase_filled_quantity(self, order_id, amount):
        with self._conn_lock:
            self.conn.execute(
                "UPDATE guishi_item SET filled_quantity = filled_quantity + %s WHERE id = %s",
                (int(amount), str(order_id))
            )
            self._commit_write()

    def get_guishi_orders(self, user_id=None, name=None, type=None, id=None):
        """
        兼容：
        - type='qiugou' -> 匹配 qiugou/求购
        - type='baitan' -> 匹配 baitan/摆摊
        """
        with self._conn_lock:
            cond = []
            params = []

            if user_id is not None:
                cond.append("user_id = %s")
                params.append(str(user_id))
            if name:
                cond.append("item_name = %s")
                params.append(str(name))
            if type:
                if type == "qiugou":
                    cond.append("(item_type = %s OR item_type = %s)")
                    params.extend(["qiugou", "求购"])
                elif type == "baitan":
                    cond.append("(item_type = %s OR item_type = %s)")
                    params.extend(["baitan", "摆摊"])
                else:
                    cond.append("item_type = %s")
                    params.append(str(type))
            if id:
                cond.append("id = %s")
                params.append(str(id))

            q = "SELECT * FROM guishi_item"
            if cond:
                q += " WHERE " + " AND ".join(cond)

            cur = self.conn.cursor()
            cur.execute(q, params)
            rows = cur.fetchall()
            if not rows:
                return None
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in rows]

    def get_stored_stone(self, user_id):
        with self._conn_lock:
            cur = self.conn.cursor()
            cur.execute("SELECT stored_stone FROM guishi_info WHERE user_id = %s", (str(user_id),))
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def get_stored_items(self, user_id):
        with self._conn_lock:
            cur = self.conn.cursor()
            cur.execute("SELECT items FROM guishi_info WHERE user_id = %s", (str(user_id),))
            row = cur.fetchone()
            if not row:
                return {}
            raw = row[0] if row[0] else "{}"
            try:
                obj = json.loads(raw)
                return obj if isinstance(obj, dict) else {}
            except Exception:
                return {}

    def add_stored_item(self, user_id, item_id, quantity=1):
        with self._business_lock, self._conn_lock:
            user_id = str(user_id)
            item_id = str(item_id)
            quantity = int(quantity)

            current = self.get_stored_items(user_id)
            current[item_id] = int(current.get(item_id, 0)) + quantity
            payload = json.dumps(current, ensure_ascii=False)

            cur = self.conn.cursor()
            cur.execute("SELECT 1 FROM guishi_info WHERE user_id = %s", (user_id,))
            if cur.fetchone():
                cur.execute("UPDATE guishi_info SET items=%s WHERE user_id=%s", (payload, user_id))
            else:
                cur.execute(
                    "INSERT INTO guishi_info (user_id, stored_stone, items) VALUES (%s, 0, %s)",
                    (user_id, payload)
                )
            self._commit_write()

    def remove_stored_item(self, user_id, item_id):
        with self._business_lock, self._conn_lock:
            user_id = str(user_id)
            item_id = str(item_id)

            cur_items = self.get_stored_items(user_id)
            if item_id in cur_items:
                del cur_items[item_id]
                payload = json.dumps(cur_items, ensure_ascii=False)
                self.conn.execute("UPDATE guishi_info SET items=%s WHERE user_id=%s", (payload, user_id))
                self._commit_write()

    def update_stored_stone(self, user_id, amount, operation):
        """
        operation: add / subtract
        subtract 下限0
        """
        with self._business_lock, self._conn_lock:
            user_id = str(user_id)
            amount = int(amount)

            cur = self.conn.cursor()
            cur.execute("SELECT stored_stone FROM guishi_info WHERE user_id=%s", (user_id,))
            row = cur.fetchone()

            if row is None:
                init_val = amount if operation == "add" else 0
                cur.execute(
                    "INSERT INTO guishi_info (user_id, stored_stone, items) VALUES (%s, %s, '{}')",
                    (user_id, init_val)
                )
                self._commit_write()
                return

            old = int(row[0])
            if operation == "add":
                newv = old + amount
            else:
                newv = max(old - amount, 0)

            cur.execute("UPDATE guishi_info SET stored_stone=%s WHERE user_id=%s", (newv, user_id))
            self._commit_write()

    def try_update_stored_stone(self, user_id, amount, operation):
        """更新鬼市灵石并返回是否成功；扣减时要求余额足够。"""
        with self._business_lock, self._conn_lock:
            user_id = str(user_id)
            amount = abs(int(amount))
            if amount <= 0:
                return True

            cur = self.conn.cursor()
            if operation == "add":
                cur.execute("SELECT 1 FROM guishi_info WHERE user_id=%s", (user_id,))
                if cur.fetchone():
                    cur.execute(
                        "UPDATE guishi_info SET stored_stone=stored_stone+%s WHERE user_id=%s",
                        (amount, user_id),
                    )
                else:
                    cur.execute(
                        "INSERT INTO guishi_info (user_id, stored_stone, items) VALUES (%s, %s, '{}')",
                        (user_id, amount),
                    )
            elif operation == "subtract":
                cur.execute(
                    """
                    UPDATE guishi_info
                    SET stored_stone=stored_stone-%s
                    WHERE user_id=%s AND COALESCE(stored_stone, 0) >= %s
                    """,
                    (amount, user_id, amount),
                )
                if cur.rowcount <= 0:
                    self.conn.rollback()
                    return False
            else:
                return False
            self._commit_write()
            return True

    # ======== 拍卖等待区 ========

    def add_player_auction_item(self, user_id, item_id, item_name, start_price, user_name):
        with self._conn_lock:
            sql = """
                INSERT INTO auction_player_upload (user_id, item_id, item_name, start_price, user_name)
                VALUES (%s, %s, %s, %s, %s)
            """
            self.conn.execute(sql, (str(user_id), int(item_id), str(item_name), int(start_price), str(user_name)))
            self._commit_write()

    def get_player_auction_items(self, user_id=None):
        with self._conn_lock:
            cur = self.conn.cursor()
            if user_id is None:
                cur.execute("SELECT user_id, item_id, item_name, start_price, user_name FROM auction_player_upload")
            else:
                cur.execute(
                    "SELECT user_id, item_id, item_name, start_price, user_name FROM auction_player_upload WHERE user_id = %s",
                    (str(user_id),)
                )
            rows = cur.fetchall()
            cols = ["user_id", "item_id", "item_name", "start_price", "user_name"]
            return [dict(zip(cols, r)) for r in rows]

    def remove_player_auction_item(self, user_id, item_id):
        with self._conn_lock:
            self.conn.execute(
                "DELETE FROM auction_player_upload WHERE user_id = %s AND item_id = %s",
                (str(user_id), int(item_id))
            )
            self._commit_write()

    def claim_player_auction_item(self, user_id, item_id):
        """原子领取等待区拍卖物品，用于下架时避免重复退回。"""
        with self._conn_lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT user_id, item_id, item_name, start_price, user_name
                FROM auction_player_upload
                WHERE user_id = %s AND item_id = %s
                """,
                (str(user_id), int(item_id)),
            )
            row = cur.fetchone()
            if not row:
                return None
            self.conn.execute(
                "DELETE FROM auction_player_upload WHERE user_id = %s AND item_id = %s",
                (str(user_id), int(item_id)),
            )
            self._commit_write()
            cols = ["user_id", "item_id", "item_name", "start_price", "user_name"]
            return dict(zip(cols, row))

    def clear_player_auctions(self):
        with self._conn_lock:
            self.conn.execute("DELETE FROM auction_player_upload")
            self._commit_write()

    # ======== 当前拍卖 ========

    def set_current_auction(self, auction_items: list):
        with self._business_lock, self._conn_lock:
            self.clear_current_auction()
            cur = self.conn.cursor()
            for x in auction_items:
                bids_json = json.dumps(x.get("bids", {}), ensure_ascii=False)
                bid_times_json = json.dumps(x.get("bid_times", {}), ensure_ascii=False)
                cur.execute("""
                    INSERT INTO auction_current
                    (id, item_id, name, start_price, current_price, seller_id, seller_name, bids, bid_times, is_system, last_bid_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(x["id"]),
                    int(x["item_id"]),
                    str(x["name"]),
                    int(x["start_price"]),
                    int(x["current_price"]),
                    str(x["seller_id"]),
                    str(x["seller_name"]),
                    bids_json,
                    bid_times_json,
                    1 if bool(x.get("is_system", False)) else 0,
                    float(x.get("last_bid_time")) if x.get("last_bid_time") is not None else None
                ))
            self._commit_write()

    def get_current_auction(self, auction_id=None):
        with self._conn_lock:
            cur = self.conn.cursor()
            if auction_id is None:
                cur.execute("SELECT * FROM auction_current")
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description]
                out = []
                for r in rows:
                    d = dict(zip(cols, r))
                    try:
                        d["bids"] = json.loads(d["bids"]) if d["bids"] else {}
                    except Exception:
                        d["bids"] = {}
                    try:
                        d["bid_times"] = json.loads(d["bid_times"]) if d["bid_times"] else {}
                    except Exception:
                        d["bid_times"] = {}
                    d["is_system"] = bool(d["is_system"])
                    out.append(d)
                return out
            else:
                cur.execute("SELECT * FROM auction_current WHERE id=%s", (str(auction_id),))
                r = cur.fetchone()
                if not r:
                    return None
                cols = [c[0] for c in cur.description]
                d = dict(zip(cols, r))
                try:
                    d["bids"] = json.loads(d["bids"]) if d["bids"] else {}
                except Exception:
                    d["bids"] = {}
                try:
                    d["bid_times"] = json.loads(d["bid_times"]) if d["bid_times"] else {}
                except Exception:
                    d["bid_times"] = {}
                d["is_system"] = bool(d["is_system"])
                return d

    def update_auction_bid(self, auction_id, new_current_price, new_bids, new_bid_times, new_last_bid_time):
        with self._conn_lock:
            sql = """
                UPDATE auction_current
                SET current_price=%s, bids=%s, bid_times=%s, last_bid_time=%s
                WHERE id=%s
            """
            self.conn.execute(
                sql,
                (
                    int(new_current_price),
                    json.dumps(new_bids or {}, ensure_ascii=False),
                    json.dumps(new_bid_times or {}, ensure_ascii=False),
                    float(new_last_bid_time) if new_last_bid_time is not None else None,
                    str(auction_id)
                )
            )
            self._commit_write()

    def try_update_auction_bid(self, auction_id, old_current_price, new_current_price, new_bids, new_bid_times, new_last_bid_time):
        """仅当当前价格未变化时更新竞拍记录。"""
        with self._business_lock, self._conn_lock:
            sql = """
                UPDATE auction_current
                SET current_price=%s, bids=%s, bid_times=%s, last_bid_time=%s
                WHERE id=%s AND current_price=%s
            """
            cur = self.conn.cursor()
            cur.execute(
                sql,
                (
                    int(new_current_price),
                    json.dumps(new_bids or {}, ensure_ascii=False),
                    json.dumps(new_bid_times or {}, ensure_ascii=False),
                    float(new_last_bid_time) if new_last_bid_time is not None else None,
                    str(auction_id),
                    int(old_current_price),
                ),
            )
            success = cur.rowcount > 0
            self._commit_write()
            return success

    def clear_current_auction(self):
        with self._conn_lock:
            self.conn.execute("DELETE FROM auction_current")
            self._commit_write()

    # ======== 拍卖历史 ========

    def add_auction_history_record(self, record: dict):
        with self._conn_lock:
            sql = """
                INSERT INTO auction_history
                (auction_id, item_id, item_name, start_price, final_price, seller_id, seller_name,
                 winner_id, winner_name, status, fee, seller_earnings, start_time, end_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.conn.execute(sql, (
                str(record["auction_id"]),
                int(record["item_id"]),
                str(record["item_name"]),
                int(record["start_price"]),
                int(record["final_price"]) if record.get("final_price") is not None else None,
                str(record["seller_id"]),
                str(record["seller_name"]),
                str(record["winner_id"]) if record.get("winner_id") is not None else None,
                str(record["winner_name"]) if record.get("winner_name") is not None else None,
                str(record["status"]),
                int(record["fee"]) if record.get("fee") is not None else None,
                int(record["seller_earnings"]) if record.get("seller_earnings") is not None else None,
                float(record["start_time"]),
                float(record["end_time"])
            ))
            self._commit_write()

    def get_auction_history(self, auction_id=None):
        with self._conn_lock:
            cur = self.conn.cursor()
            if auction_id is None:
                cur.execute("SELECT * FROM auction_history ORDER BY end_time DESC")
            else:
                cur.execute("SELECT * FROM auction_history WHERE auction_id=%s ORDER BY end_time DESC", (str(auction_id),))
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in rows]

    def close(self):
        with self._conn_lock:
            if getattr(self, "conn", None):
                self.conn.close()
                self.conn = None
                logger.opt(colors=True).info("<green>trade数据库关闭！</green>")

# 这里是Player部分
class PlayerDataManager:
    global player_num
    _instance = {}
    _has_init = {}

    def __new__(cls):
        if cls._instance.get(player_num) is None:
            cls._instance[player_num] = super(PlayerDataManager, cls).__new__(cls)
        return cls._instance[player_num]

    def __init__(self):
        if not self._has_init.get(player_num):
            self._has_init[player_num] = True
            self.database_path = DATABASE / "player.db"

            # 持久连接
            self.conn = db_backend.connect(self.database_path, check_same_thread=False)
            self._conn_lock = threading.RLock()
            self.lock = self._conn_lock
            self._ensured_tables = set()
            self._ensured_fields = set()
            logger.opt(colors=True).info(f"<green>player数据库已连接！</green>")

    def _get_cursor(self):
        return self.conn.cursor()

    def _commit_write(self, conn=None):
        (conn or self.conn).commit()

    def _quote_ident(self, name):
        return db_backend.quote_ident(str(name))

    def _ensure_table_exists(self, table_name):
        if table_name in self._ensured_tables:
            return
        with self._conn_lock:
            if table_name in self._ensured_tables:
                return
            cursor = self._get_cursor()
            if not self.conn.table_exists(table_name):
                cursor.execute(f"CREATE TABLE {self._quote_ident(table_name)} (user_id TEXT PRIMARY KEY)")
                logger.opt(colors=True).info(f"<green>表 {table_name} 已创建！</green>")
            self._commit_write()
            self._ensured_tables.add(table_name)

    def _ensure_field_exists(self, table_name, field, data_type='TEXT'):
        cache_key = (table_name, field)
        if cache_key in self._ensured_fields:
            return
        with self._conn_lock:
            if cache_key in self._ensured_fields:
                return
            cursor = self._get_cursor()
            fields = self.conn.column_names(table_name)
            if field not in fields:
                cursor.execute(
                    f"ALTER TABLE {self._quote_ident(table_name)} ADD COLUMN {self._quote_ident(field)} {data_type} DEFAULT NULL"
                )
                logger.opt(colors=True).info(
                    f"<green>字段 {field} 已添加到表 {table_name}，类型为 {data_type}！</green>"
                )
            self._commit_write()
            self._ensured_fields.add(cache_key)

    def update_or_write_data(self, user_id, table_name, field, value, data_type='TEXT'):
        dt = str(data_type).upper().strip() if data_type is not None else "TEXT"
        alias = {
            "INT": "INTEGER",
            "STR": "TEXT",
            "STRING": "TEXT",
            "FLOAT": "REAL",
            "DOUBLE": "REAL",
            "BOOL": "NUMERIC",
            "BOOLEAN": "NUMERIC",
            "BLOB": "BYTEA",
        }
        data_type = alias.get(dt, dt)

        if data_type not in ['INTEGER', 'REAL', 'TEXT', 'BYTEA', 'NUMERIC']:
            logger.warning(f"不支持的数据类型: {data_type} 已设置为默认类型：TEXT")
            data_type = 'TEXT'

        self._ensure_table_exists(table_name)
        self._ensure_field_exists(table_name, field, data_type)

        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)
        else:
            value = str(value)

        with self._conn_lock:
            cursor = self._get_cursor()
            table_sql = self._quote_ident(table_name)
            field_sql = self._quote_ident(field)
            cursor.execute(
                f"UPDATE {table_sql} SET {field_sql}=%s WHERE user_id=%s",
                (value, str(user_id))
            )
            if cursor.rowcount == 0:
                cursor.execute(
                    f"INSERT INTO {table_sql} (user_id, {field_sql}) VALUES (%s, %s)",
                    (str(user_id), value)
                )
            self._commit_write()

    def get_fields(self, user_id, table_name):
        """通过user_id查看一个表这个主键的全部字段"""
        if user_id is None:
            logger.warning(f"尝试获取表 {table_name} 的字段数据但 user_id 为 None")
            return None

        self._ensure_table_exists(table_name)

        with self._conn_lock:
            cursor = self._get_cursor()
            table_sql = self._quote_ident(table_name)
            try:
                cursor.execute(f"SELECT * FROM {table_sql} WHERE user_id=%s", (str(user_id),))
                result = cursor.fetchone()
            except Exception as e:
                logger.error(f"查询表 {table_name} user_id {user_id} 时出错: {e}")
                return None

            if result is None:
                return None

            columns = [column[0] for column in cursor.description]
            user_dict = {}
            for col, val in zip(columns, result):
                if isinstance(val, str):
                    try:
                        user_dict[col] = json.loads(val)
                    except json.JSONDecodeError:
                        user_dict[col] = val
                else:
                    user_dict[col] = val

            return user_dict

    def get_field_data(self, user_id, table_name, field):
        if user_id is None:
            logger.warning(f"尝试获取表 {table_name} 字段 {field} 的数据但 user_id 为 None")
            return None

        self._ensure_table_exists(table_name)
        self._ensure_field_exists(table_name, field)

        with self._conn_lock:
            cursor = self._get_cursor()
            cursor.execute(
                f"SELECT {self._quote_ident(field)} FROM {self._quote_ident(table_name)} WHERE user_id=%s",
                (str(user_id),)
            )
            result = cursor.fetchone()

            if result and result[0] is not None:
                val = result[0]
                if isinstance(val, str):
                    try:
                        return json.loads(val)
                    except json.JSONDecodeError:
                        pass
                return val
            return None

    def get_all_field_data(self, table_name, field):
        self._ensure_table_exists(table_name)
        self._ensure_field_exists(table_name, field)

        with self._conn_lock:
            cursor = self._get_cursor()
            cursor.execute(f"SELECT user_id, {self._quote_ident(field)} FROM {self._quote_ident(table_name)}")
            result = cursor.fetchall()

            processed_results = []
            for user_id_str, val in result:
                if isinstance(val, str):
                    try:
                        processed_results.append((user_id_str, json.loads(val)))
                    except json.JSONDecodeError:
                        processed_results.append((user_id_str, val))
                else:
                    processed_results.append((user_id_str, val))
            return processed_results

    def update_all_records(self, table_name, field, value, data_type='TEXT'):
        """
        更新指定表中所有记录的某个字段的值
        """
        if data_type == 'BLOB':
            data_type = 'BYTEA'
        if data_type not in ['INTEGER', 'REAL', 'TEXT', 'BYTEA', 'NUMERIC']:
            logger.warning(f"<yellow>Unsupported data type: {data_type}. Defaulting to TEXT.</yellow>")
            data_type = 'TEXT'

        self._ensure_table_exists(table_name)
        self._ensure_field_exists(table_name, field, data_type=data_type)

        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)
        else:
            value = str(value)

        with self._conn_lock:
            cursor = self._get_cursor()
            cursor.execute(
                f"UPDATE {self._quote_ident(table_name)} SET {self._quote_ident(field)}=%s",
                (value,)
            )
            self._commit_write()

    def get_all_records(self, table_name) -> list[dict]:
        """
        获取指定表中的所有记录
        """
        self._ensure_table_exists(table_name)

        with self._conn_lock:
            cursor = self._get_cursor()
            cursor.execute(f"SELECT * FROM {self._quote_ident(table_name)}")
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            results = []
            for row in rows:
                record = {}
                for col, val in zip(columns, row):
                    if isinstance(val, str):
                        try:
                            record[col] = json.loads(val)
                        except json.JSONDecodeError:
                            record[col] = val
                    else:
                        record[col] = val
                results.append(record)
            return results

    def delete_record(self, user_id, table_name):
        """
        删除指定 user_id 在指定表中的记录
        """
        self._ensure_table_exists(table_name)

        with self._conn_lock:
            cursor = self._get_cursor()
            cursor.execute(f"DELETE FROM {self._quote_ident(table_name)} WHERE user_id=%s", (str(user_id),))
            self._commit_write()

    # ===== 通用文档接口（JSON字段友好） =====
    def _json_load_if_possible(self, val):
        if isinstance(val, str):
            try:
                return json.loads(val)
            except Exception:
                return val
        return val

    def get_doc(self, user_id, table_name, fields=None, default_factory=None):
        """
        读取某个 user_id 在 table_name 的文档（仅指定字段）。
        fields=None 时读取整行。
        default_factory: callable -> dict，记录不存在时提供默认结构
        """
        self._ensure_table_exists(table_name)
        user_id = str(user_id)

        with self._conn_lock:
            cursor = self._get_cursor()

            if fields:
                for f in fields:
                    self._ensure_field_exists(table_name, f, "TEXT")
                col_sql = ", ".join([self._quote_ident("user_id")] + [self._quote_ident(f) for f in fields])
                cursor.execute(f"SELECT {col_sql} FROM {self._quote_ident(table_name)} WHERE user_id=%s", (user_id,))
            else:
                cursor.execute(f"SELECT * FROM {self._quote_ident(table_name)} WHERE user_id=%s", (user_id,))

            row = cursor.fetchone()
            if not row:
                if default_factory:
                    data = default_factory()
                    data["user_id"] = user_id
                    return data
                return None

            columns = [c[0] for c in cursor.description]
            out = {}
            for k, v in zip(columns, row):
                out[k] = self._json_load_if_possible(v)
            return out

    def save_doc(self, user_id, table_name, data: dict, fields=None, dirty_check=True):
        """
        保存文档到 table，支持一次写多个字段。
        - data: dict
        - fields: 仅写入这些字段（None=除user_id外全部）
        - dirty_check: 开启后，仅值变化时更新
        """
        self._ensure_table_exists(table_name)
        user_id = str(user_id)

        if fields is None:
            fields = [k for k in data.keys() if k != "user_id"]

        for f in fields:
            self._ensure_field_exists(table_name, f, "TEXT")

        with self._conn_lock:
            cursor = self._get_cursor()
            table_sql = self._quote_ident(table_name)
            cursor.execute(f"SELECT 1 FROM {table_sql} WHERE user_id=%s", (user_id,))
            exists = cursor.fetchone() is not None

            old_map = {}
            if dirty_check and exists and fields:
                col_sql = ", ".join(self._quote_ident(f) for f in fields)
                cursor.execute(f"SELECT {col_sql} FROM {table_sql} WHERE user_id=%s", (user_id,))
                old_row = cursor.fetchone()
                if old_row:
                    for k, v in zip(fields, old_row):
                        old_map[k] = v

            new_map = {}
            for f in fields:
                v = data.get(f)
                if isinstance(v, (dict, list)):
                    v = json.dumps(v, ensure_ascii=False)
                elif v is None:
                    v = None
                else:
                    v = str(v)
                new_map[f] = v

            if dirty_check and exists:
                changed = any(old_map.get(f) != new_map.get(f) for f in fields)
                if not changed:
                    return

            if not exists:
                cols = ["user_id"] + fields
                vals = [user_id] + [new_map[f] for f in fields]
                ph = ",".join(["%s"] * len(cols))
                col_sql = ",".join(self._quote_ident(c) for c in cols)
                cursor.execute(f"INSERT INTO {table_sql} ({col_sql}) VALUES ({ph})", vals)
            else:
                if fields:
                    set_sql = ", ".join([f"{self._quote_ident(f)}=%s" for f in fields])
                    vals = [new_map[f] for f in fields] + [user_id]
                    cursor.execute(f"UPDATE {table_sql} SET {set_sql} WHERE user_id=%s", vals)

            self._commit_write()

    def patch_doc(self, user_id, table_name, fields, mutator, default_factory=None):
        """
        原子化读改写：
        mutator(doc) -> bool(是否有变化) 或 None(默认视为有变化)
        """
        self._ensure_table_exists(table_name)
        user_id = str(user_id)

        with self._conn_lock:
            doc = self.get_doc(user_id, table_name, fields=fields, default_factory=default_factory)
            if doc is None and default_factory:
                doc = default_factory()
                doc["user_id"] = user_id
            elif doc is None:
                doc = {"user_id": user_id}

            ret = mutator(doc)
            need_save = True if ret is None else bool(ret)
            if need_save:
                self.save_doc(user_id, table_name, doc, fields=fields, dirty_check=True)
            return doc

    def close(self):
        with self._conn_lock:
            if getattr(self, "conn", None):
                self.conn.close()
                self.conn = None
                logger.opt(colors=True).info(f"<green>player数据库已关闭！</green>")
    
# 这里是虚神界部分
class XIUXIAN_IMPART_BUFF:
    global impart_num
    _instance = {}
    _has_init = {}
    _default_fields = {
        "impart_hp_per": 0,
        "impart_atk_per": 0,
        "impart_mp_per": 0,
        "impart_exp_up": 0,
        "boss_atk": 0,
        "impart_know_per": 0,
        "impart_burst_per": 0,
        "impart_mix_per": 0,
        "impart_reap_per": 0,
        "impart_two_exp": 0,
        "stone_num": 0,
        "impart_lv": 0,
        "impart_num": 0,
        "exp_day": 0,
        "wish": 0,
    }

    def __new__(cls):
        if cls._instance.get(impart_num) is None:
            cls._instance[impart_num] = super(XIUXIAN_IMPART_BUFF, cls).__new__(cls)
        return cls._instance[impart_num]

    def __init__(self):
        if not self._has_init.get(impart_num):
            self._has_init[impart_num] = True
            self.database_path = DATABASE
            self.db_file = self.database_path / "xiuxian_impart.db"
            if not self.database_path.exists():
                self.database_path.mkdir(parents=True)

            self.conn = db_backend.connect(self.db_file, check_same_thread=False)
            self._conn_lock = threading.RLock()
            self.lock = self._conn_lock
            logger.opt(colors=True).info(f"<green>xiuxian_impart数据库已连接!</green>")
            self._check_data()

    def close(self):
        with self._conn_lock:
            if getattr(self, "conn", None):
                self.conn.close()
                self.conn = None
                logger.opt(colors=True).info(f"<green>xiuxian_impart数据库关闭!</green>")

    def _commit_write(self, conn=None):
        (conn or self.conn).commit()

    def _create_file(self) -> None:
        """创建数据库文件"""
        with self._conn_lock:
            c = self.conn.cursor()
            c.execute('''CREATE TABLE xiuxian_impart
                               (NO            INTEGER PRIMARY KEY UNIQUE,
                               USERID         TEXT     ,
                               level          INTEGER  ,
                               root           INTEGER
                               );''')
            c.execute('''''')
            c.execute('''''')
            self._commit_write()

    def _check_data(self):
        """检查数据完整性"""
        with self._conn_lock:
            c = self.conn.cursor()

            for i in config_impart.sql_table:
                if i == "xiuxian_impart":
                    try:
                        c.execute(f"select count(1) from {i}")
                    except db_backend.OperationalError:
                        c.execute(f"""CREATE TABLE "xiuxian_impart" (
        "id" INTEGER PRIMARY KEY,
        "user_id" TEXT DEFAULT 0,
        "impart_hp_per" integer DEFAULT 0,
        "impart_atk_per" integer DEFAULT 0,
        "impart_mp_per" integer DEFAULT 0,
        "impart_exp_up" integer DEFAULT 0,
        "boss_atk" integer DEFAULT 0,
        "impart_know_per" integer DEFAULT 0,
        "impart_burst_per" integer DEFAULT 0,
        "impart_mix_per" integer DEFAULT 0,
        "impart_reap_per" integer DEFAULT 0,
        "impart_two_exp" integer DEFAULT 0,
        "stone_num" integer DEFAULT 0,
        "impart_lv" integer DEFAULT 0,
        "impart_num" integer DEFAULT 0,
        "exp_day" integer DEFAULT 0,
        "wish" integer DEFAULT 0
        );""")

            for s in config_impart.sql_table_impart_buff:
                try:
                    c.execute(f"select {s} from xiuxian_impart")
                except db_backend.OperationalError:
                    sql = f"ALTER TABLE xiuxian_impart ADD COLUMN {s} integer DEFAULT 0;"
                    logger.opt(colors=True).info(f"<green>{sql}</green>")
                    logger.opt(colors=True).info(f"<green>xiuxian_impart数据库核对成功!</green>")
                    c.execute(sql)

            self._commit_write()

    @classmethod
    def close_dbs(cls):
        XIUXIAN_IMPART_BUFF().close()

    def create_user(self, user_id):
        """校验用户是否存在"""
        user_id = str(user_id)
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"select * from xiuxian_impart WHERE user_id=%s"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if not result:
                return False
            else:
                return True

    def _create_user(self, user_id: str) -> None:
        """在数据库中创建用户并初始化"""
        user_id = str(user_id)
        with self._conn_lock:
            if self.create_user(user_id):
                pass
            else:
                c = self.conn.cursor()
                columns = ["user_id", *self._default_fields.keys()]
                values = [user_id, *self._default_fields.values()]
                placeholders = ", ".join(["%s"] * len(columns))
                sql = (
                    "INSERT INTO xiuxian_impart "
                    f"({', '.join(_quote_ident(column) for column in columns)}) "
                    f"VALUES({placeholders})"
                )
                c.execute(sql, tuple(values))
                self._commit_write()

    def _repair_user_impart_info(self, user_id: str, user_dict: dict) -> dict:
        """补齐旧库或异常空数据，避免传承/虚神界入口误报未知错误。"""
        updates = []
        values = []
        for field, default in self._default_fields.items():
            if user_dict.get(field) in (None, ""):
                updates.append(f"{_quote_ident(field)}=%s")
                values.append(default)
                user_dict[field] = default

        if updates:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET {', '.join(updates)} WHERE user_id=%s"
            cur.execute(sql, tuple(values + [user_id]))
            self._commit_write()

        return user_dict

    def get_user_impart_info_with_id(self, user_id):
        """根据USER_ID获取用户impart_buff信息"""
        user_id = str(user_id)
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"select * from xiuxian_impart WHERE user_id=%s"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if not result:
                self._create_user(user_id)
                cur = self.conn.cursor()
                cur.execute(sql, (user_id,))
                result = cur.fetchone()

            if result:
                columns = [column[0] for column in cur.description]
                user_dict = dict(zip(columns, result))
                return self._repair_user_impart_info(user_id, user_dict)
            else:
                return None

    def update_impart_hp_per(self, impart_num, user_id):
        """更新impart_hp_per"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_hp_per=%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def add_impart_hp_per(self, impart_num, user_id):
        """add impart_hp_per"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_hp_per=impart_hp_per+%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def update_impart_atk_per(self, impart_num, user_id):
        """更新impart_atk_per"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_atk_per=%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def add_impart_atk_per(self, impart_num, user_id):
        """add impart_atk_per"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_atk_per=impart_atk_per+%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def update_impart_mp_per(self, impart_num, user_id):
        """impart_mp_per"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_mp_per=%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def add_impart_mp_per(self, impart_num, user_id):
        """add impart_mp_per"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_mp_per=impart_mp_per+%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def update_impart_exp_up(self, impart_num, user_id):
        """impart_exp_up"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_exp_up=%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def add_impart_exp_up(self, impart_num, user_id):
        """add impart_exp_up"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_exp_up=impart_exp_up+%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def update_boss_atk(self, impart_num, user_id):
        """boss_atk"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET boss_atk=%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def add_boss_atk(self, impart_num, user_id):
        """add boss_atk"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET boss_atk=boss_atk+%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def update_impart_know_per(self, impart_num, user_id):
        """impart_know_per"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_know_per=%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def add_impart_know_per(self, impart_num, user_id):
        """add impart_know_per"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_know_per=impart_know_per+%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def update_impart_burst_per(self, impart_num, user_id):
        """impart_burst_per"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_burst_per=%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def add_impart_burst_per(self, impart_num, user_id):
        """add impart_burst_per"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_burst_per=impart_burst_per+%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def update_impart_mix_per(self, impart_num, user_id):
        """impart_mix_per"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_mix_per=%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def add_impart_mix_per(self, impart_num, user_id):
        """add impart_mix_per"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_mix_per=impart_mix_per+%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def update_impart_reap_per(self, impart_num, user_id):
        """impart_reap_per"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_reap_per=%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def add_impart_reap_per(self, impart_num, user_id):
        """add impart_reap_per"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_reap_per=impart_reap_per+%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def update_impart_two_exp(self, impart_num, user_id):
        """更新双修"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_two_exp=%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def update_impart_num(self, impart_num, user_id):
        """更新抽卡次数"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_num=impart_num+%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def add_impart_two_exp(self, impart_num, user_id):
        """add impart_two_exp"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_two_exp=impart_two_exp+%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def update_impart_wish(self, impart_num, user_id):
        """更新祈愿值/次数"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET wish=%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def add_impart_wish(self, impart_num, user_id):
        """增加祈愿值/次数"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET wish=wish+%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def update_stone_num(self, impart_num, user_id, type_):
        """更新结晶数量"""
        with self._conn_lock:
            if type_ == 1:
                cur = self.conn.cursor()
                sql = f"UPDATE xiuxian_impart SET stone_num=stone_num+%s WHERE user_id=%s"
                cur.execute(sql, (impart_num, user_id))
                self._commit_write()
                return True
            if type_ == 2:
                cur = self.conn.cursor()
                sql = f"UPDATE xiuxian_impart SET stone_num=stone_num-%s WHERE user_id=%s"
                cur.execute(sql, (impart_num, user_id))
                self._commit_write()
                return True

    def update_impart_stone_all(self, impart_stone):
        """所有用户增加结晶"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = "UPDATE xiuxian_impart SET stone_num=stone_num+%s"
            cur.execute(sql, (impart_stone,))
            self._commit_write()

    def update_impart_lv(self, user_id, impart_lv):
        """更新虚神界等级"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = "UPDATE xiuxian_impart SET impart_lv=%s WHERE user_id=%s"
            cur.execute(sql, (impart_lv, user_id))
            self._commit_write()

    def impart_lv_reset(self):
        """重置所有用户虚神界等级"""
        with self._conn_lock:
            sql = f"UPDATE xiuxian_impart SET impart_lv=0"
            cur = self.conn.cursor()
            cur.execute(sql)
            self._commit_write()

    def impart_num_reset(self):
        """重置所有用户传承抽卡次数"""
        with self._conn_lock:
            sql = f"UPDATE xiuxian_impart SET impart_num=0"
            cur = self.conn.cursor()
            cur.execute(sql)
            self._commit_write()

    def get_impart_rank(self):
        """获取虚神界等级排行榜"""
        with self._conn_lock:
            sql = "SELECT user_id, impart_lv FROM xiuxian_impart WHERE impart_lv > 0 ORDER BY impart_lv DESC, stone_num DESC LIMIT 50"
            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            columns = [column[0] for column in cur.description]
            return [dict(zip(columns, row)) for row in result]

    def update_all_users_impart_lv(self, num, operation):
        """
        更新所有用户的虚神界等级
        :param num: 要增加/减少的数值
        :param operation: 1-增加, 2-减少
        """
        with self._conn_lock:
            cur = self.conn.cursor()
            if operation == 1:
                sql = """
                UPDATE xiuxian_impart 
                SET impart_lv = CASE 
                    WHEN impart_lv + %s > 30 THEN 30 
                    ELSE impart_lv + %s 
                END 
                WHERE impart_lv >= 0
                """
                cur.execute(sql, (num, num))
            elif operation == 2:
                sql = """
                UPDATE xiuxian_impart 
                SET impart_lv = CASE 
                    WHEN impart_lv - %s < 0 THEN 0 
                    ELSE impart_lv - %s 
                END 
                WHERE impart_lv > 0
                """
                cur.execute(sql, (num, num))
            else:
                return

            self._commit_write()

    def convert_stone_to_wishing_stone(self, user_id):
        """将思恋结晶转换为祈愿石（100:1），多余废弃"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = "SELECT stone_num FROM xiuxian_impart WHERE user_id=%s"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if result is None:
                return
            stone_num = result[0]
            if stone_num < 100:
                return
            wishing_stone_num = stone_num // 100
            sql_update = "UPDATE xiuxian_impart SET stone_num=0 WHERE user_id=%s"
            cur.execute(sql_update, (user_id,))
            self._commit_write()
            sql_message.send_back(user_id, 20005, "祈愿石", "特殊道具", wishing_stone_num, 1)

    def add_impart_exp_day(self, impart_num, user_id):
        """add impart_exp_day"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = "UPDATE xiuxian_impart SET exp_day=exp_day+%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True

    def use_impart_exp_day(self, impart_num, user_id):
        """use impart_exp_day"""
        with self._conn_lock:
            cur = self.conn.cursor()
            sql = "UPDATE xiuxian_impart SET exp_day=exp_day-%s WHERE user_id=%s"
            cur.execute(sql, (impart_num, user_id))
            self._commit_write()
            return True


def leave_harm_time(user_id):
    """重伤恢复时间"""
    user_mes = sql_message.get_user_info_with_id(user_id)
    level = user_mes['level']
    level_rate = sql_message.get_root_rate(user_mes['root_type'], user_id) # 灵根倍率
    realm_rate = jsondata.level_data()[level]["spend"] # 境界倍率
    main_buff_data = UserBuffDate(user_id).get_user_main_buff_data() # 主功法数据
    main_buff_rate_buff = main_buff_data['ratebuff'] if main_buff_data else 0 # 主功法修炼倍率
    
    try:
        time = int(((user_mes['exp'] / 2) + (user_mes['exp'] / 10) - user_mes['hp']) / (user_mes['exp'] / 10))
    except ZeroDivisionError:
        time = "无穷大"
    except OverflowError:
        time = "溢出"
    time = max(time, 1)
    return time


async def impart_check(user_id):
    if XIUXIAN_IMPART_BUFF().get_user_impart_info_with_id(user_id) is None:
        XIUXIAN_IMPART_BUFF()._create_user(user_id)
        return XIUXIAN_IMPART_BUFF().get_user_impart_info_with_id(user_id)
    else:
        return XIUXIAN_IMPART_BUFF().get_user_impart_info_with_id(user_id)
    
xiuxian_impart = XIUXIAN_IMPART_BUFF()

# 这里是buff部分
class BuffJsonDate:

    def __init__(self):
        """json文件路径"""
        self.mainbuff_jsonpath = SKILLPATHH / "主功法.json"
        self.secbuff_jsonpath = SKILLPATHH / "神通.json"
        self.effect1buff_jsonpath = SKILLPATHH / "身法.json"
        self.effect2buff_jsonpath = SKILLPATHH / "瞳术.json"
        self.gfpeizhi_jsonpath = SKILLPATHH / "功法概率设置.json"
        self.weapon_jsonpath = WEAPONPATH / "法器.json"
        self.armor_jsonpath = WEAPONPATH / "防具.json"

    def get_main_buff(self, id):
        return readf(self.mainbuff_jsonpath)[str(id)]

    def get_sec_buff(self, id):
        return readf(self.secbuff_jsonpath)[str(id)]
        
    def get_effect1_buff(self, id):
        return readf(self.effect1buff_jsonpath)[str(id)]

    def get_effect2_buff(self, id):
        return readf(self.effect2buff_jsonpath)[str(id)]
        
    def get_gfpeizhi(self):
        return readf(self.gfpeizhi_jsonpath)

    def get_weapon_data(self):
        return readf(self.weapon_jsonpath)

    def get_weapon_info(self, id):
        return readf(self.weapon_jsonpath)[str(id)]

    def get_armor_data(self):
        return readf(self.armor_jsonpath)

    def get_armor_info(self, id):
        return readf(self.armor_jsonpath)[str(id)]


class UserBuffDate:
    def __init__(self, user_id):
        """用户Buff数据"""
        self.user_id = user_id

    @property
    def BuffInfo(self):
        """获取最新的 Buff 信息"""
        return get_user_buff(self.user_id)

    def get_user_main_buff_data(self):
        """获取用户主功法数据"""
        main_buff_data = None
        buff_info = self.BuffInfo
        main_buff_id = buff_info.get('main_buff', 0)
        if main_buff_id != 0:
            main_buff_data = items.get_data_by_item_id(main_buff_id)
        return main_buff_data
    
    def get_user_sub_buff_data(self):
        """获取用户辅修功法数据"""
        sub_buff_data = None
        buff_info = self.BuffInfo
        sub_buff_id = buff_info.get('sub_buff', 0)
        if sub_buff_id != 0:
            sub_buff_data = items.get_data_by_item_id(sub_buff_id)
        return sub_buff_data

    def get_user_sec_buff_data(self):
        """获取用户神通数据"""
        sec_buff_data = None
        buff_info = self.BuffInfo
        sec_buff_id = buff_info.get('sec_buff', 0)
        if sec_buff_id != 0:
            sec_buff_data = items.get_data_by_item_id(sec_buff_id)
        return sec_buff_data

    def get_user_effect1_buff_data(self):
        """获取用户身法数据"""
        effect1_buff_data = None
        buff_info = self.BuffInfo
        effect1_buff_id = buff_info.get('effect1_buff', 0)
        if effect1_buff_id != 0:
            effect1_buff_data = items.get_data_by_item_id(effect1_buff_id)
        return effect1_buff_data

    def get_user_effect2_buff_data(self):
        """获取用户瞳术数据"""
        effect2_buff_data = None
        buff_info = self.BuffInfo
        effect2_buff_id = buff_info.get('effect2_buff', 0)
        if effect2_buff_id != 0:
            effect2_buff_data = items.get_data_by_item_id(effect2_buff_id)
        return effect2_buff_data
        
    def get_user_weapon_data(self):
        """获取用户法器数据"""
        weapon_data = None
        buff_info = self.BuffInfo
        weapon_id = buff_info.get('faqi_buff', 0)
        if weapon_id != 0:
            weapon_data = items.get_data_by_item_id(weapon_id)
        return weapon_data

    def get_user_armor_buff_data(self):
        """获取用户防具数据"""
        armor_buff_data = None
        buff_info = self.BuffInfo
        armor_buff_id = buff_info.get('armor_buff', 0)
        if armor_buff_id != 0:
            armor_buff_data = items.get_data_by_item_id(armor_buff_id)
        return armor_buff_data

def calc_accessory_effects(user_id: str | int) -> dict:
    """
    计算饰品效果：
    - 基础词条总和（气血/攻击/会心/会伤/减伤/抗暴）
    - 套装件数
    - 套装激活列表（2件/4件）

    返回:
    {
      "hp_pct": float,
      "atk_pct": float,
      "crit_rate": float,
      "crit_damage": float,
      "dmg_reduction": float,
      "crit_resist": float,
      "speed": float,
      "speed_pct": float,
      "set_count": {"烈阳":2,...},
      "set_bonus": [{"set":"烈阳","pieces":2,"type":"attack","value":0.08}, ...]
    }
    """
    result = {
        "hp_pct": 0.0,
        "atk_pct": 0.0,
        "crit_rate": 0.0,
        "crit_damage": 0.0,
        "dmg_reduction": 0.0,
        "crit_resist": 0.0,
        "speed": 0.0,
        "speed_pct": 0.0,
        "set_count": {},
        "set_bonus": []
    }

    from ..xiuxian_back import AFFIX_KEY_MAP, SET_BONUS
    acc_data = get_user_accessory_data(user_id)
    equipped = acc_data.get("equipped", {})

    for _, acc in equipped.items():
        if not acc:
            continue

        set_type = acc.get("set_type", "")
        if set_type:
            result["set_count"][set_type] = result["set_count"].get(set_type, 0) + 1

        affixes = acc.get("affixes", [])
        if not isinstance(affixes, list):
            continue

        for af in affixes:
            a_type = af.get("type")
            a_val = float(af.get("value", 0))
            mapped = AFFIX_KEY_MAP.get(a_type)

            if mapped == "hp_pct":
                result["hp_pct"] += a_val
            elif mapped == "atk_pct":
                result["atk_pct"] += a_val
            elif mapped == "crit_rate":
                result["crit_rate"] += a_val
            elif mapped == "crit_damage":
                result["crit_damage"] += a_val
            elif mapped == "dmg_reduction":
                result["dmg_reduction"] += a_val
            elif mapped == "crit_resist":
                result["crit_resist"] += a_val
            elif mapped == "speed":
                result["speed"] += a_val

    # 套装激活（2件/4件）
    for set_name, cnt in result["set_count"].items():
        conf = SET_BONUS.get(set_name, {})

        if cnt >= 2 and 2 in conf:
            v = conf[2]
            result["set_bonus"].append({
                "set": set_name,
                "pieces": 2,
                "type": v.get("type"),
                "value": float(v.get("value", 0))
            })

        if cnt >= 4 and 4 in conf:
            v = conf[4]
            result["set_bonus"].append({
                "set": set_name,
                "pieces": 4,
                "type": v.get("type"),
                "value": float(v.get("value", 0))
            })

    return result


def calc_realm_base_speed(level_name: str) -> int:
    """按境界生成基础速度；境界越高，基础速度越高。"""
    _, ranks = convert_rank("江湖好手")
    level = str(level_name or "")

    try:
        idx = ranks.index(level)
    except ValueError:
        idx = 0
        for i, rank in enumerate(ranks):
            if level and (level in rank or rank in level):
                idx = i
                break

    return 100 + idx * 4


def get_user_accessory_data(user_id: str | int) -> dict:
    """
    读取玩家饰品数据（player.db -> player_accessory）
    返回结构:
    {
        "equipped": {"手镯": {...}|None, "戒指": ..., "手环": ..., "项链": ...},
        "bag": [...]
    }
    """
    data = player_data_manager.get_fields(str(user_id), "player_accessory")
    if not data:
        return {
            "equipped": {"手镯": None, "戒指": None, "手环": None, "项链": None},
            "bag": []
        }

    equipped = data.get("equipped") or {"手镯": None, "戒指": None, "手环": None, "项链": None}
    bag = data.get("bag") or []
    return {"equipped": equipped, "bag": bag}

def final_user_data(user_data, columns):
    """传入数据库行与列描述，返回叠加buff后的用户信息（统一口径）"""
    user_dict = dict(zip((col[0] for col in columns), user_data))
    user_id = user_dict.get("user_id")
    if not user_id:
        return user_dict

    final_attr = get_final_attributes(user_id, ratio=1.0, include_current=True)
    if not final_attr:
        return user_dict

    # 仅覆盖需要动态计算的字段，其他字段保持原样
    user_dict["hp"] = int(final_attr["current_hp"])
    user_dict["mp"] = int(final_attr["current_mp"])
    user_dict["atk"] = int(final_attr["final_atk"])
    return user_dict

def get_base_attributes(user_id: str | int) -> dict | None:
    """获取基础属性（不吃任何buff）"""
    user = sql_message.get_user_info_with_id(user_id)
    if not user:
        return None

    return {
        "user_id": user["user_id"],
        "nickname": user["user_name"],
        "level": user["level"],
        "exp": int(user["exp"]),
        "stone": int(user["stone"]),

        "base_hp": int(user["hp"]),
        "base_mp": int(user["mp"]),
        "base_atk": int(user["atk"]),

        "atkpractice": int(user.get("atkpractice", 0)),
        "hppractice": int(user.get("hppractice", 0)),
        "mppractice": int(user.get("mppractice", 0)),
    }


def get_final_attributes(user_id: str | int, ratio: float = 1.0, include_current: bool = True) -> dict | None:
    """获取buff加成后的最终属性（统一口径）"""
    base = get_base_attributes(user_id)
    if not base:
        return None

    # buff数据
    user_buff = UserBuffDate(user_id)
    buff_info = user_buff.BuffInfo or {}

    main = user_buff.get_user_main_buff_data() or {}
    sub = user_buff.get_user_sub_buff_data() or {}
    weapon = user_buff.get_user_weapon_data() or {}
    armor = user_buff.get_user_armor_buff_data() or {}

    impart = xiuxian_impart.get_user_impart_info_with_id(user_id) or {}

    # 主功法
    main_hp = float(main.get("hpbuff", 0))
    main_mp = float(main.get("mpbuff", 0))
    main_atk = float(main.get("atkbuff", 0))
    main_crit = float(main.get("crit_buff", 0))
    main_critatk = float(main.get("critatk", 0))
    main_def = float(main.get("def_buff", 0))

    # ===== 独立减会心伤害（减法区）=====
    main_crit_dmg_reduce = float(main.get("crit_damage_reduction", 0))

    # 装备
    weapon_atk = float(weapon.get("atk_buff", 0))
    weapon_crit = float(weapon.get("crit_buff", 0))
    weapon_critatk = float(weapon.get("critatk", 0))
    weapon_def = float(weapon.get("def_buff", 0))
    weapon_crit_dmg_reduce = float(weapon.get("crit_damage_reduction", 0))

    armor_atk = float(armor.get("atk_buff", 0))
    armor_crit = float(armor.get("crit_buff", 0))
    armor_def = float(armor.get("def_buff", 0))
    armor_crit_dmg_reduce = float(armor.get("crit_damage_reduction", 0))

    # 传承
    impart_hp = float(impart.get("impart_hp_per", 0))
    impart_mp = float(impart.get("impart_mp_per", 0))
    impart_atk = float(impart.get("impart_atk_per", 0))
    impart_know = float(impart.get("impart_know_per", 0))
    impart_burst = float(impart.get("impart_burst_per", 0))
    boss_atk = float(impart.get("boss_atk", 0))

    impart_crit_dmg_reduce = float(impart.get("crit_damage_reduction", 0))

    # 修炼等级
    hppractice = base["hppractice"] * 0.05
    mppractice = base["mppractice"] * 0.05
    atkpractice = base["atkpractice"] * 0.04

    # 永久攻击buff
    perm_atk = int(buff_info.get("atk_buff", 0) or 0)

    # ===== 常规最终值（不含饰品）=====
    max_hp = int((base["exp"] / 2) * (1 + main_hp + impart_hp + hppractice))
    max_mp = int(base["exp"] * (1 + main_mp + impart_mp + mppractice))

    current_hp = int(base["base_hp"] * (1 + main_hp + impart_hp + hppractice))
    current_mp = int(base["base_mp"] * (1 + main_mp + impart_mp + mppractice))

    final_atk = int(
        (base["base_atk"] * (1 + atkpractice) * (1 + main_atk) * (1 + weapon_atk) * (1 + armor_atk)) * (1 + impart_atk)
    ) + perm_atk

    # 会心率
    crit_rate = max(0, min(1, weapon_crit + armor_crit + main_crit + impart_know))

    # 会心伤害倍率（比如1.5表示150%）
    crit_damage = 1.5 + impart_burst + weapon_critatk + main_critatk

    # 减伤
    damage_reduction = main_def + weapon_def + armor_def

    # 护甲穿透
    armor_penetration = 0.0

    # ===== 独立防暴体系 =====
    # 抗暴（乘法区）
    crit_resist = 0.0

    # 减会心伤害（减法区）
    crit_damage_reduction = (
        main_crit_dmg_reduce
        + weapon_crit_dmg_reduce
        + armor_crit_dmg_reduce
        + impart_crit_dmg_reduce
    )

    # ===== 饰品加成 =====
    acc_effect = calc_accessory_effects(user_id)

    # 百分比型基础属性加成
    max_hp = int(max_hp * (1 + float(acc_effect.get("hp_pct", 0))))
    current_hp = int(current_hp * (1 + float(acc_effect.get("hp_pct", 0))))
    final_atk = int(final_atk * (1 + float(acc_effect.get("atk_pct", 0))))

    # 战斗率属性加成
    crit_rate += float(acc_effect.get("crit_rate", 0))
    crit_damage += float(acc_effect.get("crit_damage", 0))
    damage_reduction += float(acc_effect.get("dmg_reduction", 0))

    # 饰品抗暴 -> 独立进入抗暴乘法区
    crit_resist += float(acc_effect.get("crit_resist", 0))

    # 速度：境界提供基础值；装备、功法、饰品词条提供固定值或百分比。
    base_speed = calc_realm_base_speed(base["level"])
    speed_flat = (
        float(main.get("speed", 0))
        + float(sub.get("speed", 0))
        + float(weapon.get("speed", 0))
        + float(armor.get("speed", 0))
        + float(acc_effect.get("speed", 0))
    )
    speed_pct = (
        float(main.get("speed_buff", 0))
        + float(weapon.get("speed_buff", 0))
        + float(armor.get("speed_buff", 0))
        + float(acc_effect.get("speed_pct", 0))
    )

    # ===== 套装效果直接落地的部分 =====
    set_bonus_effects = acc_effect.get("set_bonus", []) or []
    for sb in set_bonus_effects:
        sb_type = sb.get("type")
        sb_val = float(sb.get("value", 0))

        if sb_type == "attack":
            final_atk = int(final_atk * (1 + sb_val))
        elif sb_type == "dmg_reduction":
            damage_reduction += sb_val
        elif sb_type == "crit_rate":
            crit_rate += sb_val
        elif sb_type == "armor_pen":
            armor_penetration += sb_val
        elif sb_type == "dodge":
            # dodge 为固定点数，战斗层读取
            pass
        elif sb_type == "speed_pct":
            speed_pct += sb_val
        elif sb_type in ["shield", "reflect", "true_damage", "shield_break"]:
            # 这些交给战斗层处理
            pass

    final_speed = max(1, int((base_speed + speed_flat) * max(0.1, 1 + speed_pct)))

    # 上限裁剪
    crit_rate = max(0.0, crit_rate)
    crit_resist = max(0.0, crit_resist)
    crit_damage_reduction = max(0.0, crit_damage_reduction)

    # 比例缩放（例如PVE多队平衡）
    max_hp = int(max_hp * ratio)
    max_mp = int(max_mp * ratio)
    current_hp = int(current_hp * ratio) if include_current else max_hp
    current_mp = int(current_mp * ratio) if include_current else max_mp
    final_atk = int(final_atk * ratio)

    # 当前值不能超过上限，也不能低于0
    max_hp = max(1, max_hp)
    max_mp = max(1, max_mp)
    current_hp = max(0, min(current_hp, max_hp))
    current_mp = max(0, min(current_mp, max_mp))

    # ===== 炼体固定加值=====
    _tianti_hp = 0
    _tianti_add_hp = 0
    _tianti_add_atk = 0
    try:
        from ..xiuxian_tianti.tianti_data import TiantiDataManager
        _tm = TiantiDataManager()
        _tdata = _tm.get_user_tianti_info(user_id)
        _tianti_hp = int(_tdata.get("tianti_hp", 0))

        # 规则：1炼体气血 = 1hp；100炼体气血 = 1攻击
        _tianti_add_hp = _tianti_hp
        _tianti_add_atk = _tianti_hp // 100

        max_hp += _tianti_add_hp
        current_hp += _tianti_add_hp
        final_atk += _tianti_add_atk

        # 再次防溢出
        max_hp = max(1, max_hp)
        current_hp = max(0, min(current_hp, max_hp))
    except Exception:
        pass

    return {
        **base,
        "max_hp": max_hp,
        "max_mp": max_mp,
        "current_hp": current_hp,
        "current_mp": current_mp,
        "final_atk": final_atk,
        "base_speed": base_speed,
        "speed_flat": speed_flat,
        "speed_pct": speed_pct,
        "speed": final_speed,

        # 暴击体系
        "crit_rate": crit_rate,
        "crit_damage": crit_damage,

        "crit_resist": crit_resist,  # 抗暴：乘法
        "crit_damage_reduction": crit_damage_reduction,  # 减会伤：减法

        "damage_reduction": damage_reduction,
        "armor_penetration": armor_penetration,
        "boss_damage_bonus": boss_atk,

        # 饰品/套装扩展
        "accessory_effect": acc_effect,
        "set_bonus_effects": set_bonus_effects,

        # 炼体扩展
        "tianti_hp_raw": _tianti_hp,
        "tianti_atk_add": _tianti_add_atk
    }

def get_weapon_info_msg(weapon_id, weapon_info=None):
    """
    获取一个法器(武器)信息msg
    :param weapon_id:法器(武器)ID
    :param weapon_info:法器(武器)信息json,可不传
    :return 法器(武器)信息msg
    """
    msg = ''
    if weapon_info is None:
        weapon_info = items.get_data_by_item_id(weapon_id)
    atk_buff_msg = f"提升{int(weapon_info['atk_buff'] * 100)}%攻击力！" if weapon_info['atk_buff'] != 0 else ''
    crit_buff_msg = f"提升{int(weapon_info['crit_buff'] * 100)}%会心率！" if weapon_info['crit_buff'] != 0 else ''
    crit_atk_msg = f"提升{int(weapon_info['critatk'] * 100)}%会心伤害！" if weapon_info['critatk'] != 0 else ''
    def_buff_msg = f"{'提升' if weapon_info['def_buff'] > 0 else '降低'}{int(abs(weapon_info['def_buff']) * 100)}%减伤率！" if weapon_info['def_buff'] != 0 else ''
    speed_msg = f"提升{int(weapon_info.get('speed', 0))}点速度！" if weapon_info.get('speed', 0) != 0 else ''
    speed_buff_msg = f"提升{int(weapon_info.get('speed_buff', 0) * 100)}%速度！" if weapon_info.get('speed_buff', 0) != 0 else ''
    zw_buff_msg = f"装备专属武器时提升伤害！！" if weapon_info['zw'] != 0 else ''
    mp_buff_msg = f"降低真元消耗{int(weapon_info['mp_buff'] * 100)}%！" if weapon_info['mp_buff'] != 0 else ''
    crit_damage_reduction_msg = f"降低敌方会心伤害{int(weapon_info.get('crit_damage_reduction', 0) * 100)}%！" if weapon_info.get('crit_damage_reduction', 0) != 0 else ''
    msg += f"名字：{weapon_info['name']}\n"
    msg += f"品阶：{weapon_info['level']}\n"
    msg += f"效果：{weapon_info['desc']}，{atk_buff_msg}{crit_buff_msg}{crit_atk_msg}{def_buff_msg}{speed_msg}{speed_buff_msg}{mp_buff_msg}{crit_damage_reduction_msg}{zw_buff_msg}"
    return msg


def get_armor_info_msg(armor_id, armor_info=None):
    """
    获取一个法宝(防具)信息msg
    :param armor_id:法宝(防具)ID
    :param armor_info;法宝(防具)信息json,可不传
    :return 法宝(防具)信息msg
    """
    msg = ''
    if armor_info is None:
        armor_info = items.get_data_by_item_id(armor_id)
    def_buff_msg = f"提升{int(armor_info['def_buff'] * 100)}%减伤率！"
    atk_buff_msg = f"提升{int(armor_info['atk_buff'] * 100)}%攻击力！" if armor_info['atk_buff'] != 0 else ''
    crit_buff_msg = f"提升{int(armor_info['crit_buff'] * 100)}%会心率！" if armor_info['crit_buff'] != 0 else ''
    speed_msg = f"提升{int(armor_info.get('speed', 0))}点速度！" if armor_info.get('speed', 0) != 0 else ''
    speed_buff_msg = f"提升{int(armor_info.get('speed_buff', 0) * 100)}%速度！" if armor_info.get('speed_buff', 0) != 0 else ''
    msg += f"名字：{armor_info['name']}\n"
    msg += f"品阶：{armor_info['level']}\n"
    msg += f"效果：{armor_info['desc']}，{def_buff_msg}{atk_buff_msg}{crit_buff_msg}{speed_msg}{speed_buff_msg}"
    return msg


def get_main_info_msg(id):
    """获取一个主功法信息msg"""
    mainbuff = items.get_data_by_item_id(id)
    hpmsg = f"提升{round(mainbuff['hpbuff'] * 100, 0)}%气血" if mainbuff['hpbuff'] != 0 else ''
    mpmsg = f"，提升{round(mainbuff['mpbuff'] * 100, 0)}%真元" if mainbuff['mpbuff'] != 0 else ''
    atkmsg = f"，提升{round(mainbuff['atkbuff'] * 100, 0)}%攻击力" if mainbuff['atkbuff'] != 0 else ''
    ratemsg = f"，提升{round(mainbuff['ratebuff'] * 100, 0)}%修炼速度" if mainbuff['ratebuff'] != 0 else ''
    speed_msg = f"，提升{round(mainbuff.get('speed', 0))}点战斗速度" if mainbuff.get('speed', 0) != 0 else ''
    speed_buff_msg = f"，提升{round(mainbuff.get('speed_buff', 0) * 100, 0)}%战斗速度" if mainbuff.get('speed_buff', 0) != 0 else ''
    
    cri_tmsg = f"，提升{round(mainbuff['crit_buff'] * 100, 0)}%会心率" if mainbuff['crit_buff'] != 0 else ''
    def_msg = f"，{'提升' if mainbuff['def_buff'] > 0 else '降低'}{round(abs(mainbuff['def_buff']) * 100, 0)}%减伤率" if mainbuff['def_buff'] != 0 else ''
    dan_msg = f"，增加炼丹产出{round(mainbuff['dan_buff'])}枚" if mainbuff['dan_buff'] != 0 else ''
    dan_exp_msg = f"，每枚丹药额外增加{round(mainbuff['dan_exp'])}炼丹经验" if mainbuff['dan_exp'] != 0 else ''
    reap_msg = f"，提升药材收取数量{round(mainbuff['reap_buff'])}个" if mainbuff['reap_buff'] != 0 else ''
    exp_msg = f"，突破失败{round(mainbuff['exp_buff'] * 100, 0)}%经验保护" if mainbuff['exp_buff'] != 0 else ''
    critatk_msg = f"，提升{round(mainbuff['critatk'] * 100, 0)}%会心伤害" if mainbuff['critatk'] != 0 else ''
    two_msg = f"，增加{round(mainbuff['two_buff'])}次双修次数" if mainbuff['two_buff'] != 0 else ''
    number_msg = f"，提升{round(mainbuff['number'])}%突破概率" if mainbuff['number'] != 0 else ''
    
    clo_exp_msg = f"，提升{round(mainbuff['clo_exp'] * 100, 0)}%闭关经验" if mainbuff['clo_exp'] != 0 else ''
    clo_rs_msg = f"，提升{round(mainbuff['clo_rs'] * 100, 0)}%闭关生命回复" if mainbuff['clo_rs'] != 0 else ''
    random_buff_msg = f"，战斗时随机获得一个战斗属性" if mainbuff['random_buff'] != 0 else ''
    ew_name = items.get_data_by_item_id(mainbuff['ew']) if mainbuff['ew'] != 0 else ''
    ew_msg =  f"，使用{ew_name['name']}时伤害增加50%！" if mainbuff['ew'] != 0 else ''
    msg = f"{hpmsg}{mpmsg}{atkmsg}{ratemsg}{speed_msg}{speed_buff_msg}{cri_tmsg}{def_msg}{dan_msg}{dan_exp_msg}{reap_msg}{exp_msg}{critatk_msg}{two_msg}{number_msg}{clo_exp_msg}{clo_rs_msg}{random_buff_msg}{ew_msg}！"
    return mainbuff, msg

def get_sub_info_msg(id): #辅修功法8
    """获取辅修信息msg"""
    subbuff = items.get_data_by_item_id(id)
    submsg = ""
    if subbuff['buff_type'] == '1':
        submsg = "提升" + subbuff['buff'] + "%攻击力"
    if subbuff['buff_type'] == '2':
        submsg = "提升" + subbuff['buff'] + "%暴击率"
    if subbuff['buff_type'] == '3':
        submsg = "提升" + subbuff['buff'] + "%暴击伤害"
    if subbuff['buff_type'] == '4':
        submsg = "提升" + subbuff['buff'] + "%每回合气血回复"
    if subbuff['buff_type'] == '5':
        submsg = "提升" + subbuff['buff'] + "%每回合真元回复"
    if subbuff['buff_type'] == '6':
        submsg = "提升" + subbuff['buff'] + "%气血吸取"
    if subbuff['buff_type'] == '7':
        submsg = "提升" + subbuff['buff'] + "%真元吸取"
    if subbuff['buff_type'] == '8':
        submsg = "给对手造成" + subbuff['buff'] + "%中毒"
    if subbuff['buff_type'] == '9':
        submsg = f"提升{subbuff['buff']}%气血吸取,提升{subbuff['buff2']}%真元吸取"
    if subbuff['buff_type'] == '15':
        submsg = "提升" + subbuff['buff'] + "%战斗速度"
    if subbuff['buff_type'] == '16':
        submsg = "降低对手" + subbuff['buff'] + "%战斗速度"

    stone_msg  = "提升{}%boss战灵石获取".format(round(subbuff['stone'] * 100, 0)) if subbuff['stone'] != 0 else ''
    integral_msg = "，提升{}点boss战积分获取".format(round(subbuff['integral'])) if subbuff['integral'] != 0 else ''
    jin_msg = "禁止对手吸取" if subbuff['jin'] != 0 else ''
    drop_msg = "，提升boss掉落率" if subbuff['drop'] != 0 else ''
    fan_msg = "使对手发出的debuff失效" if subbuff['fan'] != 0 else ''
    break_msg = "获得{}%穿甲".format(round(subbuff['break'] * 100, 0)) if subbuff['break'] != 0 else ''
    exp_msg = "，增加战斗获得的修为" if subbuff['exp'] != 0 else ''
    

    msg = f"{submsg}{stone_msg}{integral_msg}{jin_msg}{drop_msg}{fan_msg}{break_msg}{exp_msg}"
    return subbuff, msg

def get_user_buff(user_id):
    BuffInfo = sql_message.get_user_buff_info(user_id)
    if BuffInfo is None:
        sql_message.initialize_user_buff_info(user_id)
        return sql_message.get_user_buff_info(user_id)
    else:
        return BuffInfo


def readf(FILEPATH):
    with open(FILEPATH, "r", encoding="UTF-8") as f:
        data = f.read()
    return json.loads(data)


def get_sec_msg(secbuffdata):
    msg = None
    if secbuffdata is None:
        msg = "无"
        return msg
    hpmsg = f"，消耗当前血量{int(secbuffdata['hpcost'] * 100)}%" if secbuffdata['hpcost'] != 0 else ''
    mpmsg = f"，消耗真元{int(secbuffdata['mpcost'] * 100)}%" if secbuffdata['mpcost'] != 0 else ''

    if secbuffdata['skill_type'] == 1:
        shmsg = ''
        for value in secbuffdata['atkvalue']:
            shmsg += f"{value}倍、"
        if secbuffdata['turncost'] == 0:
            msg = f"攻击{len(secbuffdata['atkvalue'])}次，造成{shmsg[:-1]}伤害{hpmsg}{mpmsg}，释放概率：{secbuffdata['rate']}%"
        else:
            msg = f"连续攻击{len(secbuffdata['atkvalue'])}次，造成{shmsg[:-1]}伤害{hpmsg}{mpmsg}，休息{secbuffdata['turncost']}回合，释放概率：{secbuffdata['rate']}%"
    elif secbuffdata['skill_type'] == 2:
        msg = f"持续伤害，造成{secbuffdata['atkvalue']}倍攻击力伤害{hpmsg}{mpmsg}，持续{secbuffdata['turncost']}回合，释放概率：{secbuffdata['rate']}%"
    elif secbuffdata['skill_type'] == 3:
        if secbuffdata['bufftype'] == 1:
            msg = f"增强自身，提高{secbuffdata['buffvalue']}倍攻击力{hpmsg}{mpmsg}，持续{secbuffdata['turncost']}回合，释放概率：{secbuffdata['rate']}%"
        elif secbuffdata['bufftype'] == 2:
            msg = f"增强自身，提高{secbuffdata['buffvalue'] * 100}%减伤率{hpmsg}{mpmsg}，持续{secbuffdata['turncost']}回合，释放概率：{secbuffdata['rate']}%"
    elif secbuffdata['skill_type'] == 4:
        msg = f"封印对手{hpmsg}{mpmsg}，持续{secbuffdata['turncost']}回合，释放概率：{secbuffdata['rate']}%，命中成功率{secbuffdata['success']}%"
    elif secbuffdata['skill_type'] == 5:
        if secbuffdata['turncost'] == 0:
            msg = f"随机伤害，造成{secbuffdata['atkvalue']}倍～{secbuffdata['atkvalue2']}倍攻击力伤害{hpmsg}{mpmsg}，释放概率：{secbuffdata['rate']}%"
        else:
            msg = f"随机伤害，造成{secbuffdata['atkvalue']}倍～{secbuffdata['atkvalue2']}倍攻击力伤害{hpmsg}{mpmsg}，休息{secbuffdata['turncost']}回合，释放概率：{secbuffdata['rate']}%"
        
    elif secbuffdata['skill_type'] == 6:
        msg = f"叠加伤害，每回合叠加{secbuffdata['buffvalue']}倍攻击力{hpmsg}{mpmsg}，持续{secbuffdata['turncost']}回合，释放概率：{secbuffdata['rate']}%"

    elif secbuffdata['skill_type'] == 7:
        msg = "变化神通，战斗时随机获得一个神通"
            
    return msg

def get_effect_info_msg(id): #身法、瞳术
    """获取秘术信息msg"""
    effectbuff = items.get_data_by_item_id(id)
    effectmsg = ""
    if effectbuff['buff_type'] == '1':
        effectmsg = f"提升{effectbuff['buff2']}%～{effectbuff['buff']}%闪避率"
    if effectbuff['buff_type'] == '2':
        effectmsg = f"提升{effectbuff['buff2']}%～{effectbuff['buff']}%命中率"
    if effectbuff['buff_type'] == '3':
        effectmsg = f"提升{effectbuff['buff2']}%～{effectbuff['buff']}%战斗速度"
    speed_low = effectbuff.get("speed_buff")
    speed_high = effectbuff.get("speed_buff2")
    if speed_low is not None or speed_high is not None:
        speed_low = float(speed_low or speed_high or 0)
        speed_high = float(speed_high or speed_low)
        if speed_low > speed_high:
            speed_low, speed_high = speed_high, speed_low
        effectmsg += f"，提升{round(speed_low * 100, 0)}%～{round(speed_high * 100, 0)}%战斗速度"
    

    msg = f"{effectmsg}"
    return effectbuff, msg

mix_elixir_infoconfigkey = ["收取时间", "收取等级", "灵田数量", '药材速度', '灵田傀儡', "丹药控火", "丹药耐药性", "炼丹记录", "炼丹经验"]

def read_player_info(user_id, info_name):
    player_data_manager = PlayerDataManager()
    user_id_str = str(user_id)
    info = {}
    record = player_data_manager.get_fields(user_id_str, info_name) # 直接获取所有字段
    if record:
        for field in mix_elixir_infoconfigkey:
            if field in record:
                info[field] = record[field]
    return info

def save_player_info(user_id, data, info_name):
    player_data_manager = PlayerDataManager()
    user_id_str = str(user_id)
    for field, value in data.items():
        player_data_manager.update_or_write_data(user_id_str, info_name, field, value, data_type="TEXT")

def get_player_info(user_id, info_name):
    MIXELIXIRINFOCONFIG = {
        "收取时间": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), # str
        "收取等级": 0,
        "灵田数量": 1,
        '药材速度': 0,
        '灵田傀儡': 0,
        "丹药控火": 0,
        "丹药耐药性": 0,
        "炼丹记录": {},
        "炼丹经验": 0
    }
    
    player_info = read_player_info(user_id, info_name)
    
    # 检查并补全缺失字段
    updated_info = False
    for key in mix_elixir_infoconfigkey:
        if key not in player_info:
            player_info[key] = MIXELIXIRINFOCONFIG[key]
            updated_info = True
            
    if updated_info: # 如果有更新，则保存
        save_player_info(user_id, player_info, info_name)
        
    return player_info

def _safe_parse_dt(dt_str):
    if not dt_str:
        return None
    if isinstance(dt_str, datetime):
        return dt_str
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(dt_str), fmt)
        except ValueError:
            continue
    return None


def number_count(num):
    """
    数据库安全处理：
    如果数值超过 SQLite INTEGER 限制 (9,223,372,036,854,775,807)，返回科学计数法字符串。
    否则返回 int。
    """
    MAX_SQLITE_INT = 9223372036854775807
    try:
        val = float(num)
    except (TypeError, ValueError):
        raise ValueError("输入必须是数字")
    
    if val > MAX_SQLITE_INT:
        # 超过上限，返回科学计数法字符串，例如 "1.23e+20"
        return "{:.2e}".format(val)
    return int(val)

def backup_db_files():
    """
    兼容旧调用：转发到 UpdateManager.backup_db_files
    """
    return UpdateManager().backup_db_files()

_ID_DB_PATHS = {
    "xiuxian.db": DATABASE / "xiuxian.db",
    "xiuxian_impart.db": DATABASE / "xiuxian_impart.db",
    "trade.db": DATABASE / "trade.db",
    "player.db": DATABASE / "player.db",
}

_ID_TARGET_COLS_BY_DB = {
    "xiuxian.db": {"user_id", "sect_owner"},
    "xiuxian_impart.db": {"user_id"},
    "trade.db": {"user_id"},
    "player.db": {"user_id", "partner_id", "group_id", "main_id", "active_id"},
}

_TEXT_TYPES = {"text", "character varying", "character"}


def _id_table_targets(conn, db_name: str):
    target_cols = _ID_TARGET_COLS_BY_DB.get(db_name, {"user_id"})
    for table in conn.list_tables():
        fields_info = conn.table_info(table)
        columns = {row[1] for row in fields_info}
        hit_cols = sorted(columns.intersection(target_cols))
        for col in hit_cols:
            yield table, col, fields_info


def _ensure_id_target_columns_text() -> list[str]:
    logs = []
    for db_name, db_path in _ID_DB_PATHS.items():
        conn = db_backend.connect(db_path, check_same_thread=False)
        try:
            checked = sum(1 for _table, _col, _fields_info in _id_table_targets(conn, db_name))
            logs.append(f"{db_name}: ID字段检查完成，字段数={checked}")
        except Exception as e:
            conn.rollback()
            logs.append(f"{db_name}: ID字段TEXT检查失败 -> {e}")
            raise
        finally:
            conn.close()
    return logs


def _collect_all_candidate_ids() -> set[str]:
    all_ids = set()
    for db_name, db_path in _ID_DB_PATHS.items():
        conn = db_backend.connect(db_path, check_same_thread=False)
        try:
            cur = conn.cursor()
            for table, col, _fields_info in _id_table_targets(conn, db_name):
                table_sql = db_backend.quote_ident(table)
                col_sql = db_backend.quote_ident(col)
                cur.execute(
                    f"SELECT DISTINCT CAST({col_sql} AS TEXT) FROM {table_sql} WHERE {col_sql} IS NOT NULL"
                )
                for row in cur.fetchall():
                    if row and row[0] is not None:
                        value = str(row[0]).strip()
                        if value:
                            all_ids.add(value)
        finally:
            conn.close()
    return all_ids


def _update_ids_in_table(conn, table: str, col: str, id_map: dict[str, str]) -> int:
    cur = conn.cursor()
    table_sql = db_backend.quote_ident(table)
    col_sql = db_backend.quote_ident(col)
    updated_cells = 0
    for old_id, new_id in id_map.items():
        if str(old_id) == str(new_id):
            continue
        cur.execute(
            f"UPDATE {table_sql} SET {col_sql}=%s WHERE CAST({col_sql} AS TEXT)=%s",
            (str(new_id), str(old_id)),
        )
        if cur.rowcount and cur.rowcount > 0:
            updated_cells += cur.rowcount
    return updated_cells


def _update_ids(id_map: dict[str, str]) -> tuple[int, list[str]]:
    logs = []
    total_updated = 0
    for db_name, db_path in _ID_DB_PATHS.items():
        conn = db_backend.connect(db_path, check_same_thread=False)
        try:
            db_updated = 0
            for table, col, _fields_info in _id_table_targets(conn, db_name):
                db_updated += _update_ids_in_table(conn, table, col, id_map)
            conn.commit()
            total_updated += db_updated
            logs.append(f"{db_name}: ID替换完成，更新单元格={db_updated}")
        except Exception as e:
            conn.rollback()
            logs.append(f"{db_name}: ID替换失败 -> {e}")
            raise
        finally:
            conn.close()
    return total_updated, logs


def _id_exists(value: str) -> tuple[bool, list[str]]:
    exists = False
    details = []
    for db_name, db_path in _ID_DB_PATHS.items():
        conn = db_backend.connect(db_path, check_same_thread=False)
        try:
            cur = conn.cursor()
            for table, col, _fields_info in _id_table_targets(conn, db_name):
                table_sql = db_backend.quote_ident(table)
                col_sql = db_backend.quote_ident(col)
                cur.execute(
                    f"SELECT 1 FROM {table_sql} WHERE CAST({col_sql} AS TEXT)=%s LIMIT 1",
                    (value,),
                )
                if cur.fetchone():
                    exists = True
                    details.append(f"{db_name}.{table}.{col}")
        finally:
            conn.close()
    return exists, details


def migrate_user_id_to_openid():
    """将数据库中的 QQ user_id 迁移为真实 ID。"""
    try:
        type_logs = _ensure_id_target_columns_text()
        all_candidate_ids = _collect_all_candidate_ids()
        if not all_candidate_ids:
            return False, "未找到任何可迁移ID"

        from .utils import get_real_id
        id_map = {}
        fail_ids = []

        for old_id in all_candidate_ids:
            try:
                real_id = get_real_id(old_id)
                if real_id and str(real_id).strip():
                    id_map[str(old_id)] = str(real_id).strip()
                else:
                    fail_ids.append(old_id)
            except Exception:
                fail_ids.append(old_id)

        if not id_map:
            return False, "真实ID转换全部失败，未执行替换"

        total_updated, data_logs = _update_ids(id_map)

        players_dir = DATABASE / "players"
        rename_count = 0
        if players_dir.exists():
            for old_id, new_id in id_map.items():
                old_p = players_dir / str(old_id)
                new_p = players_dir / str(new_id)
                if old_p.exists() and (not new_p.exists()):
                    try:
                        old_p.rename(new_p)
                        rename_count += 1
                    except Exception:
                        pass

        msg = (
            f"QQID转换完成！\n"
            f"候选ID总数：{len(all_candidate_ids)}\n"
            f"成功映射：{len(id_map)}\n"
            f"转换失败：{len(fail_ids)}\n"
            f"更新总单元格：{total_updated}\n"
            f"players目录改名：{rename_count}\n"
            f"\n[字段类型阶段]\n" + "\n".join(type_logs) +
            f"\n\n[数据替换阶段]\n" + "\n".join(data_logs)
        )
        return True, msg
    except Exception as e:
        return False, f"迁移异常中止：{e}"


def migrate_single_user_id(old_id: str, new_id: str):
    """手动迁移单个用户ID：old_id -> new_id。"""
    try:
        old_id = str(old_id).strip()
        new_id = str(new_id).strip()

        if not old_id or not new_id:
            return False, "参数错误：ID1 和 ID2 不能为空"
        if old_id == new_id:
            return False, "ID1 与 ID2 相同，无需更新"

        type_logs = _ensure_id_target_columns_text()
        old_exists, old_exists_detail = _id_exists(old_id)
        new_exists, new_exists_detail = _id_exists(new_id)

        if not old_exists:
            return False, f"ID1（{old_id}）不存在，未执行更新"
        if new_exists:
            return False, f"ID2（{new_id}）已存在，禁止覆盖。\n命中位置：{', '.join(new_exists_detail[:10])}"

        total_updated, data_logs = _update_ids({old_id: new_id})

        players_dir = DATABASE / "players"
        rename_msg = "未处理"
        if players_dir.exists():
            old_p = players_dir / old_id
            new_p = players_dir / new_id
            if old_p.exists() and (not new_p.exists()):
                try:
                    old_p.rename(new_p)
                    rename_msg = "已重命名"
                except Exception as e:
                    rename_msg = f"重命名失败: {e}"
            elif not old_p.exists():
                rename_msg = "旧目录不存在，跳过"
            else:
                rename_msg = "新目录已存在，跳过"

        msg = (
            f"手动ID更新完成：{old_id} -> {new_id}\n"
            f"命中位置：{', '.join(old_exists_detail[:10])}\n"
            f"总更新单元格：{total_updated}\n"
            f"players目录：{rename_msg}\n"
            f"\n[字段类型阶段]\n" + "\n".join(type_logs) +
            f"\n\n[数据替换阶段]\n" + "\n".join(data_logs)
        )
        return True, msg
    except Exception as e:
        return False, f"手动ID更新异常：{e}"


def swap_two_user_ids(id1: str, id2: str):
    """交换两个用户ID。"""
    try:
        id1 = str(id1).strip()
        id2 = str(id2).strip()

        if not id1 or not id2:
            return False, "参数错误：ID1 和 ID2 不能为空"
        if id1 == id2:
            return False, "ID1 与 ID2 相同，无法交换"

        type_logs = _ensure_id_target_columns_text()
        id1_exists, _id1_detail = _id_exists(id1)
        id2_exists, _id2_detail = _id_exists(id2)

        if not id1_exists or not id2_exists:
            return False, f"交换失败：ID1存在={id1_exists}，ID2存在={id2_exists}。要求两者都存在。"

        temp_id = f"{id1}__swap__{int(time.time())}"
        total_updated = 0
        data_logs = []

        for db_name, db_path in _ID_DB_PATHS.items():
            conn = db_backend.connect(db_path, check_same_thread=False)
            try:
                db_updated = 0
                targets = list(_id_table_targets(conn, db_name))
                for table, col, _fields_info in targets:
                    db_updated += _update_ids_in_table(conn, table, col, {id1: temp_id})
                for table, col, _fields_info in targets:
                    db_updated += _update_ids_in_table(conn, table, col, {id2: id1})
                for table, col, _fields_info in targets:
                    db_updated += _update_ids_in_table(conn, table, col, {temp_id: id2})
                conn.commit()
                total_updated += db_updated
                data_logs.append(f"{db_name}: 更新单元格={db_updated}")
            except Exception as e:
                conn.rollback()
                data_logs.append(f"{db_name}: 交换失败 -> {e}")
                return False, f"ID交换失败并已回滚（{db_name}）：{e}"
            finally:
                conn.close()

        players_dir = DATABASE / "players"
        rename_msg = "未处理"
        if players_dir.exists():
            p1 = players_dir / id1
            p2 = players_dir / id2
            pb = players_dir / temp_id
            try:
                if p1.exists():
                    p1.rename(pb)
                if p2.exists():
                    p2.rename(p1)
                if pb.exists():
                    pb.rename(p2)
                rename_msg = "已完成交换"
            except Exception as e:
                rename_msg = f"目录交换失败: {e}"

        msg = (
            f"ID交换完成：{id1} - {id2}\n"
            f"总更新单元格：{total_updated}\n"
            f"players目录：{rename_msg}\n"
            f"\n[字段类型阶段]\n" + "\n".join(type_logs) +
            f"\n\n[数据替换阶段]\n" + "\n".join(data_logs)
        )
        return True, msg
    except Exception as e:
        return False, f"ID交换异常：{e}"

driver = get_driver()
sql_message = XiuxianDateManage()  # sql类
items = Items()
trade_manager = TradeDataManager()
player_data_manager = PlayerDataManager()

@driver.on_shutdown
async def close_db():
    # 统一调用单例关闭连接
    XiuxianDateManage().close()
    XIUXIAN_IMPART_BUFF().close()
    TradeDataManager().close()
    PlayerDataManager().close()
