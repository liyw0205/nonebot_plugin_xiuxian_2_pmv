try:
    import ujson as json
except ImportError:
    import json
import copy
import threading
import time

from nonebot.log import logger
from ...paths import get_paths

from . import db_backend


DATABASE = get_paths().data
player_num = "123451234"


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
            if not DATABASE.exists():
                DATABASE.mkdir(parents=True)
            self.database_path = DATABASE / "player.db"

            # 持久连接
            self.conn = db_backend.connect(self.database_path, check_same_thread=False)
            self._conn_lock = threading.RLock()
            self.lock = self._conn_lock
            self._ensured_tables = set()
            self._ensured_fields = set()
            # 排行榜/同节点类全表读缓存：(key) -> (expire_mono, value)
            self._field_list_cache: dict = {}
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
            self._invalidate_field_list_cache(table_name, field)

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

    def get_all_field_data(self, table_name, field, *, cache_ttl: float = 45.0):
        """全表字段列表（排行榜等）。默认 45s 缓存；写路径会失效。"""
        self._ensure_table_exists(table_name)
        self._ensure_field_exists(table_name, field)
        cache_key = ("get_all_field_data", str(table_name), str(field))
        ttl = max(0.0, float(cache_ttl or 0))
        if ttl > 0:
            with self._conn_lock:
                hit = self._field_list_cache.get(cache_key)
                if hit and hit[0] > time.monotonic():
                    return copy.deepcopy(hit[1])

        with self._conn_lock:
            cursor = self._get_cursor()
            cursor.execute(
                f"SELECT user_id, {self._quote_ident(field)} FROM {self._quote_ident(table_name)}"
            )
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
            if ttl > 0:
                self._field_list_cache[cache_key] = (
                    time.monotonic() + ttl,
                    copy.deepcopy(processed_results),
                )
            return processed_results

    def _invalidate_field_list_cache(self, table_name=None, field=None) -> None:
        with self._conn_lock:
            if table_name is None:
                self._field_list_cache.clear()
                return
            t = str(table_name)
            for key in list(self._field_list_cache.keys()):
                if not isinstance(key, tuple) or len(key) < 2:
                    continue
                kind = key[0]
                if kind == "get_all_field_data" and key[1] == t:
                    if field is None or (len(key) > 2 and key[2] == str(field)):
                        self._field_list_cache.pop(key, None)
                elif kind == "list_users_by_fields" and key[1] == t:
                    # 同表位置类查询整体失效
                    self._field_list_cache.pop(key, None)

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
            self._invalidate_field_list_cache(table_name, field)

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
            self._invalidate_field_list_cache(table_name)

    def list_users_by_fields(
        self,
        table_name: str,
        equals: dict,
        *,
        cache_ttl: float = 20.0,
        exclude_user_id: str | None = None,
    ) -> list[str]:
        """按表字段等值筛选 user_id（地图同节点/洞府候选），带短缓存。"""
        if not equals:
            return []
        self._ensure_table_exists(table_name)
        for field in equals:
            self._ensure_field_exists(table_name, field)
        # 规范 key 顺序，保证缓存命中
        items = sorted((str(k), equals[k]) for k in equals)
        cache_key = (
            "list_users_by_fields",
            str(table_name),
            tuple((k, str(v) if v is not None else "") for k, v in items),
            str(exclude_user_id or ""),
        )
        ttl = max(0.0, float(cache_ttl or 0))
        if ttl > 0:
            with self._conn_lock:
                hit = self._field_list_cache.get(cache_key)
                if hit and hit[0] > time.monotonic():
                    return list(hit[1])

        clauses = []
        params = []
        for k, v in items:
            clauses.append(f"{self._quote_ident(k)}=%s")
            # JSON/数字字段在库中多为 TEXT
            if isinstance(v, (dict, list)):
                params.append(json.dumps(v, ensure_ascii=False))
            else:
                params.append(str(v) if v is not None else "")
        if exclude_user_id:
            clauses.append("user_id<>%s")
            params.append(str(exclude_user_id))
        sql = (
            f"SELECT user_id FROM {self._quote_ident(table_name)} "
            f"WHERE {' AND '.join(clauses)}"
        )
        with self._conn_lock:
            cursor = self._get_cursor()
            try:
                cursor.execute(sql, tuple(params))
                rows = cursor.fetchall() or []
            except Exception as e:
                logger.warning(f"list_users_by_fields 失败 table={table_name}: {e}")
                return []
            out = [str(r[0]) for r in rows if r and r[0] is not None]
            if ttl > 0:
                self._field_list_cache[cache_key] = (time.monotonic() + ttl, list(out))
            return out

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

    def reconnect(self):
        """恢复 player.db 后重建当前单例持有的连接。"""
        with self._conn_lock:
            if getattr(self, "conn", None):
                try:
                    self.conn.close()
                except Exception:
                    pass
                self.conn = None
            self.conn = db_backend.connect(self.database_path, check_same_thread=False)
            self._ensured_tables.clear()
            self._ensured_fields.clear()
            logger.opt(colors=True).info(f"<green>player数据库已重连！</green>")
