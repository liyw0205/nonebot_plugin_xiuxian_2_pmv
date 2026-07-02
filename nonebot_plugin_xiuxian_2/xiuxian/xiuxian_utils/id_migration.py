import time
from pathlib import Path

from . import db_backend
from .download_xiuxian_data import UpdateManager

DATABASE = Path() / "data" / "xiuxian"


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
