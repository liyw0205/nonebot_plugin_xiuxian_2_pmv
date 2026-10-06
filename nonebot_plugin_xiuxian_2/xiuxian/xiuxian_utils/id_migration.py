from nonebot.log import logger

from ...paths import get_paths

from . import db_backend

DATABASE = get_paths().data
_qqid_application = None


def configure_qqid_application(application) -> None:
    global _qqid_application
    _qqid_application = application


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


def migrate_user_id_to_openid(*, application=None, operation_id=None, operator_id="compatibility"):
    """兼容旧入口；批次及单ID写入由已配置的 feature 应用管理。"""
    from ...features.admin.qqid_application import AdminQqidApplication
    from ...infrastructure.ids import UUIDGenerator

    owner = application if application is not None else _qqid_application
    if owner is None:
        return False, "QQID转换服务尚未就绪，请检查启动配置。"
    try:
        result = owner.run(operation_id or f"admin-qqid-conversion:{UUIDGenerator().new_id()}", operator_id)
        return AdminQqidApplication.format_result(result)
    except Exception as exc:
        logger.warning("QQID compatibility conversion failed: {}", type(exc).__name__)
        return False, "QQID转换未确认，请检查服务日志；已开始的批次会在下次请求恢复。"


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
