from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ...xiuxian.xiuxian_utils.numeric_bind import parse_web_number
from .repository import DatabaseConsoleRepository
from .schemas import (
    BATCH_ADD_OPERATION,
    BATCH_SET_OPERATION,
    BATCH_SUBTRACT_OPERATION,
    DEFAULT_PRIMARY_KEY,
    DYNAMIC_TABLE_PRIMARY_KEY,
    IMPART_CARDS_PRIMARY_KEYS,
    IMPART_CARDS_TABLE,
    LIKE_SEARCH_OPERATOR,
    RANGE_SEARCH_OPERATORS,
)


class DatabaseConsoleApplication:
    """Use cases for database listing, row editing and guarded batch edits."""

    def __init__(self, repository: DatabaseConsoleRepository) -> None:
        self.repository = repository

    def list_tables(self) -> Mapping[str, Any]:
        return self.repository.all_tables()

    def resolve_table(self, table_name: str) -> tuple[Any, dict[str, Any] | None]:
        return self.repository.resolve_table(table_name)

    def table_data(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.repository.table_data(*args, **kwargs)

    def row_key(self, table_name: str, table_info: Mapping[str, Any], row_id: str) -> tuple[dict[str, str], list[str], bool]:
        is_dynamic_table = bool(table_info.get("is_dynamic", False))
        primary_key = table_info.get(
            "primary_key", DYNAMIC_TABLE_PRIMARY_KEY if is_dynamic_table else DEFAULT_PRIMARY_KEY
        )
        is_composite_key = isinstance(primary_key, list)
        primary_fields = primary_key if is_composite_key else [primary_key]
        if table_name == IMPART_CARDS_TABLE:
            key_parts = row_id.split("_")
            if len(key_parts) < 2:
                raise ValueError("无效的主键格式")
            user_field, card_field = IMPART_CARDS_PRIMARY_KEYS
            conditions = {user_field: key_parts[0], card_field: "_".join(key_parts[1:])}
        elif is_composite_key:
            key_parts = row_id.split("_")
            if len(key_parts) != len(primary_fields):
                raise ValueError("无效的主键格式")
            conditions = dict(zip(primary_fields, key_parts))
        else:
            conditions = {primary_fields[0]: row_id}
        return conditions, list(primary_fields), is_dynamic_table

    def read_row(self, db_path: Any, table_name: str, conditions: Mapping[str, Any]) -> Any:
        where = " AND ".join(f'{self.repository.quote_identifier(key)} = %s' for key in conditions)
        result = self.repository.execute(
            db_path,
            f"SELECT * FROM {self.repository.quote_identifier(table_name)} WHERE {where}",
            list(conditions.values()),
        )
        if not result or (isinstance(result, list) and not result) or (isinstance(result, dict) and not result):
            return None
        if isinstance(result, dict):
            return result
        return result[0] if isinstance(result, list) and result else None

    def update_row(
        self,
        db_path: Any,
        table_name: str,
        fields: list[str],
        conditions: Mapping[str, Any],
        form: Mapping[str, Any],
    ) -> dict[str, Any]:
        update_data: dict[str, Any] = {}
        for field in fields:
            if field in form and field not in conditions:
                value = form[field]
                update_data[field] = None if value == "" else parse_web_number(value)
        if update_data:
            set_clause = ", ".join(f"{self.repository.quote_identifier(field)} = %s" for field in update_data)
            where = " AND ".join(f"{self.repository.quote_identifier(key)} = %s" for key in conditions)
            result = self.repository.execute(
                db_path,
                f"UPDATE {self.repository.quote_identifier(table_name)} SET {set_clause} WHERE {where}",
                list(update_data.values()) + list(conditions.values()),
            )
            if isinstance(result, dict) and "error" in result:
                return {"success": False, "error": result["error"]}
        return {"success": True, "message": "更新成功"}

    def delete_row(self, db_path: Any, table_name: str, conditions: Mapping[str, Any]) -> dict[str, Any]:
        where = " AND ".join(f"{self.repository.quote_identifier(key)} = %s" for key in conditions)
        result = self.repository.execute(
            db_path,
            f"DELETE FROM {self.repository.quote_identifier(table_name)} WHERE {where}",
            list(conditions.values()),
        )
        if isinstance(result, dict) and "error" in result:
            return {"success": False, "error": result["error"]}
        return {"success": True, "message": "删除成功"}

    def batch_edit(self, table_name: str, form: Mapping[str, Any]) -> dict[str, Any]:
        db_path, table_info = self.resolve_table(table_name)
        if not db_path:
            return {"success": False, "error": f"表不存在：{table_name}"}
        search_field = form.get("search_field")
        search_value = form.get("search_value")
        search_condition = form.get("search_condition", LIKE_SEARCH_OPERATOR)
        batch_field = form.get("batch_field")
        operation = form.get("operation")
        value = form.get("value")
        apply_to_all = form.get("apply_to_all") == "on"
        if not all([batch_field, operation, value]):
            return {"success": False, "error": "参数不完整"}
        fields = table_info.get("fields", [])
        if batch_field not in fields:
            return {"success": False, "error": "批量修改字段不存在"}
        if search_field and search_field not in fields:
            return {"success": False, "error": "搜索字段不存在"}
        if not apply_to_all and not (search_value and search_value.strip()):
            return {"success": False, "error": "请填写搜索内容，或勾选应用到整张表"}
        if not search_field and not batch_field:
            return {"success": False, "error": "全字段搜索时请选择要修改的字段"}
        try:
            if not self.repository.table_exists(db_path, table_name):
                return {"success": False, "error": f"表不存在：{table_name}"}
        except Exception as exc:
            return {"success": False, "error": f"检查表失败：{exc}"}

        parsed_value = parse_web_number(value)
        if parsed_value is None:
            return {"success": False, "error": "修改值不能为空"}
        try:
            table_sql = self.repository.quote_identifier(table_name)
            field_sql = self.repository.quote_identifier(batch_field)
            if operation == BATCH_SET_OPERATION:
                sql = f"UPDATE {table_sql} SET {field_sql} = %s"
            elif operation == BATCH_ADD_OPERATION:
                sql = f"UPDATE {table_sql} SET {field_sql} = {field_sql} + %s"
            elif operation == BATCH_SUBTRACT_OPERATION:
                sql = f"UPDATE {table_sql} SET {field_sql} = {field_sql} - %s"
            else:
                return {"success": False, "error": "无效的操作类型"}
            params: list[Any] = [parsed_value]
            if not apply_to_all:
                if search_field and search_value:
                    if search_condition == LIKE_SEARCH_OPERATOR:
                        values = search_value.split()
                        if len(values) > 1:
                            sql += " WHERE (" + " OR ".join(
                                self.repository.like_text(search_field) for _ in values
                            ) + ")"
                            params.extend(f"%{value}%" for value in values)
                        else:
                            sql += f" WHERE {self.repository.like_text(search_field)}"
                            params.append(f"%{search_value}%")
                    elif search_condition in RANGE_SEARCH_OPERATORS:
                        values = search_value.split()
                        if len(values) == 1:
                            if not search_value.replace(".", "", 1).isdigit():
                                return {"success": False, "error": "搜索值必须是数值"}
                            sql += f" WHERE {self.repository.quote_identifier(search_field)} {search_condition} %s"
                            params.append(float(values[0]))
                        else:
                            if not values[0].replace(".", "", 1).isdigit():
                                return {"success": False, "error": "第一个搜索值必须是数值"}
                            if not values[1]:
                                return {"success": False, "error": "第二个搜索值不能为空"}
                            primary_key = table_info.get("primary_key", DYNAMIC_TABLE_PRIMARY_KEY)
                            primary_keys = set(primary_key if isinstance(primary_key, list) else [primary_key])
                            searchable = [field for field in fields if field not in primary_keys]
                            if searchable:
                                sql += (
                                    f" WHERE {self.repository.quote_identifier(search_field)} {search_condition} %s"
                                    " AND (" + " OR ".join(self.repository.like_text(field) for field in searchable) + ")"
                                )
                                params.extend([float(values[0])] + [f"%{values[1]}%" for _ in searchable])
                            else:
                                sql += f" WHERE {self.repository.quote_identifier(search_field)} {search_condition} %s"
                                params.append(float(values[0]))
                    else:
                        return {"success": False, "error": "无效的搜索条件"}
                elif search_value:
                    primary_key = table_info.get("primary_key", DYNAMIC_TABLE_PRIMARY_KEY)
                    primary_keys = set(primary_key if isinstance(primary_key, list) else [primary_key])
                    searchable = [field for field in fields if field not in primary_keys]
                    if not searchable:
                        return {"success": False, "error": "没有可搜索的字段"}
                    sql += " WHERE (" + " OR ".join(self.repository.like_text(field) for field in searchable) + ")"
                    params.extend(f"%{search_value}%" for _ in searchable)
            result = self.repository.execute(db_path, sql, params)
            if isinstance(result, dict) and "error" in result:
                return {"success": False, "error": result["error"]}
            affected_rows = result.get("affected_rows", 0) if isinstance(result, dict) else 0
            return {"success": True, "message": f"成功更新 {affected_rows} 条记录"}
        except Exception as exc:
            return {"success": False, "error": f"执行错误：{exc}"}


__all__ = ["DatabaseConsoleApplication"]
