from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from .._service_port import ServicePort
from ...core.errors import OperationConflictError
from ...core.result import OperationOutcome
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from .newapi_policy import (
    MAX_ACCOUNT_BASE_URL_CHARS,
    MAX_ACCOUNT_ID_CHARS,
    MAX_ACCOUNT_LABEL_CHARS,
    MAX_ACCOUNT_LIST_ROWS,
    MAX_CHECKIN_BASE_URL_CHARS,
    MAX_CHECKIN_HISTORY_ROWS,
    MAX_CHECKIN_HISTORY_SUMMARY_CHARS,
    MAX_CHECKIN_SECRET_CHARS,
    MAX_CHECKIN_SELECTOR_CHARS,
    MAX_INFO_TARGETS,
)
from .schemas import (
    NewApiAccountListResult,
    NewApiAccountSummary,
    NewApiCheckinTarget,
    NewApiCheckinTargetsResult,
)

ACCOUNT_TABLE = "entertainment_newapi_accounts"
HISTORY_TABLE = "entertainment_newapi_checkin_history"
MAX_AUTO_CHECKIN_PAGE = 32


def _bounded_display_text(value, limit: int) -> str:
    if isinstance(value, str):
        text = value
    elif isinstance(value, int) and not isinstance(value, bool):
        text = str(value)
    else:
        return ""
    text = " ".join(text.split())
    text = " ".join("".join(char for char in text if char.isprintable()).split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)] + "…"


def _display_base_url(value) -> str:
    raw = _bounded_display_text(value, MAX_ACCOUNT_BASE_URL_CHARS)
    if not raw:
        return "—"
    candidate = raw if "://" in raw else f"https://{raw}"
    try:
        parsed = urlsplit(candidate)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return "—"
    if parsed.scheme.casefold() not in {"http", "https"} or not hostname:
        return "—"
    if not all(char.isalnum() or char in ".-:" for char in hostname):
        return "—"
    if port is not None and not 1 <= port <= 65535:
        return "—"
    if ":" in hostname:
        hostname = f"[{hostname}]"
    authority = f"{hostname}:{port}" if port is not None else hostname
    path = parsed.path[:96]
    if len(parsed.path) > len(path):
        path = path[:-1] + "…"
    safe = urlunsplit((parsed.scheme.casefold(), authority, path, "", ""))
    return _bounded_display_text(safe, MAX_ACCOUNT_BASE_URL_CHARS) or "—"


def _account_summary(row: dict) -> NewApiAccountSummary:
    api_user_id = _bounded_display_text(row.get("api_user_id"), MAX_ACCOUNT_ID_CHARS)
    if not api_user_id.isdigit():
        api_user_id = "?"
    mode_value = row.get("mode")
    return NewApiAccountSummary(
        api_user_id=api_user_id,
        mode="cookie" if isinstance(mode_value, str) and mode_value.casefold() == "cookie" else "token",
        base_url=_display_base_url(row.get("base_url")),
        label=_bounded_display_text(row.get("label"), MAX_ACCOUNT_LABEL_CHARS),
        auto_checkin=bool(row.get("auto_checkin")),
    )


def _checkin_selector_indices(text: str, account_count: int) -> tuple[list[int] | None, str | None]:
    selector = str(text or "").strip()
    if len(selector) > MAX_CHECKIN_SELECTOR_CHARS:
        return None, "序号输入过长"
    if not selector:
        return list(range(1, account_count + 1)), None

    indices: set[int] = set()
    for part in selector.replace("，", ",").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            left, right = part.split("-", 1)
            try:
                start, end = int(left.strip()), int(right.strip())
            except ValueError:
                return None, f"无法解析序号段：{part}"
            low, high = sorted((start, end))
            if high - low + 1 > MAX_ACCOUNT_LIST_ROWS:
                return None, f"一次最多选择 {MAX_ACCOUNT_LIST_ROWS} 个账号"
            indices.update(range(low, high + 1))
        else:
            try:
                indices.add(int(part))
            except ValueError:
                return None, f"无法解析序号：{part}"
        if len(indices) > MAX_ACCOUNT_LIST_ROWS:
            return None, f"一次最多选择 {MAX_ACCOUNT_LIST_ROWS} 个账号"
    if not indices:
        return None, "请指定序号，例如：newapi签到 1"
    for index in sorted(indices):
        if index < 1 or index > account_count:
            return None, f"序号 {index} 超出范围（1～{account_count}）"
    return sorted(indices), None


class EntertainmentRepository(ServicePort):
    def __init__(self, database: str | Path, *, ledger: OperationLedger | None = None) -> None:
        super().__init__("entertainment", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment")
        self.database = Path(database)
        self.ledger = ledger or OperationLedger()

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        names = {
            str(row["name"])
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }
        return {
            ACCOUNT_TABLE,
            HISTORY_TABLE,
            "operation_ledger",
            "operation_audit",
        } <= names

    def _require_schema(self, uow: DatabaseUnitOfWork) -> None:
        if not self._schema_ready(uow):
            raise RuntimeError("legacy.entertainment.003 schema_missing: NewAPI account state")

    def _read_accounts(self, uow: DatabaseUnitOfWork, user_id: str) -> list[dict]:
        self._require_schema(uow)
        rows = uow.query_all(
            f"SELECT account_id,position,api_user_id,mode,secret,base_url,label,auto_checkin,extra_json "
            f"FROM {ACCOUNT_TABLE} WHERE user_id=? ORDER BY position LIMIT ?",
            (str(user_id), MAX_ACCOUNT_LIST_ROWS + 1),
        )
        if len(rows) > MAX_ACCOUNT_LIST_ROWS:
            raise RuntimeError("NewAPI account count exceeds repository limit")
        accounts: list[dict] = []
        for source in rows:
            row = dict(source)
            try:
                extras = json.loads(str(row.pop("extra_json") or "{}"))
            except (ValueError, RecursionError) as exc:
                raise RuntimeError("NewAPI account extension data is corrupt") from exc
            if not isinstance(extras, dict):
                raise RuntimeError("NewAPI account extension data is corrupt")
            row.update(
                {
                    key: value
                    for key, value in extras.items()
                    if key not in {"account_id", "position", "extra_json"}
                }
            )
            accounts.append(row)
        return accounts

    @staticmethod
    def _target(row: dict) -> NewApiCheckinTarget | None:
        raw_id = row.get("api_user_id")
        api_user_id = str(raw_id) if isinstance(raw_id, (str, int)) and not isinstance(raw_id, bool) else ""
        secret = row.get("secret")
        base_url = row.get("base_url")
        mode = row.get("mode")
        if (
            not api_user_id
            or len(api_user_id) > MAX_ACCOUNT_ID_CHARS
            or not api_user_id.isdigit()
            or not isinstance(secret, str)
            or len(secret) > MAX_CHECKIN_SECRET_CHARS
            or not isinstance(base_url, str)
            or len(base_url) > MAX_CHECKIN_BASE_URL_CHARS
            or not isinstance(mode, str)
            or len(mode) > 16
        ):
            return None
        return NewApiCheckinTarget(
            index=int(row["position"]),
            api_user_id=api_user_id,
            mode=mode,
            secret=secret,
            base_url=base_url,
            account_id=int(row["account_id"]),
        )

    def _read_status(self, user_id: str) -> tuple[str, list[dict] | None]:
        if not self.database.is_file():
            return "missing", None
        try:
            with DatabaseUnitOfWork(self.database, read_only=True, timeout=0) as uow:
                rows = self._read_accounts(uow, user_id)
            return "ok", rows
        except (OSError, sqlite3.Error):
            return "unavailable", None
        except RuntimeError:
            return "unavailable", None

    def list_account_summaries(self, user_id: str) -> NewApiAccountListResult:
        status, rows = self._read_status(user_id)
        if status != "ok":
            return NewApiAccountListResult(status)
        return NewApiAccountListResult("ok", tuple(_account_summary(row) for row in rows or ()))

    def _resolve_account_targets(
        self,
        user_id: str,
        selector: str,
        *,
        max_targets: int = MAX_ACCOUNT_LIST_ROWS,
    ) -> NewApiCheckinTargetsResult:
        status, rows = self._read_status(user_id)
        if status != "ok":
            return NewApiCheckinTargetsResult(status)
        if not rows:
            return NewApiCheckinTargetsResult("empty")
        targets = [self._target(row) for row in rows]
        if any(target is None for target in targets):
            return NewApiCheckinTargetsResult("invalid")
        indices, message = _checkin_selector_indices(selector, len(targets))
        if message:
            return NewApiCheckinTargetsResult("invalid_selector", message=message)
        selected = indices or ()
        if len(selected) > max_targets:
            return NewApiCheckinTargetsResult(
                "too_many",
                message=f"一次最多查询 {max_targets} 个账号",
            )
        return NewApiCheckinTargetsResult("ok", tuple(targets[index - 1] for index in selected))

    def resolve_checkin_targets(self, user_id: str, selector: str) -> NewApiCheckinTargetsResult:
        return self._resolve_account_targets(user_id, selector)

    def resolve_info_targets(self, user_id: str, selector: str) -> NewApiCheckinTargetsResult:
        return self._resolve_account_targets(user_id, selector, max_targets=MAX_INFO_TARGETS)

    def list_auto_checkin_targets(
        self, after_account_id: int = 0, limit: int = MAX_AUTO_CHECKIN_PAGE
    ) -> tuple[tuple[int, str, NewApiCheckinTarget], ...]:
        after_account_id = max(0, int(after_account_id))
        limit = max(1, min(int(limit), MAX_AUTO_CHECKIN_PAGE))
        if not self.database.is_file():
            raise RuntimeError("legacy.entertainment.003 schema_missing: game database")
        with DatabaseUnitOfWork(self.database, read_only=True, timeout=0) as uow:
            self._require_schema(uow)
            rows = uow.query_all(
                f"SELECT account_id,user_id,position,api_user_id,mode,secret,base_url,label,auto_checkin,extra_json "
                f"FROM {ACCOUNT_TABLE} WHERE auto_checkin=1 AND account_id>? "
                "ORDER BY account_id LIMIT ?",
                (after_account_id, limit),
            )
        targets: list[tuple[int, str, NewApiCheckinTarget]] = []
        for source in rows:
            row = dict(source)
            target = self._target(row)
            if target is None:
                raise RuntimeError("NewAPI scheduled account fields are invalid")
            account_id = int(row["account_id"])
            targets.append((account_id, str(row["user_id"]), target))
        return tuple(targets)

    def _write_operation(self, operation_id: str, user_id: str, action: str, payload: dict, mutate):
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        ledger_action = f"entertainment.{action}"
        request = {**payload, "user_id": user_id}
        try:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                self._require_schema(uow)
                existing = self.ledger.begin(uow, operation_id, ledger_action, request)
                if existing is not None:
                    previous = existing.outcome()
                    if previous is not None:
                        return previous.replay()
                    result = {
                        "status": "rejected",
                        "message": "此前同一操作的结果无法确认，已停止自动重放，请检查账号状态。",
                    }
                    outcome = OperationOutcome.rejected(
                        operation_id,
                        ledger_action,
                        result["message"],
                        code="legacy_in_progress",
                        data=result,
                        audit_category="entertainment",
                    )
                    self.ledger.finish(uow, outcome)
                    return outcome
                result = mutate(uow)
                if result.get("status") == "applied":
                    outcome = OperationOutcome.applied(
                        operation_id,
                        ledger_action,
                        data=result,
                        audit_category="entertainment",
                    )
                else:
                    outcome = OperationOutcome.rejected(
                        operation_id,
                        ledger_action,
                        str(result.get("message") or "操作未完成"),
                        code=str(result.get("status") or "rejected"),
                        data=result,
                        audit_category="entertainment",
                    )
                self.ledger.finish(uow, outcome)
                return outcome
        except OperationConflictError:
            raise
        except Exception as exc:
            self.ledger.record_failure(self.database, operation_id, ledger_action, request, str(exc))
            raise

    def bind_account(
        self,
        operation_id: str,
        user_id: str,
        *,
        mode: str,
        api_user_id: str,
        secret: str,
        base_url: str,
        label: str = "",
    ) -> OperationOutcome[dict]:
        payload = {
            "mode": mode,
            "api_user_id": api_user_id,
            "secret": secret,
            "base_url": base_url,
            "label": label,
        }

        def mutate(uow: DatabaseUnitOfWork) -> dict:
            account_id = str(api_user_id).strip()
            credential = str(secret or "").strip()
            url = str(base_url or "").strip().rstrip("/")
            name = str(label or "").strip()
            if (
                not account_id.isdigit()
                or len(account_id) > MAX_ACCOUNT_ID_CHARS
                or int(account_id) <= 0
                or not credential
                or len(credential) > MAX_CHECKIN_SECRET_CHARS
                or mode not in {"token", "cookie"}
                or not url
                or len(url) > MAX_CHECKIN_BASE_URL_CHARS
                or len(name) > MAX_ACCOUNT_LABEL_CHARS
            ):
                return {"status": "rejected", "message": "绑定字段无效或超过限制"}
            rows = self._read_accounts(uow, user_id)
            normalized_url = url.rstrip("/")
            if any(
                str(row.get("api_user_id")) == account_id
                and str(row.get("base_url") or "").rstrip("/") == normalized_url
                for row in rows
            ):
                return {
                    "status": "rejected",
                    "message": f"已存在相同站点用户 {account_id}（{_display_base_url(url)}）",
                }
            if len(rows) >= MAX_ACCOUNT_LIST_ROWS:
                return {"status": "rejected", "message": f"每个 QQ 最多绑定 {MAX_ACCOUNT_LIST_ROWS} 个账号"}
            uow.execute(
                f"INSERT INTO {ACCOUNT_TABLE}(user_id,position,api_user_id,mode,secret,base_url,label,auto_checkin,extra_json) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (user_id, len(rows) + 1, account_id, mode, credential, url, name, 0, "{}"),
            )
            message = f"已绑定第 {len(rows) + 1} 个账号（站点用户 {account_id} · {_display_base_url(url)}）"
            return {"status": "applied", "message": message}

        return self._write_operation(operation_id, user_id, "newapi_bind", payload, mutate)

    def delete_accounts(
        self,
        operation_id: str,
        user_id: str,
        indices: list[int] | None,
        *,
        legacy_state_path: str,
    ) -> OperationOutcome[dict]:
        requested = None if indices is None else sorted({int(index) for index in indices})
        payload = {"state_path": str(legacy_state_path), "indices": requested}

        if requested is not None and (
            len(requested) > MAX_ACCOUNT_LIST_ROWS or any(index < 1 for index in requested)
        ):
            result = {"status": "rejected", "message": "序号输入无效或一次选择过多账号"}
            return OperationOutcome.rejected(
                str(operation_id),
                "entertainment.delete_accounts",
                result["message"],
                code="invalid_selector",
                data=result,
                audit_category="entertainment",
            )

        def mutate(uow: DatabaseUnitOfWork) -> dict:
            rows = self._read_accounts(uow, user_id)
            if not rows:
                return {"status": "rejected", "message": "当前没有已绑定的 NewAPI 账号"}
            if requested is None:
                removed = len(rows)
                uow.execute(f"DELETE FROM {ACCOUNT_TABLE} WHERE user_id=?", (user_id,))
                return {"status": "applied", "removed": removed, "remaining": 0}
            to_remove = {index for index in requested if 1 <= index <= len(rows)}
            survivors = [row for row in rows if int(row["position"]) not in to_remove]
            removed = len(rows) - len(survivors)
            if not removed:
                return {"status": "rejected", "message": f"序号无效，当前共 {len(rows)} 个账号"}
            uow.execute(f"DELETE FROM {ACCOUNT_TABLE} WHERE user_id=?", (user_id,))
            uow.executemany(
                f"INSERT INTO {ACCOUNT_TABLE}(account_id,user_id,position,api_user_id,mode,secret,base_url,label,auto_checkin,extra_json) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    (
                        row["account_id"],
                        user_id,
                        position,
                        str(row["api_user_id"]),
                        str(row["mode"]),
                        str(row["secret"]),
                        str(row["base_url"]),
                        str(row["label"]),
                        int(bool(row["auto_checkin"])),
                        json.dumps(
                            {key: value for key, value in row.items() if key not in {
                                "account_id", "position", "api_user_id", "mode", "secret", "base_url", "label", "auto_checkin"
                            }},
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    )
                    for position, row in enumerate(survivors, start=1)
                ),
            )
            return {"status": "applied", "removed": removed, "remaining": len(survivors)}

        return self._write_operation(operation_id, user_id, "delete_accounts", payload, mutate)

    def toggle_auto_checkin(
        self,
        operation_id: str,
        user_id: str,
        account_id: int,
        index: int,
        *,
        legacy_state_path: str,
    ) -> OperationOutcome[dict]:
        payload = {"state_path": str(legacy_state_path), "index": int(index)}

        def mutate(uow: DatabaseUnitOfWork) -> dict:
            cursor = uow.execute(
                f"UPDATE {ACCOUNT_TABLE} SET auto_checkin=CASE auto_checkin WHEN 0 THEN 1 ELSE 0 END "
                "WHERE user_id=? AND account_id=?",
                (user_id, int(account_id)),
            )
            if cursor.rowcount != 1:
                return {"status": "rejected", "message": f"序号 {index} 对应账号已变化，请重新查看绑定列表"}
            row = uow.query_one(
                f"SELECT position,api_user_id,auto_checkin FROM {ACCOUNT_TABLE} WHERE user_id=? AND account_id=?",
                (user_id, int(account_id)),
            )
            enabled = bool(row["auto_checkin"])
            state = "已开启" if enabled else "已关闭"
            return {
                "status": "applied",
                "index": int(row["position"]),
                "enabled": enabled,
                "message": f"账号 {row['position']}（站点用户 {row['api_user_id']}）自动签到{state}",
            }

        return self._write_operation(operation_id, user_id, "toggle_auto_checkin", payload, mutate)

    def list_checkin_history(self, user_id: str) -> list[dict] | None:
        if not self.database.is_file():
            return None
        try:
            with DatabaseUnitOfWork(self.database, read_only=True, timeout=0) as uow:
                self._require_schema(uow)
                rows = uow.query_all(
                    f"SELECT at,account_index AS 'index',api_user_id,base_url,summary,source "
                    f"FROM {HISTORY_TABLE} WHERE user_id=? ORDER BY position LIMIT ?",
                    (str(user_id), MAX_CHECKIN_HISTORY_ROWS),
                )
        except (OSError, sqlite3.Error, RuntimeError):
            return None
        return [dict(row) for row in rows]

    def append_checkin_history(
        self,
        user_id: str,
        *,
        account_index: int,
        api_user_id: str,
        base_url_stored: str,
        summary: str,
        source: str = "manual",
    ) -> None:
        if not self.database.is_file():
            raise RuntimeError("legacy.entertainment.003 schema_missing: game database")
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._require_schema(uow)
            previous = uow.query_all(
                f"SELECT at,account_index,api_user_id,base_url,summary,source FROM {HISTORY_TABLE} "
                "WHERE user_id=? ORDER BY position LIMIT ?",
                (str(user_id), MAX_CHECKIN_HISTORY_ROWS - 1),
            )
            safe_source = source if source in {"manual", "auto"} else "manual"
            new_row = (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                int(account_index),
                str(api_user_id)[:MAX_ACCOUNT_ID_CHARS],
                _display_base_url(base_url_stored),
                _bounded_display_text(summary or "", MAX_CHECKIN_HISTORY_SUMMARY_CHARS),
                safe_source,
            )
            uow.execute(f"DELETE FROM {HISTORY_TABLE} WHERE user_id=?", (str(user_id),))
            uow.executemany(
                f"INSERT INTO {HISTORY_TABLE}(user_id,position,at,account_index,api_user_id,base_url,summary,source) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (
                    (str(user_id), position, *row)
                    for position, row in enumerate((new_row, *(
                        (
                            str(row["at"]),
                            int(row["account_index"]),
                            str(row["api_user_id"]),
                            _display_base_url(row["base_url"]),
                            str(row["summary"])[:MAX_CHECKIN_HISTORY_SUMMARY_CHARS],
                            str(row["source"]),
                        )
                        for row in previous
                    )), start=1)
                ),
            )

    def execute(self, operation_id: str, user_id: str, action: str, payload: dict) -> dict:
        return super().execute(operation_id, user_id, action, payload)


__all__ = [
    "EntertainmentRepository",
    "MAX_ACCOUNT_BASE_URL_CHARS",
    "MAX_ACCOUNT_ID_CHARS",
    "MAX_ACCOUNT_LABEL_CHARS",
    "MAX_ACCOUNT_LIST_ROWS",
    "MAX_CHECKIN_BASE_URL_CHARS",
    "MAX_CHECKIN_HISTORY_ROWS",
    "MAX_CHECKIN_SECRET_CHARS",
    "MAX_CHECKIN_SELECTOR_CHARS",
    "MAX_INFO_TARGETS",
]
