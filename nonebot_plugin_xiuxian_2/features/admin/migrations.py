from __future__ import annotations

import os
import shutil

from ...infrastructure.database import DatabaseUnitOfWork


def apply_admin(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS admin_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO admin_feature_migrations(version) VALUES ('legacy.admin.001')")


def apply_admin_id_swap_receipts(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_id_swap_step_receipts("
        "operation_id TEXT PRIMARY KEY,payload_hash TEXT NOT NULL,"
        "updated_cells INTEGER NOT NULL,created_at TEXT NOT NULL)"
    )


def apply_admin_id_swap_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_id_swap_operations("
        "operation_id TEXT PRIMARY KEY,payload_hash TEXT NOT NULL,"
        "id1 TEXT NOT NULL,id2 TEXT NOT NULL,temp_id TEXT NOT NULL,"
        "directory_id1_present INTEGER NOT NULL,directory_id2_present INTEGER NOT NULL,"
        "directory_phase TEXT NOT NULL DEFAULT 'pending',"
        "status TEXT NOT NULL DEFAULT 'started',completed_databases TEXT NOT NULL DEFAULT '[]',"
        "last_error TEXT NOT NULL DEFAULT '',"
        "created_at TEXT NOT NULL,updated_at TEXT NOT NULL,"
        "CHECK(directory_phase IN ('pending','first_moved','second_moved','completed')) ,"
        "CHECK(status IN ('started','needs_reconcile','applied','rejected'))"
        ")"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS admin_id_swap_pending_operations "
        "ON admin_id_swap_operations(status,created_at) "
        "WHERE status IN ('started','needs_reconcile')"
    )


def apply_admin_id_update_step_receipts(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_id_update_step_receipts("
        "operation_id TEXT PRIMARY KEY,payload_hash TEXT NOT NULL,"
        "updated_cells INTEGER NOT NULL,created_at TEXT NOT NULL)"
    )


def apply_admin_id_update_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_id_update_operations("
        "operation_id TEXT PRIMARY KEY,payload_hash TEXT NOT NULL,"
        "old_id TEXT NOT NULL,new_id TEXT NOT NULL,target_columns_json TEXT NOT NULL,"
        "directory_old_present INTEGER NOT NULL,directory_phase TEXT NOT NULL DEFAULT 'pending',"
        "status TEXT NOT NULL DEFAULT 'started',completed_databases TEXT NOT NULL DEFAULT '[]',"
        "last_error TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,updated_at TEXT NOT NULL,"
        "CHECK(directory_phase IN ('pending','completed')),"
        "CHECK(status IN ('started','needs_reconcile','applied','rejected')))"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS admin_id_update_pending_operations "
        "ON admin_id_update_operations(status,created_at) "
        "WHERE status IN ('started','needs_reconcile')"
    )


def apply_admin_qqid_batch(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_qqid_batches("
        "batch_id TEXT PRIMARY KEY,operator_id TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'active',"
        "total INTEGER NOT NULL CHECK(total>=0),"
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "CHECK(status IN ('active','completed')))"
    )
    uow.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS admin_qqid_single_active "
        "ON admin_qqid_batches(status) WHERE status='active'"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_qqid_batch_requests("
        "request_id TEXT PRIMARY KEY,batch_id TEXT NOT NULL REFERENCES admin_qqid_batches(batch_id))"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_qqid_batch_entries("
        "batch_id TEXT NOT NULL REFERENCES admin_qqid_batches(batch_id),source_id TEXT NOT NULL,"
        "ordinal INTEGER NOT NULL,target_id TEXT,child_operation_id TEXT NOT NULL UNIQUE,"
        "attempt INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL DEFAULT 'pending',result_json TEXT NOT NULL DEFAULT '{}',"
        "PRIMARY KEY(batch_id,source_id),UNIQUE(batch_id,ordinal),"
        "CHECK(status IN ('pending','resolved','applied','unchanged','resolve_failed','conflict','failed')))"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS admin_qqid_batch_target "
        "ON admin_qqid_batch_entries(batch_id,target_id)"
    )
    _require_columns(uow, "admin_qqid_batches", {"batch_id", "operator_id", "status", "total", "created_at", "updated_at"})
    _require_columns(uow, "admin_qqid_batch_requests", {"request_id", "batch_id"})
    _require_columns(uow, "admin_qqid_batch_entries", {
        "batch_id", "source_id", "ordinal", "target_id", "child_operation_id", "attempt", "status", "result_json",
    })


def _table_exists(uow: DatabaseUnitOfWork, name: str) -> bool:
    return uow.query_one(
        "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ) is not None


def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
    return {str(row["name"]) for row in uow.query_all(f'PRAGMA table_info("{table}")')}


def _add_missing_columns(
    uow: DatabaseUnitOfWork, table: str, definitions: dict[str, str]
) -> None:
    columns = _columns(uow, table)
    for name, definition in definitions.items():
        if name not in columns:
            uow.execute(f'ALTER TABLE "{table}" ADD COLUMN "{name}" {definition}')


def _require_columns(uow: DatabaseUnitOfWork, table: str, required: set[str]) -> None:
    missing = sorted(required - _columns(uow, table))
    if missing:
        raise RuntimeError(
            f"admin reset schema incomplete for {table}: {','.join(missing)}"
        )


def _available_memory() -> int | None:
    candidates: list[int] = []
    try:
        with open("/proc/meminfo", encoding="ascii") as stream:
            for line in stream:
                if line.startswith("MemAvailable:"):
                    candidates.append(int(line.split()[1]) * 1024)
                    break
    except (OSError, ValueError, IndexError):
        pass
    try:
        candidates.append(
            int(os.sysconf("SC_AVPHYS_PAGES")) * int(os.sysconf("SC_PAGE_SIZE"))
        )
    except (AttributeError, OSError, ValueError):
        pass
    for limit_path, usage_path in (
        ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.current"),
        (
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
            "/sys/fs/cgroup/memory/memory.usage_in_bytes",
        ),
    ):
        try:
            with open(limit_path, encoding="ascii") as stream:
                limit_text = stream.read().strip()
            with open(usage_path, encoding="ascii") as stream:
                usage = int(stream.read().strip())
            limit = int(limit_text)
            if 0 < limit < 2**60:
                candidates.append(max(0, limit - usage))
        except (OSError, ValueError):
            continue
    return min(candidates) if candidates else None


def _preflight_batch_migration(uow: DatabaseUnitOfWork) -> None:
    if not _table_exists(uow, "admin_player_status_batch_reset_operations"):
        payload_bytes = target_count = max_payload_bytes = 0
    else:
        bad_request = uow.query_one(
            "SELECT operation_id FROM admin_player_status_batch_reset_operations "
            "WHERE CASE WHEN json_valid(payload) THEN "
            "COALESCE(json_type(payload,'$.request'),'')<>'object' "
            "OR COALESCE(json_type(payload,'$.request.operator_id'),'')<>'text' "
            "OR trim(COALESCE(json_extract(payload,'$.request.operator_id'),''))='' "
            "OR COALESCE(json_type(payload,'$.request.max_stamina'),'')<>'integer' "
            "OR CAST(json_extract(payload,'$.request.max_stamina') AS INTEGER)<=0 "
            "OR (json_type(payload,'$.users') IS NOT NULL "
            "AND json_type(payload,'$.users')<>'array') "
            "ELSE 1 END LIMIT 1"
        )
        if bad_request is not None:
            raise RuntimeError(
                f"legacy admin reset payload is invalid: {bad_request['operation_id']}"
            )
        bad_user = uow.query_one(
            "SELECT o.operation_id FROM admin_player_status_batch_reset_operations o "
            "JOIN json_each(CASE WHEN json_valid(o.payload) "
            "AND json_type(o.payload,'$.users')='array' "
            "THEN json_extract(o.payload,'$.users') ELSE '[]' END) j "
            "WHERE j.type NOT IN ('text','integer') "
            "OR trim(CAST(j.value AS TEXT))='' LIMIT 1"
        )
        if bad_user is not None:
            raise RuntimeError(
                f"legacy admin reset roster is invalid: {bad_user['operation_id']}"
            )
        bad_total = uow.query_one(
            "SELECT o.operation_id FROM admin_player_status_batch_reset_operations o "
            "WHERE o.total IS NULL OR (json_type(o.payload,'$.users')='array' AND "
            "CAST(o.total AS INTEGER)<>"
            "(SELECT COUNT(DISTINCT trim(CAST(j.value AS TEXT))) FROM json_each(o.payload,'$.users') j) "
            ") LIMIT 1"
        )
        if bad_total is not None:
            raise RuntimeError(
                f"legacy admin reset roster count is inconsistent: {bad_total['operation_id']}"
            )
        size = uow.query_one(
            "SELECT COALESCE(SUM(length(CAST(payload AS BLOB))),0) AS payload_bytes,"
            "COALESCE(SUM(CASE WHEN json_type(payload,'$.users')='array' THEN "
            "(SELECT COUNT(*) FROM json_each(payload,'$.users')) ELSE 0 END),0) AS target_count,"
            "COALESCE(MAX(length(CAST(payload AS BLOB))),0) AS max_payload_bytes "
            "FROM admin_player_status_batch_reset_operations"
        )
        payload_bytes = int(size["payload_bytes"])
        target_count = int(size["target_count"])
        max_payload_bytes = int(size["max_payload_bytes"])

    required = 8 * 1024 * 1024 + target_count * 256 + payload_bytes * 4
    available = shutil.disk_usage(uow.database.parent).free
    if available < required:
        raise RuntimeError(
            f"admin reset migration needs about {required} bytes free; found {available}"
        )
    available_memory = _available_memory()
    required_memory = 64 * 1024 * 1024 + max_payload_bytes * 64
    if available_memory is not None and available_memory < required_memory:
        raise RuntimeError(
            "admin reset migration needs about "
            f"{required_memory} bytes available RAM; found {available_memory}"
        )
    if available_memory is None and max_payload_bytes > 16 * 1024 * 1024:
        raise RuntimeError("admin reset migration cannot bound RAM for a legacy payload over 16 MiB")


def apply_admin_player_status_batch_reset(uow: DatabaseUnitOfWork) -> None:
    """Prepare and normalize legacy resumable reset batches in the game DB."""
    uow.execute("PRAGMA cache_size=-2048")
    uow.execute("PRAGMA temp_store=FILE")
    _preflight_batch_migration(uow)

    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_player_status_reset_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,previous_state TEXT NOT NULL,"
        "final_state TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_player_status_batch_reset_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,total INTEGER NOT NULL,"
        "status TEXT NOT NULL DEFAULT 'running',"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "operator_id TEXT NOT NULL DEFAULT '',max_stamina INTEGER NOT NULL DEFAULT 0)"
    )
    _add_missing_columns(
        uow,
        "admin_player_status_batch_reset_operations",
        {
            "operator_id": "TEXT NOT NULL DEFAULT ''",
            "max_stamina": "INTEGER NOT NULL DEFAULT 0",
        },
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_player_status_batch_reset_progress("
        "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL,"
        "reset_applied INTEGER NOT NULL,result_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(operation_id,user_id))"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_player_status_batch_reset_targets("
        "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,ordinal INTEGER NOT NULL,"
        "PRIMARY KEY(operation_id,user_id))"
    )

    _require_columns(
        uow,
        "admin_player_status_reset_operations",
        {"operation_id", "payload", "previous_state", "final_state"},
    )
    _require_columns(
        uow,
        "admin_player_status_batch_reset_operations",
        {"operation_id", "payload", "total", "status", "created_at", "updated_at", "operator_id", "max_stamina"},
    )
    _require_columns(
        uow,
        "admin_player_status_batch_reset_progress",
        {"operation_id", "user_id", "status", "reset_applied", "result_json"},
    )
    _require_columns(
        uow,
        "admin_player_status_batch_reset_targets",
        {"operation_id", "user_id", "ordinal"},
    )

    uow.execute(
        "UPDATE admin_player_status_batch_reset_operations SET "
        "operator_id=CAST(json_extract(payload,'$.request.operator_id') AS TEXT),"
        "max_stamina=CAST(json_extract(payload,'$.request.max_stamina') AS INTEGER) "
        "WHERE json_valid(payload) AND json_type(payload,'$.request')='object'"
    )
    uow.execute(
        "INSERT OR IGNORE INTO admin_player_status_batch_reset_targets"
        "(operation_id,user_id,ordinal) "
        "SELECT operation_id,user_id,MIN(ordinal) FROM ("
        "SELECT o.operation_id,trim(CAST(j.value AS TEXT)) AS user_id,"
        "CAST(j.key AS INTEGER) AS ordinal "
        "FROM admin_player_status_batch_reset_operations o "
        "JOIN json_each(CASE WHEN json_valid(o.payload) "
        "AND json_type(o.payload,'$.users')='array' "
        "THEN json_extract(o.payload,'$.users') ELSE '[]' END) j "
        "WHERE j.type IN ('text','integer') AND trim(CAST(j.value AS TEXT))<>'') "
        "GROUP BY operation_id,user_id"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS admin_player_status_batch_reset_pending_idx "
        "ON admin_player_status_batch_reset_targets(operation_id,ordinal,user_id)"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS admin_player_status_batch_reset_running_idx "
        "ON admin_player_status_batch_reset_operations(status,operator_id,max_stamina,created_at)"
    )

    inconsistent = uow.query_one(
        "SELECT o.operation_id FROM admin_player_status_batch_reset_operations o "
        "WHERE EXISTS(SELECT 1 FROM admin_player_status_batch_reset_progress p "
        "LEFT JOIN admin_player_status_batch_reset_targets t "
        "ON t.operation_id=p.operation_id AND t.user_id=p.user_id "
        "WHERE p.operation_id=o.operation_id AND t.user_id IS NULL) "
        "OR (json_type(o.payload,'$.users')='array' AND "
        "(SELECT COUNT(*) FROM admin_player_status_batch_reset_targets t "
        "WHERE t.operation_id=o.operation_id)<>CAST(o.total AS INTEGER)) LIMIT 1"
    )
    if inconsistent is not None:
        raise RuntimeError(
            f"admin reset history cannot be reconciled: {inconsistent['operation_id']}"
        )


def _blackhouse_columns_required(
    uow: DatabaseUnitOfWork, table: str, required: set[str]
) -> None:
    missing = sorted(required - _columns(uow, table))
    if missing:
        raise RuntimeError(
            f"admin blackhouse schema incomplete for {table}: {','.join(missing)}"
        )


def _blackhouse_primary_key_required(uow: DatabaseUnitOfWork, table: str, column: str) -> None:
    primary_key = {
        str(row["name"]) for row in uow.query_all(f'PRAGMA table_info("{table}")') if row["pk"]
    }
    if primary_key != {column}:
        raise RuntimeError(f"admin blackhouse schema requires primary key {table}.{column}")


def _blackhouse_legacy_users(uow: DatabaseUnitOfWork) -> dict[str, dict[str, str]]:
    import json

    legacy_file = uow.database.parent / "blackhouse.json"
    if not legacy_file.exists():
        return {}

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    with legacy_file.open("rb") as stream:
        payload = stream.read(16 * 1024 * 1024 + 1)
    if len(payload) > 16 * 1024 * 1024:
        raise RuntimeError("legacy blackhouse JSON exceeds 16 MiB")
    try:
        raw = json.loads(payload, object_pairs_hook=unique_object)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise RuntimeError("legacy blackhouse JSON is invalid") from exc
    if not isinstance(raw, dict):
        raise RuntimeError("legacy blackhouse JSON must be an object")
    users = raw.get("users", raw)
    if not isinstance(users, dict):
        raise RuntimeError("legacy blackhouse users must be an object")
    cleaned: dict[str, dict[str, str]] = {}
    for key, value in users.items():
        user_id = key.strip()
        if not user_id or "\x00" in user_id or user_id in cleaned:
            raise RuntimeError("legacy blackhouse user id is invalid or duplicated")
        # Older releases accepted scalar values as membership-only entries.
        metadata = value if isinstance(value, dict) else {}
        for field in ("name", "reason", "updated_at"):
            if isinstance(metadata.get(field), (dict, list)):
                raise RuntimeError(f"legacy blackhouse {field} must be scalar")
        cleaned[user_id] = {
            field: str(metadata.get(field) or "")
            for field in ("name", "reason", "updated_at")
        }
    return cleaned


def _blackhouse_sync_projection(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "UPDATE user_xiuxian SET is_ban=CASE WHEN EXISTS("
        "SELECT 1 FROM admin_blackhouse_users b WHERE b.user_id=user_xiuxian.user_id"
        ") THEN 1 ELSE 0 END WHERE is_ban IS NULL OR is_ban<>CASE WHEN EXISTS("
        "SELECT 1 FROM admin_blackhouse_users b WHERE b.user_id=user_xiuxian.user_id"
        ") THEN 1 ELSE 0 END"
    )
    mismatch = uow.query_one(
        "SELECT 1 AS present FROM user_xiuxian WHERE is_ban IS NULL OR is_ban<>CASE WHEN EXISTS("
        "SELECT 1 FROM admin_blackhouse_users b WHERE b.user_id=user_xiuxian.user_id"
        ") THEN 1 ELSE 0 END LIMIT 1"
    )
    if mismatch is not None:
        raise RuntimeError("admin blackhouse projection synchronization failed")


def apply_admin_blackhouse(uow: DatabaseUnitOfWork) -> None:
    """Import legacy membership once, without modifying its JSON source or receipts."""
    player_table_exists = _table_exists(uow, "user_xiuxian")
    if player_table_exists:
        _blackhouse_columns_required(uow, "user_xiuxian", {"user_id", "is_ban"})
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_blackhouse_users("
        "user_id TEXT PRIMARY KEY,name TEXT NOT NULL DEFAULT '',"
        "reason TEXT NOT NULL DEFAULT '',updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_blackhouse_status_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_blackhouse_imports("
        "import_key TEXT PRIMARY KEY,imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    for table, columns in (
        ("admin_blackhouse_users", {"user_id", "name", "reason", "updated_at"}),
        ("admin_blackhouse_status_operations", {"operation_id", "payload", "result_json", "created_at"}),
        ("admin_blackhouse_imports", {"import_key", "imported_at"}),
    ):
        _blackhouse_columns_required(uow, table, columns)
    for table, column in (
        ("admin_blackhouse_users", "user_id"),
        ("admin_blackhouse_status_operations", "operation_id"),
        ("admin_blackhouse_imports", "import_key"),
    ):
        _blackhouse_primary_key_required(uow, table, column)
    import_key = "legacy_json_and_is_ban_v1"
    imported = uow.query_one(
        "SELECT 1 AS present FROM admin_blackhouse_imports WHERE import_key=?",
        (import_key,),
    )
    if imported is None:
        users = _blackhouse_legacy_users(uow)
        uow.executemany(
            "INSERT OR IGNORE INTO admin_blackhouse_users(user_id,name,reason,updated_at) "
            "VALUES(?,?,?,?)",
            (
                (user_id, metadata["name"], metadata["reason"], metadata["updated_at"])
                for user_id, metadata in users.items()
            ),
        )
        for user_id in users:
            if uow.query_one(
                "SELECT 1 AS present FROM admin_blackhouse_users WHERE user_id=?", (user_id,),
            ) is None:
                raise RuntimeError("legacy blackhouse JSON membership import failed")
        if player_table_exists:
            name_expression = "user_name" if "user_name" in _columns(uow, "user_xiuxian") else "''"
            for row in uow.query_all(
                f"SELECT user_id,{name_expression} AS name FROM user_xiuxian WHERE COALESCE(is_ban,0)=1"
            ):
                if row["user_id"] is None:
                    raise RuntimeError("legacy blackhouse SQL user id is invalid")
                user_id = str(row["user_id"]).strip()
                if not user_id or "\x00" in user_id:
                    raise RuntimeError("legacy blackhouse SQL user id is invalid")
                uow.execute(
                    "INSERT OR IGNORE INTO admin_blackhouse_users(user_id,name,reason,updated_at) "
                    "VALUES(?,?,'legacy_is_ban',CURRENT_TIMESTAMP)",
                    (user_id, str(row["name"] or "")),
                )
                if uow.query_one(
                    "SELECT 1 AS present FROM admin_blackhouse_users WHERE user_id=?", (user_id,),
                ) is None:
                    raise RuntimeError("legacy blackhouse SQL membership import failed")
        marker = uow.execute("INSERT INTO admin_blackhouse_imports(import_key) VALUES(?)", (import_key,))
        if marker.rowcount != 1 or uow.query_one(
            "SELECT 1 AS present FROM admin_blackhouse_imports WHERE import_key=?", (import_key,),
        ) is None:
            raise RuntimeError("admin blackhouse import marker was not saved")
    if player_table_exists:
        _blackhouse_sync_projection(uow)


__all__ = ["apply_admin", "apply_admin_player_status_batch_reset", "apply_admin_blackhouse", "apply_admin_qqid_batch"]
