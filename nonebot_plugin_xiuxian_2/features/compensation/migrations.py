import json
from datetime import datetime
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


def apply_compensation(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS compensation_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO compensation_feature_migrations(version) VALUES ('legacy.compensation.001')")


def apply_compensation_reward_claim_schema(uow: DatabaseUnitOfWork) -> None:
    """Prepare the claim ledger before any reward request can run."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS reward_claims("
        "reward_type TEXT NOT NULL,record_id TEXT NOT NULL,user_id TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(reward_type,record_id,user_id))"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS reward_claim_counters("
        "reward_type TEXT NOT NULL,record_id TEXT NOT NULL,"
        "baseline_count INTEGER NOT NULL DEFAULT 0,"
        "PRIMARY KEY(reward_type,record_id))"
    )


def apply_compensation_invitation_reward_schema(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS invitation_reward_invites("
        "inviter_id TEXT NOT NULL,invited_id TEXT NOT NULL,source TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(inviter_id,invited_id))"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS invitation_reward_claims("
        "user_id TEXT NOT NULL,threshold INTEGER NOT NULL,source TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(user_id,threshold))"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS invitation_reward_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
        "thresholds_json TEXT NOT NULL,invitation_count INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_compensation_invitation_definition_schema(uow: DatabaseUnitOfWork) -> None:
    """Prepare the invitation reward catalog before admin writes can run."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS invitation_reward_definitions("
        "threshold INTEGER PRIMARY KEY,rewards_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_compensation_definition_schema(
    uow: DatabaseUnitOfWork,
    legacy_definitions_path: str | Path | None = None,
    legacy_claims_path: str | Path | None = None,
    occurred_at: str | None = None,
) -> None:
    """Prepare definition tables and import the legacy JSON snapshot once."""
    package_root = Path(__file__).resolve().parents[2]
    legacy_dir = (
        package_root
        / "xiuxian"
        / "xiuxian_compensation"
        / "compensation_data"
        / "compensation"
    )
    definitions_path = Path(legacy_definitions_path or legacy_dir / "compensation_records.json")
    claims_path = Path(legacy_claims_path or legacy_dir / "claimed_records.json")
    migrated_at = str(occurred_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    uow.execute(
        "CREATE TABLE IF NOT EXISTS compensation_definition_revisions("
        "record_id TEXT PRIMARY KEY,last_version INTEGER NOT NULL)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS compensation_definitions("
        "record_id TEXT PRIMARY KEY,version INTEGER NOT NULL,record_json TEXT NOT NULL,"
        "created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS compensation_definition_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,action TEXT NOT NULL,"
        "record_id TEXT NOT NULL DEFAULT '',version INTEGER NOT NULL DEFAULT 0,"
        "outcome TEXT NOT NULL,removed_definitions INTEGER NOT NULL DEFAULT 0,"
        "removed_claims INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL,"
        "result_json TEXT NOT NULL DEFAULT '{}')"
    )
    operation_columns = {
        str(row["name"])
        for row in uow.execute("PRAGMA table_info(compensation_definition_operations)")
    }
    if "result_json" not in operation_columns:
        uow.execute(
            "ALTER TABLE compensation_definition_operations "
            "ADD COLUMN result_json TEXT NOT NULL DEFAULT '{}'"
        )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS compensation_legacy_migrations("
        "migration_key TEXT PRIMARY KEY,definitions_payload TEXT NOT NULL,"
        "claims_payload TEXT NOT NULL,migrated_at TEXT NOT NULL)"
    )

    required_claim_tables = {"reward_claims", "reward_claim_counters"}
    existing_tables = {
        str(row["name"])
        for row in uow.query_all(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('reward_claims','reward_claim_counters')"
        )
    }
    if existing_tables != required_claim_tables:
        raise RuntimeError("compensation reward-claim schema must be migrated first")

    migration_key = "legacy-compensation-json-v1"
    if uow.query_one(
        "SELECT 1 AS found FROM compensation_legacy_migrations WHERE migration_key=?",
        (migration_key,),
    ) is not None:
        return

    def load_dict(path: Path) -> dict:
        if not path.is_file():
            return {}
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return {}
        return value if isinstance(value, dict) else {}

    definitions = load_dict(definitions_path)
    claims = load_dict(claims_path)
    for record_id, record in definitions.items():
        record_id = str(record_id).strip()
        if not record_id or not isinstance(record, dict):
            continue
        normalized = {
            str(key): value
            for key, value in record.items()
            if str(key) != "_definition_version"
        }
        payload = json.dumps(
            normalized, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        uow.execute(
            "INSERT INTO compensation_definition_revisions(record_id,last_version) "
            "VALUES(?,1) ON CONFLICT(record_id) DO NOTHING",
            (record_id,),
        )
        uow.execute(
            "INSERT INTO compensation_definitions("
            "record_id,version,record_json,created_at,updated_at) VALUES(?,1,?,?,?) "
            "ON CONFLICT(record_id) DO NOTHING",
            (record_id, payload, migrated_at, migrated_at),
        )

    for user_id, record_ids in claims.items():
        if not isinstance(record_ids, (list, tuple, set)):
            continue
        user_id = str(user_id).strip()
        if not user_id:
            continue
        for record_id in dict.fromkeys(str(value).strip() for value in record_ids):
            if record_id:
                uow.execute(
                    "INSERT INTO reward_claims(reward_type,record_id,user_id,created_at) "
                    "VALUES('补偿',?,?,?) "
                    "ON CONFLICT(reward_type,record_id,user_id) DO NOTHING",
                    (record_id, user_id, migrated_at),
                )

    uow.execute(
        "INSERT INTO compensation_legacy_migrations("
        "migration_key,definitions_payload,claims_payload,migrated_at) VALUES(?,?,?,?)",
        (
            migration_key,
            json.dumps(definitions, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
            json.dumps(claims, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
            migrated_at,
        ),
    )


__all__ = [
    "apply_compensation",
    "apply_compensation_definition_schema",
    "apply_compensation_reward_claim_schema",
    "apply_compensation_invitation_reward_schema",
    "apply_compensation_invitation_definition_schema",
]
