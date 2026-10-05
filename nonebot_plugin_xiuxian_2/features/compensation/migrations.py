import hashlib
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


def apply_compensation_invitation_snapshot_migration(
    uow: DatabaseUnitOfWork,
    legacy_records_path: str | Path | None = None,
    legacy_claimed_path: str | Path | None = None,
    legacy_rewards_path: str | Path | None = None,
    occurred_at: str | None = None,
) -> None:
    """Import invitation JSON snapshots once before request handlers run."""
    package_root = Path(__file__).resolve().parents[2]
    legacy_dir = (
        package_root
        / "xiuxian"
        / "xiuxian_compensation"
        / "compensation_data"
        / "invitation_data"
    )
    paths = {
        "records": Path(legacy_records_path or legacy_dir / "invitation_records.json"),
        "claimed": Path(legacy_claimed_path or legacy_dir / "invitation_claimed.json"),
        "rewards": Path(legacy_rewards_path or legacy_dir / "invitation_rewards.json"),
    }
    migrated_at = str(occurred_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    required_tables = {
        "invitation_reward_invites",
        "invitation_reward_claims",
        "invitation_reward_definitions",
    }
    existing_tables = {
        str(row["name"])
        for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
    }
    if not required_tables.issubset(existing_tables):
        raise RuntimeError("invitation schemas must be migrated before snapshot import")

    uow.execute(
        "CREATE TABLE IF NOT EXISTS invitation_reward_migrations("
        "migration_key TEXT PRIMARY KEY,records_sha256 TEXT NOT NULL,"
        "claimed_sha256 TEXT NOT NULL,rewards_sha256 TEXT NOT NULL,migrated_at TEXT NOT NULL)"
    )
    migration_key = "legacy.compensation.invitation-json-v1"
    if uow.query_one(
        "SELECT 1 AS found FROM invitation_reward_migrations WHERE migration_key=?",
        (migration_key,),
    ) is not None:
        return

    def load_snapshot(path: Path) -> tuple[dict, str]:
        try:
            with path.open("rb") as snapshot:
                raw = snapshot.read(1024 * 1024 + 1)
        except FileNotFoundError:
            raw = b""
        except OSError as exc:
            raise RuntimeError(f"could not read invitation snapshot: {path}") from exc
        if len(raw) > 1024 * 1024:
            raise ValueError(f"invitation snapshot exceeds 1 MiB: {path}")
        if not raw.strip():
            value = {}
        else:
            try:
                value = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, ValueError, TypeError) as exc:
                raise ValueError(f"invalid invitation snapshot: {path}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"invitation snapshot must be an object: {path}")
        return value, hashlib.sha256(raw).hexdigest()

    records, records_hash = load_snapshot(paths["records"])
    claimed, claimed_hash = load_snapshot(paths["claimed"])
    rewards, rewards_hash = load_snapshot(paths["rewards"])

    for raw_inviter, raw_invited in records.items():
        inviter_id = str(raw_inviter).strip()
        if not inviter_id or not isinstance(raw_invited, (list, tuple, set)):
            continue
        for raw_invited_id in raw_invited:
            invited_id = str(raw_invited_id).strip()
            if invited_id and invited_id != inviter_id:
                uow.execute(
                    "INSERT INTO invitation_reward_invites(inviter_id,invited_id,source) "
                    "VALUES(?,?,?) ON CONFLICT(inviter_id,invited_id) DO NOTHING",
                    (inviter_id, invited_id, "legacy_json"),
                )

    for raw_user_id, raw_thresholds in claimed.items():
        user_id = str(raw_user_id).strip()
        if not user_id or not isinstance(raw_thresholds, (list, tuple, set)):
            continue
        for raw_threshold in raw_thresholds:
            try:
                threshold = int(raw_threshold)
            except (TypeError, ValueError):
                continue
            if threshold > 0:
                uow.execute(
                    "INSERT INTO invitation_reward_claims(user_id,threshold,source) "
                    "VALUES(?,?,?) ON CONFLICT(user_id,threshold) DO NOTHING",
                    (user_id, threshold, "legacy_json"),
                )

    for raw_threshold, raw_items in rewards.items():
        try:
            threshold = int(raw_threshold)
        except (TypeError, ValueError):
            continue
        if threshold <= 0 or not isinstance(raw_items, (list, tuple)):
            continue
        items = [dict(item) for item in raw_items if isinstance(item, dict)]
        if not items:
            continue
        uow.execute(
            "INSERT INTO invitation_reward_definitions(threshold,rewards_json) "
            "VALUES(?,?) ON CONFLICT(threshold) DO NOTHING",
            (
                threshold,
                json.dumps(items, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
            ),
        )

    uow.execute(
        "INSERT INTO invitation_reward_migrations(migration_key,records_sha256,"
        "claimed_sha256,rewards_sha256,migrated_at) VALUES(?,?,?,?,?)",
        (migration_key, records_hash, claimed_hash, rewards_hash, migrated_at),
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
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return {}
        except OSError as exc:
            raise RuntimeError(f"could not read compensation snapshot: {path}") from exc
        if not raw.strip():
            return {}
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, ValueError, TypeError) as exc:
            raise ValueError(f"invalid compensation snapshot: {path}") from exc
        if not isinstance(value, dict):
            raise ValueError(f"compensation snapshot must be an object: {path}")
        return value

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


def apply_compensation_reward_catalog_schema(
    uow: DatabaseUnitOfWork,
    legacy_gift_definitions_path: str | Path | None = None,
    legacy_gift_claims_path: str | Path | None = None,
    legacy_redeem_definitions_path: str | Path | None = None,
    legacy_redeem_claims_path: str | Path | None = None,
    occurred_at: str | None = None,
) -> None:
    """Create SQL-owned gift/redeem catalogs and import their snapshots once."""
    package_root = Path(__file__).resolve().parents[2]
    legacy_root = package_root / "xiuxian" / "xiuxian_compensation" / "compensation_data"
    paths = {
        "礼包": (
            Path(
                legacy_gift_definitions_path
                or legacy_root / "gift_package" / "gift_package_records.json"
            ),
            Path(
                legacy_gift_claims_path
                or legacy_root / "gift_package" / "claimed_gift_packages.json"
            ),
        ),
        "兑换码": (
            Path(
                legacy_redeem_definitions_path
                or legacy_root / "redeem_code" / "redeem_codes.json"
            ),
            Path(
                legacy_redeem_claims_path
                or legacy_root / "redeem_code" / "claimed_redeem_codes.json"
            ),
        ),
    }
    migrated_at = str(occurred_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    uow.execute(
        "CREATE TABLE IF NOT EXISTS compensation_reward_definition_revisions("
        "reward_type TEXT NOT NULL,record_id TEXT NOT NULL,last_version INTEGER NOT NULL,"
        "PRIMARY KEY(reward_type,record_id))"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS compensation_reward_definitions("
        "reward_type TEXT NOT NULL,record_id TEXT NOT NULL,version INTEGER NOT NULL,"
        "record_json TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,"
        "PRIMARY KEY(reward_type,record_id))"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS compensation_reward_catalog_migrations("
        "migration_key TEXT PRIMARY KEY,gift_definitions_sha256 TEXT NOT NULL,"
        "gift_claims_sha256 TEXT NOT NULL,redeem_definitions_sha256 TEXT NOT NULL,"
        "redeem_claims_sha256 TEXT NOT NULL,definitions_imported INTEGER NOT NULL,"
        "claims_imported INTEGER NOT NULL,migrated_at TEXT NOT NULL)"
    )

    required_claim_tables = {"reward_claims", "reward_claim_counters"}
    existing_claim_tables = {
        str(row["name"])
        for row in uow.query_all(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('reward_claims','reward_claim_counters')"
        )
    }
    if existing_claim_tables != required_claim_tables:
        raise RuntimeError("compensation reward-claim schema must be migrated first")

    migration_key = "legacy-gift-redeem-json-v1"
    if uow.query_one(
        "SELECT 1 AS found FROM compensation_reward_catalog_migrations "
        "WHERE migration_key=?",
        (migration_key,),
    ) is not None:
        return

    def read_snapshot(path: Path) -> tuple[dict, str]:
        try:
            digest = hashlib.sha256()
            has_content = False
            with path.open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
                    has_content = has_content or bool(chunk.strip())
        except FileNotFoundError:
            return {}, hashlib.sha256(b"").hexdigest()
        if not has_content:
            return {}, digest.hexdigest()
        try:
            with path.open("r", encoding="utf-8") as source:
                value = json.load(source)
        except (UnicodeDecodeError, ValueError, TypeError) as exc:
            raise ValueError(f"invalid compensation reward snapshot: {path}") from exc
        if not isinstance(value, dict):
            raise ValueError(f"compensation reward snapshot must be an object: {path}")
        return value, digest.hexdigest()

    snapshot_hashes: dict[str, tuple[str, str]] = {}
    redeem_used_counts: dict[str, int] = {}
    definitions_imported = 0
    claims_imported = 0

    for reward_type, (definition_path, claim_path) in paths.items():
        definitions, definitions_hash = read_snapshot(definition_path)
        for raw_record_id, raw_record in definitions.items():
            record_id = str(raw_record_id).strip()
            if not record_id or not isinstance(raw_record, dict):
                continue
            if reward_type == "兑换码":
                redeem_used_counts[record_id] = max(
                    int(raw_record.get("used_count", 0) or 0), 0
                )
            record = {
                str(key): value
                for key, value in raw_record.items()
                if str(key) != "_definition_version"
            }
            if reward_type == "兑换码":
                record.pop("used_count", None)
            payload = json.dumps(
                record, ensure_ascii=True, sort_keys=True, separators=(",", ":")
            )
            revision = uow.execute(
                "INSERT INTO compensation_reward_definition_revisions("
                "reward_type,record_id,last_version) VALUES(?,?,1) "
                "ON CONFLICT(reward_type,record_id) DO NOTHING",
                (reward_type, record_id),
            )
            cursor = uow.execute(
                "INSERT INTO compensation_reward_definitions("
                "reward_type,record_id,version,record_json,created_at,updated_at) "
                "VALUES(?,?,1,?,?,?) ON CONFLICT(reward_type,record_id) DO NOTHING",
                (reward_type, record_id, payload, migrated_at, migrated_at),
            )
            if cursor.rowcount:
                definitions_imported += 1
            if revision.rowcount == 0 and cursor.rowcount:
                uow.execute(
                    "UPDATE compensation_reward_definition_revisions "
                    "SET last_version=MAX(last_version,1) "
                    "WHERE reward_type=? AND record_id=?",
                    (reward_type, record_id),
                )

        del definitions
        claims, claims_hash = read_snapshot(claim_path)
        for raw_user_id, raw_record_ids in claims.items():
            if not isinstance(raw_record_ids, (list, tuple, set)):
                continue
            user_id = str(raw_user_id).strip()
            if not user_id:
                continue
            for raw_record_id in raw_record_ids:
                record_id = str(raw_record_id).strip()
                if not record_id:
                    continue
                cursor = uow.execute(
                    "INSERT INTO reward_claims("
                    "reward_type,record_id,user_id,created_at) VALUES(?,?,?,?) "
                    "ON CONFLICT(reward_type,record_id,user_id) DO NOTHING",
                    (reward_type, record_id, user_id, migrated_at),
                )
                claims_imported += int(bool(cursor.rowcount))
        del claims
        snapshot_hashes[reward_type] = (definitions_hash, claims_hash)

    for record_id, legacy_used_count in redeem_used_counts.items():
        claim_count = int(
            uow.execute(
                "SELECT COUNT(*) FROM reward_claims "
                "WHERE reward_type='兑换码' AND record_id=?",
                (record_id,),
            ).fetchone()[0]
        )
        baseline = max(
            legacy_used_count - claim_count,
            0,
        )
        if baseline:
            uow.execute(
                "INSERT INTO reward_claim_counters("
                "reward_type,record_id,baseline_count) VALUES('兑换码',?,?) "
                "ON CONFLICT(reward_type,record_id) DO UPDATE SET "
                "baseline_count=excluded.baseline_count",
                (record_id, baseline),
            )
        else:
            uow.execute(
                "DELETE FROM reward_claim_counters "
                "WHERE reward_type='兑换码' AND record_id=?",
                (record_id,),
            )

    uow.execute(
        "INSERT INTO compensation_reward_catalog_migrations("
        "migration_key,gift_definitions_sha256,gift_claims_sha256,"
        "redeem_definitions_sha256,redeem_claims_sha256,definitions_imported,"
        "claims_imported,migrated_at) VALUES(?,?,?,?,?,?,?,?)",
        (
            migration_key,
            snapshot_hashes["礼包"][0],
            snapshot_hashes["礼包"][1],
            snapshot_hashes["兑换码"][0],
            snapshot_hashes["兑换码"][1],
            definitions_imported,
            claims_imported,
            migrated_at,
        ),
    )


__all__ = [
    "apply_compensation",
    "apply_compensation_definition_schema",
    "apply_compensation_reward_claim_schema",
    "apply_compensation_reward_catalog_schema",
    "apply_compensation_invitation_reward_schema",
    "apply_compensation_invitation_definition_schema",
]
