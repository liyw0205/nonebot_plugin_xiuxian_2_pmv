from ...infrastructure.database import DatabaseUnitOfWork


def apply_pet(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS pet_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO pet_feature_migrations(version) VALUES ('pet.001')")


def apply_pet_hatch(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS pet_hatch_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(pet_hatch_operations)").fetchall()}
    if "result_json" not in columns:
        uow.execute("ALTER TABLE pet_hatch_operations ADD COLUMN result_json TEXT")


def apply_pet_skill_replace(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS pet_skill_replace_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,skill_id TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


__all__ = ["apply_pet", "apply_pet_hatch", "apply_pet_skill_replace"]
