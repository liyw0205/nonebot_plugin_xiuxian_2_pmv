from ...infrastructure.database import DatabaseUnitOfWork


def apply_sect(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS sect_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO sect_feature_migrations(version) VALUES ('sect.001')")


def apply_sect_rename(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS sect_rename_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,previous_name TEXT NOT NULL,new_name TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_sect_join(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS sect_member_join_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,sect_id INTEGER NOT NULL,member_count INTEGER NOT NULL,member_limit INTEGER NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_sect_removal(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS sect_member_removal_operations(operation_id TEXT PRIMARY KEY,operation_type TEXT NOT NULL,actor_id TEXT NOT NULL,target_id TEXT NOT NULL,sect_id INTEGER,sect_name TEXT NOT NULL,actor_name TEXT NOT NULL,target_name TEXT NOT NULL,actor_position INTEGER,target_position INTEGER,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_sect_position(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS sect_position_change_operations(operation_id TEXT PRIMARY KEY,actor_id TEXT NOT NULL,target_id TEXT NOT NULL,sect_id INTEGER NOT NULL,actor_name TEXT NOT NULL,target_name TEXT NOT NULL,old_position INTEGER NOT NULL,new_position INTEGER NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_sect_donation(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS sect_donation_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,sect_id INTEGER NOT NULL,stone INTEGER NOT NULL,materials INTEGER NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_sect_shop(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS sect_shop_weekly_purchases(user_id TEXT NOT NULL,week_key TEXT NOT NULL,item_id INTEGER NOT NULL,quantity INTEGER NOT NULL,PRIMARY KEY(user_id,week_key,item_id))")
    uow.execute("CREATE TABLE IF NOT EXISTS sect_shop_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,quantity INTEGER NOT NULL,cost INTEGER NOT NULL,contribution INTEGER NOT NULL,materials INTEGER NOT NULL,purchased INTEGER NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_sect_mainbuff(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS sect_mainbuff_learn_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,sect_id INTEGER NOT NULL,buff_id INTEGER NOT NULL,materials_cost INTEGER NOT NULL,materials_left INTEGER NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


__all__ = ["apply_sect", "apply_sect_rename", "apply_sect_join", "apply_sect_removal", "apply_sect_position", "apply_sect_donation", "apply_sect_shop", "apply_sect_mainbuff"]
