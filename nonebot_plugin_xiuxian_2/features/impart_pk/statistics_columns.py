"""`player_data.statistics` 的列契约兜底。

`statistics` 由 `features/base` / `training` / `auction` / `boss` 等迁移以
`CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)` 先建好，此后任何
`CREATE TABLE IF NOT EXISTS statistics(user_id ..., "<列>" ...)` 都是 **no-op**，
内联的业务列永远不会被创建。依赖这种写法去写统计列的代码，在生产 schema 上必然抛
`no such column` / `no column named`。

这里提供与 `closing_settlement_repository`、`past_life/start_repository` 以及旧
`_increment_entry_stat` 完全一致的做法：先用 `PRAGMA` 取真实列，缺列才 `ALTER TABLE
ADD COLUMN ... INTEGER DEFAULT 0`；再 `UPDATE` 累加，`rowcount==0`（玩家还没有统计行）
时 `INSERT` 补行，保证计数从 1 开始且后续可累加。

全部操作都在调用方已有的写事务内执行，不自行开事务、不提交。
"""

from __future__ import annotations

SCHEMA = "player_data"


def ensure_statistics_column(uow, field: str, *, schema: str = SCHEMA) -> None:
    """确保 `statistics` 表与其业务列存在（幂等，不 DROP、不改已有列类型）。

    `schema=None`/`""` 表示 UoW 的主库就是 player_db（未 attach）。
    """
    prefix = f"{schema}." if schema else ""
    uow.execute(f"CREATE TABLE IF NOT EXISTS {prefix}statistics(user_id TEXT PRIMARY KEY)")
    columns = {
        str(row["name"]) for row in uow.query_all(f"PRAGMA {prefix}table_info(statistics)")
    }
    if field not in columns:
        uow.execute(
            f'ALTER TABLE {prefix}statistics ADD COLUMN "{field}" INTEGER DEFAULT 0'
        )


def increment_statistics(
    uow, user_id: str, field: str, amount: int = 1, *, schema: str = SCHEMA
) -> int:
    """把 `field` 累加 `amount`，返回累加后的真实值。"""
    user_id, amount = str(user_id), int(amount)
    ensure_statistics_column(uow, field, schema=schema)
    prefix = f"{schema}." if schema else ""
    updated = uow.execute(
        f'UPDATE {prefix}statistics SET "{field}"=COALESCE("{field}",0)+? WHERE user_id=?',
        (amount, user_id),
    )
    if updated.rowcount == 0:
        uow.execute(
            f'INSERT INTO {prefix}statistics(user_id,"{field}") VALUES(?,?)',
            (user_id, amount),
        )
    row = uow.query_one(
        f'SELECT COALESCE("{field}",0) AS value FROM {prefix}statistics WHERE user_id=?',
        (user_id,),
    )
    return int(row["value"]) if row is not None else amount


__all__ = ["ensure_statistics_column", "increment_statistics"]
