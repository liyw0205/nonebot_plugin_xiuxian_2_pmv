from __future__ import annotations

from ..xiuxian_buff.relation_transaction_utils import increment_stat


DAILY_FIELDS = ("pk_num", "impart_num", "exp_used", "exp_count", "exp_load", "exp_gain")


def ensure_daily_state(conn) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS player_data.impart_pk_daily ("
        "user_id TEXT PRIMARY KEY,pk_num INTEGER NOT NULL DEFAULT 7,"
        "impart_num INTEGER NOT NULL DEFAULT 10,exp_used INTEGER NOT NULL DEFAULT 0,"
        "exp_count INTEGER NOT NULL DEFAULT 0,exp_load INTEGER NOT NULL DEFAULT 0,"
        "exp_gain INTEGER NOT NULL DEFAULT 0)"
    )


def load_daily_state(conn, user_id: str, legacy_state: dict | None = None) -> dict[str, int]:
    ensure_daily_state(conn)
    row = conn.execute(
        "SELECT pk_num,impart_num,exp_used,exp_count,exp_load,exp_gain "
        "FROM player_data.impart_pk_daily WHERE user_id=%s",
        (str(user_id),),
    ).fetchone()
    if row is None:
        defaults = {"pk_num": 7, "impart_num": 10, "exp_used": 0, "exp_count": 0, "exp_load": 0, "exp_gain": 0}
        if legacy_state:
            defaults.update({key: int(legacy_state.get(key, value) or 0) for key, value in defaults.items()})
        conn.execute(
            "INSERT INTO player_data.impart_pk_daily "
            "(user_id,pk_num,impart_num,exp_used,exp_count,exp_load,exp_gain) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (str(user_id), *(defaults[key] for key in DAILY_FIELDS)),
        )
        return defaults
    return {key: int(value or 0) for key, value in zip(DAILY_FIELDS, row)}


__all__ = ["DAILY_FIELDS", "ensure_daily_state", "increment_stat", "load_daily_state"]
