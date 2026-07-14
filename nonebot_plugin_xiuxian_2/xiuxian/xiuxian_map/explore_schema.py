from __future__ import annotations


def ensure_explore_status_schema(conn, schema: str = "player_data") -> set[str]:
    """Upgrade the legacy exploration snapshot column inside the caller's transaction."""

    if schema != "player_data":
        raise ValueError("unsupported exploration schema")
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA player_data.table_info(map_explore_status)").fetchall()
    }
    if not columns:
        return columns
    if "settlement" not in columns:
        conn.execute(
            'ALTER TABLE player_data.map_explore_status '
            'ADD COLUMN "settlement" TEXT DEFAULT NULL'
        )
        columns.add("settlement")
    if "reward_plan" in columns:
        conn.execute(
            'UPDATE player_data.map_explore_status '
            'SET "settlement"=COALESCE(NULLIF(CAST("settlement" AS TEXT),\'\'),CAST("reward_plan" AS TEXT),\'\') '
            'WHERE COALESCE(CAST("reward_plan" AS TEXT),\'\')<>\'\''
        )
        conn.execute(
            'UPDATE player_data.map_explore_status SET "reward_plan"=\'\' '
            'WHERE "reward_plan" IS NULL OR CAST("reward_plan" AS TEXT)<>\'\''
        )
    conn.execute(
        'UPDATE player_data.map_explore_status SET "settlement"=\'\' '
        'WHERE "settlement" IS NULL'
    )
    return columns


def snapshot_value_matches(actual, expected) -> bool:
    if str(actual) == str(expected):
        return True
    try:
        import json

        return json.loads(str(actual)) == json.loads(str(expected))
    except (TypeError, ValueError):
        return False


__all__ = ["ensure_explore_status_schema", "snapshot_value_matches"]
