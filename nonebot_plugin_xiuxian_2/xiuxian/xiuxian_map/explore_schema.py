from __future__ import annotations


def _blank_snapshot(value) -> str:
    """Normalize legacy snapshot fields; 'None'/'null' are empty."""
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        import json

        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "undefined"}:
        return ""
    return text


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
        # 仅迁移真实 JSON；忽略遗留字面量 None/null
        conn.execute(
            'UPDATE player_data.map_explore_status '
            'SET "settlement"=CAST("reward_plan" AS TEXT) '
            'WHERE (COALESCE(CAST("settlement" AS TEXT),\'\')=\'\' '
            'OR LOWER(TRIM(CAST("settlement" AS TEXT))) IN (\'none\',\'null\')) '
            'AND COALESCE(CAST("reward_plan" AS TEXT),\'\')<>\'\' '
            'AND LOWER(TRIM(CAST("reward_plan" AS TEXT))) NOT IN (\'none\',\'null\') '
            'AND (CAST("reward_plan" AS TEXT) LIKE \'{%\' OR CAST("reward_plan" AS TEXT) LIKE \'[%\')'
        )
        conn.execute(
            'UPDATE player_data.map_explore_status SET "reward_plan"=\'\' '
            'WHERE "reward_plan" IS NULL OR CAST("reward_plan" AS TEXT)<>\'\''
        )
    conn.execute(
        'UPDATE player_data.map_explore_status SET "settlement"=\'\' '
        'WHERE "settlement" IS NULL '
        'OR LOWER(TRIM(CAST("settlement" AS TEXT))) IN (\'none\',\'null\')'
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


__all__ = ["ensure_explore_status_schema", "snapshot_value_matches", "_blank_snapshot"]
