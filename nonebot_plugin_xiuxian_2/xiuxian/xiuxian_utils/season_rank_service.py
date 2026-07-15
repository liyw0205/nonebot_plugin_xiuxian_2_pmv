from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable

try:
    from nonebot.log import logger
except Exception:  # pragma: no cover
    logger = None

from .json_store import safe_json_dumps as _json_dumps
from .json_store import safe_json_loads as _json_loads
from .periods import get_season_key
from .season_service import build_season_rank_key, normalize_season_mode
from .xiuxian2_handle import XiuxianDateManage

SeasonNow = date | datetime | None

SEASON_RANK_MODES = ("weekly", "monthly", "quarterly")
DEFAULT_SEASON_RANK_TYPES = ("交易活跃", "讨伐", "宗门贡献", "试炼", "战力")

EVENT_SEASON_RANK_TYPES: dict[str, tuple[str, ...]] = {
    "trade_buy": ("交易活跃",),
    "trade_sell": ("交易活跃",),
    "boss_attack": ("讨伐",),
    "world_event_attack": ("讨伐",),
    "sect_task_complete": ("宗门贡献",),
    "sect_donate": ("宗门贡献",),
    "dungeon_clear": ("试炼",),
}

__all__ = [
    "DEFAULT_SEASON_RANK_TYPES",
    "SEASON_RANK_MODES",
    "add_season_rank_score",
    "ensure_season_rank_table",
    "get_top_season_rank",
    "get_user_current_season_entries",
    "get_user_season_rank",
    "record_event_season_scores",
]


def _log_warning(message: str) -> None:
    if logger:
        logger.warning(message)


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _normalize_modes(raw_modes: Any = None) -> tuple[str, ...]:
    if raw_modes is None:
        return SEASON_RANK_MODES
    if isinstance(raw_modes, str):
        candidates = [part.strip() for part in raw_modes.replace("，", ",").split(",")]
    elif isinstance(raw_modes, Iterable):
        candidates = list(raw_modes)
    else:
        candidates = [raw_modes]

    modes: list[str] = []
    for mode in candidates:
        normalized = normalize_season_mode(str(mode))
        if normalized not in modes:
            modes.append(normalized)
    return tuple(modes) or SEASON_RANK_MODES


def _normalize_subject(
    *,
    user_id: str | int | None,
    sect_id: str | int | None,
) -> tuple[str, int]:
    user_id_text = "" if user_id in (None, "") else str(user_id)
    sect_id_int = 0 if sect_id in (None, "") else _to_int(sect_id, 0)
    return user_id_text, sect_id_int


def ensure_season_rank_table() -> None:
    sql_message = XiuxianDateManage()
    with sql_message.lock:
        cur = sql_message.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS season_rank (
                rank_key TEXT NOT NULL,
                mode TEXT NOT NULL,
                period_key TEXT NOT NULL,
                rank_type TEXT NOT NULL,
                user_id TEXT NOT NULL DEFAULT '',
                sect_id INTEGER NOT NULL DEFAULT 0,
                score INTEGER NOT NULL DEFAULT 0,
                extra TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (rank_key, user_id, sect_id)
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_season_rank_query
            ON season_rank(mode, period_key, rank_type, score)
            """
        )
        sql_message._commit_write()


def _build_rank_identity(
    rank_type: str,
    mode: str = "monthly",
    now: SeasonNow = None,
) -> tuple[str, str, str]:
    normalized_mode = normalize_season_mode(mode)
    period_key = get_season_key(now=now, mode=normalized_mode)
    rank_key = build_season_rank_key(rank_type, normalized_mode, now=now)
    return rank_key, normalized_mode, period_key


def add_season_rank_score(
    *,
    rank_type: str,
    score: int,
    mode: str = "monthly",
    user_id: str | int | None = None,
    sect_id: str | int | None = None,
    extra: dict[str, Any] | None = None,
    now: SeasonNow = None,
) -> dict[str, Any] | None:
    score_value = _to_int(score)
    if score_value <= 0:
        return None

    rank_type_text = str(rank_type or "").strip()
    if not rank_type_text:
        return None

    user_id_text, sect_id_int = _normalize_subject(user_id=user_id, sect_id=sect_id)
    if not user_id_text and not sect_id_int:
        return None

    ensure_season_rank_table()
    rank_key, normalized_mode, period_key = _build_rank_identity(rank_type_text, mode, now)
    extra_text = _json_dumps(extra, {})
    updated_at = _now_text()
    sql_message = XiuxianDateManage()

    with sql_message.lock:
        cur = sql_message.conn.cursor()
        cur.execute(
            """
            UPDATE season_rank
            SET score=CAST(COALESCE(score,0) AS REAL)+CAST(%s AS REAL),
                extra = %s,
                updated_at = %s
            WHERE rank_key = %s AND user_id = %s AND sect_id = %s
            """,
            (score_value, extra_text, updated_at, rank_key, user_id_text, sect_id_int),
        )
        if cur.rowcount <= 0:
            cur.execute(
                """
                INSERT INTO season_rank (
                    rank_key, mode, period_key, rank_type,
                    user_id, sect_id, score, extra, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    rank_key,
                    normalized_mode,
                    period_key,
                    rank_type_text,
                    user_id_text,
                    sect_id_int,
                    score_value,
                    extra_text,
                    updated_at,
                ),
            )
        sql_message._commit_write()

    return {
        "rank_key": rank_key,
        "mode": normalized_mode,
        "period_key": period_key,
        "rank_type": rank_type_text,
        "user_id": user_id_text,
        "sect_id": sect_id_int,
        "score": score_value,
    }


def _hydrate_rank_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    hydrated: list[dict[str, Any]] = []
    for index, row in enumerate(rows, 1):
        next_row = dict(row)
        next_row["rank"] = index
        next_row["score"] = _to_int(next_row.get("score"))
        next_row["extra_data"] = _json_loads(next_row.get("extra"), {})
        hydrated.append(next_row)
    return hydrated


def get_top_season_rank(
    rank_type: str,
    mode: str = "monthly",
    *,
    limit: int = 10,
    now: SeasonNow = None,
) -> list[dict[str, Any]]:
    ensure_season_rank_table()
    rank_type_text = str(rank_type or "").strip()
    if not rank_type_text:
        return []

    rank_key, _, _ = _build_rank_identity(rank_type_text, mode, now)
    limit_value = max(1, min(_to_int(limit, 10), 100))
    sql_message = XiuxianDateManage()
    rows = sql_message._read_query(
        """
        SELECT
            r.rank_key,
            r.mode,
            r.period_key,
            r.rank_type,
            r.user_id,
            r.sect_id,
            r.score,
            r.extra,
            r.updated_at,
            COALESCE(u.user_name, r.user_id) AS user_name,
            COALESCE(s.sect_name, CAST(r.sect_id AS TEXT)) AS sect_name
        FROM season_rank AS r
        LEFT JOIN user_xiuxian AS u ON u.user_id = r.user_id AND r.user_id <> ''
        LEFT JOIN sects AS s ON s.sect_id = r.sect_id AND r.sect_id <> 0
        WHERE r.rank_key = %s
        ORDER BY r.score DESC, r.updated_at ASC
        LIMIT %s
        """,
        (rank_key, limit_value),
        dict_row=True,
    )
    return _hydrate_rank_rows(rows)


def get_user_season_rank(
    user_id: str | int,
    rank_type: str,
    mode: str = "monthly",
    *,
    now: SeasonNow = None,
) -> dict[str, Any] | None:
    ensure_season_rank_table()
    rank_type_text = str(rank_type or "").strip()
    if not rank_type_text:
        return None

    rank_key, _, _ = _build_rank_identity(rank_type_text, mode, now)
    user_id_text = str(user_id)
    sql_message = XiuxianDateManage()
    row = sql_message._read_query(
        """
        SELECT
            r.rank_key,
            r.mode,
            r.period_key,
            r.rank_type,
            r.user_id,
            r.sect_id,
            r.score,
            r.extra,
            r.updated_at,
            COALESCE(u.user_name, r.user_id) AS user_name
        FROM season_rank AS r
        LEFT JOIN user_xiuxian AS u ON u.user_id = r.user_id AND r.user_id <> ''
        WHERE r.rank_key = %s AND r.user_id = %s
        ORDER BY r.score DESC, r.updated_at ASC
        LIMIT 1
        """,
        (rank_key, user_id_text),
        one=True,
        dict_row=True,
    )
    if not row:
        return None

    rank_value_row = sql_message._read_query(
        """
        SELECT COUNT(1) + 1 AS rank_value
        FROM season_rank
        WHERE rank_key = %s AND score > %s
        """,
        (rank_key, _to_int(row.get("score"))),
        one=True,
        dict_row=True,
    )
    row = dict(row)
    row["rank"] = _to_int((rank_value_row or {}).get("rank_value"), 1)
    row["score"] = _to_int(row.get("score"))
    row["extra_data"] = _json_loads(row.get("extra"), {})
    return row


def get_user_current_season_entries(
    user_id: str | int,
    *,
    modes: Iterable[str] | None = None,
    rank_types: Iterable[str] | None = None,
    now: SeasonNow = None,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for mode in _normalize_modes(modes):
        for rank_type in (tuple(rank_types) if rank_types else DEFAULT_SEASON_RANK_TYPES):
            row = get_user_season_rank(user_id, rank_type, mode, now=now)
            if row:
                result.append(row)
    return result


def _score_from_event(event_key: str, amount: int, meta: dict[str, Any]) -> int:
    if event_key in {"trade_buy", "trade_sell"}:
        return abs(_to_int(meta.get("stone_delta"))) or amount
    if event_key in {"sect_task_complete", "sect_donate"}:
        return abs(_to_int(meta.get("sect_contribution_delta"))) or amount
    return amount


def _iter_explicit_score_items(raw_scores: Any, amount: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if isinstance(raw_scores, dict):
        for rank_type, value in raw_scores.items():
            if isinstance(value, dict):
                item = dict(value)
                item.setdefault("rank_type", rank_type)
                item.setdefault("score", item.get("amount", amount))
            else:
                item = {"rank_type": rank_type, "score": value}
            items.append(item)
        return items

    if isinstance(raw_scores, (list, tuple, set)):
        for value in raw_scores:
            if isinstance(value, dict):
                item = dict(value)
                item.setdefault("score", item.get("amount", amount))
                items.append(item)
            elif isinstance(value, (list, tuple)) and value:
                item = {"rank_type": value[0], "score": value[1] if len(value) > 1 else amount}
                items.append(item)
            elif value:
                items.append({"rank_type": value, "score": amount})
    return items


def _build_event_score_items(
    event_key: str,
    amount: int,
    meta: dict[str, Any],
) -> list[dict[str, Any]]:
    raw_scores = meta.get("season_scores")
    explicit_items = _iter_explicit_score_items(raw_scores, amount) if raw_scores is not None else []
    if explicit_items:
        return explicit_items

    score = _score_from_event(event_key, amount, meta)
    return [
        {"rank_type": rank_type, "score": score}
        for rank_type in EVENT_SEASON_RANK_TYPES.get(event_key, ())
    ]


def record_event_season_scores(
    user_id: str | int,
    event_key: str,
    amount: int = 1,
    meta: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    meta = meta or {}
    if meta.get("skip_season_rank") is True:
        return []

    amount_value = max(0, _to_int(amount, 1))
    if amount_value <= 0:
        return []

    event_key_text = str(event_key)
    entries: list[dict[str, Any]] = []
    for item in _build_event_score_items(event_key_text, amount_value, meta):
        rank_type = str(item.get("rank_type") or item.get("type") or "").strip()
        score = _to_int(item.get("score"), amount_value)
        if not rank_type or score <= 0:
            continue

        target = str(item.get("target") or "").strip().lower()
        item_user_id: str | int | None = item.get("user_id", user_id)
        item_sect_id: str | int | None = None
        if target == "sect" or (item.get("sect_id") not in (None, "") and item.get("user_id") in (None, "")):
            item_user_id = None
            item_sect_id = item.get("sect_id", meta.get("sect_id"))

        extra = {
            "event_key": event_key_text,
            "amount": amount_value,
            "source": meta.get("source"),
            "action": meta.get("action"),
            "sect_id": meta.get("sect_id"),
        }
        item_extra = item.get("extra")
        if isinstance(item_extra, dict):
            extra.update(item_extra)

        for mode in _normalize_modes(item.get("modes", item.get("mode"))):
            try:
                entry = add_season_rank_score(
                    rank_type=rank_type,
                    score=score,
                    mode=mode,
                    user_id=item_user_id,
                    sect_id=item_sect_id,
                    extra=extra,
                )
                if entry:
                    entries.append(entry)
            except Exception as exc:
                _log_warning(
                    f"记录赛季积分失败：user_id={user_id}, event={event_key_text}, "
                    f"rank_type={rank_type}, mode={mode}, error={exc}"
                )
    return entries
