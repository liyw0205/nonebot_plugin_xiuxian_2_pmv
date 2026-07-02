def _as_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clean_text(value, default: str = "") -> str:
    text = str(value if value is not None else "").strip()
    return text or default


def _normalize_activity_key(value, fallback: str) -> str:
    text = _clean_text(value)
    if not text:
        text = fallback
    cleaned = "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in text)
    return cleaned.strip("_") or fallback


def _drop_rate(value, default: float = 0.35) -> float:
    rate = _as_float(value, default)
    if rate > 1:
        rate = rate / 100
    return min(max(rate, 0.0), 1.0)
