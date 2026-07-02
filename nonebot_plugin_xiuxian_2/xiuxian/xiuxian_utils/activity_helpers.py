def as_bool(value, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in ("true", "1", "yes", "on", "开启"):
        return True
    if text in ("false", "0", "no", "off", "关闭"):
        return False
    return default


def default_stage_features(stage_type: str) -> list[str]:
    if stage_type == "warmup":
        return ["sign", "claim"]
    if stage_type == "settlement":
        return ["shop", "claim", "exchange"]
    if stage_type == "closed":
        return []
    return ["sign", "task", "pass", "points", "collect", "boss", "shop", "claim", "exchange"]
