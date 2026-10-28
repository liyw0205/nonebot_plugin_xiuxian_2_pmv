"""Pure personal-contribution rendering for the demon invasion status."""

from __future__ import annotations

from typing import Any, Callable, Mapping, Sequence


NumberFormatter = Callable[[int], str]


def _plain_number(value: int) -> str:
    return str(int(value))


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _wave(record: Mapping[str, Any]) -> int:
    return max(_as_int(record.get("wave"), 1), 1)


def _is_claimed(
    claimed: Mapping[str, Any], user_id: str, record_key: str | None
) -> bool:
    if claimed.get(str(user_id)):
        return True
    if record_key and claimed.get(str(record_key)):
        return True
    suffix = f":{user_id}"
    return any(
        bool(value) and str(key).endswith(suffix) for key, value in claimed.items()
    )


def find_current_record(
    participants: Mapping[str, Any],
    user_id: str,
    realm: str,
    wave: int,
) -> tuple[str, dict[str, Any]]:
    """Find the player's record for the current realm/wave across legacy keys."""

    user_id, realm = str(user_id), str(realm)
    wave = max(int(wave), 1)
    keyed = participants.get(f"{realm}:{wave}:{user_id}")
    if isinstance(keyed, Mapping) and str(keyed.get("user_id", user_id)) == user_id:
        return f"{realm}:{wave}:{user_id}", dict(keyed)
    legacy = participants.get(f"{realm}:{user_id}")
    if isinstance(legacy, Mapping) and str(legacy.get("user_id", user_id)) == user_id:
        if _wave(legacy) == wave or "wave" not in legacy:
            return f"{realm}:{user_id}", dict(legacy)
    for record_key, record in participants.items():
        if not isinstance(record, Mapping):
            continue
        if str(record.get("user_id")) != user_id:
            continue
        if str(record.get("realm")) != realm:
            continue
        if _wave(record) != wave:
            continue
        return str(record_key), dict(record)
    return "", {}


def _user_records(
    participants: Mapping[str, Any], user_id: str
) -> list[dict[str, Any]]:
    return [
        dict(record)
        for record in participants.values()
        if isinstance(record, Mapping) and str(record.get("user_id")) == user_id
    ]


def latest_wave(
    participants: Mapping[str, Any], user_id: str, realm: str, default: int = 1
) -> int:
    """Return the player's latest recorded wave for a realm."""

    waves = [
        _wave(record)
        for record in participants.values()
        if isinstance(record, Mapping)
        and str(record.get("user_id")) == str(user_id)
        and str(record.get("realm")) == str(realm)
    ]
    return max(waves, default=max(int(default), 1))


def render_personal_contribution(
    participants: Mapping[str, Any],
    claimed: Mapping[str, Any],
    user_id: str,
    realm: str,
    wave: int,
    *,
    active: bool,
    number: NumberFormatter = _plain_number,
) -> tuple[str, ...]:
    """Render the player's own war record without touching the top-five list."""

    user_id = str(user_id or "").strip()
    if not user_id:
        return ()
    records = _user_records(participants, user_id)
    if not records:
        return ("【你的战功】", "本期尚未参与讨伐。")

    attacks = sum(max(_as_int(record.get("attacks")), 0) for record in records)
    total_damage = sum(max(_as_int(record.get("damage")), 0) for record in records)
    record_key, current = find_current_record(participants, user_id, realm, wave)
    lines = [
        "【你的战功】",
        f"本期出手：{attacks} 次，真实伤害 {number(total_damage)}",
    ]

    if current:
        rows = [
            dict(row)
            for row in participants.values()
            if isinstance(row, Mapping)
            and str(row.get("realm")) == str(realm)
            and _wave(row) == max(int(wave), 1)
            and max(_as_int(row.get("damage")), 0) > 0
        ]
        rows.sort(key=lambda row: (-max(_as_int(row.get("damage")), 0), str(row.get("user_id"))))
        rank = next(
            (
                index
                for index, row in enumerate(rows, 1)
                if str(row.get("user_id")) == user_id
            ),
            len(rows) or 1,
        )
        damage = max(_as_int(current.get("damage")), 0)
        wave_damage = sum(max(_as_int(row.get("damage")), 0) for row in rows)
        share = (damage / wave_damage * 100) if wave_damage > 0 else 0.0
        lines.append(
            f"本境界第{max(int(wave), 1)}波：第 {rank}/{len(rows) or 1} 名，"
            f"本波伤害 {number(damage)}（占 {share:.1f}%）"
        )
    else:
        lines.append(f"本境界第{max(int(wave), 1)}波暂无你的战功记录。")

    if _is_claimed(claimed, user_id, record_key):
        lines.append("奖励：本期已领取")
    elif current and _as_int(current.get("reward_ready")) == 1:
        lines.append("奖励：可领取，发送【领取魔修奖励】")
    elif active:
        lines.append("奖励：本期结束后按贡献结算")
    else:
        lines.append("奖励：本期未领取，发送【领取魔修奖励】")
    return tuple(lines)


__all__ = ["find_current_record", "latest_wave", "render_personal_contribution"]
