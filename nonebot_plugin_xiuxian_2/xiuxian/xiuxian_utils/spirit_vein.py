from ..xiuxian_world_events import get_spirit_vein_exp_bonus_msg, get_spirit_vein_exp_multiplier


def apply_spirit_vein_exp_bonus(exp: int, cap: int | None = None) -> tuple[int, str]:
    multiplier = get_spirit_vein_exp_multiplier()
    exp = max(0, int(exp))
    if multiplier <= 1:
        return exp, ""
    exp = int(exp * multiplier)
    if cap is not None:
        exp = min(exp, max(0, int(cap)))
    return exp, get_spirit_vein_exp_bonus_msg()
