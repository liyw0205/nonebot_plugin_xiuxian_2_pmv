"""世界BOSS 讨伐的修为奖励（gap 锚定）。

此前 `xiuxian_boss/__init__.py` 手写：

```python
now_exp = int(user_info['exp'] * exp_buff / 10000 * min(0.1 * max(user_rank // 3, 1), 1))
```

即「自身当前累计修为 × 百分比」。这与 `docs/features/xiuxian_progression.md`
规定的口径不一致——高频可重复玩法必须锚定「本境 → 下境」的 gap，否则单次奖励会随
玩家累计修为一起膨胀，后期一次首领战就能推掉大段进度（文档点名的"后期难度崩塌源"）。
世界BOSS 由体力驱动、可反复挑战，正属该形态。

这里把公式收进纯函数并改用共享助手：
`percent_exp_reward(..., divide_by_three=True, anchor="gap")`，百分比的数值来源
（`exp_buff / 10000`）与压制口径（`min(0.1 × (rank//3), 1)`）都保持不变，
只把锚点从 `current` 换成 `gap`。
"""

from __future__ import annotations

from typing import Any

from ...xiuxian.xiuxian_utils.numeric_bind import percent_exp_reward, stage_power_gap

# exp_buff 是「万分比」配置：10000 表示 100%。
EXP_BUFF_SCALE = 10000


def boss_exp_reward(
    *,
    user_exp: Any,
    exp_buff: Any,
    level: Any,
    root: Any,
    victor: Any,
) -> int:
    """返回本次世界BOSS讨伐应发的修为（非负 int）。

    三个零奖励分支与改动前完全一致：
    - `exp_buff <= 0`：该首领没配修为奖励；
    - `root == "凡人"`：凡人境界没有修为概念；
    - `victor != "群友赢了"`：没打赢。

    另加一条 fail-closed 守卫：境界名不在境界表里时 `stage_power_gap` 返回 0，而
    `percent_exp_reward` 在 gap 未知时会**静默回退到 current 锚定**——那正是
    「单次奖励随累计修为膨胀」的原始缺陷。这里宁可本次不发修为，也不回到 current。
    """
    try:
        buff = float(exp_buff or 0)
    except (TypeError, ValueError):
        return 0
    if buff <= 0:
        return 0
    if str(root or "").strip() == "凡人":
        return 0
    if str(victor or "").strip() != "群友赢了":
        return 0
    if stage_power_gap(level) <= 0:
        return 0
    return percent_exp_reward(
        user_exp,
        buff / EXP_BUFF_SCALE,
        level,
        divide_by_three=True,
        anchor="gap",
    )


__all__ = ["EXP_BUFF_SCALE", "boss_exp_reward"]
