"""师徒解除清算的纯规则。

`师徒传功` 是包内唯一的“修为转移”动作：师父把自己的修为经
`MentorTransmissionService.apply` 写进徒弟 `user_xiuxian.exp`，并在
`mentor_transmission_operations` 留下权威账目。解除关系（`逐出师门` / 徒弟自行
`解除师徒`）此前只清关系字段与写冷却，从不回收这些修为，于是
`拜师 → 传功 → 解除 → 再拜入同一师父` 成为纯收益循环。

本模块只承载“应收多少、能不能结清”的纯算术与判定，无 I/O、无 NoneBot：
按该配对**权威账目的 120% 向上取整**清算，余额不足则不允许解除，
因此每一步先付出 120%，关系循环不再产生净修为。

只读规则：不写库、不改奖励数值、不改出师路径。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

# 清算比例：权威传功修为的 120%（向上取整）。
DISSOLUTION_CHARGE_NUMERATOR = 6
DISSOLUTION_CHARGE_DENOMINATOR = 5

STATUS_READY = "ready"
STATUS_NO_CHARGE = "no_charge"
STATUS_INSUFFICIENT = "insufficient_cultivation"


def _as_int(value: Any) -> int:
    """Map any value to a non-negative int; unusable values become 0."""
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    return number if number > 0 else 0


def dissolution_charge(gained_exp: Any) -> int:
    """120% of ``gained_exp``, rounded **up** to a whole cultivation point.

    向上取整保证玩家无法通过“先花掉零头再解除”少付；`gained_exp <= 0`
    或坏值时为 0（历史关系没有权威账目就不收费）。
    """
    gained = _as_int(gained_exp)
    if gained <= 0:
        return 0
    return -(-gained * DISSOLUTION_CHARGE_NUMERATOR // DISSOLUTION_CHARGE_DENOMINATOR)


@dataclass(frozen=True)
class DissolutionDecision:
    """One canonical answer to "what must be paid to dissolve this pair?"."""

    gained: int = 0
    charge: int = 0
    apprentice_exp: int = 0
    status: str = STATUS_NO_CHARGE

    @property
    def shortfall(self) -> int:
        return max(0, self.charge - self.apprentice_exp)

    @property
    def payable(self) -> bool:
        return self.status == STATUS_READY

    @property
    def charged(self) -> bool:
        """True when dissolving under this decision actually deducts cultivation."""
        return self.charge > 0


def dissolution_decision(*, gained_exp: Any, apprentice_exp: Any) -> DissolutionDecision:
    """Decide whether the apprentice can afford the 120% dissolution charge.

    `insufficient_cultivation` is a refusal, never a clamp: the caller must keep
    the relation and change no asset, so cultivation can neither go negative nor
    be minted to cover the gap.
    """
    gained = _as_int(gained_exp)
    charge = dissolution_charge(gained)
    balance = _as_int(apprentice_exp)
    if charge <= 0:
        return DissolutionDecision(gained=gained, charge=0, apprentice_exp=balance, status=STATUS_NO_CHARGE)
    if balance < charge:
        return DissolutionDecision(
            gained=gained, charge=charge, apprentice_exp=balance, status=STATUS_INSUFFICIENT
        )
    return DissolutionDecision(gained=gained, charge=charge, apprentice_exp=balance, status=STATUS_READY)


def render_dissolution_line(
    decision: Any,
    *,
    format_number: Callable[[int], str] | None = None,
    refusal_tail: str = "修为不足时无法解除师徒关系。",
) -> str:
    """One actionable line for the dissolution replies (empty for no charge).

    ``decision`` only needs ``status``/``charge``/``apprentice_exp``/``shortfall``,
    so both the pure ``DissolutionDecision`` and the repository result render here.
    """
    status = str(getattr(decision, "status", ""))
    if status == STATUS_NO_CHARGE:
        return ""
    fmt = format_number or (lambda value: str(int(value)))
    charge = _as_int(getattr(decision, "charge", 0))
    balance = _as_int(getattr(decision, "apprentice_exp", 0))
    shortfall = _as_int(getattr(decision, "shortfall", 0)) or max(0, charge - balance)
    if status == STATUS_INSUFFICIENT:
        return (
            f"\n需清退通过本段师徒获得的修为 {fmt(charge)}（120%），"
            f"当前修为 {fmt(balance)}，还差 {fmt(shortfall)}；{refusal_tail}"
        )
    return f"\n已清退通过本段师徒获得的修为 {fmt(charge)}（120%）。"


__all__ = [
    "DISSOLUTION_CHARGE_DENOMINATOR",
    "DISSOLUTION_CHARGE_NUMERATOR",
    "STATUS_INSUFFICIENT",
    "STATUS_NO_CHARGE",
    "STATUS_READY",
    "DissolutionDecision",
    "dissolution_charge",
    "dissolution_decision",
    "render_dissolution_line",
]
