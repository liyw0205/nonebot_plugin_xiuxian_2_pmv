"""Temporary compatibility shims retained until the legacy entry points age out."""
from .commands import compatibility_hits, forward_daily_fortune, record_compatibility_hit
from .release_gate import CompatibilityReleaseGate, ReleaseGateReport
from .sign_in import SignInResult, SignInService
from .stone_gift import StoneGiftResult, StoneGiftService

__all__ = [
    "SignInResult",
    "SignInService",
    "StoneGiftResult",
    "StoneGiftService",
    "compatibility_hits",
    "CompatibilityReleaseGate",
    "forward_daily_fortune",
    "record_compatibility_hit",
    "ReleaseGateReport",
]
