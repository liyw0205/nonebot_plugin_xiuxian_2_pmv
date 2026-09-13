"""Idempotent daily sign-in vertical slice."""

from .application import SignInApplication
from .manifest import FEATURE

__all__ = ["FEATURE", "SignInApplication"]
