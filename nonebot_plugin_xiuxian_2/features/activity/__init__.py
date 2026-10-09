"""Activity lifecycle feature boundary.

The application graph imports legacy activity helpers, and those helpers live in
a package whose own ``__init__`` imports this feature back.  Resolving the two
names on first attribute access keeps either import order working, while every
consumer keeps importing the concrete submodules directly.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .application import ActivityApplication
    from .manifest import FEATURE

_LAZY = ("ActivityApplication", "FEATURE")

__all__ = list(_LAZY)


def __getattr__(name: str):
    if name == "ActivityApplication":
        from .application import ActivityApplication

        return ActivityApplication
    if name == "FEATURE":
        from .manifest import FEATURE

        return FEATURE
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
