"""Application boundary for the web database console."""

from .application import DatabaseConsoleApplication
from .repository import DatabaseConsoleRepository

__all__ = ["DatabaseConsoleApplication", "DatabaseConsoleRepository"]
