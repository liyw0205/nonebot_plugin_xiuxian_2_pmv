"""No scheduler jobs.

The pass runs once per boot from ``runtime._maintain_database`` under
``xiuxian_startup_database_maintenance``; a periodic job would re-scan a converged
database forever, so nothing is registered here.
"""

from __future__ import annotations

JOBS: tuple = ()

__all__ = ["JOBS"]
