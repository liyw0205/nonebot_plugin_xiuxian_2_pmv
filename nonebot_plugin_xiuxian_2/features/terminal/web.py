"""Web surface of the terminal owner.

``ROUTES`` stays empty: the paths below are still registered by the legacy Flask
module ``xiuxian/xiuxian_web/system.py``.  ``GET /terminal`` renders the terminal
page and the GET half of ``/terminal/confirm`` renders the password form; both
delegate nothing, so only ``POST /terminal/confirm`` is listed as a delegation.
``/pages/terminal`` is a ``runtime_web`` redirect and is not owned here.
"""

ROUTES = ()
LEGACY_ROUTES = (
    ("POST", "/terminal/confirm", "TerminalApplication.authorize"),
    ("GET", "/terminal/output", "TerminalApplication.output"),
    ("POST", "/terminal/write", "TerminalApplication.write"),
    ("GET", "/terminal/pwd", "TerminalApplication.cwd"),
)

__all__ = ["LEGACY_ROUTES", "ROUTES"]
