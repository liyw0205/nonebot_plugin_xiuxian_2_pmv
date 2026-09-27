"""Compatibility exports for legacy dongfu transactions.

Default dongfu handlers use ``features.dongfu``. This module remains for
external callers that still import the historical service names.
"""

from ...compatibility.legacy_dongfu_transactions import *
from ...compatibility.legacy_dongfu_transactions import __all__
