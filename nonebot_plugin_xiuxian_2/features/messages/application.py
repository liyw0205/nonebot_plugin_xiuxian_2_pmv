"""Application entry point of the Web message-send slice.

``WebMessageSendApplication`` is implemented in ``send_application.py`` and this
module re-exports that single object.  The Phase 2 frozen legacy-path ledger
binds the ``POST /api/messages/send`` evidence and call graph onto
``send_application.py``, so the implementation stays there until that ledger
entry is re-anchored; the slice still exposes the canonical ``application``
module every other slice has, and the identity test keeps the two module paths
pointing at one class instead of two copies.
"""

from .schemas import MessageSendResult
from .send_application import WebMessageSendApplication

__all__ = ["MessageSendResult", "WebMessageSendApplication"]
