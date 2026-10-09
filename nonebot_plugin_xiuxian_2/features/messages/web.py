"""Web surface of the Web message-send owner.

``ROUTES`` is empty: ``POST /api/messages/send`` is still registered by the
legacy Flask module ``xiuxian/xiuxian_web/messages.py:487`` behind the admin
session, which accepts JSON or multipart and returns the owner's envelope
verbatim.  The sibling ``/api/messages/*`` paths are not this owner's: history
and revoke belong to the logs slice, group remark/session pin/config to the
admin slice, and ``/api/messages/broadcast*`` still runs through the legacy
``xiuxian/broadcast_manager``.
"""

ROUTES = ()
LEGACY_ROUTES = (
    ("POST", "/api/messages/send", "WebMessageSendApplication.send"),
)

__all__ = ["LEGACY_ROUTES", "ROUTES"]
