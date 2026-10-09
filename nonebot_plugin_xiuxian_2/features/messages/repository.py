"""Repository boundary of the Web message-send owner.

Nothing here talks to a database or an adapter directly.  The four effects a send
can produce live in other owners and reach this slice as injected ports:

* ``MessageReplyLookupPort``  - the logs slice reply index, used to turn a
  reference id into the real adapter message id.
* ``WebSendRecorderPort``     - the logs slice writer that records the outgoing
  message so the console list shows what the admin sent from the Web UI.
* ``StickerPathResolverPort`` - the stickers owner, which resolves a
  ``<pack_id>/<file>`` token to an installed path.
* ``MessageTransportPort``    - the adapter delivery service that performs the
  actual send and the media upload/URL resolution.

Declaring them keeps the slice honest: it owns Web send policy and nothing else.
"""

from __future__ import annotations

from typing import Any, Protocol


class MessageReplyLookupPort(Protocol):
    def find_reply_target(self, reference_id: str) -> Any: ...

    def find_message_id(self, reference_id: str) -> Any: ...


class WebSendRecorderPort(Protocol):
    def __call__(self, **fields: Any) -> Any: ...


class StickerPathResolverPort(Protocol):
    def resolve_sticker_path(self, token: str) -> Any: ...


class MessageTransportPort(Protocol):
    async def send(self, **fields: Any) -> Any: ...

    async def send_message(self, **fields: Any) -> Any: ...

    async def upload_and_resolve(self, **fields: Any) -> Any: ...


__all__ = [
    "MessageReplyLookupPort",
    "MessageTransportPort",
    "StickerPathResolverPort",
    "WebSendRecorderPort",
]
