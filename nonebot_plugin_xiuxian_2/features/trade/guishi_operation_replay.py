"""Read message receipts written before payload-independent operation IDs."""
from __future__ import annotations

from ...infrastructure.database import DatabaseUnitOfWork


def order_receipts(uow: DatabaseUnitOfWork, table: str, operation_id: str, *, legacy_parts: int):
    if not operation_id:
        return []
    prefix = operation_id + ":"
    rows = uow.query_all(
        f"SELECT * FROM {table} WHERE operation_id=? OR substr(operation_id,1,?)=?",
        (operation_id, len(prefix), prefix),
    )
    # Message IDs are opaque (including colons). Batch cancellations keep exact
    # per-order identities; creation/single cancellation/item take read suffixes.
    message_identity = operation_id.startswith(("guishi-order:", "guishi-cancel:qiugou:", "guishi-item-take:"))
    return [row for row in rows if row["operation_id"] == operation_id or (
        message_identity and len(str(row["operation_id"])[len(prefix):].split(":")) == legacy_parts
    )]
