"""Validate duplicated historical state without deleting or merging rows."""

from ...core.errors import DomainError
from ...core.numeric import as_int_like


def state_row_count(uow, user_id, fields, *, schema="main"):
    if schema not in {"main", "impart_data"}:
        raise ValueError("invalid impart schema")
    rows = uow.query_all(
        f"SELECT * FROM {schema}.xiuxian_impart WHERE user_id=?", (user_id,)
    )
    for field in fields:
        if len({as_int_like(row[field] or 0) for row in rows}) > 1:
            raise DomainError("impart_rows_inconsistent", "historical impart rows disagree", {"reason": f"impart_rows_{field}", "subject": "actor"})
    return len(rows)


def check_state_write(cursor, expected_count):
    if cursor.rowcount != expected_count:
        raise DomainError("impart_write_rejected", "impart write count mismatch")
