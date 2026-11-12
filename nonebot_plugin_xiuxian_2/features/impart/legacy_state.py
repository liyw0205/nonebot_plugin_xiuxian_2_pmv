"""Validate duplicated historical state without deleting or merging rows."""

import json
from ...core.errors import DomainError
from ...core.numeric import as_int_like, crystal_precision_reason, exact_crystal_integer


def crystal_balance(row, *, subject=None, field="stone_num"):
    if field != "stone_num":
        row = {
            "user_id": row.get("user_id"),
            "stone_num": row.get(field),
            "stone_num_exact": row.get(f"{field}_exact"),
            "stone_num_precision_state": row.get(f"{field}_precision_state", "exact"),
        }
    subject = str(subject or row.get("user_id") or "actor")
    precision_state = str(row.get("stone_num_precision_state", "exact"))
    if precision_state != "exact":
        reason = crystal_precision_reason(precision_state)
        raise DomainError(
            "impart_crystal_precision_unavailable",
            f"legacy crystal balance is not exact: {precision_state}",
            {"reason": reason, "subject": subject},
        )
    exact = row.get("stone_num_exact")
    if exact is not None:
        value = exact_crystal_integer(exact, "text")
        if value is not None:
            return value
        raise DomainError(
            "impart_crystal_precision_unavailable",
            "exact crystal shadow value is invalid",
            {
                "reason": "crystal_invalid_numeric_value",
                "subject": subject,
            },
        )
    legacy = row.get("stone_num", 0)
    value = exact_crystal_integer(legacy, row.get("stone_num_storage_type"))
    if value is not None:
        return value
    reason = crystal_precision_reason("invalid_numeric_value")
    if isinstance(legacy, float) and abs(legacy) >= 2**53:
        reason = crystal_precision_reason("precision_lost_real")
    raise DomainError(
        "impart_crystal_precision_unavailable",
        "legacy crystal value cannot be interpreted as an exact integer",
        {"reason": reason, "subject": subject},
    )


def state_row_count(uow, user_id, fields, *, schema="main"):
    if schema not in {"main", "impart_data"}:
        raise ValueError("invalid impart schema")
    rows = uow.query_all(
        f"SELECT * FROM {schema}.xiuxian_impart WHERE user_id=?", (user_id,)
    )
    for field in fields:
        values = (
            {crystal_balance(row) for row in rows}
            if field == "stone_num"
            else {as_int_like(row[field] or 0) for row in rows}
        )
        if len(values) > 1:
            raise DomainError("impart_rows_inconsistent", "historical impart rows disagree", {"reason": f"impart_rows_{field}", "subject": str(user_id)})
    return len(rows)


def check_state_write(cursor, expected_count):
    if cursor.rowcount != expected_count:
        raise DomainError("impart_write_rejected", "impart write count mismatch")


def crystal_progress_sum(rows):
    """Stream exact JSON deltas; INTEGER-affinity progress columns are mirrors."""
    total = 0
    for row in rows:
        receipt = json.loads(row["result_json"] or "{}")
        value = receipt.get("applied_delta", row["applied_delta"])
        total += crystal_balance({"stone_num": value}, subject=row["user_id"])
    return total
