from __future__ import annotations

import csv
from tempfile import SpooledTemporaryFile
from typing import Any

from flask import Response

from ...features.economy_ledger.repository import ECONOMY_LOG_FIELDS


def build_csv_response(application: Any, filters: Any) -> Response:
    output = SpooledTemporaryFile(
        max_size=1024 * 1024,
        mode="w+",
        encoding="utf-8",
        newline="",
    )
    writer = csv.DictWriter(
        output,
        fieldnames=ECONOMY_LOG_FIELDS,
        extrasaction="ignore",
    )
    writer.writeheader()
    try:
        for row in application.iter_export_rows(filters):
            writer.writerow({field: row.get(field, "") for field in ECONOMY_LOG_FIELDS})
    except Exception:
        output.close()
        raise
    output.seek(0)

    def chunks():
        try:
            while True:
                chunk = output.read(64 * 1024)
                if not chunk:
                    break
                yield chunk
        finally:
            output.close()

    return Response(
        chunks(),
        content_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=economy_logs.csv"},
    )


__all__ = ["build_csv_response"]
