from __future__ import annotations

from typing import Any

from flask import Blueprint, render_template, request, url_for

from ..api import api_success
from ..economy_ledger_csv import build_csv_response
from ._common import guard


def _page_args(result: dict[str, Any], page: int) -> dict[str, Any]:
    filters = result.get("filters") or {}
    args = {key: value for key, value in filters.items() if value not in (None, "")}
    args["page"] = page
    args["page_size"] = result.get("page_size", result.get("limit", 100))
    return args


def create_blueprint(*, application, permission=None) -> Blueprint:
    blueprint = Blueprint("economy_logs", __name__, template_folder="../templates")
    resolver = permission or (lambda _required: True)

    @blueprint.get("/pages/economy_logs")
    @guard("admin", resolver)
    def page():
        result = application.query_page(request.args)
        current_page = int(result.get("page", 1))
        total_pages = max(int(result.get("total_pages", 1)), 1)
        page_links = {
            "first": url_for(".page", **_page_args(result, 1)),
            "previous": url_for(".page", **_page_args(result, max(current_page - 1, 1))),
            "next": url_for(".page", **_page_args(result, min(current_page + 1, total_pages))),
            "last": url_for(".page", **_page_args(result, total_pages)),
        }
        export_args = {
            key: value
            for key, value in (result.get("filters") or {}).items()
            if value not in (None, "")
        }
        return render_template(
            "pages/economy_logs.html",
            result=result,
            page_links=page_links,
            export_url=url_for(".export_csv", **export_args),
        )

    @blueprint.get("/api/v1/economy-logs")
    @guard("admin", resolver)
    def query():
        return api_success(application.query_page(request.args))

    @blueprint.get("/api/v1/economy-logs/export")
    @guard("admin", resolver)
    def export_csv():
        return build_csv_response(application, request.args)

    return blueprint


__all__ = ["create_blueprint"]
