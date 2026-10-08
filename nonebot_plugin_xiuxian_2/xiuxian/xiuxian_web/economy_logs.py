from __future__ import annotations

from ...adapters.web.economy_ledger_csv import build_csv_response
from ...adapters.web.blueprints.economy_logs import create_blueprint
from ...features.economy_ledger.application import EconomyLedgerApplication
from ...infrastructure.clock import SystemClock
from .core import DATABASE, app, redirect, request, session, url_for


runtime_clock = SystemClock()
ledger = EconomyLedgerApplication(DATABASE, clock=runtime_clock)

app.register_blueprint(
    create_blueprint(
        application=ledger,
        permission=lambda _required: "admin_id" in session,
    )
)


@app.route("/economy_logs")
def economy_logs():
    if "admin_id" not in session:
        return redirect(url_for("login"))
    return redirect(url_for("economy_logs.page", **request.args), code=308)


@app.route("/economy_logs/export")
def economy_logs_export():
    if "admin_id" not in session:
        return redirect(url_for("login"))
    return build_csv_response(ledger, request.args)
