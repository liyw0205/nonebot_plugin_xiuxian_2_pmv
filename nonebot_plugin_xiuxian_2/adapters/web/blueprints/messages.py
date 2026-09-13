from flask import Blueprint, render_template
from ..api import api_success
from ._common import guard


def create_blueprint(*, context=None, permission=None) -> Blueprint:
    blueprint = Blueprint("messages", __name__)
    resolver = permission or (lambda _required: True)

    @blueprint.get("/api/v1/messages")
    @guard("admin", resolver)
    def messages():
        return api_success([])

    @blueprint.get("/messages")
    @guard("admin", resolver)
    def messages_page():
        return render_template("pages/messages.html", endpoint="/api/v1/messages")

    return blueprint


__all__ = ["create_blueprint"]
