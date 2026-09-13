from flask import Blueprint
from ..api import api_success, api_error
from ._common import guard


def create_blueprint(*, context=None, permission=None) -> Blueprint:
    blueprint = Blueprint("database", __name__)
    resolver = permission or (lambda _required: True)

    @blueprint.get("/api/v1/database")
    @guard("admin", resolver)
    def database_status():
        catalog = getattr(context, "database", None)
        if catalog is None:
            return api_error("unavailable", "数据库目录不可用", status=503)
        return api_success({spec.key: {"path": spec.path.name, "exists": spec.path.exists(), "writable": spec.writable} for spec in catalog.specs()})

    @blueprint.get("/api/v1/database/<key>/tables")
    @guard("admin", resolver)
    def database_tables(key: str):
        catalog = getattr(context, "database", None)
        if catalog is None or key not in catalog.keys():
            return api_error("validation_error", "未知数据库", status=400)
        query = catalog.readonly_query(key)
        rows = query.all("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name") if catalog.path(key).exists() else []
        return api_success({"database": key, "tables": rows})

    @blueprint.get("/api/v1/reconcile")
    @guard("admin", resolver)
    def reconcile_status():
        from ....infrastructure.database import DatabaseUnitOfWork, ReconcileService

        catalog = getattr(context, "database", None)
        if catalog is None:
            return api_error("unavailable", "数据库目录不可用", status=503)
        with DatabaseUnitOfWork(catalog.path("game_db")) as uow:
            report = ReconcileService().inspect(uow)
        return api_success(report.to_dict())

    @blueprint.post("/api/v1/reconcile")
    @guard("admin", resolver, write=True)
    def reconcile_run():
        from ....infrastructure.database import DatabaseUnitOfWork, ReconcileService

        catalog = getattr(context, "database", None)
        if catalog is None:
            return api_error("unavailable", "数据库目录不可用", status=503)
        with DatabaseUnitOfWork(catalog.path("game_db")) as uow:
            report = ReconcileService().run(
                uow,
                operation_handlers=getattr(context, "reconcile_handlers", None),
            )
        return api_success(report.to_dict())

    return blueprint


__all__ = ["create_blueprint"]
