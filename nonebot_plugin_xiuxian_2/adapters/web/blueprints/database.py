from flask import Blueprint
from ..api import api_success, api_error
from ._common import guard


def create_blueprint(*, context=None, permission=None) -> Blueprint:
    blueprint = Blueprint("database", __name__)
    resolver = permission or (lambda _required: True)

    def reconcile_database():
        from ....infrastructure.database import DatabaseUnitOfWork

        catalog = getattr(context, "database", None)
        if catalog is None:
            return None, api_error("unavailable", "数据库目录不可用", status=503)
        database = catalog.path("game_db")
        if not database.exists():
            return None, api_error("migrations_required", "数据库尚未初始化", status=503)
        with DatabaseUnitOfWork(database) as uow:
            ledger = uow.query_one(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                ("operation_ledger",),
            )
            if ledger is None:
                return None, api_error("migrations_required", "数据库尚未完成迁移", status=503)
        return database, None

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

        database, error = reconcile_database()
        if error is not None:
            return error
        with DatabaseUnitOfWork(database) as uow:
            report = ReconcileService().inspect(uow)
        return api_success(report.to_dict())

    @blueprint.post("/api/v1/reconcile")
    @guard("admin", resolver, write=True)
    def reconcile_run():
        from ....infrastructure.database import DatabaseUnitOfWork, ReconcileService

        database, error = reconcile_database()
        if error is not None:
            return error
        with DatabaseUnitOfWork(database) as uow:
            report = ReconcileService().run(
                uow,
                handlers=getattr(context, "outbox_handlers", None),
                operation_handlers=getattr(context, "reconcile_handlers", None),
            )
        return api_success(report.to_dict())

    return blueprint


__all__ = ["create_blueprint"]
