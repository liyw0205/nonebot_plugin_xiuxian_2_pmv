from pathlib import Path
from flask import Blueprint, request
from ..api import api_success
from ._common import guard


def create_blueprint(*, context=None, permission=None) -> Blueprint:
    blueprint = Blueprint("backups", __name__)
    resolver = permission or (lambda _required: True)

    @blueprint.get("/api/v1/backups")
    @guard("admin", resolver)
    def backups():
        directory = getattr(getattr(context, "paths", None), "backups", None)
        files = []
        if directory is not None and Path(directory).is_dir():
            files = [{"name": path.name, "size": path.stat().st_size} for path in sorted(Path(directory).iterdir()) if path.is_file()]
        return api_success(files)

    @blueprint.post("/api/v1/backups")
    @guard("admin", resolver, write=True)
    def create_backup():
        from ....infrastructure.database import BackupService

        paths = getattr(context, "paths", None)
        catalog = getattr(context, "database", None)
        if paths is None or catalog is None:
            from ..api import api_error
            return api_error("unavailable", "备份服务不可用", status=503)
        directory = BackupService(
            catalog,
            extra_files={"config": paths.config_file},
            clock=getattr(context, "clock", None),
        ).create(paths.backups)
        return api_success({"name": directory.name})

    @blueprint.post("/api/v1/backups/restore")
    @guard("admin", resolver, write=True)
    def restore_backup():
        from ....infrastructure.database import BackupService

        paths = getattr(context, "paths", None)
        catalog = getattr(context, "database", None)
        if paths is None or catalog is None:
            from ..api import api_error
            return api_error("unavailable", "备份服务不可用", status=503)
        payload = request.get_json(silent=True) or {}
        name = Path(str(payload.get("name", ""))).name
        backup_root = Path(getattr(paths, "backups", "")) if paths is not None else Path(".")
        source = (backup_root / name).resolve()
        if source.parent != backup_root.resolve() or not name or not source.is_dir():
            from ..api import api_error
            return api_error("validation_error", "备份名称无效", status=400)
        result = BackupService(
            catalog,
            extra_files={"config": paths.config_file},
            clock=getattr(context, "clock", None),
        ).restore(source, dry_run=bool(payload.get("dry_run", False)))
        return api_success(result)

    return blueprint


__all__ = ["create_blueprint"]
