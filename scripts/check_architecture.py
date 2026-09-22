"""Read-only architecture guardrails for CI and local pre-commit checks."""

from __future__ import annotations

import ast
from contextlib import redirect_stdout
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "nonebot_plugin_xiuxian_2"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def check_core_imports() -> list[str]:
    errors: list[str] = []
    for path in (PACKAGE / "core").rglob("*.py"):
        forbidden = {name for name in _imports(path) if name.split(".", 1)[0] in {"nonebot", "flask", "sqlite3", "requests"}}
        if forbidden:
            errors.append(f"{path.relative_to(ROOT)} imports {sorted(forbidden)}")
    return errors


def check_no_flask_or_nonebot_import_in_core() -> list[str]:
    return check_core_imports()


def check_feature_connections() -> list[str]:
    errors: list[str] = []
    for path in (PACKAGE / "features").rglob("*.py"):
        if "tests" in path.parts:
            continue
        names = _imports(path)
        source = path.read_text(encoding="utf-8")
        if any(name.split(".", 1)[0] in {"sqlite3", "nonebot", "flask"} for name in names) or any(token in source for token in ("db_backend.connect", "db_backend.connection", "sqlite3.connect")):
            errors.append(f"{path.relative_to(ROOT)} imports a framework/database driver")
    return errors


def check_no_direct_db_connect_in_features() -> list[str]:
    return check_feature_connections()


def check_feature_lifecycle_hooks() -> list[str]:
    errors: list[str] = []
    for path in (PACKAGE / "features").rglob("*.py"):
        if "tests" in path.parts:
            continue
        source = path.read_text(encoding="utf-8")
        if "on_startup" in source or "on_shutdown" in source:
            errors.append(f"{path.relative_to(ROOT)} registers a lifecycle hook")
    return errors


def check_no_startup_decorator_outside_bootstrap() -> list[str]:
    return check_no_lifecycle_hooks_outside_bootstrap()


def check_no_lifecycle_hooks_outside_bootstrap() -> list[str]:
    """Reject direct driver lifecycle registration outside the composition root.

    Adapter vendor code owns its transport shutdown hooks and is intentionally
    excluded.  Legacy gameplay modules must use ``bootstrap.legacy`` so the
    root can order and drain them as one lifecycle.
    """
    errors: list[str] = []
    excluded_parts = {"bootstrap", "__pycache__", "vendor"}
    for path in PACKAGE.rglob("*.py"):
        if excluded_parts.intersection(path.parts):
            continue
        if path.name == "plugin.py":
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            errors.append(f"{path.relative_to(ROOT)} has syntax error: {exc}")
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr in {"on_startup", "on_shutdown"}:
                    errors.append(f"{path.relative_to(ROOT)} registers {node.func.attr} outside bootstrap")
            if isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
                for decorator in node.decorator_list:
                    if isinstance(decorator, ast.Attribute) and decorator.attr in {"on_startup", "on_shutdown"}:
                        errors.append(f"{path.relative_to(ROOT)} decorates {node.name} with {decorator.attr}")
    return errors


def check_all_web_endpoints_have_permission() -> list[str]:
    """Ensure every concrete Flask route is present in a permission manifest."""
    try:
        from nonebot_plugin_xiuxian_2.plugin import build_registry
        from nonebot_plugin_xiuxian_2.adapters.web.app import create_app
        from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context

        registry = build_registry()
        route_index = registry.route_index()
        with tempfile.TemporaryDirectory(prefix="xiuxian-web-guard-") as directory:
            app = create_app(context=build_runtime_context(data_dir=directory), registry=registry)
            errors: list[str] = []
            for rule in app.url_map.iter_rules():
                methods = {method for method in rule.methods if method not in {"HEAD", "OPTIONS"}}
                for method in methods:
                    declared = route_index.get((rule.rule, method))
                    if declared is None or not declared[1].strip():
                        errors.append(f"missing permission: {method} {rule.rule}")
            return errors
    except Exception as exc:
        return [f"web permission check failed: {type(exc).__name__}: {exc}"]


def check_manifest_ids_are_unique() -> list[str]:
    try:
        from nonebot_plugin_xiuxian_2.plugin import build_registry

        build_registry().validate()
        return []
    except Exception as exc:
        return [f"manifest uniqueness failed: {type(exc).__name__}: {exc}"]


def check_legacy_scheduler_manifest_alignment() -> list[str]:
    """Ensure every legacy scheduler declaration has a stable manifest ID."""
    try:
        from nonebot_plugin_xiuxian_2.compatibility.legacy_manifest import FEATURE

        declared = {job.id for job in FEATURE.jobs}
        source_ids: set[str] = set()
        for path in (PACKAGE / "xiuxian").rglob("*.py"):
            if "vendor" in path.parts or "__pycache__" in path.parts:
                continue
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                    continue
                if node.func.attr not in {"scheduled_job", "add_job"}:
                    continue
                for keyword in node.keywords:
                    if keyword.arg != "id" or not isinstance(keyword.value, ast.Constant):
                        continue
                    if isinstance(keyword.value.value, str):
                        source_ids.add(keyword.value.value)
        missing = sorted(source_ids - declared)
        return [f"legacy scheduler IDs missing from manifest: {missing}"] if missing else []
    except Exception as exc:
        return [f"legacy scheduler manifest check failed: {type(exc).__name__}: {exc}"]


def check_migration_versions_are_monotonic() -> list[str]:
    try:
        from nonebot_plugin_xiuxian_2.plugin import build_registry

        versions = [feature.migration_version for feature in build_registry().features if feature.migration_version]
        if versions != sorted(versions):
            return [f"migration versions are not monotonic: {versions}"]
        return []
    except Exception as exc:
        return [f"migration monotonicity check failed: {type(exc).__name__}: {exc}"]


def check_operation_id_on_asset_writes() -> list[str]:
    """Flag new application services that expose mutating methods without an operation ID."""
    errors: list[str] = []
    for path in (PACKAGE / "features").rglob("application.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or node.name.startswith("_"):
                continue
            names = {argument.arg for argument in node.args.args + node.args.kwonlyargs}
            read_only_name = node.name.startswith(("get_", "has_", "is_", "read_", "list_"))
            if any(token in node.name.casefold() for token in ("claim", "purchase", "grant", "settle", "transfer", "withdraw", "deposit")) and not read_only_name and "operation_id" not in names:
                errors.append(f"{path.relative_to(ROOT)}:{node.lineno} mutating method lacks operation_id: {node.name}")
    return errors


def _legacy_migrated_features():
    from nonebot_plugin_xiuxian_2.features._legacy_migrated import FEATURES

    return FEATURES


def check_migrated_command_inventory() -> list[str]:
    """Every legacy slice must expose its historical command surface."""
    errors: list[str] = []
    for feature in _legacy_migrated_features():
        if feature.key == "simulator":
            # The simulator package is an intentionally empty compatibility
            # shell and has no historical matcher declarations.
            continue
        if not feature.commands:
            errors.append(f"{feature.key} has no legacy command inventory")
    return errors


def check_legacy_command_manifest_alignment() -> list[str]:
    """Compare primary ``on_command`` names with each migrated manifest."""
    from nonebot_plugin_xiuxian_2.compatibility.command_inventory import legacy_command_names

    errors: list[str] = []
    for feature in _legacy_migrated_features():
        expected = set(legacy_command_names(feature.key))
        declared = {command.name for command in feature.commands}
        if expected != declared:
            missing = sorted(expected - declared)
            extra = sorted(declared - expected)
            errors.append(
                f"{feature.key} command inventory mismatch: missing={missing}, extra={extra}"
            )
    return errors


def check_migrated_command_aliases() -> list[str]:
    """Ensure aliases of migrated commands remain registered by new slices."""
    from nonebot_plugin_xiuxian_2.compatibility.command_inventory import legacy_migrated_command_names
    from nonebot_plugin_xiuxian_2.plugin import build_registry

    command_index = build_registry().command_index()
    errors: list[str] = []
    for feature in _legacy_migrated_features():
        missing = sorted(
            name
            for name in legacy_migrated_command_names(feature.key)
            if name.casefold() not in command_index
        )
        if missing:
            errors.append(f"{feature.key} migrated command aliases missing: {missing}")
    return errors


def check_legacy_repository_placeholders() -> list[str]:
    """Reject the old ``pending`` result placeholder in migrated repositories."""
    errors: list[str] = []
    for feature in _legacy_migrated_features():
        path = PACKAGE / "features" / feature.key / "repository.py"
        if not path.is_file():
            errors.append(f"{path.relative_to(ROOT)} is missing")
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            errors.append(f"{path.relative_to(ROOT)} has syntax error: {exc}")
            continue
        if any(isinstance(node, ast.Constant) and node.value == "pending" for node in ast.walk(tree)):
            errors.append(f"{path.relative_to(ROOT)} contains pending placeholder")
    return errors


def check_migrated_legacy_entrypoints() -> list[str]:
    """Ensure the known mutating legacy entrypoints cross application facades."""
    package_names = {
        "activity": ("xiuxian_activity", "features.activity.application"),
        "interactive": ("xiuxian_Interactive", "features.interactive.application"),
        "beg": ("xiuxian_beg", "features.beg.application"),
        "tasks": ("xiuxian_tasks", "features.tasks.application"),
        "training": ("xiuxian_training", "features.training.application"),
        "title": ("xiuxian_title", "features.title.application"),
        "lunhui": ("xiuxian_lunhui", "features.lunhui.application"),
        "illusion": ("xiuxian_Illusion", "features.illusion.application"),
        "admin": ("xiuxian_admin", ("features.admin_asset.application",)),
        "compensation": ("xiuxian_compensation", "features.compensation.application"),
        "dongfu": ("xiuxian_dongfu", "features.dongfu.application"),
        "dufang": ("xiuxian_dufang", "features.dufang.application"),
        "entertainment": ("xiuxian_entertainment", "features.entertainment.application"),
        "fusion": ("xiuxian_fusion", "features.fusion.application"),
        "impart": ("xiuxian_impart", "features.impart.application"),
        "impart_pk": ("xiuxian_impart_pk", "features.impart_pk.application"),
        "info": ("xiuxian_info", "features.info.application"),
        "past_life": ("xiuxian_past_life", "features.past_life.application"),
        "status": ("xiuxian_status", "features.status.application"),
        "tianti": (
            "xiuxian_tianti",
            ("features.tianti.application", "features.tianti_settlement.application", "features.tianti_training.application"),
        ),
    }
    errors: list[str] = []
    for feature, (package, application_imports) in package_names.items():
        if isinstance(application_imports, str):
            application_imports = (application_imports,)
        package_path = PACKAGE / "xiuxian" / package
        path = package_path / "__init__.py"
        if not path.is_file():
            errors.append(f"missing legacy entrypoint: {path.relative_to(ROOT)}")
            continue
        sources = "\n".join(
            candidate.read_text(encoding="utf-8")
            for candidate in package_path.rglob("*.py")
        )
        if not any(application_import in sources for application_import in application_imports):
            errors.append(f"{path.relative_to(ROOT)} does not import one of {application_imports}")
        if not re.search(r"\b\w+_application\.(?:execute|execute_legacy_call|[a-z_]+)\s*\(", sources):
            errors.append(f"{path.relative_to(ROOT)} does not dispatch mutations through application")

    # No historical simulator package is checked in.  The new simulator
    # feature remains an explicit empty compatibility shell, so there is no
    # legacy entrypoint to inspect here.
    return errors


def check_no_secrets_or_runtime_data_in_git() -> list[str]:
    return check_runtime_files()


def check_refactored_web_templates_modular() -> list[str]:
    """Reject page-level executable scripts in the refactored templates.

    The legacy Jinja templates remain compatibility assets for one release
    cycle; this guard applies only to the new adapter-owned pages.
    """
    errors: list[str] = []
    template_root = PACKAGE / "adapters" / "web" / "templates"
    for path in template_root.rglob("*.html"):
        source = path.read_text(encoding="utf-8")
        for match in re.finditer(r"<script\b([^>]*)>", source, flags=re.IGNORECASE):
            attributes = match.group(1).casefold()
            if "src=" not in attributes and "application/json" not in attributes:
                errors.append(f"{path.relative_to(ROOT)} contains inline executable script")
    return errors


def check_feature_contracts() -> list[str]:
    """Check the required vertical-slice files and documentation skeleton."""
    errors: list[str] = []
    required = (
        "manifest.py",
        "application.py",
        "repository.py",
        "schemas.py",
        "migrations.py",
        "commands.py",
        "web.py",
        "jobs.py",
    )
    headings = (
        "## 用户流程",
        "## 命令与别名",
        "## Web API",
        "## 数据模型与迁移",
        "## 事务与失败回滚",
        "## 定时任务",
        "## 配置项",
        "## 适配器差异",
        "## 测试与手工验收",
    )
    for directory in sorted((PACKAGE / "features").iterdir()):
        if not directory.is_dir() or directory.name.startswith("_") or directory.name == "__pycache__":
            continue
        for name in required:
            if not (directory / name).is_file():
                errors.append(f"{directory.relative_to(ROOT)} is missing {name}")
        tests = tuple((directory / "tests").glob("test_*.py")) if (directory / "tests").is_dir() else ()
        if not tests:
            errors.append(f"{directory.relative_to(ROOT)}/tests has no test_*.py")
        documentation = ROOT / "docs" / "features" / f"{directory.name}.md"
        if not documentation.is_file():
            errors.append(f"missing feature documentation: {documentation.relative_to(ROOT)}")
            continue
        source = documentation.read_text(encoding="utf-8")
        for heading in headings:
            if heading not in source:
                errors.append(f"{documentation.relative_to(ROOT)} is missing {heading}")
    return errors


def check_manifest_documentation() -> list[str]:
    """Ensure every public manifest identifier is named in its feature doc.

    The feature document is the operator-facing contract for a slice.  Checking
    only migration/config names lets command aliases, routes, and jobs silently
    drift from the registry, so all transport and scheduling identifiers are
    required here as well.
    """
    try:
        from nonebot_plugin_xiuxian_2.plugin import build_registry
    except Exception as exc:
        return [f"manifest documentation check failed: {type(exc).__name__}: {exc}"]
    errors: list[str] = []
    feature_dirs = {path.name for path in (PACKAGE / "features").iterdir() if path.is_dir()}
    for feature in build_registry().features:
        if feature.key not in feature_dirs:
            continue
        documentation = ROOT / "docs" / "features" / f"{feature.key}.md"
        if not documentation.is_file():
            continue
        source = documentation.read_text(encoding="utf-8")
        identifiers: list[str] = []
        for command in feature.commands:
            identifiers.extend((command.name, *command.aliases))
        for route in feature.routes:
            identifiers.extend(f"{method.upper()} {route.path}" for method in route.methods)
        identifiers.extend(job.id for job in feature.jobs)
        identifiers.extend(config.name for config in feature.config)
        if feature.migration_version:
            identifiers.append(feature.migration_version)
        for identifier in identifiers:
            if identifier not in source:
                errors.append(f"{documentation.relative_to(ROOT)} does not name {identifier}")
    return errors


def check_adr_coverage() -> list[str]:
    """Keep the long-lived architecture decisions auditable in ADR files."""
    directory = ROOT / "docs" / "adr"
    source = "\n".join(path.read_text(encoding="utf-8") for path in directory.glob("*.md")) if directory.is_dir() else ""
    topics = {
        "multiple databases": ("多数据库", "DatabaseCatalog"),
        "frontend and API version": ("前端", "/api/v1"),
        "compatibility period": ("兼容周期", "完整发布周期"),
        "ledger retention": ("流水", "备份过期"),
        "remote downtime window": ("远端", "停机窗口"),
    }
    return [f"ADR coverage missing: {name}" for name, terms in topics.items() if not all(term in source for term in terms)]


def check_remote_smoke_contract() -> list[str]:
    path = ROOT / "scripts" / "remote_smoke.sh"
    if not path.is_file():
        return ["scripts/remote_smoke.sh is missing"]
    source = path.read_text(encoding="utf-8")
    required = (
        "REMOTE_PROJECT_DIR",
        "REMOTE_DATA_DIR",
        "REMOTE_STOP_COMMAND",
        "REMOTE_START_COMMAND",
        "REMOTE_WRITE_COMMAND",
        "REMOTE_ROLLBACK_COMMAND",
        "REMOTE_STOP_NEW_COMMAND",
        "health/ready",
        "migrate --dry-run",
        "lmm-server.sh",
    )
    errors = [f"remote smoke script is missing {token}" for token in required if token not in source]

    # The operator hooks are what actually prove the deployment can be isolated,
    # exercised and rolled back.  They must exist, stay executable, and each keep
    # the safety property it is responsible for.
    hooks = ROOT / "scripts" / "remote_smoke_hooks"
    expectations = {
        "ssh_wrapper.sh": ("docker exec",),
        "start.sh": ("</dev/null", "instance.pid"),
        "stop.sh": ("kill", "instance.pid"),
        "rollback.sh": ("restore --backup", "remote-smoke-marker.json", "instance.pid"),
        "run.sh": ("remote_smoke.sh",),
        "provision.sh": ("health/ready",),
    }
    for name, tokens in expectations.items():
        hook = hooks / name
        if not hook.is_file():
            errors.append(f"remote smoke hook is missing: scripts/remote_smoke_hooks/{name}")
            continue
        if not hook.stat().st_mode & 0o111:
            errors.append(f"remote smoke hook is not executable: {name}")
        body = hook.read_text(encoding="utf-8")
        errors.extend(f"{name} is missing {token}" for token in tokens if token not in body)
    return errors


def check_browser_smoke_contract() -> list[str]:
    """Ensure the P6 browser smoke stays a real, two-viewport exercise.

    The Definition of Done requires desktop *and* mobile browser smoke.  A
    script that only loads one viewport, or that no longer asserts the page
    fits its viewport, would silently stop proving that requirement.
    """
    path = ROOT / "scripts" / "browser_smoke.py"
    if not path.is_file():
        return ["scripts/browser_smoke.py is missing"]
    source = path.read_text(encoding="utf-8")
    required = (
        "playwright",
        "1440",
        "390",
        "scrollWidth",
        "innerWidth",
        "screenshot",
        "health/ready",
    )
    return [f"browser smoke script is missing {token}" for token in required if token not in source]


def check_compatibility_gate_contract() -> list[str]:
    """Ensure P7 has an executable evidence gate instead of a prose-only flag."""
    gate = PACKAGE / "compatibility" / "release_gate.py"
    script = ROOT / "scripts" / "check_compatibility_release.py"
    recovery = ROOT / "scripts" / "recovery_smoke.py"
    errors: list[str] = []
    if not gate.is_file():
        errors.append("compatibility release gate is missing")
    else:
        source = gate.read_text(encoding="utf-8")
        for token in ("baseline_hits", "legacy_logs", "historical_migrations", "backup_restore", "backup_manifest_sha256", "completed_release", "RELEASE_RE", "_release_version"):
            if token not in source:
                errors.append(f"compatibility release gate is missing {token}")
    generic_boundary = PACKAGE / "features" / "_legacy_feature.py"
    legacy_jobs = PACKAGE / "compatibility" / "legacy_jobs.py"
    if not generic_boundary.is_file() or "DeprecationWarning" not in generic_boundary.read_text(encoding="utf-8"):
        errors.append("generic legacy feature boundary is missing deprecation telemetry")
    if not legacy_jobs.is_file() or "record_compatibility_hit" not in legacy_jobs.read_text(encoding="utf-8"):
        errors.append("legacy job bridge is missing compatibility hit telemetry")
    if not script.is_file():
        errors.append("scripts/check_compatibility_release.py is missing")
    if not recovery.is_file() or "--evidence" not in recovery.read_text(encoding="utf-8"):
        errors.append("recovery smoke does not emit compatibility evidence")
    return errors


def check_dependency_lock() -> list[str]:
    """Keep the P0 lock installable for both runtime and test tooling.

    ``requirements.lock`` is what CI and the documented baseline install into a
    clean environment.  When it only pinned the runtime packages, ``pytest`` and
    the NoneBot FastAPI driver's ``uvicorn`` were resolved from the network and
    the suite failed on a clean checkout.  This guard keeps those pins explicit
    and sorted so the lock stays reproducible.
    """
    lock = ROOT / "requirements.lock"
    if not lock.is_file():
        return ["requirements.lock is missing"]
    errors: list[str] = []
    entries: list[str] = []
    for raw in lock.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "==" not in line:
            errors.append(f"requirements.lock has an unpinned requirement: {line}")
            continue
        entries.append(line)
    lowered = {entry.split("==", 1)[0].lower() for entry in entries}
    for required in ("pytest", "uvicorn", "nonebot2", "flask"):
        if required not in lowered:
            errors.append(f"requirements.lock is missing {required}")
    ordered = sorted(entries, key=lambda entry: entry.split("==", 1)[0].lower())
    if entries != ordered:
        errors.append("requirements.lock is not sorted by package name")
    return errors


def check_documented_migration_count() -> list[str]:
    """Reject docs that hardcode a stale ``build_migrations()`` count.

    The architecture doc previously claimed a fixed "52 项清单" while the real
    registry had grown to 53 migrations, so readers could not trust the count.
    Any hardcoded total next to ``build_migrations`` must match reality.
    """
    import re

    docs = [ROOT / "docs" / "refactor_architecture.md", ROOT / "docs" / "refactor_baseline.md"]
    pattern = re.compile(r"(\d+)\s*项(?:迁移)?(?:dry-run|清单)")
    try:
        from nonebot_plugin_xiuxian_2.plugin import build_migrations

        actual = len(build_migrations())
    except Exception:  # pragma: no cover - surfaced by other checks
        return []
    errors: list[str] = []
    for path in docs:
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        for match in pattern.finditer(text):
            if int(match.group(1)) != actual:
                line = text[: match.start()].count("\n") + 1
                errors.append(
                    f"{path.relative_to(ROOT)}:{line} claims {match.group(1)} migrations "
                    f"but build_migrations() has {actual}"
                )
    return errors


def check_completion_audit_contract() -> list[str]:
    path = ROOT / "scripts" / "refactor_completion_audit.py"
    if not path.is_file():
        return ["scripts/refactor_completion_audit.py is missing"]
    source = path.read_text(encoding="utf-8")
    required = ("P0", "P7", "CompatibilityReleaseGate", "real release-cycle evidence")
    return [f"completion audit is missing {token}" for token in required if token not in source]


def check_refactor_inventory() -> list[str]:
    """Ensure the P0 command/data inventory is present and reproducible."""
    path = ROOT / "docs" / "refactor_inventory.json"
    if not path.is_file():
        return ["docs/refactor_inventory.json is missing"]
    try:
        actual = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(actual, dict) or actual.get("schema") != 1:
            return ["refactor inventory has an unsupported schema"]
        required = {
            "features",
            "commands",
            "routes",
            "jobs",
            "legacy_routes",
            "legacy_jobs",
            "database_tables",
            "json_files",
            "database_files",
        }
        missing = sorted(required - set(actual))
        if missing:
            return [f"refactor inventory is missing sections: {missing}"]
        from scripts.export_refactor_inventory import build_inventory

        expected = build_inventory()
        return [] if actual == expected else ["refactor inventory is stale; run export_refactor_inventory.py"]
    except Exception as exc:
        return [f"refactor inventory check failed: {type(exc).__name__}: {exc}"]


def check_manifest() -> list[str]:
    try:
        from nonebot_plugin_xiuxian_2.plugin import build_registry

        registry = build_registry()
        registry.validate()
        route_index = registry.route_index()
        # Compare concrete adapter routes against the declared route inventory.
        from nonebot_plugin_xiuxian_2.adapters.web.app import create_app
        from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context

        with tempfile.TemporaryDirectory(prefix="xiuxian-architecture-") as directory:
            app = create_app(context=build_runtime_context(data_dir=directory), registry=registry)
            errors: list[str] = []
            for rule in app.url_map.iter_rules():
                methods = {method for method in rule.methods if method not in {"HEAD", "OPTIONS"}}
                for method in methods:
                    if (rule.rule, method) not in route_index:
                        errors.append(f"undeclared route: {method} {rule.rule}")
            return errors
    except Exception as exc:
        return [f"manifest check failed: {type(exc).__name__}: {exc}"]


def check_runtime_files() -> list[str]:
    errors: list[str] = []
    tracked = subprocess.run(["git", "ls-files"], cwd=ROOT, text=True, capture_output=True, check=False).stdout.splitlines()
    forbidden_suffixes = (".db", ".db-wal", ".db-shm", ".sqlite", ".sqlite3")
    for name in tracked:
        if name.endswith(forbidden_suffixes) or Path(name).name in {"config.json", ".env", ".env.prod", ".env.dev"}:
            errors.append(f"runtime data tracked: {name}")
    return errors


def main() -> int:
    # Importing the compatibility package may emit transport diagnostics;
    # keep the machine-readable report on stdout and diagnostics on stderr.
    with redirect_stdout(sys.stderr):
        checks = {
            "core_imports": check_core_imports(),
            "feature_connections": check_feature_connections(),
            "feature_lifecycle_hooks": check_feature_lifecycle_hooks(),
            "lifecycle_hooks": check_no_lifecycle_hooks_outside_bootstrap(),
            "manifest_routes": check_manifest(),
            "runtime_files": check_runtime_files(),
            "refactored_web_templates": check_refactored_web_templates_modular(),
            "feature_contracts": check_feature_contracts(),
            "manifest_documentation": check_manifest_documentation(),
            "adr_coverage": check_adr_coverage(),
            "remote_smoke_contract": check_remote_smoke_contract(),
            "browser_smoke_contract": check_browser_smoke_contract(),
            "compatibility_gate_contract": check_compatibility_gate_contract(),
            "completion_audit_contract": check_completion_audit_contract(),
            "dependency_lock": check_dependency_lock(),
            "documented_migration_count": check_documented_migration_count(),
            "refactor_inventory": check_refactor_inventory(),
            "web_permissions": check_all_web_endpoints_have_permission(),
            "manifest_ids": check_manifest_ids_are_unique(),
            "legacy_scheduler_manifest": check_legacy_scheduler_manifest_alignment(),
            "migration_versions": check_migration_versions_are_monotonic(),
            "operation_ids": check_operation_id_on_asset_writes(),
            "migrated_command_inventory": check_migrated_command_inventory(),
            "legacy_command_manifest": check_legacy_command_manifest_alignment(),
            "migrated_command_aliases": check_migrated_command_aliases(),
            "legacy_repository_placeholders": check_legacy_repository_placeholders(),
            "migrated_legacy_entrypoints": check_migrated_legacy_entrypoints(),
        }
    errors = [error for values in checks.values() for error in values]
    print(json.dumps({"ok": not errors, "errors": errors, "checks": checks}, ensure_ascii=False, indent=2))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
