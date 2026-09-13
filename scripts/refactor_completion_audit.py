"""Produce a machine-readable P0-P7 refactor completion report."""

from __future__ import annotations

import argparse
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Importing the plugin package emits transport diagnostics on stdout.  Keep the
# real stdout reserved for the JSON report so operators can pipe this command
# into JSON tooling.
with redirect_stdout(sys.stderr):
    try:
        from check_architecture import (  # type: ignore[import-not-found]
            check_adr_coverage,
            check_all_web_endpoints_have_permission,
            check_browser_smoke_contract,
            check_compatibility_gate_contract,
            check_dependency_lock,
            check_documented_migration_count,
            check_feature_connections,
            check_feature_contracts,
            check_feature_lifecycle_hooks,
            check_legacy_command_manifest_alignment,
            check_legacy_repository_placeholders,
            check_legacy_scheduler_manifest_alignment,
            check_manifest,
            check_manifest_documentation,
            check_manifest_ids_are_unique,
            check_migrated_command_aliases,
            check_migrated_command_inventory,
            check_migrated_legacy_entrypoints,
            check_migration_versions_are_monotonic,
            check_no_lifecycle_hooks_outside_bootstrap,
            check_operation_id_on_asset_writes,
            check_refactor_inventory,
            check_refactored_web_templates_modular,
            check_remote_smoke_contract,
            check_runtime_files,
        )
    except ModuleNotFoundError:  # importing as ``scripts.refactor_completion_audit``
        from scripts.check_architecture import (
            check_adr_coverage,
            check_all_web_endpoints_have_permission,
            check_browser_smoke_contract,
            check_compatibility_gate_contract,
            check_dependency_lock,
            check_documented_migration_count,
            check_feature_connections,
            check_feature_contracts,
            check_feature_lifecycle_hooks,
            check_legacy_command_manifest_alignment,
            check_legacy_repository_placeholders,
            check_legacy_scheduler_manifest_alignment,
            check_manifest,
            check_manifest_documentation,
            check_manifest_ids_are_unique,
            check_migrated_command_aliases,
            check_migrated_command_inventory,
            check_migrated_legacy_entrypoints,
            check_migration_versions_are_monotonic,
            check_no_lifecycle_hooks_outside_bootstrap,
            check_operation_id_on_asset_writes,
            check_refactor_inventory,
            check_refactored_web_templates_modular,
            check_remote_smoke_contract,
            check_runtime_files,
        )
    from nonebot_plugin_xiuxian_2.compatibility.release_gate import CompatibilityReleaseGate

_STDOUT = sys.stdout


Check = Callable[[], list[str]]


def _validated_data_dir(value: str | Path | None) -> str | Path | None:
    """Treat an empty CLI value as invalid instead of falling back to defaults."""
    if value is not None and not str(value).strip():
        raise ValueError("--data-dir must be a non-empty path")
    return value


def _static_requirements() -> dict[str, list[Check]]:
    return {
        "P0": [
            lambda: _missing_files(
                "P0 baseline files",
                ROOT / "requirements.lock",
                ROOT / "docs" / "refactor_baseline.md",
                ROOT / "docs" / "refactor_inventory.json",
                ROOT / "tests" / "bootstrap.py",
            ),
            check_dependency_lock,
            check_documented_migration_count,
            check_refactor_inventory,
        ],
        "P1": [check_no_lifecycle_hooks_outside_bootstrap, check_feature_lifecycle_hooks, check_manifest, check_manifest_ids_are_unique, check_runtime_files],
        "P2": [check_feature_connections, check_migration_versions_are_monotonic, check_operation_id_on_asset_writes],
        "P3": [check_feature_contracts, check_manifest_documentation, check_migrated_legacy_entrypoints],
        "P4": [check_legacy_repository_placeholders, check_migrated_command_inventory, check_migrated_command_aliases],
        "P5": [check_refactored_web_templates_modular, check_all_web_endpoints_have_permission],
        "P6": [check_legacy_scheduler_manifest_alignment, check_remote_smoke_contract, check_browser_smoke_contract, check_compatibility_gate_contract, check_adr_coverage],
    }


def _missing_files(label: str, *paths: Path) -> list[str]:
    return [f"{label}: missing {path.relative_to(ROOT)}" for path in paths if not path.is_file()]


def audit(
    *,
    data_dir: str | Path | None = None,
    current_release: str | None = None,
    logs: tuple[str, ...] = (),
    recovery_evidence: str | Path | None = None,
) -> dict[str, object]:
    data_dir = _validated_data_dir(data_dir)
    stages: dict[str, dict[str, object]] = {}
    for stage, checks in _static_requirements().items():
        errors: list[str] = []
        for check in checks:
            try:
                errors.extend(check())
            except Exception as exc:  # keep the audit report actionable
                errors.append(f"{check.__name__}: {type(exc).__name__}: {exc}")
        stages[stage] = {"ready": not errors, "errors": errors}

    if data_dir is None or current_release is None or recovery_evidence is None:
        stages["P7"] = {
            "ready": False,
            "errors": [
                "real release-cycle evidence is required: --data-dir, --current-release and --evidence"
            ],
        }
    else:
        try:
            from nonebot_plugin_xiuxian_2.plugin import build_migrations

            report = CompatibilityReleaseGate(data_dir).evaluate(
                current_release,
                log_paths=logs,
                recovery_evidence=recovery_evidence,
                required_migrations=[migration.version for migration in build_migrations()],
            )
            stages["P7"] = {
                "ready": report.ready,
                "errors": list(report.reasons),
                "checks": dict(report.checks),
                "details": dict(report.details),
            }
        except Exception as exc:
            stages["P7"] = {
                "ready": False,
                "errors": [f"{type(exc).__name__}: {exc}"],
            }

    return {"ready": all(bool(stage["ready"]) for stage in stages.values()), "stages": stages}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="refactor-completion-audit")
    parser.add_argument("--data-dir")
    parser.add_argument("--current-release")
    parser.add_argument("--evidence")
    parser.add_argument("--log", action="append", default=[])
    args = parser.parse_args(argv)
    try:
        report = audit(
            data_dir=args.data_dir,
            current_release=args.current_release,
            logs=tuple(args.log),
            recovery_evidence=args.evidence,
        )
    except ValueError as exc:
        report = {"ready": False, "stages": {"P7": {"ready": False, "errors": [str(exc)]}}}
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if report["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
