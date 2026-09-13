"""Start, inspect, or close the legacy compatibility release gate."""

from __future__ import annotations

import argparse
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Importing the plugin package emits transport diagnostics on stdout.  Keep the
# real stdout reserved for the JSON report so operators can pipe this command
# into JSON tooling.
with redirect_stdout(sys.stderr):
    from nonebot_plugin_xiuxian_2.compatibility.release_gate import CompatibilityReleaseGate
    from nonebot_plugin_xiuxian_2.plugin import build_migrations


def _non_empty_data_dir(value: str) -> str:
    """Require an explicit path so maintenance commands cannot use default data."""
    if not str(value).strip():
        raise argparse.ArgumentTypeError("--data-dir must be a non-empty path")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="check-compatibility-release")
    parser.add_argument("--data-dir", required=True, type=_non_empty_data_dir)
    commands = parser.add_subparsers(dest="command", required=True)

    begin = commands.add_parser("begin", help="snapshot compatibility hits at a release start")
    begin.add_argument("--release", required=True)
    begin.add_argument("--replace", action="store_true")

    for name in ("status", "close"):
        command = commands.add_parser(name)
        command.add_argument("--current-release", required=True)
        command.add_argument("--log", action="append", default=[])
        command.add_argument("--evidence")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    gate = CompatibilityReleaseGate(args.data_dir)
    if args.command == "begin":
        try:
            state = gate.start(args.release, replace=args.replace)
        except (RuntimeError, ValueError) as exc:
            print(json.dumps({"ready": False, "error": str(exc)}, ensure_ascii=False))
            return 1
        print(json.dumps(state, ensure_ascii=False, sort_keys=True))
        return 0

    required_migrations = [migration.version for migration in build_migrations()]
    if args.command == "status":
        try:
            report = gate.evaluate(
                args.current_release,
                log_paths=args.log,
                recovery_evidence=args.evidence,
                required_migrations=required_migrations,
            )
        except (RuntimeError, ValueError) as exc:
            print(json.dumps({"ready": False, "error": str(exc)}, ensure_ascii=False))
            return 1
        print(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True))
        return 0 if report.ready else 1

    try:
        state = gate.close(
            args.current_release,
            log_paths=args.log,
            recovery_evidence=args.evidence,
            required_migrations=required_migrations,
        )
    except (RuntimeError, ValueError) as exc:
        print(json.dumps({"ready": False, "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(state, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
