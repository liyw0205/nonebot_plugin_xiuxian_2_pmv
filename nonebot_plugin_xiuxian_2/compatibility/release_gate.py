"""Persistent evidence gate for retiring legacy compatibility entry points.

The gate deliberately does not infer a release cycle from wall-clock time.  An
operator starts a gate for one release and closes it from a later release after
the runtime, logs, migration receipt, and recovery receipt have been checked.
This keeps a passing report auditable and prevents a fresh installation from
being mistaken for a completed compatibility period.
"""

from __future__ import annotations

import json
import hashlib
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from ..infrastructure.clock import SystemClock
from ..infrastructure.filesystem import atomic_write


STATE_VERSION = 1
STATE_FILE_NAME = "compatibility_release_gate.json"
RELEASE_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)(?:[-+][0-9A-Za-z.-]+)?$")
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

# These markers are intentionally narrow.  A normal compatibility migration
# may mention the word "legacy" in logs without indicating an old entry point
# was used.
LEGACY_LOG_PATTERNS = (
    re.compile(r"legacy[ _-]+import", re.IGNORECASE),
    re.compile(r"old[ _-]+import", re.IGNORECASE),
    re.compile(r"legacy[ _-]+url", re.IGNORECASE),
    re.compile(r"web:/[A-Za-z0-9_./-]+"),
    re.compile(r"DeprecationWarning[^\n]*(?:compatibility|legacy)[^\n]*", re.IGNORECASE),
)


@dataclass(frozen=True)
class ReleaseGateReport:
    ready: bool
    checks: Mapping[str, bool]
    reasons: tuple[str, ...]
    details: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "checks": dict(self.checks),
            "reasons": list(self.reasons),
            "details": dict(self.details),
        }


def released_tags() -> set[str]:
    """Return the real release tags recorded in this repository.

    P7 may only be closed by a genuine deployment cycle, so the gate anchors the
    start/close versions to tags that actually exist instead of trusting an
    operator-supplied string.  A temporary rehearsal in a scratch directory has
    no tags of its own and therefore cannot fabricate a completed cycle.
    """
    try:
        result = subprocess.run(
            ["git", "-C", str(REPOSITORY_ROOT), "tag", "--list"],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return set()
    if result.returncode != 0:
        return set()
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def _tag_aliases(release: str) -> set[str]:
    version = _release_version(release)
    canonical = "v%d.%d.%d" % version
    return {canonical, canonical[1:]}


class CompatibilityReleaseGate:
    """Track and validate the evidence required by architecture P7."""

    def __init__(
        self,
        data_dir: str | Path,
        *,
        clock: Any | None = None,
        hits_reader: Callable[[], Mapping[str, int]] | None = None,
        tag_reader: Callable[[], Iterable[str]] | None = None,
    ) -> None:
        if not str(data_dir).strip():
            raise ValueError("data_dir must be a non-empty path")
        self.data_dir = Path(data_dir).expanduser().resolve()
        self.path = self.data_dir / STATE_FILE_NAME
        self.clock = clock or SystemClock()
        self._hits_reader = hits_reader
        self._tag_reader = tag_reader or released_tags

    def load(self) -> dict[str, Any] | None:
        if not self.path.is_file():
            return None
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError) as exc:
            raise ValueError(f"invalid compatibility gate state: {self.path}") from exc
        if not isinstance(value, dict) or value.get("schema") != STATE_VERSION:
            raise ValueError(f"unsupported compatibility gate state: {self.path}")
        if not str(value.get("started_release", "")).strip():
            raise ValueError("compatibility gate state has no started_release")
        return value

    def _released(self, release: str) -> bool:
        tags = {str(tag).strip() for tag in self._tag_reader()}
        return bool(_tag_aliases(release) & tags)

    def start(self, release_id: str, *, started_at: Any | None = None, replace: bool = False) -> dict[str, Any]:
        release = _release_id(release_id, "release_id")
        if not self._released(release):
            raise RuntimeError(
                f"release {release} is not a real repository tag; P7 baselines must come "
                "from an actual deployment, not a rehearsal"
            )
        existing = self.load()
        if existing is not None and not replace:
            if existing.get("started_release") == release and not existing.get("completed_release"):
                return existing
            raise RuntimeError("an active compatibility release gate already exists")
        state = {
            "schema": STATE_VERSION,
            "started_release": release,
            "started_at": _timestamp(started_at, self.clock),
            "baseline_hits": self._read_hits(),
            "completed_release": None,
            "completed_at": None,
            "evidence": None,
        }
        self._write(state)
        return state

    def evaluate(
        self,
        current_release: str,
        *,
        log_paths: Iterable[str | Path] = (),
        recovery_evidence: str | Path | None = None,
        required_migrations: Iterable[str] = (),
    ) -> ReleaseGateReport:
        current = _release_id(current_release, "current_release")
        state = self.load()
        if state is None:
            return ReleaseGateReport(
                False,
                {"release_cycle": False, "compatibility_hits": False, "legacy_logs": False, "historical_migrations": False, "backup_restore": False},
                ("compatibility gate has not been started",),
                {"current_release": current},
            )

        baseline = _normalise_hits(state.get("baseline_hits"))
        current_hits = self._read_hits()
        deltas = {
            key: current_hits.get(key, 0) - baseline.get(key, 0)
            for key in set(current_hits) | set(baseline)
            if current_hits.get(key, 0) > baseline.get(key, 0)
        }
        resets = {
            key: (baseline.get(key, 0), current_hits.get(key, 0))
            for key in set(current_hits) | set(baseline)
            if current_hits.get(key, 0) < baseline.get(key, 0)
        }
        started = _release_id(state.get("started_release"), "started_release")
        release_cycle = (
            state.get("completed_release") == current
            or (
                not state.get("completed_release")
                and _release_version(current) > _release_version(started)
            )
        ) and self._released(current)
        files = _resolve_log_files(self.data_dir, log_paths)
        log_matches = _scan_logs(files)
        evidence = _load_recovery_evidence(recovery_evidence)
        required = tuple(dict.fromkeys(str(item) for item in required_migrations if str(item)))
        applied = set(evidence.get("migrations", ())) if evidence and isinstance(evidence.get("migrations"), list) else set()
        missing_migrations = sorted(set(required) - applied)
        recovery_clean = _recovery_is_clean(evidence)
        backup_restore = bool(
            recovery_clean
            and evidence.get("backup")
            and _backup_manifest_matches(self.data_dir, evidence)
            and evidence.get("restore_dry_run")
            and evidence.get("restore")
        ) if evidence else False
        checks = {
            "release_cycle": release_cycle,
            "compatibility_hits": not deltas and not resets,
            "legacy_logs": bool(files) and not log_matches,
            "historical_migrations": not missing_migrations,
            "backup_restore": backup_restore,
        }
        reasons: list[str] = []
        if not release_cycle:
            if not self._released(current):
                reasons.append(
                    f"{current} is not a real repository tag; a completed deployment cycle is required"
                )
            else:
                reasons.append("a later release has not completed the compatibility cycle")
        if deltas:
            reasons.append(f"legacy entry points were hit after baseline: {sorted(deltas)}")
        if resets:
            reasons.append(f"compatibility hit counters were reset after baseline: {sorted(resets)}")
        if not files:
            reasons.append("no runtime log files were supplied for the zero-hit check")
        elif log_matches:
            reasons.append(f"legacy import/URL markers found in logs: {len(log_matches)}")
        if missing_migrations:
            reasons.append(f"recovery evidence is missing migrations: {missing_migrations}")
        if not backup_restore:
            reasons.append("backup/restore evidence is missing or reconcile is not clean")
        details = {
            "started_release": state.get("started_release"),
            "started_at": state.get("started_at"),
            "completed_release": state.get("completed_release"),
            "current_release": current,
            "hit_deltas": deltas,
            "hit_resets": resets,
            "log_files": [str(path) for path in files],
            "log_matches": log_matches,
            "missing_migrations": missing_migrations,
            "recovery_evidence": str(recovery_evidence) if recovery_evidence else None,
        }
        return ReleaseGateReport(all(checks.values()), checks, tuple(reasons), details)

    def close(
        self,
        current_release: str,
        *,
        log_paths: Iterable[str | Path] = (),
        recovery_evidence: str | Path | None = None,
        required_migrations: Iterable[str] = (),
    ) -> dict[str, Any]:
        report = self.evaluate(
            current_release,
            log_paths=log_paths,
            recovery_evidence=recovery_evidence,
            required_migrations=required_migrations,
        )
        if not report.ready:
            raise RuntimeError("compatibility gate is not ready: " + "; ".join(report.reasons))
        state = self.load()
        if state is None:  # pragma: no cover - evaluate already guards this
            raise RuntimeError("compatibility gate has not been started")
        state["completed_release"] = _release_id(current_release, "current_release")
        state["completed_at"] = _timestamp(None, self.clock)
        state["evidence"] = report.to_dict()
        self._write(state)
        return state

    def _read_hits(self) -> dict[str, int]:
        if self._hits_reader is not None:
            return _normalise_hits(self._hits_reader())
        path = self.data_dir / "compatibility_hits.json"
        if not path.is_file():
            return {}
        try:
            return _normalise_hits(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, ValueError, TypeError) as exc:
            raise ValueError(f"invalid compatibility hit state: {path}") from exc

    def _write(self, state: Mapping[str, Any]) -> None:
        atomic_write(
            self.path,
            json.dumps(dict(state), ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8"),
        )


def _required_text(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required")
    return text


def _release_id(value: Any, name: str) -> str:
    text = _required_text(value, name)
    if RELEASE_RE.fullmatch(text) is None:
        raise ValueError(f"{name} must be a semantic release such as v1.2.0")
    return text


def _release_version(value: str) -> tuple[int, int, int]:
    match = RELEASE_RE.fullmatch(value)
    if match is None:  # pragma: no cover - callers validate release IDs first
        raise ValueError(f"invalid release: {value}")
    return tuple(int(part) for part in match.groups())


def _timestamp(value: Any | None, clock: Any) -> str:
    if value is None:
        value = clock.now()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    return _required_text(value, "timestamp")


def _normalise_hits(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): max(0, int(raw)) for key, raw in value.items()}


def _resolve_log_files(data_dir: Path, paths: Iterable[str | Path]) -> tuple[Path, ...]:
    supplied = tuple(Path(path).expanduser() for path in paths)
    candidates: list[Path] = []
    if supplied:
        candidates.extend(supplied)
    else:
        directory = data_dir / "logs"
        if directory.is_dir():
            candidates.extend(sorted(path for path in directory.rglob("*") if path.is_file()))
    return tuple(path.resolve() for path in candidates if path.is_file())


def _scan_logs(paths: Iterable[Path]) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for path in paths:
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            matches.append({"path": str(path), "line": 0, "text": "unreadable log"})
            continue
        for number, line in enumerate(lines, 1):
            if any(pattern.search(line) for pattern in LEGACY_LOG_PATTERNS):
                matches.append({"path": str(path), "line": number, "text": line[:500]})
    return matches


def _load_recovery_evidence(path: str | Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    candidate = Path(path).expanduser()
    try:
        value = json.loads(candidate.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError) as exc:
        raise ValueError(f"invalid recovery evidence: {candidate}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"recovery evidence must be an object: {candidate}")
    return value


def _recovery_is_clean(evidence: Mapping[str, Any] | None) -> bool:
    if not evidence or evidence.get("schema") != 1:
        return False
    reconcile = evidence.get("reconcile")
    if not isinstance(reconcile, Mapping) or reconcile.get("clean") is not True:
        return False
    return all(reconcile.get(key) == 0 for key in ("operations", "outbox_events", "dead_events"))


def _backup_manifest_matches(data_dir: Path, evidence: Mapping[str, Any]) -> bool:
    backup = Path(str(evidence.get("backup", ""))).name
    expected = str(evidence.get("backup_manifest_sha256", ""))
    if not backup or not expected:
        return False
    manifest = data_dir / "backups" / backup / "manifest.json"
    if not manifest.is_file():
        return False
    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    return digest == expected


__all__ = [
    "CompatibilityReleaseGate",
    "LEGACY_LOG_PATTERNS",
    "ReleaseGateReport",
    "STATE_FILE_NAME",
    "released_tags",
]
