"""V25 adds late/missing owner seeks; reuses V22 dispatch and write oracles.

Only sixteen background sect owners differ. No new million-row population or
claim of scheduler throughput: that caller is covered by real-job regressions.
"""
from __future__ import annotations

from tests import mixed_sect_directory_fixture as base

PARTICIPANTS = base.PARTICIPANTS
COMMANDS = base.COMMANDS
classify_result = base.classify_result
check_invariants = base.check_invariants


def phase_specs():
    return [dict(spec, order="sect-seek-v25") for spec in base.phase_specs()]


def _owner_cases(sect_ids, late, early):
    if len(sect_ids) != 128 or len(late) != 4 or len(early) != 4:
        raise ValueError("V25 owner case shape differs from the frozen plan")
    return [(sect_ids[4 + index], row["user_id"]) for index, row in enumerate(late)] + [
        (sect_ids[8 + index], str(8_990_000_000_000 + index)) for index in range(4)
    ] + [
        (sect_ids[12 + index], "00" + str(row["user_id"])) for index, row in enumerate(early)
    ] + [(sect_ids[16 + index], f"v25-absent-owner-{index}") for index in range(4)]


def seed(context, managers, *, participants=PARTICIPANTS):
    manifest = base.seed(context, managers, participants=participants)
    shared, database = base.base, context.paths.game_db
    late = shared.shared._rows(database, "SELECT id,user_id FROM user_xiuxian ORDER BY id DESC LIMIT 4")
    early = shared.shared._rows(database, "SELECT id,user_id FROM user_xiuxian ORDER BY id LIMIT 4")
    if any(not str(row["user_id"]).isdecimal() for row in late + early):
        raise ValueError("V25 requires existing canonical numeric identities")
    cases = _owner_cases(manifest["sect_ids"], late, early)
    with shared.shared._uow(database) as uow:
        for sect_id, owner in cases:
            if uow.execute("UPDATE sects SET sect_owner=? WHERE sect_id=?", (owner, sect_id)).rowcount != 1:
                raise ValueError("V25 owner case identity is not unique")
    sects = shared._sects(database)
    owners = [str(row["sect_owner"]) for row in sects.values() if row["sect_owner"] is not None]
    names = {}
    for row in shared.shared._rows(database, "SELECT user_id,user_name FROM user_xiuxian "
                                  f"WHERE user_id IN ({shared.shared._marks(owners)}) ORDER BY rowid", owners):
        names.setdefault(str(row["user_id"]), row["user_name"])
    # INTEGER owner affinity canonicalizes the leading-zero cases. Prove the
    # independent reply oracle's TEXT keys match the actual stored identities.
    for sect_id, _ in cases[8:12]:
        if str(sects[str(sect_id)]["sect_owner"]) not in names:
            raise ValueError("owner alias did not canonicalize under fixture INTEGER affinity")
    ids = list(sects)
    counts = {int(row["sect_id"]): int(row["n"]) for row in shared.shared._rows(
        database, "SELECT sect_id,COUNT(*) AS n FROM user_xiuxian "
        f"WHERE sect_id IN ({shared.shared._marks(ids)}) GROUP BY sect_id", ids)}
    nodes = base._directory_nodes(sects, names, counts)
    manifest["display_nodes"]["sect_directory"] = nodes
    manifest["display_oracles"]["sect_directory"] = {"sha256": base._reply_digest(nodes), "entries": len(nodes)}
    manifest.update(fixture_version="mixed-sect-seek-v25", phase_specs=phase_specs(),
                    initial_sects=sects, owner_cases=cases,
                    late_owner_row_ids=[row["id"] for row in late])
    manifest["limits"].extend([
        "V25 changes16 background owners:4 last-row numeric,4 missing numeric,4 leading-zero aliases,4 missing opaque",
        "owner/member consistency is intentionally malformed only for directory display; gameplay actors stay in original4sects",
        "same128 populated sects/8192members/tasks; unchanged catalog/practice/asset commands are controls",
        "zero-cast buckets with many opaque user identities are NOT populated; small differential regressions cover those semantics",
        "background candidate batching is validated by real scheduler regressions, not sampled by this command load",
    ])
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = shared._digest(manifest)
    return manifest


def request_plan(manifest, mode, count, phase_number=0):
    plan = base.request_plan(manifest, mode, count, phase_number)
    for row in plan:
        if row["key"] != "sect_directory":
            row["performance_role"] = "unchanged_control"
    return plan
