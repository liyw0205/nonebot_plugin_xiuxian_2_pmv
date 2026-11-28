"""Bounded nonempty player projections for the one shared mixed fixture."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from tests import mixed_command_fixture as shared


POPULATION = 8192
MAX_ACTORS = 128
FETCH_ROWS = 128
TITLE_IDS = ("30001", "30002")
TABLES = ("avatar", "title")


def _encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True,
                      separators=(",", ":")).encode("ascii")


def _digest(value):
    return hashlib.sha256(_encoded(value)).hexdigest()


def _paths(paths):
    game, player = (Path(getattr(paths, name)).resolve(strict=True)
                    for name in ("game_db", "player_db"))
    if not game.is_file() or not player.is_file() or game == player or game.parent != player.parent:
        raise ValueError("player projections require distinct existing fixture databases in one directory")
    return game, player


def _actors(actors):
    result = list(actors)
    if (not 1 <= len(result) <= MAX_ACTORS or len(set(result)) != len(result)
            or any(not isinstance(actor, str) or not actor.isascii()
                   or not actor.isdecimal() or int(actor) <= 0 for actor in result)):
        raise ValueError("player projections require unique positive synthetic actor IDs")
    return result


def _catalog(game):
    path = game.parent / "\u4fee\u70bc\u7269\u54c1" / "\u79f0\u53f7.json"
    raw = path.read_bytes()
    definitions = json.loads(raw)
    if not isinstance(definitions, dict):
        raise ValueError("player projection title catalog must be an object")
    selected = {}
    for title_id in TITLE_IDS:
        definition = definitions.get(title_id)
        if (not isinstance(definition, dict) or not isinstance(definition.get("name"), str)
                or not definition["name"].strip() or definition.get("url") not in (None, "")):
            raise ValueError("player projection titles require existing named definitions without media")
        selected[title_id] = definition["name"]
    return selected, hashlib.sha256(raw).hexdigest()


def _population(game, player, actors, row_count):
    required = set(actors)
    for table in TABLES:
        rows = shared._rows(player, f'SELECT user_id FROM "{table}" ORDER BY user_id LIMIT ?',
                            (row_count + 1,))
        if len(rows) > row_count:
            raise ValueError("existing player projection table exceeds the seed row bound")
        required.update(str(row["user_id"]) for row in rows)
    if len(required) > row_count:
        raise ValueError("existing player projection owners exceed the seed population")
    population = list(actors) + sorted(required - set(actors))
    selected = set(population)
    with shared._uow(game, read_only=True) as uow:
        found = set()
        for offset in range(0, len(population), 512):
            batch = population[offset:offset + 512]
            rows = uow.query_all(
                f"SELECT user_id FROM user_xiuxian WHERE user_id IN ({shared._marks(batch)})", batch)
            found.update(str(row["user_id"]) for row in rows)
        if found != required:
            raise ValueError("player projection owners must already exist in the synthetic user roster")
        reserves = []
        cursor = uow.connection.execute("SELECT user_id FROM user_xiuxian ORDER BY id LIMIT ?",
                                        (2 * row_count + MAX_ACTORS,))
        try:
            while rows := cursor.fetchmany(FETCH_ROWS):
                for row in rows:
                    actor = str(row["user_id"])
                    if actor in selected:
                        continue
                    if len(population) < row_count:
                        population.append(actor)
                        selected.add(actor)
                    else:
                        reserves.append(actor)
                        if len(reserves) == row_count:
                            return sorted(population), reserves
        finally:
            cursor.close()
    raise ValueError("player projections need one distinct existing reserve user per population owner")


def _table_snapshot(uow, table, actors, max_rows):
    cursor = uow.connection.execute(f'SELECT * FROM "{table}" ORDER BY user_id LIMIT ?',
                                    (max_rows + 1,))
    digest, count, exact = hashlib.sha256(), 0, {}
    columns = [column[0] for column in cursor.description]
    digest.update(_encoded(columns) + b"\n")
    try:
        while rows := cursor.fetchmany(FETCH_ROWS):
            for row in rows:
                count += 1
                if count > max_rows:
                    raise ValueError("player projection snapshot exceeds its streaming row bound")
                values = dict(row)
                digest.update(_encoded(values) + b"\n")
                actor = str(values["user_id"])
                if actor in actors:
                    exact[actor] = values
    finally:
        cursor.close()
    if set(exact) != actors:
        raise ValueError("player projection snapshot is missing a participating actor")
    return {"rows": count, "columns": columns, "sha256": digest.hexdigest(), "actor_rows": exact}


def snapshot(paths, actors, *, max_rows=POPULATION):
    """Hash at most 8192 rows/table and retain only participating actors exactly."""
    if not isinstance(max_rows, int) or isinstance(max_rows, bool) or not 1 <= max_rows <= POPULATION:
        raise ValueError("invalid player projection snapshot row bound")
    _, player = _paths(paths)
    actors = set(_actors(actors))
    with shared._uow(player, read_only=True) as uow:
        return {table: _table_snapshot(uow, table, actors, max_rows) for table in TABLES}


def seed(paths, actors, *, row_count=POPULATION):
    """Populate only player tables; every referenced user already exists in game."""
    actors = _actors(actors)
    if (not isinstance(row_count, int) or isinstance(row_count, bool)
            or not len(actors) <= row_count <= POPULATION):
        raise ValueError("invalid bounded player projection population")
    game, player = _paths(paths)
    titles, catalog_sha256 = _catalog(game)
    population, reserves = _population(game, player, actors, row_count)
    pairs = dict(zip(population, reserves))
    unlocked = json.dumps(list(TITLE_IDS), separators=(",", ":"))
    with shared._uow(player) as uow:
        uow.executemany(
            "INSERT INTO avatar(user_id,main_id,avatar_id,active_id,create_time) VALUES(?,?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET main_id=excluded.main_id,avatar_id=excluded.avatar_id,"
            "active_id=excluded.active_id,create_time=excluded.create_time",
            ((actor, actor, pairs[actor], actor, shared.ANCHOR) for actor in population))
        uow.executemany(
            "INSERT INTO title(user_id,unlocked,equipped) VALUES(?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET unlocked=excluded.unlocked,equipped=excluded.equipped",
            ((actor, unlocked, TITLE_IDS[int(actor) % len(TITLE_IDS)]) for actor in population))
    saved = snapshot(paths, actors)
    if any(saved[table]["rows"] != row_count for table in TABLES):
        raise ValueError("player projection seed did not retain exactly the bounded population")
    return {
        "population_count": row_count, "population_sha256": _digest(sorted(population)),
        "actor_ids": actors, "actor_pairs": {actor: pairs[actor] for actor in actors},
        "reserve_population_count": len(reserves), "reserve_population_sha256": _digest(reserves),
        "roster_scan_row_bound": 2 * row_count + MAX_ACTORS,
        "title_ids": list(TITLE_IDS), "title_names": titles, "catalog_sha256": catalog_sha256,
        "feature_table_rows": {table: saved[table]["rows"] for table in TABLES},
        "snapshot": saved, "snapshot_row_bound_per_table": POPULATION,
        "limits": ["existing synthetic users only; no added game users or fixture database copies",
                   "all seeded avatar identities remain active=self; one distinct existing nonpopulation reserve per owner",
                   "titles are synthetic previously unlocked display state, not eligibility or reward evidence",
                   "two bounded streaming table digests plus exact participating actor rows; no all-row report payload"],
    }
