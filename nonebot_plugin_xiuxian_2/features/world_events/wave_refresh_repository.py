from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Protocol

from ...infrastructure.database import DatabaseUnitOfWork
from .domain import DemonWaveRefreshResult
from .event_state import (
    STATE_FIELDS,
    decode_event_state_value,
    event_state_schema_ready,
    read_event_state,
    verify_event_state,
    write_event_state,
)


class DemonWaveRefreshRepository(Protocol):
    def replay(self, operation_id: str) -> DemonWaveRefreshResult | None: ...

    def refresh(
        self,
        operation_id: str,
        event_key: str,
        expected_state: Mapping[str, Any],
        next_bosses: Mapping[str, Mapping[str, Any]],
        last_result: str,
    ) -> DemonWaveRefreshResult: ...


class DemonWaveRefreshSqlRepository:
    """Owns wave replacement, reward snapshots and replay in one player UoW."""

    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        return (
            event_state_schema_ready(uow)
            and uow.query_one(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='demon_wave_refresh_operations'"
            )
            is not None
        )

    @staticmethod
    def _result(value: str) -> DemonWaveRefreshResult:
        data = json.loads(value)
        data["refreshed_realms"] = tuple(data.get("refreshed_realms") or ())
        return DemonWaveRefreshResult(**data)

    def replay(self, operation_id: str) -> DemonWaveRefreshResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return None
            row = uow.query_one(
                "SELECT result_json FROM demon_wave_refresh_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
        return None if row is None else self._result(str(row["result_json"]))

    def refresh(
        self,
        operation_id: str,
        event_key: str,
        expected_state: Mapping[str, Any],
        next_bosses: Mapping[str, Mapping[str, Any]],
        last_result: str,
    ) -> DemonWaveRefreshResult:
        operation_id = str(operation_id).strip()
        event_key = str(event_key)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        expected = {
            field: decode_event_state_value(field, expected_state.get(field))
            for field in STATE_FIELDS
        }
        payload = json.dumps(
            {
                "event_key": event_key,
                "expected_state": expected,
                "next_bosses": next_bosses,
                "last_result": last_result,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return DemonWaveRefreshResult("schema_missing")
            previous = uow.query_one(
                "SELECT payload,result_json FROM demon_wave_refresh_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return DemonWaveRefreshResult("operation_conflict")
                return self._result(str(previous["result_json"]))

            current = read_event_state(uow, event_key)
            if current != expected or current is None or current.get("status") != "active":
                return DemonWaveRefreshResult("state_changed")

            bosses = dict(current.get("bosses") or {})
            participants = {
                key: dict(value)
                for key, value in (current.get("participants") or {}).items()
            }
            defeated: list[str] = []
            for realm, boss in bosses.items():
                if int(boss.get("boss_hp") or 0) > 0:
                    continue
                wave = max(int(boss.get("wave") or 1), 1)
                replacement = next_bosses.get(realm)
                if not replacement or int(replacement.get("wave") or 0) != wave + 1:
                    return DemonWaveRefreshResult("invalid_plan")
                reward_base_hp = max(int(boss.get("boss_max_hp") or 0), 1)
                for record in participants.values():
                    if (
                        record.get("realm") != realm
                        or int(record.get("wave") or 1) != wave
                        or int(record.get("damage") or 0) <= 0
                    ):
                        continue
                    record["reward_ready"] = 1
                    record["reward_wave"] = wave
                    record["reward_base_hp"] = max(
                        int(record.get("reward_base_hp") or 0), reward_base_hp
                    )
                    record["reward_total_damage"] = record["reward_base_hp"]
                    if "reward_contribution" not in record:
                        base = min(
                            max(
                                int(record.get("damage") or 0)
                                / record["reward_base_hp"],
                                0.0,
                            ),
                            1.0,
                        )
                        record["base_contribution"] = base
                        record["reward_contribution"] = min(
                            base
                            * max(
                                float(record.get("reward_multiplier") or 1.0), 1.0
                            ),
                            1.0,
                        )
                bosses[realm] = dict(replacement)
                defeated.append(realm)

            if set(next_bosses) != set(defeated):
                return DemonWaveRefreshResult("invalid_plan")
            updated = dict(current)
            updated["bosses"] = bosses
            updated["participants"] = participants
            if defeated:
                updated["last_result"] = str(last_result)
                write_event_state(uow, event_key, updated)
                try:
                    verify_event_state(uow, event_key, updated)
                except RuntimeError as exc:
                    raise RuntimeError(
                        "demon wave refresh state verification failed"
                    ) from exc

            result = DemonWaveRefreshResult("applied", tuple(defeated), updated)
            uow.execute(
                "INSERT INTO demon_wave_refresh_operations "
                "(operation_id,payload,result_json,created_at) VALUES(?,?,?,CURRENT_TIMESTAMP)",
                (
                    operation_id,
                    payload,
                    json.dumps(asdict(result), ensure_ascii=False, sort_keys=True),
                ),
            )
            return result


__all__ = ["DemonWaveRefreshRepository", "DemonWaveRefreshSqlRepository"]
