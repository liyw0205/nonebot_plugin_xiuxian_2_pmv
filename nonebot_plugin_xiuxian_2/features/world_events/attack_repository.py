from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Protocol

from ...infrastructure.database import DatabaseUnitOfWork
from .domain import DemonAttackSettlementResult

_SQLITE_INT_MIN = -(2**63)
_SQLITE_INT_MAX = 2**63 - 1


class DemonAttackSettlementRepository(Protocol):
    def get_result(self, operation_id: str) -> DemonAttackSettlementResult | None: ...

    def settle(self, **kwargs: Any) -> DemonAttackSettlementResult: ...


class DemonAttackSettlementSqlRepository:
    """Owns the player-database transaction for demon attack settlement."""

    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    @staticmethod
    def _json(value: Any, default: Any) -> Any:
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value or "{}")
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _integer(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _participant_key(user_id: str, realm: str, wave: int) -> str:
        return f"{realm}:{max(wave, 1)}:{user_id}"

    @staticmethod
    def _count_attacks(participants: Mapping[str, Any], user_id: str) -> int:
        return sum(
            max(DemonAttackSettlementSqlRepository._integer(record.get("attacks")), 0)
            for record in participants.values()
            if isinstance(record, Mapping) and str(record.get("user_id")) == user_id
        )

    @staticmethod
    def _claimed(claimed: Mapping[str, Any], user_id: str, record_key: str) -> bool:
        if claimed.get(user_id) or claimed.get(record_key):
            return True
        return any(
            value and str(key).endswith(f":{user_id}")
            for key, value in claimed.items()
        )

    @staticmethod
    def _sqlite_integer(value: int) -> int | str:
        value = int(value)
        return value if _SQLITE_INT_MIN <= value <= _SQLITE_INT_MAX else str(value)

    @classmethod
    def _increment_stat(
        cls, uow: DatabaseUnitOfWork, user_id: str, key: str, amount: int
    ) -> None:
        column = '"' + key.replace('"', '""') + '"'
        amount_param = cls._sqlite_integer(amount)
        changed = uow.execute(
            f"UPDATE statistics SET {column}=COALESCE({column},0)+? WHERE user_id=?",
            (amount_param, user_id),
        )
        if changed.rowcount == 0:
            uow.execute(
                f"INSERT INTO statistics (user_id,{column}) VALUES (?,?)",
                (user_id, amount_param),
            )

    @staticmethod
    def _from_json(value: str, status: str) -> DemonAttackSettlementResult:
        data = json.loads(str(value))
        data["status"] = status
        return DemonAttackSettlementResult(**data)

    @staticmethod
    def _payload(
        event_key: str,
        user_id: str,
        realm: str,
        expected_event: Mapping[str, Any],
    ) -> str:
        return json.dumps(
            {
                "event_key": event_key,
                "user_id": user_id,
                "realm": realm,
                "event_id": str(expected_event.get("event_id") or ""),
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    def get_result(self, operation_id: str) -> DemonAttackSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with DatabaseUnitOfWork(self.player_database) as uow:
            row = uow.query_one(
                "SELECT result_json FROM demon_attack_settlement_operations WHERE operation_id=?",
                (operation_id,),
            )
            return self._from_json(row["result_json"], "duplicate") if row else None

    def settle(
        self,
        operation_id: str,
        event_key: str,
        user_id: str,
        user_name: str,
        realm: str,
        total_damage: int,
        expected_event: Mapping[str, Any],
        expected_boss: Mapping[str, Any],
        expected_participants: Mapping[str, Any],
        *,
        attack_limit: int,
        real_hp_multiplier: float,
        max_damage_ratio: float,
        max_pursuit_ratio: float,
    ) -> DemonAttackSettlementResult:
        operation_id = str(operation_id).strip()
        event_key, user_id, realm = str(event_key), str(user_id), str(realm)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        payload = self._payload(event_key, user_id, realm, expected_event)

        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT payload,result_json FROM demon_attack_settlement_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return DemonAttackSettlementResult("operation_conflict")
                return self._from_json(previous["result_json"], "duplicate")

            row = uow.query_one(
                "SELECT status,event_id,bosses,participants,claimed "
                "FROM world_event_state WHERE user_id=?",
                (event_key,),
            )
            if row is None:
                return DemonAttackSettlementResult("state_changed")
            status = str(row["status"] or "")
            event_id = str(row["event_id"] or "")
            bosses = self._json(row["bosses"], {})
            participants = self._json(row["participants"], {})
            claimed = self._json(row["claimed"], {})
            boss = bosses.get(realm)
            if (
                status != str(expected_event.get("status") or "")
                or event_id != str(expected_event.get("event_id") or "")
                or status != "active"
                or boss != dict(expected_boss)
                or participants != dict(expected_participants)
            ):
                return DemonAttackSettlementResult("state_changed")

            wave = max(self._integer(boss.get("wave"), 1), 1)
            record_key = self._participant_key(user_id, realm, wave)
            if self._claimed(claimed, user_id, record_key) or self._count_attacks(
                participants, user_id
            ) >= int(attack_limit):
                return DemonAttackSettlementResult("already_settled")

            boss_all_hp = max(self._integer(boss.get("boss_max_hp")), 1)
            reward_multiplier = max(float(boss.get("reward_multiplier") or 1.0), 1.0)
            current_hp = max(self._integer(boss.get("boss_hp")), 0)
            pursuit_mode = current_hp <= 0
            ratio = float(max_pursuit_ratio if pursuit_mode else max_damage_ratio)
            maximum = max(int(boss_all_hp * ratio), 1)
            raw_damage = max(int(total_damage), 0) * int(real_hp_multiplier)
            real_damage = min(raw_damage, maximum) if pursuit_mode else min(raw_damage, maximum, current_hp)
            boss_now_hp = current_hp if pursuit_mode else max(current_hp - real_damage, 0)
            killed = not pursuit_mode and boss_now_hp <= 0
            contribution_ratio = min(max(real_damage / boss_all_hp, 0.0), 1.0)

            boss = dict(boss)
            boss["boss_hp"] = boss_now_hp
            battle_hp = boss.get("battle_max_hp", boss.get("battle_hp", 1))
            boss["battle_hp"] = battle_hp
            boss["气血"] = battle_hp
            boss["总血量"] = boss.get("battle_max_hp", boss.get("总血量", battle_hp))
            if pursuit_mode:
                boss["last_result"] = f"{user_name or user_id}追击了{realm}魔修。"
            elif killed:
                boss["battle_hp"] = 0
                boss["气血"] = 0
                boss["last_result"] = f"{user_name or user_id}击退了{realm}魔修。"
            bosses = dict(bosses)
            bosses[realm] = boss

            participants = dict(participants)
            record = dict(participants.get(record_key) or {})
            record.update({"user_id": user_id, "realm": realm, "wave": wave, "name": user_name or user_id})
            record["damage"] = self._integer(record.get("damage")) + real_damage
            record["attacks"] = self._integer(record.get("attacks")) + 1
            record["reward_base_hp"] = max(self._integer(record.get("reward_base_hp")), boss_all_hp, 1)
            record["reward_total_damage"] = record["reward_base_hp"]
            settlement_contribution = contribution_ratio * reward_multiplier
            record["reward_multiplier"] = max(float(record.get("reward_multiplier") or 1.0), reward_multiplier)
            record["base_contribution"] = min(
                float(record.get("base_contribution") or 0.0) + contribution_ratio,
                1.0,
            )
            record["reward_contribution"] = min(
                float(record.get("reward_contribution") or 0.0) + settlement_contribution,
                1.0,
            )
            mode = "pursuit" if pursuit_mode else "normal"
            record[f"{mode}_damage"] = self._integer(record.get(f"{mode}_damage")) + real_damage
            record[f"{mode}_contribution"] = min(
                float(record.get(f"{mode}_contribution") or 0.0) + settlement_contribution,
                1.0,
            )
            record[f"{mode}_attacks"] = self._integer(record.get(f"{mode}_attacks")) + 1
            if killed:
                record["last_hit"] = 1
            participants[record_key] = record

            if pursuit_mode or killed:
                for item in participants.values():
                    if (
                        item.get("realm") == realm
                        and max(self._integer(item.get("wave"), 1), 1) == wave
                        and self._integer(item.get("damage")) > 0
                    ):
                        item["reward_ready"] = 1
                        item["reward_wave"] = wave
                        item["reward_base_hp"] = max(self._integer(item.get("reward_base_hp")), boss_all_hp)
                        item["reward_total_damage"] = item["reward_base_hp"]

            total_contribution = min(
                sum(
                    max(float(item.get("reward_contribution") or 0.0), 0.0)
                    for item in participants.values()
                    if str(item.get("user_id")) == user_id and self._integer(item.get("damage")) > 0
                ),
                1.0,
            )
            result = DemonAttackSettlementResult(
                "applied", real_damage, boss_now_hp, boss_all_hp, killed, pursuit_mode,
                contribution_ratio, reward_multiplier, total_contribution,
            )
            uow.execute(
                "UPDATE world_event_state SET bosses=?,participants=? WHERE user_id=?",
                (json.dumps(bosses, ensure_ascii=False), json.dumps(participants, ensure_ascii=False), event_key),
            )
            self._increment_stat(uow, user_id, "魔修入侵参与", 1)
            if real_damage > 0:
                self._increment_stat(uow, user_id, "魔修入侵伤害", real_damage)
            if killed:
                self._increment_stat(uow, user_id, "魔修入侵击退", 1)
            uow.execute(
                "INSERT INTO demon_attack_settlement_operations "
                "(operation_id,payload,result_json,created_at) VALUES (?,?,?,CURRENT_TIMESTAMP)",
                (operation_id, payload, json.dumps(asdict(result), ensure_ascii=False)),
            )
            return result


__all__ = ["DemonAttackSettlementRepository", "DemonAttackSettlementSqlRepository"]
