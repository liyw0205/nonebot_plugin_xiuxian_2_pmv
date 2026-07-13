from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from ..xiuxian_utils import db_backend


def _json_value(value, default):
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return default


def _integer(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class DemonAttackSettlementResult:
    status: str
    real_damage: int = 0
    boss_now_hp: int = 0
    boss_all_hp: int = 1
    killed: bool = False
    pursuit_mode: bool = False
    contribution_ratio: float = 0.0
    reward_multiplier: float = 1.0
    total_contribution: float = 0.0


class DemonAttackSettlementService:
    def __init__(self, player_db):
        self.player_db = player_db

    @staticmethod
    def _participant_key(user_id, realm, wave):
        return f"{realm}:{max(_integer(wave, 1), 1)}:{user_id}"

    @staticmethod
    def _count_attacks(participants, user_id):
        return sum(
            max(_integer(record.get("attacks")), 0)
            for record in participants.values()
            if str(record.get("user_id")) == user_id
        )

    @staticmethod
    def _claimed(claimed, user_id, record_key):
        if claimed.get(user_id) or claimed.get(record_key):
            return True
        return any(value and str(key).endswith(f":{user_id}") for key, value in claimed.items())

    @staticmethod
    def _increment_stat(conn, user_id, key, amount):
        key_sql = db_backend.quote_ident(key)
        conn.execute("CREATE TABLE IF NOT EXISTS statistics (user_id TEXT PRIMARY KEY)")
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(statistics)").fetchall()}
        if key not in columns:
            conn.execute(f"ALTER TABLE statistics ADD COLUMN {key_sql} INTEGER")
        changed = conn.execute(
            f"UPDATE statistics SET {key_sql}=COALESCE({key_sql},0)+%s WHERE user_id=%s",
            (int(amount), user_id),
        )
        if changed.rowcount == 0:
            conn.execute(
                f"INSERT INTO statistics (user_id,{key_sql}) VALUES (%s,%s)",
                (user_id, int(amount)),
            )

    def settle(
        self,
        operation_id,
        event_key,
        user_id,
        user_name,
        realm,
        total_damage,
        expected_event,
        expected_boss,
        expected_participants,
        *,
        attack_limit,
        real_hp_multiplier,
        max_damage_ratio,
        max_pursuit_ratio,
    ):
        operation_id = str(operation_id).strip()
        event_key, user_id, realm = str(event_key), str(user_id), str(realm)
        if not operation_id:
            raise ValueError("operation_id must not be empty")

        payload = json.dumps(
            {
                "event_key": event_key,
                "user_id": user_id,
                "realm": realm,
                "total_damage": int(total_damage),
                "expected_event": expected_event,
                "expected_boss": expected_boss,
                "expected_participants": expected_participants,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        conn = db_backend.connect(self.player_db)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS demon_attack_settlement_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TEXT NOT NULL)"
            )
            previous = conn.execute(
                "SELECT payload,result_json FROM demon_attack_settlement_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous:
                if str(previous[0]) != payload:
                    conn.rollback()
                    return DemonAttackSettlementResult("operation_conflict")
                conn.commit()
                return DemonAttackSettlementResult(**json.loads(str(previous[1])))

            row = conn.execute(
                "SELECT status,event_id,bosses,participants,claimed FROM world_event_state WHERE user_id=%s",
                (event_key,),
            ).fetchone()
            if row is None:
                conn.rollback()
                return DemonAttackSettlementResult("state_changed")
            status, event_id = str(row[0] or ""), str(row[1] or "")
            bosses = _json_value(row[2], {})
            participants = _json_value(row[3], {})
            claimed = _json_value(row[4], {})
            boss = bosses.get(realm)
            if (
                status != str(expected_event.get("status") or "")
                or event_id != str(expected_event.get("event_id") or "")
                or status != "active"
                or boss != expected_boss
                or participants != expected_participants
            ):
                conn.rollback()
                return DemonAttackSettlementResult("state_changed")

            wave = max(_integer(boss.get("wave"), 1), 1)
            record_key = self._participant_key(user_id, realm, wave)
            if self._claimed(claimed, user_id, record_key) or self._count_attacks(participants, user_id) >= int(attack_limit):
                conn.rollback()
                return DemonAttackSettlementResult("already_settled")

            boss_all_hp = max(_integer(boss.get("boss_max_hp")), 1)
            reward_multiplier = max(float(boss.get("reward_multiplier") or 1.0), 1.0)
            current_hp = max(_integer(boss.get("boss_hp")), 0)
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
            record["damage"] = _integer(record.get("damage")) + real_damage
            record["attacks"] = _integer(record.get("attacks")) + 1
            record["reward_base_hp"] = max(_integer(record.get("reward_base_hp")), boss_all_hp, 1)
            record["reward_total_damage"] = record["reward_base_hp"]
            settlement_contribution = contribution_ratio * reward_multiplier
            record["reward_multiplier"] = max(float(record.get("reward_multiplier") or 1.0), reward_multiplier)
            record["base_contribution"] = min(float(record.get("base_contribution") or 0.0) + contribution_ratio, 1.0)
            record["reward_contribution"] = min(float(record.get("reward_contribution") or 0.0) + settlement_contribution, 1.0)
            mode = "pursuit" if pursuit_mode else "normal"
            record[f"{mode}_damage"] = _integer(record.get(f"{mode}_damage")) + real_damage
            record[f"{mode}_contribution"] = min(float(record.get(f"{mode}_contribution") or 0.0) + settlement_contribution, 1.0)
            record[f"{mode}_attacks"] = _integer(record.get(f"{mode}_attacks")) + 1
            if killed:
                record["last_hit"] = 1
            participants[record_key] = record

            if pursuit_mode or killed:
                for item in participants.values():
                    if item.get("realm") == realm and max(_integer(item.get("wave"), 1), 1) == wave and _integer(item.get("damage")) > 0:
                        item["reward_ready"] = 1
                        item["reward_wave"] = wave
                        item["reward_base_hp"] = max(_integer(item.get("reward_base_hp")), boss_all_hp)
                        item["reward_total_damage"] = item["reward_base_hp"]

            total_contribution = min(
                sum(
                    max(float(item.get("reward_contribution") or 0.0), 0.0)
                    for item in participants.values()
                    if str(item.get("user_id")) == user_id and _integer(item.get("damage")) > 0
                ),
                1.0,
            )
            result = DemonAttackSettlementResult(
                "applied", real_damage, boss_now_hp, boss_all_hp, killed, pursuit_mode,
                contribution_ratio, reward_multiplier, total_contribution,
            )
            conn.execute(
                "UPDATE world_event_state SET bosses=%s,participants=%s WHERE user_id=%s",
                (json.dumps(bosses, ensure_ascii=False), json.dumps(participants, ensure_ascii=False), event_key),
            )
            self._increment_stat(conn, user_id, "魔修入侵参与", 1)
            if real_damage > 0:
                self._increment_stat(conn, user_id, "魔修入侵伤害", real_damage)
            if killed:
                self._increment_stat(conn, user_id, "魔修入侵击退", 1)
            conn.execute(
                "INSERT INTO demon_attack_settlement_operations VALUES (%s,%s,%s,CURRENT_TIMESTAMP)",
                (operation_id, payload, json.dumps(asdict(result), ensure_ascii=False)),
            )
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


__all__ = ["DemonAttackSettlementResult", "DemonAttackSettlementService"]
