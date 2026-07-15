from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from ..xiuxian_utils.fight_models import Entity
from ..xiuxian_utils.player_fight import BattleSystem, apply_player_buffs, get_players_attributes
from .relation_transaction_utils import increment_stat


@dataclass(frozen=True)
class NormalPvpResult:
    status: str
    winner_id: str = ""
    winner_name: str = "没有人"
    battle_messages: list = field(default_factory=list)
    challenger_hp: int = 0
    challenger_mp: int = 0
    opponent_hp: int = 0
    opponent_mp: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class NormalPvpSettlementService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(challenger_id, opponent_id, stamina_cost) -> str:
        # Request identity only — HP/MP/exp snapshots and battle results are outcomes.
        return json.dumps(
            {
                "challenger_id": str(challenger_id),
                "opponent_id": str(opponent_id),
                "stamina_cost": int(stamina_cost),
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )

    @staticmethod
    def _saved_result(row, status="duplicate") -> NormalPvpResult:
        saved = json.loads(str(row))
        return NormalPvpResult(status=status, **saved)

    def replay(self, operation_id, challenger_id, opponent_id) -> NormalPvpResult | None:
        with closing(db_backend.connect(self._game_database)) as conn:
            exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='normal_pvp_operations'"
            ).fetchone()
            if exists is None:
                return None
            row = conn.execute(
                "SELECT payload,result_json FROM normal_pvp_operations WHERE operation_id=%s",
                (str(operation_id),),
            ).fetchone()
            if row is None:
                return None
            payload = json.loads(str(row[0]))
            if isinstance(payload, dict):
                participants = [payload.get("challenger_id"), payload.get("opponent_id")]
            else:
                # Keep previously persisted operations replayable during rollout.
                participants = payload[:2]
            if participants != [str(challenger_id), str(opponent_id)]:
                return NormalPvpResult("operation_conflict")
            return self._saved_result(row[1])

    @staticmethod
    def calculate_battle(challenger_id, opponent_id, bot_id=0):
        players = [get_players_attributes(challenger_id), get_players_attributes(opponent_id)]
        entities = []
        for team_id, player in enumerate(players):
            attributes = player["属性"]
            attributes["natal_data"] = player.get("本命法宝")
            entity = Entity(attributes, team_id=team_id)
            apply_player_buffs(entity, player)
            entities.append(entity)
        messages, winner, statuses = BattleSystem([entities[0]], [entities[1]], bot_id).run_battle()
        final = {}
        for item in statuses:
            for attributes in item.values():
                hp_multiplier = float(attributes.get("hp_multiplier", 1) or 1)
                mp_multiplier = float(attributes.get("mp_multiplier", 1) or 1)
                final[str(attributes["user_id"])] = (
                    max(1, int(float(attributes.get("hp", 1)) / hp_multiplier)),
                    max(1, int(float(attributes.get("mp", 1)) / mp_multiplier)),
                )
        winner_id = "" if winner == 2 else str((challenger_id, opponent_id)[winner])
        winner_name = "没有人" if winner == 2 else str(players[winner]["属性"]["nickname"])
        return messages, winner_id, winner_name, final

    def settle(
        self,
        operation_id,
        challenger_id,
        opponent_id,
        *,
        expected_challenger_hp,
        expected_challenger_mp,
        expected_challenger_stamina,
        expected_challenger_exp,
        expected_opponent_hp,
        expected_opponent_mp,
        expected_opponent_stamina,
        expected_opponent_exp,
        challenger_final_hp,
        challenger_final_mp,
        opponent_final_hp,
        opponent_final_mp,
        winner_id="",
        winner_name="没有人",
        battle_messages=None,
        stamina_cost=1,
    ) -> NormalPvpResult:
        operation_id = str(operation_id).strip()
        challenger_id, opponent_id = str(challenger_id), str(opponent_id)
        snapshots = (
            expected_challenger_hp,
            expected_challenger_mp,
            expected_challenger_stamina,
            expected_challenger_exp,
            expected_opponent_hp,
            expected_opponent_mp,
            expected_opponent_stamina,
            expected_opponent_exp,
        )
        snapshots = tuple(int(value) for value in snapshots)
        finals = tuple(
            max(1, int(value))
            for value in (challenger_final_hp, challenger_final_mp, opponent_final_hp, opponent_final_mp)
        )
        stamina_cost = int(stamina_cost)
        winner_id = str(winner_id or "")
        battle_messages = list(battle_messages or [])
        if not operation_id or challenger_id == opponent_id or stamina_cost < 0:
            raise ValueError("invalid normal pvp settlement arguments")
        if winner_id not in {"", challenger_id, opponent_id}:
            raise ValueError("invalid normal pvp winner")
        payload = self._payload(
            challenger_id,
            opponent_id,
            stamina_cost,
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS normal_pvp_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,result_json FROM normal_pvp_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return NormalPvpResult("operation_conflict")
                    return self._saved_result(previous[1])

                rows = conn.execute(
                    "SELECT user_id,COALESCE(hp,0),COALESCE(mp,0),COALESCE(user_stamina,0),COALESCE(exp,0) "
                    "FROM user_xiuxian WHERE user_id IN (%s,%s)",
                    (challenger_id, opponent_id),
                ).fetchall()
                current = {str(row[0]): tuple(int(value) for value in row[1:]) for row in rows}
                if challenger_id not in current or opponent_id not in current:
                    conn.rollback()
                    return NormalPvpResult("user_missing")
                challenger_snapshot, opponent_snapshot = snapshots[:4], snapshots[4:]
                if current[challenger_id] != challenger_snapshot or current[opponent_id] != opponent_snapshot:
                    conn.rollback()
                    return NormalPvpResult("state_changed")
                if snapshots[0] <= snapshots[3] / 10:
                    conn.rollback()
                    return NormalPvpResult("challenger_injured")
                if snapshots[2] < stamina_cost:
                    conn.rollback()
                    return NormalPvpResult("stamina_insufficient")

                changed = conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s,user_stamina=user_stamina-%s "
                    "WHERE user_id=%s AND hp=%s AND mp=%s AND user_stamina=%s AND exp=%s AND user_stamina>=%s",
                    (finals[0], finals[1], stamina_cost, challenger_id, *challenger_snapshot, stamina_cost),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return NormalPvpResult("state_changed")
                changed = conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s WHERE user_id=%s AND hp=%s AND mp=%s AND user_stamina=%s AND exp=%s",
                    (finals[2], finals[3], opponent_id, *opponent_snapshot),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return NormalPvpResult("state_changed")

                if winner_id:
                    loser_id = opponent_id if winner_id == challenger_id else challenger_id
                    increment_stat(conn, winner_id, "切磋胜利", 1)
                    increment_stat(conn, loser_id, "切磋失败", 1)
                saved = {
                    "winner_id": winner_id,
                    "winner_name": str(winner_name),
                    "battle_messages": battle_messages,
                    "challenger_hp": finals[0],
                    "challenger_mp": finals[1],
                    "opponent_hp": finals[2],
                    "opponent_mp": finals[3],
                }
                conn.execute(
                    "INSERT INTO normal_pvp_operations (operation_id,payload,result_json) VALUES (%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, ensure_ascii=False, separators=(",", ":"))),
                )
                conn.commit()
                return NormalPvpResult("applied", **saved)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass


__all__ = ["NormalPvpResult", "NormalPvpSettlementService"]
