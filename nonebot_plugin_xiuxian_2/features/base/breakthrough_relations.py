from __future__ import annotations

import json
from pathlib import Path

from ...core.numeric import as_int_like
from ...infrastructure.database import DatabaseUnitOfWork


def _encode(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


class DirectBreakthroughRelations:
    """Durable player reservations followed by game rewards and player projections."""

    def __init__(self, game_database, player_database, *, power_for, cap_for):
        self.game_database = Path(game_database)
        self.player_database = Path(player_database)
        self.power_for = power_for
        self.cap_for = cap_for

    @staticmethod
    def _check_schema(uow, required):
        for table, fields in required.items():
            columns = {row["name"] for row in uow.query_all(f'PRAGMA table_info("{table}")')}
            if not set(fields) <= columns:
                raise RuntimeError(f"direct breakthrough schema_missing: {table}")

    def _prepare(self, event_id, intent, *, skip=False):
        with DatabaseUnitOfWork(self.player_database, immediate=True) as player:
            self._check_schema(player, {
                "direct_breakthrough_relation_receipts": ("event_id", "intent", "payload", "status"),
                "partner": ("user_id", "partner_id", "bind_time"),
                "mentor": ("user_id", "mentor_id", "apprentice_ids", "bind_time", "breakthrough_reward_count", "mentor_history"),
                "statistics": ("user_id", "师父突破返修", "徒弟突破回馈"),
            })
            previous = player.query_one(
                "SELECT * FROM direct_breakthrough_relation_receipts WHERE event_id=?", (event_id,),
            )
            if previous is not None:
                if previous["intent"] != _encode(intent):
                    raise ValueError("direct breakthrough relation intent conflict")
                return previous
            source, target = intent["source_id"], intent["target_id"]
            kind = intent["kind"]
            table = "partner" if kind == "partner" else "mentor"
            left = player.query_one(f"SELECT * FROM {table} WHERE user_id=?", (source,)) or {}
            right = player.query_one(f"SELECT * FROM {table} WHERE user_id=?", (target,)) or {}
            valid = str(left.get("bind_time") or "") == intent["bind_time"]
            payload = dict(intent)
            if kind == "partner":
                valid = valid and str(left.get("partner_id")) == target and str(right.get("partner_id")) == source
                valid = valid and str(right.get("bind_time") or "") == intent["target_bind_time"]
            else:
                children = json.loads(right.get("apprentice_ids") or "[]")
                count = as_int_like(left.get("breakthrough_reward_count"))
                valid = valid and str(left.get("mentor_id")) == target and source in [str(v) for v in children]
                valid = valid and 0 <= count < int(intent["reward_limit"])
                payload["reward_count"] = count + 1
            status = "prepared" if valid and not skip else "skipped"
            if status == "prepared" and kind == "mentor":
                player.execute(
                    "UPDATE mentor SET breakthrough_reward_count=? WHERE user_id=?",
                    (payload["reward_count"], source),
                )
            player.execute(
                "INSERT INTO direct_breakthrough_relation_receipts(event_id,intent,payload,status) VALUES(?,?,?,?)",
                (event_id, _encode(intent), _encode(payload), status),
            )
            return {"payload": _encode(payload), "status": status}

    def _grant(self, game, event_id, payload):
        target = payload["target_id"]
        row = game.query_one("SELECT rowid AS _rowid,* FROM user_xiuxian WHERE user_id=? ORDER BY rowid LIMIT 1", (target,))
        if row is None:
            raise RuntimeError("direct breakthrough reward recipient unavailable")
        reward = int(payload["reward_exp"])
        new_exp = as_int_like(row["exp"]) + reward
        if payload["kind"] == "mentor" and new_exp > min(int(payload["max_exp"]), int(self.cap_for(row))):
            raise RuntimeError("direct breakthrough reward deferred: mentor exp cap")
        power = self.power_for(dict(row), new_exp)
        game.execute(
            "UPDATE user_xiuxian SET exp=CAST(COALESCE(exp,0) AS REAL)+CAST(? AS REAL),power=? WHERE rowid=?",
            (str(reward), str(power), row["_rowid"]),
        )
        game.execute(
            "INSERT INTO direct_breakthrough_relation_rewards(event_id,payload) VALUES(?,?)",
            (event_id, _encode(payload)),
        )

    def _finalize(self, event_id, payload):
        with DatabaseUnitOfWork(self.player_database, immediate=True) as player:
            receipt = player.query_one(
                "SELECT status,payload FROM direct_breakthrough_relation_receipts WHERE event_id=?", (event_id,),
            )
            if receipt is None or receipt["payload"] != _encode(payload) or receipt["status"] == "skipped":
                raise RuntimeError("direct breakthrough reward reservation mismatch")
            if receipt["status"] == "applied":
                return
            if payload["kind"] == "mentor":
                count = f'{payload["reward_count"]}/{payload["reward_limit"]}'
                for user_id, related_id, key, description in (
                    (payload["target_id"], payload["source_id"], "师父突破返修", payload["target_description"]),
                    (payload["source_id"], payload["target_id"], "徒弟突破回馈", payload["source_description"]),
                ):
                    player.execute(
                        f'INSERT INTO statistics(user_id,"{key}") VALUES(?,?) '
                        f'ON CONFLICT(user_id) DO UPDATE SET "{key}"=COALESCE("{key}",0)+excluded."{key}"',
                        (user_id, str(payload["reward_exp"])),
                    )
                    row = player.query_one("SELECT mentor_history FROM mentor WHERE user_id=?", (user_id,))
                    history = json.loads((row or {}).get("mentor_history") or "[]")
                    if not isinstance(history, list):
                        raise ValueError("mentor_history must be a list")
                    history.append({
                        "time": payload["occurred_at"], "type": "breakthrough_reward",
                        "related_id": related_id, "description": f"{description}（{count}）",
                    })
                    player.execute(
                        "INSERT INTO mentor(user_id,mentor_history) VALUES(?,?) "
                        "ON CONFLICT(user_id) DO UPDATE SET mentor_history=excluded.mentor_history",
                        (user_id, _encode(history[-int(payload["history_limit"]):])),
                    )
            # Never restore the old count here: the relationship may have changed.
            player.execute("UPDATE direct_breakthrough_relation_receipts SET status='applied' WHERE event_id=?", (event_id,))

    def settle(self, operation_id, event_id, intent, *, skip=False):
        if not self.game_database.is_file() or not self.player_database.is_file():
            raise RuntimeError("direct breakthrough relation databases unavailable")
        if intent["kind"] not in {"partner", "mentor"} or int(intent["reward_exp"]) <= 0 or intent["source_id"] == intent["target_id"]:
            raise ValueError("invalid direct breakthrough reward")
        if event_id != f"direct-breakthrough:{operation_id}:effects:{intent['kind']}":
            raise ValueError("direct breakthrough reward event ID mismatch")
        # All legacy relation writers take the game lock before touching player data.
        with DatabaseUnitOfWork(self.game_database, immediate=True) as game:
            self._check_schema(game, {"direct_breakthrough_relation_rewards": ("event_id", "payload")})
            root = game.query_one(
                "SELECT p.payload FROM direct_breakthrough_plans p JOIN direct_breakthrough_operations o "
                "ON o.operation_id=p.operation_id WHERE p.operation_id=? AND o.user_id=? AND o.outcome='success'",
                (str(operation_id), intent["source_id"]),
            )
            if root is None or intent not in json.loads(root["payload"])["relations"]:
                raise ValueError("direct breakthrough reward has no committed root intent")
            prepared = self._prepare(event_id, intent, skip=skip)
            if prepared["status"] == "skipped":
                return None
            payload = json.loads(prepared["payload"])
            previous = game.query_one("SELECT payload FROM direct_breakthrough_relation_rewards WHERE event_id=?", (event_id,))
            if previous is None:
                self._grant(game, event_id, payload)
            elif previous["payload"] != _encode(payload):
                raise ValueError("direct breakthrough game reward conflict")
        self._finalize(event_id, payload)
        return payload


__all__ = ["DirectBreakthroughRelations"]
