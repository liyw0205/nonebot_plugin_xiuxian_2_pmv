from __future__ import annotations

from ...core.numeric import as_int_like, sqlite_int_compare, bind_sqlite_param, require_exact_integer
from ...core.errors import DomainError
from pathlib import Path
from datetime import date
from typing import Any, Protocol

import json

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.clock import SystemClock
from .schema import SCHEMA_MISSING, table_ready
from .state_repository import ArenaStateRepository


# Columns each request actually reads; startup migrations own the tables.
_SETTLEMENT_COLUMNS = frozenset(
    {
        "score", "total_wins", "total_losses", "win_streak", "max_win_streak", "rank",
        "daily_challenges_used", "daily_extra_challenges", "last_challenge_time",
    }
)
_TICKET_ARENA_COLUMNS = frozenset({"daily_challenges_used", "daily_extra_challenges"})
_PURCHASE_ARENA_COLUMNS = frozenset({"honor_points", "weekly_purchases"})
_CHALLENGE_PURCHASE_ARENA_COLUMNS = frozenset(
    {"daily_challenge_buys", "daily_extra_challenges", "last_buy_date"}
)
_SETTLEMENT_RECEIPT_COLUMNS = frozenset({"challenger_id", "payload", "result_json"})
_TICKET_RECEIPT_COLUMNS = frozenset(
    {"payload", "used_tickets", "item_remaining", "challenges_used", "challenges_remaining", "challenge_cap"}
)
_PURCHASE_RECEIPT_COLUMNS = frozenset({"payload", "status", "result_json"})
_CHALLENGE_PURCHASE_RECEIPT_COLUMNS = frozenset({"payload", "amount", "cost", "stone", "bought", "extra"})


class _ArenaSettlementWriteRejected(Exception):
    def __init__(self, result: dict[str, Any]) -> None:
        self.result = result


class _ArenaTicketWriteRejected(Exception):
    def __init__(self, result: dict[str, Any]) -> None:
        self.result = result


class _ArenaChallengePurchaseWriteRejected(Exception):
    def __init__(self, result: dict[str, Any]) -> None:
        self.result = result


class _ArenaPurchaseWriteRejected(Exception):
    def __init__(self, result: dict[str, Any]) -> None:
        self.result = result


class _SettlementProjections:
    """Check each settlement write now and again after the receipt write."""

    def __init__(self, uow: DatabaseUnitOfWork) -> None:
        self.uow = uow
        self.rows: list[tuple[str, tuple, dict[str, Any], str, str]] = []

    @staticmethod
    def _matches(row: Any, expected: dict[str, Any]) -> bool:
        if row is None:
            return False
        for key, value in expected.items():
            if key not in row:
                return False
            if isinstance(value, int):
                try:
                    if int(row[key]) != value:
                        return False
                except (TypeError, ValueError):
                    return False
            elif str(row[key]) != str(value):
                return False
        return True

    def reject(self, reason: str, subject: str) -> None:
        raise _ArenaSettlementWriteRejected({
            "status": "state_changed",
            "reason": reason,
            "subject": subject,
        })

    def affected(self, cursor: Any, reason: str, subject: str) -> None:
        if cursor.rowcount != 1:
            self.reject(reason, subject)

    def expect(self, sql: str, params: tuple, expected: dict[str, Any], reason: str, subject: str) -> None:
        item = (sql, params, expected, reason, subject)
        self.rows.append(item)
        self.verify_one(item)

    def verify_one(self, item: tuple[str, tuple, dict[str, Any], str, str]) -> None:
        sql, params, expected, reason, subject = item
        if not self._matches(self.uow.query_one(sql, params), expected):
            self.reject(reason, subject)

    def verify(self) -> None:
        for item in self.rows:
            self.verify_one(item)


class ArenaRepository(Protocol):
    def purchase(self, *args: Any, **kwargs: Any) -> Any: ...
    def purchase_result(self, *args: Any, **kwargs: Any) -> Any: ...
    def purchase_challenges(self, *args: Any, **kwargs: Any) -> Any: ...
    def settle(self, *args: Any, **kwargs: Any) -> Any: ...
    def use_challenge_ticket(self, *args: Any, **kwargs: Any) -> Any: ...
    def settlement_result(self, *args: Any, **kwargs: Any) -> Any: ...


class LegacyArenaRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def _services(self):
        from ...compatibility.legacy_arena_transactions import (
            ArenaPurchaseService,
            ArenaChallengePurchaseService,
            ArenaChallengeSettlementService,
        )

        return ArenaPurchaseService(self.game_database, self.player_database), ArenaChallengePurchaseService(self.game_database, self.player_database), ArenaChallengeSettlementService(self.game_database, self.player_database)

    def purchase(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[0].purchase(*args, **kwargs)

    def purchase_challenges(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[1].purchase(*args, **kwargs)

    def settle(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[2].settle(*args, **kwargs)

    def settlement_result(self, operation_id: str, challenger_id: str) -> Any:
        from ...compatibility.legacy_arena_transactions import ArenaChallengeSettlementService
        return ArenaChallengeSettlementService(self.game_database, self.player_database).get_result(operation_id, challenger_id)


class ArenaChallengePurchaseSqlRepository(LegacyArenaRepository):
    def __init__(self, game_database: str | Path, player_database: str | Path, *, clock=None) -> None:
        super().__init__(game_database, player_database)
        self.clock = clock or SystemClock()

    def _require_databases(self) -> None:
        if not all(Path(path).is_file() for path in (self.game_database, self.player_database)):
            raise FileNotFoundError("arena databases are unavailable")

    @staticmethod
    def _replay_result(encoded: Any) -> dict[str, Any]:
        try:
            result = json.loads(encoded)
        except (TypeError, ValueError):
            return {"status": "operation_conflict"}
        if not isinstance(result, dict) or not isinstance(result.get("status"), str) or not result["status"]:
            return {"status": "operation_conflict"}
        if result["status"] in {"applied", "duplicate"}:
            result["status"] = "duplicate"
        return result

    def settlement_result(self, operation_id: str, challenger_id: str) -> dict[str, Any] | None:
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            if not table_ready(uow, "arena_challenge_settlement_operations", _SETTLEMENT_RECEIPT_COLUMNS):
                return None
            row = uow.query_one("SELECT challenger_id,payload,result_json FROM arena_challenge_settlement_operations WHERE operation_id=?", (str(operation_id),))
        if row is None:
            return None
        if str(row["challenger_id"]) != str(challenger_id):
            return {"status": "operation_conflict"}
        return self._replay_result(row["result_json"])

    def settle(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        try:
            return self._settle(*args, **kwargs)
        except _ArenaSettlementWriteRejected as rejected:
            return rejected.result

    def _settle(self, operation_id, challenger_id, opponent_id, outcome, challenge_cap, stamina_cost, challenged_at, expected_challenger_arena, expected_opponent_arena, expected_challenger_player, expected_opponent_player, final_challenger_hp, final_challenger_mp, final_opponent_hp, final_opponent_mp, win_points, lose_points, no_match_points) -> dict[str, Any]:
        self._require_databases()
        operation_id, challenger_id = str(operation_id).strip(), str(challenger_id)
        opponent_id = "" if opponent_id is None else str(opponent_id)
        outcome, challenged_at = str(outcome), str(challenged_at)
        challenge_cap, stamina_cost = int(challenge_cap), int(stamina_cost)
        win_points, lose_points, no_match_points = map(int, (win_points, lose_points, no_match_points))
        challenger = {key: (str(expected_challenger_arena.get(key, "") or "") if key in {"rank", "last_challenge_time"} else int(expected_challenger_arena.get(key, 0) or 0)) for key in ("score", "total_wins", "total_losses", "win_streak", "max_win_streak", "rank", "daily_challenges_used", "daily_extra_challenges", "last_challenge_time")}
        opponent = None if expected_opponent_arena is None else {key: (str(expected_opponent_arena.get(key, "") or "") if key == "rank" else int(expected_opponent_arena.get(key, 0) or 0)) for key in ("score", "total_wins", "total_losses", "win_streak", "max_win_streak", "rank")}
        challenger_player = {"hp": as_int_like(expected_challenger_player.get("hp", 0) or 0), "mp": as_int_like(expected_challenger_player.get("mp", 0) or 0), "user_stamina": int(expected_challenger_player.get("user_stamina", 0) or 0)}
        opponent_player = None if expected_opponent_player is None else {"hp": as_int_like(expected_opponent_player.get("hp", 0) or 0), "mp": as_int_like(expected_opponent_player.get("mp", 0) or 0)}
        final_challenger_hp, final_challenger_mp = max(1, int(final_challenger_hp)), max(1, int(final_challenger_mp))
        final_opponent_hp = None if final_opponent_hp is None else max(1, int(final_opponent_hp))
        final_opponent_mp = None if final_opponent_mp is None else max(1, int(final_opponent_mp))
        if not operation_id or not challenger_id or outcome not in {"win", "loss", "draw", "no_match"} or challenge_cap < 0 or stamina_cost < 0:
            raise ValueError("valid arena settlement is required")
        if outcome != "no_match" and (not opponent_id or opponent is None or opponent_player is None):
            raise ValueError("opponent snapshot is required")
        payload = json.dumps([challenger_id, opponent_id, outcome, challenge_cap, stamina_cost, challenged_at, challenger, opponent, challenger_player, opponent_player, final_challenger_hp, final_challenger_mp, final_opponent_hp, final_opponent_mp, win_points, lose_points, no_match_points], ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        def result(status, score=None, rank=None, opponent_score=None, used=None, stamina=None, *, reason="", subject=""):
            return {"status": status, "reason": reason, "subject": subject, "outcome": outcome, "challenger_score": int(challenger["score"] if score is None else score), "challenger_rank": str(challenger["rank"] if rank is None else rank), "opponent_score": None if opponent_score is None else int(opponent_score), "score_delta": win_points if outcome == "win" else no_match_points if outcome == "no_match" else 0, "used": int(challenger["daily_challenges_used"] if used is None else used), "remaining": max(0, challenge_cap - int(challenger["daily_challenges_used"] if used is None else used)), "stamina": int(challenger_player["user_stamina"] if stamina is None else stamina), "challenged_at": challenged_at}
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            if not table_ready(uow, "arena_challenge_settlement_operations", _SETTLEMENT_RECEIPT_COLUMNS) or not table_ready(
                uow, "arena", _SETTLEMENT_COLUMNS, prefix="player_data."
            ):
                return result(SCHEMA_MISSING)
            writes = _SettlementProjections(uow)
            old = uow.query_one("SELECT challenger_id,payload,result_json FROM arena_challenge_settlement_operations WHERE operation_id=?", (operation_id,))
            if old:
                if str(old["challenger_id"]) != challenger_id or str(old["payload"]) != payload:
                    return result("operation_conflict")
                return self._replay_result(old["result_json"])
            arena = uow.query_one("SELECT score,total_wins,total_losses,win_streak,max_win_streak,rank,daily_challenges_used,daily_extra_challenges,last_challenge_time FROM player_data.arena WHERE user_id=?", (challenger_id,))
            player = uow.query_one("SELECT COALESCE(hp,0) AS hp,COALESCE(mp,0) AS mp,COALESCE(user_stamina,0) AS user_stamina FROM user_xiuxian WHERE user_id=?", (challenger_id,))
            actual = dict(arena) if arena else None
            reason = self._battle_reason(actual, player, challenger, challenger_player)
            if reason:
                return self._record_settlement(uow, operation_id, challenger_id, payload, result("state_changed", reason=reason, subject="challenger"), writes)
            if opponent is not None:
                if opponent_player is None:
                    return self._record_settlement(uow, operation_id, challenger_id, payload, result("state_changed", reason="user_missing", subject="opponent"), writes)
                opponent_actual = uow.query_one("SELECT score,total_wins,total_losses,win_streak,max_win_streak,rank FROM player_data.arena WHERE user_id=?", (opponent_id,))
                opponent_vitals = uow.query_one("SELECT COALESCE(hp,0) AS hp,COALESCE(mp,0) AS mp FROM user_xiuxian WHERE user_id=?", (opponent_id,))
                reason = self._battle_reason(opponent_actual, opponent_vitals, opponent, opponent_player)
                if reason:
                    return self._record_settlement(uow, operation_id, challenger_id, payload, result("state_changed", reason=reason, subject="opponent"), writes)
            if challenger["daily_challenges_used"] >= challenge_cap: return self._record_settlement(uow, operation_id, challenger_id, payload, result("limit_reached"), writes)
            if challenger_player["user_stamina"] < stamina_cost: return self._record_settlement(uow, operation_id, challenger_id, payload, result("stamina_insufficient"), writes)
            challenger_new = dict(challenger); opponent_new = None if opponent is None else dict(opponent)
            if outcome == "win":
                challenger_new["score"] += win_points; challenger_new["total_wins"] += 1; challenger_new["win_streak"] += 1; challenger_new["max_win_streak"] = max(challenger_new["max_win_streak"], challenger_new["win_streak"]); opponent_new["score"] = max(0, opponent_new["score"] - lose_points); opponent_new["total_losses"] += 1; opponent_new["win_streak"] = 0
            elif outcome in {"loss", "draw"}: challenger_new["total_losses"] += 1; challenger_new["win_streak"] = 0
            else: challenger_new["score"] += no_match_points; challenger_new["total_losses"] += 1; challenger_new["win_streak"] = 0
            challenger_new["rank"] = self._rank(challenger_new["score"]); challenger_new["daily_challenges_used"] += 1; challenger_new["last_challenge_time"] = challenged_at
            if opponent_new is not None: opponent_new["rank"] = self._rank(opponent_new["score"])
            challenger_update = uow.execute("UPDATE player_data.arena SET score=?,total_wins=?,total_losses=?,win_streak=?,max_win_streak=?,rank=?,daily_challenges_used=?,last_challenge_time=? WHERE user_id=?", tuple(challenger_new[key] for key in ("score", "total_wins", "total_losses", "win_streak", "max_win_streak", "rank", "daily_challenges_used", "last_challenge_time")) + (challenger_id,))
            challenger_reason = "arena_settlement_challenger_write_rejected"
            writes.affected(challenger_update, challenger_reason, challenger_id)
            writes.expect(
                "SELECT score,total_wins,total_losses,win_streak,max_win_streak,rank,daily_challenges_used,daily_extra_challenges,last_challenge_time FROM player_data.arena WHERE user_id=?",
                (challenger_id,), challenger_new, challenger_reason, challenger_id,
            )
            if opponent_new is not None:
                opponent_update = uow.execute("UPDATE player_data.arena SET score=?,total_wins=?,total_losses=?,win_streak=?,max_win_streak=?,rank=? WHERE user_id=?", tuple(opponent_new[key] for key in ("score", "total_wins", "total_losses", "win_streak", "max_win_streak", "rank")) + (opponent_id,))
                opponent_reason = "arena_settlement_opponent_write_rejected"
                writes.affected(opponent_update, opponent_reason, opponent_id)
                writes.expect(
                    "SELECT score,total_wins,total_losses,win_streak,max_win_streak,rank FROM player_data.arena WHERE user_id=?",
                    (opponent_id,), opponent_new, opponent_reason, opponent_id,
                )
            stamina = challenger_player["user_stamina"] - stamina_cost; challenger_vitals_update = uow.execute("UPDATE user_xiuxian SET hp=?,mp=?,user_stamina=? WHERE user_id=? AND xiuxian_int_cmp(COALESCE(hp,0),?)=0 AND xiuxian_int_cmp(COALESCE(mp,0),?)=0 AND COALESCE(user_stamina,0)=?", (final_challenger_hp, final_challenger_mp, stamina, challenger_id, challenger_player["hp"], challenger_player["mp"], challenger_player["user_stamina"]))
            challenger_vitals_reason = "arena_settlement_challenger_vitals_write_rejected"
            writes.affected(challenger_vitals_update, challenger_vitals_reason, challenger_id)
            writes.expect(
                "SELECT hp,mp,user_stamina FROM user_xiuxian WHERE user_id=?",
                (challenger_id,), {"hp": final_challenger_hp, "mp": final_challenger_mp, "user_stamina": stamina},
                challenger_vitals_reason, challenger_id,
            )
            if opponent_player is not None:
                opponent_vitals_update = uow.execute("UPDATE user_xiuxian SET hp=?,mp=? WHERE user_id=? AND xiuxian_int_cmp(COALESCE(hp,0),?)=0 AND xiuxian_int_cmp(COALESCE(mp,0),?)=0", (final_opponent_hp, final_opponent_mp, opponent_id, opponent_player["hp"], opponent_player["mp"]))
                opponent_vitals_reason = "arena_settlement_opponent_vitals_write_rejected"
                writes.affected(opponent_vitals_update, opponent_vitals_reason, opponent_id)
                writes.expect(
                    "SELECT hp,mp FROM user_xiuxian WHERE user_id=?",
                    (opponent_id,), {"hp": final_opponent_hp, "mp": final_opponent_mp},
                    opponent_vitals_reason, opponent_id,
                )
            final = result("applied", challenger_new["score"], challenger_new["rank"], None if opponent_new is None else opponent_new["score"], challenger_new["daily_challenges_used"], stamina); return self._record_settlement(uow, operation_id, challenger_id, payload, final, writes)

    @staticmethod
    def _battle_reason(actual, player, expected, vitals):
        if actual is None or player is None:
            return "user_missing"
        for key, value in expected.items():
            current = actual.get(key) or 0
            matches = str(actual.get(key) or "") == value if key in {"rank", "last_challenge_time"} else sqlite_int_compare(current, value) == 0
            if not matches:
                if key == "score": return "score_changed"
                if key == "rank": return "rank_changed"
                if key == "last_challenge_time": return "cooldown_changed"
                if key in {"daily_challenges_used", "daily_extra_challenges"}: return "challenge_count_changed"
                return "record_changed"
        for key, value in vitals.items():
            if as_int_like(player[key]) != value:
                return {"hp": "hp_changed", "mp": "mp_changed", "user_stamina": "stamina_changed"}[key]
        return ""

    @staticmethod
    def _rank(score): return "王者" if score >= 3200 else "钻石" if score >= 2700 else "铂金" if score >= 2300 else "黄金" if score >= 1900 else "白银" if score >= 1500 else "青铜"
    @staticmethod
    def _record_settlement(uow, operation_id, challenger_id, payload, result, writes):
        encoded = json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        receipt_reason = "arena_settlement_receipt_write_rejected"
        receipt = uow.execute(
            "INSERT INTO arena_challenge_settlement_operations(operation_id,challenger_id,payload,result_json) VALUES(?,?,?,?)",
            (operation_id, challenger_id, payload, encoded),
        )
        writes.affected(receipt, receipt_reason, operation_id)
        writes.expect(
            "SELECT challenger_id,payload,result_json FROM arena_challenge_settlement_operations WHERE operation_id=?",
            (operation_id,), {"challenger_id": challenger_id, "payload": payload, "result_json": encoded},
            receipt_reason, operation_id,
        )
        writes.verify()
        return result
    def use_challenge_ticket(self, operation_id, user_id, item_id, requested_count, expected_item_count, expected_challenges_used, expected_extra_challenges, challenge_cap) -> dict[str, Any]:
        try:
            return self._use_challenge_ticket(operation_id, user_id, item_id, requested_count, expected_item_count, expected_challenges_used, expected_extra_challenges, challenge_cap)
        except _ArenaTicketWriteRejected as rejected:
            return rejected.result

    def _use_challenge_ticket(self, operation_id, user_id, item_id, requested_count, expected_item_count, expected_challenges_used, expected_extra_challenges, challenge_cap) -> dict[str, Any]:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        item_id, requested_count, expected_item_count, expected_challenges_used, expected_extra_challenges, challenge_cap = map(
            int, (item_id, requested_count, expected_item_count, expected_challenges_used, expected_extra_challenges, challenge_cap)
        )
        payload = json.dumps([user_id, item_id, requested_count, challenge_cap], ensure_ascii=True, separators=(",", ":"))
        if not operation_id or requested_count <= 0 or min(expected_item_count, expected_challenges_used, expected_extra_challenges, challenge_cap) < 0:
            raise ValueError("valid arena ticket operation is required")
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            if not table_ready(uow, "arena_challenge_ticket_operations", _TICKET_RECEIPT_COLUMNS) or not table_ready(
                uow, "arena", _TICKET_ARENA_COLUMNS, prefix="player_data."
            ):
                return self._ticket_result(SCHEMA_MISSING, expected_item_count, expected_challenges_used, challenge_cap)
            old = uow.query_one(
                "SELECT payload,used_tickets,item_remaining,challenges_used,challenges_remaining,challenge_cap "
                "FROM arena_challenge_ticket_operations WHERE operation_id=?", (operation_id,)
            )
            if old:
                saved = json.loads(old["payload"])
                old_payload = saved.get("_payload") if isinstance(saved, dict) else str(old["payload"])
                if old_payload != payload:
                    return self._ticket_result("operation_conflict", expected_item_count, expected_challenges_used, challenge_cap)
                if isinstance(saved, dict):
                    return saved["_rejection"]
                return self._ticket_result(
                    "duplicate", int(old["item_remaining"]), int(old["challenges_used"]),
                    int(old["challenge_cap"]), int(old["used_tickets"]),
                )
            arena = uow.query_one(
                "SELECT COALESCE(daily_challenges_used,0) AS used,COALESCE(daily_extra_challenges,0) AS extra "
                "FROM player_data.arena WHERE user_id=?", (user_id,)
            )
            item = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num,COALESCE(bind_num,0) AS bind_num "
                "FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id)
            )
            if arena is None or sqlite_int_compare(arena["used"], expected_challenges_used) != 0 or sqlite_int_compare(arena["extra"], expected_extra_challenges) != 0:
                return self._record_ticket(uow, operation_id, payload, self._ticket_result(
                    "state_changed", expected_item_count, expected_challenges_used, challenge_cap,
                    reason="user_missing" if arena is None else "challenge_count_changed",
                ))
            if item is None or sqlite_int_compare(item["goods_num"], 0) <= 0:
                return self._record_ticket(uow, operation_id, payload, self._ticket_result(
                    "item_missing", expected_item_count, expected_challenges_used, challenge_cap,
                ))
            if sqlite_int_compare(item["goods_num"], expected_item_count) != 0:
                return self._record_ticket(uow, operation_id, payload, self._ticket_result(
                    "state_changed", expected_item_count, expected_challenges_used, challenge_cap, reason="item_changed",
                ))
            if expected_challenges_used <= 0:
                return self._record_ticket(uow, operation_id, payload, self._ticket_result(
                    "no_challenges_used", expected_item_count, expected_challenges_used, challenge_cap,
                ))
            used = min(requested_count, expected_item_count, expected_challenges_used)
            remaining = expected_item_count - used
            new_used = expected_challenges_used - used
            bound = min(max(0, int(item["bind_num"]) - used), remaining)
            result = self._ticket_result("applied", remaining, new_used, challenge_cap, used)
            with uow.savepoint("arena_ticket"):
                changed = uow.execute(
                    "UPDATE back SET goods_num=?,bind_num=? WHERE user_id=? AND goods_id=? AND COALESCE(goods_num,0)=?",
                    (remaining, bound, user_id, item_id, expected_item_count),
                )
                saved_item = uow.query_one("SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
                if changed.rowcount != 1 or not self._ticket_values_match(saved_item, {"goods_num": remaining, "bind_num": bound}):
                    raise _ArenaTicketWriteRejected(self._ticket_result(
                        "state_changed", expected_item_count, expected_challenges_used, challenge_cap,
                        reason="arena_ticket_item_write_rejected", subject="item",
                    ))
                changed = uow.execute(
                    "UPDATE player_data.arena SET daily_challenges_used=? WHERE user_id=? "
                    "AND xiuxian_int_cmp(COALESCE(daily_challenges_used,0),?)=0 "
                    "AND xiuxian_int_cmp(COALESCE(daily_extra_challenges,0),?)=0",
                    (new_used, user_id, expected_challenges_used, expected_extra_challenges),
                )
                saved_arena = uow.query_one("SELECT daily_challenges_used,daily_extra_challenges FROM player_data.arena WHERE user_id=?", (user_id,))
                if changed.rowcount != 1 or not self._ticket_values_match(saved_arena, {"daily_challenges_used": new_used, "daily_extra_challenges": expected_extra_challenges}):
                    raise _ArenaTicketWriteRejected(self._ticket_result(
                        "state_changed", expected_item_count, expected_challenges_used, challenge_cap,
                        reason="arena_ticket_count_write_rejected", subject="arena",
                    ))
                self._record_ticket(uow, operation_id, payload, result)
                saved_item = uow.query_one("SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
                saved_arena = uow.query_one("SELECT daily_challenges_used,daily_extra_challenges FROM player_data.arena WHERE user_id=?", (user_id,))
                if not self._ticket_values_match(saved_item, {"goods_num": remaining, "bind_num": bound}):
                    raise _ArenaTicketWriteRejected(self._ticket_result(
                        "state_changed", expected_item_count, expected_challenges_used, challenge_cap,
                        reason="arena_ticket_item_write_rejected", subject="item",
                    ))
                if not self._ticket_values_match(saved_arena, {"daily_challenges_used": new_used, "daily_extra_challenges": expected_extra_challenges}):
                    raise _ArenaTicketWriteRejected(self._ticket_result(
                        "state_changed", expected_item_count, expected_challenges_used, challenge_cap,
                        reason="arena_ticket_count_write_rejected", subject="arena",
                    ))
            return result

    @staticmethod
    def _ticket_values_match(row, expected):
        if row is None:
            return False
        return all(key in row and sqlite_int_compare(row[key], value) == 0 for key, value in expected.items())

    @staticmethod
    def _ticket_result(status, item_remaining, challenges_used, challenge_cap, used_tickets=0, *, reason="", subject=""):
        return {
            "status": status, "reason": reason, "subject": subject, "used_tickets": used_tickets,
            "item_remaining": item_remaining, "challenges_used": challenges_used,
            "challenges_remaining": max(0, challenge_cap - challenges_used), "challenge_cap": challenge_cap,
        }

    def _record_ticket(self, uow, operation_id, payload, result):
        if result["status"] not in {"applied", "duplicate"}:
            payload = json.dumps({"_payload": payload, "_rejection": result}, ensure_ascii=True, separators=(",", ":"))
        inserted = uow.execute(
            "INSERT INTO arena_challenge_ticket_operations(operation_id,payload,used_tickets,item_remaining,challenges_used,challenges_remaining,challenge_cap) "
            "VALUES(?,?,?,?,?,?,?)",
            (operation_id, payload, result["used_tickets"], result["item_remaining"], result["challenges_used"], result["challenges_remaining"], result["challenge_cap"]),
        )
        saved = uow.query_one(
            "SELECT payload,used_tickets,item_remaining,challenges_used,challenges_remaining,challenge_cap "
            "FROM arena_challenge_ticket_operations WHERE operation_id=?", (operation_id,)
        )
        matches = saved is not None and str(saved["payload"]) == payload and all(
            sqlite_int_compare(saved[key], result[key]) == 0
            for key in ("used_tickets", "item_remaining", "challenges_used", "challenges_remaining", "challenge_cap")
        )
        if inserted.rowcount != 1 or not matches:
            raise _ArenaTicketWriteRejected(self._ticket_result(
                "state_changed", result["item_remaining"], result["challenges_used"], result["challenge_cap"],
                result["used_tickets"], reason="arena_ticket_receipt_write_rejected", subject="receipt",
            ))
        return result
    def purchase_result(self, operation_id, user_id, item_id, quantity) -> dict[str, Any] | None:
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            if not table_ready(uow, "arena_purchase_operations", _PURCHASE_RECEIPT_COLUMNS):
                return None
            row = uow.query_one(
                "SELECT payload,status,result_json FROM arena_purchase_operations WHERE operation_id=?",
                (str(operation_id),),
            )
        if row is None:
            return None
        try:
            payload = json.loads(row["payload"])
            if not isinstance(payload, list) or len(payload) not in {9, 10}:
                return {"status": "operation_conflict"}
            if (str(payload[0]), int(payload[1]), int(payload[4])) != (str(user_id), int(item_id), int(quantity)):
                return {"status": "operation_conflict"}
        except (TypeError, ValueError, OverflowError):
            return {"status": "operation_conflict"}
        return self._purchase_replay(row)

    @classmethod
    def _purchase_replay(cls, row):
        if row["status"] == "needs_reconcile" or row["result_json"] is None:
            return {"status": "needs_reconcile"}
        result = cls._replay_result(row["result_json"])
        expected_status = "duplicate" if row["status"] in {"applied", "duplicate"} else row["status"]
        if result["status"] != expected_status:
            return {"status": "operation_conflict"}
        return result

    def purchase(self, operation_id, user_id, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, expected_honor, expected_weekly_purchases, max_goods_num, bind_flag=1, today=None, *, clamp_quantity=False) -> dict[str, Any]:
        try:
            return self._purchase(
                operation_id, user_id, item_id, item_name, item_type, quantity, unit_cost,
                weekly_limit, expected_honor, expected_weekly_purchases, max_goods_num,
                bind_flag, today, clamp_quantity=clamp_quantity,
            )
        except _ArenaPurchaseWriteRejected as rejected:
            return rejected.result

    @staticmethod
    def _purchase_values_match(row, expected: dict[str, Any]) -> bool:
        if row is None:
            return False
        for key, value in expected.items():
            try:
                if isinstance(value, int):
                    if as_int_like(row[key]) != value:
                        return False
                elif str(row[key] or "") != str(value):
                    return False
            except (KeyError, TypeError, ValueError):
                return False
        return True

    @staticmethod
    def _purchase_rejected(quantity, honor, purchased, inventory, reason, subject):
        return {
            **ArenaChallengePurchaseSqlRepository._purchase_result("state_changed", quantity, 0, honor, purchased, inventory),
            "reason": reason,
            "subject": subject,
        }

    def _purchase(self, operation_id, user_id, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, expected_honor, expected_weekly_purchases, max_goods_num, bind_flag=1, today=None, *, clamp_quantity=False) -> dict[str, Any]:
        self._require_databases()
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        item_name, item_type = str(item_name), str(item_type)
        item_id, quantity, unit_cost, weekly_limit, expected_honor, max_goods_num = map(
            int, (item_id, quantity, unit_cost, weekly_limit, expected_honor, max_goods_num),
        )
        if not operation_id or quantity <= 0 or min(item_id, unit_cost, weekly_limit, expected_honor, max_goods_num) < 0:
            raise ValueError("valid arena purchase is required")
        payload_values = [user_id, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, max_goods_num, int(bind_flag)]
        if clamp_quantity:
            payload_values.append(True)
        payload = json.dumps(payload_values, ensure_ascii=True, sort_keys=True)
        today = today or self.clock.now().date()
        today_key = today.isoformat() if hasattr(today, "isoformat") else str(today)
        business_date = date.fromisoformat(today_key)

        def record(uow, status, *, cost=0, honor=expected_honor, purchased=0, inventory=0, reason=""):
            result = self._purchase_result(status, quantity, cost, honor, purchased, inventory)
            result["item_name"] = item_name
            result["reason"] = reason
            return self._record_purchase(uow, operation_id, payload, result)

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            if not table_ready(uow, "arena_purchase_operations", _PURCHASE_RECEIPT_COLUMNS) or not table_ready(
                uow, "arena", _PURCHASE_ARENA_COLUMNS, prefix="player_data."
            ):
                return self._purchase_result(SCHEMA_MISSING, quantity, 0, expected_honor, 0, 0)
            old = uow.query_one("SELECT payload,status,result_json FROM arena_purchase_operations WHERE operation_id=?", (operation_id,))
            if old:
                if str(old["payload"]) != payload:
                    return {**self._purchase_result("state_changed", quantity, 0, expected_honor, 0, 0), "reason": "operation_conflict"}
                return self._purchase_replay(old)
            user = uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,))
            arena = uow.query_one("SELECT COALESCE(honor_points,0) AS honor,COALESCE(weekly_purchases,'{}') AS weekly FROM player_data.arena WHERE user_id=?", (user_id,))
            if user is None or arena is None:
                return record(uow, "user_missing")
            weekly, _ = ArenaStateRepository._weekly(arena["weekly"], business_date)
            if require_exact_integer(arena["honor"], field="arena_honor", subject=user_id) != expected_honor:
                return record(uow, "state_changed", reason="honor_changed")
            if weekly.get(str(item_id), 0) != expected_weekly_purchases.get(str(item_id), 0):
                return record(uow, "state_changed", reason="weekly_purchase_changed")
            purchased = int(weekly.get(str(item_id), 0) or 0)
            if clamp_quantity:
                quantity = min(quantity, max(0, weekly_limit - purchased))
            if quantity <= 0 or purchased + quantity > weekly_limit:
                return record(uow, "limit_reached", purchased=purchased)
            cost = quantity * unit_cost
            if expected_honor < cost:
                return record(uow, "honor_insufficient", purchased=purchased)
            item = uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num,COALESCE(bind_num,0) AS bind_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
            previous_inventory = int(item["goods_num"]) if item else 0
            previous_bind = int(item["bind_num"]) if item else 0
            inventory = previous_inventory + quantity
            if inventory > max_goods_num:
                return record(uow, "inventory_full", purchased=purchased, inventory=previous_inventory)
            honor, purchased = expected_honor - cost, purchased + quantity
            weekly[str(item_id)] = purchased
            weekly_payload = json.dumps(weekly, ensure_ascii=True)
            expected_inventory = {"goods_num": inventory, "bind_num": previous_bind + (quantity if int(bind_flag) else 0)}
            expected_arena = {"honor_points": honor, "weekly_purchases": weekly_payload}
            result = self._purchase_result("applied", quantity, cost, honor, purchased, inventory)
            result["item_name"] = item_name
            with uow.savepoint("arena_purchase"):
                updated = uow.execute(
                    "UPDATE player_data.arena SET honor_points=xiuxian_int_store(?,'arena_honor','actor'),weekly_purchases=? WHERE user_id=? AND xiuxian_int_cmp(COALESCE(honor_points,0),?,1,'arena_honor','actor')=0",
                    (bind_sqlite_param(honor, exact=True), weekly_payload, user_id, bind_sqlite_param(expected_honor, exact=True)),
                )
                saved = uow.query_one("SELECT honor_points,weekly_purchases FROM player_data.arena WHERE user_id=?", (user_id,))
                if updated.rowcount != 1 or not self._purchase_values_match(saved, {"honor_points": honor}):
                    raise _ArenaPurchaseWriteRejected(self._purchase_rejected(quantity, expected_honor, purchased, previous_inventory, "arena_purchase_honor_write_rejected", user_id))
                if saved is None or str(saved["weekly_purchases"] or "") != weekly_payload:
                    raise _ArenaPurchaseWriteRejected(self._purchase_rejected(quantity, expected_honor, purchased, previous_inventory, "arena_purchase_weekly_write_rejected", user_id))
                changed = uow.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                    "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                    "goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,"
                    "goods_name=excluded.goods_name,goods_type=excluded.goods_type,update_time=excluded.update_time",
                    (user_id, item_id, item_name, item_type, quantity, today_key, today_key, quantity if int(bind_flag) else 0),
                )
                saved_item = uow.query_one("SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
                if changed.rowcount != 1 or not self._purchase_values_match(saved_item, expected_inventory):
                    raise _ArenaPurchaseWriteRejected(self._purchase_rejected(quantity, expected_honor, purchased, previous_inventory, "arena_purchase_inventory_write_rejected", f"{user_id}:{item_id}"))
                self._record_purchase(uow, operation_id, payload, result)
                saved = uow.query_one("SELECT honor_points,weekly_purchases FROM player_data.arena WHERE user_id=?", (user_id,))
                if not self._purchase_values_match(saved, expected_arena):
                    reason = "arena_purchase_honor_write_rejected" if not self._purchase_values_match(saved, {"honor_points": honor}) else "arena_purchase_weekly_write_rejected"
                    raise _ArenaPurchaseWriteRejected(self._purchase_rejected(quantity, expected_honor, purchased, previous_inventory, reason, user_id))
                if not self._purchase_values_match(uow.query_one("SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id)), expected_inventory):
                    raise _ArenaPurchaseWriteRejected(self._purchase_rejected(quantity, expected_honor, purchased, previous_inventory, "arena_purchase_inventory_write_rejected", f"{user_id}:{item_id}"))
            return result

    @staticmethod
    def _purchase_result(status, quantity, cost, honor_points, purchased, inventory):
        return {"status":status,"quantity":quantity,"cost":cost,"honor_points":honor_points,"purchased":purchased,"inventory":inventory}

    @staticmethod
    def _record_purchase(uow, operation_id, payload, result):
        encoded = json.dumps(result, ensure_ascii=True, sort_keys=True)
        inserted = uow.execute(
            "INSERT INTO arena_purchase_operations(operation_id,payload,quantity,cost,honor_points,purchased,inventory,status,result_json) VALUES(?,?,?,xiuxian_int_store(?,'stone','actor'),xiuxian_int_store(?,'arena_honor','actor'),?,?,?,?)",
            (operation_id, payload, result["quantity"], bind_sqlite_param(result["cost"], exact=True), bind_sqlite_param(result["honor_points"], exact=True), result["purchased"], result["inventory"], result["status"], encoded),
        )
        saved = uow.query_one("SELECT payload,quantity,cost,honor_points,purchased,inventory,status,result_json FROM arena_purchase_operations WHERE operation_id=?", (operation_id,))
        expected = {"payload": payload, "quantity": result["quantity"], "cost": result["cost"], "honor_points": result["honor_points"], "purchased": result["purchased"], "inventory": result["inventory"], "status": result["status"], "result_json": encoded}
        if inserted.rowcount != 1 or not ArenaChallengePurchaseSqlRepository._purchase_values_match(saved, expected):
            raise _ArenaPurchaseWriteRejected({**ArenaChallengePurchaseSqlRepository._purchase_result("state_changed", result["quantity"], 0, result["honor_points"], result["purchased"], result["inventory"]), "reason": "arena_purchase_receipt_write_rejected", "subject": "receipt"})
        return result
    def purchase_challenges(self, operation_id, user_id, amount, unit_cost, daily_limit, expected_stone, expected_bought, expected_extra, expected_last_buy_date, today=None) -> dict[str, Any]:
        try:
            return self._purchase_challenges(operation_id, user_id, amount, unit_cost, daily_limit, expected_stone, expected_bought, expected_extra, expected_last_buy_date, today)
        except _ArenaChallengePurchaseWriteRejected as rejected:
            return rejected.result

    def _purchase_challenges(self, operation_id, user_id, amount, unit_cost, daily_limit, expected_stone, expected_bought, expected_extra, expected_last_buy_date, today=None) -> dict[str, Any]:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        amount, unit_cost, daily_limit, expected_stone, expected_bought, expected_extra = map(int, (amount, unit_cost, daily_limit, expected_stone, expected_bought, expected_extra))
        if not operation_id or amount <= 0 or min(unit_cost, daily_limit, expected_stone, expected_bought, expected_extra) < 0:
            raise ValueError("valid arena challenge purchase is required")
        today = today or self.clock.now().date()
        payload = json.dumps([user_id, amount, unit_cost, daily_limit], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            if not table_ready(uow, "arena_challenge_purchase_operations", _CHALLENGE_PURCHASE_RECEIPT_COLUMNS) or not table_ready(
                uow, "arena", _CHALLENGE_PURCHASE_ARENA_COLUMNS, prefix="player_data."
            ):
                return self._result(SCHEMA_MISSING, 0, 0, expected_stone, expected_bought, expected_extra)
            old = uow.query_one("SELECT payload,amount,cost,stone,bought,extra FROM arena_challenge_purchase_operations WHERE operation_id=?", (operation_id,))
            if old:
                saved = json.loads(old["payload"])
                old_payload = saved.get("_payload") if isinstance(saved, dict) else str(old["payload"])
                if old_payload != payload:
                    return {**self._result("state_changed", 0, 0, expected_stone, expected_bought, expected_extra), "reason": "operation_conflict"}
                if isinstance(saved, dict):
                    return saved["_rejection"]
                return self._result("duplicate", int(old["amount"]), int(old["cost"]), as_int_like(old["stone"]), int(old["bought"]), int(old["extra"]))
            user = uow.query_one("SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            arena = uow.query_one("SELECT COALESCE(daily_challenge_buys,0) AS bought,COALESCE(daily_extra_challenges,0) AS extra,last_buy_date FROM player_data.arena WHERE user_id=?", (user_id,))
            if user is None or arena is None:
                return self._record_challenge_purchase(uow, operation_id, payload, self._result("state_changed", 0, 0, expected_stone, expected_bought, expected_extra, reason="user_missing"))
            current_date = str(arena["last_buy_date"] or "")
            today_str = today.isoformat() if hasattr(today, "isoformat") else str(today)
            stored_bought, stored_extra = as_int_like(arena["bought"]), as_int_like(arena["extra"])
            bought, extra = stored_bought, stored_extra
            if current_date != today_str:
                bought, extra = 0, 0
                expected_bought, expected_extra, expected_last_buy_date = 0, 0, today_str
            if (current_date == today_str and (sqlite_int_compare(arena["bought"], expected_bought) != 0 or sqlite_int_compare(arena["extra"], expected_extra) != 0)) or (as_int_like(user["stone"]), bought, extra, current_date if current_date == today_str else today_str) != (expected_stone, expected_bought, expected_extra, str(expected_last_buy_date)):
                return self._record_challenge_purchase(uow, operation_id, payload, self._result("state_changed", 0, 0, expected_stone, expected_bought, expected_extra, reason="stone_changed" if as_int_like(user["stone"]) != expected_stone else "buy_count_changed"))
            real_amount = min(amount, max(0, daily_limit - expected_bought)); cost = real_amount * unit_cost
            if real_amount == 0:
                return self._record_challenge_purchase(uow, operation_id, payload, self._result("limit_reached", 0, 0, expected_stone, expected_bought, expected_extra))
            if expected_stone < cost:
                return self._record_challenge_purchase(uow, operation_id, payload, self._result("stone_insufficient", 0, 0, expected_stone, expected_bought, expected_extra))
            new_stone, new_bought, new_extra = expected_stone - cost, expected_bought + real_amount, expected_extra + real_amount
            result = self._result("applied", real_amount, cost, new_stone, new_bought, new_extra)
            with uow.savepoint("arena_challenge_purchase"):
                changed = uow.execute(
                    "UPDATE user_xiuxian SET stone=xiuxian_int_store(?,'stone','actor') WHERE user_id=? "
                    "AND xiuxian_int_cmp(COALESCE(stone,0),?,1,'stone','actor')=0",
                    (new_stone, user_id, expected_stone),
                )
                saved_user = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,))
                if changed.rowcount != 1 or saved_user is None or as_int_like(saved_user["stone"]) != new_stone:
                    raise _ArenaChallengePurchaseWriteRejected(self._result(
                        "state_changed", 0, 0, expected_stone, expected_bought, expected_extra,
                        reason="arena_challenge_purchase_stone_write_rejected", subject="user",
                    ))
                changed = uow.execute(
                    "UPDATE player_data.arena SET daily_challenge_buys=?,daily_extra_challenges=?,last_buy_date=? "
                    "WHERE user_id=? AND xiuxian_int_cmp(COALESCE(daily_challenge_buys,0),?)=0 "
                    "AND xiuxian_int_cmp(COALESCE(daily_extra_challenges,0),?)=0 AND COALESCE(last_buy_date,'')=?",
                    (new_bought, new_extra, today_str, user_id, stored_bought, stored_extra, current_date),
                )
                saved_arena = uow.query_one("SELECT daily_challenge_buys,daily_extra_challenges,last_buy_date FROM player_data.arena WHERE user_id=?", (user_id,))
                if changed.rowcount != 1 or not self._challenge_purchase_arena_matches(saved_arena, new_bought, new_extra, today_str):
                    raise _ArenaChallengePurchaseWriteRejected(self._result(
                        "state_changed", 0, 0, expected_stone, expected_bought, expected_extra,
                        reason="arena_challenge_purchase_state_write_rejected", subject="arena",
                    ))
                self._record_challenge_purchase(uow, operation_id, payload, result)
                saved_user = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,))
                saved_arena = uow.query_one("SELECT daily_challenge_buys,daily_extra_challenges,last_buy_date FROM player_data.arena WHERE user_id=?", (user_id,))
                if saved_user is None or as_int_like(saved_user["stone"]) != new_stone:
                    raise _ArenaChallengePurchaseWriteRejected(self._result(
                        "state_changed", 0, 0, expected_stone, expected_bought, expected_extra,
                        reason="arena_challenge_purchase_stone_write_rejected", subject="user",
                    ))
                if not self._challenge_purchase_arena_matches(saved_arena, new_bought, new_extra, today_str):
                    raise _ArenaChallengePurchaseWriteRejected(self._result(
                        "state_changed", 0, 0, expected_stone, expected_bought, expected_extra,
                        reason="arena_challenge_purchase_state_write_rejected", subject="arena",
                    ))
            return result

    @staticmethod
    def _challenge_purchase_arena_matches(row, bought, extra, today):
        return row is not None and sqlite_int_compare(row["daily_challenge_buys"], bought) == 0 and sqlite_int_compare(row["daily_extra_challenges"], extra) == 0 and str(row["last_buy_date"] or "") == str(today)

    @staticmethod
    def _result(status, amount, cost, stone, bought, extra, *, reason="", subject=""):
        return {"status": status, "reason": reason, "subject": subject, "amount": amount, "cost": cost, "stone": stone, "bought": bought, "extra": extra}

    def _record_challenge_purchase(self, uow, operation_id, payload, result):
        if result["status"] not in {"applied", "duplicate"}:
            payload = json.dumps({"_payload": payload, "_rejection": result}, ensure_ascii=True, separators=(",", ":"))
        inserted = uow.execute(
            "INSERT INTO arena_challenge_purchase_operations(operation_id,payload,amount,cost,stone,bought,extra) "
            "VALUES(?,?,?,xiuxian_int_store(?,'stone','actor'),xiuxian_int_store(?,'stone','actor'),?,?)",
            (operation_id, payload, result["amount"], result["cost"], result["stone"], result["bought"], result["extra"]),
        )
        saved = uow.query_one(
            "SELECT payload,amount,cost,stone,bought,extra FROM arena_challenge_purchase_operations WHERE operation_id=?",
            (operation_id,),
        )
        matches = saved is not None and str(saved["payload"]) == payload and sqlite_int_compare(saved["amount"], result["amount"]) == 0 and sqlite_int_compare(saved["cost"], result["cost"]) == 0 and as_int_like(saved["stone"]) == result["stone"] and sqlite_int_compare(saved["bought"], result["bought"]) == 0 and sqlite_int_compare(saved["extra"], result["extra"]) == 0
        if inserted.rowcount != 1 or not matches:
            raise _ArenaChallengePurchaseWriteRejected(self._result(
                "state_changed", 0, 0, result["stone"], result["bought"], result["extra"],
                reason="arena_challenge_purchase_receipt_write_rejected", subject="receipt",
            ))
        return result


__all__ = ["ArenaChallengePurchaseSqlRepository", "ArenaRepository", "LegacyArenaRepository"]
