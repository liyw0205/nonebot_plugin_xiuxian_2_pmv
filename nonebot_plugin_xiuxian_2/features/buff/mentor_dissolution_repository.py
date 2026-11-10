"""师徒解除清算的原子事务（attached game + player）。

解除关系必须与“清退 120% 传功修为”在同一事务内完成，否则会出现
「关系已断但修为未收」或「修为已收但关系仍在」的半成品状态，前者正是可刷的漏洞。
本模块只做一件事：一个 ``BEGIN IMMEDIATE`` 事务内的回执重放判定、配对/境界 CAS、
权威账目重算、扣减、关系清理、冷却/历史/统计写入与回执写入，一次提交或整体回滚。

架构边界：事务由 :class:`AttachedDatabaseUnitOfWork` 提供（feature 不自行打开连接、
不 import 驱动层），所有语句使用 ``?`` 占位符。请求路径**不执行 DDL**：
回执表由 ``buff.016``、``statistics."师徒清退修为"`` 列由 ``buff.017`` 启动迁移预建；
缺表或缺列返回 ``schema_missing`` 且零写入。

防刷语义：
* 权威账目（``mentor_transmission_operations`` 中该配对的 ``reward_exp``）在事务内重算，
  调用方传入的数值一律不采信；
* 账目**读不出来**（缺表、坏 payload 且回退扫描失败）时 fail-closed 返回
  ``ledger_unavailable``，绝不按 0 处理——否则“先弄坏账目再解除”就是免费解除；
* 已结清部分记录在回执表，重新拜师后再次解除只对新增收益收费，不重复扣费。
"""

from __future__ import annotations

from ...core.numeric import as_int_like

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any

from ...infrastructure.database import AttachedDatabaseUnitOfWork
from .mentor_dissolution import dissolution_decision

SCHEMA_MISSING = "schema_missing"
LEDGER_UNAVAILABLE = "ledger_unavailable"

OPERATION_TABLE = "mentor_dissolution_operations"
OPERATION_COLUMNS = ("operation_id", "payload", "gained_exp", "charge", "created_at")

LEDGER_TABLE = "mentor_transmission_operations"
MENTOR_TABLE = "mentor"
MENTOR_COLUMNS = (
    "bind_time",
    "breakthrough_reward_count",
    "mentor_id",
    "apprentice_ids",
    "mentor_cd_until",
    "apprentice_cd_until",
    "mentor_rebind_cd",
    "mentor_history",
)
STATISTICS_TABLE = "statistics"
STAT_CHARGE_FIELD = "师徒清退修为"

# 只允许写这两个冷却列：其余 mentor 列（如 mentor_history/apprentice_ids）
# 由结算流程自己维护，调用方传入即视为非法请求。
COOLDOWN_FIELDS = ("mentor_cd_until", "apprentice_cd_until")

# 旧库回退扫描上限：无 JSON1 时限制读取行数，避免无界全表扫描。
_LEGACY_SCAN_LIMIT = 5000


@dataclass(frozen=True)
class MentorDissolutionResult:
    status: str
    charge: int = 0
    gained: int = 0
    apprentice_exp: int = 0
    shortfall: int = 0
    extra: dict[str, Any] = field(default_factory=dict)
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

    @property
    def refused(self) -> bool:
        return self.status in {SCHEMA_MISSING, LEDGER_UNAVAILABLE, "insufficient_cultivation"}


def _as_int(value: Any) -> int:
    try:
        number = as_int_like(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    return number if number > 0 else 0


def _quote(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


class MentorDissolutionService:
    """Settle 120% of one pair's authoritative transmission cultivation, atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path, *, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    # -- schema probes (read-only, no DDL) -------------------------------
    @staticmethod
    def _table_columns(uow, schema: str, table: str) -> set[str]:
        prefix = f"{schema}." if schema else ""
        try:
            rows = uow.query_all(f"PRAGMA {prefix}table_info({_quote(table)})")
        except Exception:
            return set()
        return {str(row["name"]) for row in rows}

    @classmethod
    def schema_ready(cls, uow) -> bool:
        """Every table/column this settlement writes must exist before it starts."""
        if not set(OPERATION_COLUMNS).issubset(cls._table_columns(uow, "", OPERATION_TABLE)):
            return False
        if not set(MENTOR_COLUMNS).issubset(cls._table_columns(uow, "player_data", MENTOR_TABLE)):
            return False
        return STAT_CHARGE_FIELD in cls._table_columns(uow, "player_data", STATISTICS_TABLE)

    # -- authoritative ledger -------------------------------------------
    @classmethod
    def authoritative_gained(cls, uow, mentor_id: str, apprentice_id: str) -> int | None:
        """Sum the pair's transmission receipts.

        ``None`` means the ledger could not be read at all (missing table, no
        JSON1 and a failed fallback scan).  Callers must treat that as a refusal,
        never as "zero gained" — otherwise damaging the ledger would make
        dissolution free.
        """
        mentor_id, apprentice_id = str(mentor_id), str(apprentice_id)
        if not cls._table_columns(uow, "", LEDGER_TABLE):
            return None
        rows = None
        try:
            rows = uow.query_all(
                f"SELECT reward_exp FROM {LEDGER_TABLE} "
                "WHERE json_valid(payload) "
                "AND json_extract(payload, '$[0]') = ? AND json_extract(payload, '$[1]') = ?",
                (mentor_id, apprentice_id),
            )
        except Exception:
            rows = None
        if rows is None:
            # 旧 SQLite/坏 payload：有界回退扫描。
            try:
                raw = uow.query_all(
                    f"SELECT payload, reward_exp FROM {LEDGER_TABLE} LIMIT ?",
                    (_LEGACY_SCAN_LIMIT,),
                )
            except Exception:
                return None
            rows = []
            for row in raw:
                try:
                    parsed = json.loads(str(row["payload"]))
                except (TypeError, ValueError):
                    continue
                if (
                    isinstance(parsed, list)
                    and len(parsed) >= 2
                    and str(parsed[0]) == mentor_id
                    and str(parsed[1]) == apprentice_id
                ):
                    rows.append({"reward_exp": row["reward_exp"]})
        return sum(_as_int(row["reward_exp"]) for row in rows)

    @staticmethod
    def settled_gained(uow, mentor_id: str, apprentice_id: str) -> int | None:
        """Authoritative gain already charged for this pair by earlier dissolutions.

        Rebinding and dissolving again must not tax the same cultivation twice, so
        the charge targets only the pair's not-yet-settled gains.  ``None`` means
        the receipt ledger itself could not be read (fail closed, no free exit).
        """
        mentor_id, apprentice_id = str(mentor_id), str(apprentice_id)
        rows = None
        try:
            rows = uow.query_all(
                f"SELECT gained_exp FROM {OPERATION_TABLE} "
                "WHERE json_valid(payload) "
                "AND json_extract(payload, '$[0]') = ? AND json_extract(payload, '$[1]') = ?",
                (mentor_id, apprentice_id),
            )
        except Exception:
            rows = None
        if rows is not None:
            return sum(_as_int(row["gained_exp"]) for row in rows)
        try:
            raw = uow.query_all(
                f"SELECT payload, gained_exp FROM {OPERATION_TABLE} LIMIT ?",
                (_LEGACY_SCAN_LIMIT,),
            )
        except Exception:
            return None
        total = 0
        for row in raw:
            try:
                parsed = json.loads(str(row["payload"]))
            except (TypeError, ValueError):
                continue
            if (
                isinstance(parsed, list)
                and len(parsed) >= 2
                and str(parsed[0]) == mentor_id
                and str(parsed[1]) == apprentice_id
            ):
                total += _as_int(row["gained_exp"])
        return total

    # -- player-side helpers (no DDL: schema is migration-owned) ---------
    @staticmethod
    def _json_field(uow, user_id: str, column: str, default):
        row = uow.query_one(
            f"SELECT {_quote(column)} FROM player_data.{MENTOR_TABLE} WHERE user_id=?",
            (str(user_id),),
        )
        if row is None or row[column] in (None, ""):
            return default
        raw = row[column]
        if isinstance(raw, (dict, list)):
            return raw
        try:
            return json.loads(str(raw))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _set_field(uow, user_id: str, column: str, value: Any) -> None:
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        changed = uow.execute(
            f"UPDATE player_data.{MENTOR_TABLE} SET {_quote(column)}=? WHERE user_id=?",
            (value, str(user_id)),
        )
        if changed.rowcount == 0:
            uow.execute(
                f"INSERT INTO player_data.{MENTOR_TABLE} (user_id,{_quote(column)}) VALUES (?,?)",
                (str(user_id), value),
            )

    @staticmethod
    def _increment_stat(uow, user_id: str, key: str, amount: int) -> None:
        changed = uow.execute(
            f"UPDATE player_data.{STATISTICS_TABLE} SET {_quote(key)}=COALESCE({_quote(key)},0)+?"
            " WHERE user_id=?",
            (int(amount), str(user_id)),
        )
        if changed.rowcount == 0:
            uow.execute(
                f"INSERT INTO player_data.{STATISTICS_TABLE} (user_id,{_quote(key)}) VALUES (?,?)",
                (str(user_id), int(amount)),
            )

    @classmethod
    def _append_history(cls, uow, user_id: str, event_type: str, related_id: str, description: str, limit: int) -> None:
        history = cls._json_field(uow, user_id, "mentor_history", [])
        if not isinstance(history, list):
            history = []
        history.append(
            {
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": str(event_type),
                "related_id": str(related_id),
                "description": str(description),
            }
        )
        cls._set_field(uow, user_id, "mentor_history", history[-int(limit):])

    # -- settlement ------------------------------------------------------
    def settle(
        self,
        operation_id: str,
        mentor_id: str,
        apprentice_id: str,
        *,
        action: str,
        expected_apprentice_level: str,
        occurred_at: str,
        cooldown_field: str,
        cooldown_until: str,
        pair_rebind_until: str,
        history_limit: int,
        mentor_desc: str,
        apprentice_desc: str,
        mentor_cd_until: str | None = None,
    ) -> MentorDissolutionResult:
        operation_id = str(operation_id).strip()
        mentor_id, apprentice_id = str(mentor_id), str(apprentice_id)
        action = str(action)
        history_limit = int(history_limit)
        if not operation_id or not action or mentor_id == apprentice_id or history_limit <= 0:
            raise ValueError("invalid mentor dissolution operation")
        if cooldown_field not in COOLDOWN_FIELDS:
            raise ValueError("invalid mentor dissolution cooldown field")
        stored = [
            mentor_id,
            apprentice_id,
            action,
            str(expected_apprentice_level),
            str(occurred_at),
            str(cooldown_field),
            str(cooldown_until),
            str(pair_rebind_until),
            history_limit,
        ]
        # 幂等键只依赖稳定输入：重放时不再重新计算，避免自我冲突。
        payload = json.dumps(stored, ensure_ascii=False, separators=(",", ":"))

        with self._lock, AttachedDatabaseUnitOfWork(
            self._game_database,
            attachments={"player_data": self._player_database},
            immediate=True,
        ) as uow:
            if not self.schema_ready(uow):
                return MentorDissolutionResult(SCHEMA_MISSING)

            previous = uow.query_one(
                f"SELECT payload, charge, gained_exp FROM {OPERATION_TABLE} WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                return MentorDissolutionResult(
                    "duplicate" if str(previous["payload"]) == payload else "operation_conflict",
                    charge=_as_int(previous["charge"]),
                    gained=_as_int(previous["gained_exp"]),
                )

            # 权威账目与应收在事务内重算；读不出来就拒绝，绝不免费放行。
            gained = self.authoritative_gained(uow, mentor_id, apprentice_id)
            if gained is None:
                return MentorDissolutionResult(LEDGER_UNAVAILABLE)
            settled = self.settled_gained(uow, mentor_id, apprentice_id)
            if settled is None:
                return MentorDissolutionResult(LEDGER_UNAVAILABLE)
            unsettled = max(0, gained - settled)

            user = uow.query_one(
                "SELECT exp, level FROM user_xiuxian WHERE user_id=?", (apprentice_id,)
            )
            balance = _as_int(user["exp"]) if user is not None else 0
            decision = dissolution_decision(gained_exp=unsettled, apprentice_exp=balance)

            # 关系与状态 CAS 先于余额判定：已解除的配对必须报 state_changed，
            # 而不是拿陈旧余额报“修为不足”。
            apprentices = [
                str(value)
                for value in (self._json_field(uow, mentor_id, "apprentice_ids", []) or [])
            ]
            parent = uow.query_one(
                "SELECT mentor_id FROM player_data.mentor WHERE user_id=?", (apprentice_id,)
            )
            if (
                apprentice_id not in apprentices
                or parent is None
                or str(parent["mentor_id"]) != mentor_id
                or user is None
                or str(user["level"]) != str(expected_apprentice_level)
            ):
                return MentorDissolutionResult(
                    "state_changed", charge=decision.charge, gained=decision.gained,
                    reason=("mentor_relation_changed" if apprentice_id not in apprentices or parent is None or str(parent["mentor_id"]) != mentor_id
                            else "user_missing" if user is None else "level_changed"),
                    subject=apprentice_id,
                )
            if decision.status == "insufficient_cultivation":
                return MentorDissolutionResult(
                    "insufficient_cultivation",
                    charge=decision.charge,
                    gained=decision.gained,
                    apprentice_exp=decision.apprentice_exp,
                    shortfall=decision.shortfall,
                )
            if decision.charge > 0:
                changed = uow.execute(
                    "UPDATE user_xiuxian SET exp=COALESCE(exp,0)-?"
                    " WHERE user_id=? AND xiuxian_int_cmp(COALESCE(exp,0),?)>=0",
                    (decision.charge, apprentice_id, decision.charge),
                )
                if changed.rowcount != 1:
                    return MentorDissolutionResult(
                        "insufficient_cultivation",
                        charge=decision.charge,
                        gained=decision.gained,
                        apprentice_exp=balance,
                        shortfall=decision.shortfall,
                    )
                self._increment_stat(uow, apprentice_id, STAT_CHARGE_FIELD, decision.charge)

            self._set_field(
                uow,
                mentor_id,
                "apprentice_ids",
                [value for value in apprentices if value != apprentice_id],
            )
            self._set_field(uow, apprentice_id, "mentor_id", None)
            self._set_field(uow, apprentice_id, "bind_time", None)
            self._set_field(uow, apprentice_id, "breakthrough_reward_count", 0)
            self._set_field(uow, apprentice_id, cooldown_field, cooldown_until)
            rebind = self._json_field(uow, apprentice_id, "mentor_rebind_cd", {})
            if not isinstance(rebind, dict):
                rebind = {}
            rebind[mentor_id] = pair_rebind_until
            self._set_field(uow, apprentice_id, "mentor_rebind_cd", rebind)
            if mentor_cd_until is not None:
                self._set_field(uow, mentor_id, "mentor_cd_until", mentor_cd_until)
            if action == "expel":
                self._increment_stat(uow, apprentice_id, "被逐出师门次数", 1)
            else:
                self._increment_stat(uow, apprentice_id, "离开师门次数", 1)
            self._append_history(uow, mentor_id, action, apprentice_id, mentor_desc, history_limit)
            self._append_history(uow, apprentice_id, action, mentor_id, apprentice_desc, history_limit)
            uow.execute(
                f"INSERT INTO {OPERATION_TABLE} (operation_id,payload,gained_exp,charge)"
                " VALUES (?,?,?,?)",
                (operation_id, payload, decision.gained, decision.charge),
            )
            return MentorDissolutionResult(
                "applied",
                charge=decision.charge,
                gained=decision.gained,
                apprentice_exp=decision.apprentice_exp,
            )


__all__ = [
    "LEDGER_UNAVAILABLE",
    "OPERATION_COLUMNS",
    "OPERATION_TABLE",
    "SCHEMA_MISSING",
    "MentorDissolutionResult",
    "MentorDissolutionService",
]
