from __future__ import annotations

import json
import math
import sqlite3
import uuid
from pathlib import Path
from typing import Any, Callable

from ...infrastructure.database import DatabaseUnitOfWork

GUESS_SESSION_TABLE = "entertainment_guess_sessions"
GUESS_GAME_TYPES = frozenset({"number", "puzzle"})
MAX_GUESS_SESSION_BYTES = 16 * 1024


class EntertainmentGuessSessionSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)
        self._schema_verified = False

    @staticmethod
    def _game_type(game_type: str) -> str:
        value = str(game_type)
        if value not in GUESS_GAME_TYPES:
            raise ValueError("unsupported entertainment guess game type")
        return value

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        tables = {
            str(row["name"])
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if GUESS_SESSION_TABLE not in tables:
            return False
        columns = {
            str(row["name"])
            for row in uow.query_all(f"PRAGMA table_info({GUESS_SESSION_TABLE})")
        }
        return {
            "game_type", "user_id", "state_json", "session_token", "expires_at"
        } <= columns

    def _require_schema(self, uow: DatabaseUnitOfWork) -> None:
        if self._schema_verified:
            return
        if not self._schema_ready(uow):
            raise RuntimeError(
                "legacy.entertainment.004 schema_missing: entertainment_guess_sessions"
            )
        self._schema_verified = True

    def _require_database_file(self) -> None:
        if not self.database.is_file():
            raise RuntimeError("legacy.entertainment.004 schema_missing: game database")

    def _verify_schema(self) -> None:
        if self._schema_verified:
            return
        self._require_database_file()
        with DatabaseUnitOfWork(self.database, read_only=True, timeout=0) as uow:
            self._require_schema(uow)

    @staticmethod
    def _validate_state(game_type: str, user_id: str, state: dict[str, Any]) -> None:
        if not isinstance(state, dict) or state.get("status") != "playing":
            raise ValueError("entertainment guess session must be playing")
        if str(state.get("user_id", "")) != user_id:
            raise ValueError("entertainment guess session user does not match its key")
        if not isinstance(state.get("user_name"), str):
            raise ValueError("entertainment guess session user name is invalid")
        if not isinstance(state.get("create_time"), str) or not isinstance(
            state.get("last_action_time"), str
        ):
            raise ValueError("entertainment guess session timestamps are invalid")
        tries = state.get("tries")
        if not isinstance(tries, int) or isinstance(tries, bool) or tries < 0:
            raise ValueError("entertainment guess session tries are invalid")

        if game_type == "number":
            answer, low, high = state.get("answer"), state.get("low"), state.get("high")
            values = (answer, low, high)
            if any(not isinstance(value, int) or isinstance(value, bool) for value in values):
                raise ValueError("entertainment number session bounds are invalid")
            if not (1 <= answer <= 100 and 1 <= low <= answer <= high <= 100):
                raise ValueError("entertainment number session bounds are inconsistent")
        else:
            answer, digits = state.get("answer"), state.get("digits")
            if (
                not isinstance(answer, str)
                or not answer.isdigit()
                or not isinstance(digits, int)
                or isinstance(digits, bool)
                or digits not in {4, 7, 9}
                or len(answer) != digits
                or state.get("difficulty") not in {"简单", "普通", "困难"}
            ):
                raise ValueError("entertainment puzzle session answer is invalid")

    @classmethod
    def _encode(cls, game_type: str, user_id: str, state: dict[str, Any]) -> str:
        cls._validate_state(game_type, user_id, state)
        payload = json.dumps(state, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if len(payload.encode("utf-8")) > MAX_GUESS_SESSION_BYTES:
            raise ValueError("entertainment guess session exceeds storage limit")
        return payload

    @classmethod
    def _decode(cls, game_type: str, user_id: str, payload: str) -> dict[str, Any]:
        try:
            state = json.loads(payload)
        except (TypeError, ValueError, RecursionError) as exc:
            raise RuntimeError("entertainment guess session is corrupt") from exc
        try:
            cls._validate_state(game_type, user_id, state)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("entertainment guess session is corrupt") from exc
        return state

    @staticmethod
    def _result(status: str, state=None, token=None, expires_at=None, outcome=None) -> dict[str, Any]:
        return {
            "status": status,
            "session": state,
            "session_token": token,
            "expires_at": expires_at,
            "outcome": outcome,
        }

    def _read_row(self, game_type: str, user_id: str, uow: DatabaseUnitOfWork):
        return uow.query_one(
            f"SELECT state_json,session_token,expires_at FROM {GUESS_SESSION_TABLE} "
            "WHERE game_type=? AND user_id=?",
            (game_type, user_id),
        )

    def _decode_row(self, game_type: str, user_id: str, row) -> tuple[dict[str, Any], str, float]:
        state = self._decode(game_type, user_id, str(row["state_json"]))
        token = str(row["session_token"])
        expires_at = float(row["expires_at"])
        if not token or not math.isfinite(expires_at):
            raise RuntimeError("entertainment guess session metadata is corrupt")
        return state, token, expires_at

    @staticmethod
    def _new_token() -> str:
        return uuid.uuid4().hex

    def start(
        self,
        game_type: str,
        user_id: str,
        state: dict[str, Any],
        *,
        now_epoch: float,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        game_type = self._game_type(game_type)
        user_id = str(user_id)
        if not user_id or len(user_id) > 512:
            raise ValueError("invalid entertainment guess user id")
        if not math.isfinite(float(now_epoch)) or timeout_seconds <= 0:
            raise ValueError("invalid entertainment guess session deadline")
        payload = self._encode(game_type, user_id, state)
        self._require_database_file()
        self._verify_schema()
        with DatabaseUnitOfWork(self.database, immediate=True, timeout=0.5) as uow:
            self._require_schema(uow)
            row = self._read_row(game_type, user_id, uow)
            if row is not None:
                old_state, old_token, expires_at = self._decode_row(game_type, user_id, row)
                if expires_at > float(now_epoch):
                    return self._result("existing", old_state, old_token, expires_at)
                uow.execute(
                    f"DELETE FROM {GUESS_SESSION_TABLE} WHERE game_type=? AND user_id=? AND session_token=?",
                    (game_type, user_id, old_token),
                )
            token = self._new_token()
            expires_at = float(now_epoch) + int(timeout_seconds)
            uow.execute(
                f"INSERT INTO {GUESS_SESSION_TABLE}"
                "(game_type,user_id,state_json,session_token,expires_at) VALUES(?,?,?,?,?)",
                (game_type, user_id, payload, token, expires_at),
            )
        return self._result("started", state, token, expires_at)

    def get(self, game_type: str, user_id: str, *, now_epoch: float) -> dict[str, Any]:
        game_type = self._game_type(game_type)
        user_id = str(user_id)
        self._require_database_file()
        self._verify_schema()
        with DatabaseUnitOfWork(self.database, read_only=True, timeout=0.5) as uow:
            self._require_schema(uow)
            row = self._read_row(game_type, user_id, uow)
        if row is None:
            return self._result("missing")
        state, token, expires_at = self._decode_row(game_type, user_id, row)
        if expires_at <= float(now_epoch):
            with DatabaseUnitOfWork(self.database, immediate=True, timeout=0.5) as uow:
                self._require_schema(uow)
                uow.execute(
                    f"DELETE FROM {GUESS_SESSION_TABLE} "
                    "WHERE game_type=? AND user_id=? AND session_token=? AND expires_at<=?",
                    (game_type, user_id, token, float(now_epoch)),
                )
            return self._result("expired", state, token, expires_at)
        return self._result("active", state, token, expires_at)

    def transition(
        self,
        game_type: str,
        user_id: str,
        transition: Callable[[dict[str, Any]], tuple[dict[str, Any], str, bool]],
        *,
        now_epoch: float,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        game_type = self._game_type(game_type)
        user_id = str(user_id)
        self._require_database_file()
        self._verify_schema()
        with DatabaseUnitOfWork(self.database, immediate=True, timeout=0.5) as uow:
            self._require_schema(uow)
            row = self._read_row(game_type, user_id, uow)
            if row is None:
                return self._result("missing")
            state, token, expires_at = self._decode_row(game_type, user_id, row)
            now_epoch = float(now_epoch)
            if expires_at <= now_epoch:
                uow.execute(
                    f"DELETE FROM {GUESS_SESSION_TABLE} WHERE game_type=? AND user_id=? AND session_token=?",
                    (game_type, user_id, token),
                )
                return self._result("expired", state, token, expires_at)

            new_state, outcome, finished = transition(dict(state))
            if finished:
                uow.execute(
                    f"DELETE FROM {GUESS_SESSION_TABLE} WHERE game_type=? AND user_id=? AND session_token=?",
                    (game_type, user_id, token),
                )
                return self._result("finished", new_state, token, expires_at, outcome)

            payload = self._encode(game_type, user_id, new_state)
            next_token = self._new_token()
            next_expires_at = now_epoch + int(timeout_seconds)
            uow.execute(
                f"UPDATE {GUESS_SESSION_TABLE} SET state_json=?,session_token=?,expires_at=? "
                "WHERE game_type=? AND user_id=? AND session_token=?",
                (payload, next_token, next_expires_at, game_type, user_id, token),
            )
        return self._result("updated", new_state, next_token, next_expires_at, outcome)

    def finish(self, game_type: str, user_id: str, *, now_epoch: float) -> dict[str, Any]:
        def finish_state(state: dict[str, Any]):
            return state, "ended", True

        return self.transition(
            game_type,
            user_id,
            finish_state,
            now_epoch=now_epoch,
            timeout_seconds=1,
        )

    def expire(
        self,
        game_type: str,
        user_id: str,
        session_token: str,
        *,
        now_epoch: float,
    ) -> dict[str, Any]:
        game_type = self._game_type(game_type)
        user_id = str(user_id)
        self._require_database_file()
        self._verify_schema()
        with DatabaseUnitOfWork(self.database, immediate=True, timeout=0.5) as uow:
            self._require_schema(uow)
            row = self._read_row(game_type, user_id, uow)
            if row is None:
                return self._result("missing")
            state, token, expires_at = self._decode_row(game_type, user_id, row)
            if token != str(session_token):
                return self._result("stale", state, token, expires_at)
            if expires_at > float(now_epoch):
                return self._result("active", state, token, expires_at)
            uow.execute(
                f"DELETE FROM {GUESS_SESSION_TABLE} WHERE game_type=? AND user_id=? AND session_token=?",
                (game_type, user_id, token),
            )
        return self._result("expired", state, token, expires_at)


__all__ = [
    "EntertainmentGuessSessionSqlRepository",
    "GUESS_GAME_TYPES",
    "GUESS_SESSION_TABLE",
    "MAX_GUESS_SESSION_BYTES",
]
