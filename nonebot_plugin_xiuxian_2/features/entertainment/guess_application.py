from __future__ import annotations

from typing import Any

from .guess_repository import EntertainmentGuessSessionSqlRepository

NUMBER_SESSION_TIMEOUT = 300
PUZZLE_SESSION_TIMEOUT = 900


class EntertainmentGuessSessionApplication:
    def __init__(self, repository: EntertainmentGuessSessionSqlRepository) -> None:
        self.repository = repository

    def read(self, game_type: str, user_id: str, *, now_epoch: float):
        return self.repository.get(game_type, user_id, now_epoch=now_epoch)

    def start_number(
        self,
        *,
        user_id: str,
        user_name: str,
        answer: int,
        create_time: str,
        last_action_time: str,
        now_epoch: float,
    ):
        state = {
            "user_id": str(user_id),
            "user_name": user_name,
            "answer": int(answer),
            "low": 1,
            "high": 100,
            "tries": 0,
            "status": "playing",
            "create_time": create_time,
            "last_action_time": last_action_time,
        }
        return self.repository.start(
            "number",
            user_id,
            state,
            now_epoch=now_epoch,
            timeout_seconds=NUMBER_SESSION_TIMEOUT,
        )

    def guess_number(self, user_id: str, number: int, *, last_action_time: str, now_epoch: float):
        def apply(state: dict[str, Any]):
            state["tries"] += 1
            state["last_action_time"] = last_action_time
            answer = int(state["answer"])
            if number == answer:
                return state, "correct", True
            if number < answer:
                if number >= int(state["low"]):
                    state["low"] = max(int(state["low"]), number + 1)
                return state, "too_low", False
            if number <= int(state["high"]):
                state["high"] = min(int(state["high"]), number - 1)
            return state, "too_high", False

        return self.repository.transition(
            "number",
            user_id,
            apply,
            now_epoch=now_epoch,
            timeout_seconds=NUMBER_SESSION_TIMEOUT,
        )

    def start_puzzle(
        self,
        *,
        user_id: str,
        user_name: str,
        answer: str,
        difficulty: str,
        digits: int,
        create_time: str,
        last_action_time: str,
        now_epoch: float,
    ):
        state = {
            "user_id": str(user_id),
            "user_name": user_name,
            "answer": answer,
            "difficulty": difficulty,
            "digits": int(digits),
            "tries": 0,
            "status": "playing",
            "create_time": create_time,
            "last_action_time": last_action_time,
        }
        return self.repository.start(
            "puzzle",
            user_id,
            state,
            now_epoch=now_epoch,
            timeout_seconds=PUZZLE_SESSION_TIMEOUT,
        )

    def guess_puzzle(
        self,
        user_id: str,
        guess: str,
        *,
        last_action_time: str,
        now_epoch: float,
    ):
        def apply(state: dict[str, Any]):
            answer = str(state["answer"])
            correct = sum(1 for expected, actual in zip(answer, guess) if expected == actual)
            state["tries"] += 1
            state["last_action_time"] = last_action_time
            if correct == int(state["digits"]):
                return state, f"correct:{correct}", True
            return state, f"progress:{correct}", False

        return self.repository.transition(
            "puzzle",
            user_id,
            apply,
            now_epoch=now_epoch,
            timeout_seconds=PUZZLE_SESSION_TIMEOUT,
        )

    def end(self, game_type: str, user_id: str, *, now_epoch: float):
        return self.repository.finish(game_type, user_id, now_epoch=now_epoch)

    def expire(self, game_type: str, user_id: str, session_token: str, *, now_epoch: float):
        return self.repository.expire(
            game_type, user_id, session_token, now_epoch=now_epoch
        )


__all__ = [
    "EntertainmentGuessSessionApplication",
    "NUMBER_SESSION_TIMEOUT",
    "PUZZLE_SESSION_TIMEOUT",
]
