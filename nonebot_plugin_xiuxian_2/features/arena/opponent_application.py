from __future__ import annotations

import random


class ArenaOpponentApplication:
    def __init__(self, repository, state_application):
        self.repository = repository
        self.state_application = state_application

    def state(self, user_id):
        self.repository.require_available()
        return self.state_application.get(str(user_id))

    def find(self, user_id, operation_id=None):
        candidates = self.repository.candidates(str(user_id))
        score = int(self.state(user_id)["score"])
        close = [row["user_id"] for row in candidates if abs(row["score"] - score) <= 200]
        if close:
            return random.Random(operation_id).choice(close)
        if candidates:
            return min(candidates, key=lambda row: abs(row["score"] - score))["user_id"]
        return None

    def set_cache(self, user_id, targets):
        self.repository.set_cache(user_id, targets)

    def get_cache(self, user_id):
        return self.repository.get_cache(user_id)

    def clear_cache(self, user_id):
        self.repository.clear_cache(user_id)

    def view(self, user_id):
        score = int(self.state(user_id)["score"])
        cached = self.get_cache(user_id)
        if cached:
            profiles = {row["user_id"]: row for row in self.repository.candidates(
                str(user_id), target_ids=[row["user_id"] for row in cached],
            )}
            if all(row["user_id"] in profiles for row in cached):
                targets = [{**profiles[row["user_id"]], "score": row["score"],
                            "diff": abs(row["score"] - score)} for row in cached]
                return {"score": score, "targets": targets, "from_cache": True}
            self.clear_cache(user_id)
        candidates = self.repository.candidates(str(user_id))
        targets = sorted(candidates, key=lambda row: (abs(row["score"] - score), row["score"]))[:3]
        targets = [{**row, "diff": abs(row["score"] - score)} for row in targets]
        self.set_cache(user_id, targets)
        return {"score": score, "targets": targets, "from_cache": False}


__all__ = ["ArenaOpponentApplication"]
