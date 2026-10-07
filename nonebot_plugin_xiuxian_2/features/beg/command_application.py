from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from pathlib import Path

from ...infrastructure.clock import SystemClock
from ...infrastructure.random_source import SystemRandom
from ..info.activity_application import PlayerActivityApplication
from .application import BegApplication
from .command_repository import BegCommandRepository, BegCommandSchemaError
from .domain import normalized_rewards, parse_datetime


class BegCommandApplication:
    def __init__(
        self, database: str | Path, config_provider, levels_provider, gift_provider,
        *, clock=None, rng=None, repository=None, application=None, activity=None,
    ) -> None:
        self.config_provider = config_provider
        self.levels_provider = levels_provider
        self.gift_provider = gift_provider
        self.clock = clock or SystemClock()
        self.rng = rng or SystemRandom()
        self.repository = repository or BegCommandRepository(database)
        self.application = application or BegApplication(database)
        self.activity = activity or PlayerActivityApplication(database, clock=self.clock)

    @staticmethod
    def _integer(value, *, minimum=0):
        if isinstance(value, bool):
            raise ValueError("boolean is not an amount")
        number = int(value)
        if number < minimum or (not isinstance(value, str) and number != value):
            raise ValueError("invalid nonnegative integer")
        return number

    @staticmethod
    def _account_time(created, now):
        if not isinstance(now, datetime):
            raise ValueError("invalid command clock")
        if created.tzinfo is None:
            # Historical profiles use the process-local, timezone-naive clock.
            current = now.astimezone().replace(tzinfo=None) if now.tzinfo is not None else now
        else:
            current = now.astimezone(created.tzinfo)
        if current < created:
            raise ValueError("account creation is in the future")
        return current

    def _gift(self):
        gift = self.gift_provider()
        if not isinstance(gift, Mapping):
            raise ValueError("novice gift is missing")
        stone, rewards, index = 0, [], 1
        while f"name_{index}" in gift:
            name = str(gift[f"name_{index}"]).strip()
            amount = self._integer(gift.get(f"amount_{index}", 1), minimum=1)
            if name == "灵石":
                stone += amount
            else:
                kind = gift.get(f"type_{index}")
                if not name or not isinstance(kind, str) or not kind.strip():
                    raise ValueError("incomplete novice reward")
                if kind in {"辅修功法", "神通", "功法", "身法", "瞳术"}:
                    kind = "技能"
                elif kind in {"法器", "防具"}:
                    kind = "装备"
                rewards.append({
                    "id": self._integer(gift[f"buff_{index}"], minimum=1),
                    "name": name, "type": kind, "amount": amount,
                })
            index += 1
        if index == 1:
            raise ValueError("empty novice gift")
        return stone, [
            {"id": item_id, "name": name, "type": kind, "amount": amount}
            for item_id, name, kind, amount in normalized_rewards(rewards)
        ]

    def execute(self, *, action, operation_id="", user_id="") -> dict:
        action = str(action)
        invalid = {"action": action, "status": "invalid_argument"}
        if action == "help":
            try:
                config = self.config_provider()
                days = self._integer(config.beg_max_days)
                level = str(config.beg_max_level).strip()
                if not level:
                    raise ValueError("missing maximum level")
                return {"action": action, "status": "ok", "max_age_days": days,
                        "max_level": level, "current_time": self.clock.now().strftime("%Y-%m-%d %H:%M:%S")}
            except (AttributeError, KeyError, TypeError, ValueError, OverflowError):
                return {"action": action, "status": "config_invalid"}
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if (action not in {"daily_settle", "novice_claim"} or not operation_id or not user_id
                or len(operation_id) > 255 or len(user_id) > 255):
            return invalid
        try:
            previous = self.repository.receipt(operation_id=operation_id, user_id=user_id, action=action)
            if previous is not None:
                return previous
            profile = self.repository.profile(user_id)
        except BegCommandSchemaError:
            return {"action": action, "status": "schema_missing"}
        if profile is None:
            return {"action": action, "status": "user_missing"}
        try:
            created = parse_datetime(profile["create_time"])
            now = self._account_time(created, self.clock.now())
            wallet = self._integer(profile["stone"])
        except (KeyError, TypeError, ValueError, OverflowError):
            return {"action": action, "status": "profile_invalid"}
        if action == "daily_settle":
            self.activity.update_last_check_info_time(user_id)
        try:
            config = self.config_provider()
            max_age_days = self._integer(config.beg_max_days)
            payload = {"action": action, "expected_create_time": profile["create_time"],
                       "max_age_days": max_age_days}
            rewards = None
            if action == "daily_settle":
                levels = list(self.levels_provider())
                maximum = levels.index(str(config.beg_max_level))
                if not maximum:
                    raise ValueError("empty eligible level range")
                lower = self._integer(config.beg_lingshi_lower_limit)
                upper = self._integer(config.beg_lingshi_upper_limit)
                if upper < lower:
                    raise ValueError("invalid reward range")
                payload.update(
                    expected_stone=wallet, expected_sect_id=profile["sect_id"],
                    expected_root_type=profile["root_type"], expected_level=profile["level"],
                    settled_at=now, eligible_levels=levels[:maximum],
                    stone_reward=self.rng.randint(lower, upper),
                )
            else:
                stone, rewards = self._gift()
                payload.update(claimed_at=now, stone=stone, rewards=rewards,
                               max_goods_num=self._integer(config.max_goods_num))
        except (AttributeError, KeyError, TypeError, ValueError, OverflowError):
            return {"action": action, "status": "config_invalid"}
        outcome = self.application.execute(operation_id=operation_id, user_id=user_id, payload=payload)
        data = dict(outcome.data or {})
        status = "duplicate" if outcome.ok and outcome.replayed else data.get("status", outcome.code or outcome.status)
        result = {**data, "action": action, "status": status, "replayed": outcome.replayed,
                  "max_age_days": max_age_days}
        if rewards is not None and status == "applied":
            result["rewards"] = rewards
        return result


__all__ = ["BegCommandApplication"]
