from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Protocol, Sequence


@dataclass(frozen=True)
class RiftOperation:
    operation_id: str
    user_id: str

    def validate(self) -> None:
        if not self.operation_id or not self.user_id:
            raise ValueError("operation_id and user_id are required")


class RiftRandomSource(Protocol):
    def randint(self, start: int, end: int) -> int: ...

    def choice(self, values: Sequence[Any]) -> Any: ...


@dataclass(frozen=True)
class RiftDamageEvent:
    message: str
    delta: dict[str, int]

    def as_outcome(self) -> dict[str, Any]:
        return {"delta": dict(self.delta), "message": self.message}


class RiftDamageEventResolver:
    """Roll a damage event without reading or mutating player state."""

    def __init__(
        self,
        *,
        battle_config: Mapping[str, Any],
        exp_reward: Callable[..., int],
        format_number: Callable[[Any], str],
    ) -> None:
        self.battle_config = battle_config
        self.exp_reward = exp_reward
        self.format_number = format_number

    @staticmethod
    def _weighted_choice(
        values: Mapping[str, Mapping[str, Any]], random_source: RiftRandomSource
    ) -> str:
        rates = [
            (str(name), int(value.get("type_rate", 0)))
            for name, value in values.items()
        ]
        rates = [(name, rate) for name, rate in rates if rate > 0]
        total = sum(rate for _, rate in rates)
        if total <= 0:
            raise ValueError("damage event rates must contain a positive weight")
        draw = int(random_source.randint(1, total))
        for name, rate in rates:
            if draw <= rate:
                return name
            draw -= rate
        raise ValueError("damage event rate draw was out of range")

    def roll(
        self,
        event_type: str,
        user_info: Mapping[str, Any],
        *,
        random_source: RiftRandomSource,
    ) -> RiftDamageEvent:
        if event_type not in self.battle_config:
            raise ValueError("unknown rift damage event type")
        event = self.battle_config[event_type]
        costs = event.get("cost")
        descriptions = event.get("desc")
        if (
            not isinstance(costs, Mapping)
            or not isinstance(descriptions, Sequence)
            or not descriptions
        ):
            raise ValueError("rift damage event configuration is incomplete")
        cost_type = self._weighted_choice(costs, random_source)
        values = costs[cost_type].get("value")
        if not isinstance(values, Sequence) or not values:
            raise ValueError("rift damage event cost values are missing")
        value = random_source.choice(values)
        delta: dict[str, int]
        if cost_type == "exp":
            exp = self.exp_reward(
                user_info["exp"],
                float(value),
                user_info.get("level"),
                apply_rank_suppress=False,
                anchor="gap",
            )
            now_hp = user_info["hp"] - (exp / 2)
            now_mp = user_info["mp"] - exp
            now_hp = now_hp if now_hp > 0 else 1
            now_mp = now_mp if now_mp > 0 else 1
            delta = {
                "exp": -int(exp),
                "hp": int(now_hp) - int(user_info["hp"]),
                "mp": int(now_mp) - int(user_info["mp"]),
            }
            detail = f"修为减少了：{self.format_number(exp)}点！"
        elif cost_type == "hp":
            cost_hp = int((user_info["exp"] / 2) * value)
            now_hp = user_info["hp"] - cost_hp
            if now_hp < 0:
                now_hp = 1
            delta = {"hp": int(now_hp) - int(user_info["hp"])}
            detail = f"气血减少了：{self.format_number(cost_hp)}点！"
        elif cost_type == "stone":
            cost_stone = int(value)
            delta = {"stone": -cost_stone}
            detail = f"灵石减少了：{self.format_number(cost_stone)}枚！"
        else:
            raise ValueError("unsupported rift damage cost type")
        message = str(random_source.choice(descriptions)).format(detail)
        return RiftDamageEvent(message, delta)


@dataclass(frozen=True)
class RiftBossBattleEvent:
    battle_result: Any
    message: str
    outcome: dict[str, Any]
    victory: bool


class RiftBossBattleResolver:
    """Run a Rift Boss battle and return only its settlement outcome."""

    def __init__(
        self,
        *,
        boss_config: Mapping[str, Any],
        battle_runner: Callable[..., Awaitable[Any]],
        rank_score: Callable[[str], int],
        level_power: Callable[[str], Any],
        max_exp_factor: float,
        exp_reward: Callable[..., int],
        format_number: Callable[[Any], str],
    ) -> None:
        self.boss_config = boss_config
        self.battle_runner = battle_runner
        self.rank_score = rank_score
        self.level_power = level_power
        self.max_exp_factor = float(max_exp_factor)
        self.exp_reward = exp_reward
        self.format_number = format_number

    async def roll(
        self,
        user_info: Mapping[str, Any],
        rift_rank: int,
        bot_id: Any,
        *,
        random_source: RiftRandomSource,
        battle_mode: int = 0,
    ) -> RiftBossBattleEvent:
        boss_data = self.boss_config.get("Boss数据")
        if not isinstance(boss_data, Mapping):
            raise ValueError("rift Boss configuration is incomplete")
        base_exp = user_info["exp"]
        boss_hp = int(base_exp * random_source.choice(boss_data["hp"]) * 10)
        boss_info = {
            "name": random_source.choice(boss_data["name"]),
            "气血": boss_hp,
            "总血量": boss_hp,
            "攻击": int(base_exp * random_source.choice(boss_data["atk"])),
            "真元": base_exp * boss_data["mp"],
            "jj": "遁一境",
            "stone": 1,
        }
        result, victor, _, status_list = await self.battle_runner(
            user_info["user_id"],
            boss_info,
            type_in=int(battle_mode),
            bot_id=bot_id,
            return_status=True,
        )
        final_hp, final_mp = int(user_info["hp"]), int(user_info["mp"])
        for status in status_list:
            for attr in status.values():
                if str(attr.get("user_id")) != str(user_info["user_id"]):
                    continue
                hp_multiplier = attr.get("hp_multiplier", 1) or 1
                mp_multiplier = attr.get("mp_multiplier", 1) or 1
                final_hp = max(1, int(attr.get("hp", final_hp) / hp_multiplier))
                final_mp = max(1, int(attr.get("mp", final_mp) / mp_multiplier))
        outcome: dict[str, Any] = {
            "delta": {
                "hp": final_hp - int(user_info["hp"]),
                "mp": final_mp - int(user_info["mp"]),
            },
            "statistics": {"秘境打怪": 1},
        }
        victory = victor == "群友赢了"
        if victory:
            user_rank = self.rank_score("练气境圆满") - self.rank_score(str(user_info["level"]))
            success_info = self.boss_config["success"]
            boss_name = str(boss_info["name"])
            message = str(random_source.choice(success_info["desc"])).format(boss_name)
            level = str(user_info["level"])[:3] + "初期"
            max_exp = int(self.level_power(level) * self.max_exp_factor)
            give_exp = self.exp_reward(
                user_info["exp"],
                float(random_source.choice(success_info["give"]["exp"])),
                user_info.get("level"),
                apply_rank_suppress=False,
                anchor="gap",
            )
            give_exp = min(give_exp, max_exp)
            give_stone = (int(rift_rank) + user_rank) * int(success_info["give"]["stone"])
            outcome["delta"].update({"exp": give_exp, "stone": give_stone})
            message += (
                f"获得了修为：{self.format_number(give_exp)}点，"
                f"灵石：{self.format_number(give_stone)}枚！"
            )
        else:
            fail_info = self.boss_config["fail"]
            message = str(random_source.choice(fail_info["desc"])).format(boss_info["name"])
        outcome["message"] = message
        return RiftBossBattleEvent(result, message, outcome, victory)


__all__ = [
    "RiftBossBattleEvent",
    "RiftBossBattleResolver",
    "RiftDamageEvent",
    "RiftDamageEventResolver",
    "RiftOperation",
    "RiftRandomSource",
]
