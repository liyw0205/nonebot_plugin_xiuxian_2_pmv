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


class RiftBossBattleAssetProvider(Protocol):
    def __call__(self, user_id: Any) -> Mapping[str, Any]: ...


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
        player_asset_provider: RiftBossBattleAssetProvider | None = None,
    ) -> None:
        self.boss_config = boss_config
        self.battle_runner = battle_runner
        self.rank_score = rank_score
        self.level_power = level_power
        self.max_exp_factor = float(max_exp_factor)
        self.exp_reward = exp_reward
        self.format_number = format_number
        self.player_asset_provider = player_asset_provider

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
        runner_kwargs: dict[str, Any] = {
            "type_in": int(battle_mode),
            "bot_id": bot_id,
            "return_status": True,
        }
        if self.player_asset_provider is not None:
            player_data = self.player_asset_provider(user_info["user_id"])
            if not isinstance(player_data, Mapping):
                raise ValueError("rift Boss player asset provider returned invalid data")
            runner_kwargs["player_data"] = player_data
        result, victor, _, status_list = await self.battle_runner(
            user_info["user_id"], boss_info, **runner_kwargs
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


@dataclass(frozen=True)
class RiftTreasureEvent:
    item_name: str | None
    message: str
    outcome: dict[str, Any]


class RiftTreasureResolver:
    """Roll a treasure outcome while keeping legacy asset lookups injectable."""

    def __init__(
        self,
        *,
        treasure_config: Mapping[str, Any],
        messages: Mapping[str, Sequence[str]],
        weapon_provider: Callable[..., tuple[Any, Mapping[str, Any]]],
        armor_provider: Callable[..., tuple[Any, Mapping[str, Any]]],
        main_provider: Callable[..., tuple[bool, Any]],
        secondary_provider: Callable[..., tuple[bool, Any]],
        sub_provider: Callable[..., tuple[bool, Any]],
        item_lookup: Callable[[Any], Mapping[str, Any] | None],
        format_number: Callable[[Any], str],
    ) -> None:
        self.treasure_config = treasure_config
        self.messages = messages
        self.weapon_provider = weapon_provider
        self.armor_provider = armor_provider
        self.main_provider = main_provider
        self.secondary_provider = secondary_provider
        self.sub_provider = sub_provider
        self.item_lookup = item_lookup
        self.format_number = format_number

    @staticmethod
    def _weighted_choice(
        values: Mapping[str, Any], random_source: RiftRandomSource
    ) -> str:
        rates = [
            (str(name), int(value.get("type_rate", 0)))
            for name, value in values.items()
            if isinstance(value, Mapping)
        ]
        rates = [(name, rate) for name, rate in rates if rate > 0]
        total = sum(rate for _, rate in rates)
        if total <= 0:
            raise ValueError("treasure rates must contain a positive weight")
        draw = int(random_source.randint(1, total))
        for name, rate in rates:
            if draw <= rate:
                return name
            draw -= rate
        raise ValueError("treasure rate draw was out of range")

    def _item_outcome(
        self,
        item_id: Any,
        item_data: Mapping[str, Any],
        message_templates: Sequence[str],
        random_source: RiftRandomSource,
    ) -> RiftTreasureEvent:
        item_name = str(item_data["name"])
        item_type = str(item_data["type"])
        message = str(random_source.choice(message_templates)).format(f"{item_name}!")
        outcome = {
            "delta": {},
            "items": [{"id": item_id, "name": item_name, "type": item_type, "amount": 1}],
            "message": message,
        }
        return RiftTreasureEvent(item_name, message, outcome)

    def roll(
        self,
        user_info: Mapping[str, Any],
        rift_rank: int,
        *,
        random_source: RiftRandomSource,
    ) -> RiftTreasureEvent:
        treasure_type = self._weighted_choice(self.treasure_config, random_source)
        templates = self.messages.get(treasure_type)
        if not templates:
            raise ValueError(f"treasure message templates are missing: {treasure_type}")

        if treasure_type == "法器":
            item_id, item_data = self.weapon_provider(
                user_info, rift_rank, random_source=random_source
            )
            return self._item_outcome(item_id, item_data, templates, random_source)
        if treasure_type == "防具":
            item_id, item_data = self.armor_provider(
                user_info, rift_rank, random_source=random_source
            )
            return self._item_outcome(item_id, item_data, templates, random_source)

        provider = {
            "功法": self.main_provider,
            "神通": self.secondary_provider,
            "辅修功法": self.sub_provider,
        }.get(treasure_type)
        if provider is not None:
            success, item_id = provider(
                user_info["level"], rift_rank, random_source=random_source
            )
            if success:
                item_data = self.item_lookup(item_id)
                if item_data is None:
                    raise ValueError(f"treasure item is missing: {item_id}")
                return self._item_outcome(item_id, item_data, templates, random_source)
            fallback = {
                "功法": "道友在秘境中获得一本书籍，翻开一看居然是绿野仙踪...",
                "神通": "道友在秘境中获得一本书籍，翻开一看居然是戏书...",
                "辅修功法": "道友在秘境中获得一本书籍，翻开一看居然是四库全书...",
            }[treasure_type]
            return RiftTreasureEvent(None, fallback, {"delta": {}, "items": [], "message": fallback})

        if treasure_type == "灵石":
            user_rank = random_source.randint(1, 3)
            give_stone = (
                int(rift_rank) + user_rank
            ) * int(self.treasure_config["灵石"]["stone"])
            detail = f"灵石：{self.format_number(give_stone)}枚！"
            message = str(random_source.choice(templates)).format(detail)
            return RiftTreasureEvent(
                None,
                message,
                {"delta": {"stone": give_stone}, "items": [], "message": message},
            )

        raise ValueError(f"unsupported treasure type: {treasure_type}")


__all__ = [
    "RiftBossBattleAssetProvider",
    "RiftBossBattleEvent",
    "RiftBossBattleResolver",
    "RiftDamageEvent",
    "RiftDamageEventResolver",
    "RiftOperation",
    "RiftRandomSource",
    "RiftTreasureEvent",
    "RiftTreasureResolver",
]
