import json
import threading
import time
from pathlib import Path
from typing import Dict, List, Any, Optional

from nonebot.log import logger
from ...paths import get_paths
from ...infrastructure.clock import SystemClock
from ...infrastructure.random_source import SystemRandom
from ...infrastructure.ids import UUIDGenerator

from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import convert_rank
from ...features.dungeon.application import DungeonApplication
from ...features.dungeon.reset_repository import DungeonResetSqlRepository
from ...features.dungeon.explore_encounters import (
    DEFAULT_BOSS_SKILLS,
    DEFAULT_MINION_SKILLS,
    JINJIE_LIST,
    TYPE_DIFFICULTY_MULT,
    TYPE_EVENT_WEIGHTS,
    TYPE_NON_BATTLE_REWARD_MULT,
    TYPE_REWARD_MULT,
    boss_info as _boss_info,
    build_boss_encounter,
    build_event_encounter,
    choose_main_realm as _choose_main_realm,
    create_monster as _create_monster,
    dungeon_type as _explore_dungeon_type,
    generate_drop_item as _generate_explore_drop_item,
    minion_info as _minion_info,
    select_event as _select_explore_event,
    type_multipliers as _explore_type_multipliers,
)

item_s = Items()


jinjie_list = JINJIE_LIST

class DungeonEvent:
    """副本事件类"""

    def __init__(self, event_data: Dict):
        self.event_id = event_data.get("event_id")
        self.event_type = self.event_id
        self.weight = event_data.get("weight", 0)
        self.description = event_data.get("description", "")

        if self.event_id == "trap":
            self.damage = event_data.get("damage", [0.1, 0.2])
        elif self.event_id == "monster":
            self.battle = event_data.get("battle", {})
        elif self.event_id == "treasure":
            self.drop_items = event_data.get("reward", {}).get("drop_items", {})
        elif self.event_id == "spirit_stone":
            self.stones = event_data.get("reward", {}).get("spirit_stone", [1, 2])
        elif self.event_id == "nothing":
            self.effect = event_data.get("effect", {})


class DungeonTemplate:
    """副本模板类"""

    def __init__(self, template_data: Dict, random_source=None):
        self.random = random_source or SystemRandom()
        self.id = template_data.get("id")
        self.name = template_data.get("name")
        self.type = template_data.get("type", "explore")
        self.description = template_data.get("description")
        self.total_layers = template_data.get("total_layers", 5)

        self.events: List[DungeonEvent] = []
        for event_data in template_data.get("events", []):
            self.events.append(DungeonEvent(event_data))

        self.monster_templates = template_data.get("monster_templates", {})
        self.boss_config = template_data.get("boss", {})
        self.snapshot_data = {
            "events": template_data.get("events", []),
            "monster_templates": self.monster_templates,
            "boss": self.boss_config,
        }

    def get_event_map(self) -> Dict[str, DungeonEvent]:
        return {e.event_id: e for e in self.events}

    def get_minion_info(
        self, template_type: str = "common", random_source=None
    ) -> Dict[str, Any]:
        rng = random_source or self.random
        return _minion_info(self.snapshot_data, template_type, rng)

    def get_boss_info(self, random_source=None) -> Dict[str, Any]:
        rng = random_source or self.random
        return _boss_info(self.snapshot_data, rng)


class DungeonManager:
    """副本管理器"""

    def _reset_application(self):
        application = getattr(self, "dungeon_application", None)
        if application is not None:
            return application
        # Explicitly constructed legacy test/rollback objects may only expose
        # reset_service; normal runtime construction always sets the application.
        return getattr(self, "reset_service")

    def _reset_operation(self, operation_id: str):
        application = self._reset_application()
        reader = getattr(application, "reset_operation_result", None)
        if reader is None:
            reader = getattr(application, "operation_result", None)
        return reader(operation_id) if operation_id and callable(reader) else None

    _instance = None
    _has_init = False
    _lock = threading.RLock()

    GLOBAL_USER_ID = "0"

    TYPE_EVENT_WEIGHTS = TYPE_EVENT_WEIGHTS
    TYPE_DIFFICULTY_MULT = TYPE_DIFFICULTY_MULT
    TYPE_REWARD_MULT = TYPE_REWARD_MULT
    TYPE_NON_BATTLE_REWARD_MULT = TYPE_NON_BATTLE_REWARD_MULT

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DungeonManager, cls).__new__(cls)
        return cls._instance

    def __copy__(self):
        # Frozen encounter resolution needs an independent instance despite the singleton constructor.
        copied = object.__new__(type(self))
        copied.__dict__.update(self.__dict__)
        return copied

    def __init__(self, random_source=None, clock=None, *, business_timezone=None):
        with self._lock:
            if self.__class__._has_init:
                return
            self.__class__._has_init = True
            self.random = random_source or SystemRandom()
            self.clock = clock or SystemClock()
            self.business_timezone = business_timezone
            self.ids = UUIDGenerator()

            self.plugin_path = Path(__file__).parent.absolute()
            self.dungeon_data_path = get_paths().data / "副本"
            self.config_file = self.dungeon_data_path / "副本.json"
            self.legacy_config_file = self.plugin_path / "data" / "副本.json"

            self.dungeon_data_path.mkdir(parents=True, exist_ok=True)

            self.dungeon_templates = self._load_dungeon_templates()
            self.dungeon_application = DungeonApplication(
                get_paths().game_db, get_paths().player_db, clock=self.clock
            )

            self.current_dungeon: Optional[DungeonTemplate] = None
            self._load_or_init_today_dungeon()

            logger.info("DungeonManager 初始化完成")

    def _get_current_date(self) -> str:
        return self.clock.now().astimezone(self.business_timezone).date().isoformat()

    def _load_dungeon_templates(self) -> List[DungeonTemplate]:
        templates = []
        config_file = self.config_file
        if not config_file.exists() and self.legacy_config_file.exists():
            config_file = self.legacy_config_file
            logger.warning(
                f"副本配置文件已迁移到 {self.config_file}，当前仍从旧路径读取: {self.legacy_config_file}"
            )

        if config_file.exists():
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                for template_data in config_data:
                    templates.append(DungeonTemplate(template_data, random_source=self.random))
            except Exception as e:
                logger.error(f"加载副本模板失败: {e}")
        else:
            logger.warning(f"副本配置文件不存在: {self.config_file}")
        return templates

    def _get_global_state(self) -> dict | None:
        return self._reset_application().global_state()

    def _find_template_by_id(self, dungeon_id: str) -> Optional[DungeonTemplate]:
        for template in self.dungeon_templates:
            if template.id == dungeon_id:
                return template
        return None

    @staticmethod
    def _template_snapshot(dungeon: DungeonTemplate) -> dict:
        snapshot = {
            "dungeon_id": dungeon.id,
            "dungeon_name": dungeon.name,
            "total_layers": dungeon.total_layers,
            "dungeon_type": dungeon.type,
            "description": dungeon.description,
        }
        snapshot.update(getattr(dungeon, "snapshot_data", {}))
        return snapshot

    @staticmethod
    def _template_from_snapshot(snapshot: dict) -> DungeonTemplate:
        return DungeonTemplate(
            {
                "id": str(snapshot.get("dungeon_id", snapshot.get("id", ""))),
                "name": str(
                    snapshot.get("dungeon_name", snapshot.get("name", ""))
                ),
                "total_layers": int(snapshot.get("total_layers", 5) or 5),
                "type": str(snapshot.get("dungeon_type", snapshot.get("type", "explore"))),
                "description": str(snapshot.get("description", "")),
                "events": snapshot.get("events", []),
                "monster_templates": snapshot.get("monster_templates", {}),
                "boss": snapshot.get("boss", {}),
            },
            random_source=SystemRandom(),
        )

    def _explore_template_snapshot(self) -> dict:
        template = self.current_dungeon
        if template is None:
            raise RuntimeError("current dungeon is unavailable")
        snapshot = dict(getattr(template, "snapshot_data", {}) or {})
        if "events" not in snapshot:
            event_data = []
            for event in getattr(template, "events", []):
                if isinstance(event, dict):
                    event_data.append(dict(event))
                    continue
                data = {
                    "event_id": getattr(event, "event_id", None),
                    "weight": getattr(event, "weight", 0),
                    "description": getattr(event, "description", ""),
                }
                if data["event_id"] == "trap":
                    data["damage"] = getattr(event, "damage", [0.1, 0.2])
                elif data["event_id"] == "monster":
                    data["battle"] = getattr(event, "battle", {})
                elif data["event_id"] == "treasure":
                    data["reward"] = {"drop_items": getattr(event, "drop_items", {})}
                elif data["event_id"] == "spirit_stone":
                    data["reward"] = {"spirit_stone": getattr(event, "stones", [1, 2])}
                event_data.append(data)
            snapshot["events"] = event_data
        snapshot.setdefault("dungeon_id", getattr(template, "id", ""))
        snapshot.setdefault("dungeon_name", getattr(template, "name", ""))
        snapshot.setdefault("total_layers", getattr(template, "total_layers", 5))
        snapshot["dungeon_type"] = getattr(template, "type", "explore")
        snapshot.setdefault("description", getattr(template, "description", ""))
        snapshot.setdefault("monster_templates", getattr(template, "monster_templates", {}))
        snapshot.setdefault("boss", getattr(template, "boss_config", {}))
        return snapshot

    def _published_template(self, global_state: dict) -> Optional[DungeonTemplate]:
        dungeon_id = str(global_state.get("dungeon_id") or "")
        if not dungeon_id:
            return None
        operation_id = str(global_state.get("reset_operation_id") or "")
        operation = self._reset_operation(operation_id)
        snapshot = dict(operation.dungeon_snapshot) if operation is not None else {}
        if snapshot and str(snapshot.get("dungeon_id", "")) != dungeon_id:
            snapshot = {}

        configured = self._find_template_by_id(dungeon_id)
        if configured is not None and not snapshot:
            return configured
        if configured is not None:
            configured_snapshot = self._template_snapshot(configured)
            configured_snapshot.update(snapshot)
            snapshot = configured_snapshot
        if not snapshot:
            snapshot = {}

        snapshot["dungeon_id"] = dungeon_id
        fallback_fields = {
            "dungeon_name": "未知副本",
            "total_layers": 5,
            "dungeon_type": "explore",
            "description": "",
        }
        for field, default in fallback_fields.items():
            value = snapshot.get(field)
            if value is None or (field != "description" and value == ""):
                state_value = global_state.get(field)
                snapshot[field] = default if state_value is None else state_value

        try:
            total_layers = int(snapshot["total_layers"])
        except (TypeError, ValueError):
            total_layers = 0
        if total_layers < 1:
            state_layers = global_state.get("total_layers")
            try:
                total_layers = int(state_layers)
            except (TypeError, ValueError):
                total_layers = 0
            snapshot["total_layers"] = total_layers if total_layers > 0 else 5
        return self._template_from_snapshot(snapshot)

    def _load_or_init_today_dungeon(self):
        """
        仅初始化或同步今日副本。
        不清空玩家状态。
        """
        with self._lock:
            current_date = self._get_current_date()
            global_state = self._get_global_state()

            if global_state and global_state.get("date") == current_date:
                template = self._published_template(global_state)
                if template:
                    self.current_dungeon = template
                    return

            if not self.dungeon_templates:
                self.current_dungeon = None
                logger.error("未加载到任何副本模板")
                return

            result = self.reset_dungeon(
                DungeonResetSqlRepository.automatic_operation_id(current_date),
                source="crossday",
                business_date=current_date,
            )
            logger.info(
                f"初始化今日副本成功: {self.current_dungeon.id} - "
                f"{self.current_dungeon.name} - {current_date} - generation={result.generation}"
            )

    def sync_current_dungeon(self):
        """
        从全局状态同步当前副本到内存。
        如果跨天则自动刷新当天副本。
        """
        with self._lock:
            current_date = self._get_current_date()
            global_state = self._get_global_state()

            if not global_state:
                logger.warning("未找到副本全局状态，自动初始化")
                self._load_or_init_today_dungeon()
                return

            if global_state.get("date") != current_date:
                logger.info("检测到跨天，自动刷新今日副本")
                self.reset_dungeon(
                    DungeonResetSqlRepository.automatic_operation_id(current_date),
                    source="crossday",
                    business_date=current_date,
                )
                return

            dungeon_id = global_state.get("dungeon_id")
            if self.current_dungeon and self.current_dungeon.id == dungeon_id:
                return

            template = self._published_template(global_state)
            if template:
                self.current_dungeon = template
            else:
                logger.warning(f"全局副本ID无效: {dungeon_id}，重新初始化今日副本")
                self._load_or_init_today_dungeon()

    def reset_dungeon(self, operation_id: str | None = None, source: str = "manual", *, business_date: str | None = None):
        """
        每日刷新副本。
        只有这里才会重置所有玩家副本状态。
        """
        with self._lock:
            source = str(source).strip().lower()
            if operation_id is not None:
                operation_id = str(operation_id).strip()
            previous = None
            if operation_id is not None:
                previous = self._reset_operation(operation_id)
            # A retried message keeps its original date; explicitly supplied
            # dates still participate in repository conflict checks.
            if previous is not None and source == "manual" and business_date is None and str(previous.source) == "manual":
                current_date = str(previous.business_date)
            else:
                current_date = business_date if business_date is not None else self._get_current_date()

            if not self.dungeon_templates and previous is None:
                self.current_dungeon = None
                raise RuntimeError("副本刷新失败：没有可用副本模板")

            if operation_id is None:
                operation_id = (
                    DungeonResetSqlRepository.automatic_operation_id(current_date)
                    if source in {"daily", "crossday"}
                    else f"dungeon-reset:manual:{self.ids.new_id()}"
                )
            result = self._reset_application().reset(
                operation_id,
                current_date,
                source,
                lambda: self._template_snapshot(self.random.choice(self.dungeon_templates)),
            )
            if not result.succeeded:
                raise RuntimeError(f"dungeon reset failed: {result.status}")

            # A replayed manual operation can be older than the currently published
            # generation. Keep runtime state aligned with the authoritative row.
            global_state = self._get_global_state() or {}
            template = self._published_template(global_state)
            if template is None:
                raise RuntimeError("published dungeon template is unavailable")
            self.current_dungeon = template

            logger.info(
                f"副本已刷新: {self.current_dungeon.id} - {self.current_dungeon.name} - "
                f"{current_date} - generation={result.generation} - source={source}"
            )
            return result

    def get_dungeon_progress(self) -> Dict[str, Any]:
        with self._lock:
            self.sync_current_dungeon()
            global_state = self._get_global_state() or {}
            template = self._published_template(global_state)
            if template is None:
                return {
                    "name": "未知副本",
                    "description": "副本数据加载失败",
                    "total_layers": 0,
                    "date": str(global_state.get("date") or ""),
                    "type": "explore"
                }
            self.current_dungeon = template
            return {
                "dungeon_id": template.id,
                "name": template.name,
                "description": template.description,
                "total_layers": template.total_layers,
                "date": str(global_state.get("date") or ""),
                "type": template.type,
                "reset_generation": int(global_state.get("reset_generation") or 0),
                "reset_operation_id": str(global_state.get("reset_operation_id") or ""),
            }

    def get_player_status(self, user_id) -> Dict[str, Any]:
        """
        获取玩家副本状态。
        仅在“当天无记录”或“跨天”时初始化。
        不再因为旧实例 current_dungeon 不一致而刷状态。
        """
        self.sync_current_dungeon()

        if self.current_dungeon is None:
            raise RuntimeError("current dungeon is unavailable")
        return self._reset_application().ensure_player_status(
            str(user_id), self._template_snapshot(self.current_dungeon)
        )

    def clear_all_player_status(self) -> None:
        """
        重置所有玩家副本状态。
        仅应在 reset_dungeon() 时调用。
        """
        if self.current_dungeon is None:
            raise RuntimeError("current dungeon is unavailable")
        result = self._reset_application().reset(
            f"dungeon-reset:clear:{self.ids.new_id()}",
            self._get_current_date(),
            "manual",
            lambda: self._template_snapshot(self.current_dungeon),
        )
        logger.info(f"已重置全部玩家副本状态，共 {result.reset_players} 条")

    # =========================
    # 业务辅助
    # =========================

    def _get_dungeon_type(self) -> str:
        self.sync_current_dungeon()
        if not self.current_dungeon:
            return "explore"
        return _explore_dungeon_type(self._explore_template_snapshot())

    def _safe_level_power(self, level_name: str) -> int:
        try:
            val = int(jsondata.level_data().get(level_name, {}).get("power", 100))
            return max(100, val)
        except Exception:
            return 100

    def _choose_main_jinjie_by_player(
        self,
        user_level: str,
        prefer_player_level_for_boss=False,
        random_source=None,
    ):
        return _choose_main_realm(
            user_level,
            prefer_player_level_for_boss=prefer_player_level_for_boss,
            random_source=random_source or self.random,
            convert_rank=convert_rank,
        )

    def _get_type_multipliers(self):
        self.sync_current_dungeon()
        if not self.current_dungeon:
            return 1.0, 1.0, 1.0, 1.0
        return _explore_type_multipliers(self._explore_template_snapshot())

    # =========================
    # 掉落与怪物创建
    # =========================

    def generate_drop_item(self, user_level, drop_items, *, random_source=None):
        return _generate_explore_drop_item(
            user_level,
            drop_items,
            random_source=random_source or self.random,
            convert_rank=convert_rank,
            item_ids_for_rank=item_s.get_random_id_list_by_rank_and_item_type,
        )

    def creating_monsters(
        self,
        user_level,
        user_exp,
        monsters_info,
        monster_type="minion",
        *,
        random_source=None,
    ):
        rng = random_source or self.random
        self.sync_current_dungeon()
        if not self.current_dungeon:
            return {}
        return _create_monster(
            self._explore_template_snapshot(),
            user_level,
            user_exp,
            monsters_info,
            monster_type,
            random_source=rng,
            safe_level_power=self._safe_level_power,
            convert_rank=convert_rank,
            generate_drop=self.generate_drop_item,
        )

    # =========================
    # 事件触发
    # =========================

    def _pick_event_by_type(self, random_source=None) -> DungeonEvent:
        rng = random_source or self.random
        self.sync_current_dungeon()
        selected = _select_explore_event(self._explore_template_snapshot(), rng)
        return DungeonEvent(selected)

    def trigger_event(self, user_level, user_exp, *, random_source=None):
        rng = random_source or self.random
        self.sync_current_dungeon()
        if not self.current_dungeon:
            raise RuntimeError("current dungeon is unavailable")
        return build_event_encounter(
            self._explore_template_snapshot(),
            user_level,
            user_exp,
            random_source=rng,
            create_monster=self.creating_monsters,
            generate_drop_item=self.generate_drop_item,
            item_lookup=item_s.get_data_by_item_id,
            convert_rank=convert_rank,
        )

    # =========================
    # Boss层生成
    # =========================

    def get_boss_data(self, user_level, user_exp, *, random_source=None):
        rng = random_source or self.random
        self.sync_current_dungeon()
        if not self.current_dungeon:
            raise RuntimeError("current dungeon is unavailable")
        return build_boss_encounter(
            self._explore_template_snapshot(),
            user_level,
            user_exp,
            random_source=rng,
            create_monster=self.creating_monsters,
        )
