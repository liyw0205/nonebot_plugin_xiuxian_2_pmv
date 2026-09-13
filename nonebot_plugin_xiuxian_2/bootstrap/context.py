from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..infrastructure.database.catalog import DatabaseCatalog
from ..infrastructure.clock import SystemClock
from ..infrastructure.ids import UUIDGenerator
from ..infrastructure.random_source import SystemRandom
from ..infrastructure.observability.metrics import Metrics


@dataclass
class RuntimeContext:
    paths: Any
    database: DatabaseCatalog
    clock: Any
    ids: Any
    random: Any
    metrics: Metrics
    settings: Any = None
    message_gateway: Any = None
    registry: Any = None
    migrations: Any = None
    jobs: Any = None
    job_executor: Any = None
    web_app: Any = None
    readiness: Any = None
    lifecycle: Any = None
    compatibility: Any = None
    services: Any = None
    config_service: Any = None
    reconcile_handlers: Any = None
    # Explicit data directories are normally used by tests and maintenance
    # commands.  They must not unexpectedly execute legacy resource download
    # hooks; the real NoneBot entry point opts in explicitly.
    legacy_startup: bool = True


def build_runtime_context(
    *,
    data_dir: str | Path | None = None,
    settings: Any = None,
    message_gateway: Any = None,
    legacy_startup: bool | None = None,
) -> RuntimeContext:
    from ..paths import configure_paths, get_paths

    paths = configure_paths(data_dir) if data_dir is not None else get_paths()
    config_service = None
    if settings is None:
        from ..infrastructure.config import ConfigService, SettingDefinition

        config_service = ConfigService(
            (
                SettingDefinition("web_enabled", bool, default=False, reloadable=True, env_name="XIUXIAN_WEB_STATUS", description="是否启用新 Web 适配器"),
                SettingDefinition("web_host", str, default="127.0.0.1", reloadable=True, env_name="XIUXIAN_WEB_HOST", description="Web 监听地址"),
                SettingDefinition("web_port", int, default=5888, reloadable=True, env_name="XIUXIAN_WEB_PORT", description="Web 监听端口"),
                SettingDefinition("web_secret_key", str, default="", secret=True, reloadable=False, env_name="XIUXIAN_WEB_SECRET_KEY", description="Web session 密钥"),
                SettingDefinition("web_admin_ids", list, default=(), reloadable=False, env_name="XIUXIAN_WEB_ADMIN_IDS", description="允许登录管理面板的管理员 ID JSON 列表"),
                SettingDefinition("web_allowed_hosts", list, default=(), reloadable=True, env_name="XIUXIAN_WEB_ALLOWED_HOSTS", description="Web Host 白名单；为空时仅允许本机 Host"),
                SettingDefinition("daily_fortune_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_DAILY_FORTUNE_ENABLED", description="每日运势新实现灰度开关"),
                SettingDefinition("sign_in_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_SIGN_IN_ENABLED", description="修仙签到新实现灰度开关"),
                SettingDefinition("sign_in_lower_limit", int, default=100000, reloadable=True, env_name="XIUXIAN_SIGN_IN_LOWER_LIMIT", description="签到灵石下限"),
                SettingDefinition("sign_in_upper_limit", int, default=500000, reloadable=True, env_name="XIUXIAN_SIGN_IN_UPPER_LIMIT", description="签到灵石上限"),
                SettingDefinition("stone_gift_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_STONE_GIFT_ENABLED", description="灵石赠送新实现灰度开关"),
                SettingDefinition("stone_gift_fee_rate", float, default=0.1, reloadable=True, env_name="XIUXIAN_STONE_GIFT_FEE_RATE", description="灵石赠送手续费率"),
                SettingDefinition("accessory_package_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_ACCESSORY_PACKAGE_ENABLED", description="饰品礼包新实现灰度开关"),
                SettingDefinition("arena_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_ARENA_ENABLED", description="竞技场新资产结算灰度开关"),
                SettingDefinition("auction_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_AUCTION_ENABLED", description="拍卖竞价新实现灰度开关"),
                SettingDefinition("bank_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_BANK_ENABLED", description="灵庄新资产结算灰度开关"),
                SettingDefinition("bank_first_use_enabled", bool, default=False, reloadable=True, env_name="XIUXIAN_BANK_FIRST_USE_ENABLED", description="灵庄首次存款新路径灰度开关"),
                SettingDefinition("activity_reward_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_ACTIVITY_REWARD_ENABLED", description="活动奖励新实现灰度开关"),
                SettingDefinition("combat_settlement_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_COMBAT_SETTLEMENT_ENABLED", description="地图战斗结算新实现灰度开关"),
                SettingDefinition("admin_asset_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_ADMIN_ASSET_ENABLED", description="管理员资产新实现灰度开关"),
                SettingDefinition("tianti_settlement_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_TIANTI_SETTLEMENT_ENABLED", description="炼体结算新实现灰度开关"),
                SettingDefinition("tianti_training_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_TIANTI_TRAINING_ENABLED", description="炼体进阶新实现灰度开关"),
                SettingDefinition("tower_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_TOWER_ENABLED", description="通天塔新资产结算灰度开关"),
                SettingDefinition("sect_fairyland_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_SECT_FAIRYLAND_ENABLED", description="宗门炼体堂新实现灰度开关"),
                SettingDefinition("world_events_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_WORLD_EVENTS_ENABLED", description="世界事件奖励新实现灰度开关"),
                SettingDefinition("work_claim_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_WORK_CLAIM_ENABLED", description="悬赏令接取新实现灰度开关"),
                SettingDefinition("mixelixir_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_MIXELIXIR_ENABLED", description="炼丹灵田新实现灰度开关"),
                SettingDefinition("puppet_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_PUPPET_ENABLED", description="灵田傀儡新资产操作灰度开关"),
                SettingDefinition("boss_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_BOSS_ENABLED", description="世界BOSS资产结算灰度开关"),
                SettingDefinition("dungeon_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_DUNGEON_ENABLED", description="副本新资产边界灰度开关"),
                SettingDefinition("pet_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_PET_ENABLED", description="宠物资产动作灰度开关"),
                SettingDefinition("sect_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_SECT_ENABLED", description="宗门资产动作灰度开关"),
                SettingDefinition("natal_treasure_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_NATAL_TREASURE_ENABLED", description="本命法宝资产操作灰度开关"),
                SettingDefinition("buff_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_BUFF_ENABLED", description="功法和洞天福地事务灰度开关"),
                SettingDefinition("base_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_BASE_ENABLED", description="基础修炼事务灰度开关"),
                SettingDefinition("back_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_BACK_ENABLED", description="背包物品事务灰度开关"),
                SettingDefinition("trade_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_TRADE_ENABLED", description="交易资产事务灰度开关"),
                SettingDefinition("map_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_MAP_ENABLED", description="地图探索事务灰度开关"),
                SettingDefinition("rift_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_RIFT_ENABLED", description="裂隙世界事务灰度开关"),
                SettingDefinition("illusion_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_ILLUSION_ENABLED", description="幻境切片灰度开关"),
                SettingDefinition("interactive_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_INTERACTIVE_ENABLED", description="互动切片灰度开关"),
                SettingDefinition("activity_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_ACTIVITY_ENABLED", description="活动切片灰度开关"),
                SettingDefinition("admin_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_ADMIN_ENABLED", description="管理切片灰度开关"),
                SettingDefinition("beg_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_BEG_ENABLED", description="新手礼包切片灰度开关"),
                SettingDefinition("compensation_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_COMPENSATION_ENABLED", description="补偿切片灰度开关"),
                SettingDefinition("dongfu_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_DONGFU_ENABLED", description="洞府切片灰度开关"),
                SettingDefinition("dufang_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_DUFANG_ENABLED", description="赌坊切片灰度开关"),
                SettingDefinition("entertainment_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_ENTERTAINMENT_ENABLED", description="娱乐切片灰度开关"),
                SettingDefinition("fusion_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_FUSION_ENABLED", description="融合切片灰度开关"),
                SettingDefinition("impart_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_IMPART_ENABLED", description="传承切片灰度开关"),
                SettingDefinition("impart_pk_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_IMPART_PK_ENABLED", description="传承对战切片灰度开关"),
                SettingDefinition("info_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_INFO_ENABLED", description="信息切片灰度开关"),
                SettingDefinition("lunhui_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_LUNHUI_ENABLED", description="轮回切片灰度开关"),
                SettingDefinition("past_life_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_PAST_LIFE_ENABLED", description="前世切片灰度开关"),
                SettingDefinition("simulator_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_SIMULATOR_ENABLED", description="模拟器切片灰度开关"),
                SettingDefinition("status_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_STATUS_ENABLED", description="状态切片灰度开关"),
                SettingDefinition("tasks_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_TASKS_ENABLED", description="任务切片灰度开关"),
                SettingDefinition("tianti_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_TIANTI_ENABLED", description="炼体兼容切片灰度开关"),
                SettingDefinition("title_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_TITLE_ENABLED", description="称号切片灰度开关"),
                SettingDefinition("training_enabled", bool, default=True, reloadable=True, env_name="XIUXIAN_TRAINING_ENABLED", description="修炼切片灰度开关"),
            ),
            config_path=paths.config_file,
        )
        settings = config_service.load()
    if message_gateway is None:
        from ..infrastructure.messaging import MessageGatewayAdapter

        message_gateway = MessageGatewayAdapter()
    return RuntimeContext(
        paths=paths,
        database=DatabaseCatalog.from_paths(paths),
        clock=SystemClock(),
        ids=UUIDGenerator(),
        random=SystemRandom(),
        metrics=Metrics(),
        settings=settings,
        message_gateway=message_gateway,
        config_service=config_service,
        legacy_startup=(data_dir is None if legacy_startup is None else bool(legacy_startup)),
    )


__all__ = ["RuntimeContext", "build_runtime_context"]
