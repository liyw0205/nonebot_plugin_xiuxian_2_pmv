"""Composition root for the refactored runtime.

The historical package initializer still loads legacy feature packages for
backward compatibility.  New code should use this module as the only place to
assemble the new application boundary; it deliberately contains no gameplay
rules or SQL.
"""

from __future__ import annotations

from pathlib import Path
import os
from typing import Any

from .bootstrap import FeatureRegistry, Lifecycle, LifecyclePhase, Readiness, RuntimeContext, build_runtime_context
from .bootstrap.legacy import run_legacy_shutdown, run_legacy_startup
from .bootstrap.platform_manifest import FEATURE as PLATFORM_WEB_FEATURE
from .features.daily_fortune.manifest import FEATURE as DAILY_FORTUNE_FEATURE
from .features.daily_fortune.migrations import apply_daily_fortune
from .features.illusion.manifest import FEATURE as ILLUSION_FEATURE
from .features.illusion.migrations import apply_illusion
from .features.interactive.manifest import FEATURE as INTERACTIVE_FEATURE
from .features.interactive.migrations import apply_interactive
from .features.beg.manifest import FEATURE as BEG_FEATURE
from .features.beg.migrations import apply_beg
from .features.beg.application import BegApplication
from .features.title.manifest import FEATURE as TITLE_FEATURE
from .features.title.migrations import apply_title
from .features.title.application import TitleApplication
from .features.sign_in.manifest import FEATURE as SIGN_IN_FEATURE
from .features.sign_in.migrations import apply_sign_in, apply_sign_in_statistics
from .features.stone_gift.manifest import FEATURE as STONE_GIFT_FEATURE
from .features.stone_gift.migrations import apply_stone_gift, apply_stone_gift_limits
from .features.package_reward.manifest import FEATURE as PACKAGE_REWARD_FEATURE
from .features.package_reward.migrations import apply_package_reward
from .features.pet.manifest import FEATURE as PET_FEATURE
from .features.pet.migrations import apply_pet
from .features.sect.manifest import FEATURE as SECT_FEATURE
from .features.sect.migrations import apply_sect
from .features.natal_treasure.manifest import FEATURE as NATAL_TREASURE_FEATURE
from .features.natal_treasure.migrations import apply_natal_treasure
from .features.buff.manifest import FEATURE as BUFF_FEATURE
from .features.buff.migrations import apply_buff
from .features.base.manifest import FEATURE as BASE_FEATURE
from .features.base.migrations import apply_base
from .features.back.manifest import FEATURE as BACK_FEATURE
from .features.back.migrations import apply_back
from .features.trade.manifest import FEATURE as TRADE_FEATURE
from .features.trade.migrations import apply_trade
from .features.map.manifest import FEATURE as MAP_FEATURE
from .features.map.migrations import apply_map
from .features.rift.manifest import FEATURE as RIFT_FEATURE
from .features.rift.migrations import apply_rift
from .features.accessory_package.manifest import FEATURE as ACCESSORY_PACKAGE_FEATURE
from .features.accessory_package.migrations import apply_accessory_package
from .features.arena.manifest import FEATURE as ARENA_FEATURE
from .features.arena.migrations import apply_arena
from .features.auction.manifest import FEATURE as AUCTION_FEATURE
from .features.auction.jobs import settle as auction_settle_job
from .features.bank.manifest import FEATURE as BANK_FEATURE
from .features.bank.migrations import apply_bank
from .features.activity_reward.manifest import FEATURE as ACTIVITY_REWARD_FEATURE
from .features.activity_reward.migrations import apply_activity_reward
from .features.combat_settlement.manifest import FEATURE as COMBAT_SETTLEMENT_FEATURE
from .features.combat_settlement.migrations import apply_combat_settlement
from .features.admin_asset.manifest import FEATURE as ADMIN_ASSET_FEATURE
from .features.admin_asset.migrations import apply_admin_asset
from .features.tianti_settlement.manifest import FEATURE as TIANTI_SETTLEMENT_FEATURE
from .features.tianti_settlement.migrations import apply_tianti_settlement
from .features.tianti_training.manifest import FEATURE as TIANTI_TRAINING_FEATURE
from .features.tianti_training.migrations import apply_tianti_training
from .features.tower.manifest import FEATURE as TOWER_FEATURE
from .features.tower.migrations import apply_tower
from .features.sect_fairyland.manifest import FEATURE as SECT_FAIRYLAND_FEATURE
from .features.sect_fairyland.migrations import apply_sect_fairyland
from .features.world_events.manifest import FEATURE as WORLD_EVENTS_FEATURE
from .features.world_events.migrations import apply_world_events
from .features.work.manifest import FEATURE as WORK_FEATURE
from .features.work.migrations import apply_work
from .features.mixelixir.manifest import FEATURE as MIXELIXIR_FEATURE
from .features.mixelixir.migrations import apply_mixelixir
from .features.puppet.manifest import FEATURE as PUPPET_FEATURE
from .features.puppet.migrations import apply_puppet
from .features.boss.manifest import FEATURE as BOSS_FEATURE
from .features.boss.migrations import apply_boss
from .features.dungeon.manifest import FEATURE as DUNGEON_FEATURE
from .features.dungeon.migrations import apply_dungeon
from .features.auction.migrations import apply_auction
from .features._legacy_migrated import (
    APPLICATIONS as LEGACY_MIGRATED_APPLICATIONS,
    FEATURES as LEGACY_MIGRATED_FEATURES,
    MIGRATIONS as LEGACY_MIGRATIONS,
)
from .compatibility.legacy_manifest import FEATURE as LEGACY_SCHEDULER_FEATURE
from .compatibility.legacy_jobs import legacy_job_handler
from .compatibility import compatibility_hits
from .compatibility.feature_inventory import FEATURES as LEGACY_FEATURES
from .infrastructure.database import DatabaseUnitOfWork, Migration, MigrationRunner, OperationLedger, OutboxStore
from .infrastructure.scheduler import JobExecutor, JobRegistry


def build_migrations() -> tuple[Migration, ...]:
    """Return the one migration catalog used by every runtime entry point.

    Keeping this list in the composition root prevents the maintenance CLI,
    startup lifecycle and recovery rehearsal from drifting apart.
    """
    return (
        Migration("accessory_package.001", "accessory_package_operations", apply_accessory_package),
        Migration("activity_reward.001", "activity_reward_feature_migrations", apply_activity_reward),
        Migration("admin_asset.001", "admin_asset_feature_migrations", apply_admin_asset),
        Migration("arena.001", "arena_feature_migrations", apply_arena),
        Migration("auction.001", "auction_feature_migrations", apply_auction),
        Migration("back.001", "back_feature_migrations", apply_back),
        Migration("bank.001", "bank_feature_migrations", apply_bank),
        Migration("base.001", "base_feature_migrations", apply_base),
        Migration("beg.001", "beg_feature_migrations", apply_beg),
        Migration("boss.001", "boss_feature_migrations", apply_boss),
        Migration("buff.001", "buff_feature_migrations", apply_buff),
        Migration("combat_settlement.001", "combat_settlement_feature_migrations", apply_combat_settlement),
        Migration("daily_fortune.001", "daily_fortune_claims", apply_daily_fortune),
        Migration("dungeon.001", "dungeon_feature_migrations", apply_dungeon),
        Migration("illusion.001", "illusion_feature_migrations", apply_illusion),
        Migration("interactive.001", "interactive_feature_migrations", apply_interactive),
        *(Migration(version, f"{version.replace('.', '_')}_migrations", migration) for version, migration in LEGACY_MIGRATIONS),
        Migration("map.001", "map_feature_migrations", apply_map),
        Migration("mixelixir.001", "mixelixir_feature_migrations", apply_mixelixir),
        Migration("natal_treasure.001", "natal_treasure_feature_migrations", apply_natal_treasure),
        Migration("package_reward.001", "package_reward_operations", apply_package_reward),
        Migration("pet.001", "pet_feature_migrations", apply_pet),
        Migration("puppet.001", "puppet_feature_migrations", apply_puppet),
        Migration("rift.001", "rift_feature_migrations", apply_rift),
        Migration("sect.001", "sect_feature_migrations", apply_sect),
        Migration("sect_fairyland.001", "sect_fairyland_feature_migrations", apply_sect_fairyland),
        Migration("sign_in.001", "sign_in_operations", apply_sign_in),
        Migration("sign_in.002", "sign_in_statistics_events", apply_sign_in_statistics),
        Migration("stone_gift.001", "stone_gift_operations", apply_stone_gift),
        Migration("stone_gift.002", "stone_gift_limits", apply_stone_gift_limits),
        Migration("tianti_settlement.001", "tianti_settlement_feature_migrations", apply_tianti_settlement),
        Migration("tianti_training.001", "tianti_training_feature_migrations", apply_tianti_training),
        Migration("title.001", "title_feature_migrations", apply_title),
        Migration("tower.001", "tower_feature_migrations", apply_tower),
        Migration("trade.001", "trade_feature_migrations", apply_trade),
        Migration("work.001", "work_feature_migrations", apply_work),
        Migration("world_events.001", "world_events_feature_migrations", apply_world_events),
    )


def build_registry(*, disabled: set[str] | frozenset[str] | tuple[str, ...] = ()) -> FeatureRegistry:
    # Normalize the public input contract before applying derived feature
    # switches.  Callers may pass an immutable collection when constructing a
    # read-only manifest view.
    disabled = set(disabled)
    registry = FeatureRegistry()
    registry.register(PLATFORM_WEB_FEATURE)
    if ACCESSORY_PACKAGE_FEATURE.key not in disabled:
        registry.register(ACCESSORY_PACKAGE_FEATURE)
    if ACTIVITY_REWARD_FEATURE.key not in disabled:
        registry.register(ACTIVITY_REWARD_FEATURE)
    if ADMIN_ASSET_FEATURE.key not in disabled:
        registry.register(ADMIN_ASSET_FEATURE)
    if ARENA_FEATURE.key not in disabled:
        registry.register(ARENA_FEATURE)
    if AUCTION_FEATURE.key not in disabled:
        registry.register(AUCTION_FEATURE)
    if BACK_FEATURE.key not in disabled:
        registry.register(BACK_FEATURE)
    if BANK_FEATURE.key not in disabled:
        registry.register(BANK_FEATURE)
    if BASE_FEATURE.key not in disabled:
        registry.register(BASE_FEATURE)
    if BEG_FEATURE.key not in disabled:
        registry.register(BEG_FEATURE)
    if BOSS_FEATURE.key not in disabled:
        registry.register(BOSS_FEATURE)
    if BUFF_FEATURE.key not in disabled:
        registry.register(BUFF_FEATURE)
    if COMBAT_SETTLEMENT_FEATURE.key not in disabled:
        registry.register(COMBAT_SETTLEMENT_FEATURE)
    if DAILY_FORTUNE_FEATURE.key not in disabled:
        registry.register(DAILY_FORTUNE_FEATURE)
    if DUNGEON_FEATURE.key not in disabled:
        registry.register(DUNGEON_FEATURE)
    if ILLUSION_FEATURE.key not in disabled:
        registry.register(ILLUSION_FEATURE)
    if INTERACTIVE_FEATURE.key not in disabled:
        registry.register(INTERACTIVE_FEATURE)
    for feature in LEGACY_MIGRATED_FEATURES:
        if feature.key not in disabled:
            registry.register(feature)
    if MAP_FEATURE.key not in disabled:
        registry.register(MAP_FEATURE)
    if MIXELIXIR_FEATURE.key not in disabled:
        registry.register(MIXELIXIR_FEATURE)
    if NATAL_TREASURE_FEATURE.key not in disabled:
        registry.register(NATAL_TREASURE_FEATURE)
    if PACKAGE_REWARD_FEATURE.key not in disabled:
        registry.register(PACKAGE_REWARD_FEATURE)
    if PET_FEATURE.key not in disabled:
        registry.register(PET_FEATURE)
    if PUPPET_FEATURE.key not in disabled:
        registry.register(PUPPET_FEATURE)
    if RIFT_FEATURE.key not in disabled:
        registry.register(RIFT_FEATURE)
    if SECT_FEATURE.key not in disabled:
        registry.register(SECT_FEATURE)
    if SECT_FAIRYLAND_FEATURE.key not in disabled:
        registry.register(SECT_FAIRYLAND_FEATURE)
    if SIGN_IN_FEATURE.key not in disabled:
        registry.register(SIGN_IN_FEATURE)
    if STONE_GIFT_FEATURE.key not in disabled:
        registry.register(STONE_GIFT_FEATURE)
    if TIANTI_SETTLEMENT_FEATURE.key not in disabled:
        registry.register(TIANTI_SETTLEMENT_FEATURE)
    if TIANTI_TRAINING_FEATURE.key not in disabled:
        registry.register(TIANTI_TRAINING_FEATURE)
    if TITLE_FEATURE.key not in disabled:
        registry.register(TITLE_FEATURE)
    if TOWER_FEATURE.key not in disabled:
        registry.register(TOWER_FEATURE)
    if TRADE_FEATURE.key not in disabled:
        registry.register(TRADE_FEATURE)
    if WORK_FEATURE.key not in disabled:
        registry.register(WORK_FEATURE)
    if WORLD_EVENTS_FEATURE.key not in disabled:
        registry.register(WORLD_EVENTS_FEATURE)
    registry.register(LEGACY_SCHEDULER_FEATURE)
    registry.register_many(LEGACY_FEATURES)
    return registry


def build_lifecycle(context: RuntimeContext | None = None) -> tuple[Lifecycle, Readiness, RuntimeContext]:
    context = context or build_runtime_context()
    lifecycle = Lifecycle()
    readiness = Readiness()
    phase_state: dict[str, bool] = {}
    disabled = set()
    if context.settings is not None and not context.settings.get("daily_fortune_enabled", True):
        disabled.add(DAILY_FORTUNE_FEATURE.key)
    if context.settings is not None and not context.settings.get("illusion_enabled", True):
        disabled.add(ILLUSION_FEATURE.key)
    if context.settings is not None and not context.settings.get("interactive_enabled", True):
        disabled.add(INTERACTIVE_FEATURE.key)
    if context.settings is not None and not context.settings.get("beg_enabled", True):
        disabled.add(BEG_FEATURE.key)
    if context.settings is not None and not context.settings.get("title_enabled", True):
        disabled.add(TITLE_FEATURE.key)
    if context.settings is not None and not context.settings.get("sign_in_enabled", True):
        disabled.add(SIGN_IN_FEATURE.key)
    if context.settings is not None and not context.settings.get("stone_gift_enabled", True):
        disabled.add(STONE_GIFT_FEATURE.key)
    if context.settings is not None and not context.settings.get("accessory_package_enabled", True):
        disabled.add(ACCESSORY_PACKAGE_FEATURE.key)
    if context.settings is not None and not context.settings.get("arena_enabled", True):
        disabled.add(ARENA_FEATURE.key)
    if context.settings is not None and not context.settings.get("auction_enabled", True):
        disabled.add(AUCTION_FEATURE.key)
    if context.settings is not None and not context.settings.get("bank_enabled", True):
        disabled.add(BANK_FEATURE.key)
    if context.settings is not None and not context.settings.get("activity_reward_enabled", True):
        disabled.add(ACTIVITY_REWARD_FEATURE.key)
    if context.settings is not None and not context.settings.get("combat_settlement_enabled", True):
        disabled.add(COMBAT_SETTLEMENT_FEATURE.key)
    if context.settings is not None and not context.settings.get("admin_asset_enabled", True):
        disabled.add(ADMIN_ASSET_FEATURE.key)
    if context.settings is not None and not context.settings.get("tianti_settlement_enabled", True):
        disabled.add(TIANTI_SETTLEMENT_FEATURE.key)
    if context.settings is not None and not context.settings.get("tianti_training_enabled", True):
        disabled.add(TIANTI_TRAINING_FEATURE.key)
    if context.settings is not None and not context.settings.get("tower_enabled", True):
        disabled.add(TOWER_FEATURE.key)
    if context.settings is not None and not context.settings.get("sect_fairyland_enabled", True):
        disabled.add(SECT_FAIRYLAND_FEATURE.key)
    if context.settings is not None and not context.settings.get("world_events_enabled", True):
        disabled.add(WORLD_EVENTS_FEATURE.key)
    if context.settings is not None and not context.settings.get("work_claim_enabled", True):
        disabled.add(WORK_FEATURE.key)
    if context.settings is not None and not context.settings.get("mixelixir_enabled", True):
        disabled.add(MIXELIXIR_FEATURE.key)
    if context.settings is not None and not context.settings.get("puppet_enabled", True):
        disabled.add(PUPPET_FEATURE.key)
    if context.settings is not None and not context.settings.get("boss_enabled", True):
        disabled.add(BOSS_FEATURE.key)
    if context.settings is not None and not context.settings.get("dungeon_enabled", True):
        disabled.add(DUNGEON_FEATURE.key)
    if context.settings is not None and not context.settings.get("pet_enabled", True):
        disabled.add(PET_FEATURE.key)
    if context.settings is not None and not context.settings.get("sect_enabled", True):
        disabled.add(SECT_FEATURE.key)
    for feature in (BASE_FEATURE, BACK_FEATURE, BUFF_FEATURE, MAP_FEATURE, NATAL_TREASURE_FEATURE, RIFT_FEATURE, TRADE_FEATURE):
        if context.settings is not None and not context.settings.get(f"{feature.key}_enabled", True):
            disabled.add(feature.key)
    for feature in LEGACY_MIGRATED_FEATURES:
        if context.settings is not None and not context.settings.get(f"{feature.key}_enabled", True):
            disabled.add(feature.key)
    registry = context.registry or build_registry(disabled=disabled)
    context.registry = registry
    context.compatibility = compatibility_hits
    context.jobs = JobRegistry()
    context.readiness = readiness

    migration_runner = MigrationRunner(build_migrations(), clock=context.clock)
    context.migrations = migration_runner

    def ensure_filesystem() -> None:
        context.paths.data.mkdir(parents=True, exist_ok=True)
        context.paths.backups.mkdir(parents=True, exist_ok=True)
        context.paths.logs.mkdir(parents=True, exist_ok=True)
        phase_state["filesystem"] = True

    def ensure_database() -> None:
        for spec in context.database.specs():
            if not str(spec.path):
                raise RuntimeError("database path is empty")
            spec.path.parent.mkdir(parents=True, exist_ok=True)
        primary = context.database.path("game_db")
        with DatabaseUnitOfWork(primary) as uow:
            OperationLedger(clock=context.clock).ensure_schema(uow)
            OutboxStore(clock=context.clock).ensure_schema(uow)
        phase_state["database"] = True

    def ensure_migrations() -> None:
        for spec in context.database.specs():
            with DatabaseUnitOfWork(spec.path) as uow:
                if spec.key == "game_db":
                    runner = migration_runner
                elif spec.key == "player_db":
                    runner = MigrationRunner(
                        tuple(migration for migration in migration_runner.migrations if migration.version == "title.001"),
                        clock=context.clock,
                    )
                else:
                    runner = MigrationRunner((), clock=context.clock)
                runner.apply(uow)
        phase_state["migrations"] = True

    def ensure_repositories() -> None:
        from .features.daily_fortune.application import DailyFortuneApplication
        from .features.illusion.application import IllusionApplication
        from .features.interactive.application import InteractiveApplication
        from .features.sign_in.application import SignInApplication
        from .features.stone_gift.application import StoneGiftApplication
        from .features.package_reward.application import PackageRewardApplication
        from .features.accessory_package.application import AccessoryPackageApplication
        from .features.arena.application import ArenaApplication
        from .features.arena.repository import LegacyArenaRepository
        from .features.auction.application import AuctionBidApplication
        from .features.auction.settlement import AuctionSettlementApplication, LegacyAuctionSettlementRepository
        from .features.bank.application import BankApplication
        from .features.bank.repository import LegacyBankRepository
        from .features.activity_reward.application import ActivityRewardApplication
        from .features.activity_reward.repository import LegacyActivityRewardRepository
        from .features.combat_settlement.application import CombatSettlementApplication
        from .features.combat_settlement.repository import LegacyCombatSettlementRepository
        from .features.admin_asset.application import AdminAssetApplication
        from .features.admin_asset.repository import LegacyAdminStoneRepository
        from .features.tianti_settlement.application import TiantiSettlementApplication
        from .features.tianti_settlement.repository import LegacyTiantiSettlementRepository
        from .features.tianti_training.application import TiantiTrainingApplication
        from .features.tianti_training.repository import LegacyTiantiTrainingRepository
        from .features.tower.application import TowerApplication
        from .features.tower.repository import LegacyTowerRepository
        from .features.sect_fairyland.application import SectFairylandApplication
        from .features.sect_fairyland.repository import LegacySectFairylandRepository
        from .features.world_events.application import DemonClaimApplication
        from .features.world_events.repository import LegacyWorldEventClaimRepository
        from .features.work.application import WorkClaimApplication
        from .features.work.repository import LegacyWorkClaimRepository
        from .features.mixelixir.application import MixelixirApplication
        from .features.mixelixir.repository import LegacyMixelixirRepository
        from .features.puppet.application import PuppetApplication
        from .features.puppet.repository import LegacyPuppetRepository
        from .features.boss.application import BossApplication
        from .features.boss.repository import LegacyBossRepository
        from .features.dungeon.application import DungeonApplication
        from .features.dungeon.repository import LegacyDungeonRepository
        from .features.pet.application import PetApplication
        from .features.pet.repository import LegacyPetRepository
        from .features.sect.application import SectApplication
        from .features.sect.repository import LegacySectRepository
        from .features.natal_treasure.application import NatalTreasureApplication
        from .features.buff.application import BuffApplication
        from .features.base.application import BaseApplication
        from .features.back.application import BackApplication
        from .features.trade.application import TradeApplication
        from .features.map.application import MapApplication
        from .features.rift.application import RiftApplication

        settings = context.settings
        lower = int(settings.get("sign_in_lower_limit", 100000)) if settings is not None else 100000
        upper = int(settings.get("sign_in_upper_limit", 500000)) if settings is not None else 500000
        fee_rate = float(settings.get("stone_gift_fee_rate", 0.1)) if settings is not None else 0.1
        sign_in_effects = None
        if context.legacy_startup:
            # Keep historical lottery/statistics/task behavior at the adapter
            # boundary while the side effects are migrated independently.
            try:
                from nonebot import get_driver

                get_driver()
            except ValueError:
                # CLI/serve maintenance contexts do not own a NoneBot driver.
                # Do not import legacy adapters there; the real driver process
                # wires them after plugin loading.
                sign_in_effects = None
            else:
                from .compatibility.sign_in_effects import LegacySignInEffects
                from .features.sign_in.statistics import SignInStatisticsRepository
                from .features.sign_in.tasks import SignInTaskEffects
                from .features.sign_in.lottery_application import LotteryApplication
                from .features.sign_in.lottery_repository import LotteryRepository
                from .xiuxian.xiuxian_base.transaction_service import LotterySettlementService
                from .xiuxian.xiuxian_tasks.task_data import record_task_progress
                from .xiuxian.xiuxian_utils.utils import log_message, update_statistics_value

                legacy_lottery = os.environ.get("XIUXIAN_SIGN_IN_LEGACY_LOTTERY", "false").strip().lower() in {"1", "true", "yes", "on"}
                lottery_service = LotterySettlementService(
                    context.database.path("game_db"),
                    Path(__file__).parent / "xiuxian" / "xiuxian_base" / "lottery_pool.json",
                ) if legacy_lottery else LotteryApplication(
                    str(context.database.path("game_db")), repository=LotteryRepository(), clock=context.clock, random_source=context.random,
                )
                sign_in_effects = LegacySignInEffects(
                    context.database.path("game_db"),
                    lottery_service=lottery_service,
                    clock=context.clock,
                    task_progress=record_task_progress,
                    statistics=update_statistics_value,
                    statistics_repository=SignInStatisticsRepository(str(context.database.path("game_db"))),
                    task_effects=SignInTaskEffects(record_task_progress),
                    logger=log_message,
                )
        context.services = {
            "daily_fortune": DailyFortuneApplication(str(context.database.path("game_db")), clock=context.clock, random_source=context.random),
            "illusion": IllusionApplication(str(context.database.path("game_db")), clock=context.clock),
            "interactive": InteractiveApplication(str(context.database.path("game_db"))),
            "beg": BegApplication(str(context.database.path("game_db"))),
            "title": TitleApplication(str(context.database.path("player_db"))),
            "sign_in": SignInApplication(
                str(context.database.path("game_db")),
                random_source=context.random,
                clock=context.clock,
                lower_limit=lower,
                upper_limit=upper,
                effects=sign_in_effects,
            ),
            "stone_gift": StoneGiftApplication(
                str(context.database.path("game_db")),
                fee_rate=fee_rate,
                clock=context.clock,
            ),
            "package_reward": PackageRewardApplication(str(context.database.path("game_db"))),
            "accessory_package": AccessoryPackageApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
            ),
            "arena": ArenaApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                repository=LegacyArenaRepository(
                    str(context.database.path("game_db")),
                    str(context.database.path("player_db")),
                ),
            ),
            "auction": AuctionBidApplication(str(context.database.path("game_db"))),
            "auction_settlement": AuctionSettlementApplication(
                str(context.database.path("game_db")),
                repository=LegacyAuctionSettlementRepository(
                    str(context.database.path("game_db")),
                    str(context.database.path("trade_db")),
                ),
            ),
            "bank": BankApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                repository=LegacyBankRepository(
                    str(context.database.path("game_db")),
                    str(context.database.path("player_db")),
                ),
            ),
            "activity_reward": ActivityRewardApplication(
                str(context.database.path("game_db")),
                repository=LegacyActivityRewardRepository(context.paths.data / "activity" / "activity.db"),
            ),
            "combat_settlement": CombatSettlementApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                repository=LegacyCombatSettlementRepository(
                    str(context.database.path("game_db")),
                    str(context.database.path("player_db")),
                ),
            ),
            "admin_asset": AdminAssetApplication(
                str(context.database.path("game_db")),
                repository=LegacyAdminStoneRepository(str(context.database.path("game_db"))),
            ),
            "tianti_settlement": TiantiSettlementApplication(
                str(context.database.path("player_db")),
                repository=LegacyTiantiSettlementRepository(str(context.database.path("player_db"))),
            ),
            "tianti_training": TiantiTrainingApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                repository=LegacyTiantiTrainingRepository(
                    str(context.database.path("game_db")),
                    str(context.database.path("player_db")),
                ),
            ),
            "tower": TowerApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                repository=LegacyTowerRepository(
                    str(context.database.path("game_db")),
                    str(context.database.path("player_db")),
                ),
            ),
            "sect_fairyland": SectFairylandApplication(
                str(context.database.path("player_db")),
                repository=LegacySectFairylandRepository(str(context.database.path("player_db"))),
            ),
            "world_events": DemonClaimApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                repository=LegacyWorldEventClaimRepository(
                    str(context.database.path("game_db")),
                    str(context.database.path("player_db")),
                ),
            ),
            "work": WorkClaimApplication(
                str(context.database.path("game_db")),
                repository=LegacyWorkClaimRepository(str(context.database.path("game_db"))),
            ),
            "mixelixir": MixelixirApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                repository=LegacyMixelixirRepository(
                    str(context.database.path("game_db")),
                    str(context.database.path("player_db")),
                ),
            ),
            "puppet": PuppetApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                repository=LegacyPuppetRepository(
                    str(context.database.path("game_db")),
                    str(context.database.path("player_db")),
                ),
            ),
            "boss": BossApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                activity_database=context.paths.data / "activity" / "activity.db",
                repository=LegacyBossRepository(
                    str(context.database.path("game_db")),
                    str(context.database.path("player_db")),
                    context.paths.data / "activity" / "activity.db",
                ),
            ),
            "dungeon": DungeonApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                repository=LegacyDungeonRepository(
                    str(context.database.path("game_db")),
                    str(context.database.path("player_db")),
                ),
            ),
            "pet": PetApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                repository=LegacyPetRepository(
                    str(context.database.path("game_db")),
                    str(context.database.path("player_db")),
                ),
            ),
            "sect": SectApplication(
                str(context.database.path("game_db")),
                repository=LegacySectRepository(str(context.database.path("game_db"))),
            ),
        }
        context.services.update({
            "natal_treasure": NatalTreasureApplication(str(context.database.path("player_db")), str(context.database.path("game_db"))),
            "buff": BuffApplication(str(context.database.path("game_db")), str(context.database.path("player_db"))),
            "base": BaseApplication(str(context.database.path("game_db")), str(context.database.path("player_db"))),
            "back": BackApplication(str(context.database.path("game_db")), str(context.database.path("player_db"))),
            "trade": TradeApplication(str(context.database.path("game_db")), str(context.database.path("trade_db"))),
            "map": MapApplication(str(context.database.path("game_db")), str(context.database.path("player_db"))),
            "rift": RiftApplication(str(context.database.path("game_db")), str(context.database.path("player_db"))),
        })
        for feature_key, application_type in LEGACY_MIGRATED_APPLICATIONS.items():
            context.services[feature_key] = application_type(str(context.database.path("game_db")))
        context.reconcile_handlers = {
            "accessory_package.open": context.services["accessory_package"].reconcile,
        }
        phase_state["repositories"] = True

    async def ensure_jobs() -> None:
        from .compatibility.scheduler import activate_scheduler_bridge

        activate_scheduler_bridge()
        # Legacy APScheduler keeps its decorators for the compatibility
        # release, but registration is activated here rather than during
        # module import. The registry points at the same stable job IDs so
        # CLI/Web manual runs do not execute an unwired placeholder.
        for feature in registry.features:
            for job in feature.jobs:
                if feature.key == LEGACY_SCHEDULER_FEATURE.key:
                    handler = legacy_job_handler(job.id)
                elif feature.key == AUCTION_FEATURE.key and job.id == "auction.settle":
                    handler = lambda scheduled_at=None, _app=context.services["auction_settlement"]: auction_settle_job(
                        _app,
                        scheduled_at=scheduled_at,
                        clock=context.clock,
                        ids=context.ids,
                    )
                else:
                    handler = None
                context.jobs.register_manifest(job, handler=handler)
        context.job_executor = JobExecutor(context.jobs)
        # Compatibility modules register callbacks explicitly with the
        # bootstrap bridge.  They are executed once after all new runtime
        # resources are ready, never as import-time driver hooks.
        if context.legacy_startup:
            await run_legacy_startup()
        phase_state["jobs"] = True

    async def shutdown_jobs() -> None:
        if context.legacy_startup:
            await run_legacy_shutdown()
        phase_state["jobs"] = False

    def ensure_web() -> None:
        from .adapters.web.app import create_app

        context.web_app = create_app(context=context, registry=registry, readiness=readiness)
        phase_state["web"] = True

    def shutdown_web() -> None:
        context.web_app = None
        phase_state["web"] = False

    lifecycle.register(LifecyclePhase.FILESYSTEM, ensure_filesystem)
    lifecycle.register(LifecyclePhase.DATABASE, ensure_database)
    lifecycle.register(LifecyclePhase.MIGRATIONS, ensure_migrations)
    lifecycle.register(LifecyclePhase.REPOSITORIES, ensure_repositories)
    lifecycle.register(LifecyclePhase.JOBS, ensure_jobs, shutdown=shutdown_jobs)
    lifecycle.register(LifecyclePhase.WEB, ensure_web, shutdown=shutdown_web)
    for name in ("filesystem", "database", "migrations", "repositories", "jobs", "web"):
        readiness.register(name, lambda name=name: phase_state.get(name, False))
    return lifecycle, readiness, context


def manifest_json() -> dict[str, Any]:
    return build_registry().export()


async def startup(context: RuntimeContext | None = None):
    context = context or build_runtime_context()
    disabled = set()
    if context.settings is not None and not context.settings.get("daily_fortune_enabled", True):
        disabled.add(DAILY_FORTUNE_FEATURE.key)
    if context.settings is not None and not context.settings.get("illusion_enabled", True):
        disabled.add(ILLUSION_FEATURE.key)
    if context.settings is not None and not context.settings.get("sign_in_enabled", True):
        disabled.add(SIGN_IN_FEATURE.key)
    if context.settings is not None and not context.settings.get("stone_gift_enabled", True):
        disabled.add(STONE_GIFT_FEATURE.key)
    if context.settings is not None and not context.settings.get("accessory_package_enabled", True):
        disabled.add(ACCESSORY_PACKAGE_FEATURE.key)
    if context.settings is not None and not context.settings.get("arena_enabled", True):
        disabled.add(ARENA_FEATURE.key)
    if context.settings is not None and not context.settings.get("auction_enabled", True):
        disabled.add(AUCTION_FEATURE.key)
    if context.settings is not None and not context.settings.get("bank_enabled", True):
        disabled.add(BANK_FEATURE.key)
    if context.settings is not None and not context.settings.get("activity_reward_enabled", True):
        disabled.add(ACTIVITY_REWARD_FEATURE.key)
    if context.settings is not None and not context.settings.get("combat_settlement_enabled", True):
        disabled.add(COMBAT_SETTLEMENT_FEATURE.key)
    if context.settings is not None and not context.settings.get("admin_asset_enabled", True):
        disabled.add(ADMIN_ASSET_FEATURE.key)
    if context.settings is not None and not context.settings.get("tianti_settlement_enabled", True):
        disabled.add(TIANTI_SETTLEMENT_FEATURE.key)
    if context.settings is not None and not context.settings.get("tianti_training_enabled", True):
        disabled.add(TIANTI_TRAINING_FEATURE.key)
    if context.settings is not None and not context.settings.get("tower_enabled", True):
        disabled.add(TOWER_FEATURE.key)
    if context.settings is not None and not context.settings.get("sect_fairyland_enabled", True):
        disabled.add(SECT_FAIRYLAND_FEATURE.key)
    if context.settings is not None and not context.settings.get("world_events_enabled", True):
        disabled.add(WORLD_EVENTS_FEATURE.key)
    if context.settings is not None and not context.settings.get("work_claim_enabled", True):
        disabled.add(WORK_FEATURE.key)
    if context.settings is not None and not context.settings.get("mixelixir_enabled", True):
        disabled.add(MIXELIXIR_FEATURE.key)
    if context.settings is not None and not context.settings.get("puppet_enabled", True):
        disabled.add(PUPPET_FEATURE.key)
    if context.settings is not None and not context.settings.get("boss_enabled", True):
        disabled.add(BOSS_FEATURE.key)
    if context.settings is not None and not context.settings.get("dungeon_enabled", True):
        disabled.add(DUNGEON_FEATURE.key)
    if context.settings is not None and not context.settings.get("pet_enabled", True):
        disabled.add(PET_FEATURE.key)
    if context.settings is not None and not context.settings.get("sect_enabled", True):
        disabled.add(SECT_FEATURE.key)
    for feature in (BASE_FEATURE, BACK_FEATURE, BUFF_FEATURE, MAP_FEATURE, NATAL_TREASURE_FEATURE, RIFT_FEATURE, TRADE_FEATURE):
        if context.settings is not None and not context.settings.get(f"{feature.key}_enabled", True):
            disabled.add(feature.key)
    for feature in LEGACY_MIGRATED_FEATURES:
        if context.settings is not None and not context.settings.get(f"{feature.key}_enabled", True):
            disabled.add(feature.key)
    registry = build_registry(disabled=disabled)
    context.registry = registry
    lifecycle, readiness, context = build_lifecycle(context)
    context.lifecycle = lifecycle
    state = await lifecycle.start()
    return state, readiness, context, lifecycle


async def shutdown(lifecycle: Lifecycle):
    return await lifecycle.shutdown()


def install_driver_hooks(driver: Any) -> tuple[Any, Any]:
    """Install the single new-runtime hook pair on a NoneBot driver.

    The marker is stored on the driver rather than in a module global so test
    drivers and hot-reload drivers remain independent. Legacy modules still
    own their deprecated hooks during the compatibility release; this pair is
    the authoritative readiness/database boundary.
    """
    marker = "_xiuxian_refactored_lifecycle_hooks"
    existing = getattr(driver, marker, None)
    if existing is not None:
        return existing
    holder: dict[str, Any] = {"context": None, "lifecycle": None}

    # Matchers are wired once, before startup, but resolve applications only
    # after the lifecycle has assembled repositories and migrations.
    from .adapters.nonebot import register_migrated_matchers

    register_migrated_matchers(driver, holder)

    async def on_startup() -> None:
        data_dir = getattr(getattr(driver, "config", None), "xiuxian_data_dir", None)
        context = build_runtime_context(data_dir=data_dir, legacy_startup=True)
        if context.message_gateway is None or getattr(context.message_gateway, "sender", None) is None:
            from .adapters.nonebot import NoneBotMessageGateway
            from .xiuxian.messaging.delivery import delivery_service

            context.message_gateway = NoneBotMessageGateway(delivery_service)
        state, readiness, context, lifecycle = await startup(context)
        holder.update(context=context, lifecycle=lifecycle, readiness=readiness)
        if state.phase is LifecyclePhase.NOT_READY:
            raise RuntimeError(state.error or "xiuxian runtime is not ready")

    async def on_shutdown() -> None:
        lifecycle = holder.get("lifecycle")
        if lifecycle is not None:
            await shutdown(lifecycle)
            holder["lifecycle"] = None

    driver.on_startup(on_startup)
    driver.on_shutdown(on_shutdown)
    hooks = (on_startup, on_shutdown)
    setattr(driver, marker, hooks)
    return hooks


__all__ = ["build_lifecycle", "build_migrations", "build_registry", "install_driver_hooks", "manifest_json", "startup", "shutdown"]
