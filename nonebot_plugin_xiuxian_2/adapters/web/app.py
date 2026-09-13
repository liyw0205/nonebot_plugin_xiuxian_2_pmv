from __future__ import annotations

from time import monotonic
from typing import Any, Iterable

from ...bootstrap import FeatureRegistry, Readiness, RuntimeContext, build_runtime_context
from ...features.daily_fortune.application import DailyFortuneApplication
from ...features.daily_fortune.web import blueprint as daily_fortune_blueprint
from ...features.sign_in.application import SignInApplication
from ...features.sign_in.web import blueprint as sign_in_blueprint
from ...features.stone_gift.application import StoneGiftApplication
from ...features.stone_gift.web import blueprint as stone_gift_blueprint
from ...features.accessory_package.web import blueprint as accessory_package_blueprint
from ...features.arena.web import blueprint as arena_blueprint
from ...features.auction.web import blueprint as auction_blueprint
from ...features.bank.web import blueprint as bank_blueprint
from ...features.activity_reward.web import blueprint as activity_reward_blueprint
from ...features.combat_settlement.web import blueprint as combat_settlement_blueprint
from ...features.admin_asset.web import blueprint as admin_asset_blueprint
from ...features.tianti_settlement.web import blueprint as tianti_settlement_blueprint
from ...features.tianti_training.web import blueprint as tianti_training_blueprint
from ...features.tower.web import blueprint as tower_blueprint
from ...features.sect_fairyland.web import blueprint as sect_fairyland_blueprint
from ...features.world_events.web import blueprint as world_events_blueprint
from ...features.work.web import blueprint as work_blueprint
from ...features.mixelixir.web import blueprint as mixelixir_blueprint
from ...features.puppet.web import blueprint as puppet_blueprint
from ...features.boss.web import blueprint as boss_blueprint
from ...features.dungeon.web import blueprint as dungeon_blueprint
from ...features.pet.web import blueprint as pet_blueprint
from ...features.sect.web import blueprint as sect_blueprint
from ...features.natal_treasure.web import blueprint as natal_treasure_blueprint
from ...features.buff.web import blueprint as buff_blueprint
from ...features.base.web import blueprint as base_blueprint
from ...features.back.web import blueprint as back_blueprint
from ...features.trade.web import blueprint as trade_blueprint
from ...features.map.web import blueprint as map_blueprint
from ...features.rift.web import blueprint as rift_blueprint
from .api import api_error, api_success
from .auth import csrf_token
from .auth import HostPolicy


def create_app(
    *,
    context: RuntimeContext | None = None,
    registry: FeatureRegistry | None = None,
    readiness: Readiness | None = None,
    feature_blueprints: Iterable[Any] = (),
):
    """Create the refactored Flask app without importing the legacy global app."""
    from flask import Flask, g, request, session

    app = Flask(__name__)
    context = context or build_runtime_context()
    settings = context.settings
    configured_secret = settings.get("web_secret_key", "") if settings is not None and hasattr(settings, "get") else ""
    app.secret_key = configured_secret or "refactored-runtime-development-key"
    app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Lax")
    app.jinja_env.globals["csrf_token"] = csrf_token
    if registry is None:
        registry = context.registry
    if registry is None:
        from ...plugin import build_registry

        registry = build_registry()
    context.registry = registry
    readiness = readiness or Readiness()
    configured_hosts = settings.get("web_allowed_hosts", ()) if settings is not None and hasattr(settings, "get") else ()
    if isinstance(configured_hosts, str):
        configured_hosts = tuple(item.strip() for item in configured_hosts.split(",") if item.strip())
    host_policy = HostPolicy(configured_hosts or ("127.0.0.1", "localhost", "::1"))

    def has_permission(required: str) -> bool:
        if required in {"public", "user"}:
            return True
        role = session.get("role") or request.headers.get("X-Role", "")
        return required == "admin" and str(role).casefold() in {"admin", "superuser"}

    app.config["XIUXIAN_PERMISSION_RESOLVER"] = has_permission

    def request_metadata(name: str) -> str:
        header = {
            "operation_id": ("Idempotency-Key", "X-Operation-ID"),
            "job_id": ("X-Job-ID",),
            "user_scope": ("X-User-ID",),
        }.get(name, ())
        for key in header:
            value = request.headers.get(key)
            if value:
                return str(value).strip()[:128]
        value = request.args.get(name)
        if value:
            return str(value).strip()[:128]
        if name == "operation_id" and request.is_json:
            payload = request.get_json(silent=True) or {}
            if isinstance(payload, dict) and payload.get(name):
                return str(payload[name]).strip()[:128]
        if name == "user_scope":
            return str(session.get("admin_id") or session.get("role") or "web")[:128]
        return ""

    @app.before_request
    def enforce_host_policy():
        if not host_policy.allows(request.host):
            return api_error("invalid_host", "请求主机不在允许列表", status=400)

    @app.before_request
    def bind_request_id():
        from .api import request_id
        from ...infrastructure.observability import trace_context

        g.request_id = request_id(context.ids)
        g.request_started_at = monotonic()
        g.operation_id = request_metadata("operation_id")
        g.job_id = request_metadata("job_id")
        g.user_scope = request_metadata("user_scope")
        manager = trace_context(
            request_id=g.request_id,
            operation_id=g.operation_id or None,
            job_id=g.job_id or None,
            user_scope=g.user_scope,
            ids=context.ids,
        )
        manager.__enter__()
        g.trace_manager = manager

    @app.after_request
    def expose_request_id(response):
        response.headers["X-Request-ID"] = getattr(g, "request_id", "")
        if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            try:
                from ...infrastructure.observability import AuditLogger

                AuditLogger(context.database.path("game_db"), clock=context.clock).record(
                    request_id=getattr(g, "request_id", ""),
                    method=request.method,
                    path=request.path,
                    status=response.status_code,
                    actor=str(session.get("role") or request.headers.get("X-Role", "")),
                    operation_id=getattr(g, "operation_id", ""),
                    job_id=getattr(g, "job_id", ""),
                    user_scope=getattr(g, "user_scope", ""),
                    duration_ms=int((monotonic() - getattr(g, "request_started_at", monotonic())) * 1000),
                )
            except Exception:
                # Auditing must never turn a successful business response into
                # a 500; the database readiness check surfaces storage faults.
                pass
        try:
            from ...infrastructure.observability import emit

            emit(
                "info",
                "web request",
                request_id=getattr(g, "request_id", ""),
                operation_id=getattr(g, "operation_id", ""),
                job_id=getattr(g, "job_id", ""),
                user_scope=getattr(g, "user_scope", ""),
                duration_ms=int((monotonic() - getattr(g, "request_started_at", monotonic())) * 1000),
                method=request.method,
                path=request.path,
                status=response.status_code,
            )
        except Exception:
            pass
        manager = getattr(g, "trace_manager", None)
        if manager is not None:
            manager.__exit__(None, None, None)
        return response

    @app.errorhandler(404)
    def not_found(error):
        if request.path.startswith("/api/"):
            return api_error("not_found", "接口不存在", status=404)
        return error

    @app.errorhandler(405)
    def method_not_allowed(error):
        if request.path.startswith("/api/"):
            return api_error("method_not_allowed", "请求方法不支持", status=405)
        return error

    @app.errorhandler(500)
    def internal_error(error):
        if request.path.startswith("/api/"):
            return api_error("internal_error", "服务暂时不可用", status=500)
        return error

    @app.get("/api/v1/csrf")
    def csrf():
        return api_success({"token": csrf_token()})

    @app.get("/health/live")
    def live():
        return api_success({"status": "alive"})

    @app.get("/health/ready")
    def ready():
        report = readiness.report()
        return (api_success(report.to_dict()) if report.ready else api_error("not_ready", "服务尚未就绪", details=report.to_dict(), status=503))

    @app.get("/api/v1/registry")
    def registry_view():
        return api_success(registry.export())

    # Every feature blueprint receives an application service.  No route is
    # allowed to construct a repository or mutate a player directly.
    if any(feature.key == "daily_fortune" for feature in registry.features):
        fortune = (context.services or {}).get("daily_fortune") or DailyFortuneApplication(str(context.database.path("game_db")), clock=context.clock, random_source=context.random)
        app.register_blueprint(daily_fortune_blueprint(fortune, permission=has_permission, ids=context.ids))
    if any(feature.key == "sign_in" for feature in registry.features):
        settings = context.settings
        lower = int(settings.get("sign_in_lower_limit", 100000)) if settings is not None else 100000
        upper = int(settings.get("sign_in_upper_limit", 500000)) if settings is not None else 500000
        sign_in = (context.services or {}).get("sign_in") or SignInApplication(
            str(context.database.path("game_db")),
            clock=context.clock,
            lower_limit=lower,
            upper_limit=upper,
        )
        app.register_blueprint(sign_in_blueprint(sign_in, permission=has_permission, ids=context.ids))
    if any(feature.key == "stone_gift" for feature in registry.features):
        settings = context.settings
        fee_rate = float(settings.get("stone_gift_fee_rate", 0.1)) if settings is not None else 0.1
        stone_gift = (context.services or {}).get("stone_gift") or StoneGiftApplication(
            str(context.database.path("game_db")), fee_rate=fee_rate, clock=context.clock
        )
        app.register_blueprint(stone_gift_blueprint(stone_gift, permission=has_permission, ids=context.ids))
    if any(feature.key == "accessory_package" for feature in registry.features):
        from ...features.accessory_package.application import AccessoryPackageApplication

        accessory_package = (context.services or {}).get("accessory_package") or AccessoryPackageApplication(
            str(context.database.path("game_db")), str(context.database.path("player_db"))
        )
        app.register_blueprint(accessory_package_blueprint(accessory_package, permission=has_permission))
    if any(feature.key == "arena" for feature in registry.features):
        from ...features.arena.application import ArenaApplication

        arena = (context.services or {}).get("arena") or ArenaApplication(
            str(context.database.path("game_db")), str(context.database.path("player_db"))
        )
        app.register_blueprint(arena_blueprint(arena, permission=has_permission))
    if any(feature.key == "auction" for feature in registry.features):
        from ...features.auction.application import AuctionBidApplication
        from ...features.auction.settlement import AuctionSettlementApplication

        auction = (context.services or {}).get("auction") or AuctionBidApplication(str(context.database.path("game_db")))
        settlement = (context.services or {}).get("auction_settlement") or AuctionSettlementApplication(
            str(context.database.path("game_db"))
        )
        app.register_blueprint(
            auction_blueprint(
                auction,
                settlement=settlement,
                permission=has_permission,
                clock=context.clock,
                ids=context.ids,
            )
        )
    if any(feature.key == "bank" for feature in registry.features):
        from ...features.bank.application import BankApplication

        bank = (context.services or {}).get("bank") or BankApplication(
            str(context.database.path("game_db")), str(context.database.path("player_db"))
        )
        app.register_blueprint(bank_blueprint(bank, permission=has_permission))
    if any(feature.key == "activity_reward" for feature in registry.features):
        from ...features.activity_reward.application import ActivityRewardApplication

        activity_reward = (context.services or {}).get("activity_reward") or ActivityRewardApplication(
            str(context.database.path("game_db"))
        )
        app.register_blueprint(activity_reward_blueprint(activity_reward, permission=has_permission))
    if any(feature.key == "combat_settlement" for feature in registry.features):
        from ...features.combat_settlement.application import CombatSettlementApplication

        combat_settlement = (context.services or {}).get("combat_settlement") or CombatSettlementApplication(
            str(context.database.path("game_db")), str(context.database.path("player_db"))
        )
        app.register_blueprint(combat_settlement_blueprint(combat_settlement, permission=has_permission))
    if any(feature.key == "admin_asset" for feature in registry.features):
        from ...features.admin_asset.application import AdminAssetApplication

        admin_asset = (context.services or {}).get("admin_asset") or AdminAssetApplication(
            str(context.database.path("game_db"))
        )
        app.register_blueprint(admin_asset_blueprint(admin_asset, permission=has_permission))
    if any(feature.key == "tianti_settlement" for feature in registry.features):
        from ...features.tianti_settlement.application import TiantiSettlementApplication

        tianti_settlement = (context.services or {}).get("tianti_settlement") or TiantiSettlementApplication(
            str(context.database.path("player_db"))
        )
        app.register_blueprint(tianti_settlement_blueprint(tianti_settlement, permission=has_permission))
    if any(feature.key == "tianti_training" for feature in registry.features):
        from ...features.tianti_training.application import TiantiTrainingApplication

        tianti_training = (context.services or {}).get("tianti_training") or TiantiTrainingApplication(
            str(context.database.path("game_db")),
            str(context.database.path("player_db")),
        )
        app.register_blueprint(tianti_training_blueprint(tianti_training, permission=has_permission))
    if any(feature.key == "tower" for feature in registry.features):
        from ...features.tower.application import TowerApplication

        tower = (context.services or {}).get("tower") or TowerApplication(
            str(context.database.path("game_db")),
            str(context.database.path("player_db")),
        )
        app.register_blueprint(tower_blueprint(tower, permission=has_permission))
    if any(feature.key == "sect_fairyland" for feature in registry.features):
        from ...features.sect_fairyland.application import SectFairylandApplication

        sect_fairyland = (context.services or {}).get("sect_fairyland") or SectFairylandApplication(
            str(context.database.path("player_db"))
        )
        app.register_blueprint(sect_fairyland_blueprint(sect_fairyland, permission=has_permission))
    if any(feature.key == "world_events" for feature in registry.features):
        from ...features.world_events.application import DemonClaimApplication

        world_events = (context.services or {}).get("world_events") or DemonClaimApplication(
            str(context.database.path("game_db")), str(context.database.path("player_db"))
        )
        app.register_blueprint(world_events_blueprint(world_events, permission=has_permission))
    if any(feature.key == "work" for feature in registry.features):
        from ...features.work.application import WorkClaimApplication

        work = (context.services or {}).get("work") or WorkClaimApplication(str(context.database.path("game_db")))
        app.register_blueprint(work_blueprint(work, permission=has_permission))
    if any(feature.key == "mixelixir" for feature in registry.features):
        from ...features.mixelixir.application import MixelixirApplication

        mixelixir = (context.services or {}).get("mixelixir") or MixelixirApplication(
            str(context.database.path("game_db")), str(context.database.path("player_db"))
        )
        app.register_blueprint(mixelixir_blueprint(mixelixir, permission=has_permission))
    if any(feature.key == "puppet" for feature in registry.features):
        from ...features.puppet.application import PuppetApplication

        puppet = (context.services or {}).get("puppet") or PuppetApplication(
            str(context.database.path("game_db")), str(context.database.path("player_db"))
        )
        app.register_blueprint(puppet_blueprint(puppet, permission=has_permission))
    if any(feature.key == "boss" for feature in registry.features):
        from ...features.boss.application import BossApplication

        boss = (context.services or {}).get("boss") or BossApplication(
            str(context.database.path("game_db")),
            str(context.database.path("player_db")),
            activity_database=context.paths.data / "activity" / "activity.db",
        )
        app.register_blueprint(boss_blueprint(boss, permission=has_permission))
    if any(feature.key == "dungeon" for feature in registry.features):
        from ...features.dungeon.application import DungeonApplication

        dungeon = (context.services or {}).get("dungeon") or DungeonApplication(
            str(context.database.path("game_db")), str(context.database.path("player_db"))
        )
        app.register_blueprint(dungeon_blueprint(dungeon, permission=has_permission))
    if any(feature.key == "pet" for feature in registry.features):
        from ...features.pet.application import PetApplication

        pet = (context.services or {}).get("pet") or PetApplication(
            str(context.database.path("game_db")), str(context.database.path("player_db"))
        )
        app.register_blueprint(pet_blueprint(pet, permission=has_permission))
    if any(feature.key == "sect" for feature in registry.features):
        from ...features.sect.application import SectApplication

        sect = (context.services or {}).get("sect") or SectApplication(str(context.database.path("game_db")))
        app.register_blueprint(sect_blueprint(sect, permission=has_permission))
    if any(feature.key == "natal_treasure" for feature in registry.features):
        from ...features.natal_treasure.application import NatalTreasureApplication
        natal = (context.services or {}).get("natal_treasure") or NatalTreasureApplication(str(context.database.path("player_db")), str(context.database.path("game_db")))
        app.register_blueprint(natal_treasure_blueprint(natal, permission=has_permission))
    if any(feature.key == "buff" for feature in registry.features):
        from ...features.buff.application import BuffApplication
        buff = (context.services or {}).get("buff") or BuffApplication(str(context.database.path("game_db")), str(context.database.path("player_db")))
        app.register_blueprint(buff_blueprint(buff, permission=has_permission))
    if any(feature.key == "base" for feature in registry.features):
        from ...features.base.application import BaseApplication
        base = (context.services or {}).get("base") or BaseApplication(str(context.database.path("game_db")), str(context.database.path("player_db")))
        app.register_blueprint(base_blueprint(base, permission=has_permission))
    if any(feature.key == "back" for feature in registry.features):
        from ...features.back.application import BackApplication
        back = (context.services or {}).get("back") or BackApplication(str(context.database.path("game_db")), str(context.database.path("player_db")))
        app.register_blueprint(back_blueprint(back, permission=has_permission))
    if any(feature.key == "trade" for feature in registry.features):
        from ...features.trade.application import TradeApplication
        trade = (context.services or {}).get("trade") or TradeApplication(str(context.database.path("game_db")), str(context.database.path("trade_db")))
        app.register_blueprint(trade_blueprint(trade, permission=has_permission))
    if any(feature.key == "map" for feature in registry.features):
        from ...features.map.application import MapApplication
        mapped = (context.services or {}).get("map") or MapApplication(str(context.database.path("game_db")), str(context.database.path("player_db")))
        app.register_blueprint(map_blueprint(mapped, permission=has_permission))
    if any(feature.key == "rift" for feature in registry.features):
        from ...features.rift.application import RiftApplication
        rift = (context.services or {}).get("rift") or RiftApplication(str(context.database.path("game_db")), str(context.database.path("player_db")))
        app.register_blueprint(rift_blueprint(rift, permission=has_permission))
    from .blueprints.activity import create_blueprint as activity_blueprint
    from .blueprints.backups import create_blueprint as backups_blueprint
    from .blueprints.config import create_blueprint as config_blueprint
    from .blueprints.dashboard import create_blueprint as dashboard_blueprint
    from .blueprints.database import create_blueprint as database_blueprint
    from .blueprints.messages import create_blueprint as messages_blueprint
    from .blueprints.logs import create_blueprint as logs_blueprint
    from .blueprints.pages import create_blueprint as pages_blueprint
    from .blueprints.scheduler import create_blueprint as scheduler_blueprint
    from .legacy import create_legacy_blueprint

    app.register_blueprint(dashboard_blueprint(context=context, registry=registry, readiness=readiness))
    app.register_blueprint(config_blueprint(context=context, permission=has_permission))
    app.register_blueprint(database_blueprint(context=context, permission=has_permission))
    app.register_blueprint(scheduler_blueprint(context=context, permission=has_permission))
    app.register_blueprint(backups_blueprint(context=context, permission=has_permission))
    app.register_blueprint(activity_blueprint(context=context, permission=has_permission))
    app.register_blueprint(messages_blueprint(context=context, permission=has_permission))
    app.register_blueprint(logs_blueprint(context=context, permission=has_permission))
    app.register_blueprint(pages_blueprint(context=context, permission=has_permission))
    app.register_blueprint(create_legacy_blueprint(has_permission))
    for blueprint in feature_blueprints:
        app.register_blueprint(blueprint)
    return app


__all__ = ["create_app"]
