from pathlib import Path


def test_admin_asset_application_uses_feature_owned_stone_and_item_repositories():
    source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/features/admin_asset/application.py").read_text(encoding="utf-8")
    assert "AdminStoneSqlRepository" in source
    assert "AdminItemSqlRepository" in source
    assert "self.repository or LegacyAdminStoneRepository" not in source
    assert "self.item_repository or LegacyAdminItemRepository" not in source


def test_default_composition_uses_migrated_admin_stone_repository():
    root = Path(__file__).parents[1]
    plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    migrations = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/migrations.py").read_text(encoding="utf-8")
    assert "LegacyAdminStoneRepository" not in plugin
    assert 'Migration("admin_asset.002", "admin_stone_adjustment_operations", apply_admin_stone_adjustment)' in plugin
    assert "def apply_admin_stone_adjustment(" in migrations


def test_global_stone_command_uses_resumable_feature_batch():
    root = Path(__file__).parents[1]
    admin = (root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py").read_text(encoding="utf-8")
    handler = admin[admin.index("@gm_command.handle") : admin.index("# GM加思恋结晶")]
    plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    manifest = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/manifest.py").read_text(encoding="utf-8")
    repository = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/stone_batch_repository.py").read_text(encoding="utf-8")
    assert "update_ls_all(" not in handler
    assert "find_running_stone_batch(" in handler
    assert "admin_asset_application.adjust_stone_batch(" in handler
    assert "run_chunked_until_done" in handler
    assert 'Migration("admin_asset.003", "admin_stone_batch_adjustment_operations", apply_admin_stone_batch)' in plugin
    assert 'migration_version="admin_asset.012"' in manifest
    assert "CREATE TABLE" not in repository
    assert "shutil.disk_usage" in repository


def test_admin_exp_repository_has_no_request_time_ddl():
    root = Path(__file__).parents[1]
    plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    manifest = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/manifest.py").read_text(encoding="utf-8")
    repository = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/exp_repository.py").read_text(encoding="utf-8")
    migrations = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/migrations.py").read_text(encoding="utf-8")
    admin = (root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py").read_text(encoding="utf-8")
    handler = admin[admin.index("@adjust_exp_command.handle") : admin.index("@zaohua_xiuxian.handle")]
    assert 'Migration("admin_asset.004", "admin_exp_adjustment_operations", apply_admin_exp_adjustment)' in plugin
    assert 'migration_version="admin_asset.012"' in manifest
    assert "CREATE TABLE" not in repository
    assert "def apply_admin_exp_adjustment(" in migrations
    assert 'result.status == "schema_missing"' in handler


def test_admin_item_destroy_repository_has_no_request_time_ddl():
    root = Path(__file__).parents[1]
    plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    manifest = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/manifest.py").read_text(encoding="utf-8")
    repository = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/item_destroy_repository.py").read_text(encoding="utf-8")
    migrations = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/migrations.py").read_text(encoding="utf-8")
    admin = (root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py").read_text(encoding="utf-8")
    handler = admin[admin.index("@hmll.handle") : admin.index("@restate.handle")]
    assert 'Migration("admin_asset.005", "admin_item_destroy_operations", apply_admin_item_destroy)' in plugin
    assert 'migration_version="admin_asset.012"' in manifest
    assert "CREATE TABLE" not in repository
    assert "def apply_admin_item_destroy(" in migrations
    assert 'result.status == "schema_missing"' in handler


def test_admin_item_grant_repository_has_no_request_time_ddl():
    root = Path(__file__).parents[1]
    plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    manifest = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/manifest.py").read_text(encoding="utf-8")
    repository = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/item_repository.py").read_text(encoding="utf-8")
    migrations = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/migrations.py").read_text(encoding="utf-8")
    admin = (root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py").read_text(encoding="utf-8")
    handler = admin[admin.index("@cz.handle") : admin.index("@hmll.handle")]
    assert 'Migration("admin_asset.006", "admin_item_grant_operations", apply_admin_item_grant)' in plugin
    assert 'migration_version="admin_asset.012"' in manifest
    assert "CREATE TABLE" not in repository
    assert "def apply_admin_item_grant(" in migrations
    assert 'result.status == "schema_missing"' in handler


def test_admin_realm_changes_use_game_startup_schema_and_no_legacy_getters():
    root = Path(__file__).parents[1]
    plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    manifest = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/manifest.py").read_text(encoding="utf-8")
    migrations = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/migrations.py").read_text(encoding="utf-8")
    level_repository = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/level_repository.py").read_text(encoding="utf-8")
    root_repository = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/root_repository.py").read_text(encoding="utf-8")
    admin = (root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py").read_text(encoding="utf-8")
    level_handler = admin[admin.index("async def zaohua_xiuxian_"):admin.index("@gmm_command.handle")]
    root_handler = admin[admin.index("async def gmm_command_"):admin.index("@cz.handle")]
    assert 'Migration("admin_asset.007", "admin_level_root_change_operations", apply_admin_realm_changes)' in plugin
    assert 'migration_version="admin_asset.012"' in manifest
    assert "def apply_admin_realm_changes(" in migrations
    assert "CREATE TABLE" not in level_repository
    assert "CREATE TABLE" not in root_repository
    assert 'result.status == "schema_missing"' in level_handler
    assert 'result.status == "schema_missing"' in root_handler
    assert "_admin_level_change_service" not in admin
    assert "_admin_root_change_service" not in admin
    assert "AdminRootChangeSqlRepository.root_values(" in root_handler


def test_admin_impart_stone_uses_impart_balance_with_game_startup_receipt():
    root = Path(__file__).parents[1]
    plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    manifest = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/manifest.py").read_text(encoding="utf-8")
    migrations = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/migrations.py").read_text(encoding="utf-8")
    repository = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/impart_stone_repository.py").read_text(encoding="utf-8")
    admin = (root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py").read_text(encoding="utf-8")
    handler = admin[admin.index("# GM加思恋结晶"):admin.index("@adjust_exp_command.handle")]
    assert 'Migration("admin_asset.008", "admin_impart_stone_operations", apply_admin_impart_stone_operations)' in plugin
    assert 'migration_version="admin_asset.012"' in manifest
    assert "def apply_admin_impart_stone_operations(" in migrations
    assert "impart_data.xiuxian_impart" in repository
    assert "user_xiuxian SET stone" not in repository
    assert "impart_data.statistics" not in repository
    assert "CREATE TABLE" not in repository
    assert "ALTER TABLE" not in repository
    assert "admin_asset_application.snapshot_impart_stone(" in handler
    assert "_admin_impart_stone_adjustment_service().snapshot(user_id)" not in handler
    assert 'snapshot.status == "schema_missing"' in handler


def test_admin_accessory_single_adjustment_is_feature_owned_and_migrated():
    root = Path(__file__).parents[1]
    plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    manifest = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/manifest.py").read_text(encoding="utf-8")
    migrations = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/migrations.py").read_text(encoding="utf-8")
    repository = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/accessory_repository.py").read_text(encoding="utf-8")
    application = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/application.py").read_text(encoding="utf-8")
    admin = (root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py").read_text(encoding="utf-8")
    assert 'Migration("admin_asset.009", "admin_accessory_operations", apply_admin_accessory_operations)' in plugin
    assert 'migration_version="admin_asset.012"' in manifest
    assert "def apply_admin_accessory_operations(" in migrations
    assert "AdminAccessorySqlRepository" in application
    assert "AdminAccessoryAdjustmentService" not in application
    assert "player_data.player_accessory" in repository
    assert "CREATE TABLE" not in repository
    assert "ALTER TABLE" not in repository
    assert "admin_accessory_operations" in repository
    assert 'result.status == "schema_missing"' in admin
    assert "create_accessory=lambda: create_accessory_instance(item_id, quality)" in admin


def test_admin_accessory_batch_is_feature_owned_and_disk_preflighted():
    root = Path(__file__).parents[1]
    plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    manifest = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/manifest.py").read_text(encoding="utf-8")
    migrations = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/migrations.py").read_text(encoding="utf-8")
    repository = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/accessory_batch_repository.py").read_text(encoding="utf-8")
    application = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/application.py").read_text(encoding="utf-8")
    admin = (root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py").read_text(encoding="utf-8")
    assert 'Migration("admin_asset.010", "admin_accessory_batch_operations", apply_admin_accessory_batch)' in plugin
    assert 'migration_version="admin_asset.012"' in manifest
    assert "def apply_admin_accessory_batch(" in migrations
    assert "CREATE TABLE" not in repository
    assert "shutil.disk_usage" in repository
    assert "bytes_per_accessory" in repository
    assert "admin_accessory_batch_targets" in repository
    assert "AdminAccessoryBatchSqlRepository" in application
    assert "admin_asset_application.grant_accessory_batch(" in admin
    assert "admin_asset_application.destroy_accessory_batch(" in admin


def test_admin_impart_stone_batch_is_feature_owned_and_database_frozen():
    root = Path(__file__).parents[1]
    plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    manifest = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/manifest.py").read_text(encoding="utf-8")
    migrations = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/migrations.py").read_text(encoding="utf-8")
    repository = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/impart_stone_batch_repository.py").read_text(encoding="utf-8")
    application = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/application.py").read_text(encoding="utf-8")
    legacy_application = (root / "nonebot_plugin_xiuxian_2/features/admin/application.py").read_text(encoding="utf-8")
    admin = (root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py").read_text(encoding="utf-8")
    handler = admin[admin.index("async def ccll_command_"):admin.index("@adjust_exp_command.handle")]
    assert 'Migration("admin_asset.011", "admin_impart_stone_batch_operations", apply_admin_impart_stone_batch)' in plugin
    assert 'migration_version="admin_asset.012"' in manifest
    assert "def apply_admin_impart_stone_batch(" in migrations
    assert "AdminImpartStoneBatchSqlRepository" in application
    assert "def adjust_impart_stone_batch(" not in legacy_application
    assert "admin_asset_application.find_running_impart_stone_batch(" in handler
    assert "admin_asset_application.adjust_impart_stone_batch(" in handler
    assert "get_all_user_id()" not in handler
    assert "INSERT INTO admin_impart_stone_batch_targets" in repository
    assert "shutil.disk_usage" in repository
    assert "target_insert_chunk_size" in repository
    assert "max_chunk_size" in repository
    assert "payload_prefix_chars" in repository
    assert "max_legacy_payload_chars" in repository
    assert "_iter_legacy_users" in repository
    assert '"admin_asset.011"' not in plugin[
        plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):
        plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")
    ]


def test_admin_item_batch_is_feature_owned_and_does_not_materialize_roster():
    root = Path(__file__).parents[1]
    plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    manifest = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/manifest.py").read_text(encoding="utf-8")
    migrations = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/migrations.py").read_text(encoding="utf-8")
    repository = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/item_batch_repository.py").read_text(encoding="utf-8")
    application = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/application.py").read_text(encoding="utf-8")
    legacy_application = (root / "nonebot_plugin_xiuxian_2/features/admin/application.py").read_text(encoding="utf-8")
    admin = (root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py").read_text(encoding="utf-8")
    grant_handler = admin[admin.index("@cz.handle"):admin.index("@hmll.handle")]
    destroy_handler = admin[admin.index("@hmll.handle"):admin.index("@restate.handle")]
    grant_batch = grant_handler[grant_handler.index("operation_id = admin_asset_application.find_running_item_batch("):]
    destroy_batch = destroy_handler[destroy_handler.index("operation_id = admin_asset_application.find_running_item_batch("):]
    progress_import = repository[
        repository.index("    def _import_legacy_progress("):
        repository.index("    def _begin(")
    ]

    assert 'Migration("admin_asset.012", "admin_item_batch_operations", apply_admin_item_batch)' in plugin
    assert 'migration_version="admin_asset.012"' in manifest
    assert "def apply_admin_item_batch(" in migrations
    assert "AdminItemBatchSqlRepository" in application
    assert "def grant_item_batch(" not in legacy_application
    assert "admin_asset_application.adjust_item_batch(" in grant_batch
    assert "admin_asset_application.adjust_item_batch(" in destroy_batch
    assert "get_all_user_id()" not in grant_batch
    assert "get_all_user_id()" not in destroy_batch
    assert "CREATE TABLE" not in repository
    assert "shutil.disk_usage" in repository
    assert "max_chunk_size" in repository
    assert "_import_legacy_progress" in repository
    assert "query_all" not in progress_import
    assert '"admin_asset.012"' not in plugin[
        plugin.index("_PLAYER_DATABASE_MIGRATION_VERSIONS"):
        plugin.index("_TRADE_DATABASE_MIGRATION_VERSIONS")
    ]

    from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database

    migrations = build_migrations()
    routed = {
        key: {migration.version for migration in migrations_for_database(migrations, key)}
        for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db")
    }
    assert "admin_asset.012" in routed["game_db"]
    assert all("admin_asset.012" not in versions for key, versions in routed.items() if key != "game_db")
