# 旧调度任务兼容层

## 用户流程

旧版本 APScheduler 任务继续运行；旧模块导入时只收集声明，不修改 live scheduler。组合根在 `jobs` 生命周期阶段激活调度桥并登记稳定任务 ID，管理端只读展示登记结果。

## 命令与别名

无新增命令。旧命令仍由旧包兼容入口处理。

## Web API（方法、路径、请求/响应、权限、幂等键）

由 `/api/v1/scheduler` 提供只读任务清单，权限为 `admin`；旧管理页面保留兼容 URL。

## 数据模型与迁移

无新增表。旧调度覆盖配置仍由旧仓储读取，迁移期间不改变其字段。

## 事务与失败回滚

任务执行由旧服务负责；新层只提供稳定 ID、超时、重试和幂等策略声明。失败必须进入旧任务日志并可人工重试。

## 定时任务

任务 ID 清单见 `compatibility/legacy_manifest.py`，禁止通过模块导入新增未声明任务。

## 配置项

无新增配置。

## 适配器差异

无消息适配器差异；任务仍通过原有 NoneBot/APScheduler 运行时执行，但注册由 `compatibility/scheduler.py` 延迟到组合根的 `jobs` 阶段完成。

## 测试与手工验收

启动两次任务清单不重复，`/api/v1/scheduler` 返回稳定 ID，旧调度测试保持通过。

## 灰度开关、回滚和已知限制

移除该 manifest 即可关闭新登记和 Web 展示；旧任务会在兼容周期内继续运行。当前任务 handler 仍由旧包提供，调度声明已由桥接层统一延迟注册；下一发布周期再迁移至 feature jobs。

## Manifest 清单
- `job: auto_guishi_transactions`
- `job: auto_harvest`
- `job: auto_start_auction`
- `job: auto_handle_inactive_sect_owners_job`
- `job: backup_database_files`
- `job: cleanup_media_parser_cache_job`
- `job: check_auction_end`
- `job: clear_expired_baitan_orders`
- `job: daily_dungeon_reset`
- `job: daily_add_impart_lv`
- `job: daily_clean_expired_items`
- `job: daily_reset_arena`
- `job: daily_reset_beg`
- `job: daily_reset_boss_limits`
- `job: daily_reset_day_num`
- `job: daily_reset_illusion`
- `job: daily_reset_impart_num`
- `job: daily_reset_impart_pk`
- `job: daily_reset_lottery`
- `job: daily_reset_mixelixir_num`
- `job: daily_reset_sect_task`
- `job: daily_reset_sign`
- `job: daily_reset_stone_limits`
- `job: daily_reset_two_exp`
- `job: daily_reset_work_refresh_num`
- `job: daily_reset_xiangyuan`
- `job: demon_invasion_refresh_schedule`
- `job: demon_invasion_schedule`
- `job: generate_all_bosses`
- `job: newapi_auto_checkin_daily`
- `job: recover_user_stamina`
- `job: reset_message_rate_limits`
- `job: reset_data_by_time_job`
- `job: scheduled_rift_generation_job`
- `job: sect_materials_grant`
- `job: spirit_vein_schedule`
- `job: weekly_reduce_arena_rank`
- `job: weekly_reduce_impart_lv`
- `job: weekly_reset_tower_floors`
