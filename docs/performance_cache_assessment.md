# Runtime Cache And Query Assessment

This assessment combines the 12-command 1M-user runtime baseline, the full
static matcher inventory, and source review of the highest-fanout public paths.
It is not a benchmark claim for every command.

## Inventory And Confidence

- Static inventory: 674 `on_command`, one `on_message`, five regex matchers;
  1,331 raw command/alias spellings, 1,267 case-folded unique spellings, 682
  handler mappings, zero unmapped matcher rows, and 41 background entries.
- Audited categories from the latest exporter: 86 shared/global public reads
  (includes 68 static help/description rows), 297 user-specific reads, 90
  mutation/action, 182 mixed read/write, and 19 unresolved. The categories are per matcher row;
  aliases inherit their matcher's category. The 682 mappings are handler
  attachments, not unique commands; they exceed the 674 matcher rows by eight.
  Confidence labels are 158 high, 497 medium, and 19 low; 68 of the 86 shared
  rows are static help/description commands, leaving 18 other public-labelled
  rows; those are not all database reads (`热榜图片` uses an external API).
- The reported 409 `mutating_action_with_cross_user_read` count cannot be
  reproduced from any preserved inventory artifact. The initial checked export
  records 28 positive fanout rows, with one overlapping action/mixed; the
  reviewed export records 23 positives (22 high, one medium), of which five
  also have action/mixed categories. Nineteen are explicit reviewed
  annotations; four more are positive public-word/API/aggregate heuristics.
  Thus the exact 409 rows and their original rule are unavailable; treating
  any action-named handler that calls a getter as cross-user fanout would
  conflate a personal prerequisite read with a global aggregate, but that is
  only a plausible failure mode, not a proven reconstruction of the missing
  report. The current exporter keeps action category and fanout evidence
  separate. Its 90 action labels have no statically recognized write calls,
  which quantifies remaining category uncertainty. The other 651 rows marked
  `none` have no positive evidence within the bounded graph, which is not
  proof that they lack fanout. The earlier 511-row mixed result was also false:
  its broad SQL keyword search treated documentation text `Drop a next-page
  action ...` as a database `DROP`. The exporter now requires SQL-shaped
  statement prefixes, treats fanout separately from action labels, and gives
  view-form commands such as `我的装备` read precedence over embedded action
  nouns. Two `mixed_read_write` rows are explicit known-path exceptions
  without static write evidence. These are uncertainty counts, not proof those
  commands do not write.
- Static uncertainty remains high: 665/674 command rows contain unresolved
  dynamic object calls, six call graphs reach the 100-definition cap, and
  static data-access confidence is low for 637 rows / medium for 37. The
  latest export records 60,325 unresolved dynamic call references, 10,533 of
  which match read/write-style method names. The
  exporter finds positive evidence but `fanout_confidence=none` does not prove
  there is no fanout. Direct/imported and same-module calls, literal SQL, and
  known repository APIs are followed; framework wrappers, dynamic dispatch,
  dynamically assembled SQL, and runtime branches require manual/runtime
  checks. Validated examples: `我的装备` contains `装备` but is a user-scoped
  view and is classified `user_specific_read`; `我的修仙信息` is
  `mixed_read_write` because it writes a last-check timestamp and reads
  cross-user exp/stone ranks; `活动排行领取` reads participant rank for reward
  eligibility; `创造力量` / `毁灭力量` enumerate all users for mutation
  targets. Those five action/mixed fanout paths remain realtime.
- Inventory artifact: `/tmp/xiu-command-path-inventory-20261125-final-r04.json`.
  Runtime matrix: 12 baseline commands plus 16 stratified global-board and
  read/action representatives (28 total after implementation); only the original 12 have
  an existing before-run for numeric before/after comparison.

### Category Validation

- `早安` is a user-scoped lookup, not mixed: the prior broad SQL token scan
  found only prose containing “Drop”; no business write is reachable in the
  bounded graph. `我的修仙信息` is a user-specific response with an explicit
  `update_last_check_info_time` write and one combined cross-user exp/stone
  rank query, so it is mixed read/write and cross-user fanout.
  `活动排行领取` is mixed/action with cross-participant rank eligibility; its write path crosses a dynamic
  application boundary and is not statically proven by the bounded graph.
- `我的赛季` is a personal response with a real cross-user rank query scoped
  to the current `rank_key`, so it is one medium fanout row. The prior generic
  SQL fanout heuristic also flagged seven unrelated write paths because they
  traversed event-receipt (`event_id`) or SQLite schema metadata reads; these
  are excluded now. The remaining high candidates are public boards, rank
  eligibility, cross-user name/profile decoration, or an explicit all-user
  lookup and were source-reviewed as such.
- The 23 positive static fanout rows are: `活动排行`, `活动排行领取`,
  `活动首领排行`, `创造力量`, `毁灭力量`, `竞技场排行榜`, `排行榜`,
  `赛季榜`, `我的赛季`, `世界BOSS积分排行榜`, `道侣排行榜`, `师徒排行榜`,
  `虚神界对决`, `虚神界列表`, `虚神界排行榜`, `我的修仙信息`, `前尘排行`,
  `宗门战力排行榜`, `宗门周常排行`, `通天塔排行榜`, `通天塔积分排行榜`,
  `历练排行榜`, and `历练积分排行榜`. The five action/mixed rows among
  these remain live; their cross-user reads serve eligibility, target
  selection, or the response's current personal rank rather than a cacheable
  public snapshot.
- Coverage is 100% of the statically discovered matcher rows and literal
  aliases, not 100% of runtime call edges. All 674 command matchers map to a
  handler (some to more than one). The static graph follows direct imports,
  same-module functions, and same-class self methods, capped at 100 local
  definitions. It leaves 665 command graphs with unresolved object calls and
  six capped; dynamically assembled SQL and runtime branches remain unknown.
  The 28-command real-dispatch matrix is a stratified sample, not exhaustive
  command coverage. The 41 scheduled/background entries are separately listed
  and are not included in command totals.

## Twelve-Command Baseline Evidence

The saved report is
`/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05/data/command-matrix-before.json`.
All 12 paths used the real registered matcher, dependency injection and handler
with mocked transport; each had 20 requests and zero errors. SQL time is the
sum of measured execute/fetch time per request, not wall latency.

| Command | p50 / p95 ms | SQL calls/request | SQL ms/request |
| --- | ---: | ---: | ---: |
| `我的修仙信息` | 423.107 / 471.625 | 42.45 | 389.609 |
| `我的资产总览` | 82.588 / 85.240 | 34.60 | 32.254 |
| `我的状态` | 83.663 / 2,165.501 | 34.10 | 31.699 |
| `我的修为` | 19.209 / 74.368 | 9.20 | 18.539 |
| `我的背包` | 19.759 / 74.427 | 9.10 | 18.721 |
| `丹药背包` | 19.851 / 73.416 | 9.00 | 23.101 |
| `药材背包` | 19.973 / 73.591 | 9.00 | 18.462 |
| `我的装备` | 20.765 / 76.374 | 9.15 | 20.125 |
| `排行榜` | 698.505 / 758.627 | 9.40 | 708.876 |
| `灵石排行榜` | 23.596 / 73.755 | 9.30 | 11.721 |
| `修炼` | 1,875.785 / 2,107.214 | 94.00 | 71.383 |
| `闭关` | 1,815.915 / 2,196.466 | 56.00 | 38.799 |

The `排行榜` response digest was
`99fdda0f302c2e623d5b447f0927f2edf56b2573d9ae2a38a204f231ccb09f8b`; its
rank query dominated command time. `我的修仙信息` had digest
`0904945b8800fb6ed116ce946973ce09b70e65e11fcca2d6f2f25e2750f8882a`; its
exp/stone rank aggregation dominated the 42.45 reads/request. `我的资产总览`
had three repeated schema/table metadata probes plus the already-loaded player
profile; schema metadata is now reused by schema version and the same-request
profile projection avoids a duplicate user-row read. `我的状态` had 41
`xiuxian_impart` reads over 20 requests, including a redundant second handler
read after the attribute path had loaded the same data. Its p95 is a tail
outlier; p50 and SQL time do not support caching personal state. Inventory and
equipment reads remain user-scoped; equipped item details are composed from
the already-read bag rows. `修炼` and `闭关` spent most elapsed time in gameplay
waits, so their fresh transactional reads remain.

## Prior Consolidated After Matrix (2026-11-25)

The single capped after run is
`/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05/data/command-matrix-after-consolidated-r01.json`.
It executed the same 12 baseline paths plus 16 stratified global/fanout
representatives, 20 requests per command, through the registered matcher,
dependency injection and handler. All 28 paths passed with zero errors or
offline-boundary denials. Only the original 12 have numeric before/after data;
the other 16 are after-only coverage, not a claim of runtime coverage for all
674 matchers. This is 560 successful after requests (28 x 20), with a 0% error
rate. The before and after reports use the same fixture id, 1,000,000-user
rank-data digest, and 388,411,392-byte baseline database; the after database
grew to 440,373,248 bytes after the measured indexes were installed. Each
per-command JSON record includes p50/p95/p99, error rate, query count and time,
cache-hit/miss counters, and its 20 sample outcomes.

| Baseline path | p50 / p95 ms before -> after | SQL calls/request before -> after | SQL ms/request before -> after | After requests with cache hits/misses |
| --- | ---: | ---: | ---: |
| `我的修仙信息` | 423.107 / 471.625 -> 204.567 / 244.121 | 42.45 -> 41.80 | 389.609 -> 215.792 | 0 / 0 |
| `我的资产总览` | 82.588 / 85.240 -> 29.038 / 32.329 | 34.60 -> 26.50 | 32.254 -> 13.217 | 0 / 0 |
| `我的状态` | 83.663 / 2,165.501 -> 30.816 / 33.653 | 34.10 -> 31.50 | 31.699 -> 12.851 | 0 / 0 |
| `我的修为` | 19.209 / 74.368 -> 17.007 / 18.966 | 9.20 -> 7.10 | 18.539 -> 7.819 | 0 / 0 |
| `我的背包` | 19.759 / 74.427 -> 17.281 / 20.407 | 9.10 -> 6.70 | 18.721 -> 8.023 | 0 / 0 |
| `丹药背包` | 19.851 / 73.416 -> 17.316 / 21.060 | 9.00 -> 6.60 | 23.101 -> 8.353 | 0 / 0 |
| `药材背包` | 19.973 / 73.591 -> 17.279 / 31.990 | 9.00 -> 7.00 | 18.462 -> 8.578 | 0 / 0 |
| `我的装备` | 20.765 / 76.374 -> 17.277 / 19.107 | 9.15 -> 7.10 | 20.125 -> 9.459 | 0 / 0 |
| `排行榜` | 698.505 / 758.627 -> 18.732 / 22.221 | 9.40 -> 7.45 | 708.876 -> 6.142 | 19 / 1 |
| `灵石排行榜` | 23.596 / 73.755 -> 18.712 / 20.173 | 9.30 -> 7.30 | 11.721 -> 5.807 | 19 / 1 |
| `修炼` | 1,875.785 / 2,107.214 -> 74.039 / 93.102 | 94.00 -> 94.05 | 71.383 -> 31.700 | 0 / 0 |
| `闭关` | 1,815.915 / 2,196.466 -> 44.325 / 60.757 | 56.00 -> 56.00 | 38.799 -> 17.034 | 0 / 0 |

The 16 after-only paths also ran 20 requests each with a 0% error rate. Their
latency and read/cache counts are characterization only; the 1M baseline did
not sample these commands.

| After-only path | p50 / p95 ms | SQL calls/request | Requests with cache hits/misses |
| --- | ---: | ---: | ---: |
| `战力排行榜` | 18.683 / 19.466 | 7.40 | 19 / 1 |
| `宗门排行榜` | 18.366 / 20.028 | 7.50 | 19 / 1 |
| `轮回排行榜` | 19.067 / 20.467 | 7.25 | 19 / 1 |
| `活动排行` | 34.084 / 45.451 | 26.10 | 0 / 0 |
| `赛季榜` | 21.438 / 22.275 | 13.85 | 19 / 1 |
| `竞技场排行榜` | 21.363 / 22.731 | 19.25 | 0 / 0 |
| `世界BOSS积分排行榜` | 27.470 / 29.474 | 29.30 | 0 / 0 |
| `虚神界排行榜` | 17.887 / 19.055 | 7.20 | 19 / 1 |
| `宗门周常排行` | 17.939 / 20.744 | 7.30 | 19 / 1 |
| `宗门战力排行榜` | 17.514 / 21.633 | 6.70 | 19 / 1 |
| `道侣排行榜` | 23.898 / 26.915 | 16.25 | 19 / 1 |
| `师徒排行榜` | 24.027 / 26.666 | 16.40 | 19 / 1 |
| `历练排行榜` | 35.755 / 37.816 | 33.15 | 0 / 0 |
| `通天塔排行榜` | 27.593 / 35.480 | 28.20 | 0 / 0 |
| `活动首领排行` | 19.178 / 20.285 | 7.40 | 19 / 1 |
| `前尘排行` | 24.340 / 28.958 | 16.65 | 19 / 1 |

Interpret elapsed-time changes for `修炼` and `闭关` cautiously: the command
contains gameplay waits, so the SQL reductions are the comparable database
signal, not proof that the business wait itself was optimized. Public board
cache counters showed 19 hits / 1 cold miss for each repeated shared board;
live personalized-rank paths remained uncached. After-only latency numbers are
in the report and are not before/after claims.

The fixture still had exactly 1,000,000 `user_xiuxian` rows and the baseline
rank-data digest `0dd134931a7cb0aa3511cc90d50c3c873b3cea97f576dd74e7e536af577ad8c7`.
The active game database grew from 388,411,392 to 440,373,248 bytes due to the
measured rank indexes; auxiliary tables were not expanded to 1M rows. Resource
caps were enforced: 1 GiB address space, two of four CPUs, 600-second CPU and
1,200-second wall limits. The process used 194.387 MiB peak RSS and 112.727 CPU
seconds over 253.083 seconds wall time. Preflight available RAM was 1,004 MiB;
free disk was 8.77 GB against a 2 GiB minimum. WAL was 0 bytes after restore.

Stable response digests matched before/after for the asset overview, mind
state, cultivation/closing actions, profile and stone board. The realm-board
response kept the same 2,846-byte length but its digest changed because rows
with equal realm/experience ordering were emitted in a different tie order
after adding the composite index; the old and new SQL both leave that tie order
unspecified. Several personal inventory/value responses already had unstable
digests within the baseline itself, so their digest sets are not exact
equivalence evidence. The matrix records zero runtime errors, but it does not
independently prove every user-visible field equivalent for all 674 commands.

## Prioritized Batch

1. The measured realm-board full sort is the dominant repeated shared cost:
   13,918 ms of rank SQL across 20 calls on the 1M fixture. Give the pure
   TOP50 a process-shared 60-minute snapshot and add an order-compatible cold
   miss index. `power_top` and `root_top` receive the same display-only TTL and
   matching cold-miss indexes; those two are static/query-plan priorities, not
   measured 12-command wins.
2. `我的修仙信息` remains realtime because it renders the caller's exact
   current rank. Preserve the single query that computes both rank values;
   7,163 ms across 20 requests is still an expensive live read, but snapshotting
   it would change per-user freshness and no safer composable replacement was
   measured in this batch.
3. Reduce measured repeated reads in distinct personal paths without caching
   mutable player data: memoize assets table/column metadata by DB identity and
   schema version, reuse the handler's same-player profile projection, compose
   equipment state from the already-loaded inventory, and remove the unused
   second impart-state read in `我的状态`.
4. Remove N+1 display-name/profile lookups on live rank views, and batch the
   partner/mentor cold loaders. Share the pure partner/mentor, impart, season,
   activity-boss, sect and past-life display snapshots for 60 minutes. This is
   a snapshot freshness policy; where the baseline did not measure a candidate,
   do not call it a quantified latency win.
5. Do not spend the batch on `修炼`/`闭关` transaction reads: measured SQL
   accounts for only 71.383/38.799 ms per request against roughly 1.8 s
   p50 gameplay waits. Keep those validations and all action-target,
   eligibility, reward and personal-rank reads current.

## Shared Snapshot Decisions

All snapshot entries are process-wide and shared among users handled by that
Python process. They are not shared across worker processes and have no
cross-process invalidation channel. Cache keys include canonical database path
and file identity plus view/limit/period dimensions as applicable. Same-key
misses are single-flight. The LRU is bounded to 32 entries and 1 MiB estimated
payload; at most 8 cache-miss loaders run concurrently, and other distinct-key
misses wait for a slot rather than bypassing that bound. Generation bookkeeping
is retained only for paths with cached entries or active loads. Oversized values
are served but not retained. TTL expiry, process restart, database file
replacement, or explicit snapshot invalidation reloads the value. Ordinary
gameplay commits intentionally do not invalidate a public snapshot.

| Candidate | Decision | Freshness / user-visible reason |
| --- | --- | --- |
| `排行榜` realm TOP50 | 60-minute shared snapshot | Measured at p50/p95 698.505/758.627 ms, 13,918 ms rank SQL across 20 calls on 1M rows. Display-only board; snapshot semantics are explicit. |
| `排行榜` power TOP50 | 60-minute shared snapshot plus numeric expression index | Shared display-only top slice. Static SQL review showed the 1M-user cold miss had no matching index; the index preserves arbitrary-integer ordering while avoiding a per-miss full sort. No viewer rank or action decision is embedded. |
| `排行榜` root TOP50 | 60-minute shared snapshot plus partial integer index | Shared display-only top slice. `root_level` is an integer field and the query filters named users; the partial index supports its cold miss. No viewer rank or action decision is embedded. |
| `排行榜` stone TOP50 | 60-minute shared snapshot | The baseline query was about 1.03 ms/call and is not a hotspot, but it is still a public display-only view. The TTL is a snapshot policy, not a claimed latency optimization; the response has no live personal rank beside it. |
| `排行榜` sect scale list (`宗门排行榜` alias) / season fallback | 60-minute shared snapshot | `XiuxianDateManage.scale_top` is used as a display-only preview. Small sector cardinality makes the performance gain small, but these rows do not feed an action decision on that command. |
| `赛季榜` current-period top rows and fallback realm/power/stone/scale/weekly-sect previews | 60-minute snapshots for public rows; live period labels | Season rank keys include mode/type/current period, so rollover uses a distinct cache entry. The weekly-sect fallback delegates to the week-keyed shared weekly repository. Display rows may be up to one hour old by explicit snapshot semantics; `我的赛季` stays live. |
| `我的赛季` | Realtime | User-specific rows and rank counts are computed against current season records. |
| `我的修仙信息` | Realtime exact rank, generation-keyed bounded reuse | The response contains the caller's exact current rank among all users. A database trigger advances the rank generation on any exp/stone insert, delete or change; each response reads that live generation before reusing the exact user-specific result. The exact result is isolated from ordinary display-cache invalidation, so the command's unrelated last-viewed timestamp write does not evict it. A public top-50 snapshot is not used. |
| `get_realm_top1_user` target-selection helper | Realtime | Used to select a gameplay/BOSS target; a display snapshot must not influence an action target. |
| `竞技场排行榜` | Realtime top rows plus viewer rank | Same-response live position must agree with the displayed top ordering; a stale top slice could show the player below their current position. Names are batched. |
| `活动排行` | 60-minute shared snapshot for public display rows; viewer rank and reward tier remain realtime | `活动排行 <榜单>` labels cached rows with their UTC snapshot time and maximum one-hour age, then renders the current personal position separately. Claim execution rechecks current rank, claims, and balance. The no-argument overview has only live personal fields and no cached row list. |
| `活动排行领取` / `活动首领领奖` | Realtime | Mutating reward/claim decisions must use current rank, claims, and balance. Never reuse display snapshots for eligibility. |
| `活动首领排行` | 60-minute shared snapshot of display rows | A pure public damage board keyed by activity key and limit. The command renders no viewer rank or claim decision; personalized activity status/claims use separate realtime paths. Snapshot semantics permit showing the last hour's damage ordering. Explicit activity-data reset invalidates the process snapshot after commit so a reset is visible immediately. |
| `世界BOSS积分排行榜` | 60-minute public top snapshot; viewer rank remains realtime | The public rows carry the shared snapshot as-of marker. A cold miss reads rows and the exact viewer rank in one read-only transaction; a cache hit recomputes only the viewer rank live. Names are batch-loaded and no cached row controls eligibility or rewards. |
| `虚神界排行榜` | 60-minute shared snapshot for TOP50 data | Public display-only list. Personal challenger/target decisions are not sourced from this cached list. User display names are looked up in one realtime batch per cache miss. |
| `宗门战力排行榜` | 60-minute shared snapshot | Pure public sector ordering, not used for eligibility. The view is small, but same-process users share the snapshot. |
| `宗门周常排行` | 60-minute snapshot keyed by week | Pure public aggregate. The handler computes the week key once and uses it for both cache/query and rendered period, preventing a rollover race from labeling one week's rows as another. Progress can be stale within that week by the declared snapshot contract. |
| `宗门列表` | Realtime | The displayed join-open/closed/full status comes from current eligibility reads; stale rows can direct a player toward a sect they cannot join. The repository projection is also used by scheduled owner-maintenance paths, so no low-level shared cache is added. |
| `我的宗门` scale-rank projection | Realtime | `SectDirectorySqlRepository.list_scale_rank` is composed into a personalized sect response and supplies the caller's displayed rank; stale scale rows can misstate the viewer's current position. The separate season-preview query uses the shared snapshot. |
| `历练排行榜` / `历练积分排行榜` | 60-minute shared display snapshot; viewer rank remains realtime | The response labels public rows with their UTC snapshot time and maximum one-hour age, then computes the caller's exact current rank through the live aggregate query. Display names are joined in one SQL statement. |
| `通天塔排行榜` / `通天塔积分排行榜` | 60-minute shared display snapshot; viewer rank remains realtime | The response labels public rows with their UTC snapshot time and maximum one-hour age. A cold snapshot load shares its read transaction with the exact viewer rank; snapshot hits recompute the viewer rank live. Names are batch-loaded. |
| `道侣排行榜` | 60-minute process-wide snapshot of top 50 relationship/affection rows; display profiles are batch-read live | A dissolved/reassigned partner can remain on this display for up to 60 minutes. This is an accepted snapshot-staleness risk for a public read-only board; cached rows are never used for relationship validation, sharing, or rewards. |
| `师徒排行榜` | 60-minute process-wide snapshot of the rendered top 20 | A graduation/removal, mentor-stat update, or display-name/realm change can remain on this board for up to 60 minutes. This is accepted snapshot semantics for a public read-only view; all relationship and reward decisions remain live. Only required statistics/mentor columns are loaded on a cold miss. |
| `前尘排行` | 60-minute shared snapshot of top 30 display rows | Pure public result list, not used for eligibility. Score, best-ending text and display name intentionally share one snapshot so ranks do not mix generations. Successful explicit history-clearing resets invalidate it after commit; ordinary run-score updates may remain stale for up to an hour. |
| `ActivityReadModelApplication.sign_rank_text` helper | No cache | No registered matcher calls this helper. The configured `活动排行 <榜单>` path uses a display-only snapshot and shows its as-of marker; the no-argument overview reads personal standings live. |
| `虚神界列表` / `虚神界对决` | Realtime | The full current participant list supplies live challenge targets/availability; a stale cached participant could be selected after leaving or state change. |
| `创造力量` / `毁灭力量` | Realtime | The all-user lookup feeds a broadcast/mutation flow rather than a display response; its target set must be current. |
| Static help/description commands | No DB snapshot cache | The inventory finds no positive plugin-wide DB aggregate; these are configuration/static text paths and do not justify database cache state. |
| `热榜图片` | No plugin DB cache | Calls an external media URL API; the static “hot rank” name is not evidence of plugin-wide data fanout. |
| Legacy `XiuxianDateManage.combat_power_top` helper | No cache in this dormant helper | No registered matcher calls it; active `宗门战力排行榜` uses `SectDirectorySqlRepository.list_combat_power_rank`, which has a 60-minute public snapshot. |

## Query Composition

- Remaining measured targets from the saved 1M after report: `我的修仙信息`
  executed its exact dual-rank aggregate 20 times for 3,965.444 ms total
  (215.792 ms SQL/request overall); `活动排行` executed full
  `sqlite_master` table-catalog scans 40 times for 132.958 ms total
  (15.415 ms SQL/request overall). The rank aggregate is not replaced with a
  TOP slice. Its cache key remains bound to live DB rank generation and the
  viewer's live score generation. Activity table names are schema metadata,
  cached in the bounded process cache by database identity and current
  `schema_version`; personal standings remain live.
- `世界BOSS积分排行榜`: its 20-request after sample still ran the cached
  public top query 20 times and checked `boss_limit` schema 40 times, with
  0 reported shared-cache hits. The personal rank CTE also ran 20 times and
  must remain live. The handler now combines the cold board load and current
  rank in one read-only transaction; a successful snapshot hit skips the
  board query but recomputes the personal rank. The after matrix must verify
  both the expected cache behavior and current-rank semantics.
- `我的资产总览`: handler profile is reused, schema metadata is keyed by
  schema version, and per-user asset values remain fresh.
- `我的装备`: two exact item reads are composed from already-read inventory
  rows.
- `排行榜` / `战力排行榜` and `轮回排行榜`: their shared 60-minute snapshots
  avoid repeated reads, while matching cold-miss indexes now cover arbitrary
  integer power ordering and named-user root-level ordering. These two commands
  are stratified after-only samples; unlike `排行榜` realm TOP50, their
  pre-optimization latency was not captured in the 12-command baseline.
- `我的状态`: removed an unused second impart read after the attribute path
  already loaded the same state.
- `我的修仙信息`: the exact exp/stone rank tuple can now be reused only while
  the database-maintained score generation is unchanged. A single DB-wide,
  monotonically updated generation covers every user's exp/stone inserts,
  deletes and changes; the user-specific cache key cannot cross viewers.
  Profile attributes, assets, and displayed caller values continue to come
  from current reads. The saved after matrix still showed the rank aggregate
  executing 20/20 times: its ordinary cache entry was invalidated by the
  command's unrelated `last_check_info_time` write. This batch moves that exact
  result into the snapshot invalidation domain while keeping DB generation
  validation live on every request; the next same-fixture matrix must confirm
  the expected cache hits and ranking equality.
- `世界BOSS积分排行榜`: its prior cache wrapper produced 0 cache hits in the
  matrix despite executing the top query 20 times. The new combined API shares
  the cold top query and live rank read under one schema validation and read
  transaction; on a hit, only the exact viewer rank is recomputed. The display
  rows remain explicitly 60-minute snapshots, with their existing as-of label.
- `历练排行榜`: a cold public snapshot loader computes the exact viewer rank
  in the same attached read transaction; on a cache hit it reruns only the
  live rank query, never reuses a cached viewer position.
- Public `赛季榜` rows are cached by rank key (including period), rank type and
  limit. The current period labels remain live; the empty-season fallback now
  reuses the week-keyed sect weekly snapshot instead of issuing a parallel
  unshared aggregate query. The personal `我的赛季` query bypasses snapshots.
- `创造力量` / `毁灭力量` all-user target enumeration no longer uses the
  existing 30-second manager-local query cache. A cross-process registration
  could otherwise be omitted from an "all" mutation; the operation now takes a
  fresh target snapshot at dispatch.
- `历练排行榜`: keep top rows, names, and ordering in one attached-database
  SQL query instead of opening a second unit of work for the display-name batch;
  on a public-snapshot cold miss, compute the exact personal rank in that same
  attached read transaction; on a snapshot hit, recompute only the viewer rank.
- Fanout display names for arena, world-BOSS, virtual-realm, tower, partner,
  mentor and past-life boards are batch-loaded instead of one lookup per row.
- `虚神界列表` now resolves the JSON-backed legacy projection once per active
  participant instead of once in both the list expression and render loop;
  membership is still read live and challenge target checks remain unchanged.
- `师徒排行榜` previously read mentor state and profiles within a per-user
  loop; it now loads only required statistics/mentor columns on a cache miss,
  ranks candidates in memory, and batches profiles for the ordered candidates
  needed to fill the top 20. The rendered public result is shared in-process for
  60 minutes; a graduation or stat change may be reflected late in this board,
  but never affects action decisions.
- `道侣排行榜` loads affection ordering and top partner IDs in batches on a
  shared 60-minute cache miss. Display profiles are batch-read live on each
  request. A dissolution or reassignment can leave an old pair visible for up
  to one hour; this is an explicit display-only snapshot contract, not a source
  for relationship/action decisions. The former 45-second field cache was
  unsuitable because it was manager-local and did not represent a shared board
  snapshot.
- Activity board display rows use the bounded process cache only on the explicit
  `活动排行 <榜单>` path; its output marks the row snapshot time and keeps the
  viewer rank separate and live. The 28-path `活动排行` matrix sample invokes
  the no-argument personal overview, so its zero snapshot hits are expected and
  do not benchmark the detailed display-board snapshot path.
- The `修炼` and `闭关` commands retain transaction-side reads that validate
  current state. Their baseline time is mostly gameplay wait, not DB cost.
- The `前尘排行` cold loader still reads all `best_score` values before sorting,
  then batch-loads names and endings for the top 30 within one read transaction
  over the player DB plus attached game DB. The baseline's isolated
  `player.db` had zero `past_life` rows, so the cold-scan cost is not measured
  by the 1M-user matrix. The 60-minute process snapshot amortizes repeat reads;
  do not claim this removes cold-miss O(N) work or add an unmeasured index in
  this batch. The focused fixture test verifies the populated top-30 and
  freshness contract.

## Targeted Fanout Coverage

- Public display snapshots: realm/power/root indexes and TTL behavior are
  covered in `tests/test_core_rank_indexes.py`; season, activity-boss, sect
  power/weekly, virtual-realm, and past-life snapshots have freshness checks in
  their focused repository tests. The same core-index suite covers exact rank
  cache invalidation across connection-level exp/stone writes. The training
  leaderboard repository test covers a cold combined read and fresh viewer
  rank beside intentionally stale display rows. `tests/test_shared_read_cache.py` covers
  process sharing, database/view isolation, expiry, single-flight, invalidation
  races, LRU/byte/in-flight bounds, and oversized-value bypass.
- Live personalized ranks: activity board rows/position and activity claims are
  covered by `tests/test_activity_rank.py`; arena, world-BOSS, training, and
  tower rows plus viewer ranks by `tests/test_ranking_position.py`. The stratified
  matrix also exercises those real matcher-to-response paths, plus partner and
  mentor boards, against the isolated 1M-user fixture.
- All-user mutation/target paths: `get_all_user_id` is verified to execute a
  fresh query on every call, and the `创造力量` / `毁灭力量` handlers are checked
  to use that live target lookup for accessory batches. The normal item batch
  application resolves targets in its own live paged query. Virtual-realm
  challenge/list paths use current participant membership; the cached
  `get_impart_rank_display` projection is not used for challenge selection.
- These are targeted checks for the 23 positive fanout rows and measured
  representatives, not runtime execution of all 23 paths or all 674 command
  matchers. The inventory and source review provide the broader static coverage;
  unresolved dynamic dispatch remains the stated limitation.

## Benchmark Bounds

The 12-command baseline ran against 1,000,000 synthetic rows in
`xiuxian.db.user_xiuxian`: 388,411,392-byte user DB, 20 requests per command,
701.828 seconds wall, 186.793 MiB peak RSS, zero command errors. The fixture's
auxiliary feature tables are not expanded to one million rows, so the matrix
does not claim 1M-row fanout coverage for every subsystem. The after-run report
records each table's actual row count across fixture databases; targeted
repository tests provide the remaining subsystem-specific evidence.
This is the largest validated dataset. At linear row
sizing 10M would add roughly 3.9 GB before indexes/WAL; the host has under 2 GiB
available RAM and disk was 87% used at the pre-run check. Do not scale above
1M. The single serial after matrix retained the existing fixture, had 1,004 MiB
  available RAM and 8,768,040,960 bytes free disk at preflight, and passed the
  minimums of 768 MiB available RAM and 2 GiB free disk (disk guard includes the
  combined `.db` snapshots plus 1 GiB headroom).

## Follow-up Batch Starting Measurements

- `我的修仙信息`: the saved 1M after report measured the exact exp/stone
  aggregate 20 times for 3,965.444 ms total, with 215.792 ms SQL/request across
  the path. A public TOP slice is not a substitute for the exact result. The prior
  exact cache missed reuse because `update_last_check_info_time` invalidated the
  ordinary (non-snapshot) cache namespace after each response. The current
  implementation uses the snapshot namespace and keys each caller's result by
  a database-maintained score-set generation, read in the same transaction as
  the rank query. Expected matrix result, if no score writes or evictions
  intervene: one cold rank scan plus 19 exact cache hits while all profile/asset
  fields remain live.
- `活动排行`: its after-only 20-request sample executed the full
  `sqlite_master` table catalog 40 times for 132.958 ms total (15.415 ms SQL
  per request overall). This is runtime schema metadata, not personal state.
  The change shares the catalog in the bounded process cache and keys it by
  current SQLite `schema_version`; schema changes naturally select a new entry.
  Personal standings and reward/claim decisions are still computed live.
- These were the starting estimates for the follow-up batch. Corrected
  same-fixture measurements and their limits are recorded below.

The after process was capped at 1 GiB address space, two of four CPUs, 600
seconds CPU and 1,200 seconds wall time; it used 112.727 CPU seconds, 253.083
seconds wall time, and 194.387 MiB peak RSS. It retained a 1,000,000-row
`user_xiuxian` fixture, with auxiliary feature tables at the row counts recorded
in the JSON report. No duplicate 1M fixture was made for the after run. Future
performance claims require the same-fixture comparison and these resource guards;
passing correctness tests alone is not performance evidence.

## Runtime Batch Design and Source Evidence

The implemented batch targets measured repeated work without changing freshness or
mutation decisions:

- `我的修仙信息`: the prior 20-request matrix spent 3,965.444 ms in the exact
  exp/stone rank query (215.792 ms SQL/request overall). A database-maintained
  score-set generation now gates a bounded process cache of the exact rank
  tuple. Inserts, deletes, and exp/stone changes advance the generation in the
  same SQLite transaction; every request still reads the current generation
  before reuse. This is not a TOP-slice lookup and cannot return another
  player's stale position. If cache state survives all 20
  repeated samples, the expected expensive scans fall from 20 dual-field
  queries to one cold query; the after matrix must verify the hit/miss and SQL
  counters before any performance claim.
- `活动排行`: the prior 20-request matrix made 40 table-catalog probes. The
  updated read path groups the current player's rank aggregates in one
  read-only unit of work and combines repeated count/group scans. Expected
  reduction is one cold catalog probe per unchanged schema version and fewer
  rank-query round trips;
  validate against the prior 26.10 SQL calls/request and 34.084/45.451 ms
  p50/p95.
- `竞技场排行榜`: the prior matrix made 40 `PRAGMA table_info(arena)` probes
  over 20 requests. The updated handler reads the public top slice and exact
  current viewer rank in one read-only transaction, sharing one schema check;
  the bounded schema-version cache now limits the table-info read to one cold
  load for an unchanged schema; each request still checks the current schema
  version, and the viewer rank remains live.
- `历练排行榜`: the prior matrix made 40 `ATTACH DATABASE` calls and spent
  167.678 ms executing them across 20 requests (35.755/37.816 ms p50/p95,
  18.431 ms SQL/request). A cold public-snapshot load now computes its live
  viewer rank in the same attached read transaction. On a snapshot hit, only
  the viewer rank is queried live and the cached display rows remain governed
  by their existing 60-minute freshness marker. The cold-path expectation is
  at most 20 attaches instead of 40; the after report must confirm actual
  cache behavior and attach/query totals.
- `通天塔排行榜` / `通天塔积分排行榜`: the prior 20-request sample made 40
  `PRAGMA table_info(tower)` probes, costing 21.370 ms total (p50/p95
  27.593/35.480 ms; 28.20 SQL calls and 11.306 ms SQL/request). The cold
  display-snapshot load now shares its one schema probe and read transaction
  with the exact viewer rank. On a snapshot hit the position is still computed
  from a fresh read. The bounded schema-version cache should reduce table-info
  probes to one cold load for an unchanged schema; verify actual cache behavior
  and timings in the final matrix.
- Remaining measured schema probes on leaderboard paths are now routed through
  the bounded shared schema catalog, keyed by database identity and the live
  schema version. The old matrix recorded 40 `table_info(arena)` calls on the
  arena board, 40 `table_info(boss_limit)` plus 40 table-existence probes on
  the world-BOSS board, and 40 attached `table_info(training)` calls on the
  training board. The season-board path ran five default rank types per command
  and called `table_info(season_rank)` 100 times across 20 requests (14.241 ms);
  schema readiness now uses the same bounded cache keyed by database identity
  and live schema version. The schema cache should reduce cold metadata reads
  to one per database/table/schema version; each request still checks the
  current `schema_version`, and all personal values/ranks remain live. These
  reductions are implementation expectations only until the consolidated
  same-fixture matrix confirms them.
- `我的修为`: the 1M after sample measured 7.10 SQL calls/request and 7.819 ms
  SQL/request (17.007/18.966 ms p50/p95). Its handler reread the full
  `user_xiuxian` profile after `check_user` had already loaded it, and created a
  second `UserBuffDate` for the blessed-spot field. It now reuses the checked
  live profile and request-local BuffInfo snapshot. The query tracker only
  captured two profile reads and two BuffInfo reads across the sample, so those
  sparse table-level counts are not a per-request rate; confirm the reduction
  in the consolidated matrix.
- Markdown `我的背包`: the 1M after sample measured 6.70 SQL calls/request and
  8.023 ms SQL/request (17.281/20.407 ms p50/p95). After loading all live bag
  rows, the Markdown renderer queried equipment state once per equipment row;
  the tracker captured two such point reads (0.056 ms combined). The renderer
  now passes each row's state into the existing equipment-state helper. This
  removes the per-item N+1 read without caching mutable inventory; a focused
  regression asserts equipped/unequipped output from one list read.

These are implementation hypotheses grounded in the saved matrix, not measured
after results. The exact rank cache has a 60-minute upper bound and 32-entry /
1 MiB process-wide limits; a generation mismatch or eviction causes a live
recompute. The corrected full same-fixture matrix below validates the stable
runtime batch; the estimates above remain distinct from the measured outcomes.

The generation is updated transactionally for inserts, deletes, identity changes,
and exp/stone changes, including writes from other SQLite connections. Name-only
updates do not invalidate exact ranks. This does not eliminate the cold indexed
rank scan: frequent score writes invalidate reuse for all viewers, and a bulk
`update_ls_all()` updates the singleton once per changed row. The saved read-only
matrix cannot establish throughput or write overhead under an active workload;
do not extrapolate a warm, repeated-viewer benefit to all 1M users.

The independent boundary review also required two compatibility fixes: empty
activity identities retain the live participant count without claiming an empty
ID's sign/BOSS record, and combined public-board loaders carry an internal unique
load token. A cold personal value is reused only if its own public snapshot is
returned after invalidation/retry; otherwise it is read live. The cached payload
contains public rows, capture time and the internal token only, and the public
API strips that token. Personal values remain request-local. Focused coverage
includes discarded/retired loads, schema DROP/RENAME/file replacement/attached
aliases, WAL-consistent generation/rank reads and stone rollback; these checks
establish semantics, not an after performance measurement.

## Validated Runtime Batch (2026-11-26)

The corrected complete after report is
`/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05/data/command-matrix-after-runtime-batch-r02.json`.
It exited 0 with 560/560 successful requests: 28 real matcher/DI/handler paths,
20 requests per path, 0 errors and 0 offline-boundary denials. These are the
original 12 plus 16 stratified representatives, not exhaustive execution of
674 command matchers or all 41 scheduled/background entries. The preceding
28-path report is the before measurement for this follow-up batch; its 16
additional paths were after-only relative to the original 12-path baseline.

The r01 attempt for this batch is retained separately and excluded from the
performance conclusions. It exited 1 because all 20 training requests reached
a missing forwarding method on the composite TrainingRepository. Its top-level
JSON status incorrectly says pass; sample exceptions, error_rate and the CLI
exit code identify failure. The actual product wiring was fixed and a real,
default-constructed TrainingApplication regression added for both completed
and points, an out-of-TOP2 viewer, and fresh personal ranks beside stale public
rows. The other three combined board API chains were independently checked.
Only then was the corrected complete r02 matrix run, serially; no individual
command was retried for a better score.

Every current row below has n=20 and a 0% error rate. Queries and SQL time are
full per-request aggregate means; p50/p95 include the cold request. Cache H/M
means operation counters from shared_cache.counter_delta, not requests with
hits/misses. One request can touch several schema/display caches, so counts
can exceed 20 and must not be interpreted as viewer-cache hit rates.

| Command | n / errors | Previous -> current p50 / p95 ms | Queries/request previous -> current | SQL ms/request previous -> current | Current cache H/M |
| --- | ---: | ---: | ---: | ---: | ---: |
| `我的修仙信息` | 20 / 0 | 204.567 / 244.121 -> 33.569 / 123.688 | 41.80 -> 42.55 | 215.792 -> 22.616 | 19 / 1 |
| `我的资产总览` | 20 / 0 | 29.038 / 32.329 -> 29.950 / 36.461 | 26.50 -> 26.35 | 13.217 -> 13.027 | 0 / 0 |
| `我的状态` | 20 / 0 | 30.816 / 33.653 -> 30.643 / 32.348 | 31.50 -> 31.45 | 12.851 -> 12.621 | 0 / 0 |
| `我的修为` | 20 / 0 | 17.007 / 18.966 -> 17.108 / 20.064 | 7.10 -> 6.25 | 7.819 -> 4.933 | 0 / 0 |
| `我的背包` | 20 / 0 | 17.281 / 20.407 -> 17.454 / 18.657 | 6.70 -> 6.50 | 8.023 -> 5.509 | 0 / 0 |
| `丹药背包` | 20 / 0 | 17.316 / 21.060 -> 17.586 / 19.903 | 6.60 -> 6.60 | 8.353 -> 5.737 | 0 / 0 |
| `药材背包` | 20 / 0 | 17.279 / 31.990 -> 17.476 / 19.169 | 7.00 -> 6.40 | 8.578 -> 6.587 | 0 / 0 |
| `我的装备` | 20 / 0 | 17.277 / 19.107 -> 17.514 / 31.616 | 7.10 -> 6.40 | 9.459 -> 5.506 | 0 / 0 |
| `排行榜` | 20 / 0 | 18.732 / 22.221 -> 18.534 / 20.978 | 7.45 -> 7.15 | 6.142 -> 4.059 | 19 / 1 |
| `灵石排行榜` | 20 / 0 | 18.712 / 20.173 -> 18.325 / 19.777 | 7.30 -> 7.00 | 5.807 -> 4.340 | 19 / 1 |
| `战力排行榜` | 20 / 0 | 18.683 / 19.466 -> 18.205 / 19.413 | 7.40 -> 7.05 | 6.202 -> 3.627 | 19 / 1 |
| `宗门排行榜` | 20 / 0 | 18.366 / 20.028 -> 17.799 / 19.253 | 7.50 -> 7.10 | 6.429 -> 3.912 | 19 / 1 |
| `轮回排行榜` | 20 / 0 | 19.067 / 20.467 -> 18.735 / 19.848 | 7.25 -> 7.05 | 5.540 -> 3.870 | 19 / 1 |
| `活动排行` | 20 / 0 | 34.084 / 45.451 -> 29.420 / 31.337 | 26.10 -> 20.05 | 15.415 -> 11.876 | 0 / 0 |
| `赛季榜` | 20 / 0 | 21.438 / 22.275 -> 21.790 / 23.643 | 13.85 -> 13.70 | 6.187 -> 4.937 | 293 / 7 |
| `竞技场排行榜` | 20 / 0 | 21.363 / 22.731 -> 20.127 / 27.687 | 19.25 -> 13.20 | 7.363 -> 5.619 | 19 / 1 |
| `世界BOSS积分排行榜` | 20 / 0 | 27.470 / 29.474 -> 25.704 / 28.153 | 29.30 -> 21.55 | 11.503 -> 11.727 | 38 / 2 |
| `虚神界排行榜` | 20 / 0 | 17.887 / 19.055 -> 18.476 / 22.068 | 7.20 -> 7.30 | 3.766 -> 4.545 | 19 / 1 |
| `宗门周常排行` | 20 / 0 | 17.939 / 20.744 -> 17.995 / 19.566 | 7.30 -> 6.90 | 5.327 -> 6.761 | 19 / 1 |
| `宗门战力排行榜` | 20 / 0 | 17.514 / 21.633 -> 17.499 / 21.541 | 6.70 -> 6.25 | 3.716 -> 5.000 | 19 / 1 |
| `道侣排行榜` | 20 / 0 | 23.898 / 26.915 -> 23.885 / 26.879 | 16.25 -> 16.45 | 8.691 -> 9.689 | 19 / 1 |
| `师徒排行榜` | 20 / 0 | 24.027 / 26.666 -> 24.294 / 26.517 | 16.40 -> 16.45 | 8.400 -> 9.197 | 19 / 1 |
| `历练排行榜` | 20 / 0 | 35.755 / 37.816 -> 29.992 / 31.173 | 33.15 -> 23.35 | 18.431 -> 13.190 | 38 / 2 |
| `通天塔排行榜` | 20 / 0 | 27.593 / 35.480 -> 26.083 / 28.720 | 28.20 -> 21.25 | 11.306 -> 9.903 | 38 / 2 |
| `活动首领排行` | 20 / 0 | 19.178 / 20.285 -> 19.119 / 21.056 | 7.40 -> 7.10 | 4.754 -> 4.058 | 0 / 0 |
| `前尘排行` | 20 / 0 | 24.340 / 28.958 -> 23.920 / 25.970 | 16.65 -> 16.40 | 9.767 -> 8.389 | 19 / 1 |
| `修炼` | 20 / 0 | 74.039 / 93.102 -> 71.265 / 88.268 | 94.05 -> 94.00 | 31.700 -> 31.016 | 0 / 0 |
| `闭关` | 20 / 0 | 44.325 / 60.757 -> 41.137 / 48.588 | 56.00 -> 56.00 | 17.034 -> 15.238 | 0 / 0 |

### Observed SQL Changes and Limits

- Exact personal dual-rank SQL fell from 20 executions / 3,965.444 ms to one
  execution / 169.497 ms, with 19 exact-result hits and one miss. Profile p50
  improved 83.59%, p95 49.33%, and SQL time/request fell from 215.792 to
  22.616 ms. Total SQL calls rose slightly (41.80 to 42.55), reflecting the
  live generation/transaction checks rather than eliminating all reads.
  The cold request still took 285.804 ms; the second request took 123.688 ms.
  No score changes occurred between these repeated requests from one viewer:
  this does not establish cold, write-heavy or 1M-distinct-viewer throughput.
- Training ATTACH calls fell from 40 / 167.678 ms to 20 / 80.244 ms. Its
  p50/p95 improved 16.12%/17.57%, queries/request 33.15 -> 23.35. BOSS and tower
  query means fell 29.30 -> 21.55 and 28.20 -> 21.25; their personal ranks
  remain live on public-snapshot hits.
- Captured column probes for arena, BOSS and tower each fell from 40 table_info
  calls to one; season_rank fell from 100 to one. Live schema_version checks
  remain. The top-20 SQL list is time-ranked and not a complete statement
  inventory; absence there is not proof that a statement never executed.
- The activity overview improved 34.084/45.451 -> 29.420/31.337 ms, with query
  means 26.10 -> 20.05. This no-argument sample still does not measure the
  detailed activity-board display snapshot, and reports zero shared-cache
  operations for that command.
- Cultivation value query means fell 7.10 -> 6.25, but p50/p95 did not improve
  (17.007/18.966 -> 17.108/20.064 ms). Backpack p50 likewise did not improve
  (17.281 -> 17.454 ms). Their sparse query samples cannot establish a fixed
  per-request reduction. The Markdown equipment-state regression separately
  proves removal of per-equipment point reads without caching inventory.
- Arena queries/request fell 31.43%, but p95 worsened 22.731 -> 27.687 ms.
  Asset overview, equipment and several otherwise unchanged paths also have
  worse tail samples. BOSS SQL time/request rose slightly despite fewer calls.
  These results are retained; this one serial sample under uncontrolled host
  I/O is not evidence of a latency win for every path. No extra runs were made
  to replace those measurements.
- The fixture's arena, boss_limit, training, tower, season_rank,
  activity_boss_damage and past_life tables all had zero rows at capture.
  Populated focused fixtures prove ranking/tie/freshness behavior, but the
  matrix does not measure large populated feature-table scans or sorts.
  The only 1M-row claim is for user_xiuxian.
- Score-set generation changes transactionally on relevant writes. Cold scans,
  global invalidation under active scoring, and the per-row generation update
  in bulk update_ls_all remain costs. Write-heavy overhead and sustained
  concurrency were not benchmarked; no write-performance claim is made.

### Original Twelve-Path Comparison

These cumulative deltas also include the predecessor's optimizations; they
must not all be attributed to the current runtime batch. Both reports use the
same 1M-user fixture identity and rank-data digest. Every current row again
represents 20 requests and zero errors.

| Original baseline command | Original -> current p50 / p95 ms | Queries/request original -> current | SQL ms/request original -> current |
| --- | ---: | ---: | ---: |
| `我的修仙信息` | 423.107 / 471.625 -> 33.569 / 123.688 | 42.45 -> 42.55 | 389.609 -> 22.616 |
| `我的资产总览` | 82.588 / 85.240 -> 29.950 / 36.461 | 34.60 -> 26.35 | 32.254 -> 13.027 |
| `我的状态` | 83.663 / 2165.501 -> 30.643 / 32.348 | 34.10 -> 31.45 | 31.699 -> 12.621 |
| `我的修为` | 19.209 / 74.368 -> 17.108 / 20.064 | 9.20 -> 6.25 | 18.539 -> 4.933 |
| `我的背包` | 19.759 / 74.427 -> 17.454 / 18.657 | 9.10 -> 6.50 | 18.721 -> 5.509 |
| `丹药背包` | 19.851 / 73.416 -> 17.586 / 19.903 | 9.00 -> 6.60 | 23.101 -> 5.737 |
| `药材背包` | 19.973 / 73.591 -> 17.476 / 19.169 | 9.00 -> 6.40 | 18.462 -> 6.587 |
| `我的装备` | 20.765 / 76.374 -> 17.514 / 31.616 | 9.15 -> 6.40 | 20.125 -> 5.506 |
| `排行榜` | 698.505 / 758.627 -> 18.534 / 20.978 | 9.40 -> 7.15 | 708.876 -> 4.059 |
| `灵石排行榜` | 23.596 / 73.755 -> 18.325 / 19.777 | 9.30 -> 7.00 | 11.721 -> 4.340 |
| `修炼` | 1875.785 / 2107.214 -> 71.265 / 88.268 | 94.00 -> 94.00 | 71.383 -> 31.016 |
| `闭关` | 1815.915 / 2196.466 -> 41.137 / 48.588 | 56.00 -> 56.00 | 38.799 -> 15.238 |

Action elapsed times include gameplay waits and host effects. Their nearly unchanged
query counts in this follow-up batch do not support a new database optimization
claim for cultivation or closing.

### Resource and Acceptance Evidence

- Retained fixture: 1,000,000 user rows; a separate read-only, capped post-run
  check recomputed the original rank-data SHA256:
  `0dd134931a7cb0aa3511cc90d50c3c873b3cea97f576dd74e7e536af577ad8c7`.
  No expansion or second persistent 1M fixture was created. The harness used
  only its existing temporary restore snapshots. The five runtime DBs totaled
  441,802,752 bytes at r02 preflight; the new generation table accounts for
  the 8,192-byte increase from the preceding DB total.
- Hard limits remained 1,024 MiB address space, two of four CPUs, 600 seconds
  CPU and 1,200 seconds wall. Measured process use: 242.964 seconds wall,
  38.495 user + 67.434 system = 105.929 CPU seconds, 206.340 MiB peak RSS.
- Harness preflight available RAM: 1,730 MiB against 768 MiB minimum. Free disk
  before/after: 8,027,168,768 / 7,582,928,896 bytes against 2,147,483,648 minimum;
  disk guard includes all runtime DB snapshots plus headroom. Recorded WAL
  after restore: 0 bytes (limit 256 MiB). No overlapping heavy validation ran.
- Largest reported command-end cache state: 26 entries / 79,845 estimated
  bytes, below the unchanged 32-entry / 1,048,576-byte process-wide budget;
  this is a sampled state, not a continuous allocation-peak measurement.
- Consolidated focused acceptance: 115 tests in 3.118s. Full unittest discovery
  covered 6,734 cases in two serial shards (593.611s and 822.446s); four stale
  source/cache-counter contracts initially failed and were repaired without
  changing runtime behavior, with 8 and 31 affected-module tests passing.
  After the real training wiring fix, 39 directly related tests passed in
  1.066s, including one new integration case. Unchanged full shards were reused.
- CI-equivalent focused pytest: 1,073 passed in 184.54s. Final changed-source
  compileall, architecture, generated-inventory and whitespace gates passed.
  The generated inventory change is only the new generation table name.
  These correctness results supplement the matrix; they do not replace it.

## Shared-Instance Mixed Workload (2026-11-26)

The serial evidence above is a pre-created 1M-row user table and mostly
repeated viewers, not mixed-user capacity evidence. This successor measures
one actual runtime and one shared isolated database set per phase through real
matcher/rule/permission/DI/handler execution. Only OneBot transport is replaced;
business results, eligibility, cooldowns and writes execute normally. No QQ or
OneBot service, production database or deployment is involved.

### Scope and Reproduction

- Retained fixture: `/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05`.
  Its original 1M-user rank-data digest remains
  `0dd134931a7cb0aa3511cc90d50c3c873b3cea97f576dd74e7e536af577ad8c7`.
  Only one temporary all-DB restore snapshot exists during each process. There
  are no per-user databases, between-request restores, or extra persistent
  million-row fixtures. All requests and actual message-writer jobs drain
  before stage checks and restore.
- Baseline runtime is exact `3acaac96`, in the code-only detached worktree
  `/tmp/xiu-mixed-baseline-20261126-r01`; runtime source SHA256 is
  `6d6c6d1802ebebbebeafac489bdc2a04c419c696a43a7cf4ec2b73e3fbc3c389`.
  Candidate adds the independent rank generations and batched trade statistics
  described below. Both use the same fixture, participant selection, item
  catalog, request plan, arrival rate and resource limits.
- Select 32 existing participants by indexed row ID, spread across the retained
  dataset. Seed 640 backpack rows, 32 equipment/BuffInfo records, 32 activity
  users, 128 rows each in activity points/pass/BOSS balances, and 32 real trade
  listings with 7,944 total items. Stage manifests record actual table counts,
  seed digest and activity projection hashes. Activity availability is enabled
  only in the isolated projection, retaining costs/rewards/limits/cooldowns;
  the original bytes are restored. These are bounded feature datasets, not
  million-row feature tables or evidence about unmeasured arena/tower data.
  Participants start with equal 500M wallets and their original exp values;
  cross-user stone-rank reuse benefits from that synthetic tie distribution,
  which is not a measured production balance distribution.
- Each version has 16 phases: pure reads, 90/10 reads/writes, 50/50, and
  same-user/resource contention, each with admission limits 1/5/10/20. Each
  phase schedules 128 arrivals at 20/sec independently of the event loop.
  Pure/read-mostly/mixed distribute readers among 32 users; contention shares
  one wallet and an eight-item listing. This is 2,048 requests/version across
  13 real commands, a stratified sample rather than all 674 matchers or the
  41 background entries. The workload does not measure exp-changing gameplay
  or sustained 50 successful requests/sec.
- The producer is an independent thread. Reports retain scheduled/posted/
  received/admitted/completed times, producer and loop dispatch delay, admission
  queueing, response E2E, service time and heartbeat lag. Actual handler interval
  overlap is measured; it is explicitly not proof of parallel CPU or SQLite
  execution. E2E includes scheduled arrival to response, not just admitted work.
- SQL counts include execute/executemany/executescript and explicit commits/
  rollbacks; fetch time is separate. Per-command SQL is foreground only. Real
  durable message-writer SQL is measured separately and included in phase
  totals, because a batch commit cannot be assigned honestly to one request.
  Queue unfinished/reserved bytes and dropped-job counters are checked. Unknown
  SQL ownership still fails the run; it is not silently discarded.
- Phase cache numbers are exact shared-counter deltas, not per-request hit
  rates. Exact-rank recomputations are counted from the complete SQL recorder,
  not its top-20 display. BEGIN duration is reported as an upper bound including
  execution, not a pure transaction lock-wait measurement. SQLite busy/locked,
  successful work, business refusals, cooldowns, exceptions and timeouts remain
  distinct.

The command (with explicit existing fixture/source and a new report path) is:

```bash
MALLOC_ARENA_MAX=2 XIUXIAN_MATRIX_LIMITS=1 .venv/bin/python \
  scripts/mixed_command_probe.py --fixture FIXTURE --source-repo SOURCE \
  --report NEW_REPORT --requests-per-phase 128 --arrival-rate 20
```
The script owns `/tmp/nonebot-project-validation.lock`; do not nest the same
lock or run another heavy validation alongside it. `--preflight-only` resolves
all routes and validates seed/restore without dispatch. `--bulk-only` runs
only the separately bounded rollback control, permitting reuse of unchanged,
already validated request stages after a post-traffic control setup failure.

### Runtime Changes and Semantics

1. Split exact numeric rank invalidation into transactional exp/stone counters,
   retaining the legacy global counter for compatibility. A stone-only update
   no longer invalidates exp ranks. Read current target canonical scores and
   generations in the same BEGIN snapshot as any cold counts; reuse only an
   identical score/column/generation/database key. Tied users can share an exact
   result. This is not a cached TOP slice. Missing legacy generation columns
   fall back to live SQL, and unrelated DB errors propagate. The shared budget
   remains 32 entries / 1 MiB; distinct cold scores and changing score sets can
   still require expensive exact index range scans.
2. Batch one trade event's statistics per user: resolve impersonation once,
   project only changed counters, then one BEGIN IMMEDIATE/current read/UPSERT/
   final projection/commit under the manager lock. Failed or rewritten writes
   roll back all counters; large totals retain exact integer representation and
   display decoding. This removes repeated full-row reads and per-field commits
   in purchase/sale statistics. Single-counter events retain verification costs.
   Trade principal settlement is still its existing transaction; post-trade
   statistics are not claimed to be atomic with that principal settlement.

Balances, inventory, attributes, targets, reward/eligibility decisions and writes
stay live. The statistics change does not alter the separate legacy single-field
helper's concurrency behavior or solve cold cross-process dynamic-column DDL
races. Both remain limitations outside this measured warmed-schema event path.

### Evidence Integrity

Reports are under the retained fixture's `data` directory. Initial setup-only
failures and `mixed-before-20261126-r01.json` are excluded from performance
claims. The latter stopped after its first phase because message-writer SQL
lacked ownership instrumentation; its 626 operations cannot be retroactively
attributed. The corrected harness labels the real consumer and drains its real
queue, preserving unknown-operation rejection.

`mixed-before-20261126-r02.json` completed all 16 request phases with passing
ownership/resource/consistency checks, then exited 1 in the separate bulk
control because original seed values included ambiguous legacy currency.
That failed overall status is retained. Its unchanged valid request phases
are reused, not rerun for better numbers. `mixed-before-bulk-20261126-r01.json`
also failed selection setup without traffic; the valid supplement is
`mixed-before-bulk-20261126-r02.json`. It uses the identical baseline source
digest and original fixture, with a deterministic 2,048-row synthetic numeric
seed inside an outer rollback. All original values are restored and checked.
This isolates trigger cost while preserving actual numeric rejection rules.

The control tests stone, exp and both together, three enabled/control samples
each with alternating order, retaining UDFs/expression indexes. Trigger removal
is only inside a rolled-back child savepoint; it is never a gameplay option.
2,045 of the selected original rows contain an inexact value, so these seeded
subset timings must not be extrapolated to an unseeded million-row update.

### Mixed Workload Results

`mixed-after-20261126-r01.json` exited 0: all 16 phases and the bulk control
passed. Candidate runtime source SHA256 is
`012e81fee6301537426d5a20b1fc80c4823c0fca04108b8f562f419eb22816ad`.
An explicit cross-report check confirmed identical request objects, all 16
seed digests, phase order, arrival rates and original rank-data digests. The
bulk supplement/candidate also match selected IDs and seeded/original data
digests. Neither failed report was rewritten or relabeled as passing.

Each version has 1,888 successful responses and 160 business refusals: all
four contention phases have exactly 88 successes and 40 sold-out purchases.
Other phases have 128 successes each. There are zero cooldowns, exceptions,
timeouts, boundary violations, unknown SQL operations or resource aborts.
All balance/inventory/nonnegative/proceeds/statistics/receipt-ledger checks
pass; message jobs drain with zero drops or residual reservations. Writes are
344 successful deposits and 184 successful purchases per version, plus the
160 refused purchase attempts. Business refusals are excluded from successful
throughput, not counted as high-throughput successful transactions.

Each row below has n=128/version. Latencies are response E2E including scheduled
arrival, producer delay, event-loop dispatch and admission queueing. SQL counts
include foreground and real asynchronous message persistence. Loop is heartbeat
p95 in ms. Entries show before -> after; these are finite samples, not a
capacity certification or confidence intervals.

| Mix | Limit | p50 ms | p95 ms | p99 ms | SQL calls | Loop p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Pure | 1 | 49.049 -> 21.131 | 194.674 -> 151.288 | 233.155 -> 234.541 | 3496 -> 3496 | 128.586 -> 28.992 |
| Pure | 5 | 42.266 -> 19.235 | 226.309 -> 102.264 | 250.784 -> 255.017 | 3472 -> 3482 | 147.878 -> 22.475 |
| Pure | 10 | 29.398 -> 24.330 | 223.051 -> 220.755 | 245.430 -> 298.141 | 3471 -> 3478 | 142.448 -> 39.843 |
| Pure | 20 | 36.159 -> 19.449 | 219.257 -> 112.562 | 239.884 -> 224.500 | 3468 -> 3483 | 144.157 -> 18.537 |
| Read-mostly | 1 | 49.342 -> 51.592 | 183.566 -> 182.168 | 210.942 -> 207.073 | 4119 -> 4085 | 125.989 -> 113.208 |
| Read-mostly | 5 | 58.734 -> 41.106 | 281.183 -> 210.335 | 379.532 -> 251.328 | 4102 -> 4072 | 158.945 -> 99.658 |
| Read-mostly | 10 | 42.660 -> 47.099 | 225.016 -> 215.439 | 263.047 -> 262.206 | 4106 -> 4070 | 162.057 -> 144.697 |
| Read-mostly | 20 | 61.893 -> 39.232 | 266.288 -> 218.742 | 368.629 -> 247.294 | 4104 -> 4072 | 157.777 -> 101.096 |
| Mixed | 1 | 59.235 -> 63.517 | 202.971 -> 206.717 | 220.838 -> 277.805 | 6653 -> 6434 | 43.672 -> 45.447 |
| Mixed | 5 | 71.350 -> 63.037 | 273.014 -> 234.120 | 329.506 -> 290.843 | 6634 -> 6423 | 126.743 -> 73.076 |
| Mixed | 10 | 66.566 -> 72.451 | 247.574 -> 322.347 | 309.552 -> 396.635 | 6641 -> 6411 | 66.107 -> 102.132 |
| Mixed | 20 | 93.914 -> 66.214 | 319.412 -> 268.043 | 360.624 -> 316.013 | 6633 -> 6420 | 124.126 -> 84.856 |
| Contention | 1 | 886.030 -> 331.335 | 1525.710 -> 458.396 | 1591.821 -> 479.540 | 6568 -> 6596 | 182.902 -> 91.811 |
| Contention | 5 | 993.116 -> 295.809 | 1463.697 -> 392.403 | 1525.767 -> 425.633 | 6518 -> 6568 | 187.941 -> 102.352 |
| Contention | 10 | 1013.257 -> 196.068 | 1474.488 -> 371.742 | 1535.925 -> 429.405 | 6522 -> 6574 | 210.540 -> 107.057 |
| Contention | 20 | 1037.425 -> 383.722 | 1645.195 -> 547.639 | 1810.835 -> 613.615 | 6508 -> 6542 | 215.361 -> 125.297 |

Pure/read-mostly successful response throughput remains near the imposed
20 arrivals/sec (19.999-20.099); mixed remains 19.891-19.974. Contention
successful throughput at limits 1/5/10/20 is respectively
11.178/11.335/11.471/11.432 -> 13.363/13.429/13.771/13.383 per second,
with the same 40 refusals/phase. Actual maximum handler interval overlap is
18 -> 8; higher limits were configured but do not imply 20 simultaneous SQL
executions. Shorter queueing can lower observed overlap. Admission queue max
falls 1550.079 -> 421.853ms; max producer lateness is still 148.240 ->
134.328ms, and max loop-dispatch delay 261.985 -> 213.150ms. The producer
is not perfectly punctual, and these delays remain inside E2E.

All command-level results below aggregate the same deterministic phase mix
and all four admission limits. This aggregation is not an individual-command
standalone benchmark. Error rate is 0% for every row; the purchase row includes
160 business refusals (184 successes), while all other samples succeed.
SQL columns are foreground means/request; asynchronous message SQL is included
only in phase totals. Before -> after values are retained even when worse.

| Command | n/version | p50 ms | p95 ms | p99 ms | Queries/request | SQL ms/request |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `我的修仙信息` | 180 | 221.011 -> 188.772 | 1393.389 -> 436.648 | 1525.767 -> 570.796 | 49.88 -> 50.19 | 169.318 -> 109.215 |
| `我的资产总览` | 180 | 228.882 -> 175.170 | 1331.799 -> 393.851 | 1519.329 -> 446.142 | 28.87 -> 28.81 | 12.847 -> 13.543 |
| `我的状态` | 116 | 141.861 -> 83.494 | 223.049 -> 228.617 | 269.432 -> 262.945 | 41.07 -> 41.14 | 13.016 -> 13.904 |
| `我的修为` | 116 | 100.179 -> 61.982 | 187.789 -> 214.298 | 236.542 -> 268.923 | 18.00 -> 18.00 | 8.070 -> 8.342 |
| `我的背包` | 116 | 67.362 -> 28.348 | 137.563 -> 164.799 | 281.183 -> 220.755 | 18.00 -> 18.00 | 8.018 -> 8.476 |
| `丹药背包` | 116 | 36.837 -> 21.320 | 148.481 -> 126.677 | 250.218 -> 192.367 | 18.00 -> 18.00 | 8.160 -> 8.540 |
| `药材背包` | 112 | 19.273 -> 19.177 | 124.570 -> 97.733 | 227.380 -> 199.676 | 18.00 -> 18.00 | 8.159 -> 8.336 |
| `我的装备` | 108 | 18.877 -> 18.948 | 110.262 -> 55.016 | 183.177 -> 149.816 | 18.15 -> 18.15 | 8.190 -> 8.347 |
| `排行榜` | 108 | 13.103 -> 13.274 | 68.338 -> 37.539 | 170.733 -> 169.092 | 6.11 -> 6.11 | 3.602 -> 3.705 |
| `灵石排行榜` | 104 | 13.058 -> 13.046 | 36.475 -> 29.205 | 185.104 -> 109.889 | 6.12 -> 6.12 | 3.598 -> 3.609 |
| `活动排行` | 104 | 23.997 -> 24.199 | 45.473 -> 35.307 | 115.209 -> 61.239 | 22.15 -> 22.15 | 11.698 -> 11.809 |
| `鬼市存灵石` | 344 | 462.177 -> 231.668 | 1475.119 -> 449.584 | 1600.976 -> 547.639 | 39.67 -> 41.24 | 18.307 -> 18.720 |
| `仙肆购买` | 344 | 466.229 -> 218.940 | 1490.555 -> 444.825 | 1650.108 -> 531.810 | 78.11 -> 73.76 | 30.596 -> 30.664 |

### Attribution and Remaining Bottlenecks

All exact profile scans are counted from full SQL aggregations. Each row below
describes one phase; values repeat across its four admission limits. Cache H/M
includes public/schema/exact-rank operations, not only profile hits. The legacy
generation delta is unchanged; new exp delta is 0 in this stone-write workload.

| Mix | Profile n | Exp recomputations | Stone recomputations | All-cache H/M | Legacy / new stone generation delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| Pure | 12 | 12 -> 10 | 12 -> 1 | 20/14 -> 33/13 | 0 / 0 |
| Read-mostly | 11 | 11 -> 9 | 11 -> 11 | 18/13 -> 20/22 | 18 / 18 |
| Mixed | 6 | 6 -> 5 | 6 -> 6 | 9/8 -> 10/13 | 96 / 96 |
| Contention | 16 | 16 -> 1 | 16 -> 16 | 0/16 -> 15/17 | 64 / 64 |

- Contention confirms that unrelated stone writes no longer force exp rescans:
  profile rank SQL takes 2,713-2,871 -> 1,431-1,478ms/phase. Stone ranks still
  recompute 16 times; they are not returned stale. Pure-viewer stone ties share
  one exact count, reducing rank SQL 1,645-1,740 -> 550-590ms/phase. Synthetic
  equal wallets and repeated contention identity are favorable cache cases.
- Mixed/read-mostly still cold-scan most profiles: different exp values and
  changing stone sets limit cache reuse. Two scalar cache checks can increase
  misses and statement count versus one combined miss; a miss counter is not
  itself evidence of a regression or improvement. Live cold ranking remains
  a main event-loop blocking source. No unbounded per-user rank map was added.
- Foreground calls across all traffic fall 73,295 -> 72,393. Total foreground
  plus message SQL time falls 61,798.309 -> 51,502.858ms. Background queries
  vary 9,720 -> 9,813 because real queue batches and commits interleave
  differently. These are measured time/call reductions, not a claim that every
  path's own SQL got faster.
- Purchase foreground calls/request fall 78.11 -> 73.76, reflecting batched
  statistics. Single-counter deposits increase 39.67 -> 41.24, including the
  atomic current-read/final-projection checks. Purchase SQL mean is essentially
  unchanged and deposit SQL mean rises; queue-inclusive latency benefits also
  come from reducing blocking profile work. No claim of faster transaction
  execution on every write path is supported.
- Regressions remain: mixed/limit10 p95 247.574 -> 322.347ms and p99
  309.552 -> 396.635ms; pure/limit10 p99 245.430 -> 298.141ms. Cultivation-value
  and backpack aggregate p95 also worsen. No selective rerun replaced these
  samples. A single ordered before/after experiment, small per-path phase
  samples and host I/O variability do not establish statistical significance.
- No SQLite busy/locked errors occurred. Maximum observed BEGIN time rises
  0.990 -> 3.209ms; it includes execution and is not a proven lock-wait
  regression. Response-to-message-queue-drain tails are recorded separately;
  candidate phase drain/teardown remains 2.6-26.1ms, excluded from response
  E2E but included in phase wall. This does not test multi-process writers,
  continuously growing game data or long-running 50-success/sec capacity.

### Bounded Bulk Counter Control

Same 2,048 IDs and deterministic seeded values, three samples per enabled or
disabled variant. No real command traffic runs while triggers are disabled.
Before data is from the valid bulk supplement, after from the complete after
report. All values below are p50 milliseconds, before -> after.

| Fields updated | Trigger enabled | Trigger-free control | Incremental difference |
| --- | ---: | ---: | ---: |
| stone | 83.913 -> 84.318 | 81.530 -> 81.682 | 2.383 -> 2.636 |
| exp | 94.444 -> 96.517 | 92.893 -> 95.574 | 1.551 -> 0.943 |
| exp + stone | 174.389 -> 172.637 | 171.713 -> 171.318 | 2.676 -> 1.319 |

Enabled legacy counter delta is exactly 2,048/statement, disabled delta zero;
candidate field counters change only for updated columns. Added split counters
do not show a large write-cost increase in this bounded sample. Small noisy
incremental differences are not proof of a trigger speedup. Per-row legacy
generation updates still exist; no million-row update time is extrapolated.
Exp-changing gameplay and its sustained invalidation behavior remain unmeasured.

### Guards and Validation

- Both traffic versions used the same lock, two-CPU affinity, 1,024MiB address
  space, 600s CPU / 1,200s wall caps, 768MiB minimum available RAM, 2GiB free
  disk minimum, 900MiB RSS soft stop and 256MiB total/per-file WAL stop. A
  separate 250ms watchdog monitors RAM/RSS, CPU, process I/O, disk and WAL,
  stops new admissions on breach and permits draining. No limits were relaxed.
- Before traffic process: 207.195s wall, 164.457s CPU, 237.375MiB peak RSS;
  valid bulk supplement: 28.732s wall, 23.217s CPU, 234.242MiB RSS. After with
  all traffic and bulk: 204.451s wall, 156.723s CPU, 269.562MiB RSS. Whole-run
  times include setup/snapshot/restore, and the before process ends in the
  failed post-traffic control; use per-phase metrics for the comparison.
- Observed phase minimum available RAM: 1,728 -> 1,698MiB; minimum free disk:
  7,362,310,144 -> 7,289,741,312 bytes. Maximum total WAL: 9,599,760 ->
  9,093,000 bytes; largest single WAL 4,190,072 -> 4,177,712 bytes. All WALs
  are zero after final restore. Retained JSON report snapshots also grow
  harness RSS; the process RSS difference is not isolated application cache
  growth. Largest sampled cache state: 16/41,367 -> 22/49,171 entries/bytes,
  within the unchanged 32-entry/1MiB bound.
- Process write_bytes rises by about 16.3GB in each traffic run, dominated by
  repeatedly restoring the same five DB files. This is I/O amplification from
  the harness, not durable data growth or a measured gameplay-write volume.
  Reports retain phase I/O separately. No duplicate long-lived 1M DB remains.
- Concentrated acceptance: 115 affected tests plus 35 subtests passed in
  9.93s. Compileall, architecture (`ok=true`, `errors=[]`), generated inventory
  and whitespace passed on the exact code-only candidate tree, whose legacy
  inventory file remains at HEAD. The main checkout's user-owned inventory
  deletion and status edits are untouched. No additional full unittest or old
  serial matrix was run; the preceding full baseline is reused, with the
  new delivery's exact-SHA CI required separately.

This closes a bounded mixed-workload implementation/measurement batch, not the
repository-wide performance problem. Distinct-user cold exact ranks, frequent
dual-field gameplay updates, populated large feature tables, multi-process
contention and sustained 50 successful requests/sec remain open evidence gaps.

## Distinct-Score Ranks And Dual-Field Gameplay: 2026-11-26

This follow-up keeps the overall performance goal active. The preceding
`78549516` delivery passed exact-SHA CI `45727901853`; its results and the
Mixed/limit10 regression above remain unchanged. This experiment uses different
scores and actual exp-changing gameplay, so its absolute numbers must not be
compared with the preceding equal-wallet/store workload.

### Frozen Workload And Valid Reports

- Runtime before: exact `785495169dba33fe0750dfba9859916201e3dab1`, tree digest
  `012e81fee6301537426d5a20b1fc80c4823c0fca04108b8f562f419eb22816ad`.
  Candidate runtime digest:
  `797ab1f591bc28af23fe832ba749f68dbb6bd8819d1ad62b15188a304136731a`.
  Candidate HEAD still names the parent; the digest identifies uncommitted code.
- Valid raw reports under the same retained fixture's `data/`:
  `rank-gameplay-before-20261126-r03.json` and
  `rank-gameplay-after-20261126-r01.json`, with append-only phase journals.
  Fixture identity, all 13 ordered request plans and seed digests match;
  seed digest is
  `20ec5fb24f1c249a9034c7b20b8e50f8e9c4284fb1b54375d041be5bc3f3ff60`.
  Both restored original rank digest
  `0dd134931a7cb0aa3511cc90d50c3c873b3cea97f576dd74e7e536af577ad8c7`.
- One shared runtime/event loop/database set per version; 128 existing users
  have different exp and stone values, real equipment/BuffInfo and 2,560
  backpack rows. Activity tables contain 8,192 users and 32,768 rows each in
  point/pass/boss tables across four configured activity keys. These are the
  selected feature populations, not million-row feature tables. Only synthetic
  activity availability windows change; original configuration bytes restore.
- Seven real matcher/DI/handler paths: profile, assets, state, cultivation,
  backpack, activity rank and `灵石修炼 1000`. Training really gains 100 exp
  and spends 1,000 stone; actor snapshots and eligibility remain live. Cold
  stages use 128 distinct users and uncached result keys, not cold OS pages.
  Read-mostly has 12 writes/128 calls; mixed has 64/128 or 128/256. Contention
  targets the same training actor. Cooldowns and gameplay rules stay intact.
- Each version: 1,920/1,920 successes, zero business/cooldown/replay refusals,
  exceptions, timeouts, SQLite busy/locked or unknown/boundary SQL. All 13
  phase invariants pass: exact exp/stone/power deltas, receipt/ledger identity,
  no unsettled operations, unchanged inventory, 1M users and DB quick checks.
  This is a seven-command stratified sample, not exhaustive coverage of 674.
- Before r01 failed thread creation under the AS limit; before r02 reached
  final traffic but failed full-string JSON serialization. Neither is valid
  comparison evidence. Logs/reports remain; no successful result was replaced
  to select a favorable number. Both valid versions start with
  `MALLOC_ARENA_MAX=2`, retain the same AS/RSS limits, stream JSON and journal
  drained phases. A 44-worker allocation/serialization micro-preflight passed
  under those limits before r03. The sole additional candidate-side drain hook
  waits for its new rank workers; measured traffic/seed/instrumentation match.

### Runtime Changes

- Complete live exp/stone rank reads run in two dedicated workers, with an
  asynchronous two-slot admission bound before executor submission. ContextVar
  propagation preserves request SQL attribution. Repeated cancellation waits
  for real SQLite completion before releasing the slot. Drain includes waiting
  requests; synchronous close/reconnect wait for worker completion independently
  of the event loop, and shutdown drains first. This does not claim a global
  cross-thread database-restore barrier; concurrent administrative replacement
  of a database remains outside the measured workload.
- The existing bounded 32-entry/1MiB exact score cache and live transaction
  semantics remain. Target scores, generations and counts share one SQLite
  read snapshot; no TOP slice or stale external profile value supplies rank.
  A discarded read-pool connection now lets waiters recheck capacity rather
  than wait forever. Configuring a very small shared pool can still cause
  synchronous pool waits in other readers.
- Stone training reuses its checked user projection. The writer rechecks the
  current snapshot inside `BEGIN IMMEDIATE`. Exact bindings prevent exp/power
  overflow into SQLite REAL; ordinary binary-multiplier/ROUND behavior stays
  compatible, with integer-ratio rounding beyond the exact floating range.
  One final joined read after receipt insertion verifies all player fields and
  the complete receipt. IGNORE/trigger rewrites roll back, report a specific
  reason and release only the rollback-proven unsettled reservation for retry.
  That reservation read offsets the removed duplicate read: measured training
  foreground calls remain 56/request. No query-count speedup is claimed there.

### All Phase Results

Milliseconds before -> after; every 128-call phase is one sample, and the two
highest arrival stages have 256 calls each. E2E includes scheduled-arrival delay
and queues. Overlap is observed handler wall-interval overlap, not CPU execution
or the configured limit. Successful throughput includes phase queue drain.

| Mode | Limit / arrivals per s | n | E2E p50 | E2E p95 | E2E p99 | Success/s | Observed overlap | Loop p95 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Cold profile | 1 / 10 | 128 | 7307.093 -> 7003.560 | 13502.967 -> 13521.525 | 14067.645 -> 14096.345 | 4.765 -> 4.757 | 1 -> 1 | 197.161 -> 7.692 |
| Cold profile | 5 / 10 | 128 | 7541.467 -> 6670.904 | 13222.382 -> 12379.013 | 13805.634 -> 12857.659 | 4.829 -> 5.008 | 5 -> 5 | 787.380 -> 19.137 |
| Cold profile | 10 / 10 | 128 | 8146.866 -> 6779.600 | 14364.332 -> 12470.884 | 14664.036 -> 12867.703 | 4.709 -> 5.005 | 10 -> 10 | 1581.574 -> 17.504 |
| Cold profile | 20 / 10 | 128 | 8080.109 -> 894.669 | 14087.246 -> 1553.187 | 14587.106 -> 1590.402 | 4.833 -> 9.013 | 20 -> 16 | 2128.863 -> 14.350 |
| Read-mostly | 5 / 20 | 128 | 599.649 -> 32.038 | 899.830 -> 235.530 | 948.246 -> 329.225 | 17.615 -> 19.644 | 5 -> 3 | 193.486 -> 37.725 |
| Read-mostly | 10 / 20 | 128 | 766.031 -> 35.536 | 1381.201 -> 240.785 | 1490.025 -> 327.483 | 16.603 -> 19.684 | 10 -> 4 | 232.927 -> 37.744 |
| Mixed | 1 / 20 | 128 | 1248.208 -> 1110.559 | 2077.306 -> 2040.232 | 2102.934 -> 2070.906 | 15.164 -> 15.189 | 1 -> 1 | 181.608 -> 57.047 |
| Mixed | 5 / 20 | 128 | 1187.639 -> 653.066 | 2019.818 -> 922.178 | 2073.443 -> 1061.889 | 15.342 -> 17.722 | 5 -> 5 | 221.356 -> 129.785 |
| Mixed | 10 / 20 | 128 | 1121.330 -> 377.083 | 2100.577 -> 588.862 | 2163.522 -> 720.484 | 15.288 -> 19.510 | 10 -> 9 | 219.460 -> 117.399 |
| Mixed | 20 / 20 | 128 | 1448.102 -> 121.274 | 2260.894 -> 405.332 | 2364.905 -> 579.822 | 15.228 -> 19.126 | 20 -> 6 | 267.023 -> 97.145 |
| Contention | 10 / 20 | 128 | 1340.748 -> 534.314 | 2119.358 -> 891.989 | 2212.799 -> 1000.891 | 15.189 -> 17.734 | 10 -> 9 | 236.804 -> 122.301 |
| Mixed | 20 / 35 | 256 | 4968.621 -> 3576.656 | 9030.659 -> 6245.980 | 9244.959 -> 6468.555 | 15.486 -> 18.747 | 19 -> 20 | 286.394 -> 237.901 |
| Mixed | 20 / 50 | 256 | 6371.764 -> 5151.300 | 11651.545 -> 8644.377 | 12144.822 -> 8973.306 | 14.964 -> 18.211 | 19 -> 19 | 317.721 -> 220.899 |

The 35/50 rates are offered arrivals over short 7.3/5.1-second windows, followed
by draining. Even the observed overlap of 20 does not prove 20 simultaneous CPU
operations. Successful throughput remains below 20/s at the highest arrivals;
sustained 50 successful requests/s is unachieved and unverified. Cold/limit1
p95 and p99 slightly regress. No additional run replaced these observations.

### Per-Command And SQL Evidence

Pooled distributions combine different arrival/queue conditions; use phase
rows above for capacity comparisons. All error rates are 0%; all requests in
each row succeed. DB calls include execute/commit/rollback by the recorder's
existing definition; message-writer SQL is reported separately in raw samples.

| Command | n/version | E2E p50 | E2E p95 | E2E p99 | Foreground DB calls |
| --- | ---: | ---: | ---: | ---: | ---: |
| 我的修仙信息 | 651 | 6263.070 -> 3254.840 | 13647.499 -> 12341.033 | 14481.428 -> 13521.525 | 32175 -> 32166 |
| 我的资产总览 | 139 | 1297.654 -> 415.477 | 9213.740 -> 6573.821 | 11577.502 -> 8507.270 | 3955 -> 3955 |
| 我的状态 | 135 | 1281.773 -> 424.816 | 8871.543 -> 6335.356 | 11218.996 -> 8147.864 | 5535 -> 5547 |
| 我的修为 | 135 | 1269.783 -> 432.658 | 8831.916 -> 6295.113 | 11178.894 -> 8287.446 | 2430 -> 2430 |
| 我的背包 | 130 | 1251.801 -> 419.435 | 9056.491 -> 6497.225 | 11187.796 -> 8400.093 | 2340 -> 2340 |
| 活动排行 | 130 | 1252.363 -> 464.093 | 9065.418 -> 6557.233 | 11457.134 -> 8492.378 | 2860 -> 2860 |
| 灵石修炼 | 600 | 1815.195 -> 846.595 | 9462.142 -> 7093.985 | 11820.005 -> 8644.377 | 33600 -> 33600 |

- Exact rank recomputations remain 1,302 (651 each field) on both versions;
  recorded process-cache hits/misses are 0/1,302 with 936 evictions each.
  Cold stages have no writes yet still miss on every different score. Training
  advances both field generations by 12, 64 or 128 per relevant phase; cache
  invalidations counted by the cache API remain zero because changing the SQL
  generation key is logical invalidation. This workload confirms the bounded
  cache provides no rank reuse here. It does not measure other caches' hit rates.
- Before cold rank SQL runs entirely in `MainThread`, about 92% of foreground
  SQL CPU. After it runs on the two named rank workers. Cold/limit5 and /10 rank
  SQL CPU rises from about 22 to 42 seconds/phase, consistent with phase user
  CPU; it is not merely overlapping wall time. Cold/limit20 instead consumes
  about 22 seconds of rank CPU. Shared memory/cache effects or host scheduling
  are possible causes, not established diagnoses. Thread CPU instrumentation
  times execute/fetch on the same thread and was independently reviewed. A
  single ordered experiment cannot establish significance or uniform speedup.
- Total rank SQL wall time increases 114259.933 -> 175697.019ms. Foreground
  calls remain 82895 -> 82898; with real message queue batching, total calls
  are 91692 -> 92146 and summed SQL wall is 157243.428 -> 225129.288ms.
  Offload primarily reduces event-loop stalls and other requests' queues;
  it does not reduce the indexed DISTINCT range-count work. Busy/locked is
  zero; maximum measured BEGIN execution rises 2.701 -> 7.030ms, not a direct
  lock-wait measurement. Response persistence drain stays separately recorded.

### Resource And Validation Closeout

- Same exclusive validation lock; two CPU affinity; 1GiB AS, 600s CPU and
  1200s wall; minimum 768MiB available RAM and 2GiB disk; RSS stop900MiB and
  total/per-file WAL stop256MiB. Monitoring continues throughout each phase.
  No cap was relaxed, no second long-lived million-user fixture was created.
- Whole process wall 301.047 -> 279.485s; CPU 290.229 -> 342.202s; peak RSS
  288.887 -> 299.910MiB. Traffic-phase CPU increases 209.646 -> 259.018s.
  Minimum phase available RAM 1680 -> 1555MiB; disk 7375593472 -> 7226351616
  bytes; maximum observed VmSize 842.551 -> 855.715MiB; threads45 -> 43.
  Maximum total WAL 19739016 -> 19701936 bytes, single WAL15355272 both;
  all WALs zero after restore. Process CPU includes harness/measurement cost.
- Sampled traffic-window write bytes 274808832 -> 304439296 differ from about
  13.6GB whole-process write bytes dominated by repeated snapshot restoration.
  Setup/restore/check times, process I/O and response drain are separate fields;
  none is attributed to gameplay transaction volume or durable data growth.
- Exact code-only candidate: 94 focused tests plus 66 subtests pass in 8.77s;
  compile, architecture (`ok=true`, `errors=[]`), generated inventory and
  whitespace pass. This includes repeated cancellation/drain, real worker
  connection close/reconnect, pool discard recovery, exact big-number training,
  ignored/rewritten receipts, rollback and original-message recovery/replay.
  The protected main-checkout JSON deletion remains untouched. Accepted old
  unittest shards and old serial/mixed matrices were not repeated; the new
  delivery still requires its exact-SHA CI to pass before CI closeout.

The next unresolved work is reducing cold rank scan CPU and the zero-hit
generation workload's cost, explaining the variable two-worker CPU penalty,
and testing broader gameplay/population and multi-process contention where
justified. The overall goal remains active; this bounded sample does not
establish repository-wide capacity or sustained 50-success/s service.

## Exact Rank Ranges And Training Connections (2026-11-26)

### Frozen Evidence And Runtime Changes

- Parent `6ded8d77cf087b30fe8bbc2967562b8ac6870a16` passed exact-SHA CI
  `45738323640` (quality and both unittest shards). This next batch compares
  `rank-range-before-20261126-r01.json` and
  `rank-range-after-20261126-r01.json` under the retained fixture's `data/`.
  Before runtime digest is
  `797ab1f591bc28af23fe832ba749f68dbb6bd8819d1ad62b15188a304136731a`;
  after runtime digest is
  `92a33d76a54a6a1f909841d1d91c12ff279e53f9fbafba704137a9f8db9bdbb8`.
  Baseline finished before runtime edits. Both runs passed once; no result
  was replaced to select favorable numbers.
- The same `rank-range-v3` harness, fixture identity, seven frozen phase
  specifications, seed digests and ordered request payloads match exactly.
  Descending and fixed-seed shuffled viewers avoid relying on ascending
  neighboring scores. The existing 1M users, 128 distinct-score participants,
  2,560 backpack rows, 128 BuffInfo rows and 8,192 activity users remain;
  point/pass/boss tables each hold 32,768 rows across four activity keys.
  These feature populations are not million-row feature tables. Each stage
  uses one shared runtime/database; restore follows complete drain and checks.
- The existing compact rank index already serves DISTINCT by an index range
  without a temporary sort or per-row Python conversion. Instead of another
  index, reuse a completed exact rank for the smallest cached score at or above
  the target: `rank(target) = rank(ceiling) + DISTINCT(target < score <= ceiling)`.
  Database identity, field generation, snapshot token and TTL must all match;
  target/generation/range count share one live SQLite transaction. No suitable
  ceiling or a NULL target falls back to the original scan. The scan range is
  a subset of the original range. The existing 32-entry/1MiB cache remains
  bounded; no public TOP slice, stale balance or full score map supplies rank.
- Stone training explicitly scopes one idle SQLite connection to its
  synchronous application request. Its four independent BEGIN/COMMIT pairs,
  ledger/receipt validation and rollback behavior remain. Options, file
  identity and owner thread constrain reuse; nested active UOWs use separate
  connections, lifecycle failures discard them and scope exit closes them.
  Other commands retain default UOW behavior. The training/open helper also
  checks one named sqlite_master row rather than materializing every table.

### Phase Results

Milliseconds before -> after; E2E includes planned-arrival and queue delays.
Every version executes 1,280 real matcher/DI/handler requests: 1,280 successes,
zero refusals, exceptions, timeouts, SQLite busy/locked, boundary violations or
unattributed SQL. All seven checks pass for exact training exp/stone/power,
receipt/ledger identity, no unsettled writes, unchanged inventory and DB quick
checks. The original fixture rank digest restores on both sides. Those load
checks do not independently recompute every returned rank; rank correctness
is covered by focused snapshot/generation, ties, NULL and large-number tests.

| Mode / viewer order | Limit / arrivals per s | n | E2E p50 | E2E p95 | E2E p99 | Success/s | Observed handler overlap | Loop p95 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Cold descending | 10 / 10 | 128 | 6782.236 -> 31.596 | 12493.037 -> 52.495 | 12891.162 -> 253.008 | 4.998 -> 10.054 | 10 -> 3 | 18.146 -> 9.558 |
| Cold shuffled | 1 / 10 | 128 | 7244.476 -> 33.532 | 13466.717 -> 223.434 | 13985.529 -> 339.375 | 4.778 -> 10.049 | 1 -> 1 | 7.354 -> 9.018 |
| Cold shuffled | 10 / 10 | 128 | 817.729 -> 32.907 | 1518.542 -> 230.413 | 1539.998 -> 267.955 | 9.009 -> 10.053 | 10 -> 2 | 13.038 -> 9.179 |
| Read-mostly shuffled | 10 / 20 | 256 | 39.632 -> 37.604 | 312.710 -> 297.710 | 366.504 -> 334.973 | 19.904 -> 19.840 | 5 -> 4 | 43.909 -> 26.992 |
| Mixed shuffled | 10 / 20 | 256 | 1008.470 -> 58.331 | 1786.462 -> 330.689 | 2014.234 -> 525.602 | 17.822 -> 19.974 | 10 -> 8 | 129.561 -> 60.423 |
| Same-actor contention | 10 / 20 | 128 | 921.190 -> 49.052 | 1131.922 -> 240.872 | 1261.365 -> 318.762 | 17.582 -> 19.933 | 10 -> 5 | 185.074 -> 52.775 |
| Mixed shuffled | 20 / 50 | 256 | 4284.338 -> 3007.166 | 8025.692 -> 5623.650 | 8319.939 -> 5936.260 | 19.117 -> 23.214 | 20 -> 19 | 244.657 -> 126.143 |

Overlap denotes intersecting real handler wall intervals, not simultaneous CPU
execution. The slightly above10/s cold throughput uses a finite first-send to
last-completion window; it is not sustained capacity above the offered rate.
The final phase offers 256 calls over about 5.1 seconds and includes
drain in throughput: 50 sustained successes/s is still unachieved/unverified.
Its queue p95 is 6723.058 -> 4671.544ms, dispatch delay p95 is
253.266 -> 115.981ms and producer scheduling delay p95 is 5.164 -> 3.865ms.
Thus measured queue growth is not hidden by a synchronous test-side producer.
Mixed/10 queue p95 falls 1262.047 -> 0.010ms; cold shuffled/1 still has
123.240ms queue p95. Read-mostly success throughput slightly falls and cold/1
loop p95 rises. The baseline's two cold limit10 phases also differ in CPU
(50.768 versus 27.834s); absent host/cycle diagnostics, that variation cannot
be assigned a definitive cause. One ordered experiment establishes neither
statistical significance nor a universal speedup. Earlier mixed10 regression
247.574 -> 322.347ms and the previous batch's CPU increase remain valid records.

### Commands, SQL And Cache

Pooled E2E distributions combine the seven different queues/load conditions;
phase rows are the appropriate capacity comparisons. All error rates are 0%.
Foreground DB calls use the existing execute/commit/rollback definition.

| Command | n/version | E2E p50 | E2E p95 | E2E p99 | Foreground DB calls |
| --- | ---: | ---: | ---: | ---: | ---: |
| 我的修仙信息 | 478 | 2232.469 -> 34.118 | 12420.674 -> 525.602 | 13684.369 -> 5056.105 | 23563 -> 23563 |
| 我的资产总览 | 94 | 262.419 -> 36.516 | 6468.274 -> 4566.457 | 8297.157 -> 5875.868 | 2660 -> 2660 |
| 我的状态 | 92 | 365.663 -> 45.880 | 6145.270 -> 4231.613 | 8034.677 -> 5602.978 | 3787 -> 3784 |
| 我的修为 | 91 | 400.118 -> 45.384 | 6197.713 -> 4210.567 | 8069.515 -> 5797.603 | 1638 -> 1638 |
| 我的背包 | 90 | 424.786 -> 32.099 | 6433.498 -> 4472.646 | 8339.671 -> 5924.914 | 1620 -> 1620 |
| 活动排行 | 90 | 418.697 -> 40.891 | 6393.573 -> 4565.042 | 8300.236 -> 5956.155 | 1980 -> 1980 |
| 灵石修炼 | 345 | 1198.065 -> 89.874 | 7233.721 -> 5108.855 | 8240.044 -> 5885.308 | 19320 -> 16215 |

- Exact-result hits remain zero, misses/recomputations remain 956. New partial
  reuse counts by phase are 252, 234, 233, 14, 0, 0, 0: 733 bounded range counts
  still execute SQL, not cached-result hits. Rank SQL wall sum falls
  118541.135 -> 23439.433ms; measured rank-thread CPU falls
  107555.894 -> 22425.224ms. The three cold phases' range SQL wall totals become
  409.775/2001.246/2346.103ms, from 48283.830/22090.172/26093.271ms.
- Both field generations still advance by 25/128/64/128 for read-mostly,
  mixed10, contention and mixed20. Frequent writes prevent any partial reuse
  in all three write-heavy phases. API invalidations remain zero because
  generation changes invalidate logically via keys; cache evictions are 742
  per version and maximum retained size is 33,104 bytes. No write invalidation
  reduction is claimed. Final-phase rank SQL slightly rises
  4571.309 -> 4660.851ms, despite better overall E2E.
- Training calls fall 56 -> 47 per request; total training SQL time falls
  12287.068 -> 6368.732ms. Its journal_mode initialization calls fall
  1380 -> 345, with recorded wall time 5283.420 -> 1213.416ms. Connection
  creation time falls 569.993 -> 255.897ms; these timings are not a claim that
  the pragma itself caused all initialization work. Recorded training COMMIT
  calls on xiuxian.db remain2070 on both versions, including surrounding reads.
  The named-table check's SQL time is 222.882 -> 43.103ms; the actual update
  is only 198.012 -> 185.243ms. Per-SQL top-20 aggregation is a lower bound
  when a statement is omitted; full recorder totals provide the call counts.
- All foreground calls fall 54568 -> 51460, and with real asynchronous message
  persistence, total calls fall 60734 -> 57751 and SQL wall sum
  149903.257 -> 46070.723ms. SQL wall sums can overlap across threads. Other
  command E2E benefits largely reflect less shared queuing, not direct changes
  to their SQL. For example cultivation SQL time increases 871.974 -> 933.764ms.
  Maximum BEGIN execution is 5.057 -> 2.940ms; this is an upper bound including
  SQL execution, not an independent lock-wait measurement. Busy/locked is zero.

### Resources, Validation And Remaining Work

- Exclusive `/tmp/nonebot-project-validation.lock`; two CPU affinity, 1GiB AS,
  600 CPU seconds and 1200 wall seconds, `MALLOC_ARENA_MAX=2`; preflight
  requires 768MiB available memory and 2GiB disk, with continuous resource
  checks and stop at 900MiB RSS or 256MiB single/total WAL. No cap relaxation
  or new long-lived 1M database copy. No real QQ, account or production DB.
- Process wall 189.102 -> 153.710s, CPU 231.648 -> 133.989s, traffic-phase CPU
  173.321 -> 77.003s. Peak RSS increases 306.566 -> 316.801MiB; sampled VmSize
  peak 835.902 -> 797.707MiB, sampled threads40 -> 34. Phase available memory
  minimum1507 -> 1653MiB, disk minimum7102935040 -> 6979964928 bytes.
  Maximum single WAL15355272 and total WAL19858496 bytes are unchanged;
  final restored WAL is zero. Host state and warm OS cache remain limitations.
- Setup/phase restore sums are32.413 -> 31.827s, invariant checks7.560 -> 7.356s
  and final restore11.997 -> 11.621s, outside command E2E. Sampled traffic write
  bytes197238784 -> 205914112 increase slightly. Whole-process counters end at
  roughly8.0GB written per version, dominated by snapshot operations; this is
  not application write volume, persistent growth or a second retained fixture.
- Harness preflight tests:17 passed. Complete runtime batch:75 tests plus
  83 subtests pass, including dense rank endpoints/NULL/ties/exact numbers,
  WAL snapshot with concurrent generation changes and rollback, bounded cache
  identity/TTL/dependencies, scoped connection lifecycle failures, thread/nested
  isolation, exact training and receipt replay/rejection. Code-only acceptance
  also passes compile, architecture, inventory and whitespace. Accepted old
  suites/matrices were not repeated; delivery requires the new exact-SHA CI.

The overall goal remains active. This is seven sampled commands, not exhaustive
674-matcher coverage or sustained service capacity. Remaining evidence points
to rank scans under frequent real writes (zero partial reuse), synchronous
training/read initialization and event-loop blocking at higher arrival rates,
plus broader gameplay/table populations and multi-process contention not yet
measured. Freeze any next workload before another baseline/code/after batch;
do not rerun unchanged phases to improve the displayed numbers.

## Live Asset And Protection Reads (2026-11-26)

- Parent `7af5c793b23685ee31c797df498ce75eddaf44fd` passed CI `45742957236`
  (quality and both unittest shards). This batch uses frozen `read-strata-v4`,
  `read-strata-before-20261126-r02.json` and
  `read-strata-after-20261126-r01.json` in the retained fixture's `data/`.
  Runtime SHA256 changes from
  `92a33d76a54a6a1f909841d1d91c12ff279e53f9fbafba704137a9f8db9bdbb8` to
  `519d5a4b95af9fc7d804b372fac254b085733c0e7220554d4fb4ec5e2f0aaafa`;
  exactly two runtime files differ. The main-checkout harness and fixture
  support stayed identical; `--source-repo` selects runtime only. Baseline
  completed before runtime edits, and after ran once without replacing results.
- Asset handler scopes the synchronous `check_user`/`overview` section to one
  idle game RO connection. Both UOW transactions still finish independently;
  ATTACH/DETACH and every live asset query remain. Scope closes before any
  send/finish await. Rejected users never read assets. No broader pool or cache.
- Protection `get_status` uses a single live RO SELECT without WAL/DDL on mature
  schema. Only a truly missing file or exact missing status table/protection
  column closes the reader and falls back to the existing initializer. Other
  open/query errors propagate. Setters and settlement `read_status` are unchanged.

### Frozen Population And Correctness

- Each version executes 1,024 real matcher/DI/handler requests on one shared
  instance and isolated database: 919 successes, 32 expected precision refusals
  and 73 cooldowns. Exception/timeout/busy/locked/boundary/unclassified counts
  are zero; system error rate is 0%. Cooldowns are 22 backpack, 30 cultivation,
  21 equipment; neither refusal category contributes to successful throughput.
  Every ordered request, outcome/reason, complete seed manifest, frozen phase
  spec and fixture identity matches, compared with exact Python JSON integers.
- The sole 1M user fixture is retained. Of 128 participants, 64 low-score actors
  perform real training (`exp+100`, `stone-1000`); 32 preserve original REAL
  background values; 32 readonly actors use exact overflow-safe BLOBs around
  int64/large-neighbor/tie boundaries. Cold dispatches all 128 profiles; later
  profiles use only the exact group. Each of 183 successful profile replies per
  version independently matches both expected dense ranks. Setup oracle uses
  indexed next-distinct seeks (141 exp/106 stone, cap512/column), not the product
  COUNT query, and does no request-time work. Mixed readonly ranks are provably
  above the training ceiling; mutable low ranks during writes are not checked.
- Original million-row data contains large REAL tie groups, not a million
  distinct numeric values. Historical lost precision cannot be reconstructed.
  The retained first baseline attempt `read-strata-before-20261126-r01.json`
  stopped at cold96 successes/32 unclassified precision refusals and restored
  the fixture. The fixture classifier was corrected before valid baseline/code
  edits; the product precision guard was not weakened. Those 32 refusals never
  reach rank computation and are not successful cold-rank coverage.
- Actual feature rows: back2,560, BuffInfo128, activity_user8,192;
  point/pass/boss32,768 each, bank/status/arena/impart128 each, sects4. No claim
  about million-row feature scans. Every phase passes exact training deltas,
  receipts/ledger identity, no duplicate rewards, inventory/readonly raw values,
  attributes/features unchanged, sole1M count and DB checks. Rank generations
  remain unchanged in cold/pure; all tasks drain before restore. Final original
  rank digest restoration passes on both versions.

### Phase And Command Results

Milliseconds before -> after; E2E includes planned-arrival, dispatch and queue
delay. Labels cold/pure mean no exp/stone writes; existing status timestamp and
message persistence still execute. Writes by phase are0/0/22/66/112.

| Phase | Limit / arrivals per s | n; success / precision / cooldown | E2E p50 | E2E p95 | E2E p99 | Success/s | Handler overlap | Loop p95 |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Cold | 10 / 20 | 128; 96 / 32 / 0 | 227.940 -> 38.463 | 820.627 -> 271.011 | 948.982 -> 298.941 | 14.997 -> 14.963 | 10 -> 5 | 36.317 -> 19.601 |
| Pure | 20 / 35 | 224; 198 / 0 / 26 | 44.421 -> 26.668 | 233.431 -> 179.846 | 356.508 -> 274.525 | 30.543 -> 30.606 | 9 -> 8 | 27.759 -> 20.180 |
| Read-mostly | 10 / 20 | 224; 207 / 0 / 17 | 27.572 -> 27.672 | 218.852 -> 222.585 | 267.733 -> 269.225 | 18.520 -> 18.522 | 5 -> 4 | 22.893 -> 23.232 |
| Mixed | 20 / 35 | 224; 210 / 0 / 14 | 1226.642 -> 881.310 | 1909.922 -> 1292.109 | 1968.190 -> 1390.491 | 25.599 -> 27.618 | 20 -> 19 | 154.148 -> 145.626 |
| Same-actor contention | 20 / 50 | 224; 208 / 0 / 16 | 2956.070 -> 2854.592 | 5062.585 -> 5292.066 | 5151.356 -> 5405.219 | 21.707 -> 21.208 | 20 -> 20 | 143.044 -> 173.896 |

Cold uses only profiles and does not execute either changed path: its faster
latency/rank SQL is variation, not evidence of a direct runtime improvement.
Read-mostly tails and final contention tails/throughput regress. Final queue
p95 rises4193.205 -> 4274.399ms and producer delay p95 rises6.884 -> 41.805ms;
dispatch p95 falls199.799 -> 151.056ms. Mixed queue p95 falls1197.767 ->
677.220ms. The producer's scheduled-vs-posted delay is measured independently;
arrival rate, concurrency cap, overlapping handler wall intervals and success
throughput are distinct. The final224 arrivals span about4.5s, then drain;
neither sustained50 successes/s nor50 concurrent processing is established.
One ordered comparison gives no statistical significance or universal speedup;
earlier accepted regressions remain valid.

Read-only followup found an additional driver limitation: `run_phase` first
calls `_check_matcher`, then `check_and_run_matcher`, whose installed NoneBot
implementation checks the matcher again with a separate dependency cache.
Both versions execute this same extra rule/permission check, and the recorded
handler interval includes the second check. This is offline-driver overhead;
neither its21/s throughput nor the interval outside SQL measures pure production
handler capacity. Correcting the driver requires a newly frozen before/after,
not replacing this run or comparing a single-check after to this baseline.

Pooled command E2E combines different load phases and refusal outcomes; use
phase rows for load comparison. DB calls include execute/commit/rollback;
SQL wall sums may overlap across threads. All values are per version.

| Command | n; success / precision / cooldown | E2E p50 | E2E p95 | E2E p99 | Success-only p95 | DB calls | SQL wall ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 我的修仙信息 | 215; 183 / 32 / 0 | 222.190 -> 69.918 | 2025.383 -> 1926.369 | 4683.396 -> 4545.207 | 2118.065 -> 2106.652 | 10451 -> 10451 | 17203.091 -> 15044.071 |
| 我的资产总览 | 175; 175 / 0 / 0 | 60.755 -> 37.215 | 3842.178 -> 3677.760 | 4956.981 -> 4939.543 | 3842.178 -> 3677.760 | 5155 -> 4805 | 2408.545 -> 1762.433 |
| 我的状态 | 175; 175 / 0 / 0 | 81.573 -> 44.122 | 3802.554 -> 3775.809 | 4988.539 -> 5005.586 | 3802.554 -> 3775.809 | 5966 -> 5094 | 2465.265 -> 2368.925 |
| 我的修为 | 44; 14 / 0 / 30 | 48.176 -> 36.985 | 3414.015 -> 3546.596 | 4828.463 -> 4817.507 | 791.646 -> 565.716 | 432 -> 432 | 248.174 -> 258.119 |
| 我的背包 | 44; 22 / 0 / 22 | 43.290 -> 29.159 | 3728.930 -> 3662.974 | 4968.787 -> 4917.576 | 1806.045 -> 1261.225 | 528 -> 528 | 287.617 -> 277.421 |
| 我的装备 | 43; 22 / 0 / 21 | 55.352 -> 53.082 | 3740.544 -> 4086.170 | 5131.442 -> 5289.379 | 1866.095 -> 1266.358 | 522 -> 522 | 276.489 -> 310.627 |
| 活动排行 | 44; 44 / 0 / 0 | 52.472 -> 43.139 | 3709.184 -> 3622.886 | 5087.988 -> 4975.347 | 3709.184 -> 3622.886 | 968 -> 968 | 687.639 -> 688.464 |
| 排行榜 | 42; 42 / 0 / 0 | 41.302 -> 28.744 | 3796.981 -> 4101.257 | 5138.852 -> 5405.219 | 3796.981 -> 4101.257 | 256 -> 256 | 153.064 -> 161.508 |
| 灵石排行榜 | 42; 42 / 0 / 0 | 27.517 -> 23.688 | 3994.195 -> 4060.767 | 5141.529 -> 5366.298 | 3994.195 -> 4060.767 | 256 -> 256 | 173.836 -> 154.875 |
| 灵石修炼 | 200; 200 / 0 / 0 | 1643.214 -> 1308.590 | 4914.153 -> 4975.514 | 5151.356 -> 5387.417 | 4914.153 -> 4975.514 | 9400 -> 9400 | 3847.359 -> 3664.963 |

- Assets open700 -> 525 connections (4 -> 3/request); the isolated handler test
  proves profile+overview share one game connection. Actual dispatch also has
  live blacklist/avatar reads. ATTACH still executes175 times but wall sum
  falls772.816 -> 191.430ms; profile SELECT remains175 calls,618.005 ->
  612.829ms. Removed initialization is350 DB calls; no personal SELECT removed.
- State protection removes175 WAL settings and schema initialization. Its
  necessary SELECT remains175 times: recorded cost moves from WAL190.219ms +
  SELECT9.040ms to the first RO SELECT203.782ms. Thus WAL timing alone is not
  saved wall time; state total SQL only falls3.9%. The overall state call delta
  is872:875 removed calls plus3 from one newly opened game BuffInfo reader
  (WAL/busy_timeout/synchronous), not a timestamp-write change. Status timestamp
  updates and BEGIN IMMEDIATE remain175 each. New connections875 -> 876 reflect
  this reader; the protection getter still opens one live reader/request.
  Per-SQL top20 sums are lower bounds when omitted, not full query counts.
- Foreground DB calls33934 -> 32712; with asynchronous message persistence,
  total38754 -> 37559 and SQL30002.449 -> 26908.102ms. Cold-only variation
  accounts for much of rank SQL difference; no rank runtime code changed here.
  Exact rank cache hits/misses by phase remain9/183,35/49,22/52,16/42,12/30.
  Partial range reuse171/38/6/2/0 -> 171/38/6/0/0 is SQL, not a result hit.
  Field generations advance0/0/22/66/112 on both sides. Contention still has
  no partial reuse; no full score map, TOP-slice rank or stale eligibility.
  Cache stays32 entries, maximum59,614 estimated bytes within1MiB. Cache counters
  are phase-level; per-command cache attribution is not measured.

### Resources And Acceptance

- Same exclusive lock, two CPU affinity,1GiB AS,600 CPU/1200 wall seconds and
  `MALLOC_ARENA_MAX=2`; available-memory768MiB/disk2GiB preflight and continuous
  RSS900MiB/WAL256MiB stop thresholds remain. Process wall107.882 -> 106.765s,
  CPU102.095 -> 99.157s; traffic CPU49.046 -> 46.726s. Peak RSS rises346.867 ->
  367.172MiB; sampled VmSize924.863 -> 936.453MiB. Available memory minimum
  1575 -> 1740MiB; disk minimum6834569216 -> 6684901376 bytes. Maximum single
  WAL15,355,272 bytes is unchanged; sampled total20,670,168 -> 20,666,048 bytes,
  final restored WAL zero. No threshold exceeded or extra retained million DB.
- Setup/phase-restore sum26.214 -> 26.564s, checks5.848 -> 5.449s and final
  restore12.547 -> 12.446s are outside request E2E. Sampled traffic write bytes
  rise155877376 -> 161300480; final process write bytes are about6.2GB/version,
  dominated by snapshots/restoration. Test/setup IO is not application traffic.
  BEGIN timing remains an upper bound including SQL, not pure transaction wait.
- Code-only acceptance:85 tests plus31 subtests pass, covering live external
  asset/protection changes, missing legacy schema, real error propagation,
  close-before-fallback/await, connection isolation/rollback and bounded SQL
  diagnostics/oracle/fixture behavior. Compile, architecture, inventory and
  whitespace pass. Ordinary tests have no benchmark address-space cap. Old
  green shards, old serial matrices and unchanged mixed stages were not rerun;
  the scoped delivery must pass its exact-SHA CI.

The overall performance goal remains active. These ten commands and bounded
strata do not exhaust674 matchers/41 background entries or establish sustained
capacity. The50-arrival contention backlog, first-schema/read initialization,
write-driven rank recomputation and broader real gameplay remain unresolved;
freeze the next evidence-based workload before another baseline/code/after.

## V5: Live Relationship Display Batch (2026-11-26)

### Scope And Comparable Data

- Baseline runtime is `a6ce2a7e` (CI45751014023 successful). Reports are
  `/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05/data/`:
  `relation-before-20261126-r03.json` and `relation-after-20261126-r01.json`,
  with their `.phases.jsonl` request records. This is one valid ordered pair;
  no after retries. Earlier before-r01 stopped before requests on an incorrect
  isolated cooldown-file assumption; before-r02 misclassified CR transport
  line endings after160 requests. Both failures remain retained, excluded from
  performance evidence; all160 saved replies were reclassified offline before
  the valid baseline. Product behavior was not changed to satisfy classification.
- Correct the driver's duplicate matcher check before freezing V5: one real
  `_check_matcher` then `_run_matcher`, sharing the request's dependency cache
  and exit stack; the timeout covers both. Rule/handler/resource lifecycle,
  rejection and timeout regressions pass. Both reports have identical driver
  hashes and dispatch contracts. Do not compare these latencies directly with
  V4's double-check driver or revise V4's retained results.
- The same sole1M fixture, ordered request objects, phase specs, outcome/reason,
  rank generations and business seed manifests match using Python integers.
  Full seed hashes differ only because startup compatibility-hit telemetry
  JSON differs; `xiuxian/compatibility_hits.json` is a best-effort diagnostic
  counter, not gameplay state. This exception is explicit, not full-byte seed
  equality. Every JSON file, including that telemetry file, stays unchanged
  during each measured phase. Original rank digest restoration passes twice.
- Per version608 successful real matcher/DI/handler requests:128 profiles,
  160 mentor views including all four aliases,160 partner views,160 stone
  training writes. Transport alone is mocked. All requests in a phase share
  one instance/database; tasks and asynchronous persistence drain before
  phase restore. Every phase has zero refusals, exceptions, timeouts and
  busy/locked errors; every invariant check passes.
- Retain128 participants:64 low-score training users,32 original REAL users,
  32 exact large-BLOB users. The REAL group is preserved but not a relationship
  display actor/target; full profiles still enforce the precision guard.
  There are128 mentor rows/64 links,128 partner rows/64 pairs, and4096 current
  pending applications (128 per master). Apprentice fanout includes1/3/5/7;
  historical over-limit display remains intact. Pending rows have fixed future
  expiry; expired cleanup performance is not exercised. Existing feature rows
  remain back2560/BuffInfo128/activity_user8192, point/pass/boss32768 each,
  bank/status/arena/impart128 each, sects4. No million-row feature claim.
- Each successful profile independently checks both exact ranks. Training
  changes real exp+100/stone-1000/power and verifies exact receipts/ledgers,
  inventories, no duplicates/negative balances/lost updates, relationships,
  cooldown JSON and feature/attribute projections. High readonly ranks are
  above the low training ceiling; mutable low-rank behavior is not proved.

### Runtime Changes And Results

Profile display batches live mentor backlinks and relation names; mentor
display reuses its own relation/cooldowns and batches backlinks/name+realm;
partner display reuses its first current full profile. Name-only projections
do not parse unrelated currency. A missing partner profile gets an explicit
reply; strict database failures retain their reason and recover after the
fault clears. Batches deduplicate SQL inputs, retain output order/duplicates,
first-row and empty-name semantics, use400-ID chunks and no new cache/DDL.
Action eligibility, balances, rank algorithms/generations, pending expiry,
transmission limits and writes retain their current behavior.

Milliseconds before -> after. E2E includes scheduled-arrival and all queueing;
the phases are short arrival bursts plus drain, not sustained capacity.

| Phase | n; cap / arrivals per s; writes | E2E p50 | E2E p95 | E2E p99 | Success/s | Handler wall overlap | Loop p95 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Pure | 160;10/20;0 | 29.806 -> 26.974 | 124.369 -> 151.840 | 273.995 -> 331.316 | 20.051 -> 20.028 | 4 -> 4 | 31.645 -> 16.003 |
| Mixed | 256;20/35;64 | 2340.509 -> 1987.819 | 4030.616 -> 3454.566 | 4163.170 -> 3720.234 | 22.621 -> 23.409 | 16 -> 16 | 93.003 -> 96.285 |
| Same-user contention | 192;20/50;96 | 2727.841 -> 3994.002 | 5298.993 -> 6735.366 | 5404.869 -> 6835.097 | 21.022 -> 18.190 | 15 -> 15 | 115.252 -> 156.240 |

All command errors are0%. SQL calls include execute/commit/rollback; summed
SQL wall can overlap across threads. Each command's phase result is retained:

| Phase / command | n | E2E p50 | E2E p95 | E2E p99 | DB calls | SQL wall ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Pure / profile | 32 | 66.881 -> 43.497 | 169.489 -> 331.316 | 299.017 -> 544.563 | 2277 -> 1832 | 1602.513 -> 1529.161 |
| Pure / mentor | 64 | 28.034 -> 26.925 | 94.641 -> 126.944 | 273.995 -> 180.856 | 2468 -> 2116 | 1084.137 -> 1031.960 |
| Pure / partner | 64 | 23.308 -> 22.264 | 66.641 -> 133.034 | 223.551 -> 265.367 | 1541 -> 1474 | 813.593 -> 823.682 |
| Mixed / profile | 64 | 2488.130 -> 2063.064 | 4074.999 -> 3596.069 | 4249.933 -> 3834.665 | 4561 -> 3665 | 11303.493 -> 11240.380 |
| Mixed / mentor | 64 | 2236.459 -> 1984.535 | 4008.230 -> 3454.566 | 4163.170 -> 3578.178 | 2468 -> 2116 | 1145.081 -> 1087.525 |
| Mixed / partner | 64 | 2147.615 -> 1860.163 | 3928.555 -> 3322.241 | 4077.479 -> 3467.642 | 1541 -> 1474 | 822.426 -> 910.036 |
| Mixed / training | 64 | 2356.090 -> 1976.842 | 4030.616 -> 3443.257 | 4145.691 -> 3633.604 | 3008 -> 3008 | 1271.391 -> 1491.597 |
| Contention / profile | 32 | 2691.742 -> 3965.637 | 5404.869 -> 6753.338 | 5412.922 -> 6835.097 | 2291 -> 1841 | 5353.470 -> 7586.569 |
| Contention / mentor | 32 | 2716.661 -> 3998.186 | 5279.246 -> 6794.443 | 5372.116 -> 6852.216 | 1188 -> 1060 | 507.664 -> 626.246 |
| Contention / partner | 32 | 2591.280 -> 3846.572 | 5235.600 -> 6672.273 | 5332.549 -> 6754.621 | 768 -> 738 | 399.552 -> 479.320 |
| Contention / training | 96 | 2727.841 -> 3991.958 | 5298.993 -> 6735.366 | 5392.233 -> 6832.354 | 4512 -> 4512 | 1825.792 -> 2271.836 |

- Foreground calls26623 -> 23836; including message persistence29537 ->
  26730. Current full-profile SELECTs visible in per-request top20 fall
  profile512 -> 128, mentor392 -> 160, partner480 -> 320; these diagnostics
  are lower bounds when omitted. The partner reduction is one related full
  read/request; the actor's registration read remains. Mentor's new batch
  itself costs645.281ms/160 calls, including first-schema work; its old related
  full profiles are removed. No claim that every removed query saves its
  recorded wall time. Total SQL rises27413.129 -> 30580.492ms.
- Exact-rank statements remain49/128/64 by phase. Their SQL wall rises
  643.549/9396.859/4382.505 -> 870.332/9895.394/6780.190ms. Cache result
  hits/misses remain15/49,0/128,0/64; partial range hits40/9/0 -> 40/4/0.
  Generations advance0/64/96 in both fields, matching successful training.
  Cache remains32 entries/about33KiB within1MiB; no TOP-slice substitution.
  Per-command cache attribution and isolated generation-update CPU are not
  measured. Write invalidation/cold exact scans remain dominant concerns.
- Mixed queue p95 improves3247.939 -> 2521.619ms; contention worsens4482.623
  -> 5943.190ms. Contention producer p95 is1.835 -> 2.031ms while loop/dispatch
  p95 rises115.252/93.669 -> 156.240/121.898ms: the seconds of delay cannot be
  attributed solely to the load producer. BEGIN p95 stays below0.046ms in
  every phase (including SQL, an upper bound on lock wait), with no busy errors.
  This pair proves fewer repeated reads, not universal lower latency or a
  causal explanation for rank timing variation. Pure tails and contention
  latency/throughput regress; do not hide them or rerun to choose better data.

### Resources And Remaining Work

- Exclusive validation lock;2 CPU affinity,1GiB AS,600 CPU/1200 wall seconds,
  `MALLOC_ARENA_MAX=2`; RAM768MiB/disk2GiB preflight and continuous
  RSS900MiB/single+total WAL256MiB stop thresholds. Wall84.794 -> 84.647s,
  CPU82.977 -> 86.837s, traffic CPU38.761 -> 41.932s, peak RSS399.809 ->
  404.215MiB, sampled VmSize749.551 -> 767.680MiB. Minimum available RAM
  1643 -> 1726MiB; disk6450626560 -> 6300491776 bytes. Max single WAL stays
  15355272 bytes; total21279928 -> 21296408 bytes, final restored WAL zero.
- Phase setup/restore16.425 -> 17.970s, invariants7.487 -> 3.400s, final
  restore12.151 -> 12.585s are outside request E2E. Sampled traffic write IO
  104587264 -> 105426944 bytes; full process writes are about4.28GB/version,
  mostly fixture/snapshot restoration. No extra retained million-row fixture.
- Concentrated affected acceptance:43 tests pass; two newly added fixture
  assumptions were corrected locally and only those failures rerun. Reuse
  previously passed26 harness/fixture tests with164 subtests; frozen drivers
  and fixture were unchanged afterward. Compile/architecture/inventory and
  whitespace pass in a clean code-only worktree. No unchanged full-suite shard,
  old matrix or old mixed phase rerun; scoped delivery requires its exact-SHA CI.

The repository-wide goal remains active. Four real paths are a bounded sample,
not exhaustive coverage of674 matchers/41 background entries. Actual handler
wall overlaps4/16/15 do not prove20 concurrent CPU work or sustained50 successful
requests/s. Next select cold exact-rank/write-interference or first-schema
hotspots from these saved SQL/queue/loop records, freeze a new bounded workload,
then perform one baseline -> runtime batch -> same-load comparison. Do not
reinterpret old regressions as resolved by this delivery.
