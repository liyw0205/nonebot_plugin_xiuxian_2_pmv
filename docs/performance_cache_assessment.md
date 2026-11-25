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
