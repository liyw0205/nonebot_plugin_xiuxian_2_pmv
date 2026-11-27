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

## V6: Bounded Cold Exact-Rank Seeks (2026-11-26)

### Frozen Evidence And Runtime Change

- Baseline runtime is `a3cc28bfa3c9737751a91e504c9362b035e3093d`, whose exact
  CI45759556810 passed quality and both unittest jobs. One valid first-attempt
  pair is retained under the sole fixture's `data/` directory:
  `rank-seek-before-20261126-r01.json` and
  `rank-seek-after-20261126-r01.json`, plus full `.phases.jsonl` records.
  No V5 stage, old serial matrix or unchanged full-suite shard was rerun.
- Python JSON comparison preserving large integers confirms identical driver
  hashes, fixture identity, complete seed manifests, frozen phase specs,
  ordered requests, generations and limits. Only the runtime handle file hash
  differs. Both original rank digests restore exactly. Unlike V5, this pair
  has no startup telemetry seed exception.
- Keep1M existing users and128 actors:64 low training writers,32 preserved
  original REAL users,32 exact BLOB readers. Temporarily reassign8192 existing
  nonactors to distinct score bands; do not add users or retain another DB.
  Exp/stone have8341/8307 distinct groups. A setup-only streaming grouped-count
  oracle computes dense ranks independently of product COUNT/seek code;
  no request-time oracle SQL runs.

| Reader cohort | Users | Exp dense rank | Stone dense rank | Higher rows in either field |
| --- | ---: | ---: | ---: | ---: |
| Near | 8 | 58-61 | 24-27 | 991712-991718 |
| Mid | 8 | 4150-4164 | 4116-4130 | 995808-995822 |
| Deep | 16 | 8246-8276 | 8212-8242 | 999904-999934 |

- Feature populations remain back2560/BuffInfo128/activity_user8192,
  activity point/pass/boss32768 each, bank/status/arena/impart128 each, sects4.
  These are six real command paths, not exhaustive coverage of674 matchers
  or41 background entries. Activity results do not represent empty tables,
  but no million-row feature-table performance claim follows.
- In `_read_numeric_rank_for_value`, reuse the target/generation transaction
  and compact-key expression index. One recursive SQL statement performs at
  most128 descending next-distinct seeks; if exhausted, COUNT only the strict
  remaining interval. Preserve ceiling offsets and exact arbitrary integers.
  Check both scalar lookups' range SEARCH plan and absence of temporary sort;
  missing/wrong indexes fall back once, other read/lock failures propagate.
  NULL keeps the previous count. No new cache, index, generation journal,
  worker capacity, cooldown or gameplay change; TOP slices never supply ranks.

### Results And Attribution

Each version executes416 real matcher/DI/handler requests against one shared
instance/isolated DB; only message transport is mocked. Before has416 successes;
after has400 successes and16 normal backpack cooldown refusals, zero system
errors/timeouts/busy/locked in either. All144 profiles succeed and independently
check both ranks; all128 training writes succeed with exp+100/stone-1000.
Every phase drains before invariants and restore. Exact balances/exp/power,
receipts/ledgers/inventory, no duplicate or lost writes/negative resources,
readonly storage types, all8192 band rows, feature invariants, quick_check and
original restoration pass. Training stays below every reader score: changing
low ranks across score boundaries is still unproved by this workload.

Milliseconds before -> after; E2E includes scheduled arrival and all queues.

| Phase | n; cap / arrivals per s | E2E p50 | E2E p95 | E2E p99 | Success/s | Handler wall overlap |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Pure | 96;10/20 | 36.261 -> 34.376 | 433.561 -> 61.124 | 614.948 -> 100.228 | 19.550 -> 20.093 | 6 -> 2 |
| Mixed | 192;20/35 | 4680.448 -> 1539.708 | 9649.282 -> 2487.603 | 10022.103 -> 2528.879 | 12.758 -> 22.277 | 19 -> 14 |
| Same-user contention | 128;20/50 | 2357.173 -> 1767.968 | 4079.139 -> 2991.065 | 4204.711 -> 3088.546 | 19.308 -> 23.144 | 15 -> 15 |

Mixed has192 ->176 successes, with16/192=8.333% normal cooldown outcomes.
Backpack itself has32 ->16 successes (50% cooldown after); faster execution
leaves repeat requests within its existing cooldown. Its reduced work is a
confounder: do not attribute the whole mixed E2E/CPU change to ranking. Pure
and contention outcomes, and profile/training success counts, match exactly.
All commands have0% system errors. Short arrival bursts plus drain do not
prove sustained50 successful/s. Peak admitted/inflight is10/20/20 ->2/20/20;
wall overlap is not parallel CPU/SQLite execution.

| Phase / command | n | E2E p50 | E2E p95 | E2E p99 | DB calls | SQL wall ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Pure / activity | 24 | 29.254 -> 25.225 | 240.477 -> 36.931 | 303.597 -> 60.590 | 528 -> 528 | 446.504 -> 352.499 |
| Pure / assets | 24 | 25.579 -> 20.832 | 263.358 -> 29.582 | 319.443 -> 80.522 | 655 -> 655 | 310.447 -> 247.123 |
| Pure / profile | 48 | 40.349 -> 36.965 | 533.029 -> 65.479 | 614.948 -> 100.228 | 2609 -> 2716 | 2988.866 -> 974.120 |
| Mixed / activity | 32 | 4047.410 -> 1539.708 | 8484.707 -> 2471.374 | 8754.902 -> 2557.530 | 704 -> 704 | 1057.113 -> 558.198 |
| Mixed / backpack | 32 | 3900.971 -> 1478.750 | 8433.190 -> 2425.543 | 8673.526 -> 2471.979 | 579 -> 384 | 550.273 -> 236.315 |
| Mixed / profile | 64 | 6001.000 -> 1494.519 | 9946.783 -> 2493.239 | 10031.249 -> 2500.890 | 3475 -> 3702 | 26709.393 -> 2134.413 |
| Mixed / training | 64 | 4018.738 -> 1541.561 | 8427.988 -> 2487.603 | 8784.947 -> 2528.879 | 3008 -> 3008 | 2192.169 -> 1343.998 |
| Contention / state | 32 | 2206.827 -> 1667.561 | 4019.211 -> 2970.721 | 4084.249 -> 2994.491 | 932 -> 928 | 486.499 -> 486.927 |
| Contention / profile | 32 | 2361.927 -> 1775.755 | 4204.711 -> 3010.967 | 4239.596 -> 3088.546 | 1745 -> 1859 | 7634.650 -> 1092.948 |
| Contention / training | 64 | 2334.950 -> 1767.968 | 4064.943 -> 2991.065 | 4144.073 -> 3108.754 | 3008 -> 3008 | 1560.179 -> 1203.846 |

Profile cohort latency retains the deep distinct tail, rather than reporting
only near-top users. Small cohort p99 often equals the maximum.

| Phase / cohort | n | E2E p50 | E2E p95 | E2E p99 | Rank-worker SQL wall ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Pure / near | 10 | 127.715 -> 38.090 | 476.249 -> 60.850 | 476.249 -> 60.850 | 775.439 -> 3.605 |
| Pure / mid | 12 | 38.333 -> 37.939 | 614.948 -> 100.228 | 614.948 -> 100.228 | 759.607 -> 30.303 |
| Pure / deep | 26 | 37.734 -> 36.228 | 351.092 -> 65.479 | 551.885 -> 73.361 | 456.756 -> 22.200 |
| Mixed / near | 16 | 5736.790 -> 1279.771 | 10031.249 -> 2496.304 | 10031.249 -> 2496.304 | 6211.136 -> 29.304 |
| Mixed / mid | 16 | 5999.672 -> 1427.422 | 10022.103 -> 2499.423 | 10022.103 -> 2499.423 | 5008.251 -> 205.417 |
| Mixed / deep | 32 | 6001.000 -> 1494.519 | 9824.181 -> 2493.239 | 9946.783 -> 2500.890 | 13294.406 -> 570.440 |
| Contention / near | 8 | 2625.435 -> 1941.290 | 4239.596 -> 2896.517 | 4239.596 -> 2896.517 | 1582.685 -> 25.170 |
| Contention / mid | 8 | 1176.669 -> 799.618 | 4149.260 -> 3010.967 | 4149.260 -> 3010.967 | 1927.990 -> 94.403 |
| Contention / deep | 16 | 2027.683 -> 1478.488 | 4204.711 -> 3088.546 | 4204.711 -> 3088.546 | 3483.015 -> 337.292 |

Diagnostics count execute plus fetch and all rank-worker SQL, independent of
top20/timeline64/connection12 truncation. API calls include transaction calls;
the internal128 seeks are one recursive CTE statement, not one index lookup.

| Phase | All rank-worker calls | Rank SQL wall ms | Rank SQL CPU ms | COUNT before -> after | After seek CTE / EQP |
| --- | ---: | ---: | ---: | ---: | ---: |
| Pure | 236 -> 343 | 1991.802 -> 56.108 | 1759.704 -> 55.859 | 86 -> 24 | 86 / 86 |
| Mixed | 318 -> 545 | 24513.794 -> 805.160 | 17746.501 -> 607.937 | 126 -> 94 | 128 / 128 |
| Contention | 160 -> 270 | 6993.690 -> 456.864 | 5756.748 -> 346.997 | 64 -> 46 | 64 / 64 |

- Total foreground calls17243 ->17492, including message persistence19270
  ->19429; total SQL wall44807.438 ->9555.616ms. Fewer COUNTs do not mean
  fewer SQL API calls. State SQL wall slightly regresses486.499 ->486.927ms.
- Cache hits/misses by phase are10/86 ->10/86,2/126 ->0/128,0/64 ->0/64;
  partial-range hits74/12/0 ->76/22/6. Cache attribution is phase-wide, not
  per-command. Both exp/stone generations advance0/64/64 in each version;
  writes still invalidate generation-key reuse despite `invalidations=0`.
  The existing cache retains32 entries/about33KiB within1MiB; no new cache.
- Queue p95 is0.010/7567.021/3196.823 ->0.008/1748.600/2240.653ms;
  loop p95 is28.516/130.476/115.909 ->14.416/107.303/98.068ms;
  producer p95 is0.305/10.513/1.746 ->0.199/2.051/1.606ms.
  BEGIN p95 is0.050/0.030/0.023 ->0.043/0.055/0.045ms, max
  0.330/12.809/3.103 ->0.510/5.232/2.252ms. BEGIN includes SQL execution,
  an upper bound rather than a pure lock-wait timer. No busy/locked occurred.

### Resources, Verification And Open Scope

- Exclusive shared validation lock,2 CPU affinity,1GiB AS,600 CPU/1200 wall
  seconds, `MALLOC_ARENA_MAX=2`; RAM768MiB/disk2GiB preflight and continuous
  RSS900MiB/single+total WAL256MiB abort guards all pass. Whole process wall
  91.858 ->80.140s and CPU97.460 ->70.219s; traffic CPU46.072 ->18.727s.
  Peak RSS increases409.539 ->428.547MiB. Sampled traffic VmSize peaks
  807.484 ->783.402MiB; final VmSize812.570 ->785.887MiB. Minimum monitored
  traffic RAM1750 ->1747MiB; disk6207401984 ->6076211200 bytes.
- Maximum single WAL stays15355272 bytes; sampled total WAL20661928 ->
  20649568 bytes; final restored WAL zero. Sampled traffic IO reads1130496
  ->1130496 bytes, writes64618496 ->58695680 bytes. Whole-process writes
  4268871680 ->4265234432 bytes mainly include setup/snapshot/restore IO;
  do not attribute these roughly4.27GB totals to application write amplification.
  Phase setup/restore22.237 ->22.372s, invariants4.300 ->3.902s, final
  restore12.224 ->12.498s are outside request E2E. No extra retained1M DB.
- Harness/fixture25 small tests pass using the existing shared interpreter
  where the workspace environment lacked OneBot; no package/environment edit.
  Concentrated affected acceptance68 unittest passes in1.907s. After fixing
  static inventory's CTE recognition through complete SQL templates/aliases,
  28 affected rank checks pass; architecture `ok=true/errors=[]`, inventory
  `--check`, compile and whitespace pass. No classifier/taxonomy change.
  Coverage preserves0/1/127/128/129 boundaries, ties/negative/NULL/TEXT/BLOB,
  missing/wrong indexes, read/lock failure recovery, snapshot/write/rollback
  and bounded-cache contracts. Delivery still requires its exact-SHA CI.

The expanded goal remains active. This pair supports lower exact-rank CPU and
latency for these distinct/tie distributions, not universal gains. Keep V5's
pure/contention regressions and the earlier mixed regression recorded above;
different V6 traffic cannot erase them. Dynamic low ranks crossed by actual
exp/stone writes, sustained50 successful/s, broader gameplay/background and
multi-process behavior remain unverified. Select the next batch from saved
remaining queue/loop/connection costs, freeze its data/load before one baseline,
and retain successful and refused work separately without retrying for numbers.

## V7: Live Relation Connection Reuse And Changing Exact Ranks (2026-11-26)

### Frozen Scope And Evidence

- V6 `c2014e0b` CI45766419511 completed/success. The expanded repository
  performance goal remains active. V7 changes only `my_partner_` and
  `my_mentor_`: reuse one idle game connection across current self/related
  profile reads, with separate BEGIN/COMMIT transactions. No await occurs in
  the scope; statistics, cooldowns, transmission allowance and pending-invite
  handling stay outside. Router blacklist reads remain separate. No new
  pool, cache, index, generation rule or gameplay change.
- Reports under the same sole1M fixture's `data/` directory:
  `dynamic-relation-before-20261126-r01.json` and
  `dynamic-relation-after-20261126-r01.json`, plus `.phases.jsonl` samples.
  Both first traffic attempts pass. Driver hashes, phase specs, ordered
  requests, fixture identity, limits and business seeds match exactly using
  Python integers. Only runtime `xiuxian/xiuxian_buff/partner.py` differs.
  Seed hashes differ solely through startup `compatibility_hits.json`
  telemetry and its enclosing hash; this is not full-byte seed equality.
  All JSON stays unchanged during each measured phase; original rank digest
  restoration passes both times. No extra retained million-row DB.
- 512 requests/version:128 profiles,112 mentor views (all four aliases),
  112 partner views and160 actual stone-training writes. All512 succeed;
  business refusal, cooldown, exception, timeout, boundary, unclassified and
  SQLite busy/locked counts are all zero, error rate0 on every path. One
  instance/shared isolated DB per phase, real matcher/rules/DI/handler;
  transport alone is mocked. Workers/persistence drain and invariants run
  before stage restore. This is four sampled paths, not674 matcher coverage.
- Retain128 seeded participants, of which62 distinct actors issue requests
  (62 pure,51 mixed,51 contention). Preserve32 original REAL and32 exact
  BLOB users. One low-score writer actually gains100 exp/spends1000 stone
  per training, crossing16 low-reader score pairs; profiles also include the
  writer and8 high exact readers. No multiwriter dynamic-rank claim.
- Nonempty feature data:2560 inventory rows,128 each BuffInfo/bank/arena/
  status/impart rows,4 sects,8192 activity users and32768 each activity point,
  pass and boss-damage rows. Relationships have128 mentor/128 partner rows,
  64 links/pairs each and4096 pending applications,128/master; apprentice
  fanout1/3/5/7. V7 executes relationship/profile/training paths, not activity
  commands or expired-invite cleanup, so their population is not new runtime
  coverage or a claim about million-row feature tables.
- Independent setup ranking algorithm streams GROUP BY key/multiplicity over
  all1M rows (141 exp/107 stone groups), then computes dense ordinals for25
  users at write offsets0..96. It reuses the production numeric parser/key
  UDF; independence is in the rank/multiset algorithm. Passive capture of
  the actual rank-state fetch supplies actor, both generation values and
  both target keys, without extra SQL or an audit trigger. Offset equality,
  expected keys and both reply ranks are checked, never inferred from offered
  order or completed-success count.256 reply-rank checks/version pass;
  mixed profiles observe42 ->46 distinct offsets, contention28 ->30, with
  maxima62/92. At least one rank differs from setup in34 ->32 mixed and
  12 ->13 contention profiles. Full generation delta equals fresh training
  receipts0/64/96; exact writer exp/stone/power and127 nonwriter participants'
  raw scores/types/keys/power, inventory/features/relations/JSON all pass.
  The127 check is not a per-user hash of the other999999 rows.
- All128 captured display-key observations match their respective rank
  snapshot keys in this pair. Capture is diagnostic, not mandatory for
  classification; profile attributes can precede the rank await. This does
  not establish an atomic snapshot for the whole reply.

### Same-Load Results

Every latency below includes producer delay, event-loop dispatch and admission
queueing. Values are before -> after in milliseconds; small samples and a
single ordered pair do not establish variance or universal improvements.

| Phase/path | n/version | E2E p50 | E2E p95 | E2E p99 | Foreground SQL calls | SQL wall ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Pure / profile |32|37.520 ->37.522|133.111 ->63.803|171.079 ->134.291|1829 ->1829|665.708 ->661.716|
| Pure / mentor |48|27.016 ->22.956|43.844 ->37.021|167.341 ->82.791|1588 ->1492|733.346 ->571.057|
| Pure / partner |48|22.752 ->18.588|65.561 ->31.224|216.375 ->63.228|1104 ->1008|589.579 ->419.902|
| Mixed / profile |64|1906.944 ->1547.545|3168.673 ->2624.469|3277.499 ->2695.636|3732 ->3735|1530.351 ->1521.566|
| Mixed / mentor |32|2029.262 ->1401.143|3166.096 ->2625.456|3166.995 ->2631.666|1058 ->994|537.631 ->406.504|
| Mixed / partner |32|1933.338 ->1572.073|3134.631 ->2582.889|3173.021 ->2590.486|736 ->672|438.942 ->294.582|
| Mixed / training |64|2008.973 ->1649.246|3197.373 ->2639.543|3222.783 ->2681.163|3008 ->3008|1258.143 ->1272.563|
| Contention / profile |32|2957.520 ->2876.446|5454.180 ->4917.161|5473.841 ->4936.487|1875 ->1875|770.757 ->753.559|
| Contention / mentor |32|3119.305 ->2877.710|5467.591 ->4955.958|5492.193 ->4966.455|1058 ->994|567.115 ->388.505|
| Contention / partner |32|3078.666 ->2797.348|5447.167 ->4898.015|5495.252 ->4989.882|736 ->672|428.627 ->434.184|
| Contention / training |96|3166.738 ->2905.696|5467.908 ->4948.380|5516.041 ->5010.905|4512 ->4512|1879.318 ->1789.292|

| Phase | Admission cap | Offered arrivals/s | Peak admitted | Peak handler intervals | Successful completions/s | E2E p95 | Queue p95 | Loop lag p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Pure |10|20|3 ->3|3 ->2|19.792 ->20.097|67.237 ->50.028|0.010 ->0.009|14.948 ->11.912|
| Mixed |20|35|20 ->20|15 ->14|22.404 ->23.964|3166.995 ->2624.469|2386.821 ->1871.565|106.968 ->98.051|
| Contention |20|50|20 ->20|17 ->17|20.653 ->21.841|5454.180 ->4944.732|4590.832 ->4098.364|232.491 ->221.695|

- Overlapping handler intervals prove simultaneous outstanding work, not
  parallel Python/SQLite execution. Cap20 and50 offered/s do not prove20
  actively processing handlers or sustained50 successful/s. Response E2E
  excludes asynchronous message persistence; phase wall includes its drain.
- Actual game opens for each relation path fall144 ->96 (pure),96 ->64
  (mixed),96 ->64 (contention):3 ->2 per view,224 fewer opens total and448
  fewer initialization PRAGMAs. Transactions/real decisions stay live. Total
  foreground SQL21236 ->20791 (relation calls alone remove448; profile
  recomputation timing adds3). Including background message persistence,
  calls23623 ->23172 and wall10607.593 ->9607.335ms. Recorded statement
  counts are SQL API calls, not individual recursive index seeks.
- Phase cache hits/misses22/42 ->22/42,0/128 ->0/128,0/64 ->0/64;
  partial-range hits16/21/4 ->16/18/2. General/exp/stone generations advance
  0/64/96 identically; `invalidations=0` does not mean write reuse survives:
  generation-key changes cause misses. Cache remains32 entries/about33KiB
  within1MiB. No personal decision data or TOP slice substitutes for rank.
- Rank worker SQL calls194/479/238 ->194/482/238; wall39.842/189.001/124.091
  ->36.887/231.398/136.033ms; CPU27.964/125.191/77.603
  ->31.889/128.950/90.833ms. Mixed/contention rank SQL regresses despite
  lower E2E. Rank CPU is only0.77%/1.46%/0.98% of after traffic CPU;
  remaining seconds of queueing are not explained by cold-rank SQL alone.
  COUNT API calls11/28/14 ->11/31/14, seeks42/128/64 unchanged. This pair
  does not remeasure the earlier bulk-generation update-cost probe.
- Preserve adverse measurements: pure-profile p50 rises0.002ms, mixed
  training SQL1258.143 ->1272.563ms, contention partner SQL428.627 ->434.184ms.
  Mixed producer p95 rises3.300 ->4.011ms and contention loop lag p99
  280.851 ->391.276ms; loop dispatch p95 rises215.975 ->295.796ms there.
  No retry or refactor based on a single fluctuation. Prior V5 and earlier
  mixed regressions remain valid; this different load cannot erase them.

### Resources, Verification And Remaining Work

- Same exclusive validation lock;2 cores,1GiB AS,600 CPU/1200 wall seconds,
  `MALLOC_ARENA_MAX=2`; RAM>=768MiB/disk>=2GiB and continuous RSS900MiB/
  single+total WAL256MiB stop guards. Peak RSS rises425.594 ->445.785MiB;
  sampled VmSize949.004 ->964.727MiB (close to the fixed AS cap, not headroom
  for raising load unconditionally). Minimum traffic RAM1695 ->1746MiB,
  disk5976641536 ->5871751168 bytes; max single WAL15355272 unchanged,
  total21288168 ->21300528 bytes. Restored WAL zero.
- Whole process wall87.811 ->79.397s, CPU71.105 ->68.235s; traffic CPU
  23.909 ->22.253s. Phase setup18.662 ->19.388s, invariants4.743 ->3.668s
  and final restore12.554 ->12.233s stay outside E2E. Whole-process writes
  4280008704 ->4282601472 bytes largely include fixture/snapshot/restore IO;
  sampled traffic writes100311040 ->102723584 bytes, reads1798144 ->1540096.
  Do not attribute restore IO or harness observations to game-write cost.
- 13 new harness/oracle/plan preflight tests pass; concentrated affected
  acceptance64 unittest pass in3.310s in a clean code-only worktree, including
  the new lifecycle tests, existing relationship/UOW and driver/fixture
  regressions. Two transactions see an external commit between reads;
  awaits/rejections/errors close the scope and the next request recovers.
  Compile, architecture `ok=true/errors=[]`, inventory `--check`, whitespace
  pass. Reuse unchanged full-suite baseline; exact delivered SHA CI is the
  remaining delivery gate, not another old local shard/matrix run.

The overall goal remains active beyond V7. Next selection must use the saved
SQL/connection/queue evidence for synchronous router/training costs; dynamic
multiwriter ranks, broader gameplay/background, multiple processes and sustained
50 successful/s remain unverified. Freeze any new data/load before its one
baseline and coherent runtime batch; do not rerun this pair for nicer figures.

## V8: Live Blacklist Reader And Image Projection Reuse (2026-11-26)

### Scope And Controlled Pair

- V7 `208d402d` / CI45772442300 completed/success. V8 targets the measured
  common router cost: each fresh blacklist connection's first membership
  query spent roughly3-4ms parsing/reading the schema. DDL validation was
  already cached; this was not repeated migration work. The repository now
  retains at most one idle main-thread read-only connection, with fresh
  BEGIN/COMMIT and live import-marker/membership reads every time. Workers
  and reentrant calls use independent short-lived connections. No ban,
  balance, qualification or query-result cache was added.
- The explicit UOW owner closes owned cursors before return and discards
  connections on errors, file/options changes and shutdown. Existing UOWs
  without an owner retain their previous lifecycle. DDL metadata now uses
  a64-entry LRU keyed by resolved path, device/inode and schema version;
  the mutable import marker stays live. Tests cover same-version file
  replacement, replacement during a transaction, deletion, external ban/
  unban, SQL/fetch/BEGIN/commit/rollback errors and shutdown/reopen.
- Configured image replies previously built a strict full personal-info
  projection, then entered the image handler and built it all again.
  Both handlers now pass their own freshly validated projection to a shared
  rendering/sending helper. Configured image mode removes the second check,
  title read, exact-rank call, attribute/relationship/natal reads and timestamp
  write, preserving the strict impart provider and command error boundary.
  Small tests mock rendering/transport only; no image rendering/network
  latency or real QQ delivery claim is made by the matrix below.
- Reports in the retained fixture's `data/` directory:
  `live-reader-before-20261126-r01.json` and
  `live-reader-after-20261126-r01.json`, with corresponding `.phases.jsonl`.
  Both first traffic attempts pass. New fixed observer CPU counters were
  frozen before this baseline on clean208d402d. Driver/fixture identities,
  ordered requests, phase specs and limits match exactly; runtime changes
  are limited to blackhouse_repository.py, uow.py, plugin.py and user_info.py.
  Python integer comparison confirms all business seeds match; only startup
  `compatibility_hits.json` telemetry and the enclosing seed hash differ.
  This is not full-byte seed equality and not a comparison with old V7 timing.
- Reuse the populated dynamic-relation-v7 workload:512 requests/version,
  128 profiles,112 mentor views/four aliases,112 partner views,160 actual
  training writes.128 seeded participants/62 issuing actors; one writer
  changes exp+100/stone-1000, crossing16 low readers, alongside32 preserved
  REAL and32 exact BLOB actors. Retain the V7 feature populations, including
  2560 inventories,128 mentor/partner rows each,4096 pending applications,
  8192 activity users and32768 activity point/pass/damage rows each. These
  four paths do not cover all674 matchers, activity commands or million-row
  feature tables. They also do not establish multiwriter dynamic-rank behavior.
- Every request succeeds; refusal/cooldown/exception/timeout/busy/locked and
  per-path error rates are0. All256 reply-rank assertions/version pass using
  actual observed snapshot generation/keys and the independent setup rank
  oracle. Generation advances0/64/96, matching fresh receipts and exact
  writer assets/power;127 nonwriter participant projections, all checked
  inventory/features/relations/JSON and original DB restoration pass.
  All128 observed display keys agree with rank keys, without claiming an
  atomic whole-profile reply. Shared instance/DB and real matcher/DI remain;
  requests and persistence drain before each restore, never between requests.

### Same-Load Results

Milliseconds, before -> after; E2E includes producer, dispatch and admission
queueing. One ordered pair and32-96 samples/path cannot establish variance.

| Phase/path | n/version | E2E p50 | E2E p95 | E2E p99 | Foreground SQL calls | SQL wall ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Pure / profile |32|39.632 ->34.250|143.178 ->112.473|205.115 ->218.125|1829 ->1768|707.198 ->549.515|
| Pure / mentor |48|24.198 ->19.287|69.008 ->42.233|153.017 ->165.800|1492 ->1396|602.770 ->398.092|
| Pure / partner |48|19.076 ->14.296|41.560 ->22.989|148.782 ->146.746|1008 ->912|442.909 ->260.247|
| Mixed / profile |64|1970.075 ->1262.476|3169.465 ->2063.067|3214.651 ->2129.437|3733 ->3603|1612.626 ->1282.013|
| Mixed / mentor |32|1995.357 ->1287.374|3186.680 ->2057.864|3224.263 ->2100.697|994 ->930|427.739 ->279.197|
| Mixed / partner |32|1888.803 ->1170.718|3100.615 ->2018.532|3104.708 ->2020.313|672 ->608|330.551 ->188.379|
| Mixed / training |64|2036.015 ->1319.139|3158.237 ->2078.111|3242.193 ->2131.861|3008 ->2880|1289.248 ->1001.957|
| Contention / profile |32|2719.359 ->2166.445|5046.694 ->4181.947|5161.801 ->4235.390|1875 ->1810|961.253 ->654.022|
| Contention / mentor |32|2792.649 ->2144.029|5125.684 ->4241.958|5188.455 ->4313.699|994 ->930|407.273 ->272.026|
| Contention / partner |32|2739.703 ->2109.543|5084.598 ->4201.670|5149.017 ->4212.449|672 ->608|306.334 ->189.185|
| Contention / training |96|2819.705 ->2231.243|5131.660 ->4222.256|5208.376 ->4333.807|4512 ->4320|1893.364 ->1488.567|

| Phase | Cap | Offered/s | Peak admitted | Peak handler intervals | Successful/s | E2E p95 | Queue p95 | Loop lag p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Pure |10|20|4 ->5|3 ->3|20.093 ->20.108|75.369 ->56.935|0.009 ->0.009|16.067 ->13.454|
| Mixed |20|35|20 ->20|18 ->16|22.508 ->25.771|3158.237 ->2057.864|2356.889 ->1335.204|98.359 ->103.999|
| Contention |20|50|20 ->20|16 ->18|21.452 ->23.993|5125.684 ->4201.670|4252.114 ->3412.823|116.429 ->162.425|

- Game opens321/577/544 ->194/385/352:511 fewer, consistent with one retained
  router connection across512 requests. Warm combined marker/membership
  queries still execute127/191/191 times; their CPU479.709/718.412/713.460
  ->2.932/6.326/6.128ms, wall486.756/743.583/724.592 ->2.157/13.252/12.511ms.
  Remaining cold validation occurs once/phase after restore/schema changes.
  Foreground SQL20789 ->19765; including persistence23170 ->22143 and
  SQL wall10126.366 ->7705.010ms. Counts are API calls, not internal seeks.
- Rank cache hits/misses22/42,0/128,0/64 unchanged; partial hits16/22/0
  ->16/25/3. Generation-key misses persist despite `invalidations=0`.
  No rank algorithm or generation trigger changed. Rank-worker CPU
  27.075/121.102/77.367 ->29.683/106.636/71.037ms; mixed worker wall
  regresses203.845 ->243.942ms. The queues are not explained by rank CPU.
- Keep adverse observations: pure aggregate p99 rises153.017 ->165.800ms;
  profile and mentor p99 also rise. Mixed producer p95 rises1.789 ->3.643ms.
  Contention loop lag p99 rises157.488 ->255.923ms, dispatch p95 rises
  101.559 ->172.976ms. Earlier mixed-load regressions remain valid. No repeat
  was run to select better numbers, and lower E2E is not universal improvement.
- Offered50/s and peak18 overlapping handler intervals are distinct from
  20/50 concurrently processing requests or sustained50 successful/s. Peak
  intervals include awaiting work and do not prove parallel Python/SQLite
  execution. Success throughput still falls below the offered mixed rates.

### Attribution, Limits And Validation

- Four bounded phase/per-thread CPU buckets use exclusive thread CPU for
  record/begin_statement/authorizer/watch, at most16 threads+overflow. Total
  measured observer CPU823.467/1423.452/1359.748 ->717.922/1197.851/1202.845ms;
  watcher alone598.862/974.356/936.469 ->509.000/777.419/807.044ms. MainThread
  CPU3365.231/7537.639/8059.134 ->2660.139/6517.059/7023.171ms. Traffic process
  CPU23.120 ->19.847s. Observer bookkeeping and clock overhead are only
  partly measured and perturbation is uncalibrated. Authorizer CPU overlaps
  SQL CPU: do not add them or subtract this figure from E2E to invent product
  capacity. Remaining CPU includes application, background and unmeasured
  harness work; resource monitoring is measurable test-side overhead.
- Same sole1M fixture and exclusive validation lock;2 cores/1GiB AS,
  600 CPU/1200 wall seconds, MALLOC_ARENA_MAX=2, preflight RAM>=768MiB and
  disk>=2GiB, continuous RSS900MiB/single+total WAL256MiB abort limits.
  RSS457.809 ->476.891MiB and sampled VmSize852.258 ->947.875MiB rise; final
  after VmSize951.078MiB is close to the AS cap, so no load escalation here.
  Minimum traffic RAM1653 ->1684MiB, disk5802455040 ->5793214464 bytes;
  max single WAL15355272/total21296408 bytes unchanged, final WAL0 both.
- Whole process wall86.453 ->83.380s, CPU73.814 ->69.807s; stage setup
  19.654 ->21.663s, invariant checks3.841 ->3.414s and final restore
  13.961 ->13.759s stay outside E2E. Process writes4285890560 ->4288593920
  bytes include setup/snapshot/restore; sampled traffic writes106950656
  ->108363776 bytes also rise. Traffic sampled reads3907584 ->3260416 bytes.
  Do not attribute restored million-row database IO to gameplay writes.
- 37 harness preflight tests pass5.509s; concentrated affected acceptance
  130 tests+13 subtests pass15.81s. Independent review adds two narrow
  in-flight replacement/fetch-failure regressions; both pass after correcting
  the latter's expectation to the numeric UDF's existing text result.
  Final lifecycle review also made repository close a no-op on workers, which
  never retain readers, while main-thread shutdown still releases its owner;
  the focused shutdown test verifies both. This shutdown-only guard was added
  after the measured pair and was not exercised by its traffic; the request
  paths/driver are unchanged and no matrix retry was used for this correction.
  Compile/architecture/inventory/whitespace pass in a clean code-only checkout,
  preserving the user's status edits and legacy JSON deletion. No old green
  full-suite shard or serial matrix was repeated. Exact delivered SHA CI
  remains the batch delivery gate; no main merge, release or deployment.

Overall performance goal remains active. This batch validates common router
cost reduction across four paths, not repository-wide capacity. Next selection
should use saved synchronous profile/sect/timestamp and training transaction
costs, explicitly separating watcher/observer overhead. Multiwriter dynamic
ranks, broader gameplay/background, multiple processes, real image delivery
and sustained50 successful/s remain unverified; do not rerun V8 unchanged.

## V9: Live Profile Projections With Symmetric Multiple Writers (2026-11-26)

### Scope And Frozen Evidence

- V8 `1399ac99` / CI45779082549 completed/success (quality and both unittest
  shards). The next batch targets measured cold profile/sect-name connections,
  including the live profile read before actual stone training. V8's training
  already reuses one write connection across four independent transactions;
  those transaction/replay/rollback boundaries are unchanged.
- `PlayerProfileSqlRepository.get_user_profile` and
  `SectInfoSqlRepository.get_name_by_id` share one opt-in idle main-thread
  reader. Every call starts and ends a fresh readonly transaction; no balance,
  profile, membership, eligibility or name result is cached. An active explicit
  synchronous request scope takes precedence at UOW construction, preserving
  its original connection key and close-before-await boundary. Worker/reentrant
  fallback, identity/error discard and main-thread shutdown retain the V8
  owner contracts. Other profile/sect methods and rank algorithms are unchanged.
- Driver WAL monitoring now traverses each directory once with `os.scandir`.
  It still covers the complete tree, follows file links but not directory
  links, retains thresholds/frequency and stops on traversal errors. This
  change was frozen before both measurements; it is measurement infrastructure,
  not an independently measured product speedup.
- Reports: retained sole1M fixture
  `/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05/data/` contains
  `multiwriter-before-20261126-r01.json`, `multiwriter-after-20261126-r01.json`
  and their `.phases.jsonl` files. Both first traffic attempts passed. Exact
  Python-integer comparison confirms identical driver/fixture identity,
  phase specs, limits, ordered requests and business seeds. The only seed
  differences are startup `compatibility_hits.json` telemetry and its enclosing
  seed hash. Only the four scoped runtime files differ between source trees.
- Each version executes384 real matcher/DI/handler requests on one shared
  instance and isolated DB set:168 profile,76 mentor (four aliases),76 partner,
  64 actual stone-training requests. Each stage has128 requests, including
  56 distinct profile actors (32 writers,16 different-score low readers,8 exact
  high readers). Pure/mixed/contention use caps10/20/20 and offered20/35/50/s;
  pure has36 mentor+36 partner, the other stages20+20 and32 writes. There are
  66/58/58 actual actors among128 seeded participants. This is not exhaustive
  coverage of674 matchers or background work.
- In each write stage,32 users each perform exactly one actual exp+100,
  stone-1000 update. Writers start at the same score pair. A bounded setup
  histogram over all1M rows (111 exp/76 stone groups, hard16384 groups/column)
  computes independent dense ranks. Captured snapshot dual-generation offsets
  choose the global shape; the actual target's paired keys select old/new own
  state, rejecting torn pairs and impossible endpoints. No oracle SQL is added
  during requests, no business result is mocked, and restores occur only after
  task/message/rank-worker drain and invariant checks.
- Symmetric writers create only three distinct histogram shapes: zero, some,
  all32 writes. They favor same-score cache reuse. This validates a bounded
  symmetric multiwriter case, not arbitrary multiwriter scores or repeated
  writes to one user's balance. Preserved32 REAL/32 exact BLOB participants,
  96 unchanged nonwriters, live resources/receipts, relationships and JSON
  checks all pass. Each version validates336 reply ranks; generation deltas
  are0/32/32, exactly matching successful fresh writes. All384 requests succeed;
  refusals/cooldowns/exceptions/timeouts/busy/locked are zero (error rate0%).
- Nonempty features remain:2560 backpack rows,128 BuffInfo/bank/arena/status/
  impart/mentor/partner rows each,4096 pending mentor applications,8192 activity
  users and32768 activity point/pass/damage rows each; sects has4 rows. Activity
  commands are not executed in V9, so this does not establish their runtime
  performance at that scale or a large sect-directory scan result.

### Same-Load Results

Values are before -> after; latency is milliseconds including scheduled-arrival
queueing. Each table row retains every sample, not only successful fast cases.

| Phase / Path | n/version | E2E p50 | E2E p95 | E2E p99 | Foreground SQL calls | SQL wall ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| pure / mentor |36|20.102 ->19.089|39.713 ->24.467|211.832 ->48.129|1046 ->1046|304.813 ->279.549|
| pure / partner |36|15.339 ->14.532|110.454 ->18.011|227.829 ->92.916|686 ->686|201.655 ->196.890|
| pure / profile |56|35.078 ->24.626|161.011 ->38.608|281.048 ->143.651|2984 ->2760|970.717 ->523.471|
| mixed / mentor |20|855.917 ->178.909|1014.036 ->231.742|1034.101 ->237.111|582 ->582|174.206 ->175.453|
| mixed / partner |20|853.836 ->146.368|1020.862 ->209.265|1027.963 ->210.457|380 ->380|121.979 ->125.467|
| mixed / profile |56|622.960 ->171.568|1049.188 ->263.952|1083.680 ->270.959|3077 ->2892|1196.148 ->737.309|
| mixed / training |32|748.884 ->166.778|1025.062 ->246.984|1027.103 ->260.556|1440 ->1376|535.024 ->415.182|
| contention / mentor |20|1630.268 ->987.361|2054.225 ->1172.099|2074.036 ->1172.264|582 ->582|191.096 ->164.550|
| contention / partner |20|1603.059 ->910.916|2013.893 ->1132.963|2032.848 ->1149.187|380 ->380|115.864 ->111.576|
| contention / profile |56|1169.719 ->710.864|1999.743 ->1178.907|2153.268 ->1228.268|3049 ->2825|1181.169 ->696.929|
| contention / training |32|1300.451 ->805.806|2094.141 ->1192.389|2113.835 ->1198.772|1440 ->1376|549.927 ->406.863|

| Phase | cap / offered per s | Peak admitted | Peak handler intervals | Success/s | Phase E2E p95 | Queue p95 | Loop lag p95 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| pure |10 /20|6 ->3|3 ->2|20.094 ->20.093|110.454 ->31.813|0.011 ->0.009|14.498 ->10.898|
| mixed |20 /35|20 ->10|14 ->6|28.378 ->34.110|1025.062 ->243.269|421.458 ->0.008|91.824 ->49.866|
| contention |20 /50|20 ->20|15 ->14|28.106 ->35.037|2054.225 ->1185.382|1500.066 ->687.264|91.866 ->66.773|

Handler counts measure overlapping wall intervals, not parallel CPU/SQLite
execution. Same-actor profile/training intervals actually overlap30 ->23 times
in mixed and30 ->30 in contention. Observed writer rank states are old/new
30/2 ->24/8 in mixed,32/0 ->32/0 in contention. Thus contention does not sample
a post-write own profile, although it verifies all final assets/receipts and
changing global offsets. No profile display/rank key divergence was observed;
the reply still has no whole-profile atomicity guarantee. Offered50/s yields
only35.037 successful/s over a finite run, not sustained50/s or20 simultaneous
handlers. Existing earlier regressions remain valid historical evidence.

### SQL, Cache And Measurement Costs

- Game opens243/272/272 ->131/129/128,399 fewer total. Captured profile/name
  SELECT CPU totals2074.137 ->583.326ms across628 unchanged SELECT calls;
  scoped relation profile reads intentionally retain their existing lifecycle.
  Foreground SQL15646 ->14885; including message-writer background17455 ->16699.
  Total observed SQL wall6363.657 ->4611.800ms. Training45 ->43 SQL/request
  removes initialization PRAGMAs, not business checks or write transactions.
- Main-thread CPU3119.266/3938.363/4017.465 ->2500.868/3169.675/3161.127ms;
  total traffic CPU13.320 ->10.789s. Queueing falls more than isolated SQL time,
  consistent with lower synchronous work near the offered-rate threshold.
  This single pair does not establish a universal throughput multiplier.
- Rank hits/misses pure70/42 ->70/42; mixed20/92 ->2/110; contention34/78 ->34/78.
  Partial hits33/22/19 ->33/30/19; evictions10/60/46 ->10/78/46. All explicit
  invalidations/waits/expirations/bypasses are0; generation changes still make
  keys stale, so `invalidations=0` does not mean writes caused no invalidation.
  Full COUNT(DISTINCT) operations remain0. Seek batches42/92/78 ->42/110/78;
  mixed seek CPU46.550 ->50.589ms and wall76.743 ->79.337ms regress as actual
  interleaving changes. All exact snapshot reply checks still pass. Rank-worker
  total SQL CPU28.879/80.265/83.269 ->29.392/83.732/64.120ms is a small component
  of this workload; it does not justify a further rank-algorithm rewrite.
- All BEGIN execution totals11.127/22.844/20.973 ->9.643/22.443/21.139ms;
  mixed/contention max2.237/1.273 ->3.820/2.124ms regress. These include SQL
  execution, not pure lock-wait measurements. No busy/locked error occurs.
- Watcher CPU277.315/247.956/286.009 ->247.911/203.052/166.320ms with25/17/17
  ->25/14/14 samples. Per-sample costs and stage lengths both vary. Observer
  record/begin/authorizer are retained in raw reports; authorizer overlaps SQL
  CPU and cannot be added to it. Neither measured observer cost nor unexplained
  CPU can be subtracted from E2E or labeled purely application CPU. Watcher
  traversal improvement is common to both versions, not a V9 runtime delta.
- GC total22.313/43.749/46.064 ->21.074/31.049/42.106ms; max pause remains below
  2.1ms. Contention producer delay p95/p99 worsens1.608/4.493 ->3.649/6.019ms;
  loop-dispatch p95 improves78.155 ->71.777ms, loop-lag p99 improves129.369
  ->113.163ms. Mixed mentor/partner SQL wall slightly increases as table shows.

### Resources And Acceptance

- Same hard limits:2/4 CPU affinity,1GiB address space,600 CPU seconds,1200 wall
  seconds,`MALLOC_ARENA_MAX=2`; RAM768MiB and disk2GiB floors,900MiB RSS stop,
  256MiB individual/total WAL guard. One process pair runs serially under the
  existing validation lock, with only transient restore snapshots of the sole
  1M fixture. No production/network/account traffic.
- Max RSS475.918 ->503.988MiB regresses; sampled VmSize846.488 ->871.699MiB.
  RAM minimum1628 ->1719MiB; free disk minimum5625774080 ->5963890688 bytes.
  Max individual WAL15355272 bytes both; total21288168 ->21284048 bytes;
  final WAL0, original rank digest restored both. These observations do not
  authorize raising concurrency or memory ceilings.
- Full process wall86.558 ->77.664s; setup21.759 ->20.821s, invariant checks
  5.067 ->3.846s, final restore15.050 ->13.594s. Sampled traffic reads4423680
  ->2691072 and writes55828480 ->55504896 bytes; full process writes4230926336
  ->4231430144 bytes slightly rise and primarily include fixture restoration.
  Full-run I/O/CPU and setup/cache warmth are not product request costs.
- 45 bounded driver/oracle/WAL/DI tests pass0.852s before driver freeze. After
  implementation,78 affected pytest cases and52 subtests pass9.16s, including
  unchanged relation/asset scope checks, numeric error contracts, external
  commits, missing/replaced files, copied worker contexts and shutdown. Compile,
  architecture, inventory and whitespace pass in a code-only acceptance tree.
  No old full unittest shard, old serial matrix or unchanged mixed stage was
  rerun. Exact delivered SHA CI remains the batch delivery gate.

The expanded performance goal remains active. The next evidence selection must
use the saved remaining synchronous timestamp/avatar/title/natal and relation
costs before any new load; no unchanged V9 rerun or unconditional writer-cache
change. Arbitrary multiwriter score transitions, repeated-user writes with
dynamic exact ranks, broader gameplay/background and sustained50 successful/s
remain unverified. No main merge, release or deployment is authorized.

## V10: Live Player Projections And Distinct Writers (2026-11-26)

### Scope And Frozen Evidence

- V9 `d681c5b1` / CI45784902089 completed/success, including quality and both
  unittest shards. Saved V9 samples identify cold avatar/title connections:
  avatar SELECT CPU458.7ms across384 requests and title189.255ms across168
  profiles. Training transactions and the exact-rank algorithm are unchanged.
- Avatar `get_active_id`/`get_avatar_info` and title `get_state` now share one
  independent idle main-thread player reader, avoiding eviction of the game
  projection reader. Each call still begins and ends a fresh readonly
  transaction. Actual avatar targets, equipped titles, balances and eligibility
  are never result-cached. Worker/reentrant fallback, error/identity discard,
  explicit scope lifetime and main-thread shutdown retain the existing rules.
- Avatar schema metadata uses an LRU of at most64 entries, keyed by resolved
  path, device/inode and live schema_version, including negative results.
  Explicit scopes and initialization read/write checks inspect the schema
  directly: their existing non-owner UOW does not bind a stable handle identity
  for this cross-request metadata cache. This deliberately adds schema queries
  in those paths rather than caching metadata under a potentially wrong inode.
- One new frozen pair, not a rerun of V9: sole1M fixture
  `/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05/data/` contains
  `player-projection-before-20261126-r01.json`,
  `player-projection-after-20261126-r01.json` and their `.phases.jsonl` files.
  Both first traffic attempts pass. Exact Python-integer comparison confirms
  identical driver/fixture/limits/specs/ordered requests and business seeds;
  only startup compatibility telemetry and its enclosing seed hash differ.
  The four scoped runtime files are the only product differences.
- Each version executes384 real matcher/DI/handler requests on one shared
  instance and isolated DB set:168 profiles,76 mentor,76 partner and64 actual
  stone-training requests. Three128-request stages use caps10/20/20 and offered
  rates20/35/50 per second. Each has56 profile actors:32 different-score writers
  and24 exact high readers. Actual actors56/56/56 come from128 participants.
  Coverage is a bounded sample, not all674 matchers or background tasks.
- Each writer performs one real exp+100/stone-1000 update per write stage.
  Its separate interval contains exactly one fixed non-dispatch pivot in each
  column; its exp dense rank improves1 and stone rank worsens1. Exclusive
  endpoints and disjoint intervals prove other writers' order cannot change
  that expectation. A bounded full1M setup histogram and captured dual
  generation/target keys select independent old/new ranks, without request
  oracle SQL or audit triggers (142 exp/107 stone initial distinct groups,
  hard16384 groups/column). This is32 distinct writers and32 local rank
  crossings, not arbitrary crossing intervals or repeated same-user writes.
- Avatar/title each contain8192 rows referencing existing synthetic users;
  every owner has a different existing nonpopulation avatar, active=self.
  Titles are actual named definitions with populated unlocked/equipped state.
  Streaming bounded full-table hashes plus128 exact actor rows stay unchanged;
  all168 profile replies/version show the expected title. This measures live
  routing/state reads, not large-scale avatar switching or title eligibility.
  Existing nonempty inventory/relation/activity features remain as in V9;
  activity commands are not executed, so no activity throughput claim follows.
- All384 requests/version succeed. Business refusals/cooldowns/exceptions/
  timeouts/busy/locked are0, error rate0%. All336 exact reply-rank assertions,
  live resources/receipts/ledgers,96 unchanged nonwriters,32 REAL/32 BLOB
  participants, inventories, relationships, JSON and originalDB restore pass.
  Generation deltas0/32/32 match fresh successful writes exactly.

### Same-Load Results

Before -> after, milliseconds including scheduled-arrival queueing. All samples
are retained. The pure baseline includes a GC outlier discussed below.

| Phase / Path | n/version | E2E p50 | E2E p95 | E2E p99 | Foreground SQL calls | SQL wall ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| pure / mentor |36|22.049 ->19.764|1793.552 ->23.940|1876.389 ->93.758|1046 ->1154|302.793 ->290.911|
| pure / partner |36|68.767 ->15.108|1880.558 ->30.906|1980.880 ->132.025|686 ->794|213.279 ->217.010|
| pure / profile |56|27.315 ->22.728|1689.593 ->69.151|1937.461 ->183.767|2903 ->2679|624.566 ->397.775|
| mixed / mentor |20|126.229 ->39.372|170.412 ->62.330|187.138 ->64.951|584 ->642|180.939 ->171.209|
| mixed / partner |20|126.080 ->34.286|177.932 ->76.867|212.704 ->76.908|380 ->440|131.727 ->116.068|
| mixed / profile |56|144.594 ->63.210|226.215 ->127.848|309.710 ->135.040|2926 ->2705|759.501 ->570.414|
| mixed / training |32|135.103 ->60.405|240.385 ->84.092|247.786 ->98.031|1376 ->1312|398.385 ->369.017|
| contention / mentor |20|1140.036 ->847.513|1460.175 ->1038.740|1480.274 ->1058.651|582 ->642|177.064 ->181.391|
| contention / partner |20|1057.752 ->735.776|1346.071 ->1017.914|1363.736 ->1032.598|380 ->440|116.940 ->126.042|
| contention / profile |56|863.919 ->610.641|1422.338 ->1015.145|1537.140 ->1138.320|2916 ->2696|789.267 ->680.523|
| contention / training |32|918.795 ->700.797|1498.109 ->1044.815|1517.962 ->1064.891|1376 ->1312|414.094 ->361.293|

| Phase | cap / offered per s | Peak admitted | Peak handler intervals | Success/s | Phase E2E p95 | Queue p95 | Loop lag p95 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| pure |10 /20|10 ->4|10 ->2|17.446 ->20.097|1740.357 ->30.546|325.077 ->0.009|29.196 ->9.403|
| mixed |20 /35|10 ->5|7 ->3|34.237 ->35.025|212.912 ->113.035|0.008 ->0.008|46.330 ->32.982|
| contention |20 /50|20 ->20|16 ->16|33.108 ->36.886|1422.338 ->1032.598|875.382 ->515.612|76.060 ->77.673|

Overlapping handler intervals are not parallel CPU/SQLite execution. Same-actor
profile/write intervals overlap26 ->3 times in mixed and30 ->30 in contention.
Writer rank old/new observations are22/10 ->16/16 in mixed,32/0 ->32/0 in
contention. Mixed therefore checks actual post-write one-rank changes;
contention has no post-write own profile sample. No display/rank key divergence
is observed, but whole-profile atomicity is still not guaranteed. Finite
offered50/s yields36.886 successful/s, not sustained50/s or20 actual handlers.

### Attribution, Regressions And Resources

- Player opens796 ->397,399 fewer. Captured avatar/title SELECT diagnostics
  cover552 ->551 statements, with CPU678.674 ->29.812ms and wall713.758
  ->62.383ms. One after statement exceeds the first12-per-connection diagnostic
  cap, so these are bounded captured totals, not exact all-SELECT CPU totals.
  No new per-command metadata hit counter is claimed. Foreground SQL15155
  ->14816; including background16953 ->16676; total SQL wall5007.737
  ->4348.589ms. Training43 ->41 SQL/request; business transactions remain.
- Relation SQL counts increase by about3/request because scoped avatar schema
  checks are now live. Contention mentor/partner SQL wall177.064/116.940
  ->181.391/126.042ms also regresses. This correctness cost is retained.
- Rank hits/misses pure16/96 unchanged; mixed0/112 unchanged; contention6/106
  ->4/108. Partial hits89/35/47 ->89/40/50, evictions64/80/74 ->64/80/76;
  explicit invalidations/waits/expirations/bypasses are0. Generation changes
  still invalidate keys. COUNT fallbacks0/3/3 ->0/1/3 and indexed seek batches
  96/112/106 ->96/112/108 remain bounded. Contention rank-worker SQL wall
  179.867 ->228.212ms and CPU72.467 ->74.243ms regress; exact results pass.
  The algorithm was not changed and this does not warrant a blind rewrite.
- MainThread CPU2888.250/3160.733/3282.031 ->2372.790/2956.457/2912.300ms;
  total traffic CPU11.511 ->10.370s. BEGIN counts792/888/888 are unchanged;
  contention BEGIN total22.931 ->25.662ms regresses, with no busy/locked.
  These times include SQL execution, not pure transaction wait.
- Baseline pure has a MainThread generation2 GC pause1830.058ms, coinciding
  with loop max1840.658ms and producer max1797.873ms. After pure's largest
  GC is1.396ms. The trace has no GC object provenance or page-fault counters;
  pure p951740.357 ->30.546ms cannot all be attributed to the product change.
  Cold OS/cache and setup effects are not controlled. No rerun was selected
  to replace this outlier. Contention loop p95 and producer p95/p99
  2.129/5.260 ->2.917/5.457ms regress; loop-dispatch p99 also rises
  90.705 ->96.764ms.
- Observer record/begin/authorizer/watch CPU remains in the reports. Authorizer
  overlaps SQL CPU; neither the measured sections nor unexplained residual CPU
  can be subtracted from E2E or called pure application cost. Full process wall
  94.261 ->77.933s includes setup23.129 ->19.517s, invariant7.768 ->3.913s
  and final restore14.922 ->13.992s. Sampled traffic reads46,317,568
  ->3,891,200 bytes, writes54,550,528 ->58,363,904 bytes regress. Full process
  writes4,235,288,576 ->4,238,979,072 bytes include fixture restoration and
  are not traffic I/O. Full process reads4.317 ->2.064GB also reflect warmth.
- Same caps/lock:2/4 CPU,1GiB address space,600 CPU seconds,1200 wall seconds,
  RAM768MiB/disk2GiB floors,RSS900MiB and256MiB single/total WAL stop,
  `MALLOC_ARENA_MAX=2`. Max RSS498.156 ->525.750MiB regresses; sampled virtual
  peak963.586 ->904.727MiB leaves little baseline headroom. Minimum RAM1478
  ->1823MiB, free disk5,795,504,128 ->5,713,612,800 bytes. Maximum single
  WAL15,355,272 bytes; total22,384,088 ->22,400,568 bytes; final WAL0 both.
  This does not authorize increasing load or copying the1M database.
- Driver/oracle/seed/DI39 unittest cases pass1.139s before freeze. Clean
  code-only acceptance70 pytest cases and20 subtests pass7.70s, including
  actual warm-reader toggle/equip visibility, independent game/player handles,
  scope/worker/shutdown, identity replacement, error recovery and LRU64/False
  bounds. Compile/architecture/inventory/whitespace pass. Independent runtime
  review found no blocker. No old full shard, serial matrix or unchanged mixed
  stage was rerun; exact delivered SHA CI remains the delivery gate.

The expanded performance goal stays active. Remaining evidence includes cold
timestamp writer initialization, unpopulated natal reads, arbitrary/repeated
multiwriter rank transitions, broader gameplay/background and sustained50
successful requests/s. V10 is a bounded live-read batch, not completion of the
repository-wide goal. No main merge, release or deployment is authorized.

## V11: Scoped Relationship Schema Metadata (2026-11-26)

- V10 `0f9e9525` passed exact-SHA CI `45804281267`. Its saved relation traces
  showed repeated avatar table/schema probes in `我的师徒` and `我的道侣`.
  Within an explicit database connection scope, UOW now exposes only its
  validated file identity for the existing bounded 64-entry schema-version
  cache. File replacement is rechecked at connection entry; avatar/profile
  rows, active identity, equipped title, balances, eligibility and writes
  remain live. No result or player-state cache was added.
- One valid same-fixture pair is retained at
  `/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05/data/`:
  `relation-schema-before-v11-20261126-r01.json` and
  `relation-schema-after-v11-20261126-r01.json`. Both use the same sole
  1M-user, 388,411,392-byte fixture and rank digest
  `0dd134931a7cb0aa3511cc90d50c3c873b3cea97f576dd74e7e536af577ad8c7`;
  each version runs 384 requests, 128 per pure/mixed/contention phase, through
  the real matcher/DI/handler on one shared instance and isolated DB set.
  The four paths are `我的师徒`, `我的道侣`, `我的修仙信息` and `灵石修炼`.
  All 384/version succeeded; error, refusal, cooldown, timeout and
  busy/locked counts are zero, and every invariant/restore check passed.
  This is a bounded four-path sample, not all 674 matchers or 41 background
  entries; per-command cache counters are not available.

| Phase / path | n | E2E p50 ms before -> after | E2E p95 ms before -> after | SQL calls before -> after | SQL wall ms before -> after |
| --- | ---: | ---: | ---: | ---: | ---: |
| pure / mentor | 36 | 21.221 -> 19.958 | 146.434 -> 29.613 | 1154 -> 1046 | 329.285 -> 293.141 |
| pure / partner | 36 | 15.981 -> 15.574 | 178.286 -> 23.396 | 794 -> 684 | 208.788 -> 194.297 |
| pure / profile | 56 | 23.091 -> 23.145 | 100.269 -> 33.170 | 2679 -> 2681 | 372.272 -> 374.308 |
| mixed / mentor | 20 | 37.866 -> 117.920 | 65.588 -> 249.726 | 642 -> 582 | 171.816 -> 157.550 |
| mixed / partner | 20 | 32.900 -> 109.854 | 74.944 -> 228.274 | 440 -> 380 | 110.223 -> 107.772 |
| mixed / profile | 56 | 47.793 -> 160.566 | 96.273 -> 310.030 | 2703 -> 2705 | 403.503 -> 395.837 |
| mixed / training | 32 | 53.027 -> 157.191 | 84.809 -> 303.072 | 1312 -> 1312 | 352.437 -> 356.208 |
| contention / mentor | 20 | 808.026 -> 1027.483 | 943.800 -> 1195.922 | 642 -> 582 | 167.191 -> 176.548 |
| contention / partner | 20 | 725.346 -> 923.737 | 933.785 -> 1134.193 | 440 -> 380 | 113.713 -> 111.378 |
| contention / profile | 56 | 526.200 -> 686.975 | 905.750 -> 1136.493 | 2700 -> 2699 | 424.184 -> 421.988 |
| contention / training | 32 | 656.654 -> 847.539 | 963.535 -> 1196.798 | 1312 -> 1312 | 378.555 -> 352.894 |

- Relationship paths remove 458 SQL calls across the paired samples, with
  lower relationship SQL wall time in pure and mixed phases. This does not
  translate into a consistent end-to-end gain: mixed and contention p50/p95
  regress across relationship, profile and training paths, including paths
  outside the schema-cache change. Pure-phase tails improve, but this finite
  pair cannot assign causality or resolve the mixed/contended regressions; do
  not claim an overall latency win. The underlying cached metadata remains
  correctness-bounded by device/inode and live `schema_version`.
- Cache phase counters remain 16/96 hits/misses in pure, 0/112 in mixed and
  4/108 in contention for both versions; no expiry, invalidation, wait or
  bypass occurred. Shared-cache size remained 32 entries / about33 KiB under
  its 32-entry / 1 MiB caps. Offered rates were20/35/50 per second with
  concurrency caps10/20/20; successful throughput was19.703 -> 20.098,
  35.015 -> 35.051 and37.381 -> 35.142 per second. The maximum observed
  handler-interval overlap was15 -> 18, not evidence of equivalent CPU/SQLite
  parallelism or sustained50 successful requests/s.
- Limits were1 GiB address space,600 CPU seconds,1,200 wall seconds, one
  affinity CPU on a two-CPU host, plus the existing 768 MiB free-memory,
  2 GiB disk and WAL guardrails. Observed minimum available memory was1,506
  -> 1,688 MiB and minimum free disk14.958 -> 14.936 GB; peak RSS rose
  526.609 -> 552.047 MiB; maximum WAL remained15,355,272 bytes. CPU was
  62.826 -> 60.820 seconds, wall time93.286 -> 78.887 seconds, and the
  1M fixture restore took14.717 -> 14.325 seconds. These setup/restore and
  process totals are not per-request application costs.
- Direct regression `tests.test_player_projection_reader` plus
  `tests.test_uow_connection_scope`:27 passed. The earlier full-suite attempt
  in the protected worktree is not a valid result because the pre-existing
  deletion of `docs/refactor_phase2_legacy_path_items.json` caused seven
  frozen owner assertions to fail. The deletion remains untouched; exact-SHA
  CI on a clean checkout is the full-suite gate for this delivery. Compile,
  architecture and whitespace checks are performed on the scoped diff before
  delivery. The overall performance goal remains active; no main merge,
  release or deployment is authorized.

## V12: Live Activity Standings and Indexed Public Rows (2026-11-27)

- V11 SHA `9745f1e787d3c86b06ce3bff31e901c257acb208` passed exact-SHA
  CI `45817424407` (quality and both unittest shards). This batch adds
  `activity_state.005` to the startup migration catalog, only for `game_db`.
  It installs covering sign-order and positive-boss-damage indexes on existing
  databases; published migrations remain unchanged. Sign standing now joins
  the viewer once instead of three correlated PK lookups, and both standings
  restrict their aggregate to positive rows. Sign display explicitly uses
  `user_id ASC` for the final tie, matching exact personal rank. Personal
  position/value/reward eligibility still execute live SQL in one transaction;
  public display retains the existing bounded 60-minute snapshot and freshness
  label. No new result cache, balance cache or write-rule change was added.
- Sole retained fixture and report directory:
  `/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05/data/`.
  Valid pair: `activity-rank-before-v12-20261126-r02.json` and
  `activity-rank-after-v12-20261127-r02.json`; compact extraction:
  `activity-rank-comparison-v12-20261127-r01.json`. The original 1M-user
  manifest/rank digest is identical to V11, as are frozen phase specs and
  every request identity/actor/argument/oracle. The after records runtime tree
  `53e1209a81e33ddcb0f5fcd3de46e0cb2dd625b0aa514d033f6007797fc8e743`;
  only the activity migration/repository and startup catalog differ from the
  baseline runtime. A read-only post-run check confirmed the `.005` marker
  and both index definitions in the actual fixture.
- Each version dispatches 384 real matcher/DI/handler requests on one shared
  instance/database set: three phases of128; three new activity paths each
  have12 requests/phase, alongside56 profile reads,18/2/2 mentor reads and
  18/2/2 partner reads. Mixed/contention each retain32 real distinct-user
  `灵石修炼` writes (exp+100, stone-1000), with exact dynamic rank/live
  asset/receipt and huge-number strata checks. All384/version succeed;
  business refusal, cooldown, system error, timeout, boundary violation and
  SQLite busy/locked counts are zero. All phase invariants and final original
  1M rank-digest restoration pass. This seven-path short sample is not all
  674 matchers, background jobs or sustained capacity.
- Activity tables are populated:8,192 sign users and32,768 boss rows across
  four keys (8,192 each), plus8,192 avatar/title rows and4,096 mentor
  applications. `活动排行 首领榜` uses `festival_sign` while
  `活动首领排行` uses `festival_world_boss`. All four boss cohorts are seeded
  identically, so the setup full-sort world-boss oracle also matches the
  generic board's replies. The activity invariant hashes the sign table and
  world-boss cohort; it does not independently hash every boss key. No activity
  sign/damage writes occur during traffic, so activity-index write cost and
  concurrent activity-reward changes remain unmeasured. These are explicit
  limits, not evidence for performance across other activity datasets.

All times below include producer/loop/admission queueing. Activity n12 means
p95 and p99 are both the maximum sample; this is weak tail evidence.

| Phase / path | n | E2E p50 ms before -> after | E2E p95 ms before -> after | E2E p99 ms before -> after | SQL calls before -> after | SQL wall ms before -> after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| pure / sign board | 12 | 16.933 -> 17.489 | 92.050 -> 64.403 | 92.050 -> 64.403 | 199 -> 199 | 91.605 -> 77.797 |
| pure / boss board | 12 | 17.371 -> 17.876 | 36.495 -> 23.502 | 36.495 -> 23.502 | 198 -> 198 | 99.362 -> 78.510 |
| pure / boss display | 12 | 8.774 -> 8.989 | 31.113 -> 23.588 | 31.113 -> 23.588 | 55 -> 55 | 13.329 -> 4.923 |
| pure / mentor | 18 | 20.046 -> 20.690 | 129.270 -> 33.577 | 129.270 -> 33.577 | 524 -> 524 | 139.432 -> 142.662 |
| pure / partner | 18 | 15.591 -> 15.825 | 162.517 -> 32.465 | 162.517 -> 32.465 | 342 -> 342 | 91.584 -> 100.103 |
| pure / profile | 56 | 23.192 -> 24.052 | 34.863 -> 39.934 | 142.307 -> 114.814 | 2685 -> 2685 | 415.659 -> 427.040 |
| mixed / sign board | 12 | 247.512 -> 240.658 | 394.939 -> 378.350 | 394.939 -> 378.350 | 199 -> 199 | 99.496 -> 83.027 |
| mixed / boss board | 12 | 262.595 -> 247.297 | 321.507 -> 319.996 | 321.507 -> 319.996 | 199 -> 199 | 103.578 -> 91.849 |
| mixed / boss display | 12 | 169.010 -> 144.524 | 263.018 -> 248.600 | 263.018 -> 248.600 | 55 -> 55 | 13.315 -> 6.576 |
| mixed / mentor | 2 | 78.889 -> 48.136 | 106.403 -> 54.239 | 106.403 -> 54.239 | 60 -> 60 | 16.046 -> 18.772 |
| mixed / partner | 2 | 74.067 -> 40.124 | 77.651 -> 40.552 | 77.651 -> 40.552 | 38 -> 38 | 14.778 -> 10.610 |
| mixed / profile | 56 | 275.374 -> 258.255 | 389.157 -> 375.854 | 423.037 -> 432.113 | 2699 -> 2706 | 723.178 -> 702.155 |
| mixed / training | 32 | 281.282 -> 229.037 | 384.161 -> 293.043 | 405.886 -> 301.434 | 1312 -> 1312 | 380.797 -> 371.851 |
| contention / sign board | 12 | 399.449 -> 319.199 | 705.302 -> 675.003 | 705.302 -> 675.003 | 199 -> 199 | 94.992 -> 82.696 |
| contention / boss board | 12 | 1024.063 -> 1086.964 | 1177.072 -> 1183.720 | 1177.072 -> 1183.720 | 199 -> 199 | 92.834 -> 87.649 |
| contention / boss display | 12 | 1105.662 -> 1163.911 | 1149.551 -> 1296.698 | 1149.551 -> 1296.698 | 55 -> 55 | 15.866 -> 9.230 |
| contention / mentor | 2 | 1125.966 -> 1206.570 | 1145.759 -> 1226.358 | 1145.759 -> 1226.358 | 60 -> 60 | 16.277 -> 16.158 |
| contention / partner | 2 | 1084.942 -> 1166.722 | 1105.046 -> 1185.709 | 1105.046 -> 1185.709 | 38 -> 38 | 11.934 -> 10.468 |
| contention / profile | 56 | 658.191 -> 704.313 | 1181.966 -> 1275.758 | 1228.554 -> 1295.626 | 2701 -> 2690 | 717.893 -> 962.545 |
| contention / training | 32 | 743.779 -> 949.854 | 1279.357 -> 1243.106 | 1300.043 -> 1262.067 | 1312 -> 1312 | 371.378 -> 391.034 |

- Complete captured activity statement diagnostics (execute+fetch) show sign
  standing36 SQLs223.272 ->201.897ms wall /223.090 ->201.897ms CPU;
  boss standing36 SQLs228.262 ->207.695ms wall /227.561 ->206.148ms CPU.
  These exact counts still scan qualifying index rows, not an O(1) rank cache.
  Sign cold display3 SQLs20.423 ->0.740ms wall; boss cold display6 SQLs
  60.448 ->4.785ms wall. The query-plan regression requires both covering
  indexes and no temporary display sort. This is a measured SQL reduction,
  not proof of a general E2E improvement: public/standing SQL API call counts
  stay the same, pure medians rise slightly, and contention boss/profile/
  relationship tails regress. Total contention SQL wall1804.172 ->1920.556ms
  also regresses, with profile SQL717.893 ->962.545ms. No repeated run was
  selected to replace these outcomes.
- Offered rates20/35/50 per second and caps10/20/20 are distinct from observed
  interval overlap2/9/19 ->2/9/17 and success throughput19.755/34.534/35.301
  ->20.091/34.890/34.532 per second. Contention queue p95722.952 ->782.053ms
  and producer delay p9532.235 ->38.859ms regress; loop lag p9510.824/88.549/
  90.855 ->9.518/75.692/93.907ms includes another contention regression.
  Loop dispatch p95 is0.193/69.523/103.237 ->0.980/62.380/87.180ms.
  Mixed loop p99133.691 ->208.595ms regresses. This still does not establish
  sustained50 successful requests/s or20 simultaneously executing handlers.
- Cache phase hits/misses48/103,37/114,35/116 ->48/103,33/118,39/112;
  partial hits91/44/47 ->91/40/45; evictions71/82/84 ->71/86/80; waits0/0/1
  ->0/0/2. Explicit invalidations, expiry, bypass and oversized counts are0.
  Entries remain32, estimated bytes peak105,825 ->105,819 under32 entries/
  1MiB caps. Per-command cache counters are not measured. Exp/stone generation
  changes0/32/32 in both versions; generation-key invalidation still applies.
  BEGIN counts759/855/855 are unchanged, total execution54.505 ->59.689ms
  regresses; this includes SQL execution, not pure transaction lock waiting.
- Limits:2/4 CPUs,1GiB address space,600 CPU seconds,1200 wall seconds,
  `MALLOC_ARENA_MAX=2`, common validation lock,768MiB available-memory floor,
  2GiB free-disk floor,900MiB RSS stop and256MiB single/total WAL stop.
  Minimum sampled available RAM1701 ->1742MiB; free disk14,934,732,800
  ->14,833,831,936 bytes. Peak RSS577.656 ->242.613MiB and sampled virtual
  memory951.566 ->643.293MiB reflect the driver repair below, not a claimed
  product memory reduction. Maximum single WAL15,355,272 ->18,321,672 and
  total WAL22,433,528 ->25,399,928 bytes regress but remain below caps; final
  WAL0 both. Process CPU64.087 ->62.311s / wall86.920 ->75.454s include
  setup, hashing and snapshot IO; final restore14.161 ->13.641s is not request
  latency. Sample-window IO reads2,396,160 ->180,224 / writes57,315,328
  ->55,717,888 bytes; larger full-process reads2,342,916,096 ->765,636,608 /
  writes3,768,799,232 ->3,792,945,152 bytes include restoration and are not
  application traffic. MainThread CPU2526.355/3115.967/2886.881
  ->2399.239/3064.928/3116.921ms retains the contention regression. Observer
  record/begin/authorizer/watch counters remain in the reports; authorizer
  overlaps SQL CPU, and observer overhead cannot be subtracted from E2E.
- Provenance: early setup/actor-selection failures are invalid benchmark
  evidence. Earlier after attempts never installed indexes because `.001`
  was already applied; they remain diagnostics, including worse tails, not
  accepted index results. One also failed restoration near1GiB AS because
  the harness retained about361MiB of adjacent historical JSON reports as
  application state. Recovery report
  `activity-rank-recovery-v12-20261126-r01.json` verifies all five DB quick
  checks/row counts and the original1M digest, then removes the temporary
  restore snapshot. Snapshot JSON now covers only the runtime root and
  isolated command-source tree; report evidence is neither copied nor erased.
  This corrected driver and a new runtime migration justify the single final
  after, without another baseline. Traffic/oracles match, but driver snapshot
  scope and memory footprint differ; OS warmth/GC/scheduling are uncontrolled.
  Business seed fields match; manifest hashes differ only through
  `xiuxian/compatibility_hits.json` diagnostic startup counters and derived
  `seed_sha256`. Do not claim byte-identical full manifests or attribute all
  E2E/RSS changes to indexes.
- Consolidated affected regression:91 tests and19 subtests pass9.76s,
  including populated001-004 ->005 migration/idempotence/ownership, published
  checksums, query plans, warm-public-cache/live-personal changes and harness
  restore/report-preservation/oracle/phase-selector boundaries. Compile,
  architecture and inventory checks pass on a clean code-only checkout so
  the protected legacy JSON deletion stays untouched. Old green full shards
  and V10/V11/serial matrices are not repeated; exact delivered SHA CI is the
  full-suite delivery gate. The repository performance goal remains active,
  including remaining contention/loop stalls and unverified sustained50/s.

## V13: Timestamp Connection Reuse and Live Rank Ranges (2026-11-27)

- Continue from V12 SHA `7480dc3071e70dbd5884a6640dc1783cf4326857`, whose
  exact-SHA CI `45858612953` passed quality and both unittest shards. Reuse
  the accepted V12 final after as this batch's before; no baseline, old
  workload or full local shard rerun. Its runtime tree matches that committed
  SHA. The frozen harness, fixture identity, phase specs and every request,
  actor, argument and setup oracle are identical. Only four runtime Python
  files change; the after tree is
  `a2c7791ff8fbdfce6b5bc083906c8de99ac01d7fab0138fcc88ded34e2eb85d2`.
- Timestamp writes opt in to one idle main-thread connection, sharing the
  existing owner's lifecycle and error/identity checks. Every request still
  performs its own `BEGIN IMMEDIATE`, live UPDATE and COMMIT; no timestamp
  result cache, batching or delay. Threads and reentrant calls use independent
  handles, explicit request scopes retain their own lifetime, and shutdown
  closes the owner. Missing databases cannot be recreated by an open race.
  Active retained writers check path/inode before either automatic or explicit
  commit; mismatch discards the connection and rolls back the pending write.
- Live sign/boss standings use disjoint indexed range COUNTs instead of
  evaluating the row-wise CASE predicate across every positive participant.
  Tie order, positive-participant total, missing/nonpositive viewers and live
  values remain exact in one transaction. Counts still visit qualifying index
  entries; this is not constant-time ranking. Existing bounded 60-minute public
  display snapshots and live eligibility/reward decisions are unchanged.
- Reports in the sole retained 1M fixture's `data/` directory:
  `activity-rank-after-v12-20261127-r02.json` (before),
  `activity-rank-after-v13-20261127-r01.json` (one after), and
  `activity-rank-comparison-v13-20261127-r01.json` (extraction). Three phases
  dispatch128 real matcher/DI/handler requests each, sharing one instance and
  isolated DB set: pure cap10/offered20/s, mixed cap20/35/s, contention
  cap20/50/s. Each phase has12 sign-board,12 boss-board,12 boss-display and56
  profile requests; relationship fillers and32 real distinct-user training
  writes in each mixed/contention phase complete the same384-request plan.
  Exp and stone both change in training. The existing8,192 sign/32,768 boss,
  8,192 avatar/title and4,096 mentor-application rows are retained; no new
  fixture copy or empty-feature-table extrapolation. Activity sign/damage
  writes and activity index maintenance cost remain unmeasured.
- Both versions have384 successes, zero business/cooldown refusals, system
  exceptions, timeouts, boundary violations and SQLite busy/locked errors.
  Every phase's balance/item/receipt/live-rank/huge-number/activity invariant
  passes, including32 fresh training receipts in each write phase. All tasks
  drain before restoration; original1M rank digest restoration passes and
  equals the before digest. Business manifest fields match; only
  `xiuxian/compatibility_hits.json` diagnostic counters and their derived
  `seed_sha256` differ. This is seven sampled paths, not all674 matchers,
  background jobs, activity mutations or a sustained-capacity qualification.

E2E includes producer, event-loop and admission queueing. Each latency cell is
p50/p95/p99 in milliseconds, V12 -> V13. With n12, p95/p99 are the maximum;
relationship n2 is only a sanity sample. All listed path error rates are0.

| Phase / path | n | E2E p50/p95/p99 ms before -> after | SQL calls before -> after | SQL wall ms before -> after |
| --- | ---: | --- | ---: | ---: |
| pure / sign board | 12 | 17.489/64.403/64.403 -> 15.605/82.205/82.205 | 199 -> 199 | 77.797 -> 73.539 |
| pure / boss board | 12 | 17.876/23.502/23.502 -> 16.850/24.604/24.604 | 198 -> 198 | 78.510 -> 82.099 |
| pure / boss display | 12 | 8.989/23.588/23.588 -> 8.949/24.033/24.033 | 55 -> 55 | 4.923 -> 5.177 |
| pure / mentor | 18 | 20.690/33.577/33.577 -> 20.252/33.174/33.174 | 524 -> 524 | 142.662 -> 146.860 |
| pure / partner | 18 | 15.825/32.465/32.465 -> 15.400/24.905/24.905 | 342 -> 342 | 100.103 -> 92.994 |
| pure / profile | 56 | 24.052/39.934/114.814 -> 19.067/29.814/132.599 | 2685 -> 2520 | 427.040 -> 211.571 |
| mixed / sign board | 12 | 240.658/378.350/378.350 -> 57.453/284.492/284.492 | 199 -> 199 | 83.027 -> 73.253 |
| mixed / boss board | 12 | 247.297/319.996/319.996 -> 21.501/51.552/51.552 | 199 -> 199 | 91.849 -> 75.409 |
| mixed / boss display | 12 | 144.524/248.600/248.600 -> 12.459/24.910/24.910 | 55 -> 55 | 6.576 -> 5.043 |
| mixed / mentor | 2 | 48.136/54.239/54.239 -> 36.130/39.234/39.234 | 60 -> 60 | 18.772 -> 20.122 |
| mixed / partner | 2 | 40.124/40.552/40.552 -> 19.939/21.057/21.057 | 38 -> 38 | 10.610 -> 12.847 |
| mixed / profile | 56 | 258.255/375.854/432.113 -> 43.374/180.311/225.100 | 2706 -> 2538 | 702.155 -> 348.886 |
| mixed / training | 32 | 229.037/293.043/301.434 -> 46.827/200.887/256.172 | 1312 -> 1312 | 371.851 -> 361.612 |
| contention / sign board | 12 | 319.199/675.003/675.003 -> 220.694/458.218/458.218 | 199 -> 199 | 82.696 -> 66.259 |
| contention / boss board | 12 | 1086.964/1183.720/1183.720 -> 534.008/709.040/709.040 | 199 -> 199 | 87.649 -> 81.040 |
| contention / boss display | 12 | 1163.911/1296.698/1296.698 -> 610.449/630.741/630.741 | 55 -> 55 | 9.230 -> 8.403 |
| contention / mentor | 2 | 1206.570/1226.358/1226.358 -> 774.496/794.614/794.614 | 60 -> 60 | 16.158 -> 16.909 |
| contention / partner | 2 | 1166.722/1185.709/1185.709 -> 733.799/753.472/753.472 | 38 -> 38 | 10.468 -> 10.759 |
| contention / profile | 56 | 704.313/1275.758/1295.626 -> 464.280/685.716/853.009 | 2690 -> 2531 | 962.545 -> 501.119 |
| contention / training | 32 | 949.854/1243.106/1262.067 -> 451.362/812.258/832.179 | 1312 -> 1312 | 391.034 -> 358.725 |

- Complete timestamp diagnostics:168 live writes still produce168 BEGINs,
  UPDATEs and COMMITs. New writer opens168 ->1; writer SQL1008 ->507,
  wall621.943 ->21.960ms. Journal reads168 ->1,594.926 ->3.864ms; UPDATE
  wall9.071 ->9.558ms actually regresses slightly. Independent commit time
  5.588 ->5.913ms also rises; connection initialization removal, not relaxed
  write durability or fewer business transactions, accounts for this SQL gain.
  All writer statements are captured. Other long-lived diagnostic streams
  truncate577 statements in each write phase; aggregate SQL counters include
  them, so do not claim the entire per-statement trace is complete.
- Sign standing36 SQL calls201.897 ->163.632ms, boss36 calls207.695
  ->193.472ms. These API call counts do not change; pure boss standing
  66.508 ->67.587ms regresses despite the new ranges. Query-plan tests require
  range-index searches and the independent full-sort oracle checks128 viewers,
  every tie field, nonpositive/missing viewers and updates after warm reads.
- Observed success throughput is20.091/34.890/34.532 ->20.088/35.072/39.089/s;
  handler-interval overlap2/9/17 ->2/6/14. Caps10/20/20 and offered20/35/50/s
  are separate from actual overlap and successful throughput. Sustained50
  successful requests/s and20 simultaneously executing handlers remain
  unproven. Contention admission queue p95782.053 ->232.859ms, producer p95
  38.859 ->1.984ms and loop-dispatch p9587.180 ->68.660ms improve. Mixed
  producer p953.130 ->5.076ms regresses. Loop-lag p959.518/75.692/93.907
  ->6.841/40.459/82.614ms improves, but contention loop p99152.603
  ->250.896ms and pure loop p9913.984 ->14.514ms regress. Pure sign/boss
  board/display p95 and profile p99 regress as the table shows. This finite
  pair supports SQL gains and sampled E2E gains, not an unconditional latency
  claim; host warmth, scheduling and request interleavings remain uncontrolled.
- Cache hits/misses48/103,33/118,39/112 ->48/103,33/118,35/116; partial
  hits91/40/45 ->91/36/43, evictions71/86/80 ->71/86/84, waits0/0/2
  ->0/0/0. Explicit invalidations/expiry/bypass/oversized remain0. Maximum
  32 entries/105,819 estimated bytes stays within32-entry/1MiB bounds.
  Per-path cache attribution is unmeasured. Exp/stone generation deltas stay
  0/32/32, so training still globally invalidates generation-keyed ranks;
  unchanged small-strata traffic does not establish worst-case cold-rank or
  batch-update capacity. MainThread CPU2399.239/3064.928/3116.921
  ->2080.703/2680.482/2699.262ms; all BEGIN counts759/855/855 unchanged,
  total BEGIN wall59.689 ->45.934ms. This includes execute overhead and is
  not a measurement of pure transaction-lock waiting. Observer CPU and
  queue/loop samples remain in both reports, included in E2E without subtracting
  instrument overhead.
- Limits unchanged: shared validation flock,2/4 CPUs,1GiB AS,600 CPU seconds,
  1200 wall seconds, `MALLOC_ARENA_MAX=2`,768MiB free-RAM/2GiB disk floors,
  900MiB RSS and256MiB single/total WAL stops. Minimum sampled RAM1742
  ->1569MiB, disk14,833,831,936 ->14,671,863,808 bytes; peak RSS242.613
  ->266.141MiB regresses, virtual memory643.293 ->595.602MiB. Maximum
  single/total WAL18,321,672/25,399,928 bytes unchanged, final WAL0. Process
  CPU62.311 ->60.522s, wall75.454 ->75.466s, final restore13.641 ->13.385s
  include setup/snapshot/restore and are not request costs. Sample-window IO
  reads180,224 ->1,449,984 and writes55,717,888 ->58,290,176 bytes regress;
  full-process reads765,636,608 ->527,409,152 and writes3,792,945,152
  ->3,794,661,376 bytes include restoration. Do not infer a product-memory
  or disk-IO improvement from these process totals.
- Affected consolidated regression:101 tests/291 subtests pass8.45s; the
  subsequently identified commit-before-identity gap has one additional
  focused regression passing with two subtests, covering both commit entry
  points. Compile, architecture (`ok=true/errors=[]`), inventory and whitespace
  pass on the code-only checkout with the six-file code/test whitelist.
  The protected status modification, legacy JSON deletion, local goals and
  inventory helper remain excluded. Exact delivered SHA CI supplies the full
  clean-checkout gate. Overall repository performance goal remains active;
  unresolved loop tails, wider cold-rank/write strata and sustained throughput
  require separately frozen future evidence, not a rerun to select this pair.

## V14: Schema Plan Proofs and Live Display Readers (2026-11-27)

- Continue from `ad5905d61a64751e08d989ddb4454cd7c9a95a21`, whose exact-SHA
  CI `45866105031` passed quality and both unittest shards. The old accepted
  V10-V13 matrices and full shards are not repeated. This batch removes
  repeated rank-plan inspection and cold activity/natal display connections;
  it does not cache personal values or replace exact ranks with TOP slices.
- A rank connection retains at most four plan proofs, exp/stone with/without
  a ceiling, for its current schema version. Each rank transaction lazily
  reads the schema cookie once on its first non-NULL cache miss. A normal
  schema read refreshes SQLite's parsed schema before initial/changed-epoch
  EXPLAIN; EXPLAIN alone can see stale index descriptions after external DDL.
  Both scalar lookups still must SEARCH the correct expression index with
  no temporary sort. Missing/wrong indexes retain one live COUNT fallback;
  metadata/read failures propagate. Target scores, generations and exact
  seek/count remain live in the same transaction, including huge BLOB values.
  Score writes invalidate value ranks but do not invalidate schema proofs.
- Activity personal standings reuse the existing bounded game read owner.
  Natal display reads reuse the existing player read owner and bounded
  schema-version column catalog. Every call still opens its own live read
  transaction and reads its values; no new owner, player-value TTL or gameplay
  change. Error, scope/thread, database replacement and shutdown boundaries
  use the existing owner behavior. Missing natal tables/rows remain absent;
  malformed identity columns/read denial retain the original DomainError.
- New frozen `cold-rank-v14` uses the sole retained synthetic 1M fixture,
  with exactly the V12 request schedule: three phases of128 requests,
  cap10/20/20 and offered20/35/50 arrivals/s. Each phase has56 distinct
  profile viewers (32 writers and24 huge-value readers),12 sign standings,
  12 boss standings and12 boss displays. Relationship fillers and32 real
  `灵石修炼 1000` operations in each mixed/contention phase complete384
  requests. Real exp+100/stone-1000 writes, fixed rank pivots, balances/items/
  receipts and huge-number oracles stay unchanged; all phases share one
  instance and DB set and drain before restoration.
- The new distribution updates4096 existing nonparticipants, never inserts
  users: exp=1,000,000+i*1000, stone=1,000,000,000+i*10,000, with matching
  power. There are4237 exp/4202 stone groups among1M rows, not1M distinct
  scores. These rows lie above writer intervals and are hashed with storage
  types/power after every phase. Natal data now has8192 real display rows;
  the whole bounded table is hashed and all56 profile natal replies per phase
  are checked. Existing8192 sign,32768 boss (8192 selected-key),8192 avatar/
  title,4096 mentor applications and2560 backpack rows remain populated.
  Activity sign/damage mutation and index-write costs remain unmeasured.
- Reports under the retained fixture's `data/` directory are
  `cold-rank-before-v14-20261127-r01.json`,
  `cold-rank-after-v14-20261127-r01.json` and
  `cold-rank-comparison-v14-20261127-r01.json`. One new baseline on unchanged
  V13 was needed for the new distribution; one frozen after followed the
  complete code/test batch. Driver hashes, fixture identity, limits, phase
  specs, every request/actor/argument/oracle and business manifests match.
  Only compatibility diagnostic JSON and its derived seed hash differ.
  Runtime trees are `a2c7791ff8fbdfce6b5bc083906c8de99ac01d7fab0138fcc88ded34e2eb85d2`
  -> `de539ceb8a7acbbb1487a3867281144900f3b34c991ab0c7fb7b5df501a96906`;
  exactly the three runtime files above differ.
- Both versions complete384/384 successes, zero business/cooldown refusals,
  exceptions, timeouts, boundary violations or SQLite busy/locked errors.
  All three phase invariants and original1M digest restoration pass. This is
  seven sampled paths, including two variants of `活动排行`, not exhaustive
  runtime coverage of674 matchers, background jobs or sustained capacity.

E2E includes producer, event-loop and admission queueing. Cells are p50/p95/p99
milliseconds, before -> after. With n12, p95/p99 are the maximum; relationship
n2 is only a sanity sample. Every listed error rate is0. SQL counts include
transaction API calls; aggregate counts/time are complete even when bounded
per-connection diagnostics or per-request top-query lists omit statements.

| Phase / path | n | E2E p50/p95/p99 ms before -> after | SQL calls before -> after | SQL wall ms before -> after |
| --- | ---: | --- | ---: | ---: |
| pure / boss board | 12 | 16.079/22.522/22.522 -> 11.628/17.164/17.164 | 198 -> 174 | 72.513 -> 27.241 |
| pure / boss display | 12 | 9.085/23.637/23.637 -> 9.355/130.585/130.585 | 55 -> 55 | 4.975 -> 5.374 |
| pure / sign board | 12 | 18.207/226.003/226.003 -> 10.901/62.372/62.372 | 199 -> 175 | 81.755 -> 24.736 |
| pure / mentor | 18 | 20.350/29.822/29.822 -> 19.748/31.241/31.241 | 524 -> 524 | 143.855 -> 139.160 |
| pure / partner | 18 | 15.489/27.454/27.454 -> 15.194/27.506/27.506 | 342 -> 342 | 90.166 -> 94.146 |
| pure / profile | 56 | 19.402/147.316/276.783 -> 16.689/89.778/160.750 | 2644 -> 2430 | 281.140 -> 172.391 |
| mixed / boss board | 12 | 21.858/84.479/84.479 -> 11.260/24.716/24.716 | 199 -> 175 | 87.104 -> 27.878 |
| mixed / boss display | 12 | 17.650/81.095/81.095 -> 15.151/89.746/89.746 | 55 -> 55 | 11.187 -> 4.987 |
| mixed / sign board | 12 | 22.616/67.816/67.816 -> 12.065/38.873/38.873 | 199 -> 175 | 79.085 -> 27.972 |
| mixed / mentor | 2 | 200.597/202.371/202.371 -> 27.751/35.898/35.898 | 60 -> 60 | 16.345 -> 16.979 |
| mixed / partner | 2 | 15.148/183.607/183.607 -> 14.541/14.888/14.888 | 38 -> 38 | 10.253 -> 9.697 |
| mixed / profile | 56 | 48.596/128.750/230.116 -> 33.594/89.516/97.438 | 2693 -> 2483 | 521.325 -> 288.876 |
| mixed / training | 32 | 50.421/100.672/257.613 -> 45.957/70.591/71.924 | 1312 -> 1312 | 359.122 -> 335.486 |
| contention / boss board | 12 | 517.285/728.883/728.883 -> 458.247/571.923/571.923 | 199 -> 175 | 87.399 -> 28.020 |
| contention / boss display | 12 | 631.484/675.123/675.123 -> 482.581/502.065/502.065 | 55 -> 55 | 10.636 -> 8.587 |
| contention / sign board | 12 | 234.773/498.983/498.983 -> 249.337/298.275/298.275 | 199 -> 175 | 75.414 -> 35.098 |
| contention / mentor | 2 | 627.555/647.647/647.647 -> 469.812/489.730/489.730 | 60 -> 60 | 17.293 -> 16.562 |
| contention / partner | 2 | 587.416/607.108/607.108 -> 429.603/448.595/448.595 | 38 -> 38 | 12.174 -> 10.983 |
| contention / profile | 56 | 475.175/721.240/756.152 -> 379.516/557.569/574.590 | 2688 -> 2477 | 649.145 -> 532.037 |
| contention / training | 32 | 487.497/726.546/737.622 -> 404.133/567.788/574.588 | 1312 -> 1312 | 347.441 -> 369.215 |

- Rank EXPLAIN98/112/110 ->4/8/8, wall15.392/67.769/93.869
  ->2.715/9.650/20.520ms. Worker SQL376/440/435 ->332/394/391 calls,
  wall80.632/284.310/379.405 ->58.026/149.078/353.300ms. This includes
  the newly required schema cookie/refresh statements. Recursive seek batches
  stay98/112 in pure/mixed and110 ->108 under contention. Live remainder
  COUNTs9/48/47 ->9/48/51; their wall15.475/89.621/128.506
  ->19.264/64.627/142.029ms retains pure/contention regressions. Contention
  seek wall91.758 ->102.549ms and worker CPU159.211 ->163.860ms also regress.
  Different concurrent interleavings change rank-cache reuse; exact oracles
  pass, but proofs do not remove cold remainder scans/global invalidation.
- Activity board game-reader opens13 ->1 per path/phase (other cold reads
  included); combined standing commands remove144 SQL API calls. Profile
  player-reader opens57/56/56 ->1/0/0; the per-call natal cold handle is gone.
  Baseline top-query lists capture all168 repeated natal sqlite_master probes,
  204.453ms; after code removes that query. Top-query lists are bounded, so
  natal SELECT/table-info appearances there are not full execution counts.
  Fresh natal replies/table hashes and live external-change tests verify that
  metadata/handle reuse does not cache player values.
- Success throughput20.094/33.577/40.925 ->20.097/35.131/43.100/s;
  actual handler-interval overlap4/4/15 ->3/2/15 is wall overlap, not parallel
  CPU/SQLite execution. Caps/offered arrivals differ from observed overlap
  and successful throughput. Sustained50 successful requests/s and20 active
  handlers remain unproved. Contention admission p95265.760 ->130.449ms;
  producer p952.870 ->1.986ms. Mixed dispatch p9526.456 ->28.860ms and
  contention dispatch p9986.520 ->95.454ms regress. Loop p95/p99 is
  9.485/23.880 ->5.050/13.065,37.855/62.255 ->36.042/53.509,
  75.531/85.701 ->81.456/106.048ms; contention loop tails regress.
  Pure/mixed boss-display p95, pure relationship tails and contention sign
  median regress as the table shows. Finite sample variation, host warmth,
  observer work and scheduling prevent unconditional E2E improvement claims.
- Shared-cache hits/misses48/103,33/118,35/116 ->103/104,88/119,92/115;
  partial hits91/35/49 ->91/32/42, evictions71/86/84 ->72/87/83,
  waits0/0/1 ->0/0/0. Maximum32 entries/106243 estimated bytes stays within
  32-entry/1MiB bounds; proof storage is separately bounded to four entries
  per existing rank connection. Explicit invalidations/expiry/bypass/oversized
  are0, but exp/stone generation deltas remain0/32/32 in both versions:
  real writes still globally invalidate generation-keyed value ranks.
  Per-command cache attribution is not measured. MainThread phase CPU
  2264.371/2680.263/2549.816 ->2017.903/2460.238/2422.331ms includes
  application and observer work. BEGIN count759/855/855 is unchanged;
  total BEGIN execution wall49.329 ->36.982ms includes overhead and is
  only an upper bound on transaction wait. Observer counters remain in both
  reports and cannot be subtracted from E2E or added to overlapping SQL CPU.
- Resource guards remain the shared validation lock,2/4 CPUs,1GiB AS,
  600 CPU/1200 wall seconds, `MALLOC_ARENA_MAX=2`,768MiB available-RAM and
  2GiB disk floors,900MiB RSS and256MiB single/total WAL stops. No guard
  stops. Minimum sampled RAM1621 ->1728MiB, disk14594179072 ->14446256128
  bytes; peak RSS266.141 ->237.750MiB, virtual622.887 ->620.105MiB.
  Single WAL18321672 bytes unchanged; total25865488 ->25881968 bytes
  regresses slightly, final WAL0. Process CPU66.336 ->65.448s,
  wall78.634 ->78.349s and restore13.391 ->13.549s include setup/snapshot/
  restoration and are not request costs. Traffic-window reads1130496 bytes
  unchanged; writes53202944 ->54067200 bytes regress. Full-window reads
  713527296 ->702418944 and writes3815534592 ->3815653376 include fixture
  IO; do not attribute process RSS/IO differences solely to the product.
- Harness seed/oracle boundaries passed5 direct tests. The consolidated
  affected run had128 green tests/380 subtests plus three new failing DDL
  subcases. The actual parsed-schema refresh defect was fixed without
  weakening those oracles; only seven failed/new/changed-budget methods were
  rerun, passing7 tests/57 subtests in1.27s. Existing green cases are reused.
  Compile, architecture (`ok=true/errors=[]`), inventory and scoped whitespace
  pass on the code-only checkout, preserving the user's legacy JSON deletion.
  Exact delivered SHA CI is the full clean-checkout gate. Overall performance
  goal remains active for wider score strata/activity writes/loop tails and
  sustained throughput; this accepted pair will not be rerun to select numbers.

## V15: Live Impart Reader and Training Writer Reuse (2026-11-27)

- Start from `694f63ae571e534a89f040363d007707298715bd`, exact-SHA CI
  `45877945768` completed/success. Reuse its accepted V14 after report as
  this batch's before; no old baseline, matrix or full shard was repeated.
  Saved traces show168 cold impart connections and64 cold training writers.
  Rank COUNT+seek averages only4.37ms/profile under contention, while even
  the24 huge readers without COUNT have504.8ms p95. Increasing rank seek
  budgets or reusing stale TOP slices has no supporting benefit evidence.
- `ImpartRepository.state(ensure=False)` uses one independent bounded idle
  read owner. Every call still BEGINs, checks the table, queries the earliest
  live player row, validates exact crystal values/optional duplicate agreement
  and commits. `ensure=True` keeps its real writable transaction. No personal
  attributes, balance, eligibility, rewards or target choices are cached.
- Only the stone-training synchronous scope opts plain writable UOWs into
  another single-idle owner, bound to the resolved game database. Each of
  the four independent ledger/check/settlement/finish transactions still
  commits or rolls back immediately. Selection happens at enter; an escaped
  or reused UOW cannot retain scope ownership. Explicit outer/default scopes,
  other databases, attached UOWs, readers, query-only calls, workers and
  reentrant active transactions retain their isolation. Missing databases
  retain the old create path. Existing-file identity is checked before commit;
  errors discard the handle. Two fixed extra handles avoid repeatedly evicting
  the separate timestamp writer; lifecycle shutdown closes both.
- Reports in the sole retained fixture's `data/` directory:
  before `cold-rank-after-v14-20261127-r01.json`,
  after `cold-rank-after-v15-20261127-r01.json`,
  comparison `cold-rank-comparison-v15-20261127-r01.json`.
  Driver, fixture, limits, phase specs, every request/actor/argument/oracle
  and business manifests match. Only compatibility diagnostic JSON and its
  derived seed hash differ. Runtime trees
  `de539ceb8a7acbbb1487a3867281144900f3b34c991ab0c7fb7b5df501a96906`
  -> `fb813f286b6870b920250ca3587389a54b1df17995a9c5f57a18d3b01fee01eb`
  differ in exactly UOW, buff application, impart repository and lifecycle.
  The first launcher omitted `XIUXIAN_MATRIX_LIMITS=1` and was rejected before
  fixture loading/traffic, producing no JSON. The corrected capped launch is
  the single after run; its separate log preserves this setup error.
- Same384 real matcher/DI/handler requests,128 per pure/mixed/contention
  phase, caps10/20/20 and offered20/35/50 arrivals/s. Retain128 actors,
  32 training writers,24 huge readers,32 fixed pivots,4096 distinct-score
  nonparticipants,4237/4202 exp/stone groups in1M existing users,8192 natal/
  avatar/title/sign rows,32768 boss rows and4096 mentor applications. These
  are populated sampled features, not1M distinct scores or exhaustive runtime
  coverage of674 matchers. Activity damage/sign mutation/index maintenance
  remains outside the workload. All phases share one instance/database set,
  drain before restore, and preserve real exp+100/stone-1000 writes.
- Both versions384/384 successful,0 business/cooldown refusals, exceptions,
  timeouts, SQL errors, busy/locked errors or boundary violations. All three
  phase invariants, exact rank/huge-value replies, asset/receipt/nonparticipant/
  natal checks and original1M digest restoration pass. No duplicate reward,
  lost asset update, negative asset or deadlock was observed in this sample.

Cells are E2E p50/p95/p99 milliseconds, including producer/event-loop/admission
queueing, before -> after. Each listed error rate is0. For n12 p95/p99 is the
maximum; n2 relationships are only sanity samples. SQL counts include control
API calls. Full aggregates are distinct from bounded statement diagnostics.

| Phase / path | n | E2E p50/p95/p99 ms before -> after | SQL calls before -> after | SQL wall ms before -> after |
| --- | ---: | --- | ---: | ---: |
| pure / boss board | 12 | 11.628/17.164/17.164 -> 11.608/181.606/181.606 | 174 -> 174 | 27.241 -> 36.272 |
| pure / boss display | 12 | 9.355/130.585/130.585 -> 9.034/24.117/24.117 | 55 -> 55 | 5.374 -> 4.911 |
| pure / sign board | 12 | 10.901/62.372/62.372 -> 10.600/77.211/77.211 | 175 -> 175 | 24.736 -> 27.542 |
| pure / mentor | 18 | 19.748/31.241/31.241 -> 20.182/28.284/28.284 | 524 -> 524 | 139.160 -> 146.007 |
| pure / partner | 18 | 15.194/27.506/27.506 -> 15.341/28.123/28.123 | 342 -> 342 | 94.146 -> 96.185 |
| pure / profile | 56 | 16.689/89.778/160.750 -> 15.627/50.465/132.171 | 2430 -> 2321 | 172.391 -> 141.652 |
| mixed / boss board | 12 | 11.260/24.716/24.716 -> 15.221/48.436/48.436 | 175 -> 175 | 27.878 -> 36.399 |
| mixed / boss display | 12 | 15.151/89.746/89.746 -> 9.447/35.627/35.627 | 55 -> 55 | 4.987 -> 7.617 |
| mixed / sign board | 12 | 12.065/38.873/38.873 -> 11.774/39.376/39.376 | 175 -> 175 | 27.972 -> 22.240 |
| mixed / mentor | 2 | 27.751/35.898/35.898 -> 19.693/24.978/24.978 | 60 -> 60 | 16.979 -> 16.974 |
| mixed / partner | 2 | 14.541/14.888/14.888 -> 14.986/171.988/171.988 | 38 -> 38 | 9.697 -> 10.206 |
| mixed / profile | 56 | 33.594/89.516/97.438 -> 25.883/85.468/94.966 | 2483 -> 2371 | 288.876 -> 266.749 |
| mixed / training | 32 | 45.957/70.591/71.924 -> 43.298/66.008/68.925 | 1312 -> 1219 | 335.486 -> 155.082 |
| contention / boss board | 12 | 458.247/571.923/571.923 -> 208.937/294.114/294.114 | 175 -> 175 | 28.020 -> 29.297 |
| contention / boss display | 12 | 482.581/502.065/502.065 -> 258.066/306.546/306.546 | 55 -> 55 | 8.587 -> 8.332 |
| contention / sign board | 12 | 249.337/298.275/298.275 -> 172.929/241.150/241.150 | 175 -> 175 | 35.098 -> 41.763 |
| contention / mentor | 2 | 469.812/489.730/489.730 -> 226.188/246.096/246.096 | 60 -> 60 | 16.562 -> 16.011 |
| contention / partner | 2 | 429.603/448.595/448.595 -> 185.902/205.206/205.206 | 38 -> 38 | 10.983 -> 11.799 |
| contention / profile | 56 | 379.516/557.569/574.590 -> 203.943/323.989/339.102 | 2477 -> 2370 | 532.037 -> 504.556 |
| contention / training | 32 | 404.133/567.788/574.588 -> 228.613/312.705/351.520 | 1312 -> 1216 | 369.215 -> 183.334 |

- Impart handles56/56/56 ->1/0/0; complete SQL336/336/336 ->227/224/224,
  wall39.583/44.332/57.287 ->5.868/7.163/14.175ms. Diagnostics omit no
  impart statements: CPU40.690/43.599/47.867 ->6.843/7.379/8.160ms.
  Raw open34.866 ->0.131ms and UDF setup4.933 ->0.023ms are separate.
  Each request retains its live table/row reads, with no new value cache.
- Training cold game opens32/32 ->1/0; raw open10.361 ->0.163ms. Complete
  game SQL1184/1184 ->1091/1088, wall324.032/358.807 ->145.856/164.744ms;
  all-database command SQL wall335.486/369.215 ->155.082/183.334ms.
  The journal checks captured in both diagnostics and top queries64 ->1,
  wall233.398 ->3.097ms. Training diagnostics omit576/576 statements before
  and483/480 after; their CPU/BEGIN counts are partial, not all transactions.
  Direct real-settlement tests independently retain four BEGIN/commit pairs
  per success and verify immediate assets plus replay without second debit.
- Successful throughput20.097/35.131/43.100 ->20.097/33.679/46.952/s;
  mixed throughput regresses. Observed handler wall-interval overlap3/2/15
  ->3/2/11 differs from cap10/20/20 and peak in-flight4/4/20 ->4/4/17.
  In-flight includes queued requests; overlap does not prove parallel CPU or
  SQLite execution. Neither sustained50 successes/s nor20 active handlers is
  proved. Contention admission p95130.449 ->0.009ms and dispatch p95/p99
  67.590/95.454 ->54.377/70.554ms improve; producer p951.986 ->3.663ms
  regresses. Loop p95/p99:5.050/13.065 ->9.220/19.172,
  36.042/53.509 ->28.531/54.296,81.456/106.048 ->62.766/76.265ms.
  Pure loop tails and mixed loop p99 regress. Pure boss/sign boards, mixed
  boss board/partner and other medians/SQL times above retain regressions;
  finite one-pair scheduling/host/observer variation forbids an all-path win.
- Global exp/stone generation deltas remain0/32/32, with live exact ranks.
  Remainder COUNT9/48/51 ->9/48/53, wall19.264/64.627/142.029
  ->13.717/59.985/127.049ms. Seek98/112/108 ->98/112/110; mixed/contention
  seek wall49.689/102.549 ->58.909/109.570ms regresses. Plan4/8/8 stays
  bounded, but contention plan wall20.520 ->32.388ms regresses. Rank worker
  wall58.026/149.078/353.300 ->53.997/160.440/346.052ms: mixed regresses.
  No generation, rank algorithm or gameplay change was made to select numbers.
- Shared cache hits/misses103/104,88/119,92/115 ->103/104,88/119,90/117;
  partial hits91/32/42 ->91/32/56, evictions72/87/83 ->72/87/85.
  Waits/expiry/explicit invalidation/bypass/oversized remain0. Maximum32
  entries/106243 estimated bytes before,106235 after, within1MiB; command
  attribution of cache hits is unmeasured. Main-thread CPU2017.903/2460.238/
  2422.331 ->2002.617/2496.018/2186.472ms includes observer work; mixed
  regresses. Observer counters remain in the original reports and cannot be
  subtracted from E2E or added to overlapping SQL worker CPU. Statement wall
  includes instrumentation; no separate exact SQLite transaction-wait claim.
- Guards unchanged: shared lock,2/4 CPUs,1GiB AS,600 CPU/1200 wall seconds,
  `MALLOC_ARENA_MAX=2`,768MiB available RAM/2GiB disk floors,900MiB RSS and
  256MiB single/total WAL stops. No guard stops. Minimum sampled RAM1728
  ->1552MiB, disk14446256128 ->14314532864 bytes; process peak RSS237.750
  ->229.148MiB, sampled virtual620.105 ->645.684MiB (regression).
  Single WAL18321672 bytes unchanged, total25881968 ->25890208 bytes slightly
  regresses; final WAL0. Full-process CPU65.448 ->65.943s, wall78.349
  ->81.597s and restore13.549 ->13.741s regress and include setup/fixture IO.
  Traffic-window reads1130496 ->1257472 bytes, writes54067200 ->55205888
  bytes regress. Full-window reads702418944 ->546439168, writes3815653376
  ->3815243776 bytes include restoration/setup, not just application work.
- One affected consolidated run passes81 tests/31 subtests in17.78s, covering
  the two new boundary suites, original scope/writer/reader/attached/replay/
  precision cases, impart application and real profile alias/fault probe.
  Compile, architecture (`ok=true/errors=[]`), inventory and scoped whitespace
  pass on a code-only checkout, preserving the user's legacy JSON deletion.
  Previously accepted full shards are reused; exact delivered SHA CI supplies
  the full clean-checkout gate. This bounded batch is not completion of the
  repository-wide performance goal; activity mutations, broader score/load
  distributions, loop regressions and sustained capacity remain unproved.

## V16: Live Relationship and Boss Pre-Read Reuse (2026-11-27)

- Start from delivered `20a862240585306452b9ef797283a3cda98ccbb5`, whose
  exact-SHA CI `45885559085` succeeded. This batch adds a frozen workload
  containing actual cooperative activity writes; accepted old matrices and
  full unittest shards were not repeated. Before traces identify38 mentor
  views opening114 handles,38 partner views opening76, and32 boss attacks
  opening128 game handles. The mentor global expiry/list queries cost
  32.258/34.997ms; boss table-list/journal queries cost304.977/342.392ms.
- Remove only the two relationship handlers' internal synchronous connection
  scopes, allowing existing bounded idle game/player owners to survive
  requests. These scopes never made reads across the two managers atomic.
  Batched display-name/realm reads now use the existing live projection owner
  when unscoped, preserving400-ID chunks and earliest-row duplicate handling.
  Explicit caller scopes retain their old ownership and close boundary.
- Count pending mentor applications directly instead of constructing every
  invitation. The same lock, transaction, schema checks, global expiry update,
  exact `expires_at <= now` boundary and resolved timestamps remain. The
  existing unique pending-apprentice constraint makes COUNT equivalent to the
  old dictionary length. Two nonunique partial indexes cover pending expiry
  and mentor/creation-time ranges. First index creation occurs during fixture
  setup, outside traffic; its latency and extra create/accept/reject/expiry
  write-maintenance cost are not benchmarked. Lifecycle, slot reuse and both
  expiry/COUNT failure rollback are covered by direct tests.
- Boss pre-settlement reads reuse the same existing idle projection owner.
  Each call still starts and commits its own live read transaction and checks
  schema, attributes/name, HP, inventory and daily quota. Real settlement,
  operation ledger and their three cold writer handles per attack remain
  unchanged. No new owner or value cache, stale balance/eligibility/reward
  decision, rank algorithm, TOP-slice substitution or gameplay change.

Reports under the sole retained fixture's `data/` directory:

- Before: `activity-write-before-v16-20261127-r02.json`.
- After: `activity-write-after-v16-20261127-r01.json`.
- Offline comparison: `activity-write-comparison-v16-20261127-r01.json`.
  Driver,1M fixture identity, limits, phase specs, all requests/actors/arguments
  and business manifests match. Only compatibility diagnostic JSON and its
  derived seed differ. Runtime trees
  `fb813f286b6870b920250ca3587389a54b1df17995a9c5f57a18d3b01fee01eb`
  -> `e6db5c3c51600993e5f69e61e81f7a29649dc8ed53e3c8f7f10d70fa60bc4909`
  differ exactly in activity/profile repositories and partner/transaction
  service. Before-r01 stopped on harness invariants, not a product failure:
  OneBot CR versus ledger LF and a legitimate `feature:activity` diagnostic
  increment. It is excluded from performance evidence. The repaired oracle
  permits only the exact planned counter increment, protects unrelated keys
  and business JSON, and normalizes reply newlines. Its11 tests/10 subtests
  passed before freezing r02; no driver or fixture edits followed.

The executable entry is `scripts/mixed_command_probe.py --scenario
activity-write-v16 --fixture <retained-1M-fixture> --source-repo <repo>
--report <new-report.json>`, launched with `MALLOC_ARENA_MAX=2` and
`XIUXIAN_MATRIX_LIMITS=1`. It owns the shared validation lock and all guards;
do not nest another lock or overwrite reports.

- Each version executes448 real matcher/DI/handler requests: pure128,
  mixed160, contention160, caps10/20/20, offered20/35/50 arrivals/s.
  Each phase retains56 profiles and12 each boss-board/boss-display/sign-board
  reads. Pure has18 mentor+18 partner views; mixed/contention have10+10,
  32 real stone-training writes and16 actual cooperative boss attacks.
  Training changes exp+100 and stone-1000; boss damage10 or11 derives from
  live actor attributes. All requests in a phase share one instance/DB set;
  requests, background workers and message writes drain before restore.
- Retain128 actors,32 training writers,24 huge readers,32 fixed pivots,
  4096 distinct-score nonparticipants,4237/4202 exp/stone groups in1M users,
  32768 boss-damage rows across four keys and8192 selected-boss participants,
  8192 historical fight logs,8192 sign/natal/avatar/title rows,4096 mentor
  applications and2560 backpack rows. Sixteen distinct boss writers exclude
  profile readers and TOP15; checked score gaps prove these attacks do not
  move any personal or public board rank. Historical logs have an old date;
  their damage is not claimed to reconstruct current totals.
- Both versions448/448 successful,0 business/cooldown refusals, exceptions,
  timeouts, SQL errors, busy/locked errors or boundary violations (0% each).
  All three phase invariants and original1M digest restoration pass. Checks
  retain exact ranks/huge integers, exp/stone/item balances, nonparticipants,
  natal data and JSON protections, plus exact damage/HP/log/receipt/ledger
  deltas. No lost update, duplicate debit/reward, negative asset or deadlock
  was observed. Human HP display rounds via `number_to`; precise HP is
  validated against SQL receipts and ledger, not inferred from display text.

All cells are E2E p50/p95/p99 milliseconds including producer, dispatch and
admission queues, before -> after. Each row has0% error/refusal/timeout.
For n10/n12/n16, p95/p99 equal the maximum: these are small tail samples,
not exhaustive runtime coverage of674 matchers or all feature populations.
SQL calls include control APIs; statement wall includes instrumentation.

| Phase / path | n | E2E p50/p95/p99 ms before -> after | SQL calls before -> after | SQL wall ms before -> after |
| --- | ---: | --- | ---: | ---: |
| pure / boss board |12|11.292/17.366/17.366 ->11.529/17.555/17.555|174 ->174|27.015 ->29.019|
| pure / boss display |12|9.350/23.083/23.083 ->9.068/23.638/23.638|55 ->55|5.168 ->4.835|
| pure / sign board |12|10.878/244.718/244.718 ->10.847/76.815/76.815|175 ->175|19.870 ->25.909|
| pure / mentor |18|21.459/33.826/33.826 ->12.412/21.950/21.950|524 ->488|153.443 ->38.811|
| pure / partner |18|16.505/26.276/26.276 ->8.674/19.753/19.753|342 ->270|102.354 ->15.419|
| pure / profile |56|16.058/128.915/293.560 ->15.816/24.404/127.352|2321 ->2321|139.515 ->140.447|
| mixed / boss attack |16|60.971/299.325/299.325 ->46.904/216.642/216.642|817 ->785|608.388 ->340.290|
| mixed / boss board |12|27.559/302.877/302.877 ->13.465/165.730/165.730|175 ->175|31.960 ->30.263|
| mixed / boss display |12|136.269/240.244/240.244 ->18.452/101.669/101.669|55 ->55|13.207 ->6.698|
| mixed / sign board |12|22.607/101.727/101.727 ->13.499/247.636/247.636|175 ->175|25.376 ->25.768|
| mixed / mentor |10|43.655/214.947/214.947 ->13.389/148.581/148.581|300 ->280|104.175 ->29.414|
| mixed / partner |10|35.686/232.647/232.647 ->8.739/214.986/214.986|190 ->150|64.524 ->7.180|
| mixed / profile |56|64.098/299.460/333.048 ->39.365/194.544/302.811|2377 ->2375|349.668 ->305.693|
| mixed / training |32|61.131/274.669/352.380 ->50.895/192.508/243.947|1219 ->1219|158.123 ->152.407|
| contention / boss attack |16|476.383/843.591/843.591 ->589.457/696.293/696.293|817 ->785|402.271 ->358.609|
| contention / boss board |12|531.437/755.280/755.280 ->586.322/708.428/708.428|175 ->175|31.628 ->32.205|
| contention / boss display |12|758.959/788.154/788.154 ->580.523/615.743/615.743|55 ->55|10.308 ->5.428|
| contention / sign board |12|258.620/362.189/362.189 ->243.696/320.086/320.086|175 ->175|26.214 ->22.580|
| contention / mentor |10|501.370/869.129/869.129 ->534.432/585.907/585.907|300 ->280|88.709 ->36.544|
| contention / partner |10|644.072/782.015/782.015 ->513.480/590.042/590.042|190 ->150|66.230 ->11.170|
| contention / profile |56|440.132/864.868/886.095 ->482.515/685.622/713.382|2360 ->2367|450.526 ->503.085|
| contention / training |32|488.417/862.567/882.480 ->557.415/698.169/710.824|1216 ->1216|167.769 ->161.291|

- Mentor cold handles114 ->38: the38 live count/expiry writers remain cold,
  while the game/player display handles were already warm from other paths.
  Complete command SQL1124 ->1048, wall346.327 ->104.769ms. Expiry SQL
  32.258 ->4.332ms and old list34.997 ->COUNT3.419ms; repeated checks of the
  two new indexes add76 APIs/1.890ms, included in traffic totals, distinct from
  unmeasured first creation. Partner cold handles76 ->0, complete SQL722
  ->570, wall233.108 ->33.769ms. Both relationship diagnostics omit0 statements.
- Boss cold game handles128 ->96; complete game SQL1506 ->1442,
  wall994.189 ->682.269ms. The64 table-list checks still execute, now
  304.977 ->37.569ms. The96 writer journal checks remain, wall342.392
  ->327.055ms; contention alone regresses158.878 ->169.503ms. Boss game
  diagnostics omit17 statements/phase/version; complete aggregates, not
  bounded diagnostic CPU, establish query totals. Three cold writers per
  attack are a remaining hotspot, not optimized here.
- Successful throughput20.081/34.937/40.779 ->20.117/35.154/43.405/s.
  Observed handler wall-interval overlap4/9/18 ->2/8/17, peak in-flight
  6/13/20 ->3/11/20; queued in-flight work and interval overlap are neither
  simultaneous CPU/SQLite execution nor proof of20 active handlers. This
  short pair does not demonstrate sustained50 successful requests/s.
  Admission p95371.885 ->249.805ms under contention; dispatch p95/p99
  86.501/219.736 ->79.817/111.256ms. Contention producer p95/p99 regresses
  5.394/117.894 ->6.215/123.227ms; mixed producer p99 regresses94.358
  ->101.775ms. Loop p95/p99:6.661/15.726 ->3.074/6.072,
  51.656/103.756 ->37.310/57.264,99.348/236.378 ->83.267/249.307ms.
  Contention loop p99 and several medians above regress. Mixed sign-board
  p95 regresses101.727 ->247.636ms. Do not attribute all E2E changes to SQL
  or infer an all-path win from this single pair.
- Exp and stone generation deltas remain0/32/32. COUNT remainder calls
  9/54/49 ->9/52/56, wall10.370/105.025/100.080 ->15.107/76.572/120.602ms;
  contention seek wall97.407 ->134.300ms and rank worker wall293.432
  ->355.322ms regress. Plan calls4/8/8 stay bounded; their contention wall
  13.842 ->16.314ms regresses. Profile contention SQL wall also regresses
  as shown. This batch leaves global invalidation and exact-rank computation
  unchanged and preserves these costs for subsequent evidence-based work.
- Cache hits/misses103/104,88/119,94/113 and evictions72/87/81 are unchanged;
  partial hits91/31/51 ->91/36/34. Waits, expiry, explicit invalidation,
  bypass and oversized remain0. Maximum32 entries/106235 ->106240 estimated
  bytes stays within1MiB. These are phase totals, not per-command attribution.
  Request BEGIN durations (including SQL, not a pure lock-wait timer) have
  p95/p99 .018/.066 ->.025/.293, .039/.171 ->.029/.109,
  .047/.280 ->.045/.236ms, with unchanged759/1031/1031 BEGIN counts.
  Main-thread CPU2006.807/3396.123/3164.248 ->1574.812/3021.259/2956.380ms
  includes observer work. Observer timing/bookkeeping remains uncalibrated;
  do not subtract counters from E2E or add overlapping SQL/observer CPU.
- Same shared lock,2/4 CPUs,1GiB AS,600 CPU/1200 wall seconds,
  `MALLOC_ARENA_MAX=2`,768MiB available RAM/2GiB disk floors,900MiB RSS
  and256MiB single/total WAL stops; no guard stops. Minimum sampled RAM1535
  ->1586MiB; disk14252490752 ->14118420480 bytes. Peak RSS236.641
  ->232.316MiB; virtual646.555 ->647.336MiB and total WAL25840768
  ->26075608 bytes regress; single WAL18321672 unchanged, final WAL0.
  Full-process CPU72.185 ->69.371s, wall86.680 ->84.970s and restore13.800
  ->13.486s include setup/restoration. Traffic-window reads1208320 ->1298432
  bytes regress; writes67055616 ->64974848 bytes improve. Full-window IO
  includes snapshots/fixtures: reads1415983104 ->803561472 and writes
  4302802944 ->4303421440 bytes, not an application-only cost measurement.
- New harness tests11/10 subtests pass once; consolidated affected regression
  116 tests/31 subtests passes in15.17s, including live external updates,
  authorizer failure/recovery, caller scopes, real cooperative/item settlement,
  lifecycle/replay/protection, rollback, index plans and attached UOW behavior.
  Compile, architecture (`ok=true/errors=[]`), inventory and scoped whitespace
  pass on the code-only candidate. Archive initially omitted export-ignore
  manifests/Git metadata; only that temporary validation environment was
  repaired, preserving the user's deleted JSON and other files. Accepted
  old full shards are reused; exact delivered SHA CI is the clean full gate.
- This bounded batch does not complete the repository-wide performance goal.
  Cooperative write/index costs are sampled; sign, item, world-boss and reward
  load writes remain unmeasured (item settlement has direct functional tests).
  Mentor index-maintenance/first-build cost, other score distributions,
  rank/loop tail regressions, broader commands and sustained capacity remain
  explicit gaps. The before-r01 oracle failure is retained, not counted as
  a second valid comparison or hidden by changing the frozen workload.

## V17: Reuse the Existing Boss Writer (2026-11-27)

- Start from delivered `ab1380863cb5e62b25933449e75e0e43ea041132`, whose
  exact-SHA CI `45898400343` succeeded. Reuse the accepted V16 after as the
  frozen before; no old baseline, full shards or traffic stages were rerun.
  Its32 cooperative attacks opened96 cold game writers and spent327.055ms
  on96 journal checks. This is a measured connection-initialization cost,
  distinct from the rank/loop tails discussed in V16.
- Only cooperative/item boss application execution opts into the existing
  bounded plain writer owner shared with stone training. Ledger reservation,
  real settlement and ledger finish remain THREE independent transactions;
  this does not wrap the action in one transaction. A default-off scope option
  lets existing read-only owners survive this internal writer scope, retaining
  V16 live pre-reads. Write owners still respect every caller scope; explicit
  caller scopes and all other activity actions keep their original lifetime.
  Worker/reentrant UOWs retain their independent short connections. No new
  owner, value cache, generic coordinator copy, rank algorithm or gameplay
  change. Core attributes, balances, inventory, quota, HP, CAS and rewards
  retain their live checks and existing transaction/replay semantics.

Reports in the same sole retained1M fixture's `data/` directory:

- Before: `activity-write-after-v16-20261127-r01.json`.
- After: `activity-write-after-v17-20261127-r01.json` (the only new load run).
- Comparison: `activity-write-comparison-v17-20261127-r01.json`.
  Offline assertions confirm identical driver/fixture identities, limits,
  phase specifications, every request/user/argument, business manifests and
  original rank digest. Only compatibility diagnostic JSON and its derived
  seed differ. Runtime hashes
  `e6db5c3c51600993e5f69e61e81f7a29649dc8ed53e3c8f7f10d70fa60bc4909`
  -> `1073ac21a550d57dd4d5a1c7b4cdbeb805fc2affd91c475937478906b22146fd`
  differ only in activity application and database UOW. Driver, fixture and
  request sequence were not edited to obtain this comparison.
- The unchanged V16 scenario executes448 real matcher/DI/handler requests per
  version: pure128, mixed160, contention160; caps10/20/20 and offered20/35/50
  arrivals/s. It retains128 actors,32 distinct training writers changing BOTH
  exp+100 and stone-1000, cold/different-score personal ranks and16 actual
  cooperative boss attacks in each mixed phase. Table populations remain
  1M users,32768 damage rows/8192 selected-boss participants,8192 historical
  fight logs,8192 sign/natal/avatar/title rows,4096 pending mentor applications
  and2560 backpack rows. Boss item inventory is empty in this load: item
  behavior has real functional tests, not a populated-item load claim.
- Both versions448/448 successful;0 business/cooldown refusals, exceptions,
  timeouts, SQL errors, busy/locked errors or boundary violations (0% each).
  All three phase invariants and original1M restoration pass. Exact ranks,
  huge integer values, exp/stone/power deltas, inventory/nonparticipants,
  attributes/features, JSON, boss HP/damage/quota/log/receipt/ledger and
  protected public ranks are checked. No duplicate debit/reward, lost update,
  negative asset or deadlock was observed. Requests and background workers
  drain before stage restoration; response E2E excludes asynchronous message
  persistence, whose drain is included in phase wall time.

Every row below gives E2E p50/p95/p99 milliseconds including producer,
dispatch and admission queues, before -> after. Every row has0% errors,
refusals and timeouts. n10/12/16 tails equal the maximum and are small samples.
These22 phase/path rows cover8 command paths, not all674 matchers; the earlier
28-path serial matrix also remains a stratified sample. SQL includes control
APIs and observed execution/fetch wall time, not SQLite execution alone.

| Phase / path | n | E2E p50/p95/p99 ms before -> after | SQL calls before -> after | SQL wall ms before -> after |
| --- | ---: | --- | ---: | ---: |
| pure / boss board |12|11.529/17.555/17.555 ->11.484/17.834/17.834|174 ->174|29.019 ->27.006|
| pure / boss display |12|9.068/23.638/23.638 ->9.170/23.388/23.388|55 ->55|4.835 ->5.017|
| pure / sign board |12|10.847/76.815/76.815 ->10.724/141.759/141.759|175 ->175|25.909 ->31.732|
| pure / mentor |18|12.412/21.950/21.950 ->12.454/20.883/20.883|488 ->488|38.811 ->39.085|
| pure / partner |18|8.674/19.753/19.753 ->8.876/20.525/20.525|270 ->270|15.419 ->11.288|
| pure / profile |56|15.816/24.404/127.352 ->16.053/92.007/156.029|2321 ->2321|140.447 ->127.542|
| mixed / boss attack |16|46.904/216.642/216.642 ->24.246/192.106/192.106|785 ->641|340.290 ->94.295|
| mixed / boss board |12|13.465/165.730/165.730 ->11.945/275.002/275.002|175 ->175|30.263 ->28.007|
| mixed / boss display |12|18.452/101.669/101.669 ->15.482/46.523/46.523|55 ->55|6.698 ->5.399|
| mixed / sign board |12|13.499/247.636/247.636 ->10.884/42.599/42.599|175 ->175|25.768 ->19.912|
| mixed / mentor |10|13.389/148.581/148.581 ->12.465/82.478/82.478|280 ->280|29.414 ->23.525|
| mixed / partner |10|8.739/214.986/214.986 ->8.625/11.615/11.615|150 ->150|7.180 ->6.833|
| mixed / profile |56|39.365/194.544/302.811 ->22.632/168.703/221.748|2375 ->2371|305.693 ->287.133|
| mixed / training |32|50.895/192.508/243.947 ->36.272/164.153/246.683|1219 ->1219|152.407 ->140.262|
| contention / boss attack |16|589.457/696.293/696.293 ->181.993/357.426/357.426|785 ->641|358.609 ->90.956|
| contention / boss board |12|586.322/708.428/708.428 ->183.629/225.652/225.652|175 ->175|32.205 ->43.733|
| contention / boss display |12|580.523/615.743/615.743 ->205.347/418.734/418.734|55 ->55|5.428 ->9.532|
| contention / sign board |12|243.696/320.086/320.086 ->113.291/143.205/143.205|175 ->175|22.580 ->30.650|
| contention / mentor |10|534.432/585.907/585.907 ->192.201/399.074/399.074|280 ->280|36.544 ->37.157|
| contention / partner |10|513.480/590.042/590.042 ->161.786/326.587/326.587|150 ->150|11.170 ->17.420|
| contention / profile |56|482.515/685.622/713.382 ->161.740/262.550/374.427|2367 ->2361|503.085 ->461.073|
| contention / training |32|557.415/698.169/710.824 ->166.689/339.511/377.239|1216 ->1216|161.291 ->170.638|

- Boss cold game handles96 ->0 over32 attacks: the shared plain writer was
  opened once by training earlier in mixed, not eliminated entirely. Training
  still opens1/0 handles in mixed/contention, with unchanged game SQL1091/1088.
  Boss game SQL1442 ->1154, wall682.269 ->171.033ms; all-database command SQL
  1570 ->1282, wall698.899 ->185.251ms. The96 initialization journal checks
  disappear from boss requests; all live schema/pre-read/settlement statements
  remain. Bounded connection diagnostics omit17 ->161 statements per mixed
  phase/version because more statements now touch one handle; complete SQL
  aggregates establish totals. Do not infer missing business reads from the
  diagnostic truncation. Pure has no boss writes and is an unchanged control.
- Successful throughput20.117/35.154/43.405 ->20.117/35.154/46.143 per second;
  the short contention traffic window3.686 ->3.468s remains below offered50/s.
  Peak in-flight3/11/20 ->4/10/14; observed handler wall-interval overlap
  2/8/17 ->3/6/10. Neither queued requests nor wall overlap proves concurrent
  CPU/SQLite execution,20 simultaneously active handlers or sustained50
  successes/s. Admission p95/p99 under contention249.805/300.454 ->.009/.023ms;
  dispatch p95/p9979.817/111.256 ->53.135/66.273ms. Producer contention p99
  123.227 ->142.000ms regresses. Loop p95/p99:3.074/6.072 ->2.942/22.047,
  37.310/57.264 ->31.097/67.862,83.267/249.307 ->61.544/185.090ms. Pure/mixed
  loop p99 regress. Unchanged pure profile p95 and mixed boss-board p95 also
  regress substantially, as does mixed training p99; retain all rows instead
  of calling this an all-path speedup or attributing all E2E variance to SQL.
- Exp/stone generation deltas stay0/32/32; exact-rank worker calls332/401/393
  ->332/394/387, wall53.745/197.320/355.322 ->45.804/173.883/329.071ms.
  COUNT remainder calls9/52/56 ->9/48/47 reflect scheduling/cache reuse;
  contention COUNT wall120.602 ->144.682ms regresses, seek134.300 ->92.574ms,
  plan16.314 ->19.915ms regresses with unchanged8 plan calls. Rank worker CPU
  149.366 ->165.418ms regresses there. The rank implementation and global
  generation invalidation are unchanged; these totals are not per-request
  latencies and do not justify changing exactness, thread count or TOP slices.
- Cache hits/misses103/104,88/119,94/113 ->103/104,88/119,92/115; evictions
  72/87/81 ->72/87/83, partial hits91/36/34 ->91/32/64. Waits, expiry, explicit
  invalidation, oversized and bypass remain0.32 entries/maximum106240
  ->106235 estimated bytes remain within1MiB; counters are phase totals, not
  per-command attribution. Request BEGIN counts759/1031/1031 are unchanged;
  p95/p99 .025/.293 ->.020/.196, .029/.109 ->.019/.087,
  .045/.236 ->.040/.424ms (contention p99 regresses). BEGIN includes SQL
  execution and is only an upper bound for transaction wait, not a pure timer.
  Whole-phase SQL4129/5998/5923 ->4128/5860/5782 includes background work;
  wall499.787/1215.518/1544.635 ->492.422/914.586/1420.382ms.
- Same shared lock,2/4 CPUs,1GiB AS,600 CPU/1200 wall seconds,
  `MALLOC_ARENA_MAX=2`,768MiB RAM/2GiB disk floors,900MiB RSS and256MiB
  single/total WAL stops; no guard stops. Minimum sampled RAM1586 ->1679MiB,
  disk14118420480 ->13959970816 bytes; peak RSS232.316 ->236.125MiB
  regresses, sampled virtual647.336 ->599.219MiB. Single WAL18321672 and
  peak total26075608 bytes unchanged, final WAL0. Full process CPU69.371
  ->69.480s, wall84.970 ->86.421s and restore13.486 ->13.788s regress; these
  include fixture/setup/restoration, not application throughput. Traffic-window
  reads1298432 ->1687552 and writes64974848 ->66695168 bytes regress.
  Full-window IO reads803561472 ->946081792 and writes4303421440
  ->4304449536 bytes include snapshot IO; do not attribute them to handlers.
  Main-thread CPU1574.812/3021.259/2956.380 ->1709.188/2773.692/2793.792ms
  includes observer work. Observer overhead remains uncalibrated; no subtraction
  of overlapping observer/SQL CPU from E2E is claimed.
- Consolidated affected suite96 cases:95 pass/26 subtests; one new test first
  incorrectly expected a failed ledger to replay instead of retrying via its
  permanent business receipt. Existing `ledger.begin` proves failed requests
  are retryable. Corrected it to require receipt-only recovery, no duplicate
  inventory/log/HP change, applied ledger and subsequent replay; also require
  same-ID retry after settlement rollback. Only those2 changed recovery cases
  reran and passed5.18s; other accepted passes were reused. Product code required
  no fix. Coverage includes three externally visible commits, live external
  attributes/HP/inventory/quota, real coop/item actions, conflict/replay, read/
  settlement/finish failures, scope defaults/nesting/worker/reentrancy and
  training/timestamp/readers/attached-UOW boundaries. Compile, architecture
  (`ok=true/errors=[]`), inventory and scoped whitespace pass in a code-only
  checkout; original protected files/deletion are untouched. Exact delivered
  SHA CI supplies the full clean-checkout gate; old full shards are reused.
- This bounded batch leaves the repository-wide goal active. Item/sign/
  world-boss/reward write loads, mentor index-maintenance/first-build costs,
  broader score distributions, sustained capacity and unsampled command
  populations remain unproved. No rerun was made to select favorable numbers.

## V18: Training Rank Boundaries and Arena Display Reads (2026-11-27)

- Continue from delivered `22fab701b25cde6578fb98cf633a588cc3ea8023` and
  successful exact-SHA CI `45908861000`; old accepted shards and loads were
  reused. The earlier empty-training sample measured20 ATTACH calls at
  80.244ms versus2.873ms for the rank aggregate, supporting removal of
  unnecessary connection setup, not a populated-table scan claim. Arena
  personal information read normalized state twice; honor read state twice
  and sorted TOP1000 twice. These four paths form the bounded runtime batch.
- Hot training personal rank now opens only player DB read-only; cold public
  snapshot plus live rank still uses one attached game/player transaction.
  The same query retains first-row duplicate handling, competition rank,
  valid population and arbitrary-precision comparison. Missing game/player
  files keep their previous fallback. Deliberate dependency reduction: an
  existing but corrupt unrelated game DB no longer blocks a hot player-only
  rank; actual player SQL errors still propagate. No personal rank cache or
  TOP slice substitution was introduced.
- Arena display handlers reuse one freshly normalized request state; honor
  preview/display reuse one live existing TOP1000 ordinal. Standalone cap/
  honor APIs still read fresh data every call. The ranking UOW is read-only,
  and a missing database returns empty without creating it. SQL ordering,
  equal-score ordinal positions and outside-TOP1000=0 remain unchanged;
  this does not turn the old ordinal into an exact competition rank.
  Daily/weekly repair, challenge-used reset ownership, live eligibility,
  balances and reward settlement are unchanged. No new cache, index or pool.

Reports under the sole retained fixture
`/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05/data/`:

- Valid before: `arena-training-before-v18-20261127-r02.json`.
- Only after: `arena-training-after-v18-20261127-r01.json`.
- Offline comparison: `arena-training-comparison-v18-20261127-r01.json`.
  Assertions confirm identical driver/fixture/limits/specifications, every
  request/user/argument, business manifests and original rank digest; both
  original1M restores pass. Only diagnostic compatibility JSON and its derived
  seed differ. Package Python hash `1073ac21a550d57dd4d5a1c7b4cdbeb805fc2affd91c475937478906b22146fd`
  -> `3ad0cc47b2e9a29fb55a92554ca387b5b4ec40d02529a8cf5fbc8a1e2f77ac77`
  changes four runtime files and one feature test, explicitly distinguished
  in the comparison. The harness was frozen before valid before-r02.
- Retained invalid before-r01 stopped after pure:106 successes,10 reply
  mismatches and12 precision exceptions. Harness actors had included protected
  historical REAL scores, and local-date seed disagreed with the business UTC
  date, provoking legitimate precision rejection/day repair. Only the driver
  cohort and UTC seed were corrected and covered by small tests; business
  precision/reset rules were not changed. That failed report is not paired
  performance evidence. Valid before-r02 and after both ran after UTC midnight.
- New minimal scenario executes384 real matcher/DI/handler requests/version,
  three128-request phases. Pure cap10 at20 arrivals/s has56 profiles and18
  each of `历练排行榜`, `历练积分排行榜`, `我的竞技场`, `我的荣誉`.
  Mixed/contention cap20 at35/50 arrivals/s each retain56 profiles,32 real
  stone-training writes changing BOTH exp+100/stone-1000, and10 each target.
  One instance/shared isolated DB serves each phase; all work drains before
  restoration. Original1M users/hash
  `0dd134931a7cb0aa3511cc90d50c3c873b3cea97f576dd74e7e536af577ad8c7`
  remain;8192 arena and8192 training rows reuse existing synthetic owners.
  Training includes ties, nonpositive and >int64 BLOB points; arena includes
  ties and viewers outside TOP1000. Target reads use96 exact-value actors;
  the32 historical REAL actors remain unchanged, not used to evade rejection.
  Other populations include2560 backpack,8192 avatar/title and4096 pending
  mentor rows. Arena/training themselves are not mutated by this traffic;
  their state-operation/honor-receipt tables are empty and checked unchanged.
  This is not a populated honor-history or arena-write workload.
- Both valid versions384/384 successful,0 business/cooldown refusals,
  exceptions, timeouts, SQL errors, busy/locked or boundary violations (0%).
  All phase invariants pass: exact replies including every public TOP50 name/
  value/order and personal rank, full arena/training/state-operation hashes,
  honor receipts, live exp/stone/power changes, items/nonwriters, attributes,
  JSON and permanent write receipts. No duplicate debit/reward, lost update,
  negative balance or deadlock was observed. Message persistence drains within
  phase wall time; response E2E does not include that asynchronous persistence.

Every row gives E2E p50/p95/p99 milliseconds including producer/dispatch/
admission queues, before -> after. All rows have0% errors/refusals/timeouts;
n10/18 tails are small-sample maxima. These17 phase/path rows cover6 commands,
not all674 matchers or41 background entries; prior28-command coverage remains
a stratified sample. SQL counts include control APIs; wall time includes
execute/fetch and observer effects, not SQLite engine time alone.

| Phase / path | n | E2E p50/p95/p99 ms before -> after | SQL calls before -> after | SQL wall ms before -> after |
| --- | ---: | --- | ---: | ---: |
| pure / arena honor |18|63.036/227.398/227.398 ->39.517/189.520/189.520|846 ->558|896.102 ->551.935|
| pure / arena info |18|14.696/212.937/212.937 ->11.848/208.830/208.830|488 ->344|63.135 ->41.424|
| pure / profile |56|25.173/275.847/339.718 ->15.744/159.884/188.040|2264 ->2256|154.788 ->121.645|
| pure / training points rank |18|50.360/288.881/288.881 ->45.626/258.097/258.097|345 ->311|748.009 ->781.018|
| pure / training completed rank |18|60.638/222.792/222.792 ->42.553/139.747/139.747|346 ->313|803.531 ->695.436|
| mixed / arena honor |10|266.058/577.576/577.576 ->110.469/166.380/166.380|470 ->310|477.590 ->293.401|
| mixed / arena info |10|272.191/557.058/557.058 ->116.165/194.739/194.739|272 ->192|37.782 ->27.198|
| mixed / profile |56|272.430/561.061/637.531 ->137.350/219.497/272.533|2266 ->2271|284.368 ->279.612|
| mixed / stone training |32|279.764/646.926/668.718 ->135.498/194.048/199.014|1219 ->1219|180.688 ->194.270|
| mixed / training points rank |10|268.557/616.725/616.725 ->152.323/219.898/219.898|193 ->175|439.115 ->374.660|
| mixed / training completed rank |10|272.913/522.090/522.090 ->148.415/190.666/190.666|194 ->177|421.977 ->418.264|
| contention / arena honor |10|973.177/1745.609/1745.609 ->827.465/1302.375/1302.375|472 ->310|497.769 ->260.709|
| contention / arena info |10|992.746/1765.492/1765.492 ->847.714/1301.047/1301.047|270 ->192|37.136 ->22.483|
| contention / profile |56|780.967/1651.357/1885.039 ->724.809/1328.553/1400.007|2260 ->2262|315.880 ->336.370|
| contention / stone training |32|905.313/1825.744/1845.685 ->774.067/1358.685/1378.607|1216 ->1216|166.163 ->162.741|
| contention / training points rank |10|907.218/1783.577/1783.577 ->933.273/1320.599/1320.599|193 ->175|434.119 ->375.179|
| contention / training completed rank |10|922.442/1804.065/1804.065 ->749.251/1339.241/1339.241|194 ->177|462.633 ->443.304|

- Across38 requests/path, arena info player handles76 ->38, player-DB SQL764 ->462,
  wall129.265 ->80.845ms; honor handles152 ->76, player-DB SQL1294 ->684,
  wall1703.417 ->942.616ms. State SELECTs76 ->38 in each; honor TOP1000
  sorts76 ->38, wall1486.397 ->866.644ms. Each training path game handles/
  ATTACH38 ->3, retaining one cold combined read/phase;35 hot requests move to
  player-only handles. Both training paths together ATTACH76 ->6,
  wall365.334 ->30.146ms; DETACH wall22.080 ->1.634ms. The live training
  aggregate still scans8192 rows per request and remains a material blocking
  cost. Diagnostic omission counts are0 for these targets. Total SQL and
  E2E reductions are not identical: pure points SQL wall regresses, as do
  mixed training SQL and contention profile SQL; contention points median
  also regresses. This is one frozen pair, not proof every call is faster.
- Short-window successful throughput19.968/30.972/29.866 ->
  20.015/33.765/33.498 per second; contention window4.286 ->3.821s, well
  below sustained50 successes/s. Peak in-flight7/20/20 ->6/8/20; observed
  handler wall overlap4/13/16 ->3/6/15. Offered rates, queued requests and
  overlap do not establish20 simultaneously executing CPU/SQLite requests.
  Admission contention p95/p991187.118/1264.072 ->695.951/798.322ms;
  dispatch118.466/133.801 ->79.800/177.638ms (p99 regresses). Producer
  contention p95/p992.675/3.973 ->20.145/120.064ms regresses. Loop p95/p99
  59.481/114.307 ->56.326/80.469,78.757/85.943 ->57.080/71.330,
  144.885/212.326 ->97.640/226.856ms; contention p50/p99 regress. Keep
  these queue/loop regressions instead of treating offered50/s as capacity.
- Exp/stone generation deltas remain0/32/32; rank worker calls328/348/342
  ->323/353/344, wall59.485/162.339/168.166 ->42.766/163.833/181.810ms.
  Mixed/contention worker wall regresses; contention COUNT2 ->4 calls,
  .733 ->6.278ms and CPU69.006 ->72.500ms regress. Rank algorithm/global
  generation invalidation were not changed. Cache hits/misses316/104,190/118,
  194/114 ->225/105,139/119,143/115; fewer schema accesses accompany the
  removed reads, so hit totals alone are not a hit-rate improvement. Evictions
  72/86/82 ->73/87/83, partial hits91/38/48 ->91/38/36; waits/expiry/explicit
  invalidations/oversized/bypass0. End32 entries, maximum76950 ->76949
  estimated bytes under1MiB. Counters are phase aggregates, not per-command.
  BEGIN counts882/938/938 ->828/908/908; p95/p99 .022/.084 ->.019/.264,
  .031/.277 ->.037/.280,.037/.235 ->.036/.217ms. BEGIN includes execution
  and only bounds transaction wait; pure p99/mixed both regress. Whole-phase
  foreground+background SQL4926/5186/5182 ->4424/4946/4914,
  wall2902.975/2162.097/2282.873 ->2431.713/1917.123/1933.185ms.
- Unchanged shared lock,2/4 CPUs,1GiB AS,600CPU/1200wall seconds,
  `MALLOC_ARENA_MAX=2`, RAM768MiB/disk2GiB preflight floors, RSS900MiB and
  single/total WAL256MiB stops; no guard fired. Minimum traffic-phase sampled
  RAM1564 ->1580MiB/disk13958144000 ->13817622528 bytes; pretraffic available
  RAM1887 ->1543MiB. Full-process peak RSS
  239.223 ->232.145MiB; sampled/final virtual peak626.652 ->596.992MiB.
  Single WAL18321672/total27006728 bytes unchanged; final WAL0. Process CPU
  71.147 ->67.432s, wall82.190 ->80.014s, restore13.670 ->13.456s include
  setup/snapshot IO, not application throughput. Traffic-window IO reads
  925696 ->1933312 and writes54112256 ->56868864 bytes regress; final
  cumulative reads911450112 ->990662656/writes4265664512 ->4267151360
  include restoration. Main-thread CPU4343.423/3610.167/3735.985 ->
  3672.503/3229.855/3307.690ms includes observer work; phase process CPU
  5.105/4.439/4.524 ->4.527/3.979/4.022s. Observer overhead remains
  uncalibrated and is not subtracted from request latency.
- One consolidated affected suite144 passed/43 subtests in12.70s; seven
  already-passed driver cases reused. New cases cover pure helper boundaries,
  live wrappers, day/ISO-week repair without resetting daily-used, state/rank
  query reuse, readonly/missing-file boundaries, duplicate/tied/huge ranks and
  actual authorizer failure/recovery. Handler unit tests execute real function
  bodies with mocked delivery; the paired load supplies real matcher/DI
  coverage. Two bounded independent read reviews found no blocker. Compile,
  architecture (`ok=true/errors=[]`), inventory and scoped whitespace pass in
  a code-only acceptance tree; original status/deletion remain untouched.
  No old full suite or load was repeated; exact delivered-SHA CI is the clean
  complete gate. This batch leaves repository-wide performance goal active:
  training full aggregates, remaining honor sorts, arena-write/history loads,
  unsampled commands and sustained capacity still require separate evidence.

## V19: Shared Read Stages Across Callers (2026-11-27)

This batch follows shared query patterns across modules, using the existing
674-matcher inventory and SQL/call-chain evidence. It adds no personal-value
cache, generic table/column interpolation, reward unification or connection
pool. V18 is delivered at `f070512f3003f0826ed494098dac343929fd2792`;
its exact-SHA CI45917262107 passed and its accepted gates were not repeated.

| Shared entry and actual callers | Removed database work | Live/transaction boundary |
| --- | --- | --- |
| `get_players_attributes`: 8 direct caller functions across PvE/PvP/BOSS, arena, dungeon, rift, buff and an old transaction adapter | Default Buff reads 2 -> 1/player; natal normal awakened reads 4 -> 1, unawakened 2 -> 1 | Only synchronous assembly reuses the row. Custom provider ABI and explicit rift readonly providers remain. Missing natal records/columns are repaired then actually reread; read failures propagate. The old adapter's active status is unproved. |
| `final_user_data` / `get_player_data`: 2 direct dynamic-player consumers (training puppet and heart-devil tribulation) | Reuse supplied full profile, 4 local Buff readers -> 1; whole chain 5 -> 2. Non-boss unused impart read removed (2 -> 1 across chain) | Boss-specific impart remains live; no reuse across requests/writes. Eight battle functions and two consumers are static function counts, not runtime command coverage. |
| Refining preflight (1 command), fusion (2), automatic listing (1) | Refining normal 4/worst 7 whole-bag reads -> 1, main skill 2 -> 1; fusion builds one goods index, equipment checks consume already-read rows; automatic listing removes K equipment point reads | Helpers retain standalone live fallback and distinguish empty supplied rows. Material order, equipped reservations, fees, ledger/CAS, replay and authoritative transactional checks remain. Fusion's pre-existing incomplete transactional equipment-state recheck is not expanded into a concurrency guarantee. |
| Xianshi minimum price: automatic and fast listing (2 commands, automatic overlaps preceding row) | K readonly scopes/complete listing materializations -> one readonly scope, <=256-name chunks, `id,name,price` projection and 128-row streaming | Every matched price still receives exact integer validation, including nonminimum bad REAL values; no SQL MIN shortcut, unmatched-price rejection or decision cache. Missing files/tables remain uncreated. |
| `buffinfo_` (my skills, 1 command) | Nonempty five-slot display 9 Buff reads -> 1 (empty slots 5 -> 1) | One synchronous reader supplies slot names and description IDs; next request reads fresh. No write or await crosses that reuse stage. |

Inventory/price rows cover five distinct command surfaces after overlap, plus
one skill command; these cannot be added to battle function counts as matcher
coverage. The inventory's 13 positively linked battle matchers are static
reachability only, with dynamic-call uncertainty retained. Equipment display
already passed backpack rows at V18 and is an unchanged control, not a new
V19 N+1 saving. Fusion's artificial duplicate-row test checks indexing and
first-row/accumulation behavior; it does not prove equivalent equipped-state
semantics for inconsistent data bypassing the normal unique(user,goods)
migration. Simple exp/stone domain helpers and distinct settlements remain.

### Frozen Representative Pair

- Reused the sole retained 1M synthetic fixture at
  `/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05` and its rank digest
  `0dd134931a7cb0aa3511cc90d50c3c873b3cea97f576dd74e7e536af577ad8c7`.
  Before code was archived from V18 before edits, without another large DB.
  Valid reports in `data/` are `shared-query-before-v19-20261127-r03.json`,
  `shared-query-after-v19-20261127-r01.json` and offline
  `shared-query-comparison-v19-20261127-r01.json`.
- Runtime source hashes `3ad0cc47b2e9a29fb55a92554ca387b5b4ec40d02529a8cf5fbc8a1e2f77ac77`
  -> `5908632b5f5b2c66c151112bff69a956039fe6350fde372323f3ee9c10134e98`
  differ in exactly the ten scoped runtime files. Driver hashes, 1M manifest,
  limits, phase specs, every request and business seed/phase manifests match.
  Only diagnostic compatibility-hit JSON and its derived seed digest are
  excluded from business equality. Original digests/restoration match.
- Both versions issue384 real matcher/DI/handler requests in three128-request
  phases: pure cap10 at20 arrivals/s, mixed cap20 at35/s, contention cap20
  at50/s. Each retains56 exact personal profiles; skills/equipment receive
  36 each in pure and20 each in both mixed phases; each mixed phase also has
  32 real distinct-user training writes, exp+100 and stone-1000. One instance
  and isolated DB serve all users in a phase; drain and invariants precede
  snapshot restore. The QQ transport alone is mocked.
- New feature data fills all five skill slots and eight equipment rows per
  actor (128 actors), two equipped slots, quantity20..22 and bound0..1.
  Actual full tables contain2688 backpack and128 BuffInfo rows; all rows and
  columns are hashed in <=32768-row bounded streams. Retained populations
  include8192 avatar/title rows each,4096 pending mentor applications,
  8192 activity users and32768 rows each for point/pass/boss balances.
  Those activity tables are not the targets here. Neither automatically
  listing, refining/fusion actions nor actual battle latency is measured by
  this pair: their evidence is query-count/behavior/transaction regression.
- First before-r01 failed the new oracle's forward-envelope decoding; r02
  then mistook ordinary `[CQ:at,...]` text for a Python list. Both stopped
  after pure128, restored1M successfully and remain INVALID benchmark
  reports. Only bounded known transport envelopes/CQ text were fixed, not
  product output, workload, cooldowns or success criteria. All128 captured
  replies from each were reclassified offline before r03; ten focused oracle
  cases passed. These failures are retained, not counted as before evidence.
- Valid pairs are384/384 successful each, zero business/cooldown refusals,
  exceptions, timeouts, SQL errors, busy/locked or boundary failures (0%).
  Exact profile/rank responses, exp/stone/power changes, generation increments,
  durable receipts/ledger identity, unchanged nonwriters/inventory/BuffInfo/
  other attributes/features/JSON and original1M restoration all pass. No
  duplicate debit/reward, negative balance, lost update or deadlock observed;
  the new equipment/skill actors are unique within each phase, not a new
  same-inventory writer-contention proof.

E2E includes producer, dispatch and admission queues; milliseconds below are
p50/p95/p99 before -> after. All rows have0% errors/refusals/timeouts.
SQL includes control APIs and execute/fetch observer effects. Eleven rows
cover four commands, not all674 matchers/41 background entries. Small n20
tails are maxima, not stable population p99 estimates.

| Phase / path | n | E2E p50/p95/p99 ms before -> after | SQL calls before -> after | SQL wall ms before -> after |
| --- | ---: | --- | ---: | ---: |
| pure / equipment control |36|8.632/9.492/16.471 ->8.544/10.484/16.949|432 ->432|21.882 ->23.126|
| pure / profile |56|16.004/24.473/111.426 ->15.799/22.560/107.479|2259 ->2256|121.354 ->124.179|
| pure / skills |36|9.825/12.877/61.043 ->8.531/14.214/38.243|723 ->435|36.991 ->28.114|
| mixed / equipment control |20|8.533/34.253/85.187 ->8.768/28.042/34.494|240 ->240|14.290 ->15.927|
| mixed / profile |56|20.858/82.385/105.148 ->20.462/84.546/103.723|2266 ->2266|174.279 ->162.256|
| mixed / skills |20|9.458/25.212/29.144 ->8.157/15.291/40.626|400 ->240|15.856 ->17.549|
| mixed / stone training |32|36.847/68.851/76.253 ->36.107/68.737/74.903|1219 ->1219|167.356 ->158.192|
| contention / equipment control |20|121.841/197.282/207.522 ->298.759/401.337/430.084|240 ->240|22.647 ->23.002|
| contention / profile |56|153.383/209.150/223.264 ->325.666/401.138/474.541|2268 ->2265|339.730 ->314.056|
| contention / skills |20|126.197/164.331/179.412 ->292.984/364.342/421.462|403 ->240|36.461 ->22.746|
| contention / stone training |32|146.205/192.894/198.168 ->322.756/389.263/449.340|1216 ->1216|180.994 ->192.646|

- Direct skill SQL confirms684 ->76 Buff SELECTs across76 requests:608
  repeated reads removed, exactly9 ->1/request, no omitted diagnostic
  statements/touches. Total skill SQL1526 ->915 includes three fewer setup
  PRAGMAs; wall89.308 ->68.409ms. Mixed skill SQL wall and p99 nevertheless
  regress, as do pure skill p95 and several unchanged-control measurements.
  Whole-phase foreground+background SQL3988/4722/4679 ->3697/4561/4500,
  wall399.224/599.854/866.636 ->386.644/583.035/832.448ms. This supports
  removed IO, not a claim every path or every latency improved.
- Successful short-window throughput20.130/35.157/49.623 ->
  20.128/35.191/47.761/s; contention load window2.579426 ->2.680008s.
  Peak admitted in-flight3/4/11 ->2/4/20; observed handler-wall overlap
  2/2/8 ->2/2/13. Cap20 and offered50/s do not establish20 simultaneous
  CPU/SQLite executions or sustained50 successful requests/s.
- Competition regression is retained: after MainThread generation2 GC at
  planned offset0.398s lasts143.428ms; before's longest GC is1.561ms.
  After subsequently grows to40 sampled process threads (before19), with
  higher matcher checking/dispatch/admission delays even for unchanged
  equipment/writes. Producer p95/p99 3.121/4.070 ->21.159/121.086ms,
  dispatch40.901/51.739 ->60.768/90.373ms and admission.008/.010 ->
  .256/17.457ms; loop p95/p99 43.590/55.350 ->77.736/195.162ms.
  These correlated observations identify a pause/queue component, not proof
  V19 caused the GC or an exhaustive explanation for the regression. No GC
  suppression, latency subtraction, load reduction or extra timing rerun was
  used. Pure loop p95/p99 also regresses2.476/5.435 ->2.504/7.129ms;
  mixed improves34.710/49.056 ->26.111/45.157ms. Current data cannot prove
  a net contention-latency improvement; diagnosis remains part of the goal.
- Live exp/stone generation deltas remain0/32/32. Rank worker calls
  326/348/350 ->323/348/347, wall39.924/73.423/198.988 ->
  41.190/67.068/170.937ms; pure wall regresses. Contention count2 ->1,
  seeks110 ->112 and plans12 ->8 reflect scheduling/generation overlap,
  not a changed rank algorithm. Cache hit/miss69/99,55/113,57/111 ->
  69/99,55/113,55/113; evictions67/81/79 ->67/81/81, partial hits
  91/30/54 ->91/28/57. Waits/expiry/explicit invalidations/bypass/oversized0;
  end32 entries, maximum33353 estimated bytes under1MiB. Counters are phase
  aggregates, not per-command hits; no public display or exact-rank cache
  semantics changed. BEGIN counts720/848/848 match; p95/p99 .026/.299 ->
  .029/.298, .018/.086 ->.017/.075, .042/.401 ->.043/.335ms. BEGIN
  includes SQL execution and is only an upper bound on transaction wait.
- Same shared lock,2/4 CPUs,1GiB AS,600CPU/1200wall seconds and
  `MALLOC_ARENA_MAX=2`; RAM768MiB/disk2GiB preflight floors and RSS900MiB,
  single/total WAL256MiB stops remain. No guard fired. Sampled RAM minimum
  1583 ->1674MiB, free disk13441626112 ->13415948288 bytes, process peak
  RSS266.141MiB both, sampled/final virtual maximum569.785 ->738.586MiB.
  Single WAL18321672 bytes both; total WAL25585328 ->25581208, final WAL0.
  Traffic-window reads1134592 ->335872/writes56307712 ->53714944 bytes;
  final cumulative reads829267968 ->635256832/writes4262387712 ->
  4261105664 include setup/snapshot restore. CPU58.721 ->57.936s,
  process wall74.855 ->72.255s and restore13.600 ->13.725s are not
  application throughput. Phase MainThread CPU1453.201/2150.469/2089.921
  ->1397.433/2078.318/2199.006ms includes instrumentation; contention
  phase CPU2.722 ->2.814s regresses. Observer overhead is uncalibrated and
  cannot be subtracted or added to overlapping SQL CPU.
- One consolidated affected run passed202 tests/69 subtests; three new
  fixture errors (lazy-proxy patch teardown, SQLite INTEGER affinity and
  missing catalog item_type) and the frozen-owner case blocked by the
  protected manifest deletion were resolved together. Only those four failed
  methods were rerun in a code-only tree:4 passed/4 subtests. Driver/index
  incremental checks passed9/5; final transport oracle passed10/10, including
  exact captured8-item output, CQ text, literal escapes, bounds and mutated
  quantities/labels. Battle tests retain fresh reads, narrow-provider ABI,
  readonly rift wiring, repair/read failure and boss impart; transaction
  regressions retain material rollback, replay and equipment reservations.
  Compile, architecture, inventory and whitespace passed in the code-only
  acceptance tree with exact HEAD manifests; original user files/deletion
  stayed untouched. Final transport changes receive incremental syntax/
  whitespace checks. No old full suite, accepted phases or prior load was
  rerun; delivered-SHA CI remains the clean complete gate.

This is one shared-query batch, not completion of repository-wide performance
work. Remaining evidence gaps include contention pause/DI scheduling, actual
battle/refining/fusion/automatic-listing latency and writer contention,
training full aggregates, remaining honor sorts, unsampled command chains
and sustained capacity. Continue from these shared patterns and saved SQL
evidence; do not repeat an unchanged million-user fixture per caller.

## V20: Shared Indexed Ranking Reads (2026-11-27)

V19 is delivered at `9a6425130730626b7cb778919ae136da87999637`, with
exact-SHA CI45925825136 successful. This batch addresses a shared expensive
numeric comparison/materialization pattern across training and arena, rather
than removing Python call layers or adding personal-value caching. Six
runtime files and three regression files implement the following changes.

| Shared entry / verified matcher callers | Database work reduced | Live/transaction boundary |
| --- | --- | --- |
| Training `_rank_in_uow`: completed and points leaderboards (2 matchers) | Replace full MATERIALIZED deduplication/numeric comparisons with a target lookup and two exact expression-index range COUNTs, when schema evidence proves a unique user identity | Both counts and target use one SQL statement in the current readonly main/attached transaction. Missing or mismatched indexes, nonunique identity, unknown collation and unsupported schemas retain the original query. Blank/null identities and earliest-row duplicate semantics remain. |
| Arena `_ranking_in_uow`: honor preview and public arena leaderboard (2 matchers) | Use controlled compact integer ordering index for live TOP reads, removing repeated Python numeric collation sorts | Honor still uses TOP1000, Python string identity, first match, ordinal ties and zero outside the slice. This is its existing preview contract, not an exact personal competition rank derived from a cached TOP slice. |
| Arena `_rank_in_uow`, used by the public leaderboard | Two exact index range COUNTs replace per-row numeric UDF comparisons | Invalid numeric values remain excluded from competition rank but sort as zero in TOP ordering; separate controlled indexes preserve this distinction. Duplicate rows still count and the target remains the earliest row. This compatibility API is not another matcher. |

Thus four matcher entrypoints benefit statically. The paired load measures
three optimized surfaces (training completed/points and honor); public arena
leaderboard latency is not measured here and has behavior/SQL-plan coverage
only. Arena info, personal profile and stone training are unchanged controls.
Range COUNT still traverses matching B-tree entries; this is not an O(logN)
order-statistics rank implementation. Cold training public snapshots still
join display names and retain the established public TTL; personal rank is
live. No reward, CAS, ledger, replay, balance or commit boundary changes.

New player-owned migrations `training.007` and `arena.011` create two training
and two arena expression indexes, without editing published migration bodies.
Catalog/player inclusion/game exclusion are all registered. The shared schema
helper accepts only controlled table/field/index specifications. It reads
table/index definitions from the actual connection, hashes those definitions,
and caches only the proof of schema suitability in the existing bounded
32-entry/1MiB process cache. This avoids pathname/file replacement and
rollback/reused-schema-version proof contamination. LIMIT65 and rejection
above64 objects or64KiB of fetched representation bound eligible evidence;
the byte check occurs after fetch, not as an absolute database-fetch memory
limit. Cold proof checks table/index PRAGMAs; failures propagate. No player
values or ranks are cached by this helper, including across writes.

### Frozen Pair and Coverage

- Reused the sole retained 1M fixture at
  `/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05`, with original
  rank digest `0dd134931a7cb0aa3511cc90d50c3c873b3cea97f576dd74e7e536af577ad8c7`.
  Before is a code-only archive of9a642513, not another million-user DB.
  Reports in `data/` are `indexed-ranking-before-v20-20261127-r01.json`,
  `indexed-ranking-after-v20-20261127-r01.json` and offline
  `indexed-ranking-comparison-v20-20261127-r01.json`.
- Runtime tree hashes
  `5908632b5f5b2c66c151112bff69a956039fe6350fde372323f3ee9c10134e98`
  -> `6a5caf2324b4f7bc187f236980d3c1ebcd282f1762f6118abab210f752362d38`
  differ in exactly six scoped runtime files. Driver, fixture, limits, phase
  specs, every actor/request/argument and business manifests match; diagnostic
  compatibility-hit JSON and its derived seed hash are excluded from business
  equality. Current runtime and test bytes match the accepted tree; runtime
  bytes also match the after report.
- One unchanged `arena-training-v18` scenario supplies384 requests per version
  in three128-request phases: pure cap10 at20 arrivals/s, mixed cap20 at35/s,
  contention cap20 at50/s. Each has56 personal profiles; pure has18 each of
  honor, arena info and the two training boards; mixed phases have10 each
  plus32 real distinct-user stone-training writes (exp+100, stone-1000).
  The shared instance/isolated database serves128 actors,8192 populated arena
  and8192 training rows, plus the retained1M main users. Only QQ delivery is
  mocked. Tasks drain and consistency checks precede snapshot restoration.
- This is a new source pair for a changed index/query pattern, reusing existing
  workload/data instead of repeating V19 or constructing per-caller fixtures.
  New indexes bootstrap before the disk snapshot; final restoration therefore
  preserves the migrated schema. Business-row restoration is checked
  separately; the player schema is not claimed to return to pre-migration.
  Four bounded8192-row feature refreshes exercise setup/index maintenance;
  arena/training rows do not change during traffic. Actual feature-score writer
  throughput, independently measured index bytes and startup cost remain gaps.
- Both versions succeed384/384, with0 business/cooldown refusals, exceptions,
  timeouts, SQL errors, busy/locked or boundary violations (0%). Live exact
  profile/rank output, exp/stone/power, generation, durable receipts/ledger,
  nonwriter and full arena/training business hashes, and original1M restore
  all pass. No duplicate debit/reward, negative balance, lost update or deadlock
  is observed. These checks do not establish unsampled feature settlements or
  same-feature writer contention correctness/performance.

E2E includes planned-arrival, dispatch and admission queues. Values below are
p50/p95/p99 milliseconds before -> after; SQL calls/wall include observed
execute/fetch/control API work. All rows have0% errors/refusals/timeouts.
Seventeen phase/path rows cover six commands, not all674 matchers or41
background entries. With n10/18, p95/p99 are sample maxima, not stable tails.

| Phase / path | n | E2E p50/p95/p99 ms before -> after | SQL calls before -> after | SQL wall ms before -> after |
| --- | ---: | --- | ---: | ---: |
| pure / honor |18|45.899/177.230/177.230 ->21.649/198.615/198.615|558 ->579|634.231 ->150.846|
| pure / arena info control |18|11.934/70.928/70.928 ->11.716/20.425/20.425|344 ->344|41.570 ->39.947|
| pure / profile control |56|16.095/171.484/195.470 ->15.707/79.222/149.427|2256 ->2256|121.112 ->114.382|
| pure / training points |18|44.167/133.740/133.740 ->14.157/38.869/38.869|311 ->337|737.383 ->83.906|
| pure / training completed |18|43.806/144.656/144.656 ->13.783/98.955/98.955|313 ->339|700.335 ->85.979|
| mixed / honor |10|60.438/329.112/329.112 ->21.542/165.449/165.449|310 ->323|262.747 ->81.844|
| mixed / arena info control |10|74.342/357.430/357.430 ->11.650/193.941/193.941|192 ->192|23.623 ->23.527|
| mixed / profile control |56|76.731/303.380/388.537 ->21.447/126.920/224.444|2271 ->2267|393.552 ->174.512|
| mixed / stone training control |32|80.948/402.089/418.971 ->37.228/173.000/252.215|1219 ->1219|182.918 ->158.599|
| mixed / training points |10|74.488/441.959/441.959 ->18.583/142.135/142.135|175 ->193|359.104 ->64.636|
| mixed / training completed |10|77.088/272.693/272.693 ->13.572/125.261/125.261|177 ->195|359.083 ->53.513|
| contention / honor |10|727.343/1408.370/1408.370 ->415.817/630.683/630.683|312 ->323|256.559 ->109.313|
| contention / arena info control |10|743.147/1428.438/1428.438 ->386.626/650.798/650.798|190 ->192|23.373 ->26.274|
| contention / profile control |56|652.292/1476.552/1546.102 ->354.145/634.942/686.242|2264 ->2261|351.664 ->313.109|
| contention / stone training control |32|692.220/1486.028/1505.924 ->373.948/628.593/647.862|1216 ->1216|182.937 ->179.978|
| contention / training points |10|708.799/1446.792/1446.792 ->395.628/627.413/627.413|175 ->194|364.643 ->63.042|
| contention / training completed |10|726.871/1466.267/1466.267 ->416.361/608.078/608.078|177 ->194|380.134 ->74.359|

### Benefits, Regressions and Limits

- The repeated hot-player training query runs35 times per field across the
  pair's phases: points1250.192 ->65.154ms, completed1247.677 ->66.818ms.
  Three additional cold attached queries per field are separate. Honor's38
  repeated sort/order queries fall903.461 ->52.861ms. SQL plans/regressions
  confirm index order/ranges instead of temp ordering/numeric full scans.
  Schema proof adds queries: whole-phase calls4426/4957/4907 ->
  4500/5020/4963, despite wall2511.970/2005.201/1913.194 ->
  721.267/800.328/1117.018ms. The benefit is less expensive work per query,
  not a universal reduction in query count. After cold attached connection
  diagnostics truncate4/5 statements per training command/phase under the
  first12 diagnostic cap; before omits0. Complete per-command SQL totals
  remain intact; saved statement diagnostics are not an exhaustive SQL trace.
- Pure honor p95/p99 regresses177.230 ->198.615ms despite lower median/SQL.
  Contention arena-info SQL wall23.373 ->26.274ms also regresses. No retiming,
  GC suppression, load reduction or selective exclusion was performed. A
  single small paired sample does not prove all commands became faster, nor
  causally explain every control-path improvement or tail.
- Successful short-window throughput pure/mixed/contention19.965/32.910/
  32.418 ->20.091/34.610/41.443 requests/s. Contention window3.948438 ->
  3.088593s still does not prove sustained50 successful requests/s. Peak
  admitted in-flight4/13/20 ->4/9/20 and handler-wall overlap3/8/15 ->3/6/15
  distinguish offered rate, cap, overlap and successful throughput; neither
  shows20 simultaneous CPU/SQLite executions.
- Contention admission p95/p99 falls622.585/752.785 ->186.461/230.878ms.
  Dispatch p95/p99 is36.919/65.397 ->1.070/3.999 pure,
  49.181/89.535 ->31.741/52.642 mixed,76.993/89.139 ->65.889/85.928
  contention; loop lag48.279/76.203 ->6.686/23.198,
  47.847/186.755 ->42.687/69.547,114.053/197.916 ->68.005/112.451ms.
  Producer pure p95 regresses.551 ->1.445ms and contention p95/p99
  2.330/5.661 ->2.954/5.892ms. Queuing/loop delay remains material.
- Main-user exp/stone generation deltas are0/32/32 in both versions; no global
  rank invalidation algorithm changes. Rank-worker calls323/353/346 ->
  323/349/343, wall34.894/293.636/190.130 ->35.018/80.712/163.981ms.
  COUNT calls0/4/3 ->0/0/3, seeks98/112/110 ->98/112/108 and plans4/8/8
  both reflect scheduling/generation overlap, not a new personal-rank cache.
  BEGIN828/908/908 counts match, p95/p99 .026/.126 ->.024/.308,
  .031/.082 ->.019/.046, .052/.328 ->.047/.453ms; pure/contention p99
  regress. BEGIN execution includes observer/SQL time and is only an upper
  bound on transaction wait, not an isolated lock-wait measurement.
- Cache phase hits/misses225/105,139/119,141/117 ->274/110,164/124,168/120;
  evictions73/87/85 ->78/92/88, partial hits91/30/58 ->89/32/43. Waits,
  expiry, explicit invalidation, bypass and oversized counts remain0. End
  entries32, after max77269 estimated bytes under1MiB. Added schema-proof
  traffic changes the aggregate hit mix; it is not personal-value caching or
  evidence that public display freshness changed.
- The same shared lock,2/4 CPUs,1GiB address space,600CPU/1200wall seconds,
  `MALLOC_ARENA_MAX=2`, available-RAM768MiB/disk2GiB floors and RSS900MiB,
  single/total WAL256MiB stops apply. No guard fires. Sampled minimum RAM
  1503 ->1609MiB, free disk13517574144 ->13490802688 bytes; process peak
  RSS231.977 ->240.039MiB increases. Sampled/final virtual peak646.016 ->
  614.367MiB, max single WAL18321672 both, total27019088 ->27591768
  bytes increases; final WAL0. Phase CPU4.595/3.946/4.169 ->
  2.713/3.171/3.253s; whole-process CPU67.913 ->66.166s includes setup
  and restoration. Process wall80.840 ->81.281s and restore13.529 ->14.887s
  regress; neither is application throughput. Preflight setup4.980 ->5.042s;
  phase setup7.723/6.818/6.850 ->6.720/7.835/7.116s. Final cumulative
  read984956928 ->884723712 and write4267466752 ->4271636480 bytes
  include fixture refresh/restore IO; writes increase. Index maintenance and
  migration cost are included but not independently isolated. Instrumentation
  overhead is uncalibrated; overlapping CPU/SQL timings must not be summed.
- Consolidated affected regression passes183 tests/170 subtests, covering
  huge/legacy numbers, invalid/null/blank identities, duplicate earliest-row
  fallback, ties and TOP1000, live updates/inserts/deletes, readonly and
  missing-table boundaries, denied-read recovery, controlled schema proofs,
  same-path/different-connection evidence and DDL rollback, player-only
  idempotent new migrations and published-checksum compatibility. Compile,
  architecture (`ok=true/errors=[]`), inventory and scoped whitespace pass in
  `/tmp/xiu-v20-accept-YVzPu1Ai`. Initial static-tree setup omitted export-ignored
  frozen manifests and git index metadata; exact HEAD manifests/index were
  supplied in that isolated tree, without restoring the user's deletion.
  These were validation-setup failures, not product failures. No old full
  suites, accepted phases or unchanged matrices were rerun. Delivered-SHA CI
  remains the clean complete gate; document-only closeout does not repeat
  accepted runtime tests.

The repository-wide goal remains active. This batch provides evidence for
three optimized runtime surfaces and regression evidence for a fourth, not
exhaustive674-matcher coverage. Remaining shared-query work must be selected
from saved call paths/SQL rather than another taxonomy audit. Feature-score
writer contention/index-maintenance cost, unsampled battle/material/trade
latency, residual queue/DI pauses and sustained50-success/s capacity remain
unverified; retain earlier regressions rather than replacing their evidence.

## V21: Shared Sect Read Stages and Live Member Counts (2026-11-27)

### Shared Work and Transaction Boundaries

This batch changes five distinct matcher callers, selected from existing
call paths rather than a new repository audit. Static associations with the
sect helpers are not all proven duplicate-query callers.

| Shared entry | Actual callers | Work removed | Live boundary |
| --- | --- | --- | --- |
| `sect_level_from_scale` over an already-read sect row | `升级攻击修炼`, `升级元血修炼`, `升级灵海修炼` | Repeated sect SELECTs in the same preliminary read stage: ordinary level <=100 has3 ->1; this load's level >100 has2 ->1 | Authoritative transaction rereads, balance/material checks, CAS, receipts and replay remain unchanged |
| Same scale calculation in buildings | `宗门建设` | Two sect reads ->one | No value reused across requests or after settlement |
| `SectApplication.count_sect_members` ->readonly repository COUNT | `我的宗门`, `宗门建设` | Complete member lists, transfer and Python normalization replaced with one scalar | Live `COUNT(*)`, including duplicate/NULL user identities as the old list length did |

The existing `idx_user_xiuxian_sect_id` supplies a covering-index plan; no
duplicate index is added. COUNT still traverses matching index entries, not
constant-time metadata. The standalone live `get_sect_level` wrapper remains
compatible. Physical connection-open savings are not claimed: request scope
already reuses connections. The measured benefit is less repeated SQL and
full-row processing, not fewer Python function calls. No individual player
values are cached. Personal sect ordinal ranking retains the previous full
ordering: tie order and mixed identity comparison semantics have not been
proved equivalent to a replacement SQL rank, so that hotspot remains.

### Frozen Pair and Representative Data

- Sole original fixture:
  `/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05`,1M existing
  synthetic main users, original exp/stone digest
  `0dd134931a7cb0aa3511cc90d50c3c873b3cea97f576dd74e7e536af577ad8c7`.
  Before uses a code-only d8fab80a archive, without another million-user DB.
  Valid reports in `data/`: `sect-shared-before-v21-20261127-r02.json`,
  `sect-shared-after-v21-20261127-r01.json` and offline
  `sect-shared-comparison-v21-20261127-r01.json`.
- Runtime tree hashes
  `6a5caf2324b4f7bc187f236980d3c1ebcd282f1762f6118abab210f752362d38`
  -> `a92a1d2e7eb2cb1382d9e348c87eb1c886a72aa6b0a946df560b9311f8c589e2`
  differ in exactly four runtime files. Driver, fixture, limits, phase specs,
  every request/actor/argument and all business/phase manifests match. All
  identity, consistency, resource-stop and original-digest restoration checks
  pass. Setup, traffic and restoration share the same isolated target instance
  and DB; restoration occurs only after requests drain and invariants pass.
- 128 actors across128 sects, each with64 existing members:8192 populated
  members and8192 accepted current-period task rows. Roles are owner0 and
  ordinary member11, respecting real role capacities and practice eligibility.
  Scale4b+ supports100 members; hence the measured upgrade branch is2 ->1
  reads, not3 ->1. Actors have exact1e9 wallets;8064 background members have
  exact `10**30 + ordinal` wallets through production exact binding. All8192
  rows pass production normalization before dispatch and during invariants.
  No new main-user rows or larger million-user fixture are introduced.
- The retained `before-r01` report is INVALID performance evidence: the old
  list normalizer rejected inherited ambiguous huge REAL background wallets,
  giving128 pure-phase exceptions. Later phases stopped and original digest
  restoration passed. Only synthetic fixture setup was corrected; the exact
  integer product rule was retained, with a small real-SQLite regression.
  No invalid after or per-command retry was run. The corrected pair above is
  the sole valid before/after comparison for this batch.
- Reuses the mixed harness with a minimal `sect-shared-v21` scenario to cover
  the previously missing populated membership/shared-materials write pattern.
  Each version has384 requests in three128-request phases: pure cap10 at20
  arrivals/s has64 each of my-sect/buildings; mixed cap20 at35/s has32 each
  display,32 asset-overview controls and32 upgrades (attack11/health11/mana10)
  from32 writers across4 sects. Contention cap20 at50/s has the same command
  mix,32 upgrades sharing one sect and11 users across three practice matchers.
  User/matcher write pairs do not repeat across phases; cooldowns stay enabled.
  Matcher, provider, permission, rule, DI and handler are real; only QQ delivery
  is mocked. Existing asset-control feature data is reused, without claiming
  newly populated backpack/arena scale.
- Both versions succeed384/384,0 business/cooldown refusals, exceptions,
  timeouts, SQL errors, busy/locked or boundary violations (0%). Each upgrade
  has an exact durable receipt for0 ->1 and deductions5m stone/50m materials.
  Full actor rows,8064 full background rows, all sect fields/exact materials,
  bounded selected non-user table signatures and generation checks pass.
  No duplicate debit, lost update, negative balance or deadlock is observed.
  The remaining991808 users do not have traffic-time full-column invariants;
  final1M exp/stone restoration does. Unselected non-user tables and tables
  above32768 rows are explicitly excluded, not claimed as whole-DB zero-write.

E2E includes planned-arrival, event-loop dispatch and admission queues.
Values are p50/p95/p99 milliseconds before ->after; SQL calls/wall include
execute/fetch/control APIs. All rows have0% errors/refusals/timeouts. Fourteen
phase/path rows cover six commands, including one unchanged control. For
n10/11, p95/p99 are sample maxima, not stable population tails.

| Phase / path | n | E2E p50/p95/p99 ms before -> after | SQL calls before -> after | SQL wall ms before -> after |
| --- | ---: | --- | ---: | ---: |
| pure / my-sect |64|30.405/143.261/260.453 ->28.207/38.497/81.498|2006 ->2006|1080.240 ->1005.368|
| pure / buildings |64|29.703/93.511/309.218 ->23.081/31.477/64.130|2112 ->1792|1026.956 ->727.231|
| mixed / assets control |32|23.550/168.444/266.553 ->17.658/163.760/277.044|807 ->807|214.829 ->219.068|
| mixed / my-sect |32|34.046/209.650/323.303 ->28.414/218.331/220.382|1005 ->1005|533.177 ->489.457|
| mixed / attack upgrade |11|29.916/252.776/252.776 ->23.363/251.941/251.941|429 ->374|164.795 ->120.064|
| mixed / buildings |32|35.624/181.707/294.988 ->24.466/188.928/192.192|1056 ->896|501.902 ->351.088|
| mixed / health upgrade |11|28.611/163.569/163.569 ->24.324/163.097/163.097|429 ->374|166.291 ->134.644|
| mixed / mana upgrade |10|28.097/188.812/188.812 ->23.152/134.301/134.301|390 ->340|151.113 ->107.401|
| contention / assets control |32|587.768/940.435/959.475 ->308.041/490.946/491.012|807 ->807|228.965 ->219.309|
| contention / my-sect |32|580.410/975.095/980.765 ->281.732/499.578/517.378|1005 ->1005|551.205 ->500.945|
| contention / attack upgrade |11|673.809/992.267/992.267 ->408.642/574.452/574.452|429 ->374|164.014 ->120.204|
| contention / buildings |32|585.057/956.776/960.548 ->314.615/509.847/509.896|1056 ->896|505.381 ->346.734|
| contention / health upgrade |11|722.095/1075.788/1075.788 ->422.171/572.943/572.943|429 ->374|172.789 ->127.736|
| contention / mana upgrade |10|633.625/1031.129/1031.129 ->301.426/569.636/569.636|390 ->340|152.307 ->114.526|

### SQL Evidence, Regressions and Resource Limits

- Complete foreground calls pure/mixed/contention4118/4116/4116 ->
  3798/3796/3796:320 fewer per phase,960 fewer overall (12350 ->11390).
  Including background work, phase calls4760/4749/4694 ->4444/4427/4385;
  phase SQL wall2335.554/2014.411/2100.494 ->1957.936/1693.659/1758.591ms.
  Counts include transaction/control APIs, not just SELECTs.
- Saved connection diagnostics show sect SELECTs192/160/160 ->128/96/96,
  wall679.764/553.569/556.364 ->461.797/328.614/324.565ms. Member lists
  128/64/64 calls, wall555.566/273.100/292.314ms become COUNTs with
  wall431.184/204.852/204.841ms. Across256 member queries,16384 full-row
  transfers/normalizations become256 scalars. Diagnostics retain only first12
  statements/connection: omitted statements per version are pure my-sect4;
  mixed/contention my-sect1, assets103, attack66, health66, mana60;
  buildings0. These are truncated diagnostics, not an exhaustive SQL trace;
  complete per-command/phase totals above remain valid.
- Pure overall p95143.261 ->36.154ms; contention overall
  p50/p95/p99639.168/960.548/1031.129 ->323.280/509.896/572.943ms.
  Mixed overall p95 REGRESSES181.707 ->186.778ms. Mixed my-sect p95
  209.650 ->218.331 and buildings181.707 ->188.928 also regress;
  asset-control mixed p99266.553 ->277.044ms and SQL214.829 ->219.068ms
  regress. No selective rerun, lower load or exclusion was used to improve
  these numbers; a single short pair cannot establish universal speedups or
  causally explain every tail/control improvement.
- Successful throughput pure/mixed/contention20.064/33.754/37.221 ->
  20.084/35.058/43.106 requests/s; traffic windows6.379528/3.792141/
  3.438915 ->6.373162/3.651090/2.969398s. Offered20/35/50 arrivals/s,
  admission cap10/20/20, peak admitted7/8/20 ->2/10/20 and handler-wall
  overlap4/7/14 ->1/6/13 are distinct measures. This does not prove20
  simultaneous CPU/SQLite executions or sustained50 successful requests/s.
- Contention admission p95/p99433.009/501.455 ->69.283/88.556ms. Dispatch
  p95/p992.853/52.693 ->.201/10.345 pure,17.347/24.628 ->18.589/48.258
  mixed (regresses),72.943/86.697 ->49.039/56.016 contention. Loop-lag
  p95/p9927.707/59.165 ->21.846/24.719,20.104/97.029 ->18.022/55.927,
  87.085/90.027 ->44.013/61.831ms. Producer p95/p99.219/68.133 ->
  .122/.178,4.821/118.964 ->2.364/115.879,3.556/6.362 ->2.541/4.830ms.
  Event-loop/queue stalls remain; the harness exposes them in E2E.
- BEGIN counts896/768/768 ->832/704/704; p95/p99 .022/.087 ->.027/.278,
  .021/.095 ->.021/.052, .026/.148 ->.024/.075ms. Pure tails regress;
  mixed maximum .345 ->1.095ms and contention .556 ->.696ms regress.
  BEGIN execution includes SQL/observer time and is only an upper bound on
  transaction wait, not a separately measured lock wait. Cache hits, misses,
  invalidations and recomputations are0, entries0 in every phase. Main-user
  stone/general generation deltas0/32/32, exp0 in both versions; there are no
  personal-rank requests here, so this cannot quantify rank recomputation or
  frequent exp-write invalidation cost.
- Heavy work remained serial under the shared validation lock,2/4 CPUs,
  1GiB address space,600 CPU/1200 wall seconds, `MALLOC_ARENA_MAX=2`,
  available-RAM768MiB/disk2GiB floors and RSS900MiB/single+total WAL256MiB
  stops. No guard fires. Sampled minimum RAM1540 ->1645MiB, disk
  13170016256 ->13155225600 bytes; process peak RSS214.301 ->219.828MiB
  INCREASES. Sampled/final virtual peak582.805 ->571.840MiB, max single
  WAL7090552 both, total11292984 ->11305344 bytes increases; final WAL0.
  Whole-process wall69.170 ->66.628s and CPU58.480 ->56.463s include setup,
  observation and restoration. Preflight setup2.440 ->2.551s regresses;
  phase setup6.278/6.102/5.990 ->5.285/5.506/5.251s and restore13.842 ->
  13.683s are separate from application traffic. Final cumulative read
  869199872 ->324116480 and write4134727680 ->4135325696 bytes include
  fixture refresh/restore; writes increase. Observer overhead is uncalibrated;
  overlapping CPU/SQL timings must not be summed or called application-only IO.
- Affected acceptance covers93 unique tests/78 subtests, including all three
  upgrades, ordinary-level repeated reads, level100 cap, large exact divmod,
  write-time balance/material changes, duplicate/NULL counts, readonly and
  covering-index behavior, denied-read recovery and next-request freshness;
  fixture regressions cover real command resolution, role capacity, reply and
  receipt oracles, injected inconsistency and exact large-wallet preflight.
  Compile, architecture (`ok=true/errors=[]`), inventory and scoped whitespace
  pass in `/tmp/xiu-v21-accept-j5hHhW5j`. The initial86-pass run's five failed
  progress checks were caused by export-ignored docs missing in the isolated
  tree; exact HEAD docs were supplied there only. Three tracked-but-ignored
  invitation JSON index entries were also supplied only there for inventory
  parity. Changed fixture/progress checks were verified without rerunning the
  accepted86-test group. No user deletion was restored and no full local
  unittest or accepted older load was repeated. Exact delivered-SHA CI is the
  remaining complete gate.

The repository-wide goal remains ACTIVE: this is evidence for five optimized
callers plus one control, not exhaustive674-matcher/41-background coverage.
Exp-changing gameplay, task/reward settlement, membership/scale writes,
personal exact-rank recalculation, unsampled feature data and sustained50
successful requests/s remain outside this batch. Next work must reuse saved
SQL/call paths to batch shared query patterns; this report neither replaces
previous regressions nor claims the overall performance target is complete.

## V22: Shared Catalog Read Stages and Live Directory Fanout (2026-11-27)

### Shared Work and Live Boundaries

This batch changes SIX distinct matcher callers selected from the existing
call-path inventory. Five callers of the same catalog parser already hold a
sect row: `宗门功法查看`, `宗门神通查看`, `宗门功法搜寻`,
`宗门神通搜寻`, `学习宗门神通`. They now parse that field without a second
SELECT/UoW. Standalone helpers continue to read live. The exact legacy
`str(value)[1:-1].split(',')` behavior is retained, including whitespace and
empty/malformed values; this is not a catalog-cleanup change. Search/learning
still recheck membership, catalog CAS, balances/materials and receipts in
their original authoritative transactions. Nothing is reused after a write
or across requests; no player-value cache or generic table/column API is added.

The sixth caller, `宗门列表`, replaces the aggregate plus per-sect state and
complete-member queries with ONE readonly projected SQL statement. It reads
name/identity/scale/status, first-owner name and indexed `COUNT(*)`. This
removes repeated SQL, UoW setup and transfer/normalization of full member rows,
not Python function-call depth. The populated sample has128 sects,45 open to
joining:174 business SELECTs per directory request become1;48 requests remove
8304 business SELECTs and138240 full-member row transfers/normalizations.
The240 sampled catalog views remove another240 same-stage sect SELECTs.

Existing user/sect indexes serve correlated owner/count lookups; COUNT still
traverses matching index entries, not O(1) metadata. No new index/cache is
needed. Owner/count subqueries inside one SQL are not described as zero
fanout: work still scales with displayed sects and their matching members.
The owner seek-plan regression uses a TEXT owner fixture. It does not prove
an indexed seek for the production INTEGER owner affinity: that comparison
plan remains unverified and may contribute to the saved ~60ms projection.
Do not change equality with CAST or infer equivalence for legacy numeric
spellings without evidence. V23 leaves this schema/query boundary unchanged.
The legacy five-tuple directory API remains unchanged for the inactive-owner
background task. The live compatibility join-display helper also uses COUNT.
Actual join eligibility/settlement does not consume the display projection.

`COUNT(*)` retains NULL/duplicate user rows as the old list length did. Status
priority/truthiness and exact large-scale arithmetic remain unchanged. A NULL
sect identity displays missing, and SQL equality does not match NULL members.
Real sect IDs are primary keys; for legacy schemas without that constraint,
the projection explicitly selects first rowid per sect ID and owner. The old
GROUP BY bare-column selection had no stable duplicate-row guarantee, so this
does not claim byte equivalence under every malformed duplicate-sect plan.
Unused financial/member fields are not read; required read failures propagate.

### Frozen Pair and Coverage

- Sole1M fixture remains
  `/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05`; original exp/stone
  digest `0dd134931a7cb0aa3511cc90d50c3c873b3cea97f576dd74e7e536af577ad8c7`.
  Before is code-only8e17ca00; no second persistent million-user DB was made.
  Reports in `data/`: `sect-directory-before-v22-20261127-r02.json`,
  `sect-directory-after-v22-20261127-r01.json`, offline
  `sect-directory-comparison-v22-20261127-r01.json`.
- Runtime tree hashes
  `a92a1d2e7eb2cb1382d9e348c87eb1c886a72aa6b0a946df560b9311f8c589e2`
  -> `53f45a39afc78f73e80837c8fc874840686288cd6713a9d31b71e63d9f26934c`
  differ in exactly four runtime files. Driver, fixture identity, all request
  plans/actors/arguments, business/phase manifests and limits match. Both
  versions PASS384/384, all preflight/phase invariants and final digest restore.
- Reuses V21 population:128 actors,128 sects with64 members each,8192 actual
  current-period task rows, exact1e9 actor and1e30+ordinal background wallets.
  Only catalogs/statuses add coverage: each sect has two real main and two
  secondary items from different ranks, stored in reverse order without spaces
  and verified against Items' legacy rank/level normalization. Four actor
  sects stay open; background states alternate. Every directory reply verifies
  all128 ordered entries/owners/counts/statuses/scale by an independent complete
  oracle; both catalog replies verify every item and order. This is populated
  directory fanout under live writes, not an empty-feature-table claim.
- Three128-request phases per version: pure cap10 at10 arrivals/s contains
  16 directory/56 main/56 secondary reads; mixed cap20 at20/s and contention
  cap20 at35/s each contain16 directory/32 main/32 secondary,32 practice writes
  and16 asset controls. Mixed writes use32 users across4 sects; contention
  uses11 users sharing one sect across three actual practice matchers.
  User/matcher pairs do not repeat between phases; cooldowns remain enabled.
  Practice/asset code is unchanged. Real matcher/provider/permission/rule/DI/
  handler executes on one shared target instance and isolated DB per phase;
  only QQ transport is mocked. Restore follows request/worker drain and checks.
- All business/cooldown refusals, exceptions, timeouts, SQLite errors,
  busy/locked and boundary violations are0 (0%). Exact durable receipts match
  32 successful writes per mixed/contention phase and their exact debits.
  Full128 actor rows,8064 background rows, all sect fields, bounded frozen
  non-user tables and generation deltas pass. Remaining991808 users lack
  traffic-time full-column invariants; final1M exp/stone restoration is checked.
  Unselected/oversized tables remain explicit exclusions, not whole-DB proof.
- Startup archive Git metadata failure occurred before DB access. Then
  before-r01 stopped in seed/preflight because the new fixture incorrectly
  compared normalized skill level with raw level:0 phases/requests, restore
  TRUE. It is retained and EXCLUDED from performance evidence; no after ran.
  Fix only fixture normalization, add actual bundled JSON/config preflight,
  freeze again, and execute the sole valid pair above. No per-command retry,
  accepted V21 load rerun or number-selection run was performed.

E2E includes planned-arrival, dispatch and admission queue delays. Values are
p50/p95/p99 milliseconds before ->after; SQL calls/time include execute,
fetch and transaction/setup APIs. All rows have0% errors/refusals/timeouts.

| Phase | Command | n/version | E2E before | E2E after | SQL calls before ->after | SQL wall ms before ->after |
| --- | --- | --- | --- | --- | --- | --- |
| Pure | 宗门列表 | 16 | 2995.293/5386.491/5386.491 | 69.739/115.520/115.520 | 13996 ->156 | 11060.604 ->974.204 |
| Pure | 宗门功法查看 | 56 | 3218.816/5083.390/5283.189 | 12.937/22.453/150.757 | 1186 ->906 | 448.097 ->244.640 |
| Pure | 宗门神通查看 | 56 | 3192.544/5182.781/5383.290 | 12.832/21.012/65.742 | 1176 ->896 | 431.440 ->242.880 |
| Mixed | 宗门列表 | 16 | 6196.189/11437.842/11437.842 | 71.603/90.376/90.376 | 13984 ->144 | 10891.991 ->967.542 |
| Mixed | 宗门功法查看 | 32 | 6185.011/11533.402/11733.251 | 12.811/24.380/31.690 | 685 ->525 | 250.131 ->140.903 |
| Mixed | 宗门神通查看 | 32 | 6245.998/11482.717/11682.459 | 13.110/16.911/18.150 | 672 ->512 | 246.033 ->139.021 |
| Mixed | 升级攻击修炼 (control) | 11 | 6769.094/11981.812/11981.812 | 43.630/60.485/60.485 | 374 ->374 | 125.816 ->138.217 |
| Mixed | 升级元血修炼 (control) | 11 | 7617.581/11783.693/11783.693 | 30.653/71.397/71.397 | 374 ->374 | 135.091 ->137.041 |
| Mixed | 升级灵海修炼 (control) | 10 | 5855.523/11587.652/11587.652 | 23.577/55.973/55.973 | 340 ->340 | 114.505 ->126.722 |
| Mixed | 我的资产总览 (control) | 16 | 6085.218/11632.321/11632.321 | 18.450/26.981/26.981 | 407 ->407 | 104.792 ->116.920 |
| Contention | 宗门列表 | 16 | 7139.532/13973.542/13973.542 | 71.889/79.136/79.136 | 13984 ->144 | 10877.053 ->961.263 |
| Contention | 宗门功法查看 | 32 | 7088.882/14027.545/14141.643 | 25.041/54.157/56.260 | 685 ->525 | 253.907 ->139.114 |
| Contention | 宗门神通查看 | 32 | 7167.909/13999.088/14113.756 | 14.342/38.295/38.297 | 672 ->512 | 248.194 ->125.700 |
| Contention | 升级攻击修炼 (control) | 11 | 7986.121/14283.452/14283.452 | 73.902/83.595/83.595 | 374 ->374 | 126.118 ->120.494 |
| Contention | 升级元血修炼 (control) | 11 | 8921.656/14171.095/14171.095 | 34.770/82.024/82.024 | 374 ->374 | 122.966 ->127.011 |
| Contention | 升级灵海修炼 (control) | 10 | 7224.470/14061.567/14061.567 | 22.497/80.452/80.452 | 340 ->340 | 117.931 ->110.666 |
| Contention | 我的资产总览 (control) | 16 | 7075.861/14084.396/14084.396 | 21.779/24.758/24.758 | 407 ->407 | 109.830 ->107.216 |

### Attribution, Regressions and Limits

- Foreground SQL50030 ->7310. Per-phase foreground16358/16836/16836 ->
  1958/2676/2676; total including message writer16662/17248/17244 ->
  2348/3156/3150, SQL wall12042.942/12126.046/12061.967 ->
  1654.732/1970.383/1864.268ms. Every phase's16 directory calls remove2048
  full sect reads and720 full member-list reads. Their before SQL wall totals
  6961.719+2966.614,6851.575+2906.801,6852.585+2902.459ms; after the16
  projection statements take965.534/962.859/956.937ms. New directory SQL is
  still about60ms/request in this instrumented sample, a remaining hotspot.
  Old directory connection diagnostics omit54912 touches/phase due to the
  existing bounded cap; after omits0. Top-query counts above are separate
  exact aggregates. Do not infer full physical open counts from clipped traces.
- Phase E2E p95 is5090.165/11533.402/14027.545 ->70.532/71.906/77.335ms;
  success throughput7.321/7.266/7.293 ->10.067/20.085/34.941 requests/s.
  Arrival rates10/20/35 are distinct from concurrency caps10/20/20.
  Peak admitted intervals10/20/20 ->2/2/3; actual handler wall-overlap peaks
  10/20/20 ->1/1/2. After contention has349.335ms with multiple active
  handler intervals; this proves some overlap, not parallel SQLite/CPU or
  20 simultaneously executing handlers, sustained capacity, or50success/s.
- Admission queue p95/p99 before3507.394/3513.887,7890.756/8954.457,
  10992.389/12024.767 ->.012/.022,.011/.013,.009/.010ms. Event-loop lag
  p95/p99 1061.481/1077.061,1025.567/1155.453,1052.975/1920.174 ->
  4.742/57.976,43.213/58.077,59.336/63.572ms. Loop-dispatch p95
  961.312/948.103/992.838 ->.189/19.311/40.670ms. This explains control
  E2E gains without claiming the unchanged control SQL itself became faster.
- Retain regressions: all four mixed controls' SQL wall INCREASE (table);
  contention health SQL122.966 ->127.011ms. Background message SQL counts
  304/412/408 ->390/480/474 increase as commit batching changes with timing;
  pure background SQL wall102.801 ->193.008ms increases. Producer delay max
  pure1.048 ->47.447ms and contention4.825 ->8.837ms increase; these are
  test-side arrival deviations, not application SQL. Preflight setup2.513 ->
  2.544s and contention setup5.494 ->5.621s increase. A single short pair
  does not establish statistical stability of any tail or isolated SQL timing.
- BEGIN execution p95 .017/.021/.021 ->.054/.041/.021ms, increases in the
  first two phases; counts3360/3344/3344 ->480/512/512. It is an execution
  upper bound on transaction waiting, not a separate lock-wait measurement.
  Shared-cache hits/misses/invalidations/recomputations and entries are0 in
  all phases. Stone/general generations0/32/32, exp0, both versions; no
  exact-rank request is sampled. This cannot quantify exp+stone invalidation
  or cold exact-rank recalculation; earlier coverage/limits remain in force.
- MainThread CPU17281.836/17297.196/17274.271 ->2742.338/2899.797/
  2731.132ms. Measured observer record CPU528.565/561.793/563.342 ->
  83.128/113.176/110.917ms also drops with fewer SQL calls. Instrumentation
  is uncalibrated, partially overlapping and incomplete; neither these timers
  nor total SQL can be summed/subtracted to claim application-only cost.
- Serial shared-lock execution kept2/4 CPUs,1GiB AS,600CPU/1200wall seconds,
  MALLOC_ARENA_MAX=2, RAM>=768MiB/disk>=2GiB, RSS<=900MiB and single/total
  WAL<=256MiB. No guard fires. Sampled minimum RAM1659 ->1748MiB, disk
  12940808192 ->12924833792 bytes; process peak RSS222.668 ->215.828MiB,
  sampled/final virtual759.793 ->483.133MiB, max single WAL7090552 both,
  total11276504 ->11272384 bytes; final WAL0. Process wall106.612 ->75.040s,
  CPU101.118 ->55.313s include setup/observer/restore. Phase setup
  5.498/6.506/5.494 ->5.401/5.356/5.621s, final restore13.518 ->13.449s.
  Cumulative process read666038272 ->42479616 bytes and write4105621504 ->
  4118814720 bytes include fixture setup/restore IO; writes INCREASE, and
  these totals cannot be attributed wholly to runtime application traffic.
- Affected acceptance covers66 unique tests, preserving independent status,
  NULL/duplicate, big-integer, index/readonly, deny/recovery, next-request,
  search/learning write-time CAS/assets, rollback/replay and actual parameter
  assertions. Initial62 tests passed; three failing methods were new fixture
  missing the secondary receipt migration and a source ownership gate missing
  helper COUNT. Fixed only fixture/gate; affected7 tests and19 subtests then
  passed without rerunning accepted groups. Catalog normalization correction
  adds actual bundled-data preflight (1 passed); changed fixture group8 passed
  with15 subtests. Compile/architecture/inventory/scoped whitespace pass in
  `/tmp/xiu-v22-accept-MP1gPaio`. No full local unittest, old load or CI change.

The repository-wide goal remains ACTIVE. Six callers were optimized, three
of them sampled with four unchanged controls; search/learning have direct
regressions but no runtime-load claim here. This is not exhaustive674 matcher/
41 background coverage, nor validation of all function-table scales. Continue
from saved shared-call/query evidence, retaining personal realtime semantics,
known regressions, unmeasured boundaries and the unverified50success/s target.

## V23 Shared Read-Stage Reuse (2026-11-27)

This batch removes repeated database work in shared synchronous calculation
stages across EIGHT distinct matchers: `出关`, `修炼`, `灵石修炼`, `我的状态`,
`我的修为`, `使用`, `双修`, `同意双修`. It is not a reduction in Python call
depth or a repository-wide completion claim. V22 at92a2cafd and its exact-SHA
CI45950034770 are already green; their accepted gates/load were not repeated.

### Shared Work and Live Boundaries

- `get_root_rate` accepts an explicit same-stage `root_level`. Seven buff/back
  call sites reuse their existing player row instead of loading the full
  profile again for fate roots. The two pill branches share the `使用` matcher;
  the five buff entries plus that matcher are SIX distinct callers. Ordinary
  roots previously had no nested query, so no such benefit is claimed for them.
  Explicit None/zero retains the old conversion/rate rules without a fallback
  read; the existing two-argument API still reads live. Normal training also
  reuses its existing Buff reader instead of constructing a second one.
- Both dual-cultivation entries reach `direct_two_exp`. It reuses the two
  existing profiles/Buff readers/main-skill inputs across K synchronous rounds
  and supplies same-stage root levels to both final power calculations. Each
  round still recalculates from accumulated temporary exp and keeps the same
  cap, random draws and special events. Three rounds remove18 loop SELECTs
  plus2 nested root SELECTs; transaction/fetch APIs also fall. The standalone
  async round wrapper stays live, including missing-player reasons and the
  cap path that avoids Buff reads. Other relation-power callers retain their
  live defaults because their read/write boundaries were not reviewed here.
- No await/write separates the reused calculation inputs. Authoritative
  exp/usage/protection/invite/affection/CAS/replay and settlement boundaries
  remain unchanged, including both post-settlement profile rereads. No
  cross-request personal cache, arbitrary table/column API, balance cache or
  TOP-slice approximation of exact rank is added. Callers crossing await,
  breakthrough or reentry boundaries were deliberately left unchanged.

### Frozen Pair and Coverage

- Reuses the sole1M synthetic fixture at
  `/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05`, original rank
  digest `0dd134931a7cb0aa3511cc90d50c3c873b3cea97f576dd74e7e536af577ad8c7`.
  No persistent second million-user DB. Code-only baseline
  `/tmp/xiu-v23-baseline-Dnvouule` is exact92a2cafd; accepted source
  `/tmp/xiu-v23-accept-0N4CPdsK` adds exactly FIVE runtime-file changes.
  Runtime trees are
  `53f45a39afc78f73e80837c8fc874840686288cd6713a9d31b71e63d9f26934c`
  -> `a9f8b9bc8b12c871a153ab5ce29c3fef8c80432c87c6584e28af770603eaf71e`.
- Reports in fixture `data/`: `read-stage-before-v23-20261127-r01.json`,
  `read-stage-after-v23-20261127-r01.json` and offline
  `read-stage-comparison-v23-20261127-r01.json`. This is ONE frozen pair,
  384/384 successful requests per version, with equal driver, fixture, limits,
  request plans, business/phase manifests and original digest. All phase
  invariants and final restoration pass. No retry to select better timings.
- 128 actual actors have2560 bag rows and128 Buff rows. All selected roots
  are fate roots at level4; main-skill IDs and blessing are0, so nonempty
  skill behavior has direct regressions but is not sampled under load.
  Impart population is not quantified here. Inherited8192 activity rows and
  three32768-row balance populations only reuse base setup: there is NO new
  activity/table-scan performance conclusion from these requests.
- Three128-request phases: pure cap10 at20 arrivals/s has64 `我的修为`,32
  status and32 unchanged asset controls. Mixed/contention cap20 at20/35/s
  each have32 cultivation-value reads,32 status,32 assets,16 stone-training
  writes and16 three-round dual-cultivation writes. Writer groups differ;
  contention shares display/writer identities, not competing write/write
  resources. Actor/command cooldown keys do not repeat across phases and
  cooldowns stay enabled. Real named-target, off-protection, nonpartner dual
  cultivation uses seeded real RNG `Random(20261127)`; gains are checked by
  an independent enumeration of normal/special three-round combinations.
- One shared instance/isolated DB per phase executes real matcher/provider/
  rule/DI/handler; only message transport is mocked. All tasks drain before
  phase restore. Exact exp/stone/power, usage/statistics/stamina, relationship/
  affection, receipt identity/attribute payload, bags,1M count and quick_check
  pass. Invite/partner/cap/replay have direct regression coverage, not this
  load coverage. No cold exact-rank traffic or50success/s capacity claim.
- Business refusals, cooldown refusals, exceptions, timeouts, SQLite errors,
  busy/locked and boundary violations are all0 (0%). This establishes the
  selected consistency checks, not every feature/table/concurrency boundary.

E2E includes scheduled-arrival, dispatch and admission queue delay. Values
below are p50/p95/p99 milliseconds; SQL calls/time include execute, fetch and
transaction/setup APIs. Every row has0% errors/refusals/timeouts.

| Phase | Command | n/version | E2E before | E2E after | SQL calls before ->after | SQL wall ms before ->after |
| --- | --- | --- | --- | --- | --- | --- |
| Pure | 我的资产总览 (control) | 32 | 17.536/25.213/25.627 | 17.060/24.731/26.261 | 775 ->775 | 222.202 ->215.334 |
| Pure | 我的修为 | 64 | 9.149/10.777/68.966 | 9.149/10.910/59.809 | 857 ->793 | 52.297 ->49.387 |
| Pure | 我的状态 | 32 | 13.489/19.450/49.601 | 13.156/21.790/37.976 | 903 ->871 | 77.966 ->78.818 |
| Mixed | 我的资产总览 (control) | 32 | 17.504/27.112/138.798 | 17.485/24.131/65.075 | 775 ->775 | 220.491 ->212.796 |
| Mixed | 我的修为 | 32 | 54.207/88.817/206.213 | 46.829/96.046/164.699 | 429 ->397 | 38.566 ->42.737 |
| Mixed | 双修 | 16 | 99.620/118.744/118.744 | 93.675/128.420/128.420 | 2770 ->2258 | 488.671 ->438.876 |
| Mixed | 我的状态 | 32 | 18.635/38.879/188.730 | 13.353/46.125/114.780 | 900 ->868 | 83.925 ->81.809 |
| Mixed | 灵石修炼 | 16 | 32.964/127.987/127.987 | 34.456/172.056/172.056 | 627 ->611 | 75.829 ->77.219 |
| Contention | 我的资产总览 (control) | 32 | 38.359/103.688/113.042 | 43.672/242.038/245.295 | 775 ->775 | 217.335 ->223.520 |
| Contention | 我的修为 | 32 | 79.136/160.581/170.134 | 98.289/274.561/296.431 | 429 ->397 | 39.018 ->34.532 |
| Contention | 双修 | 16 | 104.244/134.186/134.186 | 106.013/244.837/244.837 | 2658 ->2146 | 480.548 ->478.101 |
| Contention | 我的状态 | 32 | 50.727/141.497/145.047 | 71.833/246.049/269.613 | 788 ->756 | 76.117 ->78.514 |
| Contention | 灵石修炼 | 16 | 62.977/85.840/85.840 | 70.146/328.683/328.683 | 624 ->608 | 75.805 ->71.847 |

### Attribution, Regressions and Resources

- Complete foreground DB operations13310 ->12030 (-1280). Per phase:
  2535/5501/5274 ->2439/4909/4682; including background3181/6134/5886 ->
  3084/5547/5287. SQL wall569.660/1274.468/1139.077 ->571.780/1088.557/
  1140.179ms: pure and contention INCREASE despite fewer operations. Each
  sampled three-round dual request removes32 total foreground APIs. For16
  dual requests per writing phase, full-profile SELECTs336 ->112 (-224).
  Top-query diagnostics retain only20 patterns and connection diagnostics
  the first12 statements/connection; a missing Buff entry is not zero reads.
  Complete operation counts are independent of those clipped diagnostics.
  Selected connection diagnostics show128 opens in both versions; no reduction
  in physical connection creation is claimed.
- Phase E2E p95 19.450/115.701/122.009 ->18.659/96.046/245.295ms. Successful
  throughput20.098/20.052/34.649 ->20.101/20.054/33.980 requests/s. Preserve
  the contention regression, all command-tail regressions in the table and
  RSS203.781 ->216.852MiB. This pair proves reduced repeated SQL work, NOT
  unconditional E2E improvement or statistical stability of short tails.
- Handler wall-overlap peaks1/4/4 ->1/3/7; admitted inflight peaks2/5/6 ->
  2/4/12; multiple-active time0/145.427/634.796 ->0/70.790/1457.721ms.
  Arrival rates20/20/35 and caps10/20/20 differ from actual overlap and
  success throughput; none proves20 simultaneous execution or sustained50/s.
  Queue p95 .010/.010/.009 ->.011/.015/.009ms; loop-dispatch p95
  .198/49.400/71.269 ->.168/41.036/71.185ms. Loop lag p95/p99 is
  4.749/12.527,80.535/88.409,81.954/110.400 ->3.460/12.116,
  73.823/100.568,89.342/132.618ms. Producer max .178/96.214/2.082 ->
  .228/105.183/4.108ms is test-side arrival deviation, not application SQL.
- Slowest after contention stone request: E2E328.683ms, service323.987ms,
  matcher-check142.616ms, handler179.192ms, SQL3.309ms, owned-drain.039ms,
  summed BEGIN.094ms. Later unchanged asset/read handlers also spend about
  155-165ms wall with SQL<7ms. Samples locate wall delay in dispatch/DI/
  handler intervals, but have no complete CPU profile: they do NOT establish
  GC, host scheduling or a particular product-code root cause. Fewer SQL
  calls cannot dismiss the observed E2E regression; no rerun selects it away.
- Shared-cache hits/misses/invalidations/recomputations/entries all0. General
  and exp generations0/48/48, stone0/16/16 in both versions. There are no
  exact-rank requests, so this does not measure invalidation-driven cold-rank
  recomputation. BEGIN execution p95 .0293/.0425/.0334 ->.0412/.0368/.0425ms
  is an upper bound on transaction waiting, not an independent lock timer.
  MainThread contention CPU3081.450 ->3100.584ms also regresses; observer
  timers overlap/omit work and cannot be subtracted to infer application CPU.
- Serial shared-lock pair uses2/4 CPUs, AS1024MiB, RSS900MiB,600CPU/1200wall
  seconds, MALLOC_ARENA_MAX=2, RAM>=768MiB, disk>=2GiB and single/total WAL
  <=256MiB. No guard fires. Minimum RAM1675 ->1641MiB, disk12620550144 ->
  12599795712 bytes; peak virtual535.879 ->534.508MiB; max single WAL18321672
  bytes both, total23282248 ->23265768, final WAL0. Process wall72.674 ->
  68.321s and CPU55.406 ->53.363s include setup/observer/restore. Preflight
  2.665 ->2.092s; phase setup5.300/4.572/4.594 ->4.522/4.884/4.423s (mixed
  INCREASES); restore14.025 ->13.394s. Cumulative read748208128 ->531116032
  and write4250996736 ->4235735040 bytes include fixture IO and cannot all
  be attributed to runtime application traffic.
- ONE affected acceptance in the isolated accepted tree:100 tests and70
  subtests pass in27.60s; compileall, architecture, inventory `--check` and
  scoped whitespace pass. Coverage retains independent root/live-stage/error,
  dual accumulated gains/cap/special/big-number/post-write/rejection, invite/
  transaction/replay, closing, normal/stone training and both pill assertions.
  No local fullsuite, CI workflow change, old accepted gate or old load rerun.

The total performance goal remains ACTIVE. Eight matchers benefit; four
changed matchers plus one unchanged control were load-sampled. Remaining
matchers have direct regressions only. Cold precise-rank/global invalidation,
wider exp/stone write patterns, directory owner affinity and this contention
tail regression remain unresolved. This is not exhaustive674 matcher/41
background coverage or validation of all feature-table sizes.

## V24: Reuse Complete Natal/Impart Rows Within One Read Stage

This batch follows shared query patterns across 15 confirmed matcher callers,
without another repository classification pass. Seven natal callers are
awaken, reshape, info, grow, slot upgrade, engrave and forget; seven virtual
world callers are project, list, duel, train, info, explore and leave closing.
The fifteenth is confirm-lunhui, which imports `impart_uitls.impart_check`.
The similarly named, unused helper in `xiuxian2_handle.py` is unchanged and
does not count as an optimized caller.

- Complete natal records: `exists()` and first `get_data()` each previously
  reread the row after `_ensure_record`; each now uses its already-decoded
  record, reducing two SELECTs to one. An exists/data pair still uses two
  separate live reads. Missing/NULL/schema repairs retain the previous scalar
  or full-row readback API; strict battle reads and the existing object cache
  are unchanged. This adds no personal cache or schema/index.
- Impart helpers: retain initialization, repository ensure, exact balance
  validation, canonical first-row identity and legacy repair. The legacy
  getter's opt-in `read_back_repairs` preserves a real read after repair writes,
  including trigger changes; its default behavior is unchanged. Complete
  virtual-world helpers go from five to four SELECTs; the actual lunhui helper
  from two to one. Repair/missing/schema paths can use more queries.
- Virtual-world training: its successful calculation stage does not suspend
  or write impart state. Reuse the initial repaired row instead of a second
  ensure/helper and third legacy getter (normal legacy reads eleven to four,
  also removing one ensure transaction). Leave-closing reuses its impart row,
  Buff object and normal CD read, saving two legacy reads, one Buff read and
  one CD read. The fallback CD recheck remains. Settlement transactions still
  check current exp, time/daily tokens and closing identity; rewards, cooldown,
  receipts, replay, precise numeric rules and post-write boundaries remain.

The new minimal load composes the existing harness and sole 1M fixture, adding
128 complete nonempty natal/impart/daily participant records. Six real matcher
routes sample four changed callers plus unchanged assets/stone-training
controls. Three phases each have 128 requests: pure cap10 at20 arrivals/s,
mixed cap20 at20/s, contention cap20 at35/s. Each version plans 304 successes
and 80 already-awakened business refusals, with 32 real virtual-world training
and 32 stone-training writes. Refusals are excluded from successful throughput.
Contention overlaps display/writer identities, not two concurrent writers.
No natal action write, other eleven changed matchers, exact-rank, activity
traffic, all674/41 or sustained50successful/s capacity is inferred.

### V24 Accepted Evidence and Limits

Affected regression was run once in an isolated code tree: **89 passed,
43 subtests passed, 27.68s**. Compile, architecture, inventory and whitespace
gates passed. The later classifier-only repair has its own **12 passed,
13 subtests passed, 1.44s**; the runtime regression and old loads were not
repeated. No local full unittest or CI/workflow changes were made.

Evidence directory remains
`/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05/data/`:

- Raw baseline: `impart-natal-before-v24-20261127-r01.json`, SHA256
  `f1e4ce8a5f019c3923e1130df7c9e11a9c70a7316033ccd7ef221373a8a19cc8`.
- Raw after: `impart-natal-after-v24-20261127-r02.json`, SHA256
  `d0adc12132b8e724e1a39bb325b2d7a2292a1e60b7b3d22e4270bec245a613cf`.
- Derived comparison: `impart-natal-comparison-v24-20261127-r01.json`, SHA256
  `f054769d39529387135297eda449d700538d98d6c6c602a4dac3de06ef587b03`;
  status `comparison_valid_with_offline_classification_recovery`.

The raw reports deliberately retain `status=failed`: the archived classifier
counted identical rounded pre/post wallet candidates twice. All 16 contention
asset replies were consequently `unclassified`, although every other live DB
invariant passed. Both versions used the **same archived timing driver**;
classification runs after each request's completion timestamp, and can still
perturb subsequent arrivals equally. The corrected classifier deduplicates
only display candidates, retaining rejection of wrong/duplicate wallet lines.
Offline analysis checks all stored replies, proves this is the only source
change and the only 16 errors per report, recomputes outcomes/success throughput,
and preserves every latency, SQL measurement and live DB check. It does not
recreate a live DB invariant offline or overwrite either raw report. Exact
balances remain checked against full persisted rows/receipts, not rounded text.
Classifier/driver/report hashes and each corrected request are in the derived
provenance. An initial after-r01 sample is retained but **excluded**: an extra
outer CPU affinity constraint was halved again by the harness (1 vs 2 CPUs).
The comparison rejected it; only after was supplemented under matching caps.
The baseline and previously accepted gates were not rerun.

Fixture, participant manifests, every request, all limits and raw driver
identity match. Both runs restore the original rank digest. The sole 1M user
fixture also contains 2,560 bag rows, 128 Buff rows, 128 selected complete
natal/impart/daily rows, 8,192 activity users and 32,768 rows in each of three
activity balance/damage tables. Activity traffic is not sampled. All phases
use one shared instance/DB set; restore occurs only after task/worker drain.
Each version has 384 requests: **304 successes, 80 already-awakened refusals,
32 virtual-world writes and 32 stone-training writes**. Exceptions, timeouts,
boundary failures and unexpected outcomes after documented correction are
zero; per-path error rate is zero. The natal-exists samples are all business
refusals and never successful throughput. Selected full rows, exact crystal
balances, daily/time/gain tokens, receipts/ledger, inventory/frozen tables,
legacy daily JSON and exp/stone generation deltas pass. SQLite busy/locked is
zero. BEGIN timing remains an SQL-time upper bound, not pure lock wait.

All latency cells below are **p50/p95/p99 milliseconds, before -> after**.
SQL counts include execute/executemany/script/commit/rollback API calls;
they are not just SELECT statements. SQL time includes measured fetches.

| Phase | Path | n/version | E2E before | E2E after | DB calls | DB ms |
| --- | --- | ---: | --- | --- | --- | --- |
| pure | assets (control) | 32 | 17.562/22.740/25.467 | 17.921/25.904/43.090 | 775 -> 775 | 224.813 -> 229.684 |
| pure | natal exists (refusal) | 32 | 9.080/16.230/33.347 | 8.712/28.602/110.481 | 480 -> 416 | 20.079 -> 18.587 |
| pure | natal info | 32 | 8.541/9.529/69.376 | 8.422/63.175/143.675 | 438 -> 406 | 30.821 -> 30.563 |
| pure | world info | 32 | 18.536/22.593/32.470 | 18.484/31.226/77.886 | 1091 -> 1059 | 213.555 -> 220.279 |
| mixed | assets (control) | 16 | 25.287/188.554/188.554 | 25.378/44.671/44.671 | 391 -> 391 | 110.259 -> 114.873 |
| mixed | natal exists (refusal) | 24 | 9.214/10.977/89.179 | 8.844/10.395/10.767 | 360 -> 312 | 15.177 -> 14.609 |
| mixed | natal info | 24 | 8.513/22.045/138.912 | 8.588/13.648/23.038 | 325 -> 301 | 26.488 -> 28.777 |
| mixed | stone training (control) | 16 | 55.141/239.942/239.942 | 59.972/77.842/77.842 | 611 -> 611 | 90.229 -> 90.490 |
| mixed | world info | 32 | 18.631/22.274/58.950 | 18.518/22.164/24.373 | 1088 -> 1056 | 206.113 -> 215.344 |
| mixed | world training | 16 | 67.207/227.508/227.508 | 59.266/85.199/85.199 | 976 -> 784 | 339.770 -> 344.145 |
| contention | assets (control) | 16 | 55.761/101.484/101.484 | 57.481/84.667/84.667 | 391 -> 391 | 120.450 -> 126.794 |
| contention | natal exists (refusal) | 24 | 11.229/48.309/53.426 | 9.400/32.435/43.088 | 360 -> 312 | 17.964 -> 15.766 |
| contention | natal info | 24 | 29.843/72.662/76.983 | 27.916/59.041/65.232 | 325 -> 301 | 31.093 -> 27.082 |
| contention | stone training (control) | 16 | 85.408/131.323/131.323 | 64.365/114.378/114.378 | 608 -> 608 | 79.158 -> 72.907 |
| contention | world info | 32 | 18.376/43.420/43.556 | 17.933/23.294/28.926 | 1056 -> 1024 | 224.915 -> 210.439 |
| contention | world training | 16 | 62.726/99.678/99.678 | 55.545/87.535/87.535 | 976 -> 784 | 356.462 -> 342.521 |

| Phase | Whole-phase E2E before -> after | Foreground DB calls | All SQL ms | Successful/s | Peak admitted requests | Peak active handler intervals |
| --- | --- | --- | --- | --- | --- | --- |
| pure cap10 @20 arrivals/s | 16.698/22.281/33.347 -> 17.151/28.602/110.481 | 2784 -> 2656 | 711.344 -> 727.066 | 15.071 -> 15.077 | 2 -> 3 | 1 -> 2 |
| mixed cap20 @20 arrivals/s | 18.456/83.608/227.508 -> 18.452/68.572/77.842 | 3751 -> 3455 | 1032.928 -> 1052.249 | 16.329 -> 16.331 | 5 -> 2 | 4 -> 1 |
| contention cap20 @35 arrivals/s | 34.090/95.381/125.541 -> 27.916/85.557/98.633 | 3716 -> 3420 | 1088.683 -> 1025.558 | 28.422 -> 28.520 | 5 -> 4 | 3 -> 3 |

The deterministic gain is **10,251 -> 9,531 foreground DB API operations**
(720 removed, 7.02%). Training removes 12 operations per ordinary request,
including the repeated ensure transaction. Background message writer calls
are separately attributed: 646/638/620 -> 645/639/624 across phases. Public
cache hits/misses and recomputation counts are zero; invalidation counters
are 32/16/16 in both versions. No new cross-request cache is involved.

Do not generalize the faster mixed/contended tails into universal speedup:
pure p95/p99 **regress**, aggregate pure/mixed SQL time rises slightly, and
process peak RSS rises **201.305 -> 220.039 MiB**. The pure after producer has
88.156ms max arrival delay and 112.431ms max loop lag; these are observed
timing disturbances, not proof of a product or harness root cause. Queue
p95 is 0.011/0.011/0.011 -> 0.013/0.012/0.010ms; loop dispatch p95 is
0.178/24.539/33.457 -> 0.243/15.829/29.142ms; loop lag p95 is
5.268/37.413/41.804 -> 8.442/37.284/40.697ms. The complete connection/SQL,
GC, queue, observer CPU and arrival samples remain available. Instrumentation
overhead is uncalibrated and cannot be subtracted to claim production capacity.
Active intervals measure overlap, not simultaneous SQLite/CPU execution;
neither cap20 nor 35 arrivals/s proves 20 actual concurrent requests or
50 successful/s. Small per-path tails, especially n=16 p95=p99=max, are noisy.

Both valid versions use the shared lock and identical limits: 2 of 4 CPUs,
AS1GiB, CPU600s, wall1200s, RSS stop900MiB, RAM preflight/stop768MiB,
disk minimum2GiB, per/total WAL256MiB, `MALLOC_ARENA_MAX=2`. Minimum observed
phase available RAM is 1509/1684MiB; free disk exceeds 12.5GB in both; maximum
phase WAL is 18,321,672 bytes in both. Process wall is 77.705 -> 71.923s,
CPU(user+system) 57.084 -> 55.433s, including startup/setup/restore rather than
only application traffic. Preflight setup is 3.229 -> 3.172s; final restore
13.296 -> 13.428s is separate from response latency. No additional lasting
million-user fixture, production writes, QQ service or deployment is involved.
Overall performance work remains open: this four-changed-route sample does
not settle cold exact ranks/global invalidation, directory affinity or prior
contention regressions, and does not exhaust the other 11 changed matchers,
674-command inventory or 41 background entries.

## V25: Sect Traversal, Owner Seeks and Candidate Activity Batches (2026-11-27)

### Shared Work and Boundaries

This bounded batch benefits **one matcher (`宗门列表`) and one scheduled
entry (`auto_handle_inactive_sect_owners`)**. It follows the saved V22 SQL
evidence rather than a new inventory audit. It removes repeated database
work, not Python function layers:

- Directory owner equality compares TEXT user IDs with the production
  INTEGER-affinity owner column. The former correlated predicate can scan
  the entire user table even when the TEXT identity index exists. A fixed
  native index on `CAST(user_id AS NUMERIC)` now supplies a necessary seek
  condition. The original `user_id=s.sect_owner` predicate and first-rowid
  selection remain, rejecting numeric-cast collisions. There is no conversion
  of stored identities, balances or exact ranks and no display cache.
- The scheduled entry initially consumes only sect ID/name. Its dedicated
  readonly projection avoids the unused owner lookup and member aggregate;
  the existing five-tuple API remains available to other callers. Legacy
  duplicate sect IDs explicitly select first rowid, not an unspecified bare
  GROUP BY row. Required read errors still propagate.
- Its closed-sect candidate loop now reads activity times once per stage:
  N readonly UoWs/SELECTs become one UoW and ceil(N/500) SELECTs. Empty input
  opens no database. A VALUES CTE with no column affinity preserves the old
  string-parameter comparison, distinct requested spellings, first duplicate
  row, missing/invalid timestamps and local-time conversion. It is discarded
  after this stage; no cross-request personal cache is added.

Candidate ordering, final writes, ledger/CAS/replay and transaction boundaries
are unchanged. The disband transaction rechecks live activity and membership;
the actual read-then-activity-change regression verifies `candidates_changed`,
no disband receipt and later successful recovery. Inheritance retains its
existing boundary: its transaction rechecks sect/membership/position/priority
and uses the caller's `eligible_user_ids`; it does **not** newly re-read
`user_cd`. Do not generalize the disband regression into a claim that all
inheritance activity eligibility is transactionally reread. No old projection
is substituted for balances, rewards, exact ranks or target selection.

### Frozen Evidence and Coverage

- Baseline code is `0c546505`; after is the accepted V25 five-file runtime
  tree. Runtime hashes are
  `d0d8f0452a437fce41aad2bed5bffad650fc599daf4a72ffa744eef6765ce2c1`
  -> `7aa46eaecb8a1cf74985fd3eeb2afa3b428df68437b92bc61e19d4ebca23a9a4`.
  Driver hashes, fixture identity, all business/phase manifests, requests,
  limits and original rank digests match exactly. Both reports pass without
  offline outcome correction or a discarded load attempt.
- Reports under the sole retained
  `/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05/data/`:
  `sect-seek-before-v25-20261127-r01.json`,
  `sect-seek-after-v25-20261127-r01.json`,
  `sect-seek-comparison-v25-20261127-r01.json`.
  The original 1M exp/stone digest remains
  `0dd134931a7cb0aa3511cc90d50c3c873b3cea97f576dd74e7e536af577ad8c7`.
- Reuses V22's 128 actors, 128 sects, 8192 members/current-period tasks,
  exact wallets, catalog definitions and receipt/full-row invariants.
  New coverage changes only 16 background sect owners: four last-row numeric
  IDs, four absent numeric IDs, four leading-zero spellings canonicalized by
  the INTEGER column, four absent opaque IDs. Gameplay actors' four sects
  remain unchanged. These deliberately malformed directory-only owner/member
  relationships exercise lookup boundaries; they are not ordinary gameplay
  population estimates. This is why new load was needed rather than rerunning
  the previously accepted V22 fixture unchanged.
- Each version executes 384 real matcher/DI/handler requests, all successful:
  48 directory targets, 240 unchanged catalog views, 32 asset controls and
  64 real practice writes. Pure has 16/56/56 directory/main/secondary;
  mixed/contention each have 16 directory, 32+32 catalog, 16 assets and
  11+11+10 attack/health/mana writes. There are zero business refusals,
  cooldown refusals, exceptions, timeouts, boundary failures or busy/locked
  errors. All 128 actor rows, 8064 background member rows, complete sect rows,
  exact practice receipts/material debits, bounded frozen feature tables and
  generation deltas pass; original fixture restoration passes after drain.
- Background throughput is not measured by command traffic. Real scheduler
  regression covers 17 candidates in one batch, unused-query exclusion,
  actual inheritance, stale disband and failure/recovery paths. The
  consolidated affected suite passed 96 tests +29 subtests (11.50s).
  Architecture initially found a dynamic CTE literal misclassified as a
  physical table; a complete SQL template fixed it without taxonomy changes.
  The seven affected activity/scheduler tests +2 subtests then passed (1.62s).
  Compile, architecture, inventory and scoped whitespace checks pass. No
  local full unittest, old accepted suite or old load was repeated.

### Same-Load Results

Each E2E cell is p50/p95/p99 in milliseconds, including admission and loop
queueing. DB calls include execute/executemany/commit/rollback; they are not
counts of internal correlated seeks. All listed paths have error rate zero.
Catalog, asset and practice rows are unchanged controls, not additional
optimized callers.

| Phase | Path | n | E2E before | E2E after | DB calls before -> after | DB ms before -> after |
| --- | --- | --- | --- | --- | --- | --- |
| pure | directory |16|11066.620/21778.226/21778.226|15.390/155.042/155.042|156 ->156|32181.631 ->103.048|
| pure | main catalog |56|11943.995/21244.016/21643.689|13.213/21.575/28.328|906 ->906|244.175 ->258.681|
| pure | secondary catalog |56|12044.201/21343.810/21743.550|13.140/18.179/66.578|896 ->896|234.536 ->246.598|
| mixed | assets |16|14851.698/28836.396/28836.396|18.551/26.741/26.741|407 ->407|108.150 ->114.788|
| mixed | attack practice |11|17890.583/29182.345/29182.345|24.535/32.375/32.375|374 ->374|137.290 ->128.443|
| mixed | directory |16|14700.435/29033.202/29033.202|15.152/18.840/18.840|144 ->144|32616.734 ->90.128|
| mixed | health practice |11|18285.270/28990.508/28990.508|23.580/31.710/31.710|374 ->374|123.268 ->130.303|
| mixed | main catalog |32|14951.495/28738.965/28934.810|12.991/14.366/26.865|525 ->525|152.259 ->143.793|
| mixed | mana practice |10|14849.091/29382.191/29382.191|23.685/32.865/32.865|340 ->340|123.659 ->114.048|
| mixed | secondary catalog |32|14902.122/28886.525/29084.344|12.902/15.318/16.793|512 ->512|134.585 ->130.307|
| contention | assets |16|15489.544/31080.495/31080.495|17.055/18.963/18.963|407 ->407|110.211 ->104.572|
| contention | attack practice |11|17428.725/31282.856/31282.856|23.128/24.716/24.716|374 ->374|134.582 ->125.311|
| contention | directory |16|15372.317/30970.129/30970.129|14.108/21.964/21.964|144 ->144|32335.473 ->85.275|
| contention | health practice |11|19428.087/31168.203/31168.203|23.898/30.255/30.255|374 ->374|143.419 ->135.741|
| contention | main catalog |32|15543.561/31025.977/31138.127|11.994/14.948/24.665|525 ->525|153.555 ->137.040|
| contention | mana practice |10|15366.701/31057.268/31057.268|23.282/29.732/29.732|340 ->340|114.862 ->121.906|
| contention | secondary catalog |32|15514.717/30997.254/31109.187|12.122/13.568/13.739|512 ->512|140.979 ->127.315|

The 48 actual directory business SQL statements have execute+fetch
p50/p95/p99 **2018.678/2071.613/2135.048 ->5.068/6.389/10.367ms**, total
97117.187 ->259.952ms. Full directory-request DB time is
97133.838 ->278.451ms. Foreground DB API calls stay **7310 ->7310**:
this command gain is removal of internal full scans, not fewer API calls.
The batch's N+1 reduction is separately proven in the scheduled caller.
EXPLAIN regressions require the real INTEGER-owner numeric seek and indexed
activity CTE for both TEXT primary-key and legacy INTEGER-index schemas.

| Phase / cap / arrivals per second | Whole E2E before -> after | All SQL ms | Successful/s | Peak admitted | Peak active handler intervals |
| --- | --- | --- | --- | --- | --- |
| pure /10/10 |12044.201/21244.016/21743.550 ->13.311/21.575/66.578|32752.049 ->783.383|3.782 ->10.066|10 ->2|10 ->1|
| mixed /20/20 |17824.936/28886.525/29182.345 ->14.639/26.741/32.375|33610.849 ->1029.960|3.677 ->20.083|20 ->1|20 ->1|
| contention /20/35 |17283.775/31025.977/31168.203 ->13.757/25.149/29.732|33358.222 ->1006.769|3.707 ->35.031|20 ->1|20 ->1|

Before, directory scans block the shared event loop, delaying unchanged
commands as well. Admission queue p95 is
17130.980/21635.872/23599.914 ->0.013/0.011/0.010ms; loop dispatch p95
1960.247/1942.665/1964.713 ->0.201/0.186/0.177ms; loop lag p95
2123.557/2186.607/2089.178 ->1.723/7.534/12.559ms. Producer delay p95
is .139/.155/.143 ->.140/.140/.117ms, while pure maximum regresses
4.301 ->48.726ms. Actual handler overlap is measured, but wall intervals
are not parallel CPU/SQLite execution. After serves this short35-arrival/s
sample with peak handler overlap1; it does not prove20 simultaneous handlers
or sustained50 successful/s. Small n=10/11/16 tails often equal maxima.

Public cache hits/misses/invalidations/recomputations are zero in both
versions. Generation deltas are unchanged: pure0, mixed/contention each
32 stone and zero exp. No personal exact-rank request or broad exp+stone
gameplay invalidation is sampled. Message-writer DB calls are separately
attributed and increase 298/403/402 ->390/480/480; none is hidden inside
foreground gains. Observer CPU remains recorded, uncalibrated, and cannot
be subtracted to claim production capacity.

### Costs, Limits and Remaining Work

The new index occupies **3600 pages /14,745,600 bytes (14.0625MiB)**.
Actual CREATE INDEX execute time is560.889ms (thread CPU527.442ms), excluding
commit; total runtime preparation is5.835 ->7.090s and is not described as
pure index-building time. Idempotent maintenance guards old/missing schemas
and propagates actual build failures. Existing migration callbacks and
published versions are unchanged. The restored isolated fixture retains the
new index; its million-user population/exp/stone data remains unchanged.

A drained rollback-only control updates32 existing synthetic rows, three
repetitions each, preserving exact original data, generation and index.
Within after, exp+stone update p50 is5.927ms indexed vs5.405ms with this
index temporarily dropped; same-value identity update is5.674 vs2.363ms.
Baseline no-index medians are2.395/.178ms respectively. These noisy tiny
controls do not show zero cost or characterize committed gameplay/registration
throughput. The minimal EXPLAIN regression confirms score-only writes do not
maintain this identity expression index; other existing score indexes/triggers
still operate. Index drop/rollback cost is excluded from update timers, and
the index-free variant is never a gameplay mode.

Material regressions are retained: process peak RSS197.059 ->211.898MiB;
pure catalog, mixed asset/health and contention mana SQL totals rise as shown
above; background writer calls rise. Raw process read/write bytes are
1,018,322,944/4,101,955,584 ->544,358,400/4,326,039,552; the write total rises
and includes setup, snapshot/restore and diagnostic cost, not just commands.
Process wall159.331 ->78.182s and CPU(user+system)154.702 ->54.913s also
include startup/setup/check/restore. Phase setup7.403/5.592/5.681 ->
5.531/6.023/5.738s and final restore13.736 ->13.541s are outside latency.

The serial pair holds `/tmp/nonebot-project-validation.lock` and identical
limits:2 of4 CPUs,AS1GiB,CPU600s,wall1200s,RSS stop900MiB,
RAM preflight/stop768MiB,disk minimum2GiB,per/total WAL256MiB,
`MALLOC_ARENA_MAX=2`. Minimum phase RAM1624/1680MiB; minimum disk exceeds
12.2GB. Maximum phase WAL7,090,552bytes in both; after-startup WAL14,840,272
bytes is recorded separately. No stop threshold is reached. One temporary
restore snapshot is removed after checks; no lasting duplicate1M database,
production database, real QQ/OneBot connection, deployment or release is used.

Coverage remains partial: many opaque user IDs would share the NUMERIC0
bucket and may still be slow; tiny differential tests prove semantics, not
that population's performance. Large scheduler throughput and >17 candidate
timing are unmeasured. This batch does not settle cold exact-rank scans,
global invalidation under broader exp+stone writes, old tail regressions,
all674 matchers/41 background entries or sustained50 successful/s.
The overall repository performance goal remains ACTIVE.

## V26: Shared Typed Inventory Projections (2026-11-27)

### Scope and Semantics

The shared `get_back_msg` chain benefits **six matchers across two modules**:
`丹药背包`, `药材背包`, `药材背包详细`, `我的装备`, `炼丹` recipe generation
and `配方` preflight. Four plain views, two Markdown views and the recipe
helpers previously materialized every positive inventory row with `SELECT *`
before filtering in Python. They now request a fixed six-column projection
and bound inventory types. The allowed type names are explicit constants;
there is no arbitrary table/column API, new index or cross-request cache.
The existing full-row call remains unchanged by default for other callers.

Generation uses the compact projection **without** a type filter, preserving
the distinction between an entirely empty bag and a bag without herbs or a
furnace in one read. Recipe preflight reads herbs/furnaces once and supplies
those rows to its existing checks. Catalog herb names and stored furnace
names remain distinct matching rules. Positive quantity, duplicates, relative
scan order, NULL bind/state values, equipped state, first matching material
and failure order are retained. `COLLATE BINARY` preserves the previous
Python exact type equality even on a legacy NOCASE column. No await/write
boundary gains an old player cache; actual material debits, balances, daily
eligibility, ledger/CAS/replay, receipts and settlement transactions remain
live and unchanged. Reducing Python call depth is not the claimed benefit.

Affected acceptance passed **83 tests +33 subtests in20.19s**, including the
real registered recipe success/replay/failure-recovery parameter group.
Compile, architecture, inventory and scoped whitespace passed. The subsequent
format-oracle correction alone passed **4 tests +10 subtests in0.88s**.
Unchanged full unittest/old matrices were not repeated locally; the complete
batch must still pass its exact-SHA CI before being called delivered.

### Frozen Sample and Classification Provenance

The sole retained1M synthetic fixture is
`/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05`.
Each of128 existing actors has **213 catalog-valid positive inventory rows**:
the original20,192 unrelated skill items and one real furnace. The complete
`back` table has27,264 rows and `BuffInfo`128; activity controls are populated
with8192 users and32,768 rows in each balance/damage table. Only inventory
scaling is the new coverage gap: the old20-item/user sample could not show
the cost of fetching many unrelated items. No new million-user population
or per-caller DB is created.

Each version executes three stages once,128 requests/stage. Pure has24 of
each of four display commands plus32 genuine absent-specific-furnace recipe
refusals; mixed/contention have16 of each display,32 recipe refusals and32
real exp+stone training writes. Mixed uses32 distinct writers; contention
uses the same writer. Display actor/command identities remain unique across
phases, preserving real5s cooldowns. All requests within a stage share the
same instance/database; restore happens only after drain and invariants.
Successful refinement and recipe generation timing are **not** measured;
their runtime changes have the affected functional regressions only.

The first baseline pure stage stopped because the new oracle expected plain
pill/herb blocks while the actual configured Markdown fallback returned
compact quantity/binding lines.48 correct replies were marked unclassified.
Its raw report is preserved. Nothing was replayed: baseline mixed/contention
and after pure/mixed/contention each ran once with the **same archived timing
driver and original classifier**. The raw reports retain the same48/32/32
format failures per version. A strict offline correction validates every
expected item/quantity/binding/use line, rejects all other errors, preserves
timing/SQL and all original live database checks, and changes only those
112 classifications/version. It does not manufacture or rerun a DB check.
The corrected future classifier and malformed/duplicate/missing-line tests
are included in this batch; they were not substituted into timed traffic.

Raw reports under fixture `data/` are:

- `inventory-projection-before-v26-20261127-r01.json` (pure)
- `inventory-projection-before-v26-20261127-phase{1,2}-r01.json`
- `inventory-projection-after-v26-20261127-phase{0,1,2}-r01.json`
- `inventory-projection-comparison-v26-20261127-r01.json` (derived comparison)

The comparison records every correction, raw/driver/classifier/recovery-script
hash, exact manifests and limits, plus all commands/resources/regressions.
Driver hashes, fixture identity, requests, business/phase manifests and
original rank digest match. Baseline2664c39e runtime hash is
`7aa46eaecb8a1cf74985fd3eeb2afa3b428df68437b92bc61e19d4ebca23a9a4`;
accepted runtime hash is
`86417951c9fb0fc4e0e71d6fedb102b66d4b976db7100abafd650cb16a99317c`.

Both versions have **288 successes and96 business refusals**, zero cooldown
refusals, system exceptions, timeouts, busy/locked errors or boundary errors
after the explicit format correction. The96 refusals are never counted as
successful throughput.64 real training writes/version pass exact balances,
power, receipt/ledger identities/rewards and generation deltas. Full bounded
inventory/BuffInfo hashes,1M population, quick_check and original exp/stone
digest restoration pass. No raw failed status is silently rewritten.

### Complete Same-Load Results

E2E cells are p50/p95/p99 milliseconds including queue/loop delay. DB columns
are all foreground API operations and cumulative execute+fetch time for that
path, including unchanged setup/registration reads. Per-path corrected error
rate is zero; recipe rows are known business refusals rather than successes.

| Phase | Path | n | E2E before | E2E after | DB calls before -> after | DB ms before -> after |
| --- | --- | --- | --- | --- | --- | --- |
|pure|equipment|24|9.371/14.574/17.580|8.357/14.497/22.048|288 ->288|37.873 ->15.310|
|pure|herbs|24|9.576/15.359/39.232|8.781/9.394/27.575|288 ->288|27.711 ->13.154|
|pure|herb detail|24|9.527/14.933/17.081|8.383/12.787/17.028|288 ->288|39.349 ->15.230|
|pure|pills|24|9.568/11.823/75.336|8.876/10.963/63.516|313 ->313|41.989 ->26.592|
|pure|recipe refusal|32|20.538/29.547/30.486|19.164/25.388/28.859|768 ->768|294.139 ->255.541|
|mixed|equipment|16|9.435/16.856/16.856|8.854/17.992/17.992|192 ->192|18.882 ->11.259|
|mixed|herbs|16|9.684/30.844/30.844|9.125/31.875/31.875|192 ->192|31.564 ->14.861|
|mixed|herb detail|16|9.145/27.947/27.947|8.572/23.242/23.242|192 ->192|26.785 ->11.226|
|mixed|pills|16|9.472/65.625/65.625|8.891/66.646/66.646|217 ->217|32.344 ->25.506|
|mixed|recipe refusal|32|20.139/31.299/34.911|19.400/26.591/31.930|768 ->768|291.930 ->251.021|
|mixed|training control|32|35.007/57.127/57.473|35.862/54.580/55.320|1219 ->1219|161.496 ->180.304|
|contention|equipment|16|9.033/91.597/91.597|8.602/116.034/116.034|192 ->192|23.690 ->8.396|
|contention|herbs|16|9.600/64.293/64.293|8.609/76.094/76.094|192 ->192|20.793 ->9.812|
|contention|herb detail|16|14.299/85.960/85.960|15.179/122.420/122.420|192 ->192|22.062 ->9.810|
|contention|pills|16|15.076/78.340/78.340|17.460/85.582/85.582|217 ->217|41.122 ->27.158|
|contention|recipe refusal|32|20.148/62.779/72.000|20.200/81.992/87.920|768 ->768|282.594 ->269.052|
|contention|training control|32|36.076/70.354/94.262|37.434/103.692/114.351|1219 ->1219|162.762 ->161.129|

Every optimized request has exactly one timed inventory SELECT in the
recorded SQL. Total inventory SQL time is **293.281 ->37.279ms** over320
requests/version; SQL p50 ranges .641-.689 ->.081-.138ms. Pure per-path
inventory p50/p95 is equipment .662/1.042 ->.098/.185, herbs .664/.966 ->
.083/.091, detail .659/3.626 ->.083/.130, pills .663/.787 ->.085/.133 and
recipe .673/4.036 ->.105/.182ms. Complete per-phase/path p50/p95/p99 and
totals are in comparison `inventory_sql_ms`. The query still visits the
user's indexed inventory range and filters types; no claim of eliminating
all213 scanned entries is made. Returned rows fall213 ->4 for pills/herbs,
213 ->8 for equipment and213 ->5 for recipe, with six rather than the full
legacy column set. This is reduced fetch/materialization work, **not fewer SQLs**.
Foreground DB operations stay **7505 ->7505**. Background message writer
operations are1712 ->1708; this incidental batch variation is not a claimed
product optimization. Shared display-cache hit/miss/invalidation/recompute
counters are all zero, and no exact-rank operation occurs in this sample.

| Phase / cap / offered arrivals per second | E2E p50/p95/p99 before -> after | All SQL ms | Successful/s | Peak admitted | Peak active handlers |
| --- | --- | --- | --- | --- | --- |
|pure/10/20|9.851/25.648/39.232 ->8.892/21.243/28.859|675.462 ->542.422|15.094 ->15.095|2 ->2|1 ->1|
|mixed/20/20|19.195/37.900/57.473 ->18.569/45.425/55.320|783.113 ->713.119|15.037 ->15.033|2 ->2|1 ->1|
|contention/20/35|19.541/64.293/91.597 ->20.073/81.992/116.034|813.360 ->715.807|26.219 ->26.201|4 ->5|3 ->3|

The mixed p95 and all contention tails **regress** despite lower inventory
SQL time. The sample proves reduced shared inventory work and a pure-stage
improvement; it does not prove better mixed throughput or universally lower
latency. Contention producer-delay p95/p99 grows .182/2.566 ->1.267/8.104ms,
loop-dispatch p95/p99 9.702/22.730 ->21.882/37.285ms and event-loop lag
p95/p99 19.552/29.102 ->22.453/38.073ms. Admission queue p95 remains
.010 ->.011ms. The worst after herb-detail response122.420ms has only.727ms
DB, a37.771ms handler interval and substantial dispatch/pre-handler delay;
its overlapping GC events are sub-millisecond. This locates significant
non-inventory/loop cost but does **not** isolate application, scheduler or
observer ownership. Do not blame GC, declare the changed SQL the tail cause,
disable guards or rerun to select a better number. The unchanged training
control also regresses in contention and its mixed SQL cost rises as shown.

### Resources and Remaining Limits

Each stage uses the same shared validation lock and enforced2/4 CPUs,
AS1GiB,CPU600s,wall1200s,`MALLOC_ARENA_MAX=2`,RSS900MiB stop,
RAM>=768MiB,disk>=2GiB and per/total WAL<=256MiB. Six processes run serially;
no second stress group or outer taskset is used. Minimum measured phase RAM
is1557MiB and disk11,809,345,536bytes. Maximum single WAL18,321,672bytes;
sampled total WAL<=22,507,624bytes. No limit/stop threshold is reached.

Phase peak RSS grows212.316/220.168/221.172 ->218.762/221.348/223.926MiB.
Full-process maximum peaks at233.043MiB and includes setup/check/restore.
Main-thread phase CPU1592.235/2308.431/2255.930 ->
1438.709/2248.166/2297.346ms also retains the contention regression.
Phase user+system CPU2.080/2.828/2.656 ->1.904/2.772/2.740s;
observed profiler sections359.199/411.334/301.589 ->
337.735/407.744/333.016ms are incomplete overlapping diagnostics, not
subtractable application cost. Phase setup6.281/5.284/5.301 ->
5.232/5.276/5.274s, startup5.487-6.275s and final restore13.691-15.001s
are outside response latency. Per-process raw I/O/write totals are in the
comparison and include snapshot IO, not just application writes.

These3.66-6.39s traffic windows reach only handler interval peaks1/1/3.
Offered20/20/35 requests/s, configured concurrency caps10/20/20, actual
overlap and successful throughput are distinct. This does not prove20 active
handlers or sustained50 successful requests/s. One213-item bag cardinality,
few herbs, a missing furnace and64training writes do not characterize large
recipe search, successful concurrent refinement, cold precise ranks/global
invalidation or the full674-matcher/41-background-entry inventory. The overall
performance goal remains ACTIVE; old tail/global-invalidation limits remain.

## V27 Shared Recovery Read Stages (2026-11-27)

This batch changes **two matcher entrypoints, three shared read functions,
two feature modules**: recipe initial cost replay/latest-ready recovery and
bank unified/legacy command receipt lookup. Bank has four business modes,
not four matchers. The existing inventory/call-path evidence selected this
pattern; no new repository-wide taxonomy or exhaustive runtime audit was run.
V26 is delivered at `244cf6bcc1fb0d6213ac4c32c140014d3c034920`, whose exact-SHA
CI45984241087 passed. None of its accepted gates or old workloads were rerun.

### Runtime Change and Boundaries

Recipe's initial recovery stage now opens **one readonly transaction instead
of two**, reads a live table directory once, then reads the ledger and, only
when needed, the latest ready task in that same stage. Terminal replay skips
the task query; no stable event identity skips the ledger data query. Recipe
hash conflicts, pending conflicts, failed/reconcile retry, actor/recipe/task
ordering and missing tables retain their previous behavior. Standalone read
APIs open fresh transactions, and write/failure recovery still rereads after
settlement. There is no await or write inside the combined read stage.

Bank reads the live directory once instead of up to five table-existence
queries. Unified receipt precedence, all legacy rows/conflict detection,
exact-case live column validation, exact numeric identity and SQL errors
remain unchanged. The full five-table miss/legacy search removes four
metadata calls; a successful unified receipt does **not** save four calls.
Account balances, current eligibility, rewards, ledger/CAS/replay and all
write/commit boundaries remain live and unchanged. No cache/index or generic
table/column mutation API is added.

The small shared `read_sqlite_table_names` helper performs a fixed live SQL
query. Existing bounded cached schema APIs retain their policies. A candidate
cache integration failed existing legacy-receipt tests: deleted/recreated
temporary databases can reuse path/inode/schema_version. A file timestamp
alone also cannot bind metadata from an already-open old connection to its
replacement path. The final implementation therefore uses the live helper,
with an active-old-connection replacement regression, and leaves that broader
existing cache identity limitation explicit. No assertion was weakened and
no candidate code was benchmarked or pushed.

Final affected acceptance passed **169 tests +176 subtests in27.35s**,
including registered recipe success/replay/failure recovery, bank command
application/handler/legacy/CAS/double-interest behavior, large-number receipts,
read failures, missing/changed schema, task ordering and query/transaction
counts. Compile, architecture, inventory and scoped whitespace passed.
Local full unittest and CI/test-sharding changes were not repeated.

### Frozen Same-Data Comparison

The sole retained1M synthetic fixture remains
`/tmp/xiu-runtime-command-matrix-20261125-r01/baseline-r05`.
Baseline source is `244cf6bc`; accepted code is archived at
`/tmp/xiu-v27-accept-Q2gK7q5u`. Driver/fixture/oracle, requests, manifests,
rank digest and limits match exactly across both versions. Runtime hashes:
`86417951c9fb0fc4e0e71d6fedb102b66d4b976db7100abafd650cb16a99317c`
before and
`cd91aae5cb572bb067b0b4ac8d09ecc40244cb950ca3f589f03c85092db9b2dd`
after. Only the five runtime files in this batch differ.

Reuse of the existing inventory scenario measures the **new recipe recovery
IO change**, not another claim for V26 inventory optimization. Per version,
three serial stages each execute128 requests: four display controls,32 real
absent-furnace recipe refusals and, in mixed/contention,32 real exp+stone
training writes.128 actors retain213 positive items each,27,264 total back
rows,128 BuffInfo,8192 activity users and32,768 rows in each activity balance/
damage control table. Recipe task/specific replay data are empty in the timed
sample; populated claim/replay/conflict has regressions only. Bank has
query-count/correctness evidence only, not measured mixed-bank latency or
capacity. No new million-user population or persistent fixture copy exists.

Six raw reports all pass without offline outcome corrections:
`recovery-stage-{before,after}-v27-20261127-phase{0,1,2}-r01.json`, under fixture
`data/`; derived report
`recovery-stage-comparison-v27-20261127-r01.json` records their hashes, all
commands, diagnostics and resources. Every phase runs once. Each version has
288 success+96 furnace-absent business refusals, zero exceptions/timeouts/
cooldowns/unclassified/boundary/SQLite busy/locked errors. Refusals are excluded
from successful throughput. All exact wallet/exp/power, inventory/BuffInfo,
receipt/ledger/generation,1M population and original-rank restoration checks
pass after drain; no restore occurs between concurrent requests.

### All Measured Paths

E2E milliseconds include arrival/dispatch/admission delay. Each entry is
before -> after; DB totals are execute+fetch milliseconds, and query counts
include the harness's transaction/PRAGMA API calls, not only SELECTs.

|Stage/path|n/version|E2E p50|E2E p95|E2E p99|DB calls|DB total ms|
|---|---:|---:|---:|---:|---:|---:|
|pure/equipment|24|8.583 ->8.422|11.124 ->10.442|18.895 ->18.669|288 ->288|14.141 ->12.500|
|pure/herb|24|8.895 ->8.791|10.295 ->9.841|37.921 ->27.211|288 ->288|13.931 ->14.841|
|pure/herb detail|24|8.724 ->8.481|13.724 ->10.629|15.714 ->16.176|288 ->288|15.025 ->14.117|
|pure/pill|24|9.019 ->8.627|11.873 ->9.562|73.780 ->62.564|313 ->313|27.716 ->27.304|
|pure/recipe|32|19.250 ->14.991|26.652 ->22.772|30.044 ->25.558|768 ->608|255.081 ->160.055|
|mixed/equipment|16|8.679 ->8.574|17.350 ->16.550|17.350 ->16.550|192 ->192|8.995 ->10.116|
|mixed/herb|16|9.048 ->9.188|31.226 ->28.872|31.226 ->28.872|192 ->192|10.270 ->10.968|
|mixed/herb detail|16|8.394 ->8.568|23.466 ->29.132|23.466 ->29.132|192 ->192|11.389 ->12.327|
|mixed/pill|16|8.847 ->9.048|66.977 ->64.378|66.977 ->64.378|217 ->217|24.037 ->21.425|
|mixed/recipe|32|19.337 ->14.906|28.359 ->15.944|29.276 ->25.131|768 ->608|249.189 ->150.145|
|mixed/training|32|35.041 ->34.361|63.215 ->56.573|63.616 ->60.548|1219 ->1219|163.541 ->156.742|
|contention/equipment|16|8.884 ->8.658|211.632 ->42.564|211.632 ->42.564|192 ->192|9.212 ->7.414|
|contention/herb|16|9.527 ->8.457|307.735 ->59.922|307.735 ->59.922|192 ->192|8.629 ->7.147|
|contention/herb detail|16|13.490 ->14.075|222.612 ->70.866|222.612 ->70.866|192 ->192|12.459 ->10.858|
|contention/pill|16|15.939 ->13.497|336.179 ->69.297|336.179 ->69.297|204 ->217|15.558 ->22.488|
|contention/recipe|32|18.877 ->14.239|188.712 ->25.496|278.790 ->46.196|781 ->608|260.151 ->153.050|
|contention/training|32|37.135 ->35.913|248.318 ->67.244|250.979 ->69.699|1219 ->1219|149.292 ->150.132|

All path execution error rates are0%; recipe's100% business refusal rate is
reported separately. Foreground calls total7505 ->7025, saving480. Recipe
recovery connections192 ->96 and directory calls192 ->96 across96 requests.
Complete-path DB time764.421 ->463.250ms; recovery metadata time
656.420 ->373.267ms. The metadata interval includes first-query SQLite schema
parsing; it is not all directory scanning, nor Python function-call savings.
Per-stage metadata p50/p95 is6.767/7.891 ->3.833/5.456,
6.733/7.298 ->3.744/3.998 and6.657/8.163 ->3.685/3.914ms.
Connections/metadata attribution uses complete, untruncated opened recovery
connection diagnostics; the separate guard's cold catalog is not charged to
recovery. In contention its13 guard calls move from recipe to pill attribution;
both full phases still save exactly160 foreground calls. Message-writer
background batching573 ->580 calls varies independently of foreground work.
Process cache counters (hits/misses/invalidations/recomputations) are all0
in these stages; no cache-hit benefit is claimed and no exact-rank work occurs.

Mixed herb-detail p95 **regresses23.466 ->29.132ms**, mixed herb/detail/pill
medians rise, pure herb SQL/p99 detail and contention pill/training SQL also
retain regressions. The larger contention tail improvement accompanies
unchanged controls improving too: this one short sequential sample does not
isolate all application/scheduler/observer variance or prove every path faster.
The measured recipe SQL/query/connection reduction is the narrower result.

### Load, Resources and Limits

|Stage/cap/arrivals per second|Whole E2E p95|Successful/s|Handler overlap peak|Inflight peak|
|---|---:|---:|---:|---:|
|pure/10/20|20.590 ->17.017|15.098 ->15.096|1 ->1|2 ->2|
|mixed/20/20|43.241 ->35.561|15.040 ->15.039|1 ->1|2 ->2|
|contention/20/35|222.612 ->65.450|26.209 ->26.205|7 ->2|12 ->3|

Successful throughput is **not improved** and does not establish20 active
handlers or sustained50 successful requests/s. Producer delay p95/p99:
pure.138/.201 ->.136/.156ms, mixed.127/.156 ->.139/.309ms (regression),
contention8.957/78.836 ->.249/3.630ms. Loop-dispatch p95/p99:
.190/5.993 ->.179/.254, .251/9.666 ->.202/6.979,
29.805/63.809 ->20.706/33.861ms. Event-loop lag p95/p99:
6.527/12.072 ->2.298/8.473,16.658/28.742 ->19.665/38.842 (regression),
37.466/63.434 ->28.277/44.814ms. Admission queue p95 remains.010-.011ms.
BEGIN timing is an upper bound including execution, not a pure lock-wait
measurement; raw per-request waits and zero busy/locked counts are retained.

The shared validation lock serializes all heavy work. Every process enforces
2/4 CPU,AS1GiB,CPU600s,wall1200s,`MALLOC_ARENA_MAX=2`,RSS900MiB stop,
RAM>=768MiB,disk>=2GiB and single/total WAL<=256MiB; no outer taskset or
second load group is used. Phase minimum RAM1533MiB,disk11,143,176,192bytes,
single WAL18,321,672bytes,total WAL22,507,624bytes; no guard is reached.
Phase RSS208.434/221.035/221.742 ->218.723/218.957/219.148MiB retains
the pure increase. Whole-process peak231.367MiB includes setup/check/restore.
Phase user+system CPU1.919/2.761/2.764 ->1.764/2.590/2.522s;
main-thread CPU1450.785/2244.730/2327.998 ->1301.001/2080.434/2065.603ms.

Phase setup5.154-5.541s before/5.218-5.314s after, startup5.529-6.630s
and final restore13.703-14.875s are outside request E2E. Sampled phase disk
writes13,594,624/21,266,432/20,365,312 ->13,594,624/21,270,528/20,824,064bytes
retain increases. Per-process writes around1.93-1.94GB include temporary
snapshot/restore IO; do not attribute them to application transactions.
Profiler sections remain incomplete, perturb timing and cannot be subtracted
to manufacture uninstrumented throughput.

This covers six measured routes and two changed matcher entrypoints, not
all674 matchers/41 background entries. Large successful refinement/replay
datasets, bank latency, broader exp+stone global-invalidation contention,
cold exact ranks and sustained50 successful/s remain unproven. The overall
repository performance goal remains **ACTIVE** after this bounded delivery.
