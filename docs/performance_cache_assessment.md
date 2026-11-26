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
