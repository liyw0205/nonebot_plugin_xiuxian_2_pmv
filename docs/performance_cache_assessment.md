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

## Consolidated After Matrix

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
| `我的修仙信息` | Realtime | The response contains the caller's exact current rank among all users. The rank query was about 358 ms/call on the 1M baseline; a public top-50 snapshot cannot answer arbitrary viewer rank without changing freshness/meaning. |
| `get_realm_top1_user` target-selection helper | Realtime | Used to select a gameplay/BOSS target; a display snapshot must not influence an action target. |
| `竞技场排行榜` | Realtime top rows plus viewer rank | Same-response live position must agree with the displayed top ordering; a stale top slice could show the player below their current position. Names are batched. |
| `活动排行` | 60-minute shared snapshot for public display rows; viewer rank and reward tier remain realtime | `活动排行 <榜单>` labels cached rows with their UTC snapshot time and maximum one-hour age, then renders the current personal position separately. Claim execution rechecks current rank, claims, and balance. The no-argument overview has only live personal fields and no cached row list. |
| `活动排行领取` / `活动首领领奖` | Realtime | Mutating reward/claim decisions must use current rank, claims, and balance. Never reuse display snapshots for eligibility. |
| `活动首领排行` | 60-minute shared snapshot of display rows | A pure public damage board keyed by activity key and limit. The command renders no viewer rank or claim decision; personalized activity status/claims use separate realtime paths. Snapshot semantics permit showing the last hour's damage ordering. Explicit activity-data reset invalidates the process snapshot after commit so a reset is visible immediately. |
| `世界BOSS积分排行榜` | Realtime top rows and viewer rank | The same response appends current rank and points. A cached top slice could place a player below a live personal rank (or omit the new leader); names are batch-loaded and both rank reads remain live. |
| `虚神界排行榜` | 60-minute shared snapshot for TOP50 data | Public display-only list. Personal challenger/target decisions are not sourced from this cached list. User display names are looked up in one realtime batch per cache miss. |
| `宗门战力排行榜` | 60-minute shared snapshot | Pure public sector ordering, not used for eligibility. The view is small, but same-process users share the snapshot. |
| `宗门周常排行` | 60-minute snapshot keyed by week | Pure public aggregate. The handler computes the week key once and uses it for both cache/query and rendered period, preventing a rollover race from labeling one week's rows as another. Progress can be stale within that week by the declared snapshot contract. |
| `宗门列表` | Realtime | The displayed join-open/closed/full status comes from current eligibility reads; stale rows can direct a player toward a sect they cannot join. The repository projection is also used by scheduled owner-maintenance paths, so no low-level shared cache is added. |
| `我的宗门` scale-rank projection | Realtime | `SectDirectorySqlRepository.list_scale_rank` is composed into a personalized sect response and supplies the caller's displayed rank; stale scale rows can misstate the viewer's current position. The separate season-preview query uses the shared snapshot. |
| `历练排行榜` / `历练积分排行榜` | 60-minute shared display snapshot; viewer rank remains realtime | The response labels public rows with their UTC snapshot time and maximum one-hour age, then computes the caller's exact current rank through the live aggregate query. Display names are joined in one SQL statement. |
| `通天塔排行榜` / `通天塔积分排行榜` | 60-minute shared display snapshot; viewer rank remains realtime | The response labels public rows with their UTC snapshot time and maximum one-hour age, then computes the caller's exact current rank through the live aggregate query. Names are batch-loaded. |
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
  the separate personal rank remains a current read.
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
  their focused repository tests. `tests/test_shared_read_cache.py` covers
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

The after process was capped at 1 GiB address space, two of four CPUs, 600
seconds CPU and 1,200 seconds wall time; it used 112.727 CPU seconds, 253.083
seconds wall time, and 194.387 MiB peak RSS. It retained a 1,000,000-row
`user_xiuxian` fixture, with auxiliary feature tables at the row counts recorded
in the JSON report. No duplicate 1M fixture was made for the after run. Future
performance claims require the same-fixture comparison and these resource guards;
passing correctness tests alone is not performance evidence.
