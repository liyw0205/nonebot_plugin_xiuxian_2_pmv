# Architecture Acceptance

Acceptance date: 2026-07-10

This record closes the staged work described by the local architecture review.
It intentionally contains no Bot credentials, user IDs, group IDs, or local
deployment values.

## Foundation

- Runtime data paths are resolved through `XiuxianPaths`; source-quality tests
  reject new business code that recreates `Path() / "data" / "xiuxian"`.
- Dependency installation, resource download, database initialization, and
  maintenance run from explicit startup lifecycle hooks rather than ordinary
  module import. Shutdown drains the managed queues.
- `UserRepository` and `EconomyRepository` provide the first stable data-access
  boundary while `XiuxianDateManage` remains a compatibility facade.
- Web modules use explicit imports, startup-controlled server creation, and the
  same message delivery service and Bot selector as the NoneBot runtime.
- JSON state, synchronous/asynchronous HTTP access, time formatting, settings,
  TTL state, metrics, and bounded jobs each have one shared implementation and
  focused tests.

## Adapter Provenance

| Adapter | Upstream commit | Version | License |
| --- | --- | --- | --- |
| QQ | `d8db0a7f5ab195c1415fa01fed70bd0cb89c6ec1` | 1.7.1 | MIT |
| OneBot | `6fe01137868375afdb73a1c31e0c72dee1249703` | 2.4.6 | MIT |

The corresponding `vendor/*/UPSTREAM` and `vendor/*/LICENSE` files are the
source of truth. Runtime selection supports `vendor`, `installed`, and `auto`,
and diagnostics report both the requested mode and imported module source.

## QQ Capability Acceptance

- `QQEventContext` normalizes group, C2C, channel, interaction, and lifecycle
  events without exposing concrete Adapter event classes to business code.
- The same real-event fixture and `Bot.send` routing contract runs in isolated
  processes against vendored QQ and installed `nonebot-adapter-qq==1.7.1`.
- REFIDX, message IDs, send results, recording, revocation, and audit follow-up
  use the shared compatibility and delivery extraction paths.
- New Web and presenter sends use `MessageDeliveryService`; remaining direct
  Adapter paths are frozen by `docs/message_delivery_migration.md` and a
  source-quality allowlist.
- Interaction ACK has timeout fallback, failure retry, and exactly-once guards.
- Event deduplication is isolated by Bot and reliable event identity; critical
  queue submissions wait for capacity rather than dropping economic work.
- Markdown, keyboard, lifecycle, and media enhancements use capability checks
  and preserve text or core-game fallback behavior.
- Per-AppID capability selection, stable multi-Bot selection, bounded template
  reload, normalized media input, content-addressed cache, SSRF checks, size
  limits, retry, and cleanup are covered by unit tests.
- No additional Bot runtime, Webhook transport, plugin scheduler, Bot registry,
  Web panel, or general hook framework was introduced.

## Verification

Run from the repository root with the project environment:

```sh
python -m unittest tests.test_qq_adapter_contracts -v
python -m unittest discover -s tests -v
python -m compileall -q nonebot_plugin_xiuxian_2 tests
git diff --check
```

At this acceptance baseline, all 110 discovered tests pass. The contract matrix
reports `vendor` and `installed` for their explicit runs, and `auto` resolves to
the installed QQ Adapter when the pinned package is available.
