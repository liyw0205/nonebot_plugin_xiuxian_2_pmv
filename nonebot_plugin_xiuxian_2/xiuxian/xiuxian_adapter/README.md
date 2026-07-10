# Xiuxian Adapter Vendor

This directory vendors the runtime source needed by `adapter_compat.py` for:

- `nonebot/adapter-qq`
- `nonebot/adapter-onebot`

The upstream packages keep their canonical module names under `nonebot.adapters.*`.
`xiuxian_adapter.configure_adapter_paths()` selects the runtime source while
keeping upstream canonical imports under `nonebot.adapters.*`.

Configure `xiuxian_adapter_source` in NoneBot or `XIUXIAN_ADAPTER_SOURCE` in the
environment:

- `vendor` (default): prefer bundled sources for local modification.
- `installed`: use only adapters installed in the active Python environment.
- `auto`: prefer installed adapters and fall back to bundled sources per adapter.

`xiuxian_adapter.diagnostics.get_adapter_diagnostics()` reports the requested
mode, effective source, imported file and version for OneBot and QQ.

The QQ contract matrix in `tests/test_qq_adapter_contracts.py` executes the
same group, C2C, channel, interaction, lifecycle and reply-routing fixtures
against both the bundled source and `nonebot-adapter-qq==1.7.1`. It also checks
that `auto` diagnostics report the source that was actually imported.

Do not edit vendored upstream files directly unless the change is documented in
the corresponding `UPSTREAM` file. Each file records the upstream repository,
commit, version, vendored date and local changes, so upstream updates can be
compared and merged selectively instead of overwriting local work.
