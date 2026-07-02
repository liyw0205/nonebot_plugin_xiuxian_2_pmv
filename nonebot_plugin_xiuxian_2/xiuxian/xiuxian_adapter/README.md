# Xiuxian Adapter Vendor

This directory vendors the runtime source needed by `adapter_compat.py` for:

- `nonebot/adapter-qq`
- `nonebot/adapter-onebot`

The upstream packages keep their canonical module names under `nonebot.adapters.*`.
`xiuxian_adapter.ensure_vendored_adapters()` extends `nonebot.adapters.__path__`
so `adapter_compat.py` can import the bundled adapters without rewriting upstream
source files.

Do not edit vendored upstream files directly unless the change is documented in
the corresponding `UPSTREAM` file.
