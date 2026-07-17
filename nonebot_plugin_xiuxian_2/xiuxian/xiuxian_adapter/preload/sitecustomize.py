"""Python 启动时自动注入 vendor 适配器路径。

用法（可继续 nb run --reload）::

    export PYTHONPATH="/path/to/nonebot_plugin_xiuxian_2/xiuxian/xiuxian_adapter/preload${PYTHONPATH:+:$PYTHONPATH}"
    nb run --reload

子进程（reload 热重启）会继承 PYTHONPATH，因此 Intent.group_members 默认 True 会生效。
"""

from __future__ import annotations


def _boot() -> None:
    try:
        # preload 在 .../xiuxian_adapter/preload，early_inject 在上一级
        import sys
        from pathlib import Path

        here = Path(__file__).resolve().parent
        adapter_dir = here.parent
        if str(adapter_dir) not in sys.path:
            sys.path.insert(0, str(adapter_dir))

        from early_inject import inject_vendor_adapter_paths

        injected = inject_vendor_adapter_paths()
        if not injected:
            return
        # 可选日志：尽量不依赖 nonebot logger（此时可能尚未 init）
        try:
            from nonebot.adapters.qq import config as qq_config

            gm = qq_config.Intents.model_fields.get("group_members")
            default = getattr(gm, "default", None) if gm is not None else None
            print(
                f"[xiuxian_adapter.preload] vendor injected={len(injected)} "
                f"qq_config={getattr(qq_config, '__file__', '?')} "
                f"group_members_default={default}",
                flush=True,
            )
        except Exception:
            print(
                f"[xiuxian_adapter.preload] vendor injected={len(injected)}",
                flush=True,
            )
    except Exception as e:
        print(f"[xiuxian_adapter.preload] inject failed: {e}", flush=True)


_boot()
