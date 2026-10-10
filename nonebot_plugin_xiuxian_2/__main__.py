import sys

from nonebot.log import logger


# Maintenance responses are JSON; gameplay import logs must not precede them.
if len(sys.argv) > 1 and sys.argv[1] in {"manifest", "health", "migrate", "reconcile", "backup", "restore"}:
    logger.disable(__package__)

from .cli import main


if __name__ == "__main__":
    raise SystemExit(main())
