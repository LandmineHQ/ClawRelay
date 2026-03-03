#!/usr/bin/env python3
import asyncio
import logging

from bridge_config import Config
from bridge_core import OpenClawOneBotBridge


async def main() -> None:
    cfg = Config()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    bridge = OpenClawOneBotBridge(cfg)
    await bridge.run()


def cli() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    cli()
