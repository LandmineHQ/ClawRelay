#!/usr/bin/env python3
import asyncio
import contextlib
import logging
import signal

from dotenv import load_dotenv

load_dotenv()

from bridge_config import Config
from bridge_core import OpenClawOneBotBridge


async def main() -> None:
    cfg = Config()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    bridge = OpenClawOneBotBridge(cfg)
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    registered_signals: list[signal.Signals] = []

    def _request_shutdown(sig_name: str) -> None:
        if stop_event.is_set():
            return
        logging.info("Shutdown signal received: %s", sig_name)
        stop_event.set()

    for sig_name in ("SIGINT", "SIGTERM", "SIGHUP"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, _request_shutdown, sig_name)
            registered_signals.append(sig)
        except NotImplementedError:
            # Fallback for platforms/event loops that don't support add_signal_handler.
            def _fallback_handler(_signum: int, _frame: object, name: str = sig_name) -> None:
                loop.call_soon_threadsafe(_request_shutdown, name)

            signal.signal(sig, _fallback_handler)

    run_task = asyncio.create_task(bridge.run(), name="bridge-run")
    stop_task = asyncio.create_task(stop_event.wait(), name="bridge-stop-wait")
    try:
        done, _ = await asyncio.wait(
            {run_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task in done and not run_task.done():
            run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_task
    finally:
        stop_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stop_task
        for sig in registered_signals:
            with contextlib.suppress(Exception):
                loop.remove_signal_handler(sig)


def cli() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bridge stopped by user.")


if __name__ == "__main__":
    cli()
