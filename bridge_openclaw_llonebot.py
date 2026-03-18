#!/usr/bin/env python3
import asyncio
import contextlib
import logging
import signal
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().with_name(".env"))

from bridge_config import Config
from bridge_core import OpenClawOneBotBridge


async def main() -> None:
    cfg = Config()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    cfg.validate_required_tokens()
    bridge = OpenClawOneBotBridge(cfg)
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    registered_signals: list[signal.Signals] = []

    def _request_shutdown(sig_name: str) -> None:
        if stop_event.is_set():
            return
        logging.info("runtime.signal stage=received name=%s", sig_name)
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
        logging.info("runtime status=stopped reason=keyboard_interrupt")
    except ValueError as exc:
        logging.error("startup status=failed err=%s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    cli()
