"""Entry point for TwitchMultiMiner (GUI dashboard by default or --headless CLI mode)."""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

if sys.platform == "win32":
    for _stream in (sys.stdout, sys.stderr):
        if _stream is not None and hasattr(_stream, "reconfigure"):
            try:
                _stream.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass
    if "TCL_LIBRARY" not in os.environ:
        _tcl_path = os.path.join(sys.base_prefix, "tcl", "tcl8.6")
        if os.path.exists(_tcl_path):
            os.environ["TCL_LIBRARY"] = _tcl_path
    if "TK_LIBRARY" not in os.environ:
        _tk_path = os.path.join(sys.base_prefix, "tcl", "tk8.6")
        if os.path.exists(_tk_path):
            os.environ["TK_LIBRARY"] = _tk_path

import truststore

truststore.inject_into_ssl()

from constants import APP_LOG_PATH, APP_NAME, APP_VERSION, FILE_FORMATTER
from orchestrator import MultiAccountOrchestrator


class _SafeStreamHandler(logging.StreamHandler):
    """Stream handler that never raises UnicodeEncodeError on narrow Windows codepages."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            stream = self.stream
            if stream is None:
                return
            try:
                stream.write(msg + self.terminator)
            except UnicodeEncodeError:
                enc = getattr(stream, "encoding", None) or "ascii"
                safe_msg = msg.encode(enc, errors="replace").decode(enc, errors="replace")
                stream.write(safe_msg + self.terminator)
            self.flush()
        except RecursionError:
            raise
        except Exception:
            self.handleError(record)


def setup_logging(*, verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger("TwitchMultiMiner")
    logger.setLevel(level)
    if not logger.handlers:
        file_handler = logging.FileHandler(APP_LOG_PATH, encoding="utf8", errors="replace")
        file_handler.setFormatter(FILE_FORMATTER)
        logger.addHandler(file_handler)

        if sys.stderr is not None:
            stream_handler = _SafeStreamHandler(sys.stderr)
            stream_handler.setFormatter(
                logging.Formatter("{asctime} [{levelname}] {message}", style="{")
            )
            logger.addHandler(stream_handler)


async def run_headless(orchestrator: MultiAccountOrchestrator) -> None:
    if not orchestrator.workers:
        print("Нет настроенных аккаунтов в папке profiles/. Используйте --import-legacy или GUI.")
        return
    orchestrator.start_all()
    print(f"{APP_NAME} v{APP_VERSION} запущен в headless-режиме. Аккаунтов: {len(orchestrator.workers)}.")
    try:
        while True:
            await asyncio.sleep(5)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await orchestrator.stop_all()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=f"{APP_NAME} — Multi-account Twitch Drops Farmer")
    parser.add_argument("--headless", action="store_true", help="Run without GUI window")
    parser.add_argument("--tray", action="store_true", help="Start minimized to system tray")
    parser.add_argument("--autostart", action="store_true", help="Start all enabled profiles on launch")
    parser.add_argument(
        "--import-legacy",
        action="store_true",
        help="Import cookies from sibling ../TwitchDropsMiner if profiles are empty",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args(argv)

    setup_logging(verbose=args.verbose)
    orchestrator = MultiAccountOrchestrator()
    if args.import_legacy or not orchestrator.workers:
        orchestrator.import_from_legacy_miner()

    if args.headless:
        try:
            asyncio.run(run_headless(orchestrator))
        except KeyboardInterrupt:
            pass
        return 0

    from gui_dashboard import AsyncLoopThread, MultiMinerDashboard

    loop_thread = AsyncLoopThread()
    dashboard = MultiMinerDashboard(orchestrator, loop_thread, start_in_tray=args.tray)
    if args.autostart and orchestrator.workers:
        loop_thread.run_coro(_async_start(orchestrator))
    dashboard.run()
    return 0


async def _async_start(orchestrator: MultiAccountOrchestrator) -> None:
    orchestrator.start_all()


if __name__ == "__main__":
    sys.exit(main())
