"""Multi-account orchestrator managing all AccountWorkers, shared radar loop, and legacy import."""
from __future__ import annotations

import asyncio
import logging
import shutil
import time
from collections import deque
from contextlib import suppress
from pathlib import Path
from typing import Any, Callable

from constants import PROFILES_DIR, WORKING_DIR
from radar import SharedCampaignRadar
from settings import AccountProfileConfig, GlobalSettings
from worker import AccountSnapshot, AccountWorker, ClaimedDropRecord


logger = logging.getLogger("TwitchMultiMiner.orchestrator")


class MultiAccountOrchestrator:
    """Runs all account workers and the shared campaign radar in a single asyncio event loop."""

    def __init__(
        self,
        *,
        profiles_dir: Path = PROFILES_DIR,
        global_settings: GlobalSettings | None = None,
    ) -> None:
        self.profiles_dir: Path = profiles_dir
        self.profiles_dir.mkdir(parents=True, exist_ok=True)
        self.global_settings: GlobalSettings = global_settings or GlobalSettings.load()
        self.radar = SharedCampaignRadar(self.global_settings)

        self.workers: dict[str, AccountWorker] = {}
        self.global_logs: deque[str] = deque(maxlen=500)
        self.claimed_history: deque[ClaimedDropRecord] = deque(maxlen=500)

        self._on_change_callbacks: list[Callable[[], None]] = []
        self._radar_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._last_log_emit_ts: float = 0.0
        self._log_emit_handle: asyncio.TimerHandle | None = None

        self.discover_profiles()

    def add_change_listener(self, cb: Callable[[], None]) -> None:
        if cb not in self._on_change_callbacks:
            self._on_change_callbacks.append(cb)

    def _emit_change(self) -> None:
        for cb in list(self._on_change_callbacks):
            try:
                cb()
            except Exception:
                logger.exception("Error in orchestrator change listener %r", cb)

    def _flush_log_emit(self) -> None:
        self._log_emit_handle = None
        self._last_log_emit_ts = time.monotonic()
        self._emit_change()

    def _handle_worker_update(self, _worker: AccountWorker) -> None:
        self._emit_change()

    def _handle_worker_log(self, _name: str, line: str) -> None:
        self.global_logs.append(line)
        now = time.monotonic()
        elapsed = now - self._last_log_emit_ts
        if elapsed >= 0.15 and self._log_emit_handle is None:
            self._last_log_emit_ts = now
            self._emit_change()
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self._last_log_emit_ts = now
            self._emit_change()
            return
        if self._log_emit_handle is None:
            delay = max(0.01, 0.15 - elapsed)
            self._log_emit_handle = loop.call_later(delay, self._flush_log_emit)

    def _handle_worker_claim(self, record: ClaimedDropRecord) -> None:
        self.claimed_history.appendleft(record)
        self._emit_change()

    def _create_worker(self, cfg: AccountProfileConfig) -> AccountWorker:
        return AccountWorker(
            cfg,
            self.global_settings,
            self.radar,
            profiles_dir=self.profiles_dir,
            on_update=self._handle_worker_update,
            on_log=self._handle_worker_log,
            on_claim=self._handle_worker_claim,
        )

    def discover_profiles(self) -> list[str]:
        """Scan profiles_dir for existing account folders and load their workers."""
        self.profiles_dir.mkdir(parents=True, exist_ok=True)
        found: list[str] = []
        for entry in sorted(self.profiles_dir.iterdir(), key=lambda p: p.name.lower()):
            if not entry.is_dir():
                continue
            name = entry.name
            found.append(name)
            if name not in self.workers:
                cfg = AccountProfileConfig.load(name, self.profiles_dir)
                self.workers[name] = self._create_worker(cfg)
        return found

    @staticmethod
    def validate_profile_name(name: str) -> str:
        clean = name.strip()
        if not clean or not all(c.isalnum() or c in "-_ " for c in clean):
            raise ValueError("Имя профиля может содержать только буквы, цифры, пробел, '-' и '_'.")
        return clean

    def add_profile(self, name: str, *, proxy: str = "", start_immediately: bool = False) -> AccountWorker:
        clean = self.validate_profile_name(name)
        if clean in self.workers:
            raise ValueError(f"Профиль '{clean}' уже существует.")
        cfg = AccountProfileConfig.load(clean, self.profiles_dir)
        cfg.proxy = proxy.strip()
        cfg.save(self.profiles_dir)
        worker = self._create_worker(cfg)
        self.workers[clean] = worker
        if start_immediately:
            worker.start()
        self._emit_change()
        return worker

    async def remove_profile(self, name: str) -> None:
        clean = self.validate_profile_name(name)
        worker = self.workers.pop(clean, None)
        if worker is not None:
            await worker.stop()
        base_dir = self.profiles_dir.resolve()
        target_dir = (self.profiles_dir / clean).resolve()
        if not target_dir.is_relative_to(base_dir) or target_dir == base_dir:
            raise ValueError(f"Некорректный путь профиля: {name!r}")
        if target_dir.exists() and target_dir.is_dir():
            shutil.rmtree(target_dir, ignore_errors=True)
        self._emit_change()

    def update_profile_proxy(self, name: str, proxy: str) -> None:
        worker = self.workers.get(name)
        if worker is None:
            return
        worker.config.proxy = proxy.strip()
        worker.config.save(self.profiles_dir)
        self._emit_change()

    def import_from_legacy_miner(self, legacy_root: Path | None = None) -> list[str]:
        """Copy cookies.jar from sibling TwitchDropsMiner without touching original files."""
        if legacy_root is None:
            legacy_root = WORKING_DIR.parent / "TwitchDropsMiner"
        if not legacy_root.is_dir():
            return []

        imported: list[str] = []
        root_cookie = legacy_root / "cookies.jar"
        if root_cookie.is_file():
            target_name = "main_account"
            target_cookie = AccountProfileConfig.cookies_path(target_name, self.profiles_dir)
            if not target_cookie.exists():
                target_cookie.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(root_cookie, target_cookie)
                AccountProfileConfig.load(target_name, self.profiles_dir)
                imported.append(target_name)

        legacy_profiles = legacy_root / "profiles"
        if legacy_profiles.is_dir():
            for p_dir in sorted(legacy_profiles.iterdir(), key=lambda p: p.name.lower()):
                if not p_dir.is_dir():
                    continue
                cookie_src = p_dir / "cookies.jar"
                target_name = p_dir.name
                target_cookie = AccountProfileConfig.cookies_path(target_name, self.profiles_dir)
                if cookie_src.is_file() and not target_cookie.exists():
                    target_cookie.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(cookie_src, target_cookie)
                    AccountProfileConfig.load(target_name, self.profiles_dir)
                    imported.append(target_name)
                elif not (self.profiles_dir / target_name).exists():
                    AccountProfileConfig.load(target_name, self.profiles_dir)
                    imported.append(target_name)

        self.discover_profiles()
        if imported:
            self.global_logs.append(f"[IMPORT] Импортированы аккаунты из TwitchDropsMiner: {', '.join(imported)}")
            self._emit_change()
        return imported

    def start_background_radar(self) -> None:
        if self._radar_task is None or self._radar_task.done():
            self._stop_event.clear()
            self._radar_task = asyncio.create_task(self._radar_loop())

    def start_profile(self, name: str) -> None:
        self.start_background_radar()
        worker = self.workers.get(name)
        if worker is not None:
            worker.config.enabled = True
            worker.config.save(self.profiles_dir)
            worker.start()

    async def stop_profile(self, name: str) -> None:
        worker = self.workers.get(name)
        if worker is not None:
            await worker.stop()

    def start_all(self) -> None:
        self.start_background_radar()
        for worker in self.workers.values():
            if worker.config.enabled and not worker.running:
                worker.start()

    async def stop_all(self) -> None:
        self._stop_event.set()
        if self._radar_task is not None:
            self._radar_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._radar_task
            self._radar_task = None
        await asyncio.gather(*(w.stop() for w in self.workers.values()), return_exceptions=True)
        self._emit_change()

    async def force_radar_refresh(self) -> dict[str, Any]:
        """Force immediate radar check across Twitch GQL and public catalog, waking all active workers."""
        authed_worker = self._pick_authenticated_worker()
        found_new = False
        if authed_worker is not None:
            found_new = await self.radar.poll_pinned_streamers(authed_worker, force=True)
        await self.radar.get_catalog_campaigns(
            proxy_url=authed_worker.proxy_url if authed_worker else "",
            force=True,
        )
        for worker in self.workers.values():
            if worker.running:
                worker.trigger_inventory_refresh()
        self._emit_change()
        return {
            "ok": authed_worker is not None,
            "polled": authed_worker is not None,
            "found_new": found_new,
        }

    def _pick_authenticated_worker(self) -> AccountWorker | None:
        for worker in self.workers.values():
            if worker.running and worker.is_authenticated:
                return worker
        return None

    async def _radar_loop(self) -> None:
        while not self._stop_event.is_set():
            interval = max(30, self.global_settings.radar_interval_sec)
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
            if self._stop_event.is_set():
                break
            authed = self._pick_authenticated_worker()
            if authed is None:
                continue
            try:
                found_new = await self.radar.poll_pinned_streamers(authed, force=True)
                await self.radar.get_catalog_campaigns(proxy_url=authed.proxy_url, force=False)
                if found_new:
                    self.global_logs.append("[RADAR] Обнаружены новые дропы! Обновляю все аккаунты...")
                    for worker in self.workers.values():
                        if worker.running:
                            worker.trigger_inventory_refresh()
                self._emit_change()
            except Exception as exc:
                logger.warning(f"[RADAR] Loop error: {exc}")

    def save_global_settings(self) -> None:
        self.radar.sync_pinned_list()
        self.global_settings.save()
        for worker in self.workers.values():
            if worker.running:
                worker.trigger_inventory_refresh()
        self._emit_change()

    def snapshots(self) -> list[AccountSnapshot]:
        return [w.snapshot() for w in self.workers.values()]
