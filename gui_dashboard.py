"""pywebview (WebView2) desktop shell and Win32 system tray bridge for TwitchMultiMiner."""
from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import logging
import re
import shutil
import threading
import time
import webbrowser
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import aiohttp

from constants import (
    APP_NAME,
    APP_VERSION,
    DEFAULT_AUTO_PRIORITY_KEYWORDS,
    DEFAULT_PINNED_STREAMERS,
    DEFAULT_PRIORITY_GAMES,
    ICONS_DIR,
    PriorityMode,
    WORKING_DIR,
)
from tray_win32 import Win32TrayController
from utils import make_connector

try:
    import webview
except ImportError:  # pragma: no cover - verified by tests if missing
    webview = None

try:
    import win32clipboard
    import win32con
except ImportError:  # pragma: no cover
    win32clipboard = None
    win32con = None

if TYPE_CHECKING:
    from orchestrator import MultiAccountOrchestrator

logger = logging.getLogger("TwitchMultiMiner.gui")
UI_HTML_PATH = WORKING_DIR / "ui" / "dashboard.html"
IMAGE_CACHE_DIR = UI_HTML_PATH.parent / "cache" / "images"
LEGACY_IMAGE_CACHE_DIR = WORKING_DIR / "cache" / "images"

_BENEFIT_LABELS = {
    "DIRECT_ENTITLEMENT": "Предмет",
    "BADGE": "Бейдж",
    "EMOTE": "Эмоция",
    "UNKNOWN": "Награда",
}


def _normalize_image_url(url: str) -> str:
    clean = str(url or "").strip()
    if not clean:
        return ""
    if "{width}" in clean or "{height}" in clean:
        clean = clean.replace("{width}", "144").replace("{height}", "192")
    return clean


class ImageCacheManager:
    """Downloads Twitch drop/game artwork in the background via FallbackDNS and caches locally."""

    def __init__(
        self,
        loop_thread: AsyncLoopThread,
        cache_dir: Path = IMAGE_CACHE_DIR,
    ) -> None:
        self.loop_thread = loop_thread
        self.cache_dir = cache_dir
        self._ui_root = UI_HTML_PATH.parent.resolve()
        self._lock = threading.Lock()
        self._cached_uris: dict[str, str] = {}
        self._in_flight: set[str] = set()
        self._failed_until: dict[str, float] = {}
        self._sem: asyncio.Semaphore | None = None
        self._migrate_legacy_cache()

    def _migrate_legacy_cache(self) -> None:
        """Move previously downloaded cache files into ui/cache/images so WebView2 HTTP origin can load them."""
        if (
            self.cache_dir.resolve() != IMAGE_CACHE_DIR.resolve()
            or not LEGACY_IMAGE_CACHE_DIR.is_dir()
        ):
            return
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        allowed_ext = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
        for src in LEGACY_IMAGE_CACHE_DIR.iterdir():
            if not src.is_file() or src.suffix.lower() not in allowed_ext or src.stat().st_size <= 0:
                continue
            dst = self.cache_dir / src.name
            if dst.exists():
                continue
            with suppress(OSError):
                shutil.move(str(src), str(dst))
                continue
            with suppress(OSError):
                shutil.copy2(str(src), str(dst))

    def _to_client_uri(self, path: Path) -> str:
        resolved = path.resolve()
        with suppress(ValueError):
            return resolved.relative_to(self._ui_root).as_posix()
        return resolved.as_uri()

    def _find_cached_file(self, key: str) -> Path | None:
        for ext in (".png", ".jpg", ".jpeg", ".webp", ".gif"):
            candidate = self.cache_dir / f"{key}{ext}"
            if candidate.is_file() and candidate.stat().st_size > 0:
                return candidate
        return None

    @staticmethod
    def _guess_extension(url: str, content_type: str = "") -> str:
        ctype = content_type.lower()
        if "webp" in ctype:
            return ".webp"
        if "jpeg" in ctype or "jpg" in ctype:
            return ".jpg"
        if "gif" in ctype:
            return ".gif"
        if "png" in ctype:
            return ".png"
        path = urlparse(url).path.lower()
        m = re.search(r"\.(png|jpe?g|webp|gif)$", path)
        if m:
            ext = "." + m.group(1)
            return ".jpg" if ext == ".jpeg" else ext
        return ".png"

    def resolve(self, raw_url: str) -> tuple[str, str]:
        """Return (display_uri_or_url, original_https_url) and enqueue uncached URLs."""
        norm = _normalize_image_url(raw_url)
        if not norm:
            return ("", "")
        if not norm.startswith(("http://", "https://")):
            return (norm, norm)

        key = hashlib.sha256(norm.encode("utf-8")).hexdigest()[:24]
        with self._lock:
            cached = self._cached_uris.get(norm)
            if cached:
                return (cached, norm)
            existing = self._find_cached_file(key)
            if existing is not None:
                uri = self._to_client_uri(existing)
                self._cached_uris[norm] = uri
                return (uri, norm)

        return (norm, norm)

    def prefetch(self, urls: list[str]) -> None:
        loop = getattr(self.loop_thread, "_loop", None)
        if loop is None or not loop.is_running():
            return

        now = time.monotonic()
        pending: list[str] = []
        with self._lock:
            for raw in urls:
                norm = _normalize_image_url(raw)
                if not norm.startswith(("http://", "https://")):
                    continue
                if norm in self._cached_uris or norm in self._in_flight:
                    continue
                if self._failed_until.get(norm, 0.0) > now:
                    continue
                key = hashlib.sha256(norm.encode("utf-8")).hexdigest()[:24]
                existing = self._find_cached_file(key)
                if existing is not None:
                    self._cached_uris[norm] = self._to_client_uri(existing)
                    continue
                self._in_flight.add(norm)
                pending.append(norm)

        if pending:
            self.loop_thread.run_coro(self._download_batch(pending))

    async def _download_batch(self, urls: list[str]) -> None:
        if self._sem is None:
            self._sem = asyncio.Semaphore(6)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        timeout = aiohttp.ClientTimeout(total=12)
        try:
            async with aiohttp.ClientSession(
                timeout=timeout,
                connector=make_connector(),
                cookie_jar=aiohttp.DummyCookieJar(),
                headers={"User-Agent": "TwitchMultiMiner asset cache"},
            ) as session:
                await asyncio.gather(
                    *(self._download_one(session, url) for url in urls),
                    return_exceptions=True,
                )
        except Exception as exc:
            logger.debug("Image prefetch batch error: %s", exc)
            with self._lock:
                for u in urls:
                    self._in_flight.discard(u)

    async def _download_one(self, session: aiohttp.ClientSession, url: str) -> None:
        assert self._sem is not None
        key = hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]
        try:
            async with self._sem:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status}")
                    data = await resp.read()
                    if not data or len(data) > 5 * 1024 * 1024:
                        raise RuntimeError("Empty or oversized image")
                    ext = self._guess_extension(url, resp.headers.get("Content-Type", ""))
                    target = self.cache_dir / f"{key}{ext}"
                    tmp = self.cache_dir / f"{key}{ext}.tmp"
                    tmp.write_bytes(data)
                    tmp.replace(target)
                    uri = self._to_client_uri(target)
                    with self._lock:
                        self._cached_uris[url] = uri
        except Exception as exc:
            logger.debug("Failed caching image %s: %s", url, exc)
            with self._lock:
                self._failed_until[url] = time.monotonic() + 300.0
        finally:
            with self._lock:
                self._in_flight.discard(url)


class AsyncLoopThread:
    """Dedicated background thread hosting the asyncio event loop for all account workers."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._ready.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="AsyncOrchestratorLoop", daemon=True
        )
        self._thread.start()
        self._ready.wait(timeout=5.0)

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._ready.set()
        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

    def run_coro(self, coro: Any) -> asyncio.Future[Any]:
        if self._loop is None:
            self.start()
        assert self._loop is not None
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def stop(self, timeout: float = 8.0) -> None:
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=timeout)


def _format_age(ts: float) -> str:
    if not ts:
        return "не выполнялся"
    diff = max(0, int(time.time() - ts))
    if diff < 5:
        return "только что"
    if diff < 60:
        return f"{diff} сек назад"
    mins = diff // 60
    return f"{mins} мин назад"


class DesktopBridge:
    """Thread-safe JS API exposed to ui/dashboard.html inside WebView2."""

    def __init__(
        self,
        orchestrator: MultiAccountOrchestrator,
        loop_thread: AsyncLoopThread,
        on_hide_to_tray: Any = None,
    ) -> None:
        self.orchestrator = orchestrator
        self.loop_thread = loop_thread
        self._on_hide_to_tray = on_hide_to_tray
        self.image_cache = ImageCacheManager(loop_thread)

    def get_state(self) -> dict[str, Any]:
        prefetch_urls: list[str] = []

        def _img(raw_url: str) -> tuple[str, str]:
            disp, orig = self.image_cache.resolve(raw_url)
            if orig:
                prefetch_urls.append(orig)
            return disp, orig

        accounts = [dataclasses.asdict(snap) for snap in self.orchestrator.snapshots()]
        for acc in accounts:
            d_disp, d_orig = _img(acc.get("drop_image_url", ""))
            g_disp, _ = _img(acc.get("game_image_url", ""))
            acc["drop_image_url"] = d_disp
            acc["drop_image_source"] = d_orig
            acc["game_image_url"] = g_disp

        fav_set = set(self.orchestrator.global_settings.favorite_campaigns)
        exclusive_id = str(self.orchestrator.global_settings.exclusive_campaign_id or "").strip()
        live_open_games: set[str] = set()
        for w in list(self.orchestrator.workers.values()):
            for ch in list(w.channels.values()):
                if ch.online and ch.drops_enabled and ch.game is not None:
                    live_open_games.add(ch.game.name.lower())
        live_radar_camp_ids: dict[str, set[int]] = {}
        for st in list(self.orchestrator.radar.pinned_streams.values()):
            if st.online and st.channel_id:
                for rcid in st.active_campaign_ids:
                    live_radar_camp_ids.setdefault(rcid, set()).add(st.channel_id)
        campaigns_out: list[dict[str, Any]] = []
        for acc_name, worker in list(self.orchestrator.workers.items()):
            for camp in list(worker.inventory):
                try:
                    camp_img, camp_img_src = _img(str(camp.image_url or ""))
                    drops_list = []
                    for d in camp.drops:
                        benefits_list = []
                        for b in d.benefits:
                            b_img, b_src = _img(str(b.image_url or ""))
                            benefits_list.append(
                                {
                                    "id": b.id,
                                    "name": b.name,
                                    "type": _BENEFIT_LABELS.get(b.type.name, "Награда"),
                                    "image_url": b_img,
                                    "source_url": b_src,
                                }
                            )
                        drop_img, drop_src = _img(d.primary_image_url)
                        drops_list.append(
                            {
                                "id": d.id,
                                "name": d.name,
                                "rewards_text": d.rewards_text(),
                                "current_minutes": int(d.current_minutes),
                                "required_minutes": int(d.required_minutes),
                                "is_claimed": bool(d.is_claimed),
                                "progress": float(d.progress),
                                "image_url": drop_img,
                                "source_url": drop_src or camp_img_src,
                                "benefits": benefits_list,
                            }
                        )
                    live_count = 0
                    if camp.active and not camp.finished:
                        acl_ids = {ch.id for ch in camp.allowed_channels} if camp.allowed_channels else set()
                        seen_live_ids: set[int] = set()
                        for w in list(self.orchestrator.workers.values()):
                            for ch in list(w.channels.values()):
                                if not ch.online or ch.id in seen_live_ids:
                                    continue
                                if acl_ids:
                                    if ch.id in acl_ids:
                                        seen_live_ids.add(ch.id)
                                elif getattr(ch, "drops_verified", False):
                                    if camp.id in getattr(ch, "available_campaign_ids", ()):
                                        seen_live_ids.add(ch.id)
                                elif ch.drops_enabled and ch.game is not None and ch.game == camp.game:
                                    seen_live_ids.add(ch.id)
                        live_count = len(seen_live_ids)
                        if live_count == 0:
                            radar_ch_ids = live_radar_camp_ids.get(camp.id, set())
                            if acl_ids:
                                live_count = len(radar_ch_ids & acl_ids)
                            else:
                                live_count = len(radar_ch_ids)
                                if live_count == 0 and camp.game.name.lower() in live_open_games:
                                    live_count = 1
                    campaigns_out.append(
                        {
                            "account": acc_name,
                            "id": camp.id,
                            "name": camp.name,
                            "game": camp.game.name,
                            "image_url": camp_img,
                            "source_url": camp_img_src,
                            "is_favorite": bool(camp.id in fav_set),
                            "is_exclusive": bool(exclusive_id and camp.id == exclusive_id),
                            "has_live_stream": bool(live_count > 0),
                            "live_stream_count": int(live_count),
                            "linked": bool(camp.linked),
                            "eligible": bool(camp.eligible),
                            "active": bool(camp.active),
                            "upcoming": bool(camp.upcoming),
                            "finished": bool(camp.finished),
                            "claimed_drops": int(camp.claimed_drops),
                            "total_drops": int(camp.total_drops),
                            "progress": float(camp.progress),
                            "remaining_minutes": int(camp.remaining_minutes),
                            "ends_at": camp.ends_at.strftime("%d.%m %H:%M"),
                            "drops": drops_list,
                        }
                    )
                except Exception:
                    continue

        radar = self.orchestrator.radar
        pinned_streams = [
            dataclasses.asdict(st) for st in list(radar.pinned_streams.values())
        ]
        pinned_streams.sort(key=lambda s: (not s.get("online", False), -(s.get("viewers") or 0), s.get("login", "")))

        gs = self.orchestrator.global_settings
        inv_games = self._collect_inventory_games()
        available_campaign_games = radar.collect_available_campaign_games(inv_games)
        pc_games = radar.get_installed_pc_games(
            known_games=[*available_campaign_games, *DEFAULT_PRIORITY_GAMES]
        )
        claimed_history = []
        for rec in list(self.orchestrator.claimed_history):
            h_img, h_src = _img(getattr(rec, "image_url", ""))
            claimed_history.append(
                {
                    "claimed_at": (
                        rec.timestamp_str.split()[-1]
                        if getattr(rec, "timestamp_str", "")
                        else "—"
                    ),
                    "account": rec.account_name,
                    "game": rec.game_name,
                    "campaign": rec.campaign_name,
                    "drop_name": rec.drop_name,
                    "image_url": h_img,
                    "source_url": h_src,
                }
            )

        if prefetch_urls:
            self.image_cache.prefetch(prefetch_urls)

        return {
            "app_name": APP_NAME,
            "version": APP_VERSION,
            "accounts": accounts,
            "radar": {
                "last_poll_ts": float(radar.last_radar_poll_ts),
                "last_poll_label": _format_age(radar.last_radar_poll_ts),
                "last_error": str(radar.last_radar_error or ""),
                "auto_promoted_games": list(radar.auto_promoted_games),
                "pinned_streams": pinned_streams,
                "preset": gs.radar_preset,
                "available_campaign_games": available_campaign_games,
                "pc_games": pc_games,
            },
            "settings": dataclasses.asdict(gs),
            "campaigns": campaigns_out,
            "claimed_history": claimed_history,
            "global_logs": list(self.orchestrator.global_logs),
        }

    def add_profile(
        self, name: str, proxy: str = "", auto_start: bool = True
    ) -> dict[str, Any]:
        clean_name = (name or "").strip()
        clean_proxy = (proxy or "").strip()
        if not clean_name:
            return {"ok": False, "error": "Имя профиля не может быть пустым"}
        try:
            self.orchestrator.add_profile(clean_name, proxy=clean_proxy, start_immediately=False)
            if auto_start:
                self.loop_thread.run_coro(self._async_start_profile(clean_name))
            return {"ok": True, "name": clean_name}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    async def _async_start_profile(self, name: str) -> None:
        self.orchestrator.start_profile(name)

    def remove_profile(self, name: str) -> dict[str, Any]:
        if name not in self.orchestrator.workers:
            return {"ok": False, "error": f"Профиль '{name}' не найден"}
        try:
            fut = self.loop_thread.run_coro(self.orchestrator.remove_profile(name))
            fut.result(timeout=8.0)
            return {"ok": True}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def update_proxy(self, name: str, proxy: str) -> dict[str, Any]:
        if name not in self.orchestrator.workers:
            return {"ok": False, "error": f"Профиль '{name}' не найден"}
        try:
            self.orchestrator.update_profile_proxy(name, (proxy or "").strip())
            return {"ok": True}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def start_profile(self, name: str) -> dict[str, Any]:
        if name not in self.orchestrator.workers:
            return {"ok": False, "error": f"Профиль '{name}' не найден"}
        self.loop_thread.run_coro(self._async_start_profile(name))
        return {"ok": True}

    def stop_profile(self, name: str) -> dict[str, Any]:
        if name not in self.orchestrator.workers:
            return {"ok": False, "error": f"Профиль '{name}' не найден"}
        self.loop_thread.run_coro(self.orchestrator.stop_profile(name))
        return {"ok": True}

    def reauth_profile(self, name: str) -> dict[str, Any]:
        worker = self.orchestrator.workers.get(name)
        if worker is None:
            return {"ok": False, "error": f"Профиль '{name}' не найден"}

        async def _reauth() -> None:
            await self.orchestrator.stop_profile(name)
            worker.invalidate_session()
            self.orchestrator.start_profile(name)

        self.loop_thread.run_coro(_reauth())
        return {"ok": True}

    def open_activation(self, name: str) -> dict[str, Any]:
        worker = self.orchestrator.workers.get(name)
        uri = "https://www.twitch.tv/activate"
        code = ""
        if worker is not None:
            snap = worker.snapshot()
            uri = snap.verification_uri or uri
            code = snap.activation_code or ""
        if code:
            self.copy_text(code)
        try:
            webbrowser.open(uri)
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
        return {"ok": True, "uri": uri, "code": code}

    def start_all(self) -> dict[str, Any]:
        async def _start() -> None:
            self.orchestrator.start_all()

        self.loop_thread.run_coro(_start())
        return {"ok": True}

    def stop_all(self) -> dict[str, Any]:
        self.loop_thread.run_coro(self.orchestrator.stop_all())
        return {"ok": True}

    def import_legacy(self) -> dict[str, Any]:
        try:
            imported = self.orchestrator.import_from_legacy_miner()
            return {"ok": True, "imported": imported}
        except Exception as exc:
            return {"ok": False, "error": str(exc), "imported": []}

    def poll_radar_now(self) -> dict[str, Any]:
        try:
            fut = self.loop_thread.run_coro(self.orchestrator.force_radar_refresh())
            res = fut.result(timeout=12.0)
            if isinstance(res, dict):
                return res
            return {"ok": bool(res)}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def save_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            gs = self.orchestrator.global_settings
            gs.pinned_streamers = [
                str(x).strip().lower()
                for x in payload.get("pinned_streamers", [])
                if str(x).strip()
            ]
            gs.auto_priority_keywords = [
                str(x).strip()
                for x in payload.get("auto_priority_keywords", [])
                if str(x).strip()
            ]
            gs.priority_games = [
                str(x).strip()
                for x in payload.get("priority_games", [])
                if str(x).strip()
            ]
            gs.exclude_games = [
                str(x).strip()
                for x in payload.get("exclude_games", [])
                if str(x).strip()
            ]

            mode_raw = str(payload.get("priority_mode", "ENDING_SOONEST")).upper()
            gs.priority_mode = (
                mode_raw if mode_raw in PriorityMode.__members__ else "ENDING_SOONEST"
            )
            if "radar_preset" in payload:
                gs.radar_preset = str(payload.get("radar_preset") or "default").strip()

            gs.connection_quality = max(
                1, min(6, int(payload.get("connection_quality", gs.connection_quality)))
            )
            gs.radar_interval_sec = max(
                30, min(600, int(payload.get("radar_interval_sec", gs.radar_interval_sec)))
            )
            gs.hybrid_watch_spade = bool(payload.get("hybrid_watch_spade", True))
            gs.standby_on_pinned = bool(payload.get("standby_on_pinned", False))
            gs.enable_badges_emotes = bool(payload.get("enable_badges_emotes", True))
            gs.available_drops_check = bool(payload.get("available_drops_check", True))

            self.orchestrator.save_global_settings()
            return {"ok": True}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def copy_text(self, text: str) -> dict[str, Any]:
        if win32clipboard is None or win32con is None:
            return {"ok": False, "error": "Буфер обмена недоступен"}
        try:
            win32clipboard.OpenClipboard()
            try:
                win32clipboard.EmptyClipboard()
                win32clipboard.SetClipboardData(win32con.CF_UNICODETEXT, str(text))
            finally:
                win32clipboard.CloseClipboard()
            return {"ok": True}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def open_external_url(self, url: str) -> dict[str, Any]:
        clean = str(url or "").strip()
        if not clean.startswith(("http://", "https://")):
            return {"ok": False, "error": "Некорректный URL"}
        try:
            webbrowser.open(clean)
            return {"ok": True}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def _collect_inventory_games(self) -> list[str]:
        games: list[str] = []
        for worker in list(self.orchestrator.workers.values()):
            for camp in list(worker.inventory):
                if not camp.finished and getattr(camp, "game", None) and camp.game.name:
                    games.append(camp.game.name)
        return games

    def apply_radar_preset(self, preset: str) -> dict[str, Any]:
        preset_key = str(preset or "").strip().lower()
        if preset_key not in ("default", "all_campaigns", "pc_games"):
            return {"ok": False, "error": f"Неизвестный пресет: {preset}"}
        try:
            gs = self.orchestrator.global_settings
            radar = self.orchestrator.radar
            inv_games = self._collect_inventory_games()

            if preset_key == "all_campaigns":
                if not radar._catalog_campaigns:
                    with suppress(Exception):
                        self.loop_thread.run_coro(radar.get_merged_public_campaigns()).result(timeout=8.0)
                games = radar.collect_available_campaign_games(inv_games)
                gs.radar_preset = "all_campaigns"
                gs.priority_games = games
                gs.exclude_games = []
                gs.priority_mode = "ENDING_SOONEST"
            elif preset_key == "pc_games":
                known = radar.collect_available_campaign_games(inv_games) + list(DEFAULT_PRIORITY_GAMES)
                games = radar.get_installed_pc_games(force=True, known_games=known)
                gs.radar_preset = "pc_games"
                gs.priority_games = games
                gs.exclude_games = []
                gs.priority_mode = "PRIORITY_ONLY"
            else:
                gs.radar_preset = "default"
                gs.pinned_streamers = list(DEFAULT_PINNED_STREAMERS)
                gs.auto_priority_keywords = list(DEFAULT_AUTO_PRIORITY_KEYWORDS)
                gs.priority_games = list(DEFAULT_PRIORITY_GAMES)
                gs.exclude_games = []
                gs.priority_mode = "ENDING_SOONEST"

            self.orchestrator.save_global_settings()
            return {
                "ok": True,
                "preset": gs.radar_preset,
                "games_count": len(gs.priority_games),
                "settings": dataclasses.asdict(gs),
            }
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def toggle_favorite_campaign(self, campaign_id: str) -> dict[str, Any]:
        cid = str(campaign_id or "").strip()
        if not cid:
            return {"ok": False, "error": "Пустой ID кампании"}
        try:
            gs = self.orchestrator.global_settings
            if cid in gs.favorite_campaigns:
                gs.favorite_campaigns = [x for x in gs.favorite_campaigns if x != cid]
                is_fav = False
            else:
                gs.favorite_campaigns = [*gs.favorite_campaigns, cid]
                is_fav = True
            self.orchestrator.save_global_settings()
            return {
                "ok": True,
                "id": cid,
                "is_favorite": is_fav,
                "favorite_campaigns": list(gs.favorite_campaigns),
            }
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def set_exclusive_campaign(self, campaign_id: str = "") -> dict[str, Any]:
        cid = str(campaign_id or "").strip()
        try:
            gs = self.orchestrator.global_settings
            if not cid or gs.exclusive_campaign_id == cid:
                gs.exclusive_campaign_id = ""
                active = False
            else:
                gs.exclusive_campaign_id = cid
                active = True
            self.orchestrator.save_global_settings()
            return {
                "ok": True,
                "exclusive_campaign_id": gs.exclusive_campaign_id,
                "is_exclusive": active,
            }
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def hide_to_tray(self) -> dict[str, Any]:
        if callable(self._on_hide_to_tray):
            self._on_hide_to_tray()
            return {"ok": True}
        return {"ok": False}


class MultiMinerDashboard:
    """pywebview (Edge WebView2) desktop application + Win32 tray host."""

    def __init__(
        self,
        orchestrator: MultiAccountOrchestrator,
        loop_thread: AsyncLoopThread,
        *,
        start_in_tray: bool = False,
    ) -> None:
        self.orchestrator = orchestrator
        self.loop_thread = loop_thread
        self.start_in_tray = start_in_tray
        self.window: Any = None

        self._exiting = False
        self._stop_event = threading.Event()
        self._tray_active = False

        self.bridge = DesktopBridge(
            orchestrator,
            loop_thread,
            on_hide_to_tray=self._hide_window,
        )

        icon_path = ICONS_DIR / "pickaxe.ico"
        self._icon_path = icon_path if icon_path.exists() else None
        self.tray = Win32TrayController(
            app_name=APP_NAME,
            icon_path=icon_path,
            on_show=self._show_window,
            on_start_all=self._tray_start_all,
            on_stop_all=self._tray_stop_all,
            on_exit=self._on_exit,
        )

    def _tray_start_all(self) -> None:
        self.bridge.start_all()

    def _tray_stop_all(self) -> None:
        self.bridge.stop_all()

    def _hide_window(self) -> None:
        if self.window is not None and self._tray_active:
            try:
                self.window.hide()
            except Exception as exc:
                logger.debug("Не удалось скрыть окно в трей: %s", exc)

    def _show_window(self) -> None:
        if self.window is not None:
            try:
                self.window.show()
                self.window.restore()
            except Exception as exc:
                logger.debug("Не удалось восстановить окно из трея: %s", exc)

    def _on_window_closing(self) -> bool:
        # Returning True cancels WinForms FormClosing and hides to tray instead.
        if self._exiting or not self._tray_active:
            return False
        self._hide_window()
        return True

    def _on_webview_started(self) -> None:
        if self.start_in_tray and self._tray_active:
            self._hide_window()

        def _tooltip_worker() -> None:
            while not self._stop_event.wait(timeout=3.0):
                try:
                    snaps = self.orchestrator.snapshots()
                    running = sum(1 for s in snaps if s.running)
                    mining = sum(
                        1
                        for s in snaps
                        if s.running and s.watching_channel and s.watching_channel != "—" and not s.is_standby
                    )
                    self.tray.update_tooltip(
                        f"{APP_NAME} · активны {running}/{len(snaps)} · добыча {mining}"
                    )
                except Exception:
                    pass

        threading.Thread(
            target=_tooltip_worker, name="TrayTooltipUpdater", daemon=True
        ).start()

    def _on_exit(self) -> None:
        self._exiting = True
        self._stop_event.set()
        try:
            fut = self.loop_thread.run_coro(self.orchestrator.stop_all())
            fut.result(timeout=6.0)
        except Exception:
            pass
        self.tray.stop()
        self.loop_thread.stop(timeout=4.0)
        if self.window is not None:
            try:
                self.window.destroy()
            except Exception:
                pass

    def run(self) -> None:
        if webview is None:
            raise RuntimeError(
                "Модуль 'pywebview' не установлен. Запустите через .venv или установите pywebview>=5.0."
            )
        if not UI_HTML_PATH.exists():
            raise FileNotFoundError(f"Файл интерфейса не найден: {UI_HTML_PATH}")

        self._tray_active = self.tray.start()
        self.window = webview.create_window(
            title=f"{APP_NAME} v{APP_VERSION}",
            url=str(UI_HTML_PATH),
            js_api=self.bridge,
            width=1360,
            height=840,
            min_size=(960, 620),
            hidden=bool(self.start_in_tray and self._tray_active),
            background_color="#141415",
            text_select=True,
        )
        if self.window is not None:
            self.window.events.closing += self._on_window_closing

        icon_str = str(self._icon_path) if self._icon_path is not None else None
        try:
            try:
                webview.start(
                    self._on_webview_started,
                    gui="edgechromium",
                    icon=icon_str,
                )
            except Exception as exc:
                logger.warning("Запуск edgechromium не удался (%s), пробую автовыбор движка", exc)
                webview.start(self._on_webview_started, icon=icon_str)
        finally:
            if not self._exiting:
                self._on_exit()
