"""Shared Campaign & Top-Streamer Radar (direct Twitch GQL discovery + cached public catalog)."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Iterable

try:
    import winreg
except ImportError:  # pragma: no cover - non-Windows fallback
    winreg = None  # type: ignore[assignment]

import aiohttp

from auth_transport import EgressLimiterRegistry
from constants import DEFAULT_PRIORITY_GAMES, GQL_QUERIES, WORKING_DIR, JsonType, URLType
from exceptions import MinerException, RequestException
from utils import Game, chunk, deduplicate, is_socks_proxy, make_connector, timestamp

if TYPE_CHECKING:
    from settings import GlobalSettings
    from worker import AccountWorker


logger = logging.getLogger("TwitchMultiMiner.radar")
RADAR_CACHE_PATH = WORKING_DIR / "cache" / "radar_catalog.json"
_UNSET = object()

_STEAM_NON_GAME_APPIDS = {"228980", "431960", "629520", "1905180", "365670"}
_NON_GAME_PATTERNS = (
    "steamworks",
    "redistributable",
    "proton ",
    "steam linux runtime",
    "steamvr",
    "wallpaper engine",
    "soundpad",
    "obs studio",
    "blender",
    "3dmark",
    "lossless scaling",
    "borderless gaming",
    "dedicated server",
    "soundtrack",
    "benchmark",
    "sdk",
)
_RIOT_METADATA_GAMES: dict[str, str] = {
    "valorant": "VALORANT",
    "league_of_legends": "League of Legends",
    "teamfighttactics": "Teamfight Tactics",
    "bacon": "Legends of Runeterra",
    "lion": "2XKO",
}
_UNINSTALL_GAME_PUBLISHERS = (
    "blizzard entertainment",
    "rockstar games",
    "electronic arts",
    "battlestate games",
    "gaijin",
    "wargaming.net",
    "lesta games",
    "grinding gear games",
    "hoyoverse",
    "cognosphere",
    "embark studios",
)
_LAUNCHER_EXCLUDE_LOWER = {
    "battle.net",
    "riot client",
    "rockstar games launcher",
    "rockstar games social club",
    "ea app",
    "ubisoft connect",
    "epic games launcher",
    "epic online services",
    "steam",
}


def _normalize_game_key(name: str) -> str:
    cleaned = re.sub(r"[®™©]+", "", str(name or "").casefold())
    cleaned = re.sub(r"[^a-z0-9а-яё]+", " ", cleaned)
    return " ".join(cleaned.split())


def _is_non_game_app(name: str, appid: str = "") -> bool:
    if appid and appid in _STEAM_NON_GAME_APPIDS:
        return True
    low = name.strip().casefold()
    if not low or low in _LAUNCHER_EXCLUDE_LOWER:
        return True
    return any(pat in low for pat in _NON_GAME_PATTERNS)


def _scan_steam_games() -> list[str]:
    candidates: list[Path] = []
    if winreg is not None:
        with suppress(OSError):
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Valve\Steam") as k:
                sp, _ = winreg.QueryValueEx(k, "SteamPath")
                if sp:
                    candidates.append(Path(str(sp)))
    home = Path.home()
    candidates.extend(
        [
            Path(r"C:\Program Files (x86)\Steam"),
            Path(r"C:\Program Files\Steam"),
            Path(r"D:\Steam"),
            Path(r"D:\SteamLibrary"),
            Path(r"C:\SteamLibrary"),
            home / ".steam" / "steam",
            home / ".local" / "share" / "Steam",
            home / "Library" / "Application Support" / "Steam",
        ]
    )

    library_dirs: list[Path] = []
    seen_libs: set[str] = set()
    for base in candidates:
        if not base.exists():
            continue
        vdf = base / "steamapps" / "libraryfolders.vdf"
        if vdf.is_file():
            txt = vdf.read_text(encoding="utf-8", errors="ignore")
            for raw_path in re.findall(r'"path"\s+"([^"]+)"', txt):
                lib = Path(raw_path.replace("\\\\", "\\"))
                key = str(lib).casefold()
                if key not in seen_libs:
                    seen_libs.add(key)
                    library_dirs.append(lib)
        base_key = str(base).casefold()
        if base_key not in seen_libs and (base / "steamapps").is_dir():
            seen_libs.add(base_key)
            library_dirs.append(base)

    found: list[str] = []
    for lib in library_dirs:
        steamapps = lib / "steamapps"
        if not steamapps.is_dir():
            continue
        for acf in steamapps.glob("appmanifest_*.acf"):
            with suppress(OSError):
                text = acf.read_text(encoding="utf-8", errors="ignore")
                app_m = re.search(r'"appid"\s+"([^"]+)"', text)
                name_m = re.search(r'"name"\s+"([^"]+)"', text)
                if not name_m:
                    continue
                appid = app_m.group(1).strip() if app_m else ""
                name = name_m.group(1).strip()
                if name and not _is_non_game_app(name, appid=appid):
                    found.append(name)
    return found


def _scan_epic_games() -> list[str]:
    program_data = Path(os.environ.get("ProgramData", r"C:\ProgramData"))
    home = Path.home()
    manifest_dirs = [
        program_data / "Epic" / "EpicGamesLauncher" / "Data" / "Manifests",
        home / "Library" / "Application Support" / "Epic" / "EpicGamesLauncher" / "Data" / "Manifests",
    ]
    found: list[str] = []
    for manifests_dir in manifest_dirs:
        if not manifests_dir.is_dir():
            continue
        for item in manifests_dir.glob("*.item"):
            with suppress(OSError, ValueError, TypeError):
                data = json.loads(item.read_text(encoding="utf-8", errors="ignore"))
                if not isinstance(data, dict):
                    continue
                if not bool(data.get("bIsApplication", True)):
                    continue
                if not str(data.get("LaunchExecutable") or "").strip():
                    continue
                main_app = str(data.get("MainGameAppName") or "").strip()
                app_name = str(data.get("AppName") or "").strip()
                if main_app and main_app != app_name:
                    continue
                disp = str(data.get("DisplayName") or "").strip()
                if disp and not _is_non_game_app(disp):
                    found.append(disp)

    # Linux Heroic / Legendary manifests
    for alt_file in (
        home / ".config" / "heroic" / "installed.json",
        home / ".config" / "legendary" / "installed.json",
    ):
        if not alt_file.is_file():
            continue
        with suppress(OSError, ValueError, TypeError):
            payload = json.loads(alt_file.read_text(encoding="utf-8", errors="ignore"))
            entries = payload.values() if isinstance(payload, dict) else (payload if isinstance(payload, list) else [])
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                title = str(entry.get("title") or entry.get("app_title") or "").strip()
                if title and not _is_non_game_app(title):
                    found.append(title)
    return found


def _scan_riot_games() -> list[str]:
    program_data = Path(os.environ.get("ProgramData", r"C:\ProgramData"))
    riot_meta = program_data / "Riot Games" / "Metadata"
    if not riot_meta.is_dir():
        return []
    found: list[str] = []
    with suppress(OSError):
        for entry in riot_meta.iterdir():
            low = entry.name.casefold()
            prefix = low.split(".", 1)[0]
            canonical = _RIOT_METADATA_GAMES.get(prefix)
            if canonical:
                found.append(canonical)
    return found


def _scan_registry_games() -> list[str]:
    if winreg is None:
        return []
    found: list[str] = []

    def _read_key_vals(sk: Any) -> dict[str, str]:
        vals: dict[str, str] = {}
        idx = 0
        while True:
            try:
                vn, vv, _ = winreg.EnumValue(sk, idx)
                vals[str(vn)] = str(vv)
                idx += 1
            except OSError:
                break
        return vals

    for root, subkey, extractor in (
        (
            winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\WOW6432Node\GOG.com\Games",
            lambda vals: vals.get("gameName") or vals.get("GAMENAME") or "",
        ),
        (
            winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\WOW6432Node\Ubisoft\Launcher\Installs",
            lambda vals: Path(vals["InstallDir"]).name if vals.get("InstallDir") else "",
        ),
    ):
        with suppress(OSError):
            with winreg.OpenKey(root, subkey) as key:
                i = 0
                while True:
                    try:
                        child = winreg.EnumKey(key, i)
                        i += 1
                    except OSError:
                        break
                    with suppress(OSError), winreg.OpenKey(key, child) as sk:
                        name = extractor(_read_key_vals(sk)).strip()
                        if name and not _is_non_game_app(name):
                            found.append(name)

    uninstall_paths = (
        (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall"),
        (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall"),
        (winreg.HKEY_CURRENT_USER, r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall"),
    )
    for root, subkey in uninstall_paths:
        with suppress(OSError):
            with winreg.OpenKey(root, subkey) as key:
                i = 0
                while True:
                    try:
                        child = winreg.EnumKey(key, i)
                        i += 1
                    except OSError:
                        break
                    with suppress(OSError), winreg.OpenKey(key, child) as sk:
                        vals = _read_key_vals(sk)
                        disp = str(vals.get("DisplayName") or "").strip()
                        pub = str(vals.get("Publisher") or "").strip().casefold()
                        if not disp or not pub:
                            continue
                        if any(p in pub for p in _UNINSTALL_GAME_PUBLISHERS) and not _is_non_game_app(disp):
                            found.append(disp)
    return found


def scan_installed_pc_games() -> list[str]:
    """Scan installed PC games locally across launchers/manifests (strictly offline, never transmitted)."""
    raw_names: list[str] = [
        *_scan_steam_games(),
        *_scan_epic_games(),
        *_scan_riot_games(),
        *_scan_registry_games(),
    ]
    deduped: list[str] = []
    seen_keys: set[str] = set()
    for name in raw_names:
        clean = " ".join(re.sub(r"[®™©]+", "", name).split())
        key = _normalize_game_key(clean)
        if clean and key and key not in seen_keys:
            seen_keys.add(key)
            deduped.append(clean)
    return deduped


@dataclass
class PinnedStreamerStatus:
    login: str
    channel_id: int | None = None
    display_name: str = ""
    online: bool = False
    game_name: str = ""
    game_id: int | None = None
    viewers: int = 0
    title: str = ""
    active_campaign_ids: list[str] = field(default_factory=list)
    active_campaign_names: list[str] = field(default_factory=list)
    last_checked_ts: float = 0.0
    raw_channel_data: JsonType | None = None


class SharedCampaignRadar:
    """Centralizes catalog fetches, streamer AvailableDrops checks, and Spade URLs across workers."""

    def __init__(
        self,
        global_settings: GlobalSettings,
        *,
        cache_path: Path | None | object = _UNSET,
    ) -> None:
        self.global_settings: GlobalSettings = global_settings
        self._catalog_lock = asyncio.Lock()
        self._radar_lock = asyncio.Lock()
        self._details_lock = asyncio.Lock()
        self._cache_path: Path | None = (
            RADAR_CACHE_PATH if cache_path is _UNSET else cache_path  # type: ignore[assignment]
        )

        self._catalog_campaigns: dict[str, JsonType] = {}
        self._catalog_fetched_at: float = 0.0
        self._catalog_backoff_until: float = 0.0

        self._direct_campaigns: dict[str, JsonType] = {}
        self.verified_channels: dict[str, set[int]] = {}
        self.verified_campaign_games: dict[str, dict[str, JsonType]] = {}
        self.pinned_streams: dict[str, PinnedStreamerStatus] = {
            login: PinnedStreamerStatus(login=login)
            for login in global_settings.pinned_streamers
        }
        self.spade_urls: dict[str, URLType] = {}
        self.auto_promoted_games: list[str] = []
        self.last_radar_poll_ts: float = 0.0
        self.last_radar_error: str = ""
        self._pc_games_cache: list[str] = []
        self._pc_games_scanned_at: float = 0.0
        self.egress_limiters = EgressLimiterRegistry(capacity=5, window=1)
        self._load_disk_cache()

    @staticmethod
    def _is_campaign_unexpired(campaign: JsonType, now_utc: datetime) -> bool:
        if campaign.get("status") == "EXPIRED":
            return False
        end_at_raw = campaign.get("endAt")
        if isinstance(end_at_raw, str) and end_at_raw.strip():
            with suppress(ValueError, TypeError):
                return timestamp(end_at_raw) > now_utc
        return True

    def _load_disk_cache(self) -> None:
        if self._cache_path is None or not self._cache_path.is_file():
            return
        try:
            payload = json.loads(self._cache_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return
        if not isinstance(payload, dict):
            return
        now_utc = datetime.now(timezone.utc)
        now_ts = time()
        fetched_at = float(payload.get("catalog_fetched_at") or 0.0)
        backoff_until = float(payload.get("catalog_backoff_until") or 0.0)
        if fetched_at <= now_ts + 60.0:
            self._catalog_fetched_at = max(0.0, fetched_at)
        if backoff_until > now_ts and backoff_until <= now_ts + 3600.0:
            self._catalog_backoff_until = backoff_until

        for attr_name, key in (
            ("_catalog_campaigns", "catalog_campaigns"),
            ("_direct_campaigns", "direct_campaigns"),
        ):
            raw_map = payload.get(key)
            if not isinstance(raw_map, dict):
                continue
            target: dict[str, JsonType] = getattr(self, attr_name)
            for cid, raw_camp in raw_map.items():
                if not isinstance(cid, str) or not isinstance(raw_camp, dict):
                    continue
                if not self._is_campaign_unexpired(raw_camp, now_utc):
                    continue
                norm = self._normalize_campaign_dict(raw_camp)
                if norm is not None:
                    target[cid] = norm

        active_cids = set(self._catalog_campaigns.keys()) | set(self._direct_campaigns.keys())
        raw_vc = payload.get("verified_channels")
        if isinstance(raw_vc, dict):
            for cid, ch_ids in raw_vc.items():
                if cid in active_cids and isinstance(ch_ids, list):
                    self.verified_channels[cid] = {int(x) for x in ch_ids if x is not None}

        raw_vg = payload.get("verified_campaign_games")
        if isinstance(raw_vg, dict):
            for cid, g_map in raw_vg.items():
                if cid in active_cids and isinstance(g_map, dict):
                    clean_games: dict[str, JsonType] = {}
                    for gid, gobj in g_map.items():
                        if isinstance(gobj, dict) and gobj.get("id") and (gobj.get("displayName") or gobj.get("name")):
                            clean_games[str(gid)] = dict(gobj)
                    if clean_games:
                        self.verified_campaign_games[cid] = clean_games

    def _save_disk_cache(self) -> None:
        if self._cache_path is None:
            return
        now_utc = datetime.now(timezone.utc)
        catalog_clean = {
            cid: c
            for cid, c in self._catalog_campaigns.items()
            if self._is_campaign_unexpired(c, now_utc)
        }
        direct_clean = {
            cid: c
            for cid, c in self._direct_campaigns.items()
            if self._is_campaign_unexpired(c, now_utc)
        }
        active_cids = set(catalog_clean.keys()) | set(direct_clean.keys())
        verified_channels_clean = {
            cid: sorted(ch_ids)
            for cid, ch_ids in self.verified_channels.items()
            if cid in active_cids and ch_ids
        }
        verified_games_clean = {
            cid: g_map
            for cid, g_map in self.verified_campaign_games.items()
            if cid in active_cids and g_map
        }
        payload = {
            "catalog_fetched_at": self._catalog_fetched_at,
            "catalog_backoff_until": self._catalog_backoff_until,
            "catalog_campaigns": catalog_clean,
            "direct_campaigns": direct_clean,
            "verified_channels": verified_channels_clean,
            "verified_campaign_games": verified_games_clean,
        }
        with suppress(OSError, ValueError, TypeError):
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self._cache_path.with_name(f"{self._cache_path.name}.tmp")
            tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            tmp_path.replace(self._cache_path)

    def sync_pinned_list(self) -> None:
        desired = {s.strip().lower() for s in self.global_settings.pinned_streamers if s.strip()}
        for login in list(self.pinned_streams.keys()):
            if login not in desired:
                del self.pinned_streams[login]
        for login in self.global_settings.pinned_streamers:
            low = login.strip().lower()
            if low and low not in self.pinned_streams:
                self.pinned_streams[low] = PinnedStreamerStatus(login=low)

    def get_cached_spade_url(self, login: str) -> URLType | None:
        return self.spade_urls.get(login.lower())

    def set_cached_spade_url(self, login: str, url: URLType) -> None:
        self.spade_urls[login.lower()] = url

    def promote_game_if_matches(self, campaign_data: JsonType) -> str | None:
        game = campaign_data.get("game") or {}
        game_name = str(game.get("displayName") or game.get("name") or "").strip()
        camp_name = str(campaign_data.get("name") or "").strip()
        if not game_name:
            return None
        if self.global_settings.matches_auto_priority(f"{camp_name} {game_name}"):
            if game_name not in self.auto_promoted_games:
                self.auto_promoted_games.insert(0, game_name)
                logger.info(
                    f"[RADAR] Auto-priority triggered for '{camp_name}' -> game '{game_name}' promoted to #1"
                )
            return game_name
        return None

    def _normalize_campaign_dict(self, raw_campaign: JsonType, box_art_url: str = "") -> JsonType | None:
        campaign_id = raw_campaign.get("id")
        if not isinstance(campaign_id, str):
            return None
        campaign: JsonType = deepcopy(raw_campaign)
        game = campaign.get("game")
        if not isinstance(game, dict):
            return None
        if box_art_url:
            game.setdefault("boxArtURL", box_art_url)
        if not isinstance(campaign.get("self"), dict):
            campaign["self"] = {"isAccountConnected": True}
        else:
            campaign["self"].setdefault("isAccountConnected", True)
        allowed = campaign.get("allow")
        if not isinstance(allowed, dict):
            campaign["allow"] = {"isEnabled": False, "channels": None}
        else:
            allowed.setdefault("channels", None)
        if not isinstance(campaign.get("timeBasedDrops"), list):
            campaign["timeBasedDrops"] = []
        campaign.setdefault("accountLinkURL", "")
        self.promote_game_if_matches(campaign)
        return campaign

    async def get_catalog_campaigns(
        self, *, proxy_url: str = "", force: bool = False
    ) -> dict[str, JsonType]:
        ttl = max(310, self.global_settings.radar_interval_sec)
        min_interval = 305.0 if force else float(ttl)
        now = time()
        if now < self._catalog_backoff_until:
            return deepcopy(self._catalog_campaigns)
        if self._catalog_campaigns and (now - self._catalog_fetched_at) < min_interval:
            return deepcopy(self._catalog_campaigns)

        async with self._catalog_lock:
            now = time()
            if now < self._catalog_backoff_until:
                return deepcopy(self._catalog_campaigns)
            if self._catalog_campaigns and (now - self._catalog_fetched_at) < min_interval:
                return deepcopy(self._catalog_campaigns)

            catalog_url = "https://twitch-drops-api.sunkwi.com/v2/drops"
            req_kwargs: JsonType = {"headers": {"Accept": "application/json"}}
            if proxy_url and not is_socks_proxy(proxy_url):
                req_kwargs["proxy"] = proxy_url
            try:
                timeout = aiohttp.ClientTimeout(total=20)
                async with aiohttp.ClientSession(
                    timeout=timeout,
                    connector=make_connector(proxy_url or None),
                    cookie_jar=aiohttp.DummyCookieJar(),
                    headers={"User-Agent": "TwitchMultiMiner catalog sync"},
                ) as session:
                    async with session.get(catalog_url, **req_kwargs) as response:
                        if response.status == 429:
                            retry_raw = (
                                response.headers.get("Retry-After")
                                or response.headers.get("ratelimit-reset")
                                or "310"
                            )
                            try:
                                retry_sec = float(retry_raw)
                            except ValueError:
                                retry_sec = 310.0
                            backoff_sec = max(60.0, retry_sec)
                            self._catalog_backoff_until = time() + backoff_sec
                            self._catalog_fetched_at = time()
                            self._save_disk_cache()
                            raise RequestException(f"HTTP 429 (backing off {round(backoff_sec)}s)")
                        if response.status != 200:
                            raise RequestException(f"HTTP {response.status}")
                        payload: JsonType = await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, RequestException, RuntimeError) as exc:
                logger.warning(f"[RADAR] Public catalog fetch failed: {exc}")
                return deepcopy(self._catalog_campaigns)

            groups = payload.get("data")
            if not isinstance(groups, list):
                return deepcopy(self._catalog_campaigns)

            applicable = ("ACTIVE", "UPCOMING")
            parsed: dict[str, JsonType] = {}
            for group in groups:
                if not isinstance(group, dict):
                    continue
                box_art = str(group.get("gameBoxArtURL", "") or "")
                for raw_camp in group.get("rewards") or []:
                    if not isinstance(raw_camp, dict):
                        continue
                    if raw_camp.get("status") not in applicable:
                        continue
                    norm = self._normalize_campaign_dict(raw_camp, box_art)
                    if norm is not None:
                        parsed[norm["id"]] = norm

            self._catalog_campaigns = parsed
            self._catalog_fetched_at = time()
            self._catalog_backoff_until = 0.0
            self._save_disk_cache()
            return deepcopy(self._catalog_campaigns)

    def get_verified_campaign_games(self, campaign_id: str) -> list[Game]:
        """Return observed stream categories where a campaign's drops were seen live."""
        g_map = self.verified_campaign_games.get(campaign_id)
        if not g_map:
            return []
        return [
            Game(g)
            for g in g_map.values()
            if isinstance(g, dict) and g.get("id") and (g.get("displayName") or g.get("name"))
        ]

    @staticmethod
    def _normalize_observed_game(channel_game: JsonType | None) -> JsonType | None:
        if not isinstance(channel_game, dict):
            return None
        gid = str(channel_game.get("id") or "").strip()
        gname = str(channel_game.get("displayName") or channel_game.get("name") or "").strip()
        if not gid or gid == "0" or not gname:
            return None
        norm: JsonType = {"id": gid, "name": gname}
        if channel_game.get("slug"):
            norm["slug"] = str(channel_game["slug"])
        return norm

    async def ingest_channel_campaigns(
        self,
        worker: AccountWorker,
        raw_avail: list[JsonType],
        *,
        channel_id: int,
        channel_login: str,
        channel_game: JsonType | None = None,
    ) -> list[str]:
        """Register campaigns reported by Twitch AvailableDrops directly (fetching details only if drops are missing)."""
        newly_discovered: list[str] = []
        missing_ids: list[str] = []
        avail_map: dict[str, JsonType] = {}
        norm_game = self._normalize_observed_game(channel_game)
        cache_dirty = False

        for item in raw_avail:
            if not isinstance(item, dict):
                continue
            cid = item.get("id")
            if not isinstance(cid, str) or not cid:
                continue
            avail_map[cid] = item
            ch_set = self.verified_channels.setdefault(cid, set())
            if channel_id not in ch_set:
                ch_set.add(channel_id)
                cache_dirty = True
            if norm_game is not None:
                g_map = self.verified_campaign_games.setdefault(cid, {})
                if g_map.get(norm_game["id"]) != norm_game:
                    g_map[norm_game["id"]] = dict(norm_game)
                    cache_dirty = True
            if cid not in self._direct_campaigns:
                drops_list = item.get("timeBasedDrops")
                if isinstance(drops_list, list) and drops_list:
                    norm = self._normalize_campaign_dict(item)
                    if norm is not None:
                        self._direct_campaigns[cid] = norm
                        newly_discovered.append(cid)
                        logger.info(
                            f"[RADAR] Direct Twitch discovery on {channel_login}: "
                            f"{norm.get('name')} ({(norm.get('game') or {}).get('name')})"
                        )
                else:
                    missing_ids.append(cid)

        if missing_ids:
            async with self._details_lock:
                to_fetch = [cid for cid in missing_ids if cid not in self._direct_campaigns]
                uid = worker.user_id
                if to_fetch and uid is not None:
                    for cid_chunk in chunk(to_fetch, 20):
                        try:
                            ops = [
                                GQL_QUERIES["CampaignDetails"].with_variables(
                                    {"channelLogin": str(uid), "dropID": cid}
                                )
                                for cid in cid_chunk
                            ]
                            responses: list[JsonType] = await worker.gql_request(ops)
                        except MinerException as exc:
                            logger.debug(f"[RADAR] CampaignDetails direct fetch error: {exc}")
                            responses = []
                        for idx, cid in enumerate(cid_chunk):
                            resp = responses[idx] if idx < len(responses) else {}
                            raw_camp = (
                                (resp.get("data") or {}).get("user", {}) or {}
                            ).get("dropCampaign")
                            if not isinstance(raw_camp, dict):
                                raw_camp = avail_map.get(cid)
                            if not isinstance(raw_camp, dict):
                                continue
                            norm = self._normalize_campaign_dict(raw_camp)
                            if norm is not None:
                                self._direct_campaigns[cid] = norm
                                newly_discovered.append(cid)
                                logger.info(
                                    f"[RADAR] Direct Twitch discovery on {channel_login}: "
                                    f"{norm.get('name')} ({(norm.get('game') or {}).get('name')})"
                                )
        if newly_discovered or cache_dirty:
            self._save_disk_cache()
        return newly_discovered

    async def discover_from_game_directories(self, worker: AccountWorker) -> bool:
        """Fallback direct Twitch GQL campaign discovery across top priority games when public catalog is unavailable."""
        exclude = {g.casefold() for g in self.global_settings.exclude_games}
        source_games = (
            list(self.global_settings.priority_games)
            if getattr(self.global_settings, "radar_preset", "default") != "pc_games"
            else []
        )
        candidates = [
            g for g in deduplicate([*source_games, *DEFAULT_PRIORITY_GAMES])
            if g and g.casefold() not in exclude
        ][:10]
        if not candidates:
            return False

        stream_batches = await asyncio.gather(
            *(
                worker.get_live_streams(Game({"id": "0", "name": g_name}), limit=3, drops_enabled=True)
                for g_name in candidates
            ),
            return_exceptions=True,
        )
        targets: dict[int, tuple[str, JsonType | None]] = {}
        for batch in stream_batches:
            if isinstance(batch, BaseException):
                continue
            for ch in batch:
                if ch.id not in targets:
                    ch_game: JsonType | None = (
                        {"id": str(ch.game.id), "name": ch.game.name}
                        if ch.game is not None
                        else None
                    )
                    targets[ch.id] = (ch.login, ch_game)

        if not targets:
            return False

        discovered = False
        target_items = list(targets.items())
        for t_chunk in chunk(target_items, 20):
            try:
                ops = [
                    GQL_QUERIES["AvailableDrops"].with_variables({"channelID": str(cid)})
                    for cid, _ in t_chunk
                ]
                resps = await worker.gql_request(ops)
            except MinerException:
                continue
            for (cid, (login, ch_game)), resp in zip(t_chunk, resps):
                avail_list = (
                    (resp.get("data") or {}).get("channel", {}).get("viewerDropCampaigns")
                    or []
                )
                if avail_list:
                    added = await self.ingest_channel_campaigns(
                        worker,
                        avail_list,
                        channel_id=cid,
                        channel_login=login,
                        channel_game=ch_game,
                    )
                    if added:
                        discovered = True
        return discovered

    async def poll_pinned_streamers(self, worker: AccountWorker, *, force: bool = False) -> bool:
        """Query Twitch GQL directly for live status and AvailableDrops on pinned streamers."""
        ttl = max(30, self.global_settings.radar_interval_sec)
        if not force and (time() - self.last_radar_poll_ts) < (ttl * 0.75):
            return False

        async with self._radar_lock:
            if not force and (time() - self.last_radar_poll_ts) < (ttl * 0.75):
                return False
            self.sync_pinned_list()
            logins = list(self.pinned_streams.keys())
            if not logins:
                self.last_radar_poll_ts = time()
                return False

            discovered_new = False
            online_targets: list[tuple[str, int]] = []
            now_ts = time()
            try:
                for login_chunk in chunk(logins, 20):
                    ops = [
                        GQL_QUERIES["GetStreamInfo"].with_variables({"channel": login})
                        for login in login_chunk
                    ]
                    responses = await worker.gql_request(ops)
                    for login, resp in zip(login_chunk, responses):
                        user_data = (resp.get("data") or {}).get("user")
                        status = self.pinned_streams.get(login) or PinnedStreamerStatus(login=login)
                        status.last_checked_ts = now_ts
                        if not isinstance(user_data, dict):
                            status.online = False
                            status.raw_channel_data = None
                            status.active_campaign_ids = []
                            status.active_campaign_names = []
                            self.pinned_streams[login] = status
                            continue
                        status.channel_id = int(user_data["id"])
                        status.display_name = str(user_data.get("displayName") or login)
                        stream_data = user_data.get("stream")
                        b_settings = user_data.get("broadcastSettings") or {}
                        if stream_data:
                            status.online = True
                            status.viewers = int(stream_data.get("viewersCount") or 0)
                            status.title = str(b_settings.get("title") or "")
                            game_obj = b_settings.get("game") or {}
                            status.game_name = str(
                                game_obj.get("displayName") or game_obj.get("name") or ""
                            )
                            status.game_id = int(game_obj["id"]) if game_obj.get("id") else None
                            status.raw_channel_data = user_data
                            online_targets.append((login, status.channel_id))
                        else:
                            status.online = False
                            status.viewers = 0
                            status.game_name = ""
                            status.game_id = None
                            status.title = ""
                            status.raw_channel_data = None
                            status.active_campaign_ids = []
                            status.active_campaign_names = []
                        self.pinned_streams[login] = status

                if online_targets:
                    for target_chunk in chunk(online_targets, 20):
                        avail_ops = [
                            GQL_QUERIES["AvailableDrops"].with_variables({"channelID": str(cid)})
                            for _, cid in target_chunk
                        ]
                        avail_resps = await worker.gql_request(avail_ops)
                        for (login, ch_id), resp in zip(target_chunk, avail_resps):
                            avail_list = (
                                (resp.get("data") or {})
                                .get("channel", {})
                                .get("viewerDropCampaigns")
                                or []
                            )
                            status = self.pinned_streams[login]
                            status.active_campaign_ids = [
                                c["id"] for c in avail_list if isinstance(c, dict) and c.get("id")
                            ]
                            status.active_campaign_names = [
                                str(c.get("name") or c.get("id"))
                                for c in avail_list
                                if isinstance(c, dict) and c.get("id")
                            ]
                            if avail_list:
                                st_game = (
                                    (status.raw_channel_data.get("broadcastSettings") or {}).get("game")
                                    if isinstance(status.raw_channel_data, dict)
                                    else None
                                )
                                added = await self.ingest_channel_campaigns(
                                    worker,
                                    avail_list,
                                    channel_id=ch_id,
                                    channel_login=login,
                                    channel_game=st_game,
                                )
                                if added:
                                    discovered_new = True
                self.last_radar_error = ""
            except MinerException as exc:
                self.last_radar_error = str(exc)
                logger.warning(f"[RADAR] Streamer poll warning: {exc}")
            finally:
                self.last_radar_poll_ts = time()

            return discovered_new

    async def get_merged_public_campaigns(
        self, *, proxy_url: str = "", force: bool = False
    ) -> dict[str, JsonType]:
        merged = await self.get_catalog_campaigns(proxy_url=proxy_url, force=force)
        for cid, direct_data in self._direct_campaigns.items():
            if cid not in merged:
                merged[cid] = deepcopy(direct_data)
            else:
                # Merge timeBasedDrops/allow if direct Twitch GQL gave richer info
                if not merged[cid].get("timeBasedDrops") and direct_data.get("timeBasedDrops"):
                    merged[cid]["timeBasedDrops"] = deepcopy(direct_data["timeBasedDrops"])
                merged[cid]["allow"] = merged[cid].get("allow") or deepcopy(direct_data.get("allow"))
        return merged

    def get_online_pinned_streamers(self) -> list[PinnedStreamerStatus]:
        online = [st for st in self.pinned_streams.values() if st.online and st.raw_channel_data]
        online.sort(key=lambda st: (bool(st.active_campaign_ids), st.viewers), reverse=True)
        return online

    def collect_available_campaign_games(self, inventory_games: Iterable[str] = ()) -> list[str]:
        games: list[str] = [str(g).strip() for g in inventory_games if str(g or "").strip()]
        for source in (self._direct_campaigns, self._catalog_campaigns):
            for camp in source.values():
                game = camp.get("game") or {}
                name = str(game.get("displayName") or game.get("name") or "").strip()
                if name:
                    games.append(name)
        return deduplicate(games)

    def get_installed_pc_games(
        self, *, force: bool = False, known_games: Iterable[str] = ()
    ) -> list[str]:
        """Return locally installed games matched against known campaign names (strictly local-only; never sent externally)."""
        now = time()
        if force or not self._pc_games_cache or (now - self._pc_games_scanned_at) >= 60.0:
            self._pc_games_cache = scan_installed_pc_games()
            self._pc_games_scanned_at = now

        campaign_games = self.collect_available_campaign_games()
        campaign_map: dict[str, str] = {}
        for g in campaign_games:
            key = _normalize_game_key(g)
            if key and key not in campaign_map:
                campaign_map[key] = g

        known_map: dict[str, str] = {}
        for g in known_games:
            clean = str(g or "").strip()
            key = _normalize_game_key(clean)
            if clean and key and key not in known_map:
                known_map[key] = clean

        resolved: list[tuple[int, str]] = []
        seen_keys: set[str] = set()
        for raw_name in self._pc_games_cache:
            key = _normalize_game_key(raw_name)
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            if key in campaign_map:
                tier = 0
                canonical = campaign_map[key]
            elif key in known_map:
                tier = 1
                canonical = known_map[key]
            else:
                tier = 2
                canonical = raw_name
            resolved.append((tier, canonical))

        resolved.sort(key=lambda item: (item[0], item[1].casefold()))
        return [name for _, name in resolved]
