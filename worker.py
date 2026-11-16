"""Isolated per-account async Twitch mining worker (orchestration, state machine, watch loop)."""
from __future__ import annotations

import asyncio
import logging
import random
from collections import OrderedDict, deque
from collections import abc
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import time
from typing import Any, Callable, Final, Literal, NoReturn, TYPE_CHECKING, cast

import aiohttp

from auth_transport import WorkerAuthState, WorkerTransportMixin, save_cookie_jar_protected
from channel import Channel
from channel_selector import ChannelSelectorMixin
from constants import (
    CALL,
    FILE_FORMATTER,
    GQL_QUERIES,
    PROFILES_DIR,
    SPADE_INTERVAL_SECONDS,
    WATCH_INTERVAL_MAX,
    WATCH_INTERVAL_MIN,
    ClientInfo,
    ClientType,
    JsonType,
    State,
    WebsocketTopic,
)
from exceptions import ExitRequest, GQLException, LoginException, MinerException, ReloadRequest
from i18n import tr
from inventory import DropsCampaign, TimedDrop
from settings import AccountProfileConfig, GlobalSettings
from utils import (
    AwaitableValue,
    ExponentialBackoff,
    Game,
    RateLimiter,
    chunk,
    task_wrapper,
    timestamp,
)
from websocket import WebsocketPool

if TYPE_CHECKING:
    from radar import SharedCampaignRadar


logger = logging.getLogger("TwitchMultiMiner.worker")
_WorkerAuthState = WorkerAuthState


@dataclass
class ClaimedDropRecord:
    timestamp_str: str
    account_name: str
    game_name: str
    drop_name: str
    campaign_name: str
    image_url: str = ""


@dataclass
class AccountSnapshot:
    name: str
    enabled: bool
    running: bool
    user_id: int | None
    username: str
    auth_status: str
    activation_code: str
    verification_uri: str
    status_text: str
    is_standby: bool
    watching_channel: str
    watching_game: str
    watching_viewers: int
    campaign_name: str
    drop_name: str
    progress_pct: float
    current_minutes: int
    required_minutes: int
    remaining_minutes: int
    claimed_count: int
    proxy: str
    ws_connected: int
    ws_total: int
    last_error: str
    drop_image_url: str = ""
    game_image_url: str = ""
    log_tail: list[str] = field(default_factory=list)


class AccountWorker(WorkerTransportMixin, ChannelSelectorMixin):
    """Isolated async miner worker for a single Twitch account profile."""

    def __init__(
        self,
        config: AccountProfileConfig,
        global_settings: GlobalSettings,
        radar: SharedCampaignRadar,
        *,
        profiles_dir: Path = PROFILES_DIR,
        on_update: Callable[[AccountWorker], None] | None = None,
        on_log: Callable[[str, str], None] | None = None,
        on_claim: Callable[[ClaimedDropRecord], None] | None = None,
    ) -> None:
        self.config: AccountProfileConfig = config
        self.name: str = config.name
        self.global_settings: GlobalSettings = global_settings
        self.radar: SharedCampaignRadar = radar
        self.profiles_dir: Path = profiles_dir

        self._on_update_cb = on_update
        self._on_log_cb = on_log
        self._on_claim_cb = on_claim

        self.cookies_path = AccountProfileConfig.cookies_path(self.name, self.profiles_dir)
        self.log_path = AccountProfileConfig.log_path(self.name, self.profiles_dir)

        self._client_type: ClientInfo = ClientType.SMARTBOX
        self.user_agent: str = config.user_agent or ClientType.SMARTBOX.pick_user_agent()
        self._proxy_in_connector: bool = False
        self._session: aiohttp.ClientSession | None = None
        self._gql_limiter = RateLimiter(capacity=5, window=1)
        self._auth_state = WorkerAuthState(self)
        self._crash_backoff = ExponentialBackoff(base=2, shift=5, maximum=300)

        self._state: State = State.IDLE
        self._idle_status_key: str = "status_idle"
        self._state_change = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._watching_restart = asyncio.Event()

        self.wanted_games: list[Game] = []
        self.inventory: list[DropsCampaign] = []
        self._drops: dict[str, TimedDrop] = {}
        self._campaigns: dict[str, DropsCampaign] = {}
        self._mnt_triggers: deque[datetime] = deque()

        self.channels: OrderedDict[int, Channel] = OrderedDict()
        self.watching_channel: AwaitableValue[Channel] = AwaitableValue()
        self.is_standby: bool = False
        self.claimed_count: int = 0
        self._claimed_drop_ids: set[str] = set()
        self._stalled_channels: dict[int, float] = {}
        self._zero_progress_streak: int = 0
        self._zero_progress_channel_id: int | None = None
        self.status_text: str = self._t("status_stopped")
        self.last_error: str = ""
        self.log_buffer: deque[str] = deque(maxlen=200)
        self.ws_statuses: dict[int, tuple[str, int]] = {}

        self.websocket = WebsocketPool(self)
        self._main_task: asyncio.Task[None] | None = None
        self._watching_task: asyncio.Task[None] | None = None
        self._mnt_task: asyncio.Task[None] | None = None
        self._file_handler: logging.FileHandler | None = None
        self._file_logger: logging.Logger | None = None

    def _t(self, key: str, **kwargs: Any) -> str:
        return tr(key, getattr(self.global_settings, "language", "ru"), **kwargs)

    @property
    def user_id(self) -> int | None:
        return self._auth_state.user_id

    @property
    def is_authenticated(self) -> bool:
        return bool(self._auth_state.user_id is not None and self._auth_state.access_token)

    @property
    def proxy_url(self) -> str:
        return self.config.proxy.strip()

    @property
    def proxy_in_connector(self) -> bool:
        return self._proxy_in_connector

    @property
    def running(self) -> bool:
        return self._main_task is not None and not self._main_task.done()

    @property
    def stop_requested(self) -> bool:
        return self._stop_event.is_set()

    async def ensure_user_id(self) -> int:
        auth = await self.get_auth()
        assert auth.user_id is not None
        return auth.user_id

    def log(self, message: str, level: int = logging.INFO) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] [{self.name}] {message}"
        self.log_buffer.append(line)
        logger.log(level, f"[{self.name}] {message}")
        if self._file_logger is not None:
            self._file_logger.log(level, message)
        if self._on_log_cb is not None:
            self._on_log_cb(self.name, line)

    def notify_ui_update(self) -> None:
        if self._on_update_cb is not None:
            self._on_update_cb(self)

    def on_websocket_status(self, idx: int, status: str | None, topics: int | None) -> None:
        prev_status, prev_topics = self.ws_statuses.get(idx, ("disconnected", 0))
        self.ws_statuses[idx] = (
            status if status is not None else prev_status,
            topics if topics is not None else prev_topics,
        )
        self.notify_ui_update()

    def on_websocket_removed(self, idx: int) -> None:
        self.ws_statuses.pop(idx, None)
        self.notify_ui_update()

    def on_critical_error(self, exc: BaseException) -> None:
        self.last_error = str(exc)
        self.status_text = self._t("err_critical", error=exc)
        self.log(self._t("log_critical_restart", error=exc), level=logging.ERROR)
        self.change_state(State.RESTART)

    def record_claimed_drop(self, drop: Any) -> None:
        drop_id = str(getattr(drop, "id", "") or "")
        if drop_id:
            if drop_id in self._claimed_drop_ids:
                return
            self._claimed_drop_ids.add(drop_id)
        self.claimed_count += 1
        rec = ClaimedDropRecord(
            timestamp_str=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            account_name=self.name,
            game_name=drop.campaign.game.name,
            drop_name=drop.rewards_text(),
            campaign_name=drop.campaign.name,
            image_url=drop.primary_image_url,
        )
        self.log(self._t("log_drop_claimed", game=rec.game_name, drop=rec.drop_name))
        if self._on_claim_cb is not None:
            self._on_claim_cb(rec)
        self.notify_ui_update()

    def _setup_file_logger(self) -> None:
        if self._file_handler is None:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.FileHandler(self.log_path, encoding="utf8", errors="replace")
            handler.setFormatter(FILE_FORMATTER)
            file_logger = logging.getLogger(f"TwitchMultiMiner.profile.{self.name}")
            file_logger.setLevel(logging.DEBUG)
            file_logger.propagate = False
            for existing in list(file_logger.handlers):
                file_logger.removeHandler(existing)
            file_logger.addHandler(handler)
            self._file_handler = handler
            self._file_logger = file_logger

    def _close_file_logger(self) -> None:
        if self._file_logger is not None and self._file_handler is not None:
            with suppress(Exception):
                self._file_logger.removeHandler(self._file_handler)
        if self._file_handler is not None:
            with suppress(Exception):
                self._file_handler.close()
            self._file_handler = None
        self._file_logger = None

    async def sleep_or_stop(self, seconds: float) -> None:
        if seconds <= 0 or self._stop_event.is_set():
            return
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)

    def start(self) -> None:
        if self.running:
            return
        self._stop_event.clear()
        self.last_error = ""
        self.status_text = self._t("status_starting")
        self._setup_file_logger()
        self._main_task = asyncio.create_task(self.run())
        self.notify_ui_update()

    def stop_nowait(self) -> None:
        self._stop_event.set()
        self.change_state(State.EXIT)

    async def stop(self) -> None:
        self.stop_nowait()
        if self._main_task is not None:
            with suppress(asyncio.TimeoutError, asyncio.CancelledError):
                await asyncio.wait_for(self._main_task, timeout=5)
            self._main_task = None
        await self.shutdown()
        self.status_text = self._t("status_stopped")
        self.notify_ui_update()

    def trigger_inventory_refresh(self) -> None:
        if self.running and self._state is not State.EXIT:
            self.change_state(State.INVENTORY_FETCH)

    def invalidate_session(self) -> None:
        self._auth_state.invalidate(delete_cookies=True)
        if self.running:
            self.change_state(State.RESTART)

    async def shutdown(self) -> None:
        self.stop_watching()
        if self._watching_task is not None:
            self._watching_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._watching_task
            self._watching_task = None
        if self._mnt_task is not None:
            self._mnt_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._mnt_task
            self._mnt_task = None
        await self.websocket.stop(clear_topics=True)
        if self._session is not None:
            jar = cast(aiohttp.CookieJar, self._session.cookie_jar)
            if self._auth_state.access_token or any(True for _ in jar):
                with suppress(Exception):
                    save_cookie_jar_protected(
                        jar,
                        self.cookies_path,
                        self._client_type.CLIENT_URL,
                        self._auth_state.access_token,
                    )
            await self._session.close()
            self._session = None
        for ch in self.channels.values():
            ch.remove()
        self._drops.clear()
        self.channels.clear()
        self.inventory.clear()
        self._campaigns.clear()
        self._auth_state.clear()
        self.wanted_games.clear()
        self._mnt_triggers.clear()
        self._close_file_logger()

    def wait_until_login(self) -> abc.Coroutine[Any, Any, Literal[True]]:
        return self._auth_state._logged_in.wait()

    def change_state(self, state: State) -> None:
        if self._state is not State.EXIT or state is State.EXIT:
            self._state = state
        self._state_change.set()

    async def get_auth(self) -> WorkerAuthState:
        await self._auth_state.validate()
        return self._auth_state

    async def _recover_after_restart(self, delay: float = 0.0) -> bool:
        await self.shutdown()
        if delay > 0:
            await self.sleep_or_stop(delay)
        if self._stop_event.is_set():
            return False
        self._state = State.IDLE
        self._state_change.clear()
        self._setup_file_logger()
        return True

    async def run(self) -> None:
        crash_iter = iter(self._crash_backoff)
        while not self._stop_event.is_set():
            try:
                await self._run()
                break
            except ReloadRequest:
                self.log(self._t("worker_restarting"))
                if not await self._recover_after_restart():
                    break
            except (ExitRequest, asyncio.CancelledError):
                break
            except LoginException as exc:
                self.last_error = str(exc)
                delay = round(next(crash_iter))
                self.status_text = self._t("err_login", error=exc)
                self.log(self._t("worker_login_retry", error=exc, delay=delay), level=logging.ERROR)
                if not await self._recover_after_restart(delay):
                    break
            except Exception as exc:
                self.last_error = str(exc)
                delay = round(next(crash_iter))
                self.status_text = self._t("err_critical", error=exc)
                self.log(self._t("worker_crash_retry", error=exc, delay=delay), level=logging.ERROR)
                logger.exception(f"[{self.name}] Worker crashed; retrying in {delay}s")
                if not await self._recover_after_restart(delay):
                    break
        await self.shutdown()
        if self._stop_event.is_set() and not self.last_error:
            self.status_text = self._t("status_stopped")
        self.notify_ui_update()

    async def _run(self) -> None:
        self.status_text = self._t("status_auth")
        self.notify_ui_update()
        auth_state = await self.get_auth()
        await self.websocket.start()

        if self._watching_task is not None:
            self._watching_task.cancel()
        self._watching_task = asyncio.create_task(self._watch_loop())

        assert auth_state.user_id is not None
        self.websocket.add_topics(
            [
                WebsocketTopic("User", "Drops", auth_state.user_id, self.process_drops),
                WebsocketTopic("User", "Notifications", auth_state.user_id, self.process_notifications),
            ]
        )

        full_cleanup = False
        channels: Final[OrderedDict[int, Channel]] = self.channels
        self.change_state(State.INVENTORY_FETCH)

        while not self._stop_event.is_set():
            if self._state is State.IDLE:
                self.stop_watching()
                self.status_text = self._t(self._idle_status_key)
                self.notify_ui_update()
                self._state_change.clear()
            elif self._state is State.INVENTORY_FETCH:
                self._idle_status_key = "status_idle"
                self.status_text = self._t("status_inv_sync")
                self.notify_ui_update()
                await self.websocket.start()
                await self.fetch_inventory()
                self._crash_backoff.reset()
                self.last_error = ""
                self.change_state(State.GAMES_UPDATE)
            elif self._state is State.GAMES_UPDATE:
                for campaign in self.inventory:
                    if not campaign.upcoming:
                        for drop in campaign.drops:
                            if drop.can_claim or drop.ready_to_claim:
                                await drop.ensure_claim()
                self._compute_wanted_games()
                full_cleanup = True
                self.restart_watching()
                self.change_state(State.CHANNELS_CLEANUP)
            elif self._state is State.CHANNELS_CLEANUP:
                self.status_text = self._t("status_cleanup")
                if not self.wanted_games or full_cleanup:
                    to_remove = list(channels.values())
                else:
                    to_remove = [
                        ch
                        for ch in channels.values()
                        if (
                            not ch.acl_based
                            and not ch.pinned
                            and (ch.offline or ch.game is None or ch.game not in self.wanted_games)
                        )
                    ]
                full_cleanup = False
                if to_remove:
                    topics_to_rm: list[str] = []
                    for ch in to_remove:
                        topics_to_rm.append(WebsocketTopic.as_str("Channel", "StreamState", ch.id))
                        topics_to_rm.append(WebsocketTopic.as_str("Channel", "StreamUpdate", ch.id))
                    self.websocket.remove_topics(topics_to_rm)
                    for ch in to_remove:
                        channels.pop(ch.id, None)
                        ch.remove()
                if self.wanted_games:
                    self.change_state(State.CHANNELS_FETCH)
                else:
                    self.stop_watching()
                    self._idle_status_key = "status_no_campaigns"
                    self.log(self._t("status_no_campaigns"))
                    self.change_state(State.IDLE)
            elif self._state is State.CHANNELS_FETCH:
                self.status_text = self._t("status_searching")
                self.notify_ui_update()
                await self._fetch_channels()
                self.change_state(State.CHANNEL_SWITCH)
            elif self._state is State.CHANNEL_SWITCH:
                self._switch_channel()
                self._state_change.clear()
            elif self._state is State.RESTART:
                raise ReloadRequest()
            elif self._state is State.EXIT:
                break

            if not self._state_change.is_set():
                await self._state_change.wait()

    def watch(self, channel: Channel, *, standby: bool = False, update_status: bool = True) -> None:
        self.is_standby = standby
        self.watching_channel.set(channel)
        if update_status:
            if standby:
                self.status_text = self._t("status_standby", channel=channel.name)
                self.log(self._t("log_standby_start", channel=channel.name, game=channel.game or "Online"))
            else:
                self.status_text = self._t("status_mining", channel=channel.name)
                self.log(self._t("log_watch_start", channel=channel.name, game=channel.game or "?"))
        self.notify_ui_update()

    def stop_watching(self) -> None:
        self.is_standby = False
        self.watching_channel.clear()
        self.notify_ui_update()

    def restart_watching(self) -> None:
        self._watching_restart.set()

    async def _watch_sleep(self, delay: float) -> None:
        if delay <= 0:
            return
        self._watching_restart.clear()
        done, pending = await asyncio.wait(
            [
                asyncio.create_task(self._watching_restart.wait()),
                asyncio.create_task(self._stop_event.wait()),
            ],
            timeout=delay,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        for task in done:
            with suppress(Exception):
                await task

    @task_wrapper(critical=True)
    async def _watch_loop(self) -> NoReturn:
        last_spade_ts = 0.0
        last_gql_sync_ts = 0.0
        while not self._stop_event.is_set():
            channel = await self.watching_channel.get()
            if not channel.online:
                self.stop_watching()
                self.change_state(State.CHANNEL_SWITCH)
                continue

            now = time()
            send_spade = self.global_settings.hybrid_watch_spade and (
                now - last_spade_ts >= SPADE_INTERVAL_SECONDS
            )
            succeeded = await channel.send_watch_hybrid(send_spade=send_spade)
            if send_spade:
                last_spade_ts = now
            if not succeeded:
                logger.log(CALL, f"[{self.name}] Watch pulse warning on {channel.name}")

            await asyncio.sleep(10)
            if self._stop_event.is_set():
                break

            # Active GQL drop progress sync every ~55s (solves WS delays & triggers auto-claim)
            if not self.is_standby and (time() - last_gql_sync_ts) >= 55.0:
                last_gql_sync_ts = time()
                await self._sync_active_drop_progress(channel, watch_ok=succeeded)

            interval = random.uniform(WATCH_INTERVAL_MIN, WATCH_INTERVAL_MAX)
            await self._watch_sleep(max(1.0, interval - 10.0))

    async def _sync_active_drop_progress(self, channel: Channel, *, watch_ok: bool = True) -> None:
        drop_data: JsonType | None = None
        gql_ok = False
        try:
            context = await self.gql_request(
                GQL_QUERIES["CurrentDrop"].with_variables({"channelID": str(channel.id)})
            )
            drop_data = (context.get("data") or {}).get("currentUser", {}).get("dropCurrentSession")
            gql_ok = True
        except GQLException:
            drop_data = None

        if isinstance(drop_data, dict):
            drop = self._drops.get(drop_data.get("dropID", ""))
            if drop is not None and drop.can_earn(channel):
                self._zero_progress_streak = 0
                self._zero_progress_channel_id = channel.id
                mins = int(drop_data.get("currentMinutesWatched") or 0)
                drop.update_minutes(mins)
                if drop.ready_to_claim:
                    await drop.ensure_claim()
                    self.change_state(State.INVENTORY_FETCH)
                return

        # If CurrentDrop GQL succeeded with empty dropID repeatedly while watch pulses succeeded
        # (e.g. sub-only drop or channel stopped crediting drops), switch to another channel.
        if watch_ok and gql_ok:
            if self._zero_progress_channel_id != channel.id:
                self._zero_progress_channel_id = channel.id
                self._zero_progress_streak = 1
            else:
                self._zero_progress_streak += 1
            if self._zero_progress_streak >= 3:
                self._stalled_channels[channel.id] = time() + 900.0
                self._zero_progress_streak = 0
                self.log(
                    f"На канале {channel.name} прогресс дропса не начисляется — переключаюсь на другой канал."
                )
                self.stop_watching()
                self.change_state(State.CHANNEL_SWITCH)
            return

        # Only estimate +1 local minute when the watch pulse succeeded AND CurrentDrop GQL itself failed;
        # otherwise wait for real Twitch progress instead of inflating local minutes that later roll back.
        if watch_ok and not gql_ok:
            if (active_campaign := self.get_active_campaign(channel)) is not None:
                needs_resync = active_campaign.bump_minutes(channel)
                if needs_resync:
                    for d in active_campaign.drops:
                        d.reset_unconfirmed()
                    try:
                        await self.fetch_inventory(lightweight=True)
                    except MinerException:
                        pass

    @task_wrapper(critical=True)
    async def _maintenance_task(self) -> None:
        now = datetime.now(timezone.utc)
        next_period = now + timedelta(minutes=20)
        while not self._stop_event.is_set():
            now = datetime.now(timezone.utc)
            if now >= next_period:
                break
            next_trigger = next_period
            while self._mnt_triggers and self._mnt_triggers[0] <= next_trigger:
                next_trigger = self._mnt_triggers.popleft()
            wait_sec = max(1.0, (next_trigger - now).total_seconds())
            await self.sleep_or_stop(wait_sec)
            if self._stop_event.is_set():
                return
            if next_trigger != next_period:
                self.change_state(State.CHANNELS_CLEANUP)
        if not self._stop_event.is_set():
            self.change_state(State.INVENTORY_FETCH)

    async def fetch_inventory(self, *, lightweight: bool = False) -> None:
        response = await self.gql_request(GQL_QUERIES["Inventory"])
        inventory: JsonType = (response.get("data") or {}).get("currentUser", {}).get("inventory") or {}
        ongoing_campaigns: list[JsonType] = inventory.get("dropCampaignsInProgress") or []
        claimed_benefits: dict[str, datetime] = {
            b["id"]: timestamp(b["lastAwardedAt"])
            for b in (inventory.get("gameEventDrops") or [])
            if b.get("id") and b.get("lastAwardedAt")
        }
        inventory_data: dict[str, JsonType] = {c["id"]: c for c in ongoing_campaigns if c.get("id")}

        dashboard_map: dict[str, JsonType] = {}
        try:
            camp_resp = await self.gql_request(GQL_QUERIES["Campaigns"])
            dash_list = (
                (camp_resp.get("data") or {}).get("currentUser", {}).get("dropCampaigns") or []
            )
            for item in dash_list:
                if isinstance(item, dict) and item.get("id"):
                    cid = str(item["id"])
                    cached_direct = self.radar._direct_campaigns.get(cid)
                    if cached_direct and cached_direct.get("timeBasedDrops") and not item.get("timeBasedDrops"):
                        merged_item = dict(cached_direct)
                        merged_item.update({k: v for k, v in item.items() if v is not None})
                        dashboard_map[cid] = merged_item
                    else:
                        dashboard_map[cid] = item
        except MinerException:
            dashboard_map = {}

        if self.user_id is not None:
            need_details = [
                cid
                for cid, item in dashboard_map.items()
                if item.get("status") in ("ACTIVE", "UPCOMING")
                and not item.get("timeBasedDrops")
                and cid not in inventory_data
            ]
            for cid_chunk in chunk(need_details, 20):
                try:
                    ops = [
                        GQL_QUERIES["CampaignDetails"].with_variables(
                            {"channelLogin": str(self.user_id), "dropID": cid}
                        )
                        for cid in cid_chunk
                    ]
                    det_resps = await self.gql_request(ops)
                except MinerException:
                    continue
                for cid, det_resp in zip(cid_chunk, det_resps):
                    raw_det = (
                        (det_resp.get("data") or {}).get("user", {}) or {}
                    ).get("dropCampaign")
                    if isinstance(raw_det, dict):
                        base = dict(dashboard_map.get(cid, {}))
                        base.update({k: v for k, v in raw_det.items() if v is not None})
                        dashboard_map[cid] = base
                        norm = self.radar._normalize_campaign_dict(base)
                        if norm is not None:
                            self.radar._direct_campaigns[cid] = norm

        if dashboard_map:
            inventory_data = self._merge_data(inventory_data, dashboard_map)

        if not lightweight:
            await self.radar.poll_pinned_streamers(self)
        public_campaigns = await self.radar.get_merged_public_campaigns(proxy_url=self.proxy_url)
        inventory_data = self._merge_data(inventory_data, public_campaigns)

        for c_data in inventory_data.values():
            if not c_data.get("game"):
                c_data["game"] = {
                    "id": "0",
                    "name": "Special Events",
                    "displayName": "Special Events",
                    "boxArtURL": "",
                }

        campaigns: list[DropsCampaign] = []
        for cid, campaign_data in inventory_data.items():
            self.radar.promote_game_if_matches(campaign_data)
            verified_ids = self.radar.verified_channels.get(cid, set())
            campaigns.append(
                DropsCampaign(self, campaign_data, claimed_benefits, verified_channel_ids=verified_ids)
            )

        campaigns.sort(key=lambda c: c.active, reverse=True)
        campaigns.sort(key=lambda c: c.upcoming and c.starts_at or c.ends_at)
        campaigns.sort(key=lambda c: c.eligible, reverse=True)

        self._drops.clear()
        self.inventory.clear()
        self._campaigns.clear()
        self._mnt_triggers.clear()
        switch_triggers: set[datetime] = set()
        next_hour = datetime.now(timezone.utc) + timedelta(hours=1)
        for campaign in campaigns:
            self._drops.update({drop.id: drop for drop in campaign.drops})
            if campaign.can_earn_within(next_hour):
                switch_triggers.update(campaign.time_triggers)
            self.inventory.append(campaign)
            self._campaigns[campaign.id] = campaign

        self._mnt_triggers.extend(sorted(switch_triggers))
        now = datetime.now(timezone.utc)
        while self._mnt_triggers and self._mnt_triggers[0] <= now:
            self._mnt_triggers.popleft()

        if not lightweight:
            if self._mnt_task is not None and not self._mnt_task.done():
                self._mnt_task.cancel()
            self._mnt_task = asyncio.create_task(self._maintenance_task())
        self.notify_ui_update()

    @task_wrapper
    async def process_stream_state(self, channel_id: int, message: JsonType) -> None:
        msg_type = message.get("type")
        channel = self.channels.get(channel_id)
        if channel is None:
            return
        if msg_type == "viewcount":
            if not channel.online:
                channel.check_online()
            else:
                channel.viewers = int(message.get("viewers") or 0)
                self.notify_ui_update()
        elif msg_type == "stream-down":
            channel.set_offline()
        elif msg_type == "stream-up":
            channel.check_online()

    @task_wrapper
    async def process_stream_update(self, channel_id: int, message: JsonType) -> None:
        channel = self.channels.get(channel_id)
        if channel is not None:
            channel.check_online()

    @task_wrapper
    async def process_drops(self, user_id: int, message: JsonType) -> None:
        msg_type: str = message.get("type", "")
        if msg_type not in ("drop-progress", "drop-claim"):
            return
        data = message.get("data") or {}
        drop_id = data.get("drop_id", "")
        drop = self._drops.get(drop_id)
        watching = self.watching_channel.get_with_default(None)
        if msg_type == "drop-claim":
            if drop is None:
                return
            drop.update_claim(data["drop_instance_id"])
            campaign = drop.campaign
            await drop.claim()
            await asyncio.sleep(4)
            if watching is not None:
                for _ in range(6):
                    ctx = await self.gql_request(
                        GQL_QUERIES["CurrentDrop"].with_variables({"channelID": str(watching.id)})
                    )
                    cur = (ctx.get("data") or {}).get("currentUser", {}).get("dropCurrentSession")
                    if cur is None or cur.get("dropID") != drop.id:
                        break
                    await asyncio.sleep(2)
            if campaign.can_earn(watching):
                self.restart_watching()
            else:
                self.change_state(State.INVENTORY_FETCH)
            return

        if drop is not None and drop.can_earn(watching):
            cur_min = int(data.get("current_progress_min") or 0)
            drop.update_minutes(cur_min)
            if drop.ready_to_claim:
                await drop.ensure_claim()
                self.change_state(State.INVENTORY_FETCH)
        self.notify_ui_update()

    @task_wrapper
    async def process_notifications(self, user_id: int, message: JsonType) -> None:
        if message.get("type") == "create-notification":
            data: JsonType = message.get("data", {}).get("notification", {})
            if data.get("type") in (
                "user_drop_reward_reminder_notification",
                "quests_viewer_reward_campaign_earned_emote",
            ):
                self.change_state(State.INVENTORY_FETCH)
                with suppress(MinerException):
                    await self.gql_request(
                        GQL_QUERIES["NotificationsDelete"].with_variables({"input": {"id": data["id"]}})
                    )

    def snapshot(self) -> AccountSnapshot:
        watching = self.watching_channel.get_with_default(None)
        active_campaign = self.get_active_campaign(watching) if (watching and not self.is_standby) else None
        active_drop = active_campaign.first_drop if active_campaign else None

        pct = 0.0
        cur_min = 0
        req_min = 0
        rem_min = 0
        if active_drop is not None:
            pct = active_drop.progress * 100.0
            cur_min = active_drop.current_minutes
            req_min = active_drop.required_minutes
            rem_min = active_drop.remaining_minutes

        ws_conn = sum(1 for st, _ in self.ws_statuses.values() if st == "connected")
        ws_tot = len(self.ws_statuses)

        return AccountSnapshot(
            name=self.name,
            enabled=self.config.enabled,
            running=self.running,
            user_id=self.user_id,
            username=self._auth_state.username,
            auth_status=self._auth_state.auth_status,
            activation_code=self._auth_state.activation_code,
            verification_uri=self._auth_state.verification_uri,
            status_text=self.status_text,
            is_standby=self.is_standby,
            watching_channel=watching.name if watching else "—",
            watching_game=str(watching.game) if (watching and watching.game) else "—",
            watching_viewers=(watching.viewers or 0) if watching else 0,
            campaign_name=active_campaign.name if active_campaign else ("Standby Radar" if self.is_standby else "—"),
            drop_name=active_drop.rewards_text() if active_drop else "—",
            progress_pct=pct,
            current_minutes=cur_min,
            required_minutes=req_min,
            remaining_minutes=rem_min,
            claimed_count=self.claimed_count,
            proxy=self.proxy_url,
            ws_connected=ws_conn,
            ws_total=ws_tot,
            last_error=self.last_error,
            drop_image_url=active_drop.primary_image_url if active_drop else "",
            game_image_url=str(active_campaign.image_url) if active_campaign else "",
            log_tail=list(self.log_buffer),
        )
