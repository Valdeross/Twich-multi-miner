"""Isolated per-account async Twitch mining worker with Device Code auth and Standby mode."""
from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict, abc, deque
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from time import time
from typing import Any, Callable, Final, Literal, NoReturn, overload, TYPE_CHECKING, cast

import aiohttp
from yarl import URL

from channel import Channel, Stream
from constants import (
    CALL,
    FILE_FORMATTER,
    GQL_QUERIES,
    MAX_CHANNELS,
    MAX_INT,
    PROFILES_DIR,
    SPADE_INTERVAL_SECONDS,
    WATCH_INTERVAL_MAX,
    WATCH_INTERVAL_MIN,
    ClientInfo,
    ClientType,
    GQLOperation,
    JsonType,
    PriorityMode,
    State,
    WebsocketTopic,
)
from exceptions import (
    ExitRequest,
    GQLException,
    LoginException,
    MinerException,
    ReloadRequest,
    RequestException,
    RequestInvalid,
)
from inventory import DropsCampaign, TimedDrop
from settings import AccountProfileConfig, GlobalSettings
from utils import (
    AwaitableValue,
    ExponentialBackoff,
    Game,
    RateLimiter,
    chunk,
    deduplicate,
    is_socks_proxy,
    make_connector,
    task_wrapper,
    timestamp,
)
from websocket import WebsocketPool

if TYPE_CHECKING:
    from radar import SharedCampaignRadar


logger = logging.getLogger("TwitchMultiMiner.worker")


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


class _WorkerAuthState:
    def __init__(self, worker: AccountWorker) -> None:
        self._worker: AccountWorker = worker
        self._lock = asyncio.Lock()
        self._logged_in = asyncio.Event()
        self.user_id: int | None = worker.config.user_id
        self.username: str = worker.config.username
        self.device_id: str = worker.config.device_id
        self.session_id: str = worker.config.session_id
        self.access_token: str = ""
        self.activation_code: str = ""
        self.verification_uri: str = ""
        self.auth_status: str = "Не авторизован"

    def invalidate(self, *, delete_cookies: bool = False) -> None:
        self.access_token = ""
        self.user_id = None
        self.activation_code = ""
        self.verification_uri = ""
        self.auth_status = "Требуется вход"
        self._logged_in.clear()
        if delete_cookies:
            if self._worker._session is not None:
                jar = cast(aiohttp.CookieJar, self._worker._session.cookie_jar)
                jar.clear()
            self._worker.cookies_path.unlink(missing_ok=True)
            self._worker.config.user_id = None
            self._worker.config.username = ""
            self._worker.config.save(self._worker.profiles_dir)
        self._worker.notify_ui_update()

    def clear(self) -> None:
        self.access_token = ""
        self.activation_code = ""
        self.verification_uri = ""
        self._logged_in.clear()

    def headers(self, *, user_agent: str = "", gql: bool = False) -> JsonType:
        client_info: ClientInfo = self._worker._client_type
        hdrs: JsonType = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip",
            "Accept-Language": "en-US",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "Client-Id": client_info.CLIENT_ID,
            "User-Agent": user_agent or self._worker.user_agent,
            "Client-Session-Id": self.session_id,
            "X-Device-Id": self.device_id,
        }
        if gql and self.access_token:
            hdrs["Origin"] = str(client_info.CLIENT_URL)
            hdrs["Referer"] = str(client_info.CLIENT_URL)
            hdrs["Authorization"] = f"OAuth {self.access_token}"
        return hdrs

    async def _oauth_device_login(self) -> str:
        client_info: ClientInfo = self._worker._client_type
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "Accept-Language": "en-US",
            "Cache-Control": "no-cache",
            "Client-Id": client_info.CLIENT_ID,
            "Host": "id.twitch.tv",
            "Origin": str(client_info.CLIENT_URL),
            "Pragma": "no-cache",
            "Referer": str(client_info.CLIENT_URL),
            "User-Agent": self._worker.user_agent,
            "X-Device-Id": self.device_id,
        }
        while not self._worker.stop_requested:
            try:
                now = datetime.now(timezone.utc)
                init_payload = {"client_id": client_info.CLIENT_ID, "scopes": ""}
                async with self._worker.request(
                    "POST", "https://id.twitch.tv/oauth2/device", headers=headers, data=init_payload
                ) as response:
                    response_json: JsonType = await response.json()
                    required_fields = (
                        "device_code",
                        "user_code",
                        "interval",
                        "verification_uri",
                        "expires_in",
                    )
                    missing = [f for f in required_fields if f not in response_json]
                    if response.status != 200 or missing:
                        detail = str(
                            response_json.get("message")
                            or response_json.get("error")
                            or f"HTTP {response.status}"
                        )
                        raise LoginException(f"Device auth failed: {detail}")
                    device_code = str(response_json["device_code"])
                    user_code = str(response_json["user_code"])
                    interval = max(2, int(response_json["interval"]))
                    verification_uri = str(response_json["verification_uri"])
                    expires_at = now + timedelta(seconds=int(response_json["expires_in"]))

                self.activation_code = user_code
                self.verification_uri = verification_uri
                self.auth_status = f"Код: {user_code}"
                self._worker.log(
                    f"Требуется активация аккаунта! Перейдите по {verification_uri} и введите код: {user_code}"
                )
                self._worker.notify_ui_update()

                token_payload = {
                    "client_id": client_info.CLIENT_ID,
                    "device_code": device_code,
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                }
                while not self._worker.stop_requested:
                    await self._worker.sleep_or_stop(interval)
                    if self._worker.stop_requested:
                        raise ExitRequest()
                    async with self._worker.request(
                        "POST",
                        "https://id.twitch.tv/oauth2/token",
                        headers=headers,
                        data=token_payload,
                        invalidate_after=expires_at,
                    ) as token_resp:
                        if token_resp.status != 200:
                            continue
                        token_json = await token_resp.json()
                        self.access_token = str(token_json["access_token"])
                        self.activation_code = ""
                        self.verification_uri = ""
                        self._worker.notify_ui_update()
                        return self.access_token
            except RequestInvalid:
                self._worker.log("Код активации истёк, запрашиваю новый...")
                continue
        raise ExitRequest()

    async def validate(self) -> None:
        async with self._lock:
            await self._validate()

    async def _validate(self) -> None:
        if self.access_token and self.user_id is not None and self._logged_in.is_set():
            return

        session = await self._worker.get_session()
        jar = cast(aiohttp.CookieJar, session.cookie_jar)
        client_info: ClientInfo = self._worker._client_type
        self.auth_status = "Проверка входа..."
        self._worker.notify_ui_update()

        try:
            async with self._worker.request(
                "GET", client_info.CLIENT_URL, headers=self.headers()
            ) as resp:
                await resp.text("utf8")
            cookie = jar.filter_cookies(client_info.CLIENT_URL)
            if "unique_id" in cookie and cookie["unique_id"].value:
                self.device_id = cookie["unique_id"].value
                self._worker.config.device_id = self.device_id
        except RequestException:
            pass

        validate_response: JsonType | None = None
        for _mismatch in range(2):
            for _attempt in range(2):
                cookie = jar.filter_cookies(client_info.CLIENT_URL)
                if "auth-token" not in cookie or not cookie["auth-token"].value:
                    self.access_token = await self._oauth_device_login()
                    cookie["auth-token"] = self.access_token
                elif not self.access_token:
                    self._worker.log("Восстановление сессии из cookies.jar")
                    self.access_token = cookie["auth-token"].value

                async with self._worker.request(
                    "GET",
                    "https://id.twitch.tv/oauth2/validate",
                    headers={"Authorization": f"OAuth {self.access_token}"},
                ) as response:
                    if response.status == 401:
                        self._worker.log("Сохранённая сессия устарела, требуется переавторизация")
                        self.access_token = ""
                        if client_info.CLIENT_URL.host is not None:
                            jar.clear_domain(client_info.CLIENT_URL.host)
                        self._worker.cookies_path.unlink(missing_ok=True)
                        continue
                    if response.status == 200:
                        validate_response = await response.json()
                        break
            else:
                raise LoginException("Login verification failure (token validation)")

            assert validate_response is not None
            if validate_response.get("client_id") == client_info.CLIENT_ID:
                break
            self._worker.log("Несовпадение Client-ID cookie, очистка сессии")
            self.access_token = ""
            jar.clear()
            self._worker.cookies_path.unlink(missing_ok=True)
        else:
            raise LoginException("Login verification failure (client_id mismatch)")

        self.user_id = int(validate_response["user_id"])
        self.username = str(validate_response.get("login") or self.username or self.user_id)
        self.auth_status = f"Авторизован ({self.username})"
        self._worker.config.user_id = self.user_id
        self._worker.config.username = self.username
        self._worker.config.save(self._worker.profiles_dir)

        cookie = jar.filter_cookies(client_info.CLIENT_URL)
        cookie["auth-token"] = self.access_token
        cookie["persistent"] = str(self.user_id)
        jar.update_cookies(cookie, client_info.CLIENT_URL)
        self._worker.cookies_path.parent.mkdir(parents=True, exist_ok=True)
        jar.save(self._worker.cookies_path)

        self._logged_in.set()
        self._worker.log(f"Вход выполнен: {self.username} (ID: {self.user_id})")
        self._worker.notify_ui_update()


class AccountWorker:
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

        self.cookies_path: Path = AccountProfileConfig.cookies_path(self.name, self.profiles_dir)
        self.log_path: Path = AccountProfileConfig.log_path(self.name, self.profiles_dir)

        self._client_type: ClientInfo = ClientType.SMARTBOX
        self.user_agent: str = config.user_agent or ClientType.SMARTBOX.pick_user_agent()
        self._proxy_in_connector: bool = False
        self._session: aiohttp.ClientSession | None = None
        self._gql_limiter = RateLimiter(capacity=5, window=1)
        self._auth_state = _WorkerAuthState(self)

        self._state: State = State.IDLE
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
        self.status_text: str = "Остановлен"
        self.last_error: str = ""
        self.log_buffer: deque[str] = deque(maxlen=200)
        self.ws_statuses: dict[int, tuple[str, int]] = {}

        self.websocket = WebsocketPool(self)
        self._main_task: asyncio.Task[None] | None = None
        self._watching_task: asyncio.Task[None] | None = None
        self._mnt_task: asyncio.Task[None] | None = None
        self._file_handler: logging.FileHandler | None = None

    @property
    def user_id(self) -> int | None:
        return self._auth_state.user_id

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
        if self._file_handler is not None:
            record = logging.LogRecord(
                name=f"TwitchMultiMiner.{self.name}",
                level=level,
                pathname="",
                lineno=0,
                msg=message,
                args=(),
                exc_info=None,
            )
            self._file_handler.emit(record)
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
        self.status_text = f"Ошибка: {exc}"
        self.log(f"Критическая ошибка задачи: {exc}", level=logging.ERROR)
        self.stop_nowait()

    def record_claimed_drop(self, drop: Any) -> None:
        self.claimed_count += 1
        rec = ClaimedDropRecord(
            timestamp_str=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            account_name=self.name,
            game_name=drop.campaign.game.name,
            drop_name=drop.rewards_text(),
            campaign_name=drop.campaign.name,
            image_url=drop.primary_image_url,
        )
        self.log(f"Дроп получен: {rec.game_name} — {rec.drop_name}")
        if self._on_claim_cb is not None:
            self._on_claim_cb(rec)
        self.notify_ui_update()

    def _setup_file_logger(self) -> None:
        if self._file_handler is None:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.FileHandler(self.log_path, encoding="utf8", errors="replace")
            handler.setFormatter(FILE_FORMATTER)
            self._file_handler = handler

    def _close_file_logger(self) -> None:
        if self._file_handler is not None:
            with suppress(Exception):
                self._file_handler.close()
            self._file_handler = None

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
        self.status_text = "Запуск..."
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
        self.status_text = "Остановлен"
        self.notify_ui_update()

    def trigger_inventory_refresh(self) -> None:
        if self.running and self._state is not State.EXIT:
            self.change_state(State.INVENTORY_FETCH)

    def invalidate_session(self) -> None:
        self._auth_state.invalidate(delete_cookies=True)
        if self.running:
            self.change_state(State.RESTART)

    async def get_session(self) -> aiohttp.ClientSession:
        if self._session is not None and not self._session.closed:
            return self._session
        cookie_jar = aiohttp.CookieJar()
        try:
            if self.cookies_path.exists():
                cookie_jar.load(self.cookies_path)
        except Exception:
            cookie_jar.clear()

        cq = max(1, min(6, self.global_settings.connection_quality))
        timeout = aiohttp.ClientTimeout(sock_connect=5 * cq, total=10 * cq)
        proxy = self.proxy_url or None
        self._proxy_in_connector = is_socks_proxy(proxy)
        connector = make_connector(proxy, limit=30)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            cookie_jar=cookie_jar,
            headers={"User-Agent": self.user_agent},
        )
        return self._session

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
            for cookie_key, cookie in list(jar._cookies.items()):
                if not cookie:
                    del jar._cookies[cookie_key]
            with suppress(Exception):
                self.cookies_path.parent.mkdir(parents=True, exist_ok=True)
                jar.save(self.cookies_path)
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

    async def get_auth(self) -> _WorkerAuthState:
        await self._auth_state.validate()
        return self._auth_state

    @asynccontextmanager
    async def request(
        self, method: str, url: URL | str, *, invalidate_after: datetime | None = None, **kwargs: Any
    ) -> abc.AsyncIterator[aiohttp.ClientResponse]:
        session = await self.get_session()
        method = method.upper()
        if self.proxy_url and not self._proxy_in_connector and "proxy" not in kwargs:
            kwargs["proxy"] = self.proxy_url
        session_timeout = timedelta(seconds=session.timeout.total or 0)
        backoff = ExponentialBackoff(maximum=90)
        for delay in backoff:
            if self._stop_event.is_set():
                raise ExitRequest()
            if invalidate_after is not None and datetime.now(timezone.utc) >= (
                invalidate_after - session_timeout
            ):
                raise RequestInvalid()
            response: aiohttp.ClientResponse | None = None
            try:
                response = await session.request(method, url, **kwargs)
                if response.status < 500:
                    await response.read()
                    yield response
                    return
                self.log(f"Сервер недоступен ({response.status}), повтор через {round(delay)}с", level=logging.WARNING)
            except aiohttp.ClientConnectorCertificateError:
                raise
            except (
                aiohttp.ClientConnectionError,
                asyncio.TimeoutError,
                aiohttp.ClientPayloadError,
            ) as exc:
                if backoff.steps > 1:
                    self.log(f"Сетевая ошибка ({exc}), повтор через {round(delay)}с", level=logging.WARNING)
            finally:
                if response is not None:
                    response.release()
            await self.sleep_or_stop(delay)
        raise RequestException("Request retry loop exhausted")

    @overload
    async def gql_request(self, ops: GQLOperation) -> JsonType: ...

    @overload
    async def gql_request(self, ops: list[GQLOperation]) -> list[JsonType]: ...

    async def gql_request(
        self, ops: GQLOperation | list[GQLOperation]
    ) -> JsonType | list[JsonType]:
        backoff = ExponentialBackoff(maximum=60)
        single_retry = True
        for delay in backoff:
            if self._stop_event.is_set():
                raise ExitRequest()
            async with self._gql_limiter:
                auth_state = await self.get_auth()
                async with self.request(
                    "POST",
                    "https://gql.twitch.tv/gql",
                    json=ops,
                    headers=auth_state.headers(user_agent=self.user_agent, gql=True),
                ) as response:
                    response_json: JsonType | list[JsonType] = await response.json()
            orig_response = response_json
            response_list = response_json if isinstance(response_json, list) else [response_json]
            force_retry = False
            for item in response_list:
                if "errors" in item:
                    for error_dict in item["errors"]:
                        msg = error_dict.get("message", "")
                        if single_retry and msg in ("service error", "PersistedQueryNotFound"):
                            single_retry = False
                            delay = max(delay, 5.0)
                            force_retry = True
                            break
                        if msg == "server error":
                            data_dict: JsonType = item["data"]
                            path: list[str] = error_dict.get("path", [])
                            for key in path[:-1]:
                                data_dict = data_dict[key]
                            if path:
                                data_dict[path[-1]] = None
                            break
                        if msg in (
                            "service timeout",
                            "request cancelled",
                            "service unavailable",
                            "context deadline exceeded",
                        ):
                            force_retry = True
                            break
                    else:
                        raise GQLException(item["errors"])
                elif "error" in item:
                    raise GQLException(f"{item['error']}: {item.get('message')}")
                if force_retry:
                    break
            else:
                return orig_response
            await self.sleep_or_stop(delay)
        raise GQLException("GQL retry loop exhausted")

    def _merge_data(self, primary_data: JsonType, secondary_data: JsonType) -> JsonType:
        merged: JsonType = {}
        for key in set(primary_data.keys()) | set(secondary_data.keys()):
            if key in primary_data and key in secondary_data:
                vp = primary_data[key]
                vs = secondary_data[key]
                if isinstance(vp, dict) and isinstance(vs, dict):
                    merged[key] = self._merge_data(vp, vs)
                else:
                    merged[key] = vp
            elif key in primary_data:
                merged[key] = primary_data[key]
            else:
                merged[key] = secondary_data[key]
        return merged

    async def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._run()
                break
            except ReloadRequest:
                self.log("Перезапуск воркера...")
                await self.shutdown()
                self._setup_file_logger()
            except (ExitRequest, asyncio.CancelledError):
                break
            except LoginException as exc:
                self.last_error = str(exc)
                self.status_text = f"Ошибка входа: {exc}"
                self.log(f"Ошибка входа: {exc}", level=logging.ERROR)
                break
            except Exception as exc:
                self.last_error = str(exc)
                self.status_text = f"Ошибка: {exc}"
                self.log(f"Сбой воркера: {exc}", level=logging.ERROR)
                logger.exception(f"[{self.name}] Worker crashed")
                break
        await self.shutdown()
        if self._stop_event.is_set() and not self.last_error:
            self.status_text = "Остановлен"
        self.notify_ui_update()

    async def _run(self) -> None:
        self.status_text = "Авторизация..."
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
                self.status_text = "Ожидание кампаний (IDLE)"
                self.notify_ui_update()
                self._state_change.clear()
            elif self._state is State.INVENTORY_FETCH:
                self.status_text = "Синхронизация инвентаря..."
                self.notify_ui_update()
                await self.websocket.start()
                await self.fetch_inventory()
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
                self.status_text = "Очистка каналов..."
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
                if self.wanted_games or self.global_settings.standby_on_pinned:
                    self.change_state(State.CHANNELS_FETCH)
                else:
                    self.log("Нет активных кампаний для фарма.")
                    self.change_state(State.IDLE)
            elif self._state is State.CHANNELS_FETCH:
                self.status_text = "Поиск стримов..."
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

    def effective_priority_games(self) -> list[str]:
        fav_ids = set(self.global_settings.favorite_campaigns)
        fav_games = [c.game.name for c in self.inventory if c.id in fav_ids]
        preset = getattr(self.global_settings, "radar_preset", "default")
        base_priority = list(self.global_settings.priority_games)
        if preset == "pc_games" and not base_priority:
            known = [c.game.name for c in self.inventory if not c.finished]
            base_priority = self.radar.get_installed_pc_games(known_games=known)
        extra_games: list[str] = []
        if preset == "all_campaigns":
            extra_games = [c.game.name for c in self.inventory if not c.finished]
        return deduplicate(
            [*self.radar.auto_promoted_games, *fav_games, *base_priority, *extra_games]
        )

    def _compute_wanted_games(self) -> None:
        self.wanted_games.clear()
        exclude = set(self.global_settings.exclude_games)
        fav_ids = set(self.global_settings.favorite_campaigns)
        priority = self.effective_priority_games()
        priority_mode = self.global_settings.priority_mode_enum
        priority_only = priority_mode is PriorityMode.PRIORITY_ONLY
        next_hour = datetime.now(timezone.utc) + timedelta(hours=1)

        sorted_campaigns = list(self.inventory)
        if not priority_only:
            if priority_mode is PriorityMode.ENDING_SOONEST:
                sorted_campaigns.sort(key=lambda c: c.ends_at)
            elif priority_mode is PriorityMode.LOW_AVBL_FIRST:
                sorted_campaigns.sort(key=lambda c: c.availability)
        sorted_campaigns.sort(
            key=lambda c: priority.index(c.game.name) if c.game.name in priority else MAX_INT
        )
        sorted_campaigns.sort(key=lambda c: 0 if c.id in fav_ids else 1)
        for campaign in sorted_campaigns:
            game = campaign.game
            if (
                game not in self.wanted_games
                and game.name not in exclude
                and (not priority_only or game.name in priority)
                and campaign.can_earn_within(next_hour)
            ):
                self.wanted_games.append(game)

    async def _fetch_channels(self) -> None:
        channels = self.channels
        new_channels: set[Channel] = set(channels.values())
        channels.clear()

        no_acl: set[Game] = set()
        acl_channels: set[Channel] = set()
        next_hour = datetime.now(timezone.utc) + timedelta(hours=1)

        for campaign in self.inventory:
            if campaign.game in self.wanted_games and campaign.can_earn_within(next_hour):
                if campaign.allowed_channels:
                    acl_channels.update(campaign.allowed_channels)
                else:
                    no_acl.add(campaign.game)

        # Inject pinned streamers cached in SharedCampaignRadar
        for st in self.radar.pinned_streams.values():
            if st.online and st.channel_id and st.raw_channel_data:
                ch = Channel(
                    self,
                    id=st.channel_id,
                    login=st.login,
                    display_name=st.display_name or st.login,
                    pinned=True,
                )
                avail_payload = [{"id": cid} for cid in st.active_campaign_ids]
                ch.external_update(st.raw_channel_data, avail_payload)
                new_channels.add(ch)

        acl_channels.difference_update(new_channels)
        await self.bulk_check_online(acl_channels)
        new_channels.update(acl_channels)

        for game in no_acl:
            for stream_ch in await self.get_live_streams(game, drops_enabled=True):
                new_channels.add(stream_ch)

        ordered = sorted(new_channels, key=lambda c: c.viewers if c.viewers is not None else -1, reverse=True)
        ordered.sort(key=lambda c: c.acl_based, reverse=True)
        ordered.sort(key=lambda c: c.pinned, reverse=True)
        ordered.sort(key=self.get_priority)

        to_trim = ordered[MAX_CHANNELS:]
        ordered = ordered[:MAX_CHANNELS]
        if to_trim:
            rm_topics = []
            for ch in to_trim:
                rm_topics.append(WebsocketTopic.as_str("Channel", "StreamState", ch.id))
                rm_topics.append(WebsocketTopic.as_str("Channel", "StreamUpdate", ch.id))
            self.websocket.remove_topics(rm_topics)

        for ch in ordered:
            channels[ch.id] = ch

        add_topics: list[WebsocketTopic] = []
        for cid in channels:
            add_topics.append(WebsocketTopic("Channel", "StreamState", cid, self.process_stream_state))
            add_topics.append(WebsocketTopic("Channel", "StreamUpdate", cid, self.process_stream_update))
        self.websocket.add_topics(add_topics)

        watching = self.watching_channel.get_with_default(None)
        if watching is not None:
            relinked = channels.get(watching.id)
            if relinked is not None and (
                self.can_watch(relinked) or (self.is_standby and relinked.online and relinked.pinned)
            ):
                self.watch(relinked, standby=self.is_standby, update_status=False)
            else:
                self.stop_watching()

    def _switch_channel(self) -> None:
        new_watching: Channel | None = None
        new_standby = False
        candidates = sorted(
            self.channels.values(),
            key=lambda c: (
                self.get_priority(c),
                0 if c.pinned else 1,
                0 if c.acl_based else 1,
                -(c.viewers or 0),
            ),
        )
        for ch in candidates:
            if self.should_switch(ch):
                new_watching = ch
                new_standby = False
                break

        current = self.watching_channel.get_with_default(None)
        if new_watching is not None:
            self.watch(new_watching, standby=new_standby)
            return
        if current is not None and self.can_watch(current):
            self.is_standby = False
            self.status_text = f"Смотрит: {current.name}"
            self.notify_ui_update()
            return

        # Standby Mode: keep watch warm on highest-viewer live pinned streamer (e.g. kaicenat)
        if self.global_settings.standby_on_pinned:
            pinned_online = [
                ch for ch in self.channels.values() if ch.pinned and ch.online
            ]
            pinned_online.sort(key=lambda c: c.viewers or 0, reverse=True)
            if pinned_online:
                standby_ch = pinned_online[0]
                if current is None or current.id != standby_ch.id or not self.is_standby:
                    self.watch(standby_ch, standby=True)
                else:
                    self.status_text = f"Дежурство: {standby_ch.name}"
                    self.notify_ui_update()
                return

        self.log("Нет подходящих онлайн-стримов для просмотра.")
        self.change_state(State.IDLE)

    def get_priority(self, channel: Channel) -> int:
        priority = self.effective_priority_games()
        game = channel.game
        # If channel is verified for a priority campaign (even in Just Chatting), reflect that campaign's priority
        for cid in channel.available_campaign_ids:
            camp = self._campaigns.get(cid)
            if camp is not None and camp.can_earn(channel, ignore_channel_status=True):
                if camp.game.name in priority:
                    return priority.index(camp.game.name)
                if camp.game in self.wanted_games:
                    return len(priority) + self.wanted_games.index(camp.game)
        if game is not None:
            if game.name in priority:
                return priority.index(game.name)
            if game in self.wanted_games:
                return len(priority) + self.wanted_games.index(game)
        return MAX_INT

    def can_watch(self, channel: Channel) -> bool:
        if not channel.online:
            return False
        for campaign in self.inventory:
            if campaign.can_earn(channel) and (
                (channel.game is not None and channel.drops_enabled and channel.game in self.wanted_games)
                or campaign.game.is_special()
                or (channel.acl_based and channel in campaign.allowed_channels)
                or (channel.id in campaign.verified_channel_ids)
            ):
                return True
        return False

    def should_switch(self, channel: Channel) -> bool:
        if not self.can_watch(channel):
            return False
        watching = self.watching_channel.get_with_default(None)
        if watching is None or self.is_standby or not self.can_watch(watching):
            return True
        ch_order = self.get_priority(channel)
        w_order = self.get_priority(watching)
        if ch_order < w_order:
            return True
        if ch_order == w_order:
            if channel.pinned > watching.pinned:
                return True
            if channel.pinned == watching.pinned and channel.acl_based > watching.acl_based:
                return True
        return False

    def watch(self, channel: Channel, *, standby: bool = False, update_status: bool = True) -> None:
        self.is_standby = standby
        self.watching_channel.set(channel)
        if update_status:
            if standby:
                self.status_text = f"Дежурство: {channel.name}"
                self.log(f"Режим дежурства (Standby) на {channel.name} [{channel.game or 'Online'}]")
            else:
                self.status_text = f"Фарм: {channel.name}"
                self.log(f"Начал просмотр {channel.name} [{channel.game or '?'}]")
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
        import random

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
                await self._sync_active_drop_progress(channel)

            interval = random.uniform(WATCH_INTERVAL_MIN, WATCH_INTERVAL_MAX)
            await self._watch_sleep(max(1.0, interval - 10.0))

    async def _sync_active_drop_progress(self, channel: Channel) -> None:
        drop_data: JsonType | None = None
        try:
            context = await self.gql_request(
                GQL_QUERIES["CurrentDrop"].with_variables({"channelID": str(channel.id)})
            )
            drop_data = (context.get("data") or {}).get("currentUser", {}).get("dropCurrentSession")
        except GQLException:
            drop_data = None

        if isinstance(drop_data, dict):
            drop = self._drops.get(drop_data.get("dropID", ""))
            if drop is not None and drop.can_earn(channel):
                mins = int(drop_data.get("currentMinutesWatched") or 0)
                drop.update_minutes(mins)
                if drop.ready_to_claim:
                    await drop.ensure_claim()
                    self.change_state(State.INVENTORY_FETCH)
                return

        if (active_campaign := self.get_active_campaign(channel)) is not None:
            needs_resync = active_campaign.bump_minutes(channel)
            if needs_resync:
                for d in active_campaign.drops:
                    d.reset_unconfirmed()
                # Refresh inventory cleanly without dropping out of the stream
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

        if not lightweight:
            await self.radar.poll_pinned_streamers(self)
        public_campaigns = await self.radar.get_merged_public_campaigns(proxy_url=self.proxy_url)
        inventory_data = self._merge_data(inventory_data, public_campaigns)

        for cid in list(inventory_data.keys()):
            if not inventory_data[cid].get("game"):
                del inventory_data[cid]

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

    def get_active_campaign(self, channel: Channel | None = None) -> DropsCampaign | None:
        if not self.wanted_games:
            return None
        watching = self.watching_channel.get_with_default(channel)
        if watching is None:
            return None
        campaigns = [c for c in self.inventory if c.can_earn(watching)]
        if not campaigns:
            return None
        campaigns.sort(key=lambda c: c.remaining_minutes)
        return campaigns[0]

    async def get_live_streams(
        self, game: Game, *, limit: int = 20, drops_enabled: bool = True
    ) -> list[Channel]:
        filters: list[str] = ["DROPS_ENABLED"] if drops_enabled else []
        try:
            response = await self.gql_request(
                GQL_QUERIES["GameDirectory"].with_variables(
                    {
                        "limit": limit,
                        "slug": game.slug,
                        "options": {
                            "includeRestricted": ["SUB_ONLY_LIVE"],
                            "systemFilters": filters,
                        },
                    }
                )
            )
        except GQLException:
            return []
        game_data = (response.get("data") or {}).get("game")
        if not game_data:
            return []
        return [
            Channel.from_directory(self, edge["node"], drops_enabled=drops_enabled)
            for edge in (game_data.get("streams", {}).get("edges") or [])
            if edge.get("node", {}).get("broadcaster") is not None
        ]

    async def bulk_check_online(self, channels: abc.Iterable[Channel]) -> None:
        channels_list = list(channels)
        if not channels_list:
            return
        acl_streams_map: dict[int, JsonType] = {}
        ops = [ch.stream_gql for ch in channels_list]
        for op_chunk in chunk(ops, 20):
            responses = await self.gql_request(op_chunk)
            for resp in responses:
                user_data = (resp.get("data") or {}).get("user")
                if user_data is not None:
                    acl_streams_map[int(user_data["id"])] = user_data

        acl_avail_map: dict[int, list[JsonType]] = {}
        if self.global_settings.available_drops_check:
            avail_ops = [
                GQL_QUERIES["AvailableDrops"].with_variables({"channelID": str(cid)})
                for cid, cdata in acl_streams_map.items()
                if cdata.get("stream") is not None
            ]
            for op_chunk in chunk(avail_ops, 20):
                responses = await self.gql_request(op_chunk)
                for resp in responses:
                    ch_info = (resp.get("data") or {}).get("channel")
                    if ch_info:
                        acl_avail_map[int(ch_info["id"])] = ch_info.get("viewerDropCampaigns") or []

        for ch in channels_list:
            cdata = acl_streams_map.get(ch.id)
            if not cdata or not cdata.get("stream"):
                continue
            avail = acl_avail_map.get(ch.id, [])
            ch.external_update(cdata, avail)
            if avail:
                await self.radar.ingest_channel_campaigns(
                    self, avail, channel_id=ch.id, channel_login=ch.login
                )

    def on_channel_update(
        self, channel: Channel, stream_before: Stream | None, stream_after: Stream | None
    ) -> None:
        if stream_before is None and stream_after is not None:
            if self.should_switch(channel):
                self.log(f"{channel.name} вышел в онлайн — переключаюсь.")
                self.watch(channel, standby=False)
        elif stream_before is not None:
            watching = self.watching_channel.get_with_default(None)
            if watching is not None and watching == channel:
                if stream_after is None:
                    self.log(f"{channel.name} завершил стрим.")
                    self.change_state(State.CHANNEL_SWITCH)
                elif not self.is_standby and not self.can_watch(channel):
                    self.change_state(State.CHANNEL_SWITCH)
            elif stream_after is not None and self.should_switch(channel):
                self.watch(channel, standby=False)
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
