"""Token protection (Windows DPAPI / keyring), egress-IP GQL rate limiting, auth, and HTTP transport."""
from __future__ import annotations

import asyncio
import base64
import getpass
import hashlib
import logging
import os
import sys
import uuid
from collections import abc
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, overload, cast

import aiohttp
from yarl import URL

from constants import GQLOperation, JsonType
from exceptions import (
    ExitRequest,
    GQLException,
    LoginException,
    RequestException,
    RequestInvalid,
)
from i18n import tr
from utils import ExponentialBackoff, RateLimiter, is_socks_proxy, make_connector

if sys.platform == "win32":
    try:
        import win32crypt
    except ImportError:  # pragma: no cover
        win32crypt = None  # type: ignore[assignment]
else:
    win32crypt = None  # type: ignore[assignment]

try:
    import keyring  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover
    keyring = None


logger = logging.getLogger("TwitchMultiMiner.auth")

_DPAPI_PREFIX = "dpapi:"
_KEYRING_PREFIX = "keyring:"
_OBF_PREFIX = "obf:"
_PROTECTED_PREFIXES = (_DPAPI_PREFIX, _KEYRING_PREFIX, _OBF_PREFIX)
_KEYRING_SERVICE = "TwitchMultiMiner"


def _machine_xor_key(length: int) -> bytes:
    seed = f"TwitchMultiMiner:{uuid.getnode()}:{getpass.getuser()}".encode("utf-8")
    digest = hashlib.pbkdf2_hmac("sha256", seed, b"tmm_cookie_salt_v1", 10000, dklen=max(32, length))
    return digest[:length]


def protect_secret(plaintext: str) -> str:
    """Encrypt an auth token using Windows DPAPI, keyring, or a machine-bound obfuscator."""
    clean = str(plaintext or "").strip()
    if not clean:
        return ""
    if clean.startswith(_PROTECTED_PREFIXES):
        return clean

    if win32crypt is not None:
        with suppress(Exception):
            blob = win32crypt.CryptProtectData(
                clean.encode("utf-8"),
                _KEYRING_SERVICE,
                None,
                None,
                None,
                0,
            )
            return _DPAPI_PREFIX + base64.b64encode(blob).decode("ascii")

    if keyring is not None:
        key_name = "tok_" + hashlib.sha256(clean.encode("utf-8")).hexdigest()[:16]
        with suppress(Exception):
            keyring.set_password(_KEYRING_SERVICE, key_name, clean)
            return _KEYRING_PREFIX + key_name

    raw_bytes = clean.encode("utf-8")
    nonce = os.urandom(8)
    mask = _machine_xor_key(len(raw_bytes))
    masked = bytes(b ^ m for b, m in zip(raw_bytes, mask))
    encoded = base64.b64encode(nonce + masked).decode("ascii")
    return _OBF_PREFIX + encoded


def unprotect_secret(stored: str) -> str:
    """Decrypt a stored auth token, transparently accepting legacy plaintext tokens."""
    clean = str(stored or "").strip()
    if not clean:
        return ""

    if clean.startswith(_DPAPI_PREFIX):
        if win32crypt is None:
            return ""
        with suppress(Exception):
            blob = base64.b64decode(clean[len(_DPAPI_PREFIX) :].encode("ascii"))
            _, raw = win32crypt.CryptUnprotectData(blob, None, None, None, 0)
            return bytes(raw).decode("utf-8")
        return ""

    if clean.startswith(_KEYRING_PREFIX):
        if keyring is None:
            return ""
        key_name = clean[len(_KEYRING_PREFIX) :]
        with suppress(Exception):
            return str(keyring.get_password(_KEYRING_SERVICE, key_name) or "")
        return ""

    if clean.startswith(_OBF_PREFIX):
        with suppress(Exception):
            payload = base64.b64decode(clean[len(_OBF_PREFIX) :].encode("ascii"))
            if len(payload) <= 8:
                return ""
            masked = payload[8:]
            mask = _machine_xor_key(len(masked))
            return bytes(b ^ m for b, m in zip(masked, mask)).decode("utf-8")
        return ""

    return clean


def save_cookie_jar_protected(
    jar: aiohttp.CookieJar,
    cookies_path: Path,
    client_url: URL,
    raw_token: str = "",
) -> None:
    """Persist cookies.jar using public CookieJar APIs with auth-token encrypted on disk."""
    token_to_protect = str(raw_token or "").strip()
    if not token_to_protect:
        morsel = jar.filter_cookies(client_url).get("auth-token")
        if morsel is not None and morsel.value:
            token_to_protect = unprotect_secret(morsel.value)

    if token_to_protect:
        jar.update_cookies({"auth-token": protect_secret(token_to_protect)}, client_url)

    cookies_path.parent.mkdir(parents=True, exist_ok=True)
    jar.save(cookies_path)


def load_auth_token_from_jar(
    jar: aiohttp.CookieJar,
    cookies_path: Path,
    client_url: URL,
) -> str:
    """Load and decrypt auth-token via public CookieJar APIs, upgrading legacy plaintext files."""
    morsel = jar.filter_cookies(client_url).get("auth-token")
    if (morsel is None or not morsel.value) and cookies_path.is_file():
        with suppress(Exception):
            jar.load(cookies_path)
            morsel = jar.filter_cookies(client_url).get("auth-token")

    if morsel is None or not morsel.value:
        return ""

    stored_val = str(morsel.value).strip()
    raw_token = unprotect_secret(stored_val)
    if raw_token and not stored_val.startswith(_PROTECTED_PREFIXES) and cookies_path.exists():
        with suppress(Exception):
            save_cookie_jar_protected(jar, cookies_path, client_url, raw_token)
    return raw_token


def normalize_egress_key(proxy_url: str) -> str:
    """Compute a normalized shared rate-limit key for an egress proxy URL or direct IP."""
    clean = str(proxy_url or "").strip()
    if not clean:
        return "direct"
    with suppress(Exception):
        parsed = URL(clean)
        if parsed.scheme and parsed.host:
            port = f":{parsed.port}" if parsed.port else ""
            return f"{parsed.scheme.lower()}://{parsed.host.lower()}{port}"
    return clean.lower()


class EgressLimiterRegistry:
    """Shares GQL RateLimiter instances across accounts that share the same egress IP/proxy."""

    def __init__(self, *, capacity: int = 5, window: int = 1) -> None:
        self.capacity = capacity
        self.window = window
        self._limiters: dict[str, RateLimiter] = {}

    def get(self, proxy_url: str = "") -> RateLimiter:
        key = normalize_egress_key(proxy_url)
        limiter = self._limiters.get(key)
        if limiter is None:
            limiter = RateLimiter(capacity=self.capacity, window=self.window)
            self._limiters[key] = limiter
        return limiter


def _nullify_gql_error_path(item: JsonType, path: list[Any]) -> None:
    """Safely nullify a GraphQL response field identified by error path without crashing on None."""
    if not isinstance(item, dict) or not path:
        return
    cur: Any = item.get("data")
    for key in path[:-1]:
        if isinstance(cur, dict) and isinstance(key, str) and key in cur:
            cur = cur[key]
        elif isinstance(cur, list) and isinstance(key, int) and 0 <= key < len(cur):
            cur = cur[key]
        else:
            return
    last = path[-1]
    if isinstance(cur, dict) and isinstance(last, str):
        cur[last] = None
    elif isinstance(cur, list) and isinstance(last, int) and 0 <= last < len(cur):
        cur[last] = None


class WorkerAuthState:
    """Manages OAuth Device Code authentication, token validation, and protected cookie persistence."""

    def __init__(self, worker: Any) -> None:
        self._worker: Any = worker
        self._lock = asyncio.Lock()
        self._logged_in = asyncio.Event()
        self.user_id: int | None = worker.config.user_id
        self.username: str = worker.config.username
        self.device_id: str = worker.config.device_id
        self.session_id: str = worker.config.session_id
        self.access_token: str = ""
        self.activation_code: str = ""
        self.verification_uri: str = ""
        self.auth_status: str = worker._t("auth_unauthorized")

    def invalidate(self, *, delete_cookies: bool = False) -> None:
        self.access_token = ""
        self.user_id = None
        self.activation_code = ""
        self.verification_uri = ""
        self.auth_status = self._worker._t("auth_login_required")
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
        client_info = self._worker._client_type
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
        client_info = self._worker._client_type
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
                self.auth_status = self._worker._t("auth_code", code=user_code)
                self._worker.log(
                    self._worker._t("auth_activation_prompt", uri=verification_uri, code=user_code)
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
                            err_json: JsonType = {}
                            with suppress(Exception):
                                err_json = await token_resp.json()
                            err_code = str(
                                err_json.get("error") or err_json.get("message") or ""
                            ).strip().lower()
                            if err_code == "slow_down" or token_resp.status == 429:
                                interval = min(60, interval + 5)
                            elif err_code in ("expired_token", "invalid_grant", "access_denied"):
                                raise RequestInvalid()
                            continue
                        token_json = await token_resp.json()
                        self.access_token = str(token_json["access_token"])
                        self.activation_code = ""
                        self.verification_uri = ""
                        self._worker.notify_ui_update()
                        return self.access_token
            except RequestInvalid:
                self._worker.log(self._worker._t("auth_code_expired"))
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
        client_info = self._worker._client_type
        self.auth_status = self._worker._t("auth_checking")
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
                loaded_token = load_auth_token_from_jar(
                    jar, self._worker.cookies_path, client_info.CLIENT_URL
                )
                if not loaded_token and not self.access_token:
                    self.access_token = await self._oauth_device_login()
                    save_cookie_jar_protected(
                        jar, self._worker.cookies_path, client_info.CLIENT_URL, self.access_token
                    )
                elif not self.access_token:
                    self._worker.log(self._worker._t("auth_restoring"))
                    self.access_token = loaded_token

                async with self._worker.request(
                    "GET",
                    "https://id.twitch.tv/oauth2/validate",
                    headers={"Authorization": f"OAuth {self.access_token}"},
                ) as response:
                    if response.status == 401:
                        self._worker.log(self._worker._t("auth_expired_reauth"))
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
            self._worker.log(self._worker._t("auth_client_mismatch"))
            self.access_token = ""
            jar.clear()
            self._worker.cookies_path.unlink(missing_ok=True)
        else:
            raise LoginException("Login verification failure (client_id mismatch)")

        self.user_id = int(validate_response["user_id"])
        self.username = str(validate_response.get("login") or self.username or self.user_id)
        self.auth_status = self._worker._t("auth_authorized", username=self.username)
        self._worker.config.user_id = self.user_id
        self._worker.config.username = self.username
        self._worker.config.save(self._worker.profiles_dir)

        jar.update_cookies({"persistent": str(self.user_id)}, client_info.CLIENT_URL)
        save_cookie_jar_protected(
            jar, self._worker.cookies_path, client_info.CLIENT_URL, self.access_token
        )

        self._logged_in.set()
        self._worker.log(
            self._worker._t("auth_logged_in", username=self.username, user_id=self.user_id)
        )
        self._worker.notify_ui_update()


class WorkerTransportMixin:
    """Provides HTTP session management, egress-IP rate limiting, and Twitch GQL calls."""

    def get_gql_limiter(self) -> RateLimiter:
        radar = getattr(self, "radar", None)
        registry = getattr(radar, "egress_limiters", None)
        if registry is not None:
            return registry.get(getattr(self, "proxy_url", ""))
        return self._gql_limiter  # type: ignore[attr-defined]

    async def get_session(self) -> aiohttp.ClientSession:
        if self._session is not None and not self._session.closed:  # type: ignore[attr-defined]
            return self._session  # type: ignore[attr-defined]
        cookie_jar = aiohttp.CookieJar()
        try:
            if self.cookies_path.exists():  # type: ignore[attr-defined]
                cookie_jar.load(self.cookies_path)  # type: ignore[attr-defined]
        except Exception:
            cookie_jar.clear()

        cq = max(1, min(6, self.global_settings.connection_quality))  # type: ignore[attr-defined]
        timeout = aiohttp.ClientTimeout(sock_connect=5 * cq, total=10 * cq)
        proxy = self.proxy_url or None  # type: ignore[attr-defined]
        self._proxy_in_connector = is_socks_proxy(proxy)
        connector = make_connector(proxy, limit=30)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            cookie_jar=cookie_jar,
            headers={"User-Agent": self.user_agent},  # type: ignore[attr-defined]
        )
        return self._session

    @asynccontextmanager
    async def request(
        self, method: str, url: URL | str, *, invalidate_after: datetime | None = None, **kwargs: Any
    ) -> abc.AsyncIterator[aiohttp.ClientResponse]:
        session = await self.get_session()
        method = method.upper()
        proxy_url = getattr(self, "proxy_url", "")
        if proxy_url and not self._proxy_in_connector and "proxy" not in kwargs:  # type: ignore[attr-defined]
            kwargs["proxy"] = proxy_url
        session_timeout = timedelta(seconds=session.timeout.total or 0)
        backoff = ExponentialBackoff(maximum=90)
        for delay in backoff:
            if self._stop_event.is_set():  # type: ignore[attr-defined]
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
                self.log(  # type: ignore[attr-defined]
                    tr(
                        "http_unavailable",
                        getattr(self.global_settings, "language", "ru"),  # type: ignore[attr-defined]
                        status=response.status,
                        delay=round(delay),
                    ),
                    level=logging.WARNING,
                )
            except aiohttp.ClientConnectorCertificateError:
                raise
            except (
                aiohttp.ClientConnectionError,
                asyncio.TimeoutError,
                aiohttp.ClientPayloadError,
            ) as exc:
                if backoff.steps > 1:
                    self.log(  # type: ignore[attr-defined]
                        tr(
                            "http_network_err",
                            getattr(self.global_settings, "language", "ru"),  # type: ignore[attr-defined]
                            error=exc,
                            delay=round(delay),
                        ),
                        level=logging.WARNING,
                    )
            finally:
                if response is not None:
                    response.release()
            await self.sleep_or_stop(delay)  # type: ignore[attr-defined]
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
            if self._stop_event.is_set():  # type: ignore[attr-defined]
                raise ExitRequest()
            async with self.get_gql_limiter():
                auth_state = await self.get_auth()  # type: ignore[attr-defined]
                async with self.request(
                    "POST",
                    "https://gql.twitch.tv/gql",
                    json=ops,
                    headers=auth_state.headers(user_agent=self.user_agent, gql=True),  # type: ignore[attr-defined]
                ) as response:
                    response_json: JsonType | list[JsonType] = await response.json()
            orig_response = response_json
            response_list = response_json if isinstance(response_json, list) else [response_json]
            force_retry = False
            for item in response_list:
                if not isinstance(item, dict):
                    continue
                if "errors" in item:
                    for error_dict in item["errors"]:
                        msg = error_dict.get("message", "")
                        if single_retry and msg in ("service error", "PersistedQueryNotFound"):
                            single_retry = False
                            delay = max(delay, 5.0)
                            force_retry = True
                            break
                        if msg == "server error":
                            _nullify_gql_error_path(item, error_dict.get("path") or [])
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
            await self.sleep_or_stop(delay)  # type: ignore[attr-defined]
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
