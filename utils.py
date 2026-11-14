"""Shared async utilities, JSON helpers, network connector, and Game model."""
from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import random
import re
import socket
import string
import struct
import time
import traceback
import webbrowser
from collections import OrderedDict, abc
from contextlib import suppress
from datetime import datetime, timezone
from enum import Enum
from functools import cached_property, wraps
from pathlib import Path
from typing import Any, Callable, Generic, Literal, Mapping, ParamSpec, TypeVar, cast

import aiohttp
from aiohttp.abc import AbstractResolver, ResolveResult
from yarl import URL

from constants import JsonType, PriorityMode
from exceptions import ExitRequest, ReloadRequest

_T = TypeVar("_T")
_D = TypeVar("_D")
_P = ParamSpec("_P")
_JSON_T = TypeVar("_JSON_T", bound=Mapping[Any, Any])
logger = logging.getLogger("TwitchMultiMiner")

_SOCKS_SCHEMES = ("socks4://", "socks5://", "socks5h://")
PUBLIC_DNS_SERVERS: tuple[str, ...] = ("1.1.1.1", "8.8.8.8", "1.0.0.1", "8.8.4.4")
CHARS_ASCII = string.ascii_letters + string.digits
CHARS_HEX_LOWER = string.digits + "abcdef"
CHARS_HEX_UPPER = string.digits + "ABCDEF"


def is_socks_proxy(proxy: URL | str | None) -> bool:
    return bool(proxy) and str(proxy).strip().lower().startswith(_SOCKS_SCHEMES)


def _skip_dns_name(packet: bytes, offset: int) -> int:
    """Advance past an uncompressed or pointer-compressed DNS QNAME."""
    length_limit = len(packet)
    steps = 0
    while offset < length_limit and steps < 128:
        label_len = packet[offset]
        if label_len == 0:
            return offset + 1
        if (label_len & 0xC0) == 0xC0:
            if offset + 2 > length_limit:
                raise ValueError("Truncated DNS compression pointer")
            return offset + 2
        offset += 1 + label_len
        steps += 1
    raise ValueError("Invalid or cyclic DNS name")


def _build_dns_query(host: str, txid: int) -> bytes:
    """Construct a standard UDP DNS A-record query packet."""
    header = struct.pack("!HHHHHH", txid & 0xFFFF, 0x0100, 1, 0, 0, 0)
    qname_parts = []
    for label in host.rstrip(".").split("."):
        encoded = label.encode("idna")
        qname_parts.append(bytes((len(encoded),)) + encoded)
    qname_parts.append(b"\x00")
    return header + b"".join(qname_parts) + struct.pack("!HH", 1, 1)


def _parse_dns_a_response(packet: bytes, expected_txid: int) -> tuple[list[str], int]:
    """Extract IPv4 A records and bounded TTL from a raw DNS UDP response."""
    if len(packet) < 12:
        raise ValueError("Short DNS response")
    txid, flags, qdcount, ancount, _, _ = struct.unpack("!HHHHHH", packet[:12])
    if txid != (expected_txid & 0xFFFF):
        raise ValueError("DNS transaction ID mismatch")
    if (flags & 0x000F) != 0:
        return ([], 0)

    offset = 12
    for _ in range(qdcount):
        offset = _skip_dns_name(packet, offset) + 4
        if offset > len(packet):
            raise ValueError("Truncated DNS question section")

    ips: list[str] = []
    min_ttl = 120
    for _ in range(ancount):
        offset = _skip_dns_name(packet, offset)
        if offset + 10 > len(packet):
            break
        rtype, rclass, ttl, rdlength = struct.unpack("!HHIH", packet[offset : offset + 10])
        offset += 10
        if offset + rdlength > len(packet):
            break
        rdata = packet[offset : offset + rdlength]
        offset += rdlength
        if rtype == 1 and rclass == 1 and rdlength == 4:
            ips.append(socket.inet_ntop(socket.AF_INET, rdata))
            if ttl > 0:
                min_ttl = min(min_ttl, ttl)
    return (ips, max(30, min(min_ttl, 300)))


def _udp_dns_lookup(
    host: str,
    nameservers: tuple[str, ...] = PUBLIC_DNS_SERVERS,
    timeout: float = 2.0,
) -> tuple[list[str], int]:
    """Query public DNS over UDP port 53 directly, bypassing broken OS getaddrinfo."""
    last_exc: Exception | None = None
    for server in nameservers:
        txid = random.randint(0, 0xFFFF)
        packet = _build_dns_query(host, txid)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.settimeout(timeout)
            sock.sendto(packet, (server, 53))
            resp, _ = sock.recvfrom(2048)
            ips, ttl = _parse_dns_a_response(resp, txid)
            if ips:
                return (ips, ttl)
        except Exception as exc:
            last_exc = exc
        finally:
            sock.close()
    raise OSError(f"Public UDP DNS lookup failed for {host}: {last_exc}")


class FallbackDNSResolver(AbstractResolver):
    """Multi-tier DNS resolver: literal fast-path -> aiodns -> UDP 1.1.1.1/8.8.8.8 -> OS fallback."""

    def __init__(self, nameservers: tuple[str, ...] = PUBLIC_DNS_SERVERS) -> None:
        self._nameservers = nameservers
        self._cache: dict[str, tuple[float, list[str]]] = {}
        self._aiodns_resolver: aiohttp.AbstractResolver | None = None
        self._aiodns_unavailable = False
        self._os_resolver: aiohttp.ThreadedResolver | None = None

    def _to_results(self, host: str, ips: list[str], port: int) -> list[ResolveResult]:
        results: list[ResolveResult] = []
        for ip_str in ips:
            addr = ipaddress.ip_address(ip_str)
            family = socket.AF_INET6 if addr.version == 6 else socket.AF_INET
            results.append(
                {
                    "hostname": host,
                    "host": ip_str,
                    "port": port,
                    "family": int(family),
                    "proto": socket.IPPROTO_TCP,
                    "flags": socket.AI_NUMERICHOST | socket.AI_NUMERICSERV,
                }
            )
        return results

    def _ensure_aiodns(self) -> aiohttp.AbstractResolver | None:
        if self._aiodns_unavailable:
            return None
        if self._aiodns_resolver is None:
            try:
                import aiodns  # noqa: F401

                self._aiodns_resolver = aiohttp.AsyncResolver(nameservers=list(self._nameservers))
            except Exception:
                self._aiodns_unavailable = True
                return None
        return self._aiodns_resolver

    async def resolve(
        self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_INET
    ) -> list[ResolveResult]:
        clean = host.strip()
        try:
            ipaddress.ip_address(clean)
            return self._to_results(host, [clean], port)
        except ValueError:
            pass

        if clean.lower() == "localhost" or clean.lower().endswith(".localhost"):
            return self._to_results(host, ["127.0.0.1"], port)

        now = time.monotonic()
        cached = self._cache.get(clean)
        if cached is not None and cached[0] > now:
            return self._to_results(host, cached[1], port)

        aiodns_res = self._ensure_aiodns()
        if aiodns_res is not None:
            try:
                results = await aiodns_res.resolve(clean, port, family)
                if results:
                    self._cache[clean] = (now + 60.0, [r["host"] for r in results])
                    return results
            except Exception as exc:
                logger.debug("aiodns lookup failed for %s: %s", clean, exc)

        try:
            ips, ttl = await asyncio.to_thread(_udp_dns_lookup, clean, self._nameservers)
            if ips:
                self._cache[clean] = (now + ttl, ips)
                return self._to_results(host, ips, port)
        except Exception as exc:
            logger.debug("UDP DNS fallback failed for %s: %s", clean, exc)

        if self._os_resolver is None:
            self._os_resolver = aiohttp.ThreadedResolver()
        return await self._os_resolver.resolve(clean, port, family)

    async def close(self) -> None:
        if self._aiodns_resolver is not None:
            with suppress(Exception):
                await self._aiodns_resolver.close()
            self._aiodns_resolver = None
        if self._os_resolver is not None:
            with suppress(Exception):
                await self._os_resolver.close()
            self._os_resolver = None


def make_connector(proxy: URL | str | None = None, **kwargs: Any) -> aiohttp.BaseConnector:
    """Create a TCP or SOCKS connector, bypassing broken system DNS when possible."""
    if is_socks_proxy(proxy):
        try:
            from aiohttp_socks import ProxyConnector
        except ImportError as exc:
            raise RuntimeError(
                "SOCKS proxy configured, but 'aiohttp-socks' is not installed in this Python environment."
            ) from exc
        proxy_str = str(proxy).strip()
        return ProxyConnector.from_url(
            proxy_str, rdns=proxy_str.lower().startswith("socks5h://"), **kwargs
        )
    return aiohttp.TCPConnector(resolver=FallbackDNSResolver(), **kwargs)


async def first_to_complete(coros: abc.Iterable[abc.Coroutine[Any, Any, _T]]) -> _T:
    tasks = [asyncio.ensure_future(coro) for coro in coros]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    return await next(iter(done))


def chunk(to_chunk: abc.Iterable[_T], chunk_length: int) -> abc.Generator[list[_T], None, None]:
    list_to_chunk = list(to_chunk)
    for i in range(0, len(list_to_chunk), chunk_length):
        yield list_to_chunk[i : i + chunk_length]


def format_traceback(exc: BaseException, **kwargs: Any) -> str:
    return "".join(traceback.format_exception(type(exc), exc, **kwargs))


def json_minify(data: JsonType | list[JsonType]) -> str:
    return json.dumps(data, separators=(",", ":"))


def timestamp(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def isonow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def create_nonce(chars: str, length: int) -> str:
    return "".join(random.choices(chars, k=length))


def deduplicate(iterable: abc.Iterable[_T]) -> list[_T]:
    return list(OrderedDict.fromkeys(iterable).keys())


def task_wrapper(
    afunc: abc.Callable[_P, abc.Coroutine[Any, Any, _T]] | None = None, *, critical: bool = False
):
    def decorator(
        func: abc.Callable[_P, abc.Coroutine[Any, Any, _T]]
    ) -> abc.Callable[_P, abc.Coroutine[Any, Any, _T]]:
        @wraps(func)
        async def wrapper(*args: _P.args, **kwargs: _P.kwargs):
            try:
                return await func(*args, **kwargs)
            except (ExitRequest, ReloadRequest, asyncio.CancelledError):
                return None
            except Exception as exc:
                logger.exception(f"Exception in {func.__name__} task")
                if critical and args:
                    probe = args[0]
                    handler = getattr(probe, "on_critical_error", None)
                    if callable(handler):
                        handler(exc)
                    else:
                        worker = getattr(probe, "_worker", None)
                        worker_handler = getattr(worker, "on_critical_error", None)
                        if callable(worker_handler):
                            worker_handler(exc)
                raise

        return wrapper

    if afunc is None:
        return decorator
    return decorator(afunc)


def _serialize(obj: Any) -> Any:
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        d: Any = obj.timestamp()
    elif isinstance(obj, set):
        d = list(obj)
    elif isinstance(obj, Enum):
        d = obj.value
    elif isinstance(obj, URL):
        d = str(obj)
    else:
        raise TypeError(obj)
    return {"__type": type(obj).__name__, "data": d}


_MISSING = object()
SERIALIZE_ENV: dict[str, Callable[[Any], object]] = {
    "set": set,
    "URL": URL,
    "PriorityMode": PriorityMode,
    "datetime": lambda d: datetime.fromtimestamp(d, timezone.utc),
}


def _remove_missing(obj: JsonType) -> JsonType:
    for key, value in obj.copy().items():
        if value is _MISSING:
            del obj[key]
        elif isinstance(value, dict):
            _remove_missing(value)
            if not value:
                del obj[key]
    return obj


def _deserialize(obj: JsonType) -> Any:
    if "__type" in obj:
        obj_type = obj["__type"]
        if obj_type in SERIALIZE_ENV:
            return SERIALIZE_ENV[obj_type](obj["data"])
        return _MISSING
    return obj


def merge_json(obj: JsonType, template: Mapping[Any, Any]) -> None:
    for k, v in list(obj.items()):
        if k not in template:
            del obj[k]
        elif type(v) is not type(template[k]):
            obj[k] = template[k]
        elif isinstance(v, dict):
            assert isinstance(template[k], dict)
            merge_json(v, template[k])
    for k in template.keys():
        if k not in obj:
            obj[k] = template[k]


def json_load(path: Path, defaults: _JSON_T, *, merge: bool = True) -> _JSON_T:
    new_path: Path = path.with_name(f"{path.name}.new")
    combined: JsonType | None = None
    if new_path.exists():
        try:
            with new_path.open("r", encoding="utf8") as file:
                combined = _remove_missing(json.load(file, object_hook=_deserialize))
        except (json.JSONDecodeError, OSError):
            with suppress(OSError):
                new_path.unlink()
    if combined is None and path.exists():
        try:
            with path.open("r", encoding="utf8") as file:
                combined = _remove_missing(json.load(file, object_hook=_deserialize))
        except (json.JSONDecodeError, OSError):
            combined = None
    if combined is None:
        combined = dict(defaults)
    elif merge:
        merge_json(combined, dict(defaults))
    return cast(_JSON_T, combined)


def json_save(path: Path, contents: Mapping[Any, Any], *, sort: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    new_path: Path = path.with_name(f"{path.name}.new")
    with new_path.open("w", encoding="utf8") as file:
        json.dump(contents, file, default=_serialize, sort_keys=sort, indent=4, ensure_ascii=False)
    new_path.replace(path)


def webopen(url: URL | str) -> None:
    webbrowser.open_new_tab(str(url))


class ExponentialBackoff:
    def __init__(
        self,
        *,
        base: float = 2,
        variance: float | tuple[float, float] = 0.1,
        shift: float = 0,
        maximum: float = 300,
    ) -> None:
        if base <= 1:
            raise ValueError("Base has to be greater than 1")
        self.steps: int = 0
        self.base: float = float(base)
        self.shift: float = float(shift)
        self.maximum: float = float(maximum)
        if isinstance(variance, tuple):
            self.variance_min, self.variance_max = variance
        else:
            self.variance_min = 1 - variance
            self.variance_max = 1 + variance

    def __iter__(self) -> abc.Iterator[float]:
        return self

    def __next__(self) -> float:
        value: float = (
            pow(self.base, self.steps)
            * random.uniform(self.variance_min, self.variance_max)
            + self.shift
        )
        if value > self.maximum:
            return self.maximum
        self.steps += 1
        return value

    def reset(self) -> None:
        self.steps = 0


class RateLimiter:
    def __init__(self, *, capacity: int, window: int) -> None:
        self.total: int = 0
        self.concurrent: int = 0
        self.window: int = window
        self.capacity: int = capacity
        self._reset_task: asyncio.Task[None] | None = None
        self._cond: asyncio.Condition = asyncio.Condition()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.concurrent}/{self.total}/{self.capacity})"

    def __del__(self) -> None:
        if self._reset_task is not None:
            self._reset_task.cancel()

    def _can_proceed(self) -> bool:
        return max(self.total, self.concurrent) < self.capacity

    async def __aenter__(self) -> None:
        async with self._cond:
            await self._cond.wait_for(self._can_proceed)
            self.total += 1
            self.concurrent += 1
            if self._reset_task is None:
                self._reset_task = asyncio.create_task(self._rtask())

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.concurrent -= 1
        async with self._cond:
            self._cond.notify(self.capacity - self.concurrent)

    async def _reset(self) -> None:
        if self._reset_task is not None:
            self._reset_task = None
        async with self._cond:
            self.total = 0
            if self.concurrent < self.capacity:
                self._cond.notify(self.capacity - self.concurrent)

    async def _rtask(self) -> None:
        await asyncio.sleep(self.window)
        await self._reset()


class AwaitableValue(Generic[_T]):
    def __init__(self) -> None:
        self._value: _T
        self._event = asyncio.Event()

    def has_value(self) -> bool:
        return self._event.is_set()

    def wait(self) -> abc.Coroutine[Any, Any, Literal[True]]:
        return self._event.wait()

    def get_with_default(self, default: _D) -> _T | _D:
        if self._event.is_set():
            return self._value
        return default

    async def get(self) -> _T:
        await self._event.wait()
        return self._value

    def set(self, value: _T) -> None:
        self._value = value
        self._event.set()

    def clear(self) -> None:
        self._event.clear()


class Game:
    # Special Events (509663), Travel & Outdoors (509672), Just Chatting (509658)
    SPECIAL_GAME_IDS: set[int] = {509663, 509672, 509658}

    def __init__(self, data: JsonType) -> None:
        self.id: int = int(data["id"])
        self.name: str = data.get("displayName") or data["name"]
        if "slug" in data and data["slug"]:
            self.slug = data["slug"]

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"Game({self.id}, {self.name})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            return self.id == other.id
        return NotImplemented

    def __hash__(self) -> int:
        return self.id

    @cached_property
    def slug(self) -> str:
        slug_text = re.sub(r"'", "", self.name.lower())
        slug_text = re.sub(r"\W+", "-", slug_text)
        slug_text = re.sub(r"-{2,}", "-", slug_text.strip("-"))
        return slug_text

    def is_special(self) -> bool:
        return self.id in self.SPECIAL_GAME_IDS
