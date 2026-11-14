"""Per-account Twitch PubSub WebSocket pool decoupled from GUI widgets."""
from __future__ import annotations

import asyncio
import json
import logging
from contextlib import suppress
from time import time
from typing import Any, Literal, TYPE_CHECKING

import aiohttp

from constants import MAX_WEBSOCKETS, PING_INTERVAL, PING_TIMEOUT, WS_TOPICS_LIMIT, JsonType, WebsocketTopic
from exceptions import MinerException, WebsocketClosed
from utils import CHARS_ASCII, AwaitableValue, ExponentialBackoff, chunk, create_nonce, format_traceback, json_minify, task_wrapper

if TYPE_CHECKING:
    from collections import abc

    from worker import AccountWorker


WSMsgType = aiohttp.WSMsgType
ws_logger = logging.getLogger("TwitchMultiMiner.websocket")


class Websocket:
    def __init__(self, pool: WebsocketPool, index: int) -> None:
        self._pool: WebsocketPool = pool
        self._worker: AccountWorker = pool._worker
        self._state_lock = asyncio.Lock()
        self._idx: int = index
        self._ws: AwaitableValue[aiohttp.ClientWebSocketResponse] = AwaitableValue()
        self._closed = asyncio.Event()
        self._reconnect_requested = asyncio.Event()
        self._topics_changed = asyncio.Event()
        self._next_ping: float = time()
        self._max_pong: float = self._next_ping + PING_TIMEOUT.total_seconds()
        self._handle_task: asyncio.Task[None] | None = None
        self.topics: dict[str, WebsocketTopic] = {}
        self._submitted: set[WebsocketTopic] = set()
        self.set_status("disconnected")

    @property
    def connected(self) -> bool:
        return self._ws.has_value()

    def wait_until_connected(self):
        return self._ws.wait()

    def set_status(self, status: str | None = None, refresh_topics: bool = False) -> None:
        self._worker.on_websocket_status(
            self._idx, status, len(self.topics) if refresh_topics else None
        )

    def request_reconnect(self) -> None:
        self._next_ping = time()
        self._reconnect_requested.set()

    async def start(self) -> None:
        async with self._state_lock:
            self.start_nowait()
            await self.wait_until_connected()

    def start_nowait(self) -> None:
        if self._handle_task is None or self._handle_task.done():
            self._handle_task = asyncio.create_task(self._handle())

    async def stop(self, *, remove: bool = False) -> None:
        async with self._state_lock:
            if self._closed.is_set():
                return
            self._closed.set()
            ws = self._ws.get_with_default(None)
            if ws is not None:
                self.set_status("disconnecting")
                await ws.close()
            if self._handle_task is not None:
                with suppress(asyncio.TimeoutError, asyncio.CancelledError):
                    await asyncio.wait_for(self._handle_task, timeout=2)
                self._handle_task = None
            if remove:
                self.topics.clear()
                self._topics_changed.set()
                self._worker.on_websocket_removed(self._idx)

    def stop_nowait(self, *, remove: bool = False) -> None:
        asyncio.create_task(task_wrapper(self.stop)(remove=remove))

    async def _backoff_connect(
        self, ws_url: str, **kwargs: Any
    ) -> abc.AsyncGenerator[aiohttp.ClientWebSocketResponse, None]:
        session = await self._worker.get_session()
        backoff = ExponentialBackoff(**kwargs)
        proxy = (
            self._worker.proxy_url
            if (self._worker.proxy_url and not self._worker.proxy_in_connector)
            else None
        )
        for delay in backoff:
            if self._closed.is_set():
                break
            try:
                connected_at = time()
                async with session.ws_connect(ws_url, proxy=proxy) as websocket:
                    yield websocket
                if self._closed.is_set():
                    break
                if time() - connected_at < 15:
                    ws_logger.info(
                        f"[{self._worker.name}] WS[{self._idx}] ended prematurely (sleep: {round(delay)}s)"
                    )
                    await asyncio.sleep(delay)
                else:
                    backoff.reset()
            except (
                asyncio.TimeoutError,
                aiohttp.ClientResponseError,
                aiohttp.ClientConnectionError,
            ):
                if self._closed.is_set():
                    break
                ws_logger.info(
                    f"[{self._worker.name}] WS[{self._idx}] connection problem (sleep: {round(delay)}s)"
                )
                await asyncio.sleep(delay)
            except RuntimeError:
                break

    @task_wrapper(critical=True)
    async def _handle(self) -> None:
        self.set_status("initializing")
        await self._worker.wait_until_login()
        self.set_status("connecting")
        self._closed.clear()
        async for websocket in self._backoff_connect(
            "wss://pubsub-edge.twitch.tv/v1", maximum=3 * 60
        ):
            self._ws.set(websocket)
            self._reconnect_requested.clear()
            self.set_status("connected")
            ws_logger.info(f"[{self._worker.name}] WS[{self._idx}] connected.")
            try:
                try:
                    while not self._reconnect_requested.is_set():
                        await self._handle_ping()
                        await self._handle_topics()
                        await self._handle_recv()
                finally:
                    self._ws.clear()
                    self._submitted.clear()
                    self._topics_changed.set()
            except WebsocketClosed as exc:
                if exc.received:
                    ws_logger.warning(
                        f"[{self._worker.name}] WS[{self._idx}] closed unexpectedly: {websocket.close_code}"
                    )
                elif self._closed.is_set():
                    ws_logger.info(f"[{self._worker.name}] WS[{self._idx}] stopped.")
                    self.set_status("disconnected")
                    return
            except Exception:
                ws_logger.exception(f"[{self._worker.name}] Exception in WS[{self._idx}]")
            if self._closed.is_set():
                self.set_status("disconnected")
                return
            self.set_status("reconnecting")

    async def _handle_ping(self) -> None:
        now = time()
        if now >= self._next_ping:
            self._next_ping = now + PING_INTERVAL.total_seconds()
            self._max_pong = now + PING_TIMEOUT.total_seconds()
            await self.send({"type": "PING"})
        elif now >= self._max_pong:
            ws_logger.warning(f"[{self._worker.name}] WS[{self._idx}] PONG timeout, reconnecting")
            self.request_reconnect()

    async def _handle_topics(self) -> None:
        if not self._topics_changed.is_set():
            return
        self._topics_changed.clear()
        self.set_status(refresh_topics=True)
        auth_state = await self._worker.get_auth()
        current: set[WebsocketTopic] = set(self.topics.values())
        removed = self._submitted.difference(current)
        if removed:
            topics_list = list(map(str, removed))
            for topics in chunk(topics_list, 20):
                await self.send(
                    {
                        "type": "UNLISTEN",
                        "data": {"topics": topics, "auth_token": auth_state.access_token},
                    }
                )
            self._submitted.difference_update(removed)
        added = current.difference(self._submitted)
        if added:
            topics_list = list(map(str, added))
            for topics in chunk(topics_list, 20):
                await self.send(
                    {
                        "type": "LISTEN",
                        "data": {"topics": topics, "auth_token": auth_state.access_token},
                    }
                )
            self._submitted.update(added)

    async def _gather_recv(self, messages: list[JsonType], timeout: float = 0.5) -> None:
        ws = self._ws.get_with_default(None)
        assert ws is not None
        while True:
            try:
                raw_message: aiohttp.WSMessage = await ws.receive(timeout=timeout)
            except aiohttp.ClientConnectionError:
                raise WebsocketClosed(received=False)
            if raw_message.type is WSMsgType.TEXT:
                messages.append(json.loads(raw_message.data))
            elif raw_message.type is WSMsgType.CLOSE:
                raise WebsocketClosed(received=True)
            elif raw_message.type is WSMsgType.CLOSED:
                raise WebsocketClosed(received=False)
            elif raw_message.type is WSMsgType.CLOSING:
                pass
            elif raw_message.type is WSMsgType.ERROR:
                ws_logger.error(
                    f"[{self._worker.name}] WS[{self._idx}] error: {format_traceback(raw_message.data)}"
                )
                raise WebsocketClosed()
            else:
                ws_logger.error(f"[{self._worker.name}] WS[{self._idx}] unknown msg: {raw_message}")

    def _handle_message(self, message: JsonType) -> None:
        topic = self.topics.get(message["data"]["topic"])
        if topic is not None:
            asyncio.create_task(topic(json.loads(message["data"]["message"])))

    async def _handle_recv(self) -> None:
        messages: list[JsonType] = []
        with suppress(asyncio.TimeoutError):
            await self._gather_recv(messages, timeout=0.5)
        for message in messages:
            msg_type = message["type"]
            if msg_type == "MESSAGE":
                self._handle_message(message)
            elif msg_type == "PONG":
                self._max_pong = self._next_ping
            elif msg_type == "RESPONSE":
                pass
            elif msg_type == "RECONNECT":
                ws_logger.warning(f"[{self._worker.name}] WS[{self._idx}] requested reconnect.")
                self.request_reconnect()
            else:
                ws_logger.debug(f"[{self._worker.name}] WS[{self._idx}] unknown payload: {message}")

    def add_topics(self, topics_set: set[WebsocketTopic]) -> None:
        changed = False
        while topics_set and len(self.topics) < WS_TOPICS_LIMIT:
            topic = topics_set.pop()
            self.topics[str(topic)] = topic
            changed = True
        if changed:
            self._topics_changed.set()

    def remove_topics(self, topics_set: set[str]) -> None:
        existing = topics_set.intersection(self.topics.keys())
        if not existing:
            return
        topics_set.difference_update(existing)
        for topic in existing:
            del self.topics[topic]
        self._topics_changed.set()

    async def send(self, message: JsonType) -> None:
        ws = self._ws.get_with_default(None)
        assert ws is not None
        if message["type"] != "PING":
            message["nonce"] = create_nonce(CHARS_ASCII, 30)
        try:
            await ws.send_json(message, dumps=json_minify)
        except aiohttp.ClientConnectionError:
            raise WebsocketClosed(received=False)


class WebsocketPool:
    def __init__(self, worker: AccountWorker) -> None:
        self._worker: AccountWorker = worker
        self._running = asyncio.Event()
        self.websockets: list[Websocket] = []

    @property
    def running(self) -> bool:
        return self._running.is_set()

    def wait_until_connected(self) -> abc.Coroutine[Any, Any, Literal[True]]:
        return self._running.wait()

    async def start(self) -> None:
        self._running.set()
        if self.websockets:
            await asyncio.gather(*(ws.start() for ws in self.websockets))

    async def stop(self, *, clear_topics: bool = False) -> None:
        self._running.clear()
        if self.websockets:
            await asyncio.gather(*(ws.stop(remove=clear_topics) for ws in self.websockets))
            if clear_topics:
                self.websockets.clear()

    def add_topics(self, topics: abc.Iterable[WebsocketTopic]) -> None:
        topics_set = set(topics)
        if not topics_set:
            return
        topics_set.difference_update(*(ws.topics.values() for ws in self.websockets))
        if not topics_set:
            return
        for ws_idx in range(MAX_WEBSOCKETS):
            if ws_idx < len(self.websockets):
                ws = self.websockets[ws_idx]
            else:
                ws = Websocket(self, ws_idx)
                if self.running:
                    ws.start_nowait()
                self.websockets.append(ws)
            ws.add_topics(topics_set)
            if not topics_set:
                return
        raise MinerException("Maximum topics limit has been reached")

    def remove_topics(self, topics: abc.Iterable[str]) -> None:
        topics_set = set(topics)
        if not topics_set:
            return
        for ws in self.websockets:
            ws.remove_topics(topics_set)
        recycled_topics: list[WebsocketTopic] = []
        while self.websockets:
            count = sum(len(ws.topics) for ws in self.websockets)
            if count <= (len(self.websockets) - 1) * WS_TOPICS_LIMIT:
                ws = self.websockets.pop()
                recycled_topics.extend(ws.topics.values())
                ws.stop_nowait(remove=True)
            else:
                break
        if recycled_topics:
            self.add_topics(recycled_topics)
