"""Stream and Channel models with fixed dynamic watch payloads and hybrid HLS+Spade telemetry."""
from __future__ import annotations

import asyncio
import gzip
import json
import logging
import re
from base64 import b64encode
from typing import Any, SupportsInt, TYPE_CHECKING, cast

import aiohttp
from yarl import URL

from constants import CALL, GQL_QUERIES, ONLINE_DELAY, GQLPersistedQuery, GQLQuery, JsonType, URLType
from exceptions import MinerException, RequestException
from utils import Game, isonow, json_minify

if TYPE_CHECKING:
    from worker import AccountWorker


logger = logging.getLogger("TwitchMultiMiner")


class Stream:
    def __init__(
        self,
        channel: Channel,
        *,
        id: SupportsInt,
        game: JsonType | None,
        viewers: int,
        title: str,
    ) -> None:
        self.channel: Channel = channel
        self.broadcast_id = int(id)
        self.viewers: int = int(viewers or 0)
        self.drops_enabled: bool = not channel._worker.global_settings.available_drops_check
        self.game: Game | None = Game(game) if game else None
        self.title: str = title or ""
        self._stream_url: URLType | None = None

    @property
    def _watch_payload(self) -> list[JsonType]:
        # Regular property (not cached) so client_time is always fresh on every watch pulse.
        return [
            {
                "event": "minute-watched",
                "properties": {
                    "broadcast_id": str(self.broadcast_id),
                    "channel_id": str(self.channel.id),
                    "channel": self.channel._login,
                    "client_time": isonow(),
                    "game": self.game.name if self.game is not None else "",
                    "game_id": str(self.game.id) if self.game is not None else "",
                    "hidden": False,
                    "is_live": True,
                    "live": True,
                    "logged_in": True,
                    "minutes_logged": 1,
                    "muted": False,
                    "user_id": self.channel._worker.user_id or 0,
                },
            }
        ]

    @property
    def spade_payload(self) -> JsonType:
        return {
            "data": b64encode(json_minify(self._watch_payload).encode("utf8")).decode("utf8")
        }

    @property
    def gql_payload(self) -> GQLQuery:
        return GQLQuery(
            (
                "\n mutation SendEvents($input: SendSpadeEventsInput!) "
                "{\n sendSpadeEvents(input: $input) {\n statusCode\n}\n}\n"
            ),
            b64encode(gzip.compress(json_minify(self._watch_payload).encode("utf8"))).decode("utf8"),
        )

    @classmethod
    def from_get_stream(cls, channel: Channel, channel_data: JsonType) -> Stream:
        stream = channel_data["stream"]
        settings = channel_data["broadcastSettings"]
        return cls(
            channel,
            id=stream["id"],
            game=settings.get("game"),
            viewers=stream.get("viewersCount", 0),
            title=settings.get("title", ""),
        )

    @classmethod
    def from_directory(
        cls, channel: Channel, channel_data: JsonType, *, drops_enabled: bool = False
    ) -> Stream:
        self = cls(
            channel,
            id=channel_data["id"],
            game=channel_data.get("game"),
            viewers=channel_data.get("viewersCount", 0),
            title=channel_data.get("title", ""),
        )
        self.drops_enabled = drops_enabled
        return self

    def __eq__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            return self.broadcast_id == other.broadcast_id
        return NotImplemented

    def invalidate_stream_url(self) -> None:
        self._stream_url = None

    async def get_stream_url(self) -> URLType | None:
        if self._stream_url is not None:
            return self._stream_url
        playback_token_response: JsonType = await self.channel._worker.gql_request(
            GQL_QUERIES["PlaybackAccessToken"].with_variables({"login": self.channel._login})
        )
        token_data: JsonType | None = (
            playback_token_response.get("data", {}).get("streamPlaybackAccessToken")
        )
        if not token_data:
            return None
        token_value = token_data["value"]
        token_signature = token_data["signature"]
        available_qualities: str = ""
        hls_url = URL(
            f"https://usher.ttvnw.net/api/channel/hls/{self.channel._login}.m3u8"
        ).with_query({"sig": token_signature, "token": token_value})
        try:
            async with self.channel._worker.request("GET", hls_url) as qualities_response:
                if qualities_response.status >= 400:
                    self._stream_url = None
                    return None
                available_qualities = await qualities_response.text()
            try:
                available_json: JsonType = json.loads(available_qualities)
            except json.JSONDecodeError:
                pass
            else:
                if isinstance(available_json, list):
                    available_json = available_json[0]
                if "error" in available_json:
                    logger.error(f"Stream URL get error ({self.channel._login}): {available_json['error']}")
                    self.channel.set_offline()
                self._stream_url = None
                return None

            lines = [line.strip() for line in available_qualities.splitlines() if line.strip().startswith("http")]
            if not lines:
                self._stream_url = None
                return None
            # Pick the last playlist URL (lowest bandwidth stream)
            self._stream_url = cast(URLType, URL(lines[-1]))
        except (aiohttp.InvalidURL, ValueError, RequestException):
            self._stream_url = None
            return None
        return self._stream_url


class Channel:
    __slots__ = (
        "_worker",
        "id",
        "_login",
        "_display_name",
        "_spade_url",
        "_stream",
        "_pending_stream_up",
        "acl_based",
        "pinned",
        "drops_verified",
        "available_campaign_ids",
    )

    def __init__(
        self,
        worker: AccountWorker,
        *,
        id: SupportsInt,
        login: str,
        display_name: str | None = None,
        acl_based: bool = False,
        pinned: bool = False,
    ) -> None:
        self._worker: AccountWorker = worker
        self.id: int = int(id)
        self._login: str = login.lower()
        self._display_name: str | None = display_name
        self._spade_url: URLType | None = None
        self._stream: Stream | None = None
        self._pending_stream_up: asyncio.Task[Any] | None = None
        self.acl_based: bool = acl_based
        self.pinned: bool = pinned
        self.drops_verified: bool = False
        self.available_campaign_ids: set[str] = set()

    @classmethod
    def from_acl(cls, worker: AccountWorker, data: JsonType) -> Channel:
        login = data["name"].lower()
        return cls(
            worker,
            id=data["id"],
            login=login,
            display_name=data.get("displayName"),
            acl_based=True,
            pinned=login in worker.global_settings.pinned_streamers,
        )

    @classmethod
    def from_directory(
        cls, worker: AccountWorker, data: JsonType, *, drops_enabled: bool = False
    ) -> Channel:
        channel = data["broadcaster"]
        login = channel["login"].lower()
        self = cls(
            worker,
            id=channel["id"],
            login=login,
            display_name=channel.get("displayName"),
            pinned=login in worker.global_settings.pinned_streamers,
        )
        self._stream = Stream.from_directory(self, data, drops_enabled=drops_enabled)
        return self

    def __repr__(self) -> str:
        name = f"{self._display_name}({self._login})" if self._display_name else self._login
        return f"Channel({name}, {self.id})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            return self.id == other.id
        return NotImplemented

    def __hash__(self) -> int:
        return self.id

    @property
    def stream_gql(self) -> GQLPersistedQuery:
        return GQL_QUERIES["GetStreamInfo"].with_variables({"channel": self._login})

    @property
    def name(self) -> str:
        return self._display_name or self._login

    @property
    def login(self) -> str:
        return self._login

    @property
    def url(self) -> URLType:
        return URLType(f"https://www.twitch.tv/{self._login}")

    @property
    def online(self) -> bool:
        return self._stream is not None

    @property
    def offline(self) -> bool:
        return self._stream is None and self._pending_stream_up is None

    @property
    def pending_online(self) -> bool:
        return self._stream is None and self._pending_stream_up is not None

    @property
    def game(self) -> Game | None:
        return self._stream.game if self._stream is not None else None

    @property
    def viewers(self) -> int | None:
        return self._stream.viewers if self._stream is not None else None

    @viewers.setter
    def viewers(self, value: int) -> None:
        if self._stream is not None:
            self._stream.viewers = value

    @property
    def drops_enabled(self) -> bool:
        return self._stream.drops_enabled if self._stream is not None else False

    def remove(self) -> None:
        if self._pending_stream_up is not None:
            self._pending_stream_up.cancel()
            self._pending_stream_up = None

    async def get_spade_url(self) -> URLType:
        cached = self._worker.radar.get_cached_spade_url(self._login)
        if cached is not None:
            return cached
        settings_pattern = r'src="(https://[\w.]+/config/settings\.[0-9a-f]{32}\.js)"'
        spade_pattern = r'"spade_?url": ?"(https://[.\w\-/]+)"'
        async with self._worker.request("GET", self.url) as response1:
            streamer_html: str = await response1.text(encoding="utf8")
        match = re.search(spade_pattern, streamer_html, re.I)
        if not match:
            match = re.search(settings_pattern, streamer_html, re.I)
            if not match:
                raise MinerException(f"Error while spade_url extraction (step #1) for {self._login}")
            streamer_settings = match.group(1)
            async with self._worker.request("GET", streamer_settings) as response2:
                settings_js: str = await response2.text(encoding="utf8")
            match = re.search(spade_pattern, settings_js, re.I)
            if not match:
                raise MinerException(f"Error while spade_url extraction (step #2) for {self._login}")
        spade_url = URLType(match.group(1))
        self._worker.radar.set_cached_spade_url(self._login, spade_url)
        return spade_url

    def _check_drops_enabled(self, available_drops: list[JsonType]) -> bool:
        self.drops_verified = True
        self.available_campaign_ids = {
            cid for item in available_drops if isinstance(item, dict) and (cid := item.get("id"))
        }
        matched = False
        for cid in self.available_campaign_ids:
            campaign = self._worker._campaigns.get(cid)
            if campaign is not None:
                campaign.verified_channel_ids.add(self.id)
                if campaign.can_earn(self, ignore_channel_status=True):
                    matched = True
        return matched

    def external_update(self, channel_data: JsonType, available_drops: list[JsonType]) -> None:
        if not channel_data.get("stream"):
            self._stream = None
            return
        stream = Stream.from_get_stream(self, channel_data)
        has_available = self._check_drops_enabled(available_drops)
        if not stream.drops_enabled:
            stream.drops_enabled = has_available
        elif available_drops:
            stream.drops_enabled = True
        self._stream = stream

    async def get_stream(self) -> Stream | None:
        try:
            response: JsonType = await self._worker.gql_request(self.stream_gql)
        except MinerException as exc:
            raise MinerException(f"Channel: {self._login}") from exc
        channel_data: JsonType | None = response.get("data", {}).get("user")
        if not channel_data:
            return None
        if self._display_name is None and channel_data.get("displayName"):
            self._display_name = channel_data["displayName"]
        if not channel_data.get("stream"):
            return None
        stream = Stream.from_get_stream(self, channel_data)
        try:
            available_drops_resp: JsonType = await self._worker.gql_request(
                GQL_QUERIES["AvailableDrops"].with_variables({"channelID": str(self.id)})
            )
        except MinerException:
            logger.log(CALL, f"[{self._worker.name}] AvailableDrops GQL failed for {self._login}")
        else:
            raw_avail = (
                available_drops_resp.get("data", {})
                .get("channel", {})
                .get("viewerDropCampaigns")
                or []
            )
            has_available = self._check_drops_enabled(raw_avail)
            if not stream.drops_enabled:
                stream.drops_enabled = has_available
            if raw_avail:
                await self._worker.radar.ingest_channel_campaigns(
                    self._worker,
                    raw_avail,
                    channel_id=self.id,
                    channel_login=self._login,
                    channel_game=(channel_data.get("broadcastSettings") or {}).get("game"),
                )
        return stream

    async def update_stream(self) -> bool:
        old_stream = self._stream
        self._stream = await self.get_stream()
        self._worker.on_channel_update(self, old_stream, self._stream)
        return self._stream is not None

    async def _online_delay(self) -> None:
        await asyncio.sleep(ONLINE_DELAY.total_seconds())
        self._pending_stream_up = None
        await self.update_stream()

    def check_online(self) -> None:
        if self._pending_stream_up is None:
            self._pending_stream_up = asyncio.create_task(self._online_delay())

    def set_offline(self) -> None:
        if self._pending_stream_up is not None:
            self._pending_stream_up.cancel()
            self._pending_stream_up = None
        if self.online:
            old_stream = self._stream
            self._stream = None
            self._worker.on_channel_update(self, old_stream, self._stream)

    async def send_watch(self) -> bool:
        """Fetch media playlist and send HEAD to every HLS chunk; auto-reset token URL on non-200."""
        if self._stream is None:
            return False
        stream_url = await self._stream.get_stream_url()
        if stream_url is None:
            return False
        try:
            async with self._worker.request(
                "GET", stream_url, headers={"Connection": "close"}
            ) as chunks_response:
                if chunks_response.status >= 400:
                    self._stream.invalidate_stream_url()
                    return False
                available_chunks: str = await chunks_response.text()
        except RequestException:
            self._stream.invalidate_stream_url()
            return False

        available_chunks = re.sub(r'"url": ?".+}",', "", available_chunks)
        try:
            available_json: JsonType = json.loads(available_chunks)
        except json.JSONDecodeError:
            pass
        else:
            if isinstance(available_json, list):
                available_json = available_json[0]
            if "error" in available_json:
                logger.error(f"Send watch error ({self._login}): {available_json['error']}")
            self._stream.invalidate_stream_url()
            return False

        chunks_list: list[URLType] = [
            URLType(chunk_line.strip())
            for chunk_line in available_chunks.strip().splitlines()
            if chunk_line.strip().startswith("http")
        ]
        if not chunks_list:
            self._stream.invalidate_stream_url()
            return False

        # Twitch requires HEAD requests across all playlist segments (~15 chunks) to credit watch minutes
        for chunk_url in chunks_list:
            try:
                async with self._worker.request("HEAD", chunk_url) as head_response:
                    if head_response.status != 200:
                        self._stream.invalidate_stream_url()
                        return False
            except RequestException:
                self._stream.invalidate_stream_url()
                return False
        return True

    async def _send_watch_spade(self) -> bool:
        if self._stream is None:
            return False
        try:
            if self._spade_url is None:
                self._spade_url = await self.get_spade_url()
            async with self._worker.request(
                "POST", self._spade_url, data=self._stream.spade_payload
            ) as response:
                return response.status == 204
        except (MinerException, RequestException):
            return False

    async def _send_watch_gql(self) -> bool:
        if self._stream is None:
            return False
        try:
            watch_response = await self._worker.gql_request(self._stream.gql_payload)
            return watch_response.get("data", {}).get("sendSpadeEvents", {}).get("statusCode") == 204
        except (MinerException, RequestException):
            return False

    async def send_watch_hybrid(self, *, send_spade: bool = False) -> bool:
        """Run HLS watch pulse (with 1-time token refresh retry) + optional Spade/GQL pulse."""
        hls_ok = await self.send_watch()
        if not hls_ok and self._stream is not None:
            # PlaybackAccessToken may have expired; retry once with freshly invalidated URL
            self._stream.invalidate_stream_url()
            hls_ok = await self.send_watch()

        spade_ok = False
        if send_spade or not hls_ok:
            spade_ok = await self._send_watch_spade()
            if not spade_ok:
                spade_ok = await self._send_watch_gql()
        return hls_ok or spade_ok
