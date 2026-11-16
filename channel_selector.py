"""Channel prioritization, stream directory lookup, and channel-switching selection logic."""
from __future__ import annotations

from collections import abc
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from channel import Channel, Stream
from constants import GQL_QUERIES, MAX_CHANNELS, MAX_INT, JsonType, PriorityMode, State, WebsocketTopic
from exceptions import GQLException
from utils import Game, chunk, deduplicate

if TYPE_CHECKING:
    from inventory import DropsCampaign


class ChannelSelectorMixin:
    """Computes wanted campaign games locally, fetches eligible streams, and picks the best channel."""

    def _exclusive_campaign_id(self) -> str:
        return str(getattr(self.global_settings, "exclusive_campaign_id", "") or "").strip()  # type: ignore[attr-defined]

    def effective_priority_games(self) -> list[str]:
        fav_ids = set(self.global_settings.favorite_campaigns)  # type: ignore[attr-defined]
        fav_games = [c.game.name for c in self.inventory if c.id in fav_ids]  # type: ignore[attr-defined]
        preset = getattr(self.global_settings, "radar_preset", "default")  # type: ignore[attr-defined]
        base_priority = list(self.global_settings.priority_games)  # type: ignore[attr-defined]
        if preset == "pc_games" and not base_priority:
            known = [c.game.name for c in self.inventory if not c.finished]  # type: ignore[attr-defined]
            base_priority = self.radar.get_installed_pc_games(known_games=known)  # type: ignore[attr-defined]
        extra_games: list[str] = []
        if preset == "all_campaigns":
            extra_games = [c.game.name for c in self.inventory if not c.finished]  # type: ignore[attr-defined]
        return deduplicate(
            [*self.radar.auto_promoted_games, *fav_games, *base_priority, *extra_games]  # type: ignore[attr-defined]
        )

    def _compute_wanted_games(self) -> None:
        # Only games that have earnable campaigns in self.inventory are added to wanted_games;
        # local non-campaign PC games are never sent to Twitch directory queries.
        self.wanted_games.clear()  # type: ignore[attr-defined]
        next_hour = datetime.now(timezone.utc) + timedelta(hours=1)
        exclusive_id = self._exclusive_campaign_id()
        if exclusive_id:
            for campaign in self.inventory:  # type: ignore[attr-defined]
                if campaign.id == exclusive_id and campaign.can_earn_within(next_hour):
                    self.wanted_games.append(campaign.game)  # type: ignore[attr-defined]
                    break
            return

        exclude = set(self.global_settings.exclude_games)  # type: ignore[attr-defined]
        fav_ids = set(self.global_settings.favorite_campaigns)  # type: ignore[attr-defined]
        priority = self.effective_priority_games()
        priority_mode = self.global_settings.priority_mode_enum  # type: ignore[attr-defined]
        priority_only = priority_mode is PriorityMode.PRIORITY_ONLY
        next_hour = datetime.now(timezone.utc) + timedelta(hours=1)

        sorted_campaigns = list(self.inventory)  # type: ignore[attr-defined]
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
                game not in self.wanted_games  # type: ignore[attr-defined]
                and game.name not in exclude
                and (not priority_only or game.name in priority)
                and campaign.can_earn_within(next_hour)
            ):
                self.wanted_games.append(game)  # type: ignore[attr-defined]

    async def _fetch_channels(self) -> None:
        channels = self.channels  # type: ignore[attr-defined]
        new_channels: set[Channel] = set(channels.values())
        channels.clear()

        no_acl: set[Game] = set()
        acl_channels: set[Channel] = set()
        next_hour = datetime.now(timezone.utc) + timedelta(hours=1)
        exclusive_id = self._exclusive_campaign_id()

        for campaign in self.inventory:  # type: ignore[attr-defined]
            if exclusive_id and campaign.id != exclusive_id:
                continue
            if campaign.game in self.wanted_games and campaign.can_earn_within(next_hour):  # type: ignore[attr-defined]
                if campaign.allowed_channels:
                    acl_channels.update(campaign.allowed_channels)
                else:
                    no_acl.add(campaign.game)

        # Inject pinned streamers cached in SharedCampaignRadar (disabled when waiting exclusively for one campaign)
        if not exclusive_id:
            for st in self.radar.pinned_streams.values():  # type: ignore[attr-defined]
                if st.online and st.channel_id and st.raw_channel_data:
                    ch = Channel(
                        self,  # type: ignore[arg-type]
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
            self.websocket.remove_topics(rm_topics)  # type: ignore[attr-defined]

        for ch in ordered:
            channels[ch.id] = ch

        add_topics: list[WebsocketTopic] = []
        for cid in channels:
            add_topics.append(WebsocketTopic("Channel", "StreamState", cid, self.process_stream_state))  # type: ignore[attr-defined]
            add_topics.append(WebsocketTopic("Channel", "StreamUpdate", cid, self.process_stream_update))  # type: ignore[attr-defined]
        self.websocket.add_topics(add_topics)  # type: ignore[attr-defined]

        watching = self.watching_channel.get_with_default(None)  # type: ignore[attr-defined]
        if watching is not None:
            relinked = channels.get(watching.id)
            if relinked is not None and (
                self.can_watch(relinked) or (self.is_standby and relinked.online and relinked.pinned)  # type: ignore[attr-defined]
            ):
                self.watch(relinked, standby=self.is_standby, update_status=False)  # type: ignore[attr-defined]
            else:
                self.stop_watching()  # type: ignore[attr-defined]

    def _switch_channel(self) -> None:
        new_watching: Channel | None = None
        new_standby = False
        candidates = sorted(
            self.channels.values(),  # type: ignore[attr-defined]
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

        current = self.watching_channel.get_with_default(None)  # type: ignore[attr-defined]
        if new_watching is not None:
            self.watch(new_watching, standby=new_standby)  # type: ignore[attr-defined]
            return
        if current is not None and self.can_watch(current):
            self.is_standby = False
            self.status_text = self._t("status_watching", channel=current.name)  # type: ignore[attr-defined]
            self.notify_ui_update()  # type: ignore[attr-defined]
            return

        # Standby Mode: keep watch warm on highest-viewer live pinned streamer only when drop goals exist
        if self.wanted_games and self.global_settings.standby_on_pinned and not self._exclusive_campaign_id():  # type: ignore[attr-defined]
            pinned_online = [
                ch for ch in self.channels.values() if ch.pinned and ch.online  # type: ignore[attr-defined]
            ]
            pinned_online.sort(key=lambda c: c.viewers or 0, reverse=True)
            if pinned_online:
                standby_ch = pinned_online[0]
                if current is None or current.id != standby_ch.id or not self.is_standby:  # type: ignore[attr-defined]
                    self.watch(standby_ch, standby=True)  # type: ignore[attr-defined]
                else:
                    self.status_text = self._t("status_standby", channel=standby_ch.name)  # type: ignore[attr-defined]
                    self.notify_ui_update()  # type: ignore[attr-defined]
                return

        idle_key = "status_no_streams" if self.wanted_games else "status_no_campaigns"  # type: ignore[attr-defined]
        self._idle_status_key = idle_key  # type: ignore[attr-defined]
        self.log(self._t(idle_key))  # type: ignore[attr-defined]
        self.change_state(State.IDLE)  # type: ignore[attr-defined]

    def get_priority(self, channel: Channel) -> int:
        priority = self.effective_priority_games()
        game = channel.game
        # If channel is verified for a priority campaign (even in Just Chatting), reflect that campaign's priority
        for cid in channel.available_campaign_ids:
            camp = self._campaigns.get(cid)  # type: ignore[attr-defined]
            if camp is not None and camp.can_earn(channel, ignore_channel_status=True):
                if camp.game.name in priority:
                    return priority.index(camp.game.name)
                if camp.game in self.wanted_games:  # type: ignore[attr-defined]
                    return len(priority) + self.wanted_games.index(camp.game)  # type: ignore[attr-defined]
        if game is not None:
            if game.name in priority:
                return priority.index(game.name)
            if game in self.wanted_games:  # type: ignore[attr-defined]
                return len(priority) + self.wanted_games.index(game)  # type: ignore[attr-defined]
        return MAX_INT

    def can_watch(self, channel: Channel) -> bool:
        if not channel.online:
            return False
        exclusive_id = self._exclusive_campaign_id()
        for campaign in self.inventory:  # type: ignore[attr-defined]
            if exclusive_id and campaign.id != exclusive_id:
                continue
            if campaign.can_earn(channel) and (
                (channel.game is not None and channel.drops_enabled and channel.game in self.wanted_games)  # type: ignore[attr-defined]
                or campaign.game.is_special()
                or (channel.acl_based and channel in campaign.allowed_channels)
                or (channel.id in campaign.verified_channel_ids)
            ):
                return True
        return False

    def should_switch(self, channel: Channel) -> bool:
        if not self.can_watch(channel):
            return False
        watching = self.watching_channel.get_with_default(None)  # type: ignore[attr-defined]
        if watching is None or self.is_standby or not self.can_watch(watching):  # type: ignore[attr-defined]
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

    def get_active_campaign(self, channel: Channel | None = None) -> DropsCampaign | None:
        if not self.wanted_games:  # type: ignore[attr-defined]
            return None
        watching = self.watching_channel.get_with_default(channel)  # type: ignore[attr-defined]
        if watching is None:
            return None
        exclusive_id = self._exclusive_campaign_id()
        campaigns = [
            c
            for c in self.inventory  # type: ignore[attr-defined]
            if c.can_earn(watching) and (not exclusive_id or c.id == exclusive_id)
        ]
        if not campaigns:
            return None
        campaigns.sort(key=lambda c: c.remaining_minutes)
        return campaigns[0]

    async def get_live_streams(
        self, game: Game, *, limit: int = 20, drops_enabled: bool = True
    ) -> list[Channel]:
        filters: list[str] = ["DROPS_ENABLED"] if drops_enabled else []
        try:
            response = await self.gql_request(  # type: ignore[attr-defined]
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
            Channel.from_directory(self, edge["node"], drops_enabled=drops_enabled)  # type: ignore[arg-type]
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
            responses = await self.gql_request(op_chunk)  # type: ignore[attr-defined]
            for resp in responses:
                user_data = (resp.get("data") or {}).get("user")
                if user_data is not None:
                    acl_streams_map[int(user_data["id"])] = user_data

        acl_avail_map: dict[int, list[JsonType]] = {}
        if self.global_settings.available_drops_check:  # type: ignore[attr-defined]
            avail_ops = [
                GQL_QUERIES["AvailableDrops"].with_variables({"channelID": str(cid)})
                for cid, cdata in acl_streams_map.items()
                if cdata.get("stream") is not None
            ]
            for op_chunk in chunk(avail_ops, 20):
                responses = await self.gql_request(op_chunk)  # type: ignore[attr-defined]
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
                await self.radar.ingest_channel_campaigns(  # type: ignore[attr-defined]
                    self, avail, channel_id=ch.id, channel_login=ch.login
                )

    def on_channel_update(
        self, channel: Channel, stream_before: Stream | None, stream_after: Stream | None
    ) -> None:
        if stream_before is None and stream_after is not None:
            if self.should_switch(channel):
                self.log(self._t("log_channel_online_switch", channel=channel.name))  # type: ignore[attr-defined]
                self.watch(channel, standby=False)  # type: ignore[attr-defined]
        elif stream_before is not None:
            watching = self.watching_channel.get_with_default(None)  # type: ignore[attr-defined]
            if watching is not None and watching == channel:
                if stream_after is None:
                    self.log(self._t("log_channel_offline", channel=channel.name))  # type: ignore[attr-defined]
                    self.change_state(State.CHANNEL_SWITCH)  # type: ignore[attr-defined]
                elif not self.is_standby and not self.can_watch(channel):  # type: ignore[attr-defined]
                    self.change_state(State.CHANNEL_SWITCH)  # type: ignore[attr-defined]
            elif stream_after is not None and self.should_switch(channel):
                self.watch(channel, standby=False)  # type: ignore[attr-defined]
        self.notify_ui_update()  # type: ignore[attr-defined]
