"""Drop campaigns, timed drops, and fixes for 15-minute stalls, auto-claiming, and ACL/Just Chatting."""
from __future__ import annotations

import logging
import math
import re
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import cached_property
from itertools import chain
from typing import TYPE_CHECKING

from channel import Channel
from constants import GQL_QUERIES, JsonType, URLType
from exceptions import GQLException
from utils import Game, timestamp

if TYPE_CHECKING:
    from collections import abc

    from worker import AccountWorker


logger = logging.getLogger("TwitchMultiMiner")
DIMS_PATTERN = re.compile(r"-\d+x\d+(?=\.(?:jpg|png|gif)$)", re.I)
UNCONFIRMED_SYNC_THRESHOLD = 5


def remove_dimensions(url: URLType) -> URLType:
    return URLType(DIMS_PATTERN.sub("", url))


class BenefitType(Enum):
    UNKNOWN = "UNKNOWN"
    BADGE = "BADGE"
    EMOTE = "EMOTE"
    DIRECT_ENTITLEMENT = "DIRECT_ENTITLEMENT"

    def is_badge_or_emote(self) -> bool:
        return self in (BenefitType.BADGE, BenefitType.EMOTE)


class Benefit:
    __slots__ = ("id", "name", "type", "image_url")

    def __init__(self, data: JsonType) -> None:
        benefit_data: JsonType = data["benefit"]
        self.id: str = benefit_data["id"]
        self.name: str = benefit_data["name"]
        dist_type = benefit_data.get("distributionType", "UNKNOWN")
        self.type: BenefitType = (
            BenefitType(dist_type)
            if dist_type in BenefitType.__members__
            else BenefitType.UNKNOWN
        )
        image = (
            benefit_data.get("imageAssetURL")
            or benefit_data.get("imageURL")
            or data.get("imageAssetURL")
            or ""
        )
        self.image_url: URLType = URLType(str(image).strip())


class BaseDrop:
    def __init__(
        self, campaign: DropsCampaign, data: JsonType, claimed_benefits: dict[str, datetime]
    ) -> None:
        self._worker: AccountWorker = campaign._worker
        self.id: str = data["id"]
        self.name: str = data["name"]
        self.campaign: DropsCampaign = campaign
        self.benefits: list[Benefit] = [Benefit(b) for b in (data.get("benefitEdges") or [])]
        self.starts_at: datetime = (
            timestamp(data["startAt"]) if data.get("startAt") else campaign.starts_at
        )
        self.ends_at: datetime = (
            timestamp(data["endAt"]) if data.get("endAt") else campaign.ends_at
        )
        self.claim_id: str | None = None
        self.is_claimed: bool = False
        self_edge = data.get("self")
        if isinstance(self_edge, dict):
            self.claim_id = self_edge.get("dropInstanceID")
            self.is_claimed = bool(self_edge.get("isClaimed", False))
        if not self.is_claimed and self.id in getattr(self._worker, "_claimed_drop_ids", ()):
            self.is_claimed = True
        if (
            not self.is_claimed
            and self.benefits
            and (
                dts := [
                    claimed_benefits[bid]
                    for benefit in self.benefits
                    if (bid := benefit.id) in claimed_benefits
                ]
            )
            and len(dts) == len(self.benefits)
            and all(self.starts_at <= dt < self.ends_at for dt in dts)
        ):
            self.is_claimed = True
        self.precondition_drops: list[str] = [
            d["id"] for d in (data.get("preconditionDrops") or [])
        ]

    def __repr__(self) -> str:
        if self.is_claimed:
            additional = ", claimed=True"
        elif self.can_earn():
            additional = ", can_earn=True"
        else:
            additional = ""
        return f"Drop({self.rewards_text()}{additional})"

    @property
    def preconditions_met(self) -> bool:
        campaign = self.campaign
        return all(
            pid in campaign.timed_drops and campaign.timed_drops[pid].is_claimed
            for pid in self.precondition_drops
        )

    def _on_state_changed(self) -> None:
        pass

    def _base_earn_conditions(self) -> bool:
        return (
            self.preconditions_met
            and not self.is_claimed
            and (bool(self.benefits) or self.id in self.campaign.preconditions_chain())
        )

    def _base_can_earn(self) -> bool:
        now = datetime.now(timezone.utc)
        return self._base_earn_conditions() and self.starts_at <= now < self.ends_at

    def _can_earn_within(self, stamp: datetime) -> bool:
        return (
            self._base_earn_conditions()
            and self.ends_at > datetime.now(timezone.utc)
            and self.starts_at < stamp
        )

    def can_earn(
        self, channel: Channel | None = None, ignore_channel_status: bool = False
    ) -> bool:
        return self._base_can_earn() and self.campaign._base_can_earn(
            channel, ignore_channel_status
        )

    @property
    def can_claim(self) -> bool:
        return (
            self.claim_id is not None
            and not self.is_claimed
            and datetime.now(timezone.utc) < self.campaign.ends_at + timedelta(hours=24)
        )

    def update_claim(self, claim_id: str) -> None:
        self.claim_id = claim_id

    async def generate_claim(self) -> None:
        uid = self._worker.user_id
        if uid is None:
            uid = await self._worker.ensure_user_id()
        self.claim_id = f"{uid}#{self.campaign.id}#{self.id}"

    def rewards_text(self, delim: str = ", ") -> str:
        return delim.join(benefit.name for benefit in self.benefits) or self.name

    @property
    def primary_image_url(self) -> str:
        for benefit in self.benefits:
            if benefit.image_url:
                return str(benefit.image_url)
        return str(self.campaign.image_url or "")

    async def ensure_claim(self) -> bool:
        """Auto-generate claim_id when missing and claim the drop."""
        if self.is_claimed:
            return True
        if self.claim_id is None:
            await self.generate_claim()
        return await self.claim()

    async def claim(self) -> bool:
        status = await self._claim_status()
        if status == "CLAIMED_NEW":
            self.is_claimed = True
            self._worker.record_claimed_drop(self)
            return True
        if status == "ALREADY_CLAIMED":
            self.is_claimed = True
            if hasattr(self._worker, "_claimed_drop_ids"):
                self._worker._claimed_drop_ids.add(self.id)
            return True
        logger.error(f"[{self._worker.name}] Drop claim failed! Drop ID: {self.id}")
        return False

    async def _claim(self) -> bool:
        return (await self._claim_status()) in ("CLAIMED_NEW", "ALREADY_CLAIMED")

    async def _claim_status(self) -> str:
        if self.is_claimed:
            return "ALREADY_CLAIMED"
        if not self.can_claim:
            return "FAILED"
        try:
            response = await self._worker.gql_request(
                GQL_QUERIES["ClaimDrop"].with_variables({"input": {"dropInstanceID": self.claim_id}})
            )
        except GQLException:
            return "FAILED"
        data = response.get("data", {})
        if data.get("errors"):
            return "FAILED"
        claim_res = data.get("claimDropRewards")
        if not claim_res:
            return "FAILED"
        st = claim_res.get("status")
        if st == "ELIGIBLE_FOR_ALL":
            return "CLAIMED_NEW"
        if st == "DROP_INSTANCE_ALREADY_CLAIMED":
            return "ALREADY_CLAIMED"
        return "FAILED"


class TimedDrop(BaseDrop):
    def __init__(
        self, campaign: DropsCampaign, data: JsonType, claimed_benefits: dict[str, datetime]
    ) -> None:
        super().__init__(campaign, data, claimed_benefits)
        self_edge = data.get("self")
        self.real_current_minutes: int = (
            int(self_edge.get("currentMinutesWatched") or 0)
            if isinstance(self_edge, dict)
            else 0
        )
        self.required_minutes: int = int(data.get("requiredMinutesWatched") or 0)
        self.extra_current_minutes: int = 0
        self.unconfirmed_minutes: int = 0
        if self.is_claimed:
            self.real_current_minutes = self.required_minutes

    def __repr__(self) -> str:
        if self.is_claimed:
            additional = ", claimed=True"
        elif self.can_earn():
            additional = ", can_earn=True"
        else:
            additional = ""
        minutes = (
            f", {self.current_minutes}/{self.required_minutes}"
            if 0 < self.current_minutes < self.required_minutes
            else ""
        )
        return f"Drop({self.rewards_text()}{minutes}{additional})"

    @property
    def current_minutes(self) -> int:
        return self.real_current_minutes + self.extra_current_minutes

    @property
    def remaining_minutes(self) -> int:
        return max(0, self.required_minutes - self.current_minutes)

    @property
    def ready_to_claim(self) -> bool:
        return (
            not self.is_claimed
            and self.required_minutes > 0
            and self.real_current_minutes >= self.required_minutes
        )

    @property
    def total_required_minutes(self) -> int:
        return self.required_minutes + max(
            (
                self.campaign.timed_drops[pid].total_required_minutes
                for pid in self.precondition_drops
                if pid in self.campaign.timed_drops
            ),
            default=0,
        )

    @property
    def total_remaining_minutes(self) -> int:
        return self.remaining_minutes + max(
            (
                self.campaign.timed_drops[pid].total_remaining_minutes
                for pid in self.precondition_drops
                if pid in self.campaign.timed_drops
            ),
            default=0,
        )

    @property
    def progress(self) -> float:
        if self.required_minutes <= 0:
            return 1.0 if self.is_claimed else 0.0
        if self.current_minutes <= 0:
            return 0.0
        if self.current_minutes >= self.required_minutes:
            return 1.0
        return self.current_minutes / self.required_minutes

    @property
    def availability(self) -> float:
        now = datetime.now(timezone.utc)
        if self.required_minutes > 0 and self.total_remaining_minutes > 0 and now < self.ends_at:
            return ((self.ends_at - now).total_seconds() / 60) / self.total_remaining_minutes
        return math.inf

    def _base_earn_conditions(self) -> bool:
        # Never block earning due to extra_current_minutes / delayed WebSocket updates
        return super()._base_earn_conditions() and self.required_minutes > 0

    def _on_state_changed(self) -> None:
        self._worker.notify_ui_update()

    def _update_real_minutes(self, delta: int) -> None:
        new_val = self.real_current_minutes + delta
        if new_val < 0:
            self.real_current_minutes = 0
        elif new_val < self.required_minutes:
            self.real_current_minutes = new_val
        else:
            self.real_current_minutes = self.required_minutes
        self.extra_current_minutes = 0
        self.unconfirmed_minutes = 0
        self._on_state_changed()

    def _bump_minutes(self, channel: Channel | None) -> bool:
        if self.can_earn(channel):
            self.unconfirmed_minutes += 1
            if self.current_minutes < self.required_minutes:
                self.extra_current_minutes += 1
            self._on_state_changed()
            return self.unconfirmed_minutes >= UNCONFIRMED_SYNC_THRESHOLD
        return False

    def reset_unconfirmed(self) -> None:
        self.unconfirmed_minutes = 0

    async def claim(self) -> bool:
        result = await super().claim()
        if result:
            self.real_current_minutes = self.required_minutes
            self.extra_current_minutes = 0
            self.unconfirmed_minutes = 0
        self._on_state_changed()
        return result

    def update_minutes(self, new_minutes: int) -> None:
        delta: int = new_minutes - self.real_current_minutes
        if self.real_current_minutes + delta < 0:
            delta = -self.real_current_minutes
        elif self.real_current_minutes + delta > self.required_minutes:
            delta = self.required_minutes - self.real_current_minutes
        self.campaign._update_real_minutes(delta)


class DropsCampaign:
    def __init__(
        self,
        worker: AccountWorker,
        data: JsonType,
        claimed_benefits: dict[str, datetime],
        *,
        verified_channel_ids: set[int] | None = None,
    ) -> None:
        self._worker: AccountWorker = worker
        self.id: str = data["id"]
        self.name: str = data["name"]
        self.game: Game = Game(data["game"])
        self_edge = data.get("self")
        self.linked: bool = bool(
            self_edge.get("isAccountConnected", True) if isinstance(self_edge, dict) else True
        )
        self.link_url: str = data.get("accountLinkURL", "")
        box_art = data.get("game", {}).get("boxArtURL") or ""
        self.image_url: URLType = remove_dimensions(URLType(box_art)) if box_art else URLType("")
        now_utc = datetime.now(timezone.utc)
        self.starts_at: datetime = (
            timestamp(data["startAt"]) if data.get("startAt") else now_utc - timedelta(minutes=1)
        )
        self.ends_at: datetime = (
            timestamp(data["endAt"]) if data.get("endAt") else now_utc + timedelta(hours=24)
        )
        self._valid: bool = data.get("status") != "EXPIRED"
        allowed: JsonType = data.get("allow") or {}
        raw_channels = allowed.get("channels") or []
        self.allowed_channels: list[Channel] = (
            [Channel.from_acl(worker, ch) for ch in raw_channels]
            if raw_channels and allowed.get("isEnabled", True)
            else []
        )
        self.verified_channel_ids: set[int] = set(verified_channel_ids or ())
        self.timed_drops: dict[str, TimedDrop] = {
            drop_data["id"]: TimedDrop(self, drop_data, claimed_benefits)
            for drop_data in (data.get("timeBasedDrops") or [])
        }

    def __repr__(self) -> str:
        return f"Campaign({self.game!s}, {self.name}, {self.claimed_drops}/{self.total_drops})"

    @property
    def drops(self) -> abc.Iterable[TimedDrop]:
        return self.timed_drops.values()

    @property
    def time_triggers(self) -> set[datetime]:
        return set(
            chain(
                (self.starts_at, self.ends_at),
                *((d.starts_at, d.ends_at) for d in self.timed_drops.values()),
            )
        )

    @property
    def active(self) -> bool:
        return self._valid and self.starts_at <= datetime.now(timezone.utc) < self.ends_at

    @property
    def upcoming(self) -> bool:
        return self._valid and datetime.now(timezone.utc) < self.starts_at

    @property
    def expired(self) -> bool:
        return not self._valid or self.ends_at <= datetime.now(timezone.utc)

    @property
    def total_drops(self) -> int:
        return len(self.timed_drops)

    @property
    def eligible(self) -> bool:
        if self.has_badge_or_emote:
            return self._worker.global_settings.enable_badges_emotes
        return self.linked

    @cached_property
    def has_badge_or_emote(self) -> bool:
        return any(
            benefit.type.is_badge_or_emote() for drop in self.drops for benefit in drop.benefits
        )

    @property
    def finished(self) -> bool:
        return all(d.is_claimed or d.required_minutes <= 0 for d in self.drops)

    @property
    def claimed_drops(self) -> int:
        return sum(1 for d in self.drops if d.is_claimed)

    @property
    def remaining_drops(self) -> int:
        return sum(1 for d in self.drops if not d.is_claimed)

    @property
    def required_minutes(self) -> int:
        return max((d.total_required_minutes for d in self.drops), default=0)

    @property
    def remaining_minutes(self) -> int:
        return max((d.total_remaining_minutes for d in self.drops), default=0)

    @property
    def progress(self) -> float:
        if self.total_drops == 0:
            return 0.0
        return sum(d.progress for d in self.drops) / self.total_drops

    @property
    def availability(self) -> float:
        return min((d.availability for d in self.drops), default=math.inf)

    @property
    def first_drop(self) -> TimedDrop | None:
        drops: list[TimedDrop] = sorted(
            (drop for drop in self.drops if drop.can_earn()),
            key=lambda d: d.remaining_minutes,
        )
        return drops[0] if drops else None

    def matches_keywords(self, keywords: list[str]) -> bool:
        haystack = f"{self.name} {self.game.name}".lower()
        return any(kw.lower() in haystack for kw in keywords if kw)

    def _update_real_minutes(self, delta: int) -> None:
        for drop in self.drops:
            drop._update_real_minutes(delta)

    def _base_can_earn(
        self, channel: Channel | None = None, ignore_channel_status: bool = False
    ) -> bool:
        if not (self.eligible and self.active):
            return False
        if channel is None:
            return True
        if self.allowed_channels and channel not in self.allowed_channels:
            return False
        if ignore_channel_status:
            return True
        # Explicit ACL channels or AvailableDrops-verified channels qualify regardless of category (e.g. Just Chatting)
        if (self.allowed_channels and channel in self.allowed_channels) or (
            channel.id in self.verified_channel_ids
        ):
            return True
        return (
            (channel.game is not None and channel.game == self.game)
            or self.game.is_special()
        )

    def get_drop(self, drop_id: str) -> TimedDrop | None:
        return self.timed_drops.get(drop_id)

    def preconditions_chain(self) -> set[str]:
        return set(
            chain.from_iterable(
                drop.precondition_drops for drop in self.drops if not drop.is_claimed
            )
        )

    def can_earn(
        self, channel: Channel | None = None, ignore_channel_status: bool = False
    ) -> bool:
        return self._base_can_earn(channel, ignore_channel_status) and any(
            drop._base_can_earn() for drop in self.drops
        )

    def can_earn_within(self, stamp: datetime) -> bool:
        return (
            self.eligible
            and self._valid
            and self.ends_at > datetime.now(timezone.utc)
            and self.starts_at < stamp
            and any(drop._can_earn_within(stamp) for drop in self.drops)
        )

    def bump_minutes(self, channel: Channel) -> bool:
        """Estimate +1 minute when WS progress is delayed; returns True when GQL sync is needed."""
        needs_sync = False
        for drop in self.drops:
            if drop._bump_minutes(channel):
                needs_sync = True
        return needs_sync
