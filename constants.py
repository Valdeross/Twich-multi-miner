"""Protocol constants, Twitch GQL queries, WebSocket topics, and defaults."""
from __future__ import annotations

import logging
import random
import sys
from copy import deepcopy
from datetime import timedelta
from enum import Enum, auto
from pathlib import Path
from typing import Any, Dict, Literal, NewType, TYPE_CHECKING

from yarl import URL

if TYPE_CHECKING:
    from collections import abc
    from typing_extensions import TypeAlias


APP_NAME = "TwitchMultiMiner"
APP_VERSION = "1.0.0"

WORKING_DIR = Path(__file__).resolve().parent
PROFILES_DIR = WORKING_DIR / "profiles"
ICONS_DIR = WORKING_DIR / "icons"
GLOBAL_SETTINGS_PATH = WORKING_DIR / "global_settings.json"
APP_LOG_PATH = WORKING_DIR / "multiminer.log"

JsonType = Dict[str, Any]
URLType = NewType("URLType", str)
GQLOperation: TypeAlias = "GQLQuery | GQLPersistedQuery"
TopicProcess: TypeAlias = "abc.Callable[[int, JsonType], Any]"

MAX_INT = sys.maxsize
BASE_TOPICS = 2
MAX_WEBSOCKETS = 8
WS_TOPICS_LIMIT = 50
TOPICS_PER_CHANNEL = 2
MAX_TOPICS = (MAX_WEBSOCKETS * WS_TOPICS_LIMIT) - BASE_TOPICS
MAX_CHANNELS = MAX_TOPICS // TOPICS_PER_CHANNEL

PING_INTERVAL = timedelta(minutes=3)
PING_TIMEOUT = timedelta(seconds=10)
ONLINE_DELAY = timedelta(seconds=90)
WATCH_INTERVAL_MIN = 19.0
WATCH_INTERVAL_MAX = 22.5
SPADE_INTERVAL_SECONDS = 60.0

DEFAULT_PINNED_STREAMERS: list[str] = [
    "kaicenat",
    "xqc",
    "ishowspeed",
    "shroud",
    "tarik",
    "summit1g",
    "lirik",
    "hasanabi",
    "caseoh_",
    "jynxzi",
]

DEFAULT_AUTO_PRIORITY_KEYWORDS: list[str] = [
    "Grand Theft Auto",
    "GTA",
    "Rockstar",
]

DEFAULT_PRIORITY_GAMES: list[str] = [
    "Grand Theft Auto VI",
    "VALORANT",
    "Rust",
    "Fortnite",
    "Dead by Daylight",
    "Phasmophobia",
    "FragPunk",
]

CALL: int = logging.INFO - 1
logging.addLevelName(CALL, "CALL")

FILE_FORMATTER = logging.Formatter(
    "{asctime}.{msecs:03.0f}:\t{levelname:>7}:\t{message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def _merge_vars(base_vars: JsonType, vars_dict: JsonType) -> None:
    for k, v in vars_dict.items():
        if k not in base_vars:
            base_vars[k] = v
        elif isinstance(v, dict):
            if isinstance(base_vars[k], dict):
                _merge_vars(base_vars[k], v)
            elif base_vars[k] is Ellipsis:
                base_vars[k] = v
            else:
                raise RuntimeError(f"Var is a dict, base is not: '{k}'")
        elif isinstance(base_vars[k], dict):
            raise RuntimeError(f"Base is a dict, var is not: '{k}'")
        else:
            base_vars[k] = v
    for k, v in base_vars.items():
        if v is Ellipsis:
            raise RuntimeError(f"Unspecified variable: '{k}'")


class ClientInfo:
    def __init__(self, client_url: URL, client_id: str, user_agents: str | list[str]) -> None:
        self.CLIENT_URL: URL = client_url
        self.CLIENT_ID: str = client_id
        self.USER_AGENTS: list[str] = (
            list(user_agents) if isinstance(user_agents, list) else [user_agents]
        )
        self.USER_AGENT: str = random.choice(self.USER_AGENTS)

    def pick_user_agent(self) -> str:
        return random.choice(self.USER_AGENTS)

    def __iter__(self):
        return iter((self.CLIENT_URL, self.CLIENT_ID, self.USER_AGENT))


class ClientType:
    WEB = ClientInfo(
        URL("https://www.twitch.tv"),
        "kimne78kx3ncx6brgo4mv6wki5h1ko",
        [
            (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
            ),
            (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"
            ),
            (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
            ),
        ],
    )
    SMARTBOX = ClientInfo(
        URL("https://android.tv.twitch.tv"),
        "ue6666qo983tsx6so1t0vnawi233wa",
        [
            (
                "Mozilla/5.0 (Linux; Android 7.1; Smart Box C1) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
            ),
            (
                "Mozilla/5.0 (Linux; Android 11; SHIELD Android TV) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"
            ),
            (
                "Mozilla/5.0 (Linux; Android 12; Chromecast HD) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
            ),
            (
                "Mozilla/5.0 (Linux; Android 10; MiBOX4) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
            ),
            (
                "Mozilla/5.0 (Linux; Android 12; BRAVIA 4K VH2) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
            ),
            (
                "Mozilla/5.0 (Linux; Android 11; AFTMM Build/NS6297) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"
            ),
        ],
    )


class State(Enum):
    IDLE = auto()
    INVENTORY_FETCH = auto()
    GAMES_UPDATE = auto()
    CHANNELS_CLEANUP = auto()
    CHANNELS_FETCH = auto()
    CHANNEL_SWITCH = auto()
    RESTART = auto()
    EXIT = auto()


class PriorityMode(Enum):
    PRIORITY_ONLY = 0
    ENDING_SOONEST = 1
    LOW_AVBL_FIRST = 2


class GQLQuery(JsonType):
    def __init__(self, query: str, g64data: str) -> None:
        super().__init__(
            query=query,
            variables={
                "input": {
                    "data": g64data,
                    "repository": "twilight",
                    "encoding": "GZIP_B64",
                }
            },
        )


class GQLPersistedQuery(JsonType):
    def __init__(self, name: str, sha256: str, *, variables: JsonType | None = None) -> None:
        super().__init__(
            operationName=name,
            extensions={
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": sha256,
                }
            },
        )
        if variables is not None:
            self.__setitem__("variables", variables)

    def with_variables(self, variables: JsonType) -> GQLPersistedQuery:
        modified = deepcopy(self)
        if "variables" in self:
            existing_variables: JsonType = modified["variables"]
            _merge_vars(existing_variables, variables)
        else:
            modified["variables"] = variables
        return modified


GQL_QUERIES: dict[str, GQLPersistedQuery] = {
    "GetStreamInfo": GQLPersistedQuery(
        "VideoPlayerStreamInfoOverlayChannel",
        "198492e0857f6aedead9665c81c5a06d67b25b58034649687124083ff288597d",
        variables={"channel": ...},
    ),
    "ClaimCommunityPoints": GQLPersistedQuery(
        "ClaimCommunityPoints",
        "46aaeebe02c99afdf4fc97c7c0cba964124bf6b0af229395f1f6d1feed05b3d0",
        variables={"input": {"claimID": ..., "channelID": ...}},
    ),
    "ClaimDrop": GQLPersistedQuery(
        "DropsPage_ClaimDropRewards",
        "a455deea71bdc9015b78eb49f4acfbce8baa7ccbedd28e549bb025bd0f751930",
        variables={"input": {"dropInstanceID": ...}},
    ),
    "ChannelPointsContext": GQLPersistedQuery(
        "ChannelPointsContext",
        "374314de591e69925fce3ddc2bcf085796f56ebb8cad67a0daa3165c03adc345",
        variables={"channelLogin": ...},
    ),
    "Inventory": GQLPersistedQuery(
        "Inventory",
        "8337eb8541b314040b0edde0c09c5c7a2783ba1960aa9edfbf3bac16d0fec404",
        variables={"fetchRewardCampaigns": False},
    ),
    "CurrentDrop": GQLPersistedQuery(
        "DropCurrentSessionContext",
        "4d06b702d25d652afb9ef835d2a550031f1cf762b193523a92166f40ea3d142b",
        variables={"channelID": ..., "channelLogin": ""},
    ),
    "Campaigns": GQLPersistedQuery(
        "ViewerDropsDashboard",
        "c16bb890cc8ce7647a96ee69cd313d423a378a3dedadf630a1017cde18975feb",
        variables={"fetchRewardCampaigns": False},
    ),
    "CampaignDetails": GQLPersistedQuery(
        "DropCampaignDetails",
        "039277bf98f3130929262cc7c6efd9c141ca3749cb6dca442fc8ead9a53f77c1",
        variables={"channelLogin": ..., "dropID": ...},
    ),
    "AvailableDrops": GQLPersistedQuery(
        "DropsHighlightService_AvailableDrops",
        "782dad0f032942260171d2d80a654f88bdd0c5a9dddc392e9bc92218a0f42d20",
        variables={"channelID": ...},
    ),
    "PlaybackAccessToken": GQLPersistedQuery(
        "PlaybackAccessToken",
        "ed230aa1e33e07eebb8928504583da78a5173989fadfb1ac94be06a04f3cdbe9",
        variables={
            "isLive": True,
            "isVod": False,
            "login": ...,
            "platform": "web",
            "playerType": "site",
            "vodID": "",
        },
    ),
    "GameDirectory": GQLPersistedQuery(
        "DirectoryPage_Game",
        "86bcceb4e8b1a51256ff8eed8bd8aae4acacf80d737efe904f84f3aeadf8cafd",
        variables={
            "limit": 30,
            "slug": ...,
            "imageWidth": 50,
            "includeCostreaming": False,
            "options": {
                "broadcasterLanguages": [],
                "freeformTags": None,
                "includeRestricted": ["SUB_ONLY_LIVE"],
                "recommendationsContext": {"platform": "web"},
                "sort": "RELEVANCE",
                "systemFilters": [],
                "tags": [],
                "requestID": "JIRA-VXP-2397",
            },
            "sortTypeIsRecency": False,
        },
    ),
    "SlugRedirect": GQLPersistedQuery(
        "DirectoryGameRedirect",
        "1f0300090caceec51f33c5e20647aceff9017f740f223c3c532ba6fa59f6b6cc",
        variables={"name": ...},
    ),
    "NotificationsDelete": GQLPersistedQuery(
        "OnsiteNotifications_DeleteNotification",
        "13d463c831f28ffe17dccf55b3148ed8b3edbbd0ebadd56352f1ff0160616816",
        variables={"input": {"id": ""}},
    ),
}


class WebsocketTopic:
    def __init__(
        self,
        category: Literal["User", "Channel"],
        topic_name: str,
        target_id: int,
        process: TopicProcess,
    ) -> None:
        assert isinstance(target_id, int)
        self._id: str = self.as_str(category, topic_name, target_id)
        self._target_id = target_id
        self._process: TopicProcess = process

    @classmethod
    def as_str(
        cls, category: Literal["User", "Channel"], topic_name: str, target_id: int
    ) -> str:
        return f"{WEBSOCKET_TOPICS[category][topic_name]}.{target_id}"

    def __call__(self, message: JsonType):
        return self._process(self._target_id, message)

    def __str__(self) -> str:
        return self._id

    def __repr__(self) -> str:
        return f"Topic({self._id})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, WebsocketTopic):
            return self._id == other._id
        if isinstance(other, str):
            return self._id == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self._id))


WEBSOCKET_TOPICS: dict[str, dict[str, str]] = {
    "User": {
        "Presence": "presence",
        "Drops": "user-drop-events",
        "Notifications": "onsite-notifications",
        "CommunityPoints": "community-points-user-v1",
    },
    "Channel": {
        "Drops": "channel-drop-events",
        "StreamState": "video-playback-by-id",
        "StreamUpdate": "broadcast-settings-update",
        "CommunityPoints": "community-points-channel-v1",
    },
}
