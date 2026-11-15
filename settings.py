"""Global orchestrator settings and isolated per-account profile configuration."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from constants import (
    DEFAULT_AUTO_PRIORITY_KEYWORDS,
    DEFAULT_PINNED_STREAMERS,
    DEFAULT_PRIORITY_GAMES,
    GLOBAL_SETTINGS_PATH,
    PROFILES_DIR,
    ClientType,
    PriorityMode,
)
from utils import CHARS_HEX_LOWER, create_nonce, json_load, json_save


@dataclass
class GlobalSettings:
    pinned_streamers: list[str] = field(default_factory=lambda: list(DEFAULT_PINNED_STREAMERS))
    auto_priority_keywords: list[str] = field(
        default_factory=lambda: list(DEFAULT_AUTO_PRIORITY_KEYWORDS)
    )
    priority_games: list[str] = field(default_factory=lambda: list(DEFAULT_PRIORITY_GAMES))
    exclude_games: list[str] = field(default_factory=list)
    favorite_campaigns: list[str] = field(default_factory=list)
    radar_preset: str = "default"
    language: str = "ru"
    priority_mode: str = "ENDING_SOONEST"
    enable_badges_emotes: bool = True
    available_drops_check: bool = True
    standby_on_pinned: bool = True
    radar_interval_sec: int = 120
    hybrid_watch_spade: bool = True
    connection_quality: int = 1

    @property
    def priority_mode_enum(self) -> PriorityMode:
        try:
            return PriorityMode[self.priority_mode]
        except KeyError:
            return PriorityMode.ENDING_SOONEST

    @classmethod
    def load(cls, path: Path = GLOBAL_SETTINGS_PATH) -> GlobalSettings:
        defaults = asdict(cls())
        raw = json_load(path, defaults, merge=True)
        instance = cls(
            pinned_streamers=[
                s.strip().lower() for s in raw.get("pinned_streamers", []) if s and s.strip()
            ],
            auto_priority_keywords=[
                k.strip() for k in raw.get("auto_priority_keywords", []) if k and k.strip()
            ],
            priority_games=[g.strip() for g in raw.get("priority_games", []) if g and g.strip()],
            exclude_games=[g.strip() for g in raw.get("exclude_games", []) if g and g.strip()],
            favorite_campaigns=[
                str(c).strip() for c in raw.get("favorite_campaigns", []) if c and str(c).strip()
            ],
            radar_preset=str(raw.get("radar_preset", "default") or "default").strip(),
            language=(
                str(raw.get("language", "ru") or "ru").strip().lower()
                if str(raw.get("language", "ru") or "ru").strip().lower() in ("ru", "en")
                else "ru"
            ),
            priority_mode=str(raw.get("priority_mode", "ENDING_SOONEST")),
            enable_badges_emotes=bool(raw.get("enable_badges_emotes", True)),
            available_drops_check=bool(raw.get("available_drops_check", True)),
            standby_on_pinned=bool(raw.get("standby_on_pinned", True)),
            radar_interval_sec=max(30, int(raw.get("radar_interval_sec", 120))),
            hybrid_watch_spade=bool(raw.get("hybrid_watch_spade", True)),
            connection_quality=max(1, min(6, int(raw.get("connection_quality", 1)))),
        )
        return instance

    def save(self, path: Path = GLOBAL_SETTINGS_PATH) -> None:
        json_save(path, asdict(self), sort=True)

    def matches_auto_priority(self, text: str) -> bool:
        if not text:
            return False
        low = text.lower()
        return any(kw.lower() in low for kw in self.auto_priority_keywords if kw)


@dataclass
class AccountProfileConfig:
    name: str
    enabled: bool = True
    proxy: str = ""
    device_id: str = ""
    session_id: str = ""
    user_agent: str = ""
    user_id: int | None = None
    username: str = ""

    def ensure_identity(self) -> bool:
        """Populate missing unique fingerprint fields and return True if mutated."""
        mutated = False
        if not self.device_id:
            self.device_id = create_nonce(CHARS_HEX_LOWER, 32)
            mutated = True
        if not self.session_id:
            self.session_id = create_nonce(CHARS_HEX_LOWER, 16)
            mutated = True
        if not self.user_agent:
            self.user_agent = ClientType.SMARTBOX.pick_user_agent()
            mutated = True
        return mutated

    @classmethod
    def profile_dir(cls, name: str, base_dir: Path = PROFILES_DIR) -> Path:
        return base_dir / name

    @classmethod
    def config_path(cls, name: str, base_dir: Path = PROFILES_DIR) -> Path:
        return cls.profile_dir(name, base_dir) / "account.json"

    @classmethod
    def cookies_path(cls, name: str, base_dir: Path = PROFILES_DIR) -> Path:
        return cls.profile_dir(name, base_dir) / "cookies.jar"

    @classmethod
    def log_path(cls, name: str, base_dir: Path = PROFILES_DIR) -> Path:
        return cls.profile_dir(name, base_dir) / "log.txt"

    @classmethod
    def load(cls, name: str, base_dir: Path = PROFILES_DIR) -> AccountProfileConfig:
        path = cls.config_path(name, base_dir)
        defaults: dict[str, Any] = {
            "name": name,
            "enabled": True,
            "proxy": "",
            "device_id": "",
            "session_id": "",
            "user_agent": "",
            "user_id": None,
            "username": "",
        }
        if path.exists():
            raw = json_load(path, defaults, merge=False)
        else:
            raw = dict(defaults)
        cfg = cls(
            name=name,
            enabled=bool(raw.get("enabled", True)),
            proxy=str(raw.get("proxy", "") or "").strip(),
            device_id=str(raw.get("device_id", "") or "").strip(),
            session_id=str(raw.get("session_id", "") or "").strip(),
            user_agent=str(raw.get("user_agent", "") or "").strip(),
            user_id=int(raw["user_id"]) if raw.get("user_id") is not None else None,
            username=str(raw.get("username", "") or "").strip(),
        )
        if cfg.ensure_identity() or not path.exists():
            cfg.save(base_dir)
        return cfg

    def save(self, base_dir: Path = PROFILES_DIR) -> None:
        path = self.config_path(self.name, base_dir)
        json_save(path, asdict(self), sort=True)
