"""Custom exception hierarchy for TwitchMultiMiner."""
from __future__ import annotations


class MinerException(Exception):
    """Base exception class for TwitchMultiMiner."""

    def __init__(self, *args: object) -> None:
        if args:
            super().__init__(*args)
        else:
            super().__init__("Unknown miner error")


class ExitRequest(MinerException):
    """Raised when an account worker is requested to stop."""

    def __init__(self) -> None:
        super().__init__("Worker was requested to exit")


class ReloadRequest(MinerException):
    """Raised when an account worker is requested to reload its session."""

    def __init__(self) -> None:
        super().__init__("Worker was requested to reload")


class RequestException(MinerException):
    """Raised when an HTTP/network request fails unexpectedly."""

    def __init__(self, *args: object) -> None:
        if args:
            super().__init__(*args)
        else:
            super().__init__("Unknown error during request")


class RequestInvalid(RequestException):
    """Raised when a timed request expires during its retry loop."""

    def __init__(self) -> None:
        super().__init__("Request became invalid during its retry loop")


class WebsocketClosed(RequestException):
    """Raised when a PubSub websocket connection closes."""

    def __init__(self, *args: object, received: bool = False) -> None:
        if args:
            super().__init__(*args)
        else:
            super().__init__("Websocket has been closed")
        self.received: bool = received


class LoginException(RequestException):
    """Raised when authentication fails."""

    def __init__(self, *args: object) -> None:
        if args:
            super().__init__(*args)
        else:
            super().__init__("Unknown error during login")


class CaptchaRequired(LoginException):
    """Raised when Twitch demands interactive CAPTCHA verification."""

    def __init__(self) -> None:
        super().__init__("Captcha is required")


class GQLException(RequestException):
    """Raised when a Twitch GQL request returns unrecoverable errors."""

    def __init__(self, message: object) -> None:
        super().__init__(str(message))
