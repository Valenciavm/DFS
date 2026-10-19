"""Shared console logging setup for DFSha services."""

from __future__ import annotations

import logging
import os
import sys

_RESET = "\033[0m"
_LEVEL_COLORS = {
    logging.DEBUG: "\033[90m",
    logging.INFO: "\033[36m",
    logging.WARNING: "\033[33;1m",
    logging.ERROR: "\033[31;1m",
    logging.CRITICAL: "\033[97;41;1m",
}


class ConsoleFormatter(logging.Formatter):
    """Adds severity colors when explicitly enabled or writing to a TTY."""

    def __init__(self, *, use_color: bool) -> None:
        super().__init__("%(asctime)s %(levelname)-8s %(name)s: %(message)s")
        self.use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        if not self.use_color:
            return message
        color = _LEVEL_COLORS.get(record.levelno)
        if color is None:
            return message
        return f"{color}{message}{_RESET}"


class SuccessfulProbeFilter(logging.Filter):
    """Hide successful liveness chatter while retaining failed requests."""

    _QUIET_PATHS = frozenset(
        {
            ("GET", "/health"),
            ("POST", "/v1/nodes/heartbeat"),
            ("POST", "/v1/nodes/block-report"),
        }
    )

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno > logging.INFO:
            return True
        if record.name == "httpx":
            message = record.getMessage()
            return not any(
                f"{method} http" in message
                and path in message
                and '"HTTP/1.1 2' in message
                for method, path in self._QUIET_PATHS
            )
        if record.name != "uvicorn.access":
            return True
        args = record.args
        if not isinstance(args, tuple) or len(args) < 5:
            return True
        method, path, status = args[1], str(args[2]).split("?", 1)[0], args[4]
        return not (
            (method, path) in self._QUIET_PATHS
            and isinstance(status, int)
            and 200 <= status < 300
        )


def configure_logging() -> None:
    """Configure service and Uvicorn logs with one consistent console handler."""
    mode = os.getenv("DFSHA_LOG_COLORS", "auto").strip().lower()
    if mode not in {"auto", "always", "never"}:
        raise ValueError("DFSHA_LOG_COLORS must be 'auto', 'always', or 'never'")

    stream = sys.stderr
    is_tty = getattr(stream, "isatty", lambda: False)()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(
        ConsoleFormatter(use_color=mode == "always" or (mode == "auto" and is_tty))
    )
    handler.addFilter(SuccessfulProbeFilter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
