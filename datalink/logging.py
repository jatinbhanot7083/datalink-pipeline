"""Structured logging — JSON in prod, human-friendly in local.

PHI safety: never log row-level data. The `_phi_scrub` processor drops any
log field whose key matches a known-PHI name (member_id_value, dob, ssn, etc.).
This is defense-in-depth — the real PHI boundary is enforced in datalink.phi.guard,
which blocks PHI from ever reaching an LLM call.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

from datalink.config.models import LogFormat

# Keys we will never log the value of, even if a dev accidentally passes one.
# Drop silently — replace with "[REDACTED:phi]".
_PHI_VALUE_KEYS: frozenset[str] = frozenset(
    {
        "ssn",
        "social_security_number",
        "member_name",
        "patient_name",
        "subscriber_name",
        "first_name",
        "last_name",
        "dob",
        "date_of_birth",
        "address",
        "street",
        "phone",
        "email",
        "tin",
    }
)


def _phi_scrub(
    _logger: Any, _method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    for key in list(event_dict.keys()):
        if key.lower() in _PHI_VALUE_KEYS:
            event_dict[key] = "[REDACTED:phi]"
    return event_dict


def configure_logging(level: str = "INFO", fmt: LogFormat | str = LogFormat.CONSOLE) -> None:
    """Configure structlog + stdlib logging. Idempotent."""
    fmt_enum = LogFormat(fmt) if isinstance(fmt, str) else fmt

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
        force=True,
    )

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _phi_scrub,
    ]

    if fmt_enum is LogFormat.JSON:
        renderer: Any = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=False)

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)  # type: ignore[no-any-return]
