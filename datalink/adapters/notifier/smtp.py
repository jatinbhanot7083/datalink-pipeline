"""SmtpNotifier — outbound email via SMTP."""

from __future__ import annotations

from typing import Any

from datalink.config.models import NotifierConfig


class SmtpNotifier:
    def __init__(self, cfg: NotifierConfig) -> None:
        self._cfg = cfg

    def notify(
        self,
        channel: str,
        severity: str,
        title: str,
        body: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        raise NotImplementedError("SmtpNotifier lands in Phase 5 (Reporting Agent)")
