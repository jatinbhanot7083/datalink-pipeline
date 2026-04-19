"""FileNotifier — append JSON-Lines notifications to a local file.

This is a fully-working adapter used for local demos and tests. Every `notify()`
call appends one JSON object per line. Readable by `tail -f` and `jq`.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from datalink.config.models import NotifierConfig


class FileNotifier:
    def __init__(self, cfg: NotifierConfig) -> None:
        self._path = Path(cfg.path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def notify(
        self,
        channel: str,
        severity: str,
        title: str,
        body: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        record = {
            "ts": time.time(),
            "channel": channel,
            "severity": severity,
            "title": title,
            "body": body,
            "metadata": metadata or {},
        }
        line = json.dumps(record, ensure_ascii=False, default=str)
        # open-append-close per call keeps things simple and crash-safe.
        # Use "\n" (not os.linesep) to avoid double-CR on Windows text mode.
        with self._path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
