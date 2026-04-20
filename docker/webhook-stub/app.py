"""Minimal stdlib-only HTTP server that receives webhook POSTs and logs them.

Local replacement for Microsoft Teams incoming-webhook / SMTP during demos.
Endpoints:
  GET  /health   -> "ok"
  POST /notify   -> accept any JSON body; append to /app/logs/webhook.jsonl
                    and print to stdout so `docker logs datalink-webhook-stub`
                    shows the stream.
"""

from __future__ import annotations

import json
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG_DIR = "/app/logs"
LOG_PATH = os.path.join(LOG_DIR, "webhook.jsonl")
PORT = int(os.environ.get("PORT", "9000"))


class Handler(BaseHTTPRequestHandler):
    server_version = "datalink-webhook-stub/0.1"

    def log_message(self, fmt: str, *args: object) -> None:  # quieter default logs
        sys.stderr.write("[access] " + (fmt % args) + "\n")

    def do_GET(self) -> None:
        if self.path == "/health":
            self._ok("ok")
            return
        if self.path == "/" or self.path == "/inbox":
            self._inbox()
            return
        if self.path == "/api/messages":
            self._api_messages()
            return
        self._not_found()

    # ---- pretty inbox view (the executive-visible "Teams replacement") ---

    def _inbox(self) -> None:
        """Render the last 50 notifications as a DataLink-branded HTML page."""
        messages = self._recent_messages(limit=50)
        rows_html = []
        for m in messages:
            p = m.get("payload", {})
            channel = p.get("channel") or p.get("recipient") or "teams"
            severity = (p.get("severity") or p.get("priority") or "INFO").upper()
            title = p.get("title") or p.get("subject") or "(no title)"
            body = p.get("body") or p.get("message") or json.dumps(p)
            received = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(m.get("received_at", 0)))
            sev_color = {
                "CRITICAL": "#b91c1c",
                "HIGH": "#d97706",
                "MEDIUM": "#b45309",
                "LOW": "#047857",
                "INFO": "#0a1a3e",
            }.get(severity, "#0a1a3e")
            rows_html.append(
                f'<tr><td class="ts">{received}</td>'
                f'<td><span class="ch">{channel}</span></td>'
                f'<td><span class="sev" style="background:{sev_color}">{severity}</span></td>'
                f'<td><strong>{self._esc(title)}</strong><br><span class="body">{self._esc(str(body))[:300]}</span></td></tr>'
            )
        rows_joined = "\n".join(rows_html) or (
            '<tr><td colspan="4" class="empty">No notifications yet. Trigger a pipeline breach '
            "from the Control Tower to see the Reporting agent deliver here.</td></tr>"
        )
        html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>DataLink - Webhook Inbox</title>
<meta http-equiv="refresh" content="5">
<style>
 body{{font-family:system-ui,-apple-system,sans-serif;background:#f9fafc;color:#1a2a4e;margin:0;padding:2rem}}
 h1{{color:#0a1a3e;border-bottom:3px solid #d4af37;padding-bottom:.4rem;margin:0 0 1rem}}
 .lead{{color:#4a5a7e;margin:0 0 1.5rem}}
 table{{width:100%;border-collapse:collapse;background:#fff;box-shadow:0 1px 3px rgba(0,0,0,.08)}}
 th{{background:#0a1a3e;color:#d4af37;text-align:left;padding:.75rem}}
 td{{padding:.75rem;border-bottom:1px solid #e8edf5;vertical-align:top}}
 td.ts{{font-family:ui-monospace,monospace;font-size:.85rem;color:#4a5a7e;white-space:nowrap}}
 .ch{{display:inline-block;padding:.1rem .5rem;background:#e8edf5;color:#0a1a3e;border-radius:3px;font-size:.8rem;font-family:ui-monospace,monospace}}
 .sev{{display:inline-block;padding:.1rem .5rem;color:#fff;border-radius:3px;font-size:.8rem;font-weight:bold}}
 .body{{color:#4a5a7e;font-size:.9rem}}
 .empty{{text-align:center;padding:3rem;color:#4a5a7e}}
 .foot{{margin-top:1.5rem;color:#4a5a7e;font-size:.85rem}}
</style></head><body>
<h1>DataLink Notification Inbox <span style="font-size:1rem;color:#d4af37">(auto-refreshes every 5s)</span></h1>
<p class="lead">Every <code>Notifier.notify(...)</code> call from the Reporting agent lands here.
Production replaces this stub with Microsoft Teams / SMTP via the Notifier Protocol - no code changes.</p>
<table>
  <thead><tr><th style="width:10rem">Received</th><th style="width:8rem">Channel</th><th style="width:6rem">Severity</th><th>Message</th></tr></thead>
  <tbody>
    {rows_joined}
  </tbody>
</table>
<p class="foot">Showing last {len(messages)} messages. <em>Prepared by Team DataLink.</em></p>
</body></html>
"""
        payload = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _api_messages(self) -> None:
        """JSON feed for the Control Tower to poll."""
        messages = self._recent_messages(limit=50)
        body = json.dumps({"messages": messages}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    @staticmethod
    def _recent_messages(*, limit: int = 50) -> list[dict]:
        if not os.path.exists(LOG_PATH):
            return []
        try:
            with open(LOG_PATH, encoding="utf-8") as f:
                lines = f.readlines()[-limit:]
        except OSError:
            return []
        out: list[dict] = []
        for line in reversed(lines):  # newest first
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return out

    @staticmethod
    def _esc(s: str) -> str:
        return (
            s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
        )

    def do_POST(self) -> None:
        if self.path != "/notify":
            self._not_found()
            return
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            self._bad_request("invalid JSON")
            return
        record = {"received_at": time.time(), "payload": payload}
        line = json.dumps(record, ensure_ascii=False)
        os.makedirs(LOG_DIR, exist_ok=True)
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
        print(f"[webhook] {line}", flush=True)
        self._ok('{"status":"received"}')

    # ---- helpers --------------------------------------------------------

    def _ok(self, body: str) -> None:
        payload = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _not_found(self) -> None:
        self.send_response(404)
        self.end_headers()

    def _bad_request(self, msg: str) -> None:
        payload = msg.encode("utf-8")
        self.send_response(400)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def main() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[webhook] listening on 0.0.0.0:{PORT}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("[webhook] shutting down", flush=True)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
