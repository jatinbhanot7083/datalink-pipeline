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
        self._not_found()

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
