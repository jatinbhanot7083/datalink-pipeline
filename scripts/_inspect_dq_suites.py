"""Quick inventory of CONTROL.dq_suites — what are those 86 rows?"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
_env = ROOT / ".env"
if _env.exists():
    for raw in _env.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))
os.environ.setdefault("DL_ENV", "dev")

from datalink.adapters.factory import build_adapters  # noqa: E402
from datalink.config.loader import load_settings  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA  # noqa: E402

wh = build_adapters(load_settings(env="dev")).warehouse

n = next(iter(wh.query(f"SELECT COUNT(*) c FROM {CONTROL_SCHEMA}.dq_suites")))["c"]
print(f"Total dq_suites: {n}")
print()

print("Breakdown by source:")
for r in wh.query(
    f"SELECT source, COUNT(*) c FROM {CONTROL_SCHEMA}.dq_suites GROUP BY source ORDER BY c DESC"
):
    src = r["source"]
    print(f"  source={src!r:30s}  count={r['c']}")

print()
print("Breakdown by client_id:")
for r in wh.query(
    f"SELECT client_id, COUNT(*) c FROM {CONTROL_SCHEMA}.dq_suites GROUP BY client_id ORDER BY c DESC"
):
    cid = r["client_id"]
    print(f"  client_id={cid!r:30s}  count={r['c']}")

print()
print("Sample suites (first 8):")
for r in list(
    wh.query(
        f"SELECT suite_id, suite_name, client_id, source, status FROM {CONTROL_SCHEMA}.dq_suites LIMIT 8"
    )
):
    print(
        f"  {r['suite_name']:40s}  client={r['client_id']:15s}  source={r['source']:25s}  status={r['status']}"
    )
