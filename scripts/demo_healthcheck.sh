#!/usr/bin/env bash
# ============================================================================
#  DataLink — DEMO HEALTHCHECK
#  Run anytime to verify the prototype demo on :8000 is alive and shippable.
#  Use when you need to confirm before showing it to anyone.
#
#    bash scripts/demo_healthcheck.sh
#
#  Exit code 0 = green-light demo · Exit code 1 = problems found
# ============================================================================
set -u

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

fail=0
ok()   { printf "  [OK]   %s\n" "$1"; }
bad()  { printf "  [FAIL] %s\n" "$1"; fail=1; }
warn() { printf "  [WARN] %s\n" "$1"; }

echo "============================================================"
echo " DataLink Demo Healthcheck — $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"

# 1. .env present
echo ""
echo "1. .env file present + has required vars"
if [ -f .env ]; then
  ok ".env exists at $REPO_ROOT/.env"
  for var in SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_PASSWORD \
             ANTHROPIC_API_KEY VOYAGE_API_KEY; do
    if grep -q "^${var}=" .env; then
      ok "  ${var} is set"
    else
      bad "  ${var} MISSING in .env"
    fi
  done
else
  bad ".env file MISSING — copy from backup or .env.example"
fi

# 2. Docker container healthy
echo ""
echo "2. control_tower container"
state=$(docker inspect -f '{{.State.Status}}/{{.State.Health.Status}}' datalink-control-tower 2>/dev/null || echo "missing")
if [ "$state" = "running/healthy" ]; then
  ok "datalink-control-tower is running/healthy"
elif echo "$state" | grep -q "running"; then
  warn "datalink-control-tower is running but health: ${state#*/}.  Wait ~60s and re-run."
elif [ "$state" = "missing" ]; then
  bad "datalink-control-tower not found.  Run: docker compose up -d"
else
  bad "datalink-control-tower state: $state.  Run: docker restart datalink-control-tower"
fi

# 3. HTTP endpoint
echo ""
echo "3. HTTP /_stcore/health"
http=$(curl -sS -o /dev/null -w "%{http_code}" --max-time 5 http://localhost:8000/_stcore/health 2>/dev/null || echo "000")
if [ "$http" = "200" ]; then
  ok "http://localhost:8000/_stcore/health -> 200"
else
  bad "http://localhost:8000/_stcore/health -> $http (expected 200)"
fi

# 4. Snowflake reachable
echo ""
echo "4. Snowflake connectivity (WFDIQAH-WCA64552 account)"
if [ -f .env ]; then
  set -a; source .env; set +a
  out=$(.venv/bin/python -c "
from datalink.ui._query import _build_backend
try:
    r = list(_build_backend(readonly=True).query('SELECT CURRENT_ACCOUNT() a, CURRENT_USER() u'))[0]
    a = r.get('A') or r.get('a')
    u = r.get('U') or r.get('u')
    print(f'OK account={a} user={u}')
except Exception as exc:
    print(f'FAIL {type(exc).__name__}: {str(exc)[:100]}')
" 2>&1 | tail -1)
  if [[ "$out" == OK* ]]; then
    ok "$out"
  else
    bad "$out"
  fi
fi

# 5. Catalog has data?
echo ""
echo "5. Catalog state (informational — not a fail)"
if [ -f .env ]; then
  out=$(.venv/bin/python -c "
from datalink.ui._query import _build_backend
wh = _build_backend(readonly=True)
n_ds = list(wh.query('SELECT COUNT(*) c FROM CONTROL.global_bronze_catalog_datasets'))[0]['c'] or 0
n_fd = list(wh.query('SELECT COUNT(*) c FROM CONTROL.global_bronze_catalog_fields'))[0]['c'] or 0
print(f'catalog: {n_ds} datasets, {n_fd} fields')
" 2>&1 | tail -1)
  if [[ "$out" == catalog:* ]]; then
    if [[ "$out" == *"0 datasets"* ]]; then
      warn "$out -> demo starts from empty.  Upload xlsx during demo or run scripts/load_product_catalog.py to pre-seed."
    else
      ok "$out -> demo-ready"
    fi
  fi
fi

# 6. Sample template files present
echo ""
echo "6. Sample template files"
n=$(ls data/sample/canonical_templates/ 2>/dev/null | wc -l)
if [ "$n" -ge 12 ]; then
  ok "12+ canonical sample templates present"
else
  bad "Missing canonical sample templates (found $n).  Run: python scripts/build_canonical_samples.py"
fi

# 7. docx + pdf libs in container (for Client Onboarding demo)
echo ""
echo "7. Container has python-docx + pypdf (Client Onboarding spec uploads)"
docker exec datalink-control-tower python -c "import docx, pypdf" 2>/dev/null \
  && ok "python-docx + pypdf installed in container" \
  || bad "python-docx OR pypdf missing.  Restart container to re-install via entrypoint."

# 8. Port 8000 owned by container (no conflicts)
echo ""
echo "8. Port 8000 ownership (no conflicts with other apps)"
listening=$(ss -ltn 'sport = :8000' 2>/dev/null | tail -n +2 | wc -l)
if [ "$listening" -ge 1 ]; then
  ok "Port 8000 listening (Docker is forwarding it)"
else
  bad "Port 8000 NOT listening.  Container may be down or not port-mapped."
fi

# 9. Git tag for restorability
echo ""
echo "9. Demo-stable git tag"
if git tag -l demo-stable-2026-05-20 | grep -q .; then
  ok "Tag 'demo-stable-2026-05-20' exists — restore with: git checkout demo-stable-2026-05-20"
else
  warn "Demo-stable tag missing.  Create with: git tag -a demo-stable-2026-05-20 -m 'demo snapshot'"
fi

echo ""
echo "============================================================"
if [ $fail -eq 0 ]; then
  echo " [OK] Demo is READY — http://localhost:8000"
else
  echo " [FAIL] Demo has issues — fix them before showing."
fi
echo "============================================================"
exit $fail
