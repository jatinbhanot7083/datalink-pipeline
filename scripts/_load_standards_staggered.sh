#!/usr/bin/env bash
# Load all 6 industry standards one at a time with 75s cooldowns
# between them, to stay under Voyage free-tier rate limits.
# See docs/BOOTSTRAP_RUNBOOK.md gotcha #4.
set -u

CODES=("fhir-r4" "x12" "ncpdp-d0" "cms" "dv2" "hedis")
SLEEP_BETWEEN=75
INITIAL_COOLDOWN=30  # already cooled during script writing

echo "=================================================="
echo " Staggered industry-standards loader"
echo " ${#CODES[@]} standards, ${SLEEP_BETWEEN}s gaps"
echo "=================================================="

if [ "${INITIAL_COOLDOWN}" -gt 0 ]; then
    echo ""
    echo "Initial Voyage cooldown ${INITIAL_COOLDOWN}s..."
    sleep "${INITIAL_COOLDOWN}"
fi

i=0
for code in "${CODES[@]}"; do
    i=$((i+1))
    echo ""
    echo "=== [$i/${#CODES[@]}] Loading $code ==="
    docker exec -e DL_ENV=dev datalink-control-tower \
        python -u /opt/datalink/scripts/load_industry_standards.py --only "${code}" 2>&1 \
        | grep -E "Loading|file_chunked|bulk_upserted|cleared_prior|standard_registry|Traceback|Error|RuntimeError" \
        | tail -15
    rc=$?
    echo "    (exit code $rc)"

    if [ "$i" -lt "${#CODES[@]}" ]; then
        echo "    Sleeping ${SLEEP_BETWEEN}s before next standard..."
        sleep "${SLEEP_BETWEEN}"
    fi
done

echo ""
echo "=================================================="
echo " ALL ${#CODES[@]} STANDARDS PROCESSED"
echo "=================================================="
