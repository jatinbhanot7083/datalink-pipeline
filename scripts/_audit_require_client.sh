#!/usr/bin/env bash
# List every require_client() call site in datalink/ui/pages/
cd /home/jatin/dev/DataPipelinesWithGX
for f in datalink/ui/pages/*.py; do
    HITS=$(grep -n "require_client" "$f" 2>/dev/null)
    if [ -n "$HITS" ]; then
        echo "=== $f ==="
        echo "$HITS"
    fi
done
