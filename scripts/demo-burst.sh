#!/usr/bin/env bash
# Burst demo (quickstart #11): submit BURST mixed-priority SMS against the running stack, then show
# how the provider saw them: per-second rate (<= RATE_LIMIT_PER_SECOND) and priority order.
#
# Until the batch endpoint lands (US4) this uses concurrent single creates.
# Usage: scripts/demo-burst.sh [BURST]   env: API_URL (http://localhost:8080), MOCK_URL (http://localhost:8090)
set -euo pipefail

BURST=${1:-5000}
API_URL=${API_URL:-http://localhost:8080}
MOCK_URL=${MOCK_URL:-http://localhost:8090}
PARALLEL=${PARALLEL:-50}

command -v python3 >/dev/null || { echo "python3 is required for the summary" >&2; exit 1; }
curl -fsS "$API_URL/readyz" >/dev/null || { echo "API not ready at $API_URL (run: docker compose up -d)" >&2; exit 1; }
curl -fsS -X DELETE "$MOCK_URL/received" >/dev/null

echo "Submitting $BURST SMS (10% high, 30% normal, 60% low) with $PARALLEL parallel clients..."
start=$(date +%s)
seq 1 "$BURST" | awk '{ p = ($1 % 10 == 0) ? "high" : (($1 % 10 <= 3) ? "normal" : "low"); print $1, p }' |
  xargs -P "$PARALLEL" -n 2 sh -c '
    curl -fsS -o /dev/null -X POST "$0/api/v1/notifications" \
      -H "Content-Type: application/json" -H "X-Correlation-ID: burst-$1" \
      -d "{\"recipient\":\"+905551234567\",\"channel\":\"sms\",\"content\":\"burst $1\",\"priority\":\"$2\"}"
  ' "$API_URL"
echo "Accepted $BURST in $(( $(date +%s) - start ))s. Waiting for delivery..."

while :; do
  n=$(curl -fsS "$MOCK_URL/received?channel=sms" | python3 -c 'import sys,json; print(json.load(sys.stdin)["count"])')
  printf '\r  delivered %d / %d' "$n" "$BURST"
  [ "$n" -ge "$BURST" ] && break
  sleep 2
done
echo

curl -fsS "$MOCK_URL/received?channel=sms" | python3 -c '
import sys, json, collections, re
from datetime import datetime
def parse(s):
    # Go emits 0-9 fractional digits; older Pythons only accept exactly 6 in fromisoformat.
    m = re.match(r"(.*?)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)$", s)
    tz = "+00:00" if m.group(3) == "Z" else m.group(3)
    return datetime.fromisoformat(m.group(1) + tz).timestamp() + float("0." + (m.group(2) or "0"))
items = json.load(sys.stdin)["items"]
ts = sorted(parse(i["at"]) for i in items)
per_sec = collections.Counter(int(t) for t in ts)
lo = 0; peak = 0
for hi in range(len(ts)):
    while ts[hi] - ts[lo] >= 1.0: lo += 1
    peak = max(peak, hi - lo + 1)
span = ts[-1] - ts[0] if len(ts) > 1 else 0
print(f"provider received {len(ts)} in {span:.1f}s: {len(ts)/span if span else 0:.1f}/s on average")
print(f"peak in any sliding 1 s window: {peak}")
print("busiest calendar seconds:", ", ".join(str(v) for _, v in per_sec.most_common(5)))
start = int(ts[0])
print("per second:", " ".join(str(per_sec.get(s, 0)) for s in range(start, int(ts[-1]) + 1)))
'
