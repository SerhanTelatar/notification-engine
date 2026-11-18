#!/usr/bin/env bash
# Load check for SC-001: TOTAL single POST /api/v1/notifications with valid, unique SMS payloads,
# fired by vegeta at RATE against the running `docker compose` stack. Reports p50/p95/p99 and
# whether p95 < 200 ms.
#
# vegeta runs inside the stack's Docker network (target http://api:8080), built from a pinned
# version in the pinned Go image, so nothing is installed on the host.
#
# Usage: scripts/loadtest.sh   env: TOTAL (10000), RATE (500), VEGETA_VERSION (v12.12.0)
set -euo pipefail

TOTAL=${TOTAL:-10000}
RATE=${RATE:-500}
VEGETA_VERSION=${VEGETA_VERSION:-v12.12.0}
GO_IMAGE=${GO_IMAGE:-golang:1.27.4}
SLO_MS=200

command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }
api_id=$(docker compose ps -q api)
[ -n "$api_id" ] || { echo "the api service isn't running (run: docker compose up -d)" >&2; exit 1; }
network=$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$api_id")

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
duration_s=$(( (TOTAL + RATE - 1) / RATE ))
run=$(date +%s)

# One JSON target per request: a unique recipient, content and idempotency key each, so every
# request creates a new notification (no idempotent replays) and reruns don't collide.
python3 - "$TOTAL" "$run" > "$work/targets.jsonl" <<'PY'
import base64, json, sys
total, run = int(sys.argv[1]), sys.argv[2]
for i in range(total):
    body = {"recipient": f"+9055{i % 10_000_000:08d}", "channel": "sms",
            "content": f"load {run}-{i}", "idempotency_key": f"load-{run}-{i}"}
    print(json.dumps({"method": "POST", "url": "http://api:8080/api/v1/notifications",
                      "header": {"Content-Type": ["application/json"], "X-Correlation-ID": [f"load-{run}"]},
                      "body": base64.b64encode(json.dumps(body).encode()).decode()}))
PY

echo "Firing $TOTAL creates at $RATE/s (~${duration_s}s) with vegeta $VEGETA_VERSION..."
docker run --rm --network "$network" -v "$work":/work -v ne-gomod:/go/pkg/mod -v ne-gobuild:/root/.cache/go-build \
  "$GO_IMAGE" sh -c "
    go install github.com/tsenart/vegeta/v12@$VEGETA_VERSION >/dev/null 2>&1 &&
    vegeta attack -format=json -targets=/work/targets.jsonl -rate=$RATE/s -duration=${duration_s}s -timeout=10s \
      > /work/results.bin &&
    vegeta report -type=json /work/results.bin > /work/report.json &&
    vegeta report /work/results.bin"

python3 - "$work/report.json" "$SLO_MS" <<'PY'
import json, sys
r, slo = json.load(open(sys.argv[1])), int(sys.argv[2])
lat = {k: r["latencies"][k] / 1e6 for k in ("50th", "95th", "99th", "max")}
codes = r.get("status_codes", {})
ok = lat["95th"] < slo and set(codes) == {"202"}
print()
print(f"requests={r['requests']} rate={r['rate']:.0f}/s success={r['success']*100:.2f}% status_codes={codes}")
print(f"p50={lat['50th']:.1f}ms p95={lat['95th']:.1f}ms p99={lat['99th']:.1f}ms max={lat['max']:.1f}ms")
print(f"SC-001 (p95 < {slo} ms, every request 202): {'PASS' if ok else 'FAIL'}")
sys.exit(0 if ok else 1)
PY
