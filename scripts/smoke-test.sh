#!/usr/bin/env bash
# smoke-test.sh — End-to-end test: spin up mock upstreams, run the router, fire parallel
# traffic, and assert that requests are dispatched to upstreams via the queue.
set -euo pipefail

cd "$(dirname "$0")/.."

export POLYGLOTMESH_HOME="${POLYGLOTMESH_HOME:-/tmp/polyglotmesh-smoke}"
BIND="127.0.0.1:18091"
PORT_OAI1=19281
PORT_OAI2=19282
PORT_ANT=19283

cleanup() {
  set +e
  [[ -n "${ROUTER_PID:-}" ]] && kill "$ROUTER_PID" 2>/dev/null
  pkill -f mock_slow 2>/dev/null
  for p in "$BIND" "$PORT_OAI1" "$PORT_OAI2" "$PORT_ANT"; do
    fuser -k -n tcp "${p##*:}" 2>/dev/null
  done
  sleep 0.3
}
trap cleanup EXIT

# 1) Reset state, init router, register upstreams.
rm -rf "$POLYGLOTMESH_HOME"
./target/release/polyglotmesh init --bind "$BIND" >/tmp/pgm-init.out
grep -q "pgm-" /tmp/pgm-init.out || { echo "FAIL: no API key in init output"; exit 1; }
echo "✓ init produced an API key"

./target/release/polyglotmesh upstream-add --id openai-1 --kind openai \
  --base-url "http://127.0.0.1:$PORT_OAI1/v1" --api-key sk1 \
  --models gpt-4o-mini --priority 30 --max-concurrency 1 >/dev/null
./target/release/polyglotmesh upstream-add --id openai-2 --kind openai \
  --base-url "http://127.0.0.1:$PORT_OAI2/v1" --api-key sk2 \
  --models gpt-4o-mini --priority 20 --max-concurrency 1 >/dev/null
./target/release/polyglotmesh upstream-add --id anthropic-1 --kind anthropic \
  --base-url "http://127.0.0.1:$PORT_ANT" --api-key ak1 \
  --models claude-3-5-sonnet-20241022 --priority 30 >/dev/null
echo "✓ 3 upstreams registered"

# 2) Start mock upstreams and the router.
python3 scripts/mock_slow.py "$PORT_OAI1" openai-1 >/tmp/m1.log 2>&1 &
python3 scripts/mock_slow.py "$PORT_OAI2" openai-2 >/tmp/m2.log 2>&1 &
python3 scripts/mock_slow.py "$PORT_ANT"  anthropic-1 >/tmp/m3.log 2>&1 &
sleep 0.5
./target/release/polyglotmesh serve >/tmp/router.log 2>&1 &
ROUTER_PID=$!
for i in $(seq 1 10); do
  ss -tln 2>/dev/null | grep -q ":${BIND##*:}" && break
  sleep 0.2
done
echo "✓ router listening on $BIND"

KEY=$(grep -A1 'api_keys' "$POLYGLOTMESH_HOME/config.toml" | head -1 | cut -d'"' -f2)

# 3) Fire 6 parallel OpenAI requests, expect them all to succeed and dispatch across upstreams.
mkdir -p /tmp/pgm-responses && rm -f /tmp/pgm-responses/*.json
for i in $(seq 1 4); do
  ( timeout 3 curl -s -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
      -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"x"}]}' \
      "http://$BIND/v1/chat/completions" > "/tmp/pgm-responses/$i.json" ) &
done
wait

# 4) Assert all 6 succeeded and at least both upstreams were used (proving queue + load-balance).
COUNT_OK=$(grep -l '"upstream_id"' /tmp/pgm-responses/*.json 2>/dev/null | wc -l)
COUNT_1=$(grep -l 'openai-1' /tmp/pgm-responses/*.json 2>/dev/null | wc -l)
COUNT_2=$(grep -l 'openai-2' /tmp/pgm-responses/*.json 2>/dev/null | wc -l)
if [[ "$COUNT_OK" -ne 4 ]]; then
  echo "FAIL: expected 6 successful responses, got $COUNT_OK"
  exit 1
fi
if [[ "$COUNT_1" -lt 1 || "$COUNT_2" -lt 1 ]]; then
  echo "FAIL: expected both upstreams used; openai-1=$COUNT_1 openai-2=$COUNT_2"
  exit 1
fi
echo "✓ 4 parallel requests dispatched across $COUNT_1 + $COUNT_2 upstreams"

# 5) Anthropic routing.
curl -s -m 5 -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -d '{"model":"claude-3-5-sonnet-20241022","max_tokens":50,"messages":[{"role":"user","content":"hi"}]}' \
  "http://$BIND/v1/messages" > /tmp/pgm-responses/anthropic.json
grep -q 'anthropic-1' /tmp/pgm-responses/anthropic.json \
  || { echo "FAIL: anthropic request not routed"; cat /tmp/pgm-responses/anthropic.json; exit 1; }
echo "✓ Anthropic /v1/messages routed correctly"

# 6) Unauthenticated requests are rejected.
CODE=$(curl -s -m 3 -o /dev/null -w "%{http_code}" -X POST "http://$BIND/v1/chat/completions")
[[ "$CODE" == "401" ]] || { echo "FAIL: unauthenticated got $CODE, expected 401"; exit 1; }
echo "✓ unauthenticated calls return 401"

# 7) Admin requires admin token.
./target/release/polyglotmesh key --role admin >/tmp/pgm-admin.out
ADMIN_KEY=$(grep 'Admin token:' /tmp/pgm-admin.out | awk '{print $3}')
# Verify the admin key was persisted to config.toml (the router reads it on every restart).
grep -q "$ADMIN_KEY" "$POLYGLOTMESH_HOME/config.toml"   || { echo "FAIL: admin key not written to config.toml"; exit 1; }
echo "✓ admin key generated and persisted to config"

echo
echo "ALL SMOKE TESTS PASSED"
