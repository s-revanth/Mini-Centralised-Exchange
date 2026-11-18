#!/usr/bin/env bash
# Crash-recovery test for the matching engine.
#
#   ./scripts/crash-recovery-test.sh
#
# WARNING: wipes the engine's state (engine container + engine-data volume)
# before running. Postgres and Redis data are left alone.
#
# Scenario 1 (quiesced crash):
#   place orders -> wait for a snapshot -> place more orders (incl. a fill and
#   a cancel) -> record state -> docker kill -s KILL engine -> restart ->
#   state must be identical (order book depth, open orders, balances).
# Scenario 2 (crash mid-flow):
#   fire a burst of non-crossing orders concurrently and SIGKILL the engine
#   while they are in flight -> restart -> every order the API acknowledged
#   must be resting in the book, no phantom orders, asset totals conserved,
#   and the scenario-1 orders must be untouched.
set -uo pipefail
cd "$(dirname "$0")/.."

API=http://localhost:3000/api/v1
MARKET=SOL_USDC
USERS=(1 2)
BURST=${BURST:-300}
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

log()  { echo "[$(date +%H:%M:%S)] $*"; }
fail() { echo; echo "RESULT: FAIL - $*"; exit 1; }
now_ms() { node -e 'console.log(Date.now())'; }

order() { # user side price qty
  curl -s --max-time 5 -X POST "$API/order" -H 'content-type: application/json' \
    -d "{\"market\":\"$MARKET\",\"userId\":\"$1\",\"side\":\"$2\",\"price\":\"$3\",\"quantity\":\"$4\"}"
}
cancel() { # user orderId
  curl -s --max-time 5 -X DELETE "$API/order" -H 'content-type: application/json' \
    -d "{\"market\":\"$MARKET\",\"userId\":\"$1\",\"orderId\":\"$2\"}"
}
get() { curl -sf --max-time 3 "$API/$1"; }

# Canonical JSON of everything the engine exposes for the test users.
state() {
  local depth b1 b2 o1 o2
  depth=$(get "depth?symbol=$MARKET") || return 1
  b1=$(get "balance?userId=1") || return 1
  b2=$(get "balance?userId=2") || return 1
  o1=$(get "order/open?userId=1&market=$MARKET") || return 1
  o2=$(get "order/open?userId=2&market=$MARKET") || return 1
  jq -S -n --argjson d "$depth" --argjson b1 "$b1" --argjson b2 "$b2" \
    --argjson o1 "$o1" --argjson o2 "$o2" '{
      depth: {bids: ($d.payload.bids|sort), asks: ($d.payload.asks|sort)},
      balances: {"1": $b1, "2": $b2},
      open: {"1": ($o1|sort_by(.orderId)), "2": ($o2|sort_by(.orderId))}
    }'
}

wait_engine() { # waits until the engine answers through the API
  for _ in $(seq 1 300); do
    get "balance?userId=1" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  return 1
}

restart_engine() { # prints wall-clock ms from start to first answered request
  local t0 t1
  t0=$(now_ms)
  docker start engine >/dev/null
  wait_engine || fail "engine did not come back within 30s"
  t1=$(now_ms)
  echo $((t1 - t0))
}

recovery_line() { docker logs engine 2>&1 | grep '\[recovery\] done' | tail -1; }

# ---------------------------------------------------------------- setup
log "building and starting stack"
docker compose up -d --build >"$TMP/compose.log" 2>&1 || { cat "$TMP/compose.log"; fail "docker compose up failed"; }

log "resetting engine state"
docker compose rm -sf engine >/dev/null 2>&1
PROJECT=$(docker compose config --format json | jq -r .name)
docker volume rm "${PROJECT}_engine-data" >/dev/null 2>&1 || true
docker compose up -d engine >/dev/null 2>&1
# api/db crash if Postgres isn't accepting connections yet; start them again until up
for _ in $(seq 1 10); do
  wait_engine && break
  log "api not answering yet, (re)starting api and db"
  docker compose up -d api db >/dev/null 2>&1
done
wait_engine || fail "engine not reachable through API"

# ---------------------------------------------------------------- scenario 1
log "scenario 1: placing resting orders"
order 1 BUY 95 10 >"$TMP/o_bid95"
order 1 BUY 96 5 >/dev/null
order 2 SELL 105 8 >/dev/null
order 2 SELL 104 4 >/dev/null
order 1 BUY 104 3 >/dev/null # partial fill of the 104 ask
BID95=$(jq -r .orderId "$TMP/o_bid95")
[ -n "$BID95" ] && [ "$BID95" != null ] || fail "order placement failed: $(cat "$TMP/o_bid95")"

log "waiting for a periodic snapshot to cover these orders"
for _ in $(seq 1 100); do
  docker logs engine 2>&1 | grep -q '\[snapshot\] wrote' && break
  sleep 0.1
done
docker logs engine 2>&1 | grep -q '\[snapshot\] wrote' \
  && log "snapshot seen: $(docker logs engine 2>&1 | grep '\[snapshot\] wrote' | tail -1)" \
  || log "no snapshot log line seen (engine has no event log / snapshot logging)"

log "placing more orders after the snapshot (fill + cancel)"
order 2 SELL 96 6 >/dev/null # fills user 1's 96 bid, rests 1
order 1 BUY 90 7 >/dev/null
order 2 SELL 110 2 >/dev/null
cancel 1 "$BID95" >/dev/null

PRE=$(state) || fail "could not read pre-crash state"
echo "$PRE" >"$TMP/pre.json"
log "pre-crash state: $(echo "$PRE" | jq -c '{bids: .depth.bids, asks: .depth.asks, open1: (.open["1"]|length), open2: (.open["2"]|length)}')"

log "SIGKILL engine"
docker kill -s KILL engine >/dev/null
RT1=$(restart_engine)
POST=$(state) || fail "could not read post-recovery state"
echo "$POST" >"$TMP/post.json"
log "engine answering ${RT1} ms after docker start"
log "engine says: $(recovery_line || true)"

if [ "$PRE" != "$POST" ]; then
  echo "--- diff (pre vs post) ---"
  diff <(echo "$PRE") <(echo "$POST") | head -40
  fail "scenario 1: state after restart differs from state before crash"
fi
log "scenario 1 PASS: depth, open orders and balances identical"
S1_LINE=$(recovery_line || true)

# ---------------------------------------------------------------- scenario 2
log "scenario 2: firing $BURST concurrent non-crossing orders and killing engine mid-flow"
# user 1 bids at 1..3, user 2 asks at 1001..1003: nothing can match
for i in $(seq 1 "$BURST"); do
  if [ $((i % 2)) -eq 0 ]; then echo "1 BUY $((i % 3 + 1)) 1"; else echo "2 SELL $((1000 + i % 3 + 1)) 1"; fi
done >"$TMP/burst.txt"

export API MARKET
xargs -P 16 -L 1 sh -c 'curl -s --max-time 8 -X POST "$API/order" -H "content-type: application/json" \
  -d "{\"market\":\"$MARKET\",\"userId\":\"$0\",\"side\":\"$1\",\"price\":\"$2\",\"quantity\":\"$3\"}"; echo' \
  <"$TMP/burst.txt" >"$TMP/burst_resp.txt" 2>/dev/null &
BURST_PID=$!

# kill once some (not all) orders have been acknowledged
for _ in $(seq 1 200); do
  [ "$(grep -c orderId "$TMP/burst_resp.txt" || true)" -ge $((BURST / 4)) ] && break
  sleep 0.02
done
# responses already received when we pull the trigger: the engine had applied,
# logged and acknowledged these before it died
cp "$TMP/burst_resp.txt" "$TMP/acked_before_kill.txt"
docker kill -s KILL engine >/dev/null
ACKED_AT_KILL=$(grep -c orderId "$TMP/acked_before_kill.txt" || true)
log "killed engine with $ACKED_AT_KILL/$BURST burst orders acknowledged"

RT2=$(restart_engine)
wait "$BURST_PID" 2>/dev/null
log "engine answering ${RT2} ms after docker start"
log "engine says: $(recovery_line || true)"

# let any requests still queued in Redis drain
sleep 1
POST2=$(state) || fail "could not read state after second recovery"
echo "$POST2" >"$TMP/post2.json"

ACKED=$(jq -r 'select(.orderId != null and .orderId != "") | .orderId' "$TMP/burst_resp.txt" 2>/dev/null | sort -u)
N_ACKED=$(echo "$ACKED" | grep -c . || true)
RECOVERED_BURST=$(echo "$POST2" | jq -r '.open[][] | select(.price <= 3 or .price >= 1000) | .orderId' | sort -u)
N_RECOVERED=$(echo "$RECOVERED_BURST" | grep -c . || true)
MISSING=$(comm -23 <(echo "$ACKED") <(echo "$RECOVERED_BURST") | grep -c . || true)
PRE_KILL=$(grep orderId "$TMP/acked_before_kill.txt" | jq -r '.orderId' 2>/dev/null | sort -u)
MISSING_PRE_KILL=$(comm -23 <(echo "$PRE_KILL") <(echo "$RECOVERED_BURST") | grep -c . || true)

log "burst: sent=$BURST acked-before-kill=$ACKED_AT_KILL acked-total=$N_ACKED resting-after-recovery=$N_RECOVERED"
[ "$MISSING_PRE_KILL" -eq 0 ] || fail "scenario 2: $MISSING_PRE_KILL orders acknowledged before the crash were lost"
[ "$MISSING" -eq 0 ] || fail "scenario 2: $MISSING acknowledged orders missing from the book"
[ "$N_RECOVERED" -le "$BURST" ] || fail "scenario 2: more burst orders in book than were sent"

OLD_PRE=$(jq -S '.open | map_values(map(select(.price > 3 and .price < 1000)))' "$TMP/pre.json")
OLD_POST=$(echo "$POST2" | jq -S '.open | map_values(map(select(.price > 3 and .price < 1000)))')
[ "$OLD_PRE" = "$OLD_POST" ] || fail "scenario 2: scenario-1 orders changed across second crash"

TOTALS=$(echo "$POST2" | jq -c '[.balances[] | to_entries[]] | group_by(.key) | map({(.[0].key): (map(.value.available + .value.locked) | add)}) | add')
[ "$TOTALS" = '{"SOL":100000,"USDC":100000}' ] || fail "scenario 2: asset totals not conserved: $TOTALS"
log "scenario 2 PASS: all acked orders resting, no phantoms, totals $TOTALS"

echo
echo "RESULT: PASS"
echo "  scenario 1: restart->serving ${RT1} ms; $S1_LINE"
echo "  scenario 2: restart->serving ${RT2} ms; $(recovery_line)"
