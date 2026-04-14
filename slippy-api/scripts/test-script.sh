#!/usr/bin/env bash
set -euo pipefail

BASE=${BASE:-http://localhost:8080}
WRITE_API_KEY=${SLIPPY_WRITE_API_KEY:-replacewithenvvarvalue}

CORR="test-$(uuidgen | tr '[:upper:]' '[:lower:]')"
SHA="$(printf '%040d' "$(date +%s)")0000000000000000000000000000000000000000"
SHA="${SHA:0:40}"

step() { echo; echo "══> $*"; }
check() {
  local label=$1 code=$2 body=$3
  if [ "$code" = "204" ] || [ "$code" = "201" ] || [ "$code" = "200" ]; then
    echo "    ✓ HTTP $code — $label"
    [ -n "$body" ] && echo "$body" | jq . 2>/dev/null || true
  else
    echo "    ✗ HTTP $code — $label FAILED"
    [ -n "$body" ] && echo "$body"
    exit 1
  fi
}

# ── 1. Create slip ────────────────────────────────────────────────────────────
step "1. Create slip  corr=$CORR  sha=${SHA:0:12}..."
RAW=$(curl -s -w "\n%{http_code}" -X POST $BASE/v1/slips \
  -H "Authorization: Bearer $WRITE_API_KEY" \
  -H "Content-Type: application/json" \
  -d "{
    \"correlation_id\": \"$CORR\",
    \"repository\":     \"MyCarrier-DevOps/my-service\",
    \"branch\":         \"main\",
    \"commit_sha\":     \"$SHA\",
    \"components\": [
      {\"name\": \"api\",    \"dockerfile_path\": \"services/api/Dockerfile\"},
      {\"name\": \"worker\", \"dockerfile_path\": \"services/worker/Dockerfile\"}
    ]
  }")
BODY=$(echo "$RAW" | head -1)
CODE=$(echo "$RAW" | tail -1)
check "create slip" "$CODE" "$BODY"
echo "    correlation_id : $(echo "$BODY" | jq -r '.slip.correlation_id')"
echo "    status         : $(echo "$BODY" | jq -r '.slip.status')"
echo "    ancestry       : $(echo "$BODY" | jq -r '.ancestry_resolved')"
# builds is auto-started by CreateSlipForPush at the pipeline level

# ── 2. Start + Complete unit_tests ─────────────────────────────────────────
step "2. Start unit_tests"
RAW=$(curl -s -w "\n%{http_code}" \
  -X POST $BASE/v1/slips/$CORR/steps/unit_tests/start \
  -H "Authorization: Bearer $WRITE_API_KEY")
check "start unit_tests" "$(echo "$RAW" | tail -1)" "$(echo "$RAW" | head -1)"
sleep 2

step "2. Complete unit_tests"
RAW=$(curl -s -w "\n%{http_code}" \
  -X POST $BASE/v1/slips/$CORR/steps/unit_tests/complete \
  -H "Authorization: Bearer $WRITE_API_KEY")
check "complete unit_tests" "$(echo "$RAW" | tail -1)" "$(echo "$RAW" | head -1)"

# ── 2b. Start + Complete secret_scan ─────────────────────────────────────────
step "2b. Start secret_scan"
RAW=$(curl -s -w "\n%{http_code}" \
  -X POST $BASE/v1/slips/$CORR/steps/secret_scan/start \
  -H "Authorization: Bearer $WRITE_API_KEY")
check "start secret_scan" "$(echo "$RAW" | tail -1)" "$(echo "$RAW" | head -1)"
sleep 2

step "2b. Complete secret_scan"
RAW=$(curl -s -w "\n%{http_code}" \
  -X POST $BASE/v1/slips/$CORR/steps/secret_scan/complete \
  -H "Authorization: Bearer $WRITE_API_KEY")
check "complete secret_scan" "$(echo "$RAW" | tail -1)" "$(echo "$RAW" | head -1)"

# ── 3. Start + Complete builds (api + worker) ───────────────────────────────
for COMPONENT in api worker; do
  step "3. Start builds – $COMPONENT"
  RAW=$(curl -s -w "\n%{http_code}" \
    -X POST $BASE/v1/slips/$CORR/steps/builds/start \
    -H "Authorization: Bearer $WRITE_API_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"component_name\": \"$COMPONENT\"}")
  check "start builds/$COMPONENT" "$(echo "$RAW" | tail -1)" "$(echo "$RAW" | head -1)"
  sleep 2

  step "3. Complete builds – $COMPONENT"
  RAW=$(curl -s -w "\n%{http_code}" \
    -X POST $BASE/v1/slips/$CORR/steps/builds/complete \
    -H "Authorization: Bearer $WRITE_API_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"component_name\": \"$COMPONENT\"}")
  check "complete builds/$COMPONENT" "$(echo "$RAW" | tail -1)" "$(echo "$RAW" | head -1)"
done

# ── 4. Start + Complete dev_deploy ───────────────────────────────────────────
step "4. Start dev_deploy"
RAW=$(curl -s -w "\n%{http_code}" \
  -X POST $BASE/v1/slips/$CORR/steps/dev_deploy/start \
  -H "Authorization: Bearer $WRITE_API_KEY")
check "start dev_deploy" "$(echo "$RAW" | tail -1)" "$(echo "$RAW" | head -1)"
sleep 2

step "4. Complete dev_deploy"
RAW=$(curl -s -w "\n%{http_code}" \
  -X POST $BASE/v1/slips/$CORR/steps/dev_deploy/complete \
  -H "Authorization: Bearer $WRITE_API_KEY")
check "complete dev_deploy" "$(echo "$RAW" | tail -1)" "$(echo "$RAW" | head -1)"

# ── 5. Start + Complete dev_tests ────────────────────────────────────────────
step "5. Start dev_tests"
RAW=$(curl -s -w "\n%{http_code}" \
  -X POST $BASE/v1/slips/$CORR/steps/dev_tests/start \
  -H "Authorization: Bearer $WRITE_API_KEY")
check "start dev_tests" "$(echo "$RAW" | tail -1)" "$(echo "$RAW" | head -1)"
sleep 2

step "5. Complete dev_tests"
RAW=$(curl -s -w "\n%{http_code}" \
  -X POST $BASE/v1/slips/$CORR/steps/dev_tests/complete \
  -H "Authorization: Bearer $WRITE_API_KEY")
check "complete dev_tests" "$(echo "$RAW" | tail -1)" "$(echo "$RAW" | head -1)"

# ── Final read-back ───────────────────────────────────────────────────────────
step "Final state — GET /slips/$CORR"
RAW=$(curl -s -w "\n%{http_code}" $BASE/slips/$CORR \
  -H "Authorization: Bearer $WRITE_API_KEY")
BODY=$(echo "$RAW" | head -1)
CODE=$(echo "$RAW" | tail -1)
check "read slip" "$CODE" ""
echo
echo "  Overall status : $(echo "$BODY" | jq -r '.status')"
echo "  Step statuses  :"
echo "$BODY" | jq -r '.steps | to_entries[] | "    \(.key): \(.value.status)"'