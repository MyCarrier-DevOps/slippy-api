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

# run_step <stepName> [label-prefix]
# Starts and completes a pipeline-level step (no component_name body).
run_step() {
  local name=$1 prefix=${2:-}

  step "${prefix}Start $name"
  RAW=$(curl -s -w "\n%{http_code}" \
    -X POST $BASE/v1/slips/$CORR/steps/$name/start \
    -H "Authorization: Bearer $WRITE_API_KEY")
  check "start $name" "$(echo "$RAW" | tail -1)" "$(echo "$RAW" | head -1)"
  sleep 1

  step "${prefix}Complete $name"
  RAW=$(curl -s -w "\n%{http_code}" \
    -X POST $BASE/v1/slips/$CORR/steps/$name/complete \
    -H "Authorization: Bearer $WRITE_API_KEY")
  check "complete $name" "$(echo "$RAW" | tail -1)" "$(echo "$RAW" | head -1)"
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

# ── 2. unit_tests ─────────────────────────────────────────────────────────────
run_step unit_tests "2. "

# ── 3. secret_scan ────────────────────────────────────────────────────────────
run_step secret_scan "3. "

# ── 4. package_artifact ───────────────────────────────────────────────────────
run_step package_artifact "4. "

# ── 5. builds (aggregate: api + worker) ───────────────────────────────────────
for COMPONENT in api worker; do
  step "5. Start builds – $COMPONENT"
  RAW=$(curl -s -w "\n%{http_code}" \
    -X POST $BASE/v1/slips/$CORR/steps/builds/start \
    -H "Authorization: Bearer $WRITE_API_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"component_name\": \"$COMPONENT\"}")
  check "start builds/$COMPONENT" "$(echo "$RAW" | tail -1)" "$(echo "$RAW" | head -1)"
  sleep 1

  step "5. Complete builds – $COMPONENT"
  RAW=$(curl -s -w "\n%{http_code}" \
    -X POST $BASE/v1/slips/$CORR/steps/builds/complete \
    -H "Authorization: Bearer $WRITE_API_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"component_name\": \"$COMPONENT\"}")
  check "complete builds/$COMPONENT" "$(echo "$RAW" | tail -1)" "$(echo "$RAW" | head -1)"
done

# ── 6. dev_deploy ─────────────────────────────────────────────────────────────
run_step dev_deploy "6. "

# ── 7. dev_tests ──────────────────────────────────────────────────────────────
run_step dev_tests "7. "

# ── 8. preprod_deploy ─────────────────────────────────────────────────────────
run_step preprod_deploy "8. "

# ── 9. preprod_tests ──────────────────────────────────────────────────────────
run_step preprod_tests "9. "

# ── 10. prod_gate ─────────────────────────────────────────────────────────────
run_step prod_gate "10. "

# ── 11. prod_release_created ──────────────────────────────────────────────────
run_step prod_release_created "11. "

# ── 12. prod_deploy ───────────────────────────────────────────────────────────
run_step prod_deploy "12. "

# ── 13. prod_tests ────────────────────────────────────────────────────────────
run_step prod_tests "13. "


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
