#!/usr/bin/env bash
set -euo pipefail

# Quick-check script for the /v1/automation-test-results/by-correlation/* endpoints.
#
# USAGE
#   ./check-automation-test-results.sh <correlationID> [testId]
#
# When <testId> is omitted, the script hits the run-summary and tests-list
# endpoints. When <testId> is supplied, it also hits the single-test
# (stack-trace) drill-down.
#
# REQUIRED ENV
#   SLIPPY_API_KEY  — bearer token for read endpoints
#
# OPTIONAL ENV
#   BASE     base URL                                (default: http://localhost:8080)
#   ENV      filter by EnvironmentName               (e.g. prod)
#   STACK    filter by StackName
#   STAGE    filter by Stage                         (e.g. FeatureCoreApi)
#   ATTEMPT  filter to a specific Attempt (uint32)   (omit → latest per group on summary; no filter on tests)
#   STATUS   ResultStatus filter for /tests          (default: Failed; pass '*' or 'all' to disable)
#   LIMIT    page size for /tests                    (default: 100, max 1000)
#   CURSOR   pagination cursor for /tests
#
# EXAMPLES
#   SLIPPY_API_KEY=xxx ./check-automation-test-results.sh 11111111-1111-1111-1111-111111111111
#   SLIPPY_API_KEY=xxx ENV=prod STAGE=FeatureCoreApi STATUS='*' \
#     ./check-automation-test-results.sh 11111111-1111-1111-1111-111111111111
#   SLIPPY_API_KEY=xxx ./check-automation-test-results.sh <uuid> <testUuid>

BASE=${BASE:-http://localhost:8080}
API_KEY=${SLIPPY_API_KEY:?SLIPPY_API_KEY env var is required}

usage() {
  sed -n '3,30p' "$0"
  exit 2
}

[ $# -ge 1 ] || usage
KEY=$1
TEST_ID=${2:-}

PARENT_PATH="/v1/automation-test-results/by-correlation/$KEY"

# ── Build query strings from optional env vars ───────────────────────────────
PARENT_Q=""
add_q() {
  local key=$1 val=$2 sep
  [ -n "$val" ] || return 0
  if [ -z "$PARENT_Q" ]; then sep="?"; else sep="&"; fi
  PARENT_Q="$PARENT_Q$sep$key=$(printf %s "$val" | jq -sRr @uri)"
}
add_q environment "${ENV:-}"
add_q stack       "${STACK:-}"
add_q stage       "${STAGE:-}"
add_q attempt     "${ATTEMPT:-}"

# tests endpoint accepts the parent filters plus status, limit, cursor
TESTS_Q="$PARENT_Q"
add_tests_q() {
  local key=$1 val=$2 sep
  [ -n "$val" ] || return 0
  if [ -z "$TESTS_Q" ]; then sep="?"; else sep="&"; fi
  TESTS_Q="$TESTS_Q$sep$key=$(printf %s "$val" | jq -sRr @uri)"
}
add_tests_q status "${STATUS:-}"
add_tests_q limit  "${LIMIT:-}"
add_tests_q cursor "${CURSOR:-}"

step() { echo; echo "══> $*"; }

# call <label> <path>
call() {
  local label=$1 path=$2
  step "$label  GET $path"
  local RAW HTTP BODY
  RAW=$(curl -s -w "\n%{http_code}" "$BASE$path" \
    -H "Authorization: Bearer $API_KEY")
  HTTP=$(printf '%s' "$RAW" | tail -n1)
  BODY=$(printf '%s' "$RAW" | sed '$d')
  if [ "$HTTP" = "200" ]; then
    echo "    ✓ HTTP 200"
    [ -n "$BODY" ] && echo "$BODY" | jq . 2>/dev/null || echo "$BODY"
  elif [ "$HTTP" = "404" ] && [ "$label" = "single test" ]; then
    echo "    ✓ HTTP 404 (test not found in scope) — expected when testId is unrelated"
    [ -n "$BODY" ] && echo "$BODY"
  else
    echo "    ✗ HTTP $HTTP"
    [ -n "$BODY" ] && echo "$BODY"
    exit 1
  fi
}

echo "BASE=$BASE  correlationID=$KEY${TEST_ID:+  testId=$TEST_ID}"
[ -n "${ENV:-}${STACK:-}${STAGE:-}${ATTEMPT:-}${STATUS:-}${LIMIT:-}${CURSOR:-}" ] && {
  echo "Filters:"
  [ -n "${ENV:-}" ]     && echo "  environment = $ENV"
  [ -n "${STACK:-}" ]   && echo "  stack       = $STACK"
  [ -n "${STAGE:-}" ]   && echo "  stage       = $STAGE"
  [ -n "${ATTEMPT:-}" ] && echo "  attempt     = $ATTEMPT"
  [ -n "${STATUS:-}" ]  && echo "  status      = $STATUS"
  [ -n "${LIMIT:-}" ]   && echo "  limit       = $LIMIT"
  [ -n "${CURSOR:-}" ]  && echo "  cursor      = $CURSOR"
}

# 1. Run summary (RunResults)
call "runs" "$PARENT_PATH$PARENT_Q"

# 2. Tests list (TestResultsCor, paginated, default status=Failed)
call "tests" "$PARENT_PATH/tests$TESTS_Q"

# 3. Single test (stack-trace drilldown) — only when testId given.
# Single-test endpoint does not accept filters; uses just the path params.
if [ -n "$TEST_ID" ]; then
  call "single test" "$PARENT_PATH/tests/$TEST_ID"
fi

echo
echo "══> done"
