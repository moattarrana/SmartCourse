#!/usr/bin/env bash
# End-to-end Week 2 test through the gateway: content -> publish (Temporal) -> enroll.
# Run after: docker compose up --build   (wait until all services are healthy)
# Usage: bash scripts/smoke_test_week2.sh
set -u

BASE="${BASE:-http://localhost:8000}"   # api-gateway
PY="${PY:-python}"; command -v "$PY" >/dev/null 2>&1 || PY=python3
jget() { "$PY" -c "import sys,json;print(json.load(sys.stdin).get('$1',''))"; }
pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; exit 1; }

echo "==> Registering users (ignoring 'already exists')"
# NOTE: adjust these payload fields if your UserCreate schema differs.
curl -s -o /dev/null -X POST "$BASE/api/auth/register" -H 'Content-Type: application/json' \
  -d '{"email":"teacher@smart.io","full_name":"Ada","password":"pw123456","role":"instructor"}'
curl -s -o /dev/null -X POST "$BASE/api/auth/register" -H 'Content-Type: application/json' \
  -d '{"email":"student@smart.io","full_name":"Kit","password":"pw123456","role":"student"}'

echo "==> Logging in"
TOKEN_I=$(curl -s -X POST "$BASE/api/auth/login" -H 'Content-Type: application/json' \
  -d '{"email":"teacher@smart.io","password":"pw123456"}' | jget access_token)
TOKEN_S=$(curl -s -X POST "$BASE/api/auth/login" -H 'Content-Type: application/json' \
  -d '{"email":"student@smart.io","password":"pw123456"}' | jget access_token)
[ -n "$TOKEN_I" ] && pass "instructor login" || fail "instructor login"
[ -n "$TOKEN_S" ] && pass "student login" || fail "student login"

echo "==> Creating course + module + lesson"
COURSE_ID=$(curl -s -X POST "$BASE/api/courses" -H "Authorization: Bearer $TOKEN_I" \
  -H 'Content-Type: application/json' -d '{"title":"Distributed Systems 101"}' | jget id)
[ -n "$COURSE_ID" ] && pass "course created ($COURSE_ID)" || fail "course create"

MODULE_ID=$(curl -s -X POST "$BASE/api/courses/$COURSE_ID/modules" -H "Authorization: Bearer $TOKEN_I" \
  -H 'Content-Type: application/json' -d '{"title":"Consensus","position":1}' | jget id)
[ -n "$MODULE_ID" ] && pass "module created" || fail "module create"

L_CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
  "$BASE/api/courses/$COURSE_ID/modules/$MODULE_ID/lessons" -H "Authorization: Bearer $TOKEN_I" \
  -H 'Content-Type: application/json' -d '{"title":"Raft","content":"leader election","position":1}')
[ "$L_CODE" = "201" ] && pass "lesson created" || fail "lesson create (got $L_CODE)"

echo "==> Publishing via Temporal workflow"
P_CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/courses/$COURSE_ID/publish" \
  -H "Authorization: Bearer $TOKEN_I")
[ "$P_CODE" = "202" ] && pass "publish accepted (202)" || fail "publish (got $P_CODE)"

echo "==> Polling course status until published (max 30s)"
STATUS=""
for i in $(seq 1 30); do
  STATUS=$(curl -s "$BASE/api/courses/$COURSE_ID" -H "Authorization: Bearer $TOKEN_I" | jget status)
  echo "     status=$STATUS"
  [ "$STATUS" = "published" ] && break
  sleep 1
done
[ "$STATUS" = "published" ] && pass "workflow published the course" || fail "not published (last: $STATUS)"

echo "==> Enrolling student (first 201, duplicate 409)"
E1=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/enrollments" \
  -H "Authorization: Bearer $TOKEN_S" -H 'Content-Type: application/json' \
  -d "{\"course_id\":\"$COURSE_ID\"}")
[ "$E1" = "201" ] && pass "first enroll 201" || fail "first enroll (got $E1)"

E2=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/enrollments" \
  -H "Authorization: Bearer $TOKEN_S" -H 'Content-Type: application/json' \
  -d "{\"course_id\":\"$COURSE_ID\"}")
[ "$E2" = "409" ] && pass "duplicate enroll 409" || fail "duplicate enroll (got $E2)"

echo ""
echo "All Week 2 smoke checks passed."
