#!/usr/bin/env bash
# End-to-end Docker self-test for the airport disruption service.
#
# Scenarios covered:
#   A. Empty volume ........ build, unit tests, HTTP seed, restart + recreate
#                             persistence, liveness/readiness/migration probes.
#   B. Legacy v1 volume .... a database written by the early-service schema
#                             (no replay_count/created_at, no schema_meta) is
#                             upgraded in place; events + impacts survive and
#                             the original event keeps replaying idempotently
#                             across repeated restarts / recreations.
#   C. Interrupted upgrade . the migrating process hard-crashes mid
#                             transaction (MIGRATION_CRASH_AFTER_STEP); the
#                             volume must still be v1 (atomic rollback), and the
#                             next start completes the upgrade idempotently.
#   D. Corrupted schema .... a current-version database with a required index
#                             dropped stays alive (/healthz 200) but never ready
#                             (/readyz 503, /migrations blocked) and refuses all
#                             business traffic.
#
# It never reaches any external service: all traffic is local to the compose
# network.
#
# Usage: scripts/docker_selftest.sh
set -euo pipefail

cd "$(dirname "$0")/.."

PROJECT="disrupt-selftest"
LEGACY_PROJECT="disrupt-selftest-legacy"
CRASH_PROJECT="disrupt-selftest-crash"
BLOCKED_PROJECT="disrupt-selftest-blocked"
COMPOSE=(docker compose -p "$PROJECT" -f compose.yaml)
LEGACY_COMPOSE=(docker compose -p "$LEGACY_PROJECT" -f compose.yaml)
CRASH_COMPOSE=(docker compose -p "$CRASH_PROJECT" -f compose.yaml -f compose.crash.yaml)
BLOCKED_COMPOSE=(docker compose -p "$BLOCKED_PROJECT" -f compose.yaml)

log() { printf '\n=== %s ===\n' "$*"; }
fail() { printf '\nSELF-TEST FAILED: %s\n' "$*" >&2; exit 1; }

wait_healthy() {
    local project="$1" svc="${2:-api}" deadline=$(( $(date +%s) + 90 ))
    local cid health
    cid="$(docker compose -p "$project" ps -q "$svc")"
    [ -n "$cid" ] || fail "no container for $project/$svc"
    while :; do
        health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid" 2>/dev/null || echo starting)"
        [ "$health" = "healthy" ] && { echo "$project/$svc healthy"; return 0; }
        [ "$(date +%s)" -ge "$deadline" ] && fail "$project/$svc did not become healthy (last: $health)"
        sleep 1
    done
}

wait_exited() {
    local project="$1" want_code="$2" deadline=$(( $(date +%s) + 30 ))
    local cid status code
    cid="$(docker compose -p "$project" ps -q api)"
    [ -n "$cid" ] || fail "no container for $project/api"
    while :; do
        status="$(docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null || echo missing)"
        [ "$status" = "exited" ] && break
        [ "$(date +%s)" -ge "$deadline" ] && fail "$project/api did not exit (last: $status)"
        sleep 1
    done
    code="$(docker inspect -f '{{.State.ExitCode}}' "$cid")"
    echo "$project/api exited with code $code"
    [ "$code" = "$want_code" ] || fail "$project/api expected exit $want_code, got $code"
}

cleanup() {
    log "Teardown: removing test containers and volumes"
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
    "${LEGACY_COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
    "${CRASH_COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
    "${BLOCKED_COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || fail "docker is required but was not found"
docker compose version >/dev/null 2>&1 || fail "docker compose v2 is required"

# Clean slate + build ---------------------------------------------------------
log "Clean slate: removing any previous self-test resources"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${LEGACY_COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${CRASH_COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${BLOCKED_COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true

log "Building image from Dockerfile"
"${COMPOSE[@]}" build --no-cache api

###############################################################################
# A. Empty-volume flow
###############################################################################
log "A1: starting api with a fresh empty volume"
"${COMPOSE[@]}" up -d --wait api

log "A2: running automated unit tests inside the image"
"${COMPOSE[@]}" exec -T api python -m unittest discover -s tests 2>&1 | tail -5

log "A3: HTTP seed phase (validation, idempotency, cross-midnight, probes)"
"${COMPOSE[@]}" exec -T api python scripts/selftest_client.py seed http://127.0.0.1:8080

log "A4: database file lives on the persistent volume"
"${COMPOSE[@]}" exec -T api sh -c 'test -s /data/disruptions.db && echo "db file present"'

log "A5: restart container (volume attached), verify persistence"
"${COMPOSE[@]}" restart api
wait_healthy "$PROJECT"
"${COMPOSE[@]}" exec -T api python scripts/selftest_client.py verify http://127.0.0.1:8080
"${COMPOSE[@]}" exec -T api python scripts/selftest_client.py probes-ready http://127.0.0.1:8080

log "A6: recreate container against the same volume"
"${COMPOSE[@]}" up -d --force-recreate --wait api
"${COMPOSE[@]}" exec -T api python scripts/selftest_client.py verify http://127.0.0.1:8080

# A 阶段完成：释放宿主 8080 端口供后续项目使用（卷随 cleanup 统一删除）。
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true

###############################################################################
# B. Legacy v1 volume upgrade + repeated restarts
###############################################################################
log "B1: seeding a v1 database (early-service schema) onto a fresh volume"
"${LEGACY_COMPOSE[@]}" run --rm -T api python scripts/legacy_db.py build /data/disruptions.db /srv/fixtures
log "B2: confirming the volume really is legacy v1 before upgrade"
"${LEGACY_COMPOSE[@]}" run --rm -T api python scripts/legacy_db.py inspect /data/disruptions.db \
    | tee /tmp/legacy-inspect.json
grep -q '"state": "legacy"' /tmp/legacy-inspect.json || fail "seeded volume is not reported as legacy v1"
grep -q '"current_version": 1' /tmp/legacy-inspect.json || fail "seeded volume version is not 1"

log "B3: new binary starts against the v1 volume and auto-upgrades"
"${LEGACY_COMPOSE[@]}" up -d api
wait_healthy "$LEGACY_PROJECT"
"${LEGACY_COMPOSE[@]}" exec -T api python scripts/selftest_client.py legacy-verify http://127.0.0.1:8080

log "B4: repeat restart - upgrade is idempotent, original event keeps replaying"
"${LEGACY_COMPOSE[@]}" restart api
wait_healthy "$LEGACY_PROJECT"
# Previous legacy-verify performed two replays; the replay base must now be 2.
"${LEGACY_COMPOSE[@]}" exec -T -e EXPECT_REPLAY_BASE=2 api \
    python scripts/selftest_client.py legacy-verify http://127.0.0.1:8080

log "B5: recreate against same volume - still idempotent"
"${LEGACY_COMPOSE[@]}" up -d --force-recreate api
wait_healthy "$LEGACY_PROJECT"
"${LEGACY_COMPOSE[@]}" exec -T -e EXPECT_REPLAY_BASE=4 api \
    python scripts/selftest_client.py legacy-verify http://127.0.0.1:8080
"${LEGACY_COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true

###############################################################################
# C. Interrupted upgrade (hard crash mid-transaction)
###############################################################################
log "C1: seeding another v1 volume for the crash drill"
"${CRASH_COMPOSE[@]}" run --rm -T api python scripts/legacy_db.py build /data/disruptions.db /srv/fixtures

log "C2: first start must hard-crash after step 2 (exit code 27), before commit"
"${CRASH_COMPOSE[@]}" up -d api
wait_exited "$CRASH_PROJECT" 27

log "C3: rollback check - volume is still v1 with no half-migrated structure"
"${CRASH_COMPOSE[@]}" run --rm -T api python -m app.migrations inspect /data/disruptions.db \
    | tee /tmp/crash-inspect.json
grep -q '"state": "legacy"' /tmp/crash-inspect.json || fail "crash drill left a non-legacy (half-migrated?) database"
grep -q '"current_version": 1' /tmp/crash-inspect.json || fail "crash drill changed the schema version"
"${CRASH_COMPOSE[@]}" run --rm -T api python -c "
import sqlite3
c = sqlite3.connect('/data/disruptions.db')
cols = [r[1] for r in c.execute('PRAGMA table_info(events)')]
assert 'replay_count' not in cols and 'created_at' not in cols, cols
tables = [r[0] for r in c.execute(\"SELECT name FROM sqlite_master WHERE type='table'\")]
assert 'schema_meta' not in tables, tables
print('rollback verified: schema is exactly v1')
"
"${CRASH_COMPOSE[@]}" run --rm -T api sh -c 'test -s /data/.migration-crash-sent && echo "one-shot crash sentinel present"'

log "C4: restart with same env - sentinel makes this boot complete the upgrade"
"${CRASH_COMPOSE[@]}" up -d api
wait_healthy "$CRASH_PROJECT"
"${CRASH_COMPOSE[@]}" exec -T api python scripts/selftest_client.py legacy-verify http://127.0.0.1:8080

log "C5: one more restart - no crash, still ready, replay continues"
"${CRASH_COMPOSE[@]}" restart api
wait_healthy "$CRASH_PROJECT"
"${CRASH_COMPOSE[@]}" exec -T -e EXPECT_REPLAY_BASE=2 api \
    python scripts/selftest_client.py legacy-verify http://127.0.0.1:8080
"${CRASH_COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true

###############################################################################
# D. Corrupted schema blocks readiness but not liveness
###############################################################################
log "D1: initialise a current-version volume, then drop a required index"
"${BLOCKED_COMPOSE[@]}" run --rm -T api python -m app.migrations upgrade /data/disruptions.db >/dev/null
"${BLOCKED_COMPOSE[@]}" run --rm -T api python -c "
import sqlite3
c = sqlite3.connect('/data/disruptions.db')
c.execute('DROP INDEX idx_impacts_flight')
c.commit()
print('corrupted: idx_impacts_flight dropped')
"

log "D2: service stays alive but never becomes ready"
"${BLOCKED_COMPOSE[@]}" up -d api
# Do NOT use --wait: this container must never report healthy.
deadline=$(( $(date +%s) + 60 ))
until "${BLOCKED_COMPOSE[@]}" run --rm -T api \
        python -c "import urllib.request; urllib.request.urlopen('http://api:8080/healthz', timeout=2)" >/dev/null 2>&1; do
    [ "$(date +%s)" -ge "$deadline" ] && fail "blocked instance never answered /healthz"
    sleep 2
done
echo "blocked instance answers /healthz (liveness)"

# 就绪探针必须持续失败，容器健康状态最终翻为 unhealthy（start_period +
# retries 大约需要 30 秒），期间不能出现 healthy。
deadline=$(( $(date +%s) + 60 ))
saw_unhealthy=0
while [ "$(date +%s)" -lt "$deadline" ]; do
    cid="$("${BLOCKED_COMPOSE[@]}" ps -q api)"
    health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid")"
    [ "$health" = "healthy" ] && fail "blocked instance must never become healthy"
    if [ "$health" = "unhealthy" ]; then saw_unhealthy=1; break; fi
    sleep 2
done
[ "$saw_unhealthy" = 1 ] || fail "blocked instance never transitioned to unhealthy"
echo "blocked instance is alive but unhealthy as expected"

log "D3: probes differ and business traffic is refused"
"${BLOCKED_COMPOSE[@]}" run --rm -T api \
    python scripts/selftest_client.py probes-blocked http://api:8080

# trap runs the teardown ------------------------------------------------------
trap - EXIT
cleanup
rm -f /tmp/legacy-inspect.json /tmp/crash-inspect.json
printf '\nALL DOCKER SELF-TESTS PASSED\n'
