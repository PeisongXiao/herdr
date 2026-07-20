#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DEV_IMAGE=${HERDR_DEV_IMAGE:-herdr-linux-dev:latest}
SSH_IMAGE=${HERDR_SSH_IMAGE:-herdr-e2e-remote:latest}
TARGET_VOLUME=${HERDR_TARGET_VOLUME:-herdr-linux-dev-target}
WAIT_SECONDS=${HERDR_REMOTE_RESTORE_WAIT_SECONDS:-125}
RUN="herdr-remote-restore-${USER:-user}-$$"
TMP=$(mktemp -d)
ORIGIN_NET="$RUN-origin"
REMOTE_NET="$RUN-remote"
SESSION=remote-restore-e2e

containers=(
  "$RUN-origin"
  "$RUN-origin-other"
  "$RUN-gateway"
  "$RUN-remote-live"
  "$RUN-remote-park"
  "$RUN-remote-down"
)

cleanup() {
  for container in "${containers[@]}"; do
    docker rm -f "$container" >/dev/null 2>&1 || true
  done
  docker network rm "$ORIGIN_NET" >/dev/null 2>&1 || true
  docker network rm "$REMOTE_NET" >/dev/null 2>&1 || true
  docker run --rm -v "$TMP:/cleanup" "$DEV_IMAGE" \
    sh -c 'rm -rf /cleanup/*' >/dev/null 2>&1 || true
  rm -rf "$TMP"
}
trap cleanup EXIT INT TERM

fail() {
  printf 'remote restore docker test: %s\n' "$*" >&2
  exit 1
}

wait_until() {
  local timeout=$1
  shift
  local deadline=$((SECONDS + timeout))
  until "$@"; do
    (( SECONDS >= deadline )) && return 1
    sleep 1
  done
}

independent_timeouts_reported() {
  local pending timeouts
  pending=$(origin remote-resume --list 2>&1 || true)
  timeouts=$(grep -Eic 'timed out|120 second' <<<"$pending" || true)
  (( timeouts >= 4 ))
}

docker image inspect "$DEV_IMAGE" >/dev/null
docker image inspect "$SSH_IMAGE" >/dev/null

docker run --rm \
  -e PATH=/usr/local/cargo/bin:/root/.bun/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
  -v "$ROOT:/work" \
  -v "$TARGET_VOLUME:/work/target" \
  -w /work \
  "$DEV_IMAGE" \
  sh -c 'cargo build --locked --bin herdr'
docker run --rm \
  -v "$TARGET_VOLUME:/target:ro" \
  -v "$TMP:/out" \
  "$DEV_IMAGE" \
  sh -c 'cp /target/debug/herdr /out/herdr && chmod 755 /out/herdr'

mkdir -p "$TMP/ssh" "$TMP/nodes"
docker run --rm -v "$TMP/ssh:/keys" "$DEV_IMAGE" \
  sh -c "ssh-keygen -q -t ed25519 -N '' -f /keys/id_ed25519 && chown $(id -u):$(id -g) /keys/id_ed25519 /keys/id_ed25519.pub"
cp "$TMP/ssh/id_ed25519.pub" "$TMP/ssh/authorized_keys"
chmod 600 "$TMP/ssh/id_ed25519" "$TMP/ssh/authorized_keys"

cat >"$TMP/herdr-wrapper" <<'EOF'
#!/bin/sh
export HOME=/state/home
export XDG_CONFIG_HOME=/state/config
export XDG_STATE_HOME=/state/state
export XDG_RUNTIME_DIR=/state/run
mkdir -p "$HOME" "$XDG_CONFIG_HOME" "$XDG_STATE_HOME" "$XDG_RUNTIME_DIR"
chmod 700 "$XDG_RUNTIME_DIR"
exec /opt/herdr/herdr "$@"
EOF
chmod 755 "$TMP/herdr-wrapper"

cat >"$TMP/sshd.conf" <<'EOF'
PermitRootLogin prohibit-password
PubkeyAuthentication yes
PasswordAuthentication no
AllowTcpForwarding yes
AllowStreamLocalForwarding yes
StreamLocalBindUnlink yes
PermitUserEnvironment no
EOF

prepare_state() {
  local name=$1
  local state="$TMP/nodes/$name"
  mkdir -p "$state"/{home/.ssh,config/herdr-dev,state,run}
  chmod 700 "$state/home/.ssh" "$state/run"
  cp "$TMP/ssh/id_ed25519" "$state/home/.ssh/id_ed25519"
  chmod 600 "$state/home/.ssh/id_ed25519"
}

for node in origin origin-other remote-live remote-park remote-down; do
  prepare_state "$node"
done

cat >"$TMP/nodes/origin/config/herdr-dev/config.toml" <<'EOF'
[remote]
auto_remote_handoff = true
EOF
cp "$TMP/nodes/origin/config/herdr-dev/config.toml" \
  "$TMP/nodes/origin-other/config/herdr-dev/config.toml"

cat >"$TMP/nodes/origin/home/.ssh/config" <<EOF
Host gateway
  HostName gateway
  User root
  IdentityFile /state/home/.ssh/id_ed25519
  BatchMode yes
  ConnectTimeout 5
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  LogLevel ERROR

Host remote-live remote-park remote-down
  User root
  IdentityFile /state/home/.ssh/id_ed25519
  ProxyJump gateway
  BatchMode yes
  ConnectTimeout 5
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  LogLevel ERROR
EOF
cp "$TMP/nodes/origin/home/.ssh/config" \
  "$TMP/nodes/origin-other/home/.ssh/config"
chmod 600 "$TMP/nodes/origin/home/.ssh/config" \
  "$TMP/nodes/origin-other/home/.ssh/config"

docker network create --internal "$ORIGIN_NET" >/dev/null
docker network create --internal "$REMOTE_NET" >/dev/null

docker run -d --name "$RUN-gateway" --hostname gateway \
  --network "$ORIGIN_NET" --network-alias gateway \
  -v "$TMP/ssh/authorized_keys:/fixture/authorized_keys:ro" \
  -v "$TMP/sshd.conf:/etc/ssh/sshd_config.d/99-herdr.conf:ro" \
  "$SSH_IMAGE" bash -c \
  'mkdir -p /root/.ssh && cp /fixture/authorized_keys /root/.ssh/authorized_keys && chmod 700 /root/.ssh && chmod 600 /root/.ssh/authorized_keys && ssh-keygen -A && exec /usr/sbin/sshd -D -e' >/dev/null
docker network connect --alias gateway "$REMOTE_NET" "$RUN-gateway"

start_remote() {
  local node=$1
  docker run -d --name "$RUN-$node" --hostname "$node" \
    --network "$REMOTE_NET" --network-alias "$node" \
    -v "$TMP/herdr:/opt/herdr/herdr:ro" \
    -v "$TMP/herdr-wrapper:/usr/local/bin/herdr:ro" \
    -v "$TMP/nodes/$node:/state" \
    -v "$TMP/ssh/authorized_keys:/fixture/authorized_keys:ro" \
    -v "$TMP/sshd.conf:/etc/ssh/sshd_config.d/99-herdr.conf:ro" \
    "$SSH_IMAGE" bash -c \
    'mkdir -p /root/.ssh && cp /fixture/authorized_keys /root/.ssh/authorized_keys && chmod 700 /root/.ssh && chmod 600 /root/.ssh/authorized_keys && ssh-keygen -A && exec /usr/sbin/sshd -D -e' >/dev/null
}

start_origin() {
  local node=$1
  docker run -d --name "$RUN-$node" --hostname "$node" \
    --network "$ORIGIN_NET" --network-alias "$node" \
    -v "$TMP/herdr:/opt/herdr/herdr:ro" \
    -v "$TMP/herdr-wrapper:/usr/local/bin/herdr:ro" \
    -v "$TMP/nodes/$node:/state" \
    "$DEV_IMAGE" sh -c \
    'mkdir -p /root/.ssh && cp /state/home/.ssh/config /root/.ssh/config && chmod 600 /root/.ssh/config && exec sleep infinity' >/dev/null
}

start_remote remote-live
start_remote remote-park
start_remote remote-down

remote_ip() {
  docker inspect --format "{{(index .NetworkSettings.Networks \"$REMOTE_NET\").IPAddress}}" "$RUN-$1"
}

cat >>"$TMP/nodes/origin/home/.ssh/config" <<EOF

Host remote-live
  HostName $(remote_ip remote-live)
Host remote-park
  HostName $(remote_ip remote-park)
Host remote-down
  HostName $(remote_ip remote-down)
EOF
cp "$TMP/nodes/origin/home/.ssh/config" \
  "$TMP/nodes/origin-other/home/.ssh/config"
start_origin origin
start_origin origin-other

origin() {
  docker exec -e HERDR_SESSION="$SESSION" "$RUN-origin" /usr/local/bin/herdr "$@"
}

origin_other() {
  docker exec -e HERDR_SESSION=remote-restore-other "$RUN-origin-other" \
    /usr/local/bin/herdr "$@"
}

remote() {
  local node=$1
  shift
  docker exec "$RUN-$node" /usr/local/bin/herdr "$@"
}

parked_json() {
  remote "$1" terminal parked list --json
}

parked_ids() {
  python3 -c '
import json, sys
data = json.load(sys.stdin)
ids = set()
def walk(value):
    if isinstance(value, dict):
        park_id = value.get("park_id")
        if isinstance(park_id, str):
            ids.add(park_id)
        for child in value.values():
            walk(child)
    elif isinstance(value, list):
        for child in value:
            walk(child)
walk(data)
print("\n".join(sorted(ids)))
'
}

parked_count() {
  parked_json "$1" | parked_ids | sed '/^$/d' | wc -l
}

parked_count_matches() {
  local node=$1
  local expected=$2
  local actual
  actual=$(parked_count "$node" 2>/dev/null) || return 1
  [[ "$actual" =~ ^[0-9]+$ ]] || return 1
  (( actual == expected ))
}

dump_remote_restore_diagnostics() {
  local reason=$1
  local node

  printf '\n===== remote restore diagnostics: %s =====\n' "$reason" >&2
  printf '\n--- origin container log ---\n' >&2
  docker logs --tail 200 "$RUN-origin" >&2 2>&1 || true
  printf '\n--- origin Herdr logs ---\n' >&2
  docker exec "$RUN-origin" sh -c '
    find /state/config /state/state -type f \( -name "*.log" -o -name "*.log.*" \) -print 2>/dev/null |
    while IFS= read -r file; do
      printf "\n----- %s -----\n" "$file"
      tail -n 200 "$file"
    done
  ' >&2 2>&1 || true

  for node in remote-live remote-park remote-down; do
    printf '\n--- %s authoritative parked state ---\n' "$node" >&2
    parked_json "$node" >&2 2>&1 || true
    printf '\n--- %s container/sshd log ---\n' "$node" >&2
    docker logs --tail 200 "$RUN-$node" >&2 2>&1 || true
    printf '\n--- origin SSH probe to %s ---\n' "$node" >&2
    docker exec -e HOME=/state/home "$RUN-origin" \
      ssh -vv -o BatchMode=yes -o ConnectTimeout=5 "$node" true >&2 2>&1 || true
  done

  printf '\n--- gateway sshd log ---\n' >&2
  docker logs --tail 200 "$RUN-gateway" >&2 2>&1 || true
  printf '\n===== end remote restore diagnostics =====\n' >&2
}

fail_with_remote_diagnostics() {
  dump_remote_restore_diagnostics "$*"
  fail "$@"
}

start_agent() {
  local host=$1
  local label=$2
  origin agent start "$label" --ssh "$host" --no-remote-integration \
    --cwd /state -- /bin/sh -lc \
    "while :; do date +%s > /state/heartbeat-$label; sleep 1; done" >/dev/null
}

docker exec "$RUN-origin" getent hosts remote-live >/dev/null 2>&1 && \
  fail "origin unexpectedly resolves a remote node directly"
docker exec -e HOME=/state/home "$RUN-origin" ssh remote-live true || \
  fail_with_remote_diagnostics "origin could not reach remote-live through the gateway"

origin server ensure || fail_with_remote_diagnostics "origin server did not start"
start_agent remote-live restore-live
start_agent remote-live restore-live-gone
start_agent remote-park restore-park-a
start_agent remote-park restore-park-b
start_agent remote-down restore-down-a
start_agent remote-down restore-down-b

for heartbeat in \
  "$TMP/nodes/remote-live/heartbeat-restore-live" \
  "$TMP/nodes/remote-live/heartbeat-restore-live-gone" \
  "$TMP/nodes/remote-park/heartbeat-restore-park-a" \
  "$TMP/nodes/remote-park/heartbeat-restore-park-b" \
  "$TMP/nodes/remote-down/heartbeat-restore-down-a" \
  "$TMP/nodes/remote-down/heartbeat-restore-down-b"; do
  wait_until 30 test -s "$heartbeat" || \
    fail_with_remote_diagnostics "missing heartbeat $heartbeat"
done

origin server stop
wait_until 30 parked_count_matches remote-live 2 || \
  fail_with_remote_diagnostics "live remote did not park exactly two terminals"
wait_until 30 parked_count_matches remote-park 2 || \
  fail_with_remote_diagnostics "partition remote did not park exactly two terminals"
wait_until 30 parked_count_matches remote-down 2 || \
  fail_with_remote_diagnostics "down remote did not park exactly two terminals"

before=$(stat -c %Y "$TMP/nodes/remote-park/heartbeat-restore-park-a")
sleep 2
after=$(stat -c %Y "$TMP/nodes/remote-park/heartbeat-restore-park-a")
(( after > before )) || fail "parked remote process stopped running"

parked_dump=$(parked_json remote-park)
agent_dump=$(remote remote-park agent list)
while IFS= read -r park_id; do
  [[ -z "$park_id" ]] && continue
  grep -Fq "$park_id" <<<"$agent_dump" && \
    fail "parked terminal leaked into ordinary agent listing"
done < <(parked_ids <<<"$parked_dump")

remote_live_ip=$(remote_ip remote-live)
mapfile -t remote_live_park_ids < <(parked_json remote-live | parked_ids)
(( ${#remote_live_park_ids[@]} == 2 )) || fail "live remote park ids were not recorded"
remote_live_terminal_ids=$(parked_json remote-live | python3 -c '
import json, sys
data = json.load(sys.stdin)
ids = set()
def walk(value):
    if isinstance(value, dict):
        terminal_id = value.get("terminal_id")
        if isinstance(terminal_id, str):
            ids.add(terminal_id)
        for child in value.values():
            walk(child)
    elif isinstance(value, list):
        for child in value:
            walk(child)
walk(data)
print("\n".join(sorted(ids)))
')
docker network disconnect "$REMOTE_NET" "$RUN-remote-live"
docker network disconnect "$REMOTE_NET" "$RUN-remote-park"
docker stop "$RUN-remote-down" >/dev/null
cat >"$TMP/nodes/origin/config/herdr-dev/config.toml" <<'EOF'
[remote]
auto_remote_handoff = false
EOF
origin server ensure
remote remote-live terminal parked terminate "${remote_live_park_ids[0]}"
docker network connect --ip "$remote_live_ip" --alias remote-live \
  "$REMOTE_NET" "$RUN-remote-live"

sleep "$WAIT_SECONDS"
wait_until 15 independent_timeouts_reported || {
  pending=$(origin remote-resume --list 2>&1 || true)
  printf '%s\n' "$pending" >&2
  fail "independent 120-second timeouts were not reported for all unreachable terminals"
}
[[ $(parked_count remote-live) -eq 0 ]] || \
  fail "reachable terminal was not restored"
pending=$(origin remote-resume --list 2>&1 || true)
while IFS= read -r terminal_id; do
  [[ -z "$terminal_id" ]] && continue
  grep -Fq "$terminal_id" <<<"$pending" && \
    fail "authoritatively ended or restored live ticket remained pending: $terminal_id"
done <<<"$remote_live_terminal_ids"

origin server stop
if docker exec "$RUN-origin" /usr/local/bin/herdr session delete "$SESSION" >/dev/null 2>&1; then
  fail "session delete unexpectedly ignored outstanding recovery tickets"
fi
forced=$(docker exec "$RUN-origin" /usr/local/bin/herdr session delete "$SESSION" --force --json)
grep -Eq '"orphaned_remote_terminals"[[:space:]]*:[[:space:]]*[1-9]' <<<"$forced" || \
  fail "forced deletion did not report orphaned remote terminals"

docker network connect --alias remote-park "$REMOTE_NET" "$RUN-remote-park"
docker exec -e HOME=/state/home "$RUN-origin-other" ssh remote-park true
origin_other server ensure
start_other_output=$(origin_other agent start discovery-probe --ssh remote-park \
  --no-remote-integration --cwd /state -- /bin/sh -lc 'sleep 30' 2>&1 || true)
[[ -n "$start_other_output" ]] || true

mapfile -t remaining_ids < <(parked_json remote-park | parked_ids)
(( ${#remaining_ids[@]} == 2 )) || fail "discarded tickets did not leave two discoverable orphans"
remote remote-park terminal parked promote "${remaining_ids[0]}"
remote remote-park terminal parked terminate "${remaining_ids[1]}"
[[ $(parked_count remote-park) -eq 0 ]] || fail "orphan resolution left parked entries"

docker rm "$RUN-remote-down" >/dev/null
start_remote remote-down
wait_until 30 remote remote-down server ensure || \
  fail_with_remote_diagnostics "restarted remote server did not start"
[[ $(parked_count remote-down) -eq 0 ]] || \
  fail "remote machine restart fabricated parked terminals whose PTYs were lost"

printf 'remote restore docker lifecycle passed (%s)\n' "$RUN"
