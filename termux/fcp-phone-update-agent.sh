#!/data/data/com.termux/files/usr/bin/bash
set -Eeuo pipefail

ROOT=""
STATE_DIR=""
CONTAINER="fcp-phone"
PORT="5000"
POLL_SECONDS="1"
APPROVED_REPOSITORY="nettking/msh"
APPROVED_BRANCH="main"
OID_RE='^[0-9a-f]{40}$'

usage() {
    cat <<'USAGE'
Usage: bash termux/fcp-phone-update-agent.sh \
  --repo-root PATH --state-directory PATH [--container NAME] [--port PORT]
USAGE
}

while (($#)); do
    case "$1" in
    --repo-root)
        ROOT="${2:-}"
        shift 2
        ;;
    --state-directory)
        STATE_DIR="${2:-}"
        shift 2
        ;;
    --container)
        CONTAINER="${2:-}"
        shift 2
        ;;
    --port)
        PORT="${2:-}"
        shift 2
        ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        echo "Unknown option: $1" >&2
        usage >&2
        exit 2
        ;;
    esac
done

[[ -n "$ROOT" && -n "$STATE_DIR" ]] || { usage >&2; exit 2; }
ROOT="$(cd "$ROOT" && pwd -P)"
STATE_DIR="$(mkdir -p "$STATE_DIR" && cd "$STATE_DIR" && pwd -P)"
DATA_DIR="$STATE_DIR/data"
RESULTS_DIR="$STATE_DIR/results"
AGENT_DIR="$DATA_DIR/federation/update-agent"
REQUEST_FILE="$AGENT_DIR/request.json"
RESULT_FILE="$AGENT_DIR/result.json"
LOCK_DIR="$AGENT_DIR/.termux-agent.lock"
RUNNING_COMMIT_FILE="$RESULTS_DIR/termux-running-commit"
CODEC="$ROOT/termux/fcp_phone_update_codec.py"
PHONE_SCRIPT="$ROOT/termux/fcp-phone.sh"
SETUP_SCRIPT="$ROOT/termux/setup-phone.sh"
URL="http://127.0.0.1:$PORT"

mkdir -p "$AGENT_DIR" "$RESULTS_DIR"

log() {
    printf '%s %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

acquire_lock() {
    if mkdir "$LOCK_DIR" 2>/dev/null; then
        printf '%s\n' "$$" > "$LOCK_DIR/pid"
        return 0
    fi
    local owner=""
    owner="$(cat "$LOCK_DIR/pid" 2>/dev/null || true)"
    if [[ "$owner" == "$$" ]]; then
        return 0
    fi
    if [[ "$owner" =~ ^[0-9]+$ ]] && kill -0 "$owner" 2>/dev/null; then
        log "Another Termux Federation update agent is already running as PID $owner."
        exit 0
    fi
    rm -rf "$LOCK_DIR"
    mkdir "$LOCK_DIR"
    printf '%s\n' "$$" > "$LOCK_DIR/pid"
}

cleanup() {
    local owner=""
    owner="$(cat "$LOCK_DIR/pid" 2>/dev/null || true)"
    if [[ "$owner" == "$$" ]]; then
        rm -rf "$LOCK_DIR"
    fi
}
trap cleanup EXIT INT TERM
acquire_lock

command -v git >/dev/null 2>&1 || { log "Git is unavailable."; exit 1; }
command -v proot-distro >/dev/null 2>&1 || { log "proot-distro is unavailable."; exit 1; }
[[ -f "$CODEC" && -f "$PHONE_SCRIPT" && -f "$SETUP_SCRIPT" ]] || {
    log "Required Termux update files are missing."
    exit 1
}

container_exists() {
    proot-distro list -q 2>/dev/null | grep -Fxq "$CONTAINER"
}

codec() {
    proot-distro login \
        --bind "$ROOT:/app" \
        --bind "$DATA_DIR:/app/data" \
        --bind "$RESULTS_DIR:/app/results" \
        --work-dir /app \
        "$CONTAINER" -- python /app/termux/fcp_phone_update_codec.py "$@"
}

approved_remote() {
    local value="${1,,}"
    value="${value%/}"
    case "$value" in
        https://github.com/nettking/msh|https://github.com/nettking/msh.git|\
        git@github.com:nettking/msh|git@github.com:nettking/msh.git|\
        ssh://git@github.com/nettking/msh|ssh://git@github.com/nettking/msh.git)
            return 0
            ;;
    esac
    return 1
}

ancestor() {
    git -C "$ROOT" merge-base --is-ancestor "$1" "$2" >/dev/null 2>&1
}

running_commit() {
    local value=""
    if ! curl -fsS "$URL/" >/dev/null 2>&1; then
        return 1
    fi
    value="$(cat "$RUNNING_COMMIT_FILE" 2>/dev/null || true)"
    value="${value,,}"
    [[ "$value" =~ $OID_RE ]] || return 1
    printf '%s\n' "$value"
}

INSPECTION_STATE=""
INSPECTION_CURRENT=""
INSPECTION_TARGET=""
INSPECTION_CODE=""
INSPECTION_MESSAGE=""

inspect_checkout() {
    local requested_target="$1"
    local top="" remote="" branch="" status="" approved_tip="" target="" current=""

    top="$(git -C "$ROOT" rev-parse --show-toplevel)" || return 1
    top="$(cd "$top" && pwd -P)"
    [[ "$top" == "$ROOT" ]] || {
        INSPECTION_STATE="error"; INSPECTION_CODE="unsupported_checkout"; INSPECTION_MESSAGE="The checkout root is not approved."; return 0;
    }
    remote="$(git -C "$ROOT" remote get-url origin)" || return 1
    approved_remote "$remote" || {
        INSPECTION_STATE="error"; INSPECTION_CODE="unapproved_remote"; INSPECTION_MESSAGE="The origin remote is not the approved repository."; return 0;
    }
    branch="$(git -C "$ROOT" symbolic-ref --quiet --short HEAD 2>/dev/null || true)"
    [[ "$branch" == "$APPROVED_BRANCH" ]] || {
        INSPECTION_STATE="error"; INSPECTION_CODE="detached_head"; INSPECTION_MESSAGE="The checkout is not on approved main."; return 0;
    }
    current="$(git -C "$ROOT" rev-parse --verify 'HEAD^{commit}')"
    current="${current,,}"
    [[ "$current" =~ $OID_RE ]] || return 1
    INSPECTION_CURRENT="$current"
    INSPECTION_TARGET="$requested_target"

    status="$(git -C "$ROOT" status --porcelain=v1 --untracked-files=all)"
    if [[ -n "$status" ]]; then
        INSPECTION_STATE="dirty"
        INSPECTION_CODE="dirty"
        INSPECTION_MESSAGE="Local changes must be reviewed before updating."
        return 0
    fi

    git -C "$ROOT" fetch --no-tags origin "$APPROVED_BRANCH" >/dev/null
    approved_tip="$(git -C "$ROOT" rev-parse --verify 'FETCH_HEAD^{commit}')"
    approved_tip="${approved_tip,,}"
    [[ "$approved_tip" =~ $OID_RE ]] || return 1
    target="${requested_target,,}"
    [[ "$target" =~ $OID_RE ]] || {
        INSPECTION_STATE="error"; INSPECTION_CODE="target_unavailable"; INSPECTION_MESSAGE="The requested target is not an exact commit."; return 0;
    }
    INSPECTION_TARGET="$target"
    if ! git -C "$ROOT" cat-file -e "$target^{commit}" 2>/dev/null || ! ancestor "$target" "$approved_tip"; then
        INSPECTION_STATE="error"
        INSPECTION_CODE="target_unavailable"
        INSPECTION_MESSAGE="The requested target is not reachable from approved main."
        return 0
    fi

    INSPECTION_CODE=""
    INSPECTION_MESSAGE=""
    if [[ "$current" == "$target" ]]; then
        INSPECTION_STATE="up_to_date"
    elif ancestor "$current" "$target"; then
        INSPECTION_STATE="update_available"
    elif ancestor "$target" "$current"; then
        INSPECTION_STATE="ahead"
        INSPECTION_CODE="ahead"
        INSPECTION_MESSAGE="This checkout is ahead of approved main."
    else
        INSPECTION_STATE="diverged"
        INSPECTION_CODE="diverged"
        INSPECTION_MESSAGE="This checkout has diverged from approved main."
    fi
}

write_result() {
    local request_id="$1" action="$2" state="$3" current="$4" target="$5" running="$6" code="$7" message="$8"
    codec write-result /app/data/federation/update-agent/result.json \
        --request-id "$request_id" \
        --action "$action" \
        --state "$state" \
        --current "$current" \
        --target "$target" \
        --running "$running" \
        --code "$code" \
        --message "$message"
}

safe_failure() {
    local request_id="$1" action="$2" target="$3"
    local current="" running=""
    current="$(git -C "$ROOT" rev-parse --verify 'HEAD^{commit}' 2>/dev/null || true)"
    current="${current,,}"
    [[ "$current" =~ $OID_RE ]] || current=""
    running="$(running_commit 2>/dev/null || true)"
    write_result "$request_id" "$action" "error" "$current" "$target" "$running" \
        "host_update_failed" \
        "The Termux update agent stopped safely before it could verify the requested runtime." || true
}

process_request() {
    local processing="$1"
    local guest_path="/app/data/federation/update-agent/$(basename "$processing")"
    local parsed=() request_id="invalid-request" action="check" target="" activate_epoch="0"
    local running="" current_script_hash="" new_script_hash="" now="0" delay="0" proven=""

    if ! mapfile -t parsed < <(codec parse-request "$guest_path"); then
        log "Rejected malformed or expired Federation update request."
        return 0
    fi
    [[ "${#parsed[@]}" -eq 4 ]] || { log "Rejected incomplete Federation update request."; return 0; }
    request_id="${parsed[0]}"
    action="${parsed[1]}"
    target="${parsed[2]}"
    activate_epoch="${parsed[3]}"

    if [[ "$action" == "apply" && "$activate_epoch" =~ ^[0-9]+$ ]]; then
        now="$(date +%s)"
        if ((activate_epoch > now)); then
            delay=$((activate_epoch - now))
            ((delay > 30)) && delay=30
            sleep "$delay"
        fi
    fi

    if ! inspect_checkout "$target"; then
        safe_failure "$request_id" "$action" "$target"
        return 0
    fi
    running="$(running_commit 2>/dev/null || true)"

    if [[ "$action" == "check" ]]; then
        write_result "$request_id" "$action" "$INSPECTION_STATE" "$INSPECTION_CURRENT" \
            "$INSPECTION_TARGET" "$running" "$INSPECTION_CODE" "$INSPECTION_MESSAGE"
        return 0
    fi

    case "$INSPECTION_STATE" in
    update_available|up_to_date) ;;
    *)
        write_result "$request_id" "$action" "$INSPECTION_STATE" "$INSPECTION_CURRENT" \
            "$INSPECTION_TARGET" "$running" "$INSPECTION_CODE" \
            "${INSPECTION_MESSAGE:-The checkout is not eligible for activation.}"
        return 0
        ;;
    esac

    current_script_hash="$(sha256sum "$ROOT/termux/fcp-phone-update-agent.sh" | awk '{print $1}')"
    if [[ "$INSPECTION_STATE" == "update_available" ]]; then
        git -C "$ROOT" merge --ff-only "$target" >/dev/null
    fi
    proven="$(git -C "$ROOT" rev-parse --verify 'HEAD^{commit}')"
    proven="${proven,,}"
    [[ "$proven" == "$target" ]] || { safe_failure "$request_id" "$action" "$target"; return 0; }
    [[ -z "$(git -C "$ROOT" status --porcelain=v1 --untracked-files=all)" ]] || {
        safe_failure "$request_id" "$action" "$target"; return 0;
    }

    if ! FCP_PHONE_RESTART_AFTER_SETUP=true bash "$SETUP_SCRIPT" --update; then
        safe_failure "$request_id" "$action" "$target"
        return 0
    fi

    running="$(running_commit 2>/dev/null || true)"
    if [[ "$running" != "$target" ]]; then
        safe_failure "$request_id" "$action" "$target"
        return 0
    fi
    proven="$(git -C "$ROOT" rev-parse --verify 'HEAD^{commit}')"
    proven="${proven,,}"
    if [[ "$proven" != "$target" || -n "$(git -C "$ROOT" status --porcelain=v1 --untracked-files=all)" ]]; then
        safe_failure "$request_id" "$action" "$target"
        return 0
    fi

    write_result "$request_id" "$action" "runtime_verified" "$target" "$target" "$target" \
        "updated" \
        "FCP source, Termux runtime, restarted web process, and running commit were updated and verified."

    new_script_hash="$(sha256sum "$ROOT/termux/fcp-phone-update-agent.sh" | awk '{print $1}')"
    if [[ "$new_script_hash" != "$current_script_hash" ]]; then
        log "Updater changed; replacing the running Termux update agent."
        exec bash "$ROOT/termux/fcp-phone-update-agent.sh" \
            --repo-root "$ROOT" --state-directory "$STATE_DIR" --container "$CONTAINER" --port "$PORT"
    fi
}

container_exists || { log "The FCP phone container '$CONTAINER' is not installed."; exit 1; }
log "Termux Federation update agent ready for $ROOT."

while true; do
    if [[ ! -f "$REQUEST_FILE" ]]; then
        sleep "$POLL_SECONDS"
        continue
    fi
    processing="$AGENT_DIR/processing-$$.json"
    if ! mv "$REQUEST_FILE" "$processing" 2>/dev/null; then
        sleep "$POLL_SECONDS"
        continue
    fi
    if ! process_request "$processing"; then
        log "A Federation update request failed closed."
    fi
    rm -f "$processing"
done
