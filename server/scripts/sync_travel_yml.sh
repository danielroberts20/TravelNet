#!/bin/bash
#
# sync_travel_yml.sh   Commit and push travel.yml to main if (and only if) it has changed.
#
# Run by travelnet.service as ExecStartPre (every boot / `systemctl restart travelnet`).
# Pushing to main triggers .github/workflows/notify-portfolio.yml, which updates the portfolio.
#
# Rules:
#   - Only travel.yml is ever committed; other dirty files are left untouched.
#   - Only runs on branch `main` (so a dev checkout on a feature branch is never touched).
#   - Refuses to push a travel.yml that is not valid YAML.
#   - Also pushes a previously-failed (unpushed) travel.yml commit, e.g. if the network was down.
#   - Never fails the caller: always exits 0, so the stack always starts.
#
# Overrides (tests only): TRAVEL_REPO_DIR, TRAVEL_SYNC_BRANCH, TRAVEL_SYNC_REMOTE,
#                         TRAVEL_SYNC_NET_WAIT_S

REPO=${TRAVEL_REPO_DIR:-/home/dan/services/TravelNet}
BRANCH=${TRAVEL_SYNC_BRANCH:-main}
REMOTE=${TRAVEL_SYNC_REMOTE:-origin}
NET_WAIT_S=${TRAVEL_SYNC_NET_WAIT_S:-60}
FILE=travel.yml

export GIT_SSH_COMMAND="ssh -o BatchMode=yes -o ConnectTimeout=15"
export GIT_TERMINAL_PROMPT=0

log() { echo "$(date -u +%FT%TZ) [sync_travel_yml] $*"; }

main() {
    cd "$REPO" || { log "repo not found: $REPO"; return 0; }

    [ "$(git rev-parse --abbrev-ref HEAD)" = "$BRANCH" ] \
        || { log "not on $BRANCH, skipping"; return 0; }

    # Anything to do? Uncommitted edits, or a local commit touching travel.yml not yet on the remote.
    local dirty unpushed
    dirty=$(git status --porcelain -- "$FILE")
    unpushed=$(git log --oneline "$REMOTE/$BRANCH..$BRANCH" -- "$FILE" 2>/dev/null)
    if [ -z "$dirty" ] && [ -z "$unpushed" ]; then
        log "$FILE unchanged, nothing to do"
        return 0
    fi

    if [ -n "$dirty" ]; then
        python3 -c "import sys,yaml; yaml.safe_load(open(sys.argv[1]))" "$FILE" 2>/dev/null \
            || { log "$FILE is not valid YAML, NOT committing"; return 0; }
        git add -- "$FILE"
        # Pathspec commit: only travel.yml, even if something else is staged.
        git commit -q -m "chore(travel): sync travel.yml on restart [$(date -u +%FT%TZ)]" -- "$FILE" \
            || { log "commit failed"; return 0; }
        log "committed $FILE"
    fi

    # Wait (bounded) for the network, since this can run early in boot.
    local waited=0
    until git ls-remote -q "$REMOTE" HEAD >/dev/null 2>&1; do
        [ "$waited" -ge "$NET_WAIT_S" ] && { log "remote unreachable, left commit local for next restart"; return 0; }
        sleep 5; waited=$((waited + 5))
    done

    # The stats bot also pushes to main, so rebase first; retry once if we lose a race.
    local attempt
    for attempt in 1 2; do
        git pull --rebase --autostash -q "$REMOTE" "$BRANCH" \
            || { git rebase --abort 2>/dev/null; log "rebase failed (conflict?), left commit local"; return 0; }
        if git push -q "$REMOTE" "$BRANCH"; then
            log "pushed to $REMOTE/$BRANCH"
            return 0
        fi
        log "push attempt $attempt failed"
    done
    log "push failed, left commit local for next restart"
    return 0
}

# One at a time; the whole thing is time-boxed so it can never hold up boot for long.
exec 9>/tmp/sync_travel_yml.lock
flock -n 9 || { log "already running"; exit 0; }
export -f main log
export REPO BRANCH REMOTE NET_WAIT_S FILE
timeout 180 bash -c main || log "timed out or errored (rc=$?)"
exit 0
