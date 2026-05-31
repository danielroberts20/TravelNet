#!/usr/bin/env bash
# Collect SMART health data for both storage drives and write to /data/smart_data.json.
# Drives are identified by filesystem label (stable across reboots).
# Safe to run when either drive is absent.

set -uo pipefail

OUTPUT="/mnt/ssd/docker/services/travelnet/data/smart_data.json"
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

TMPD=$(mktemp -d)
trap 'rm -rf "$TMPD"' EXIT

# ── Python parser (reads raw smartctl JSON from a file) ───────────────────────
cat > "$TMPD/parse.py" << 'PYEOF'
import json, sys

raw_file, disk, label, role, timestamp, out_file = sys.argv[1:7]

try:
    with open(raw_file) as f:
        data = json.load(f)
except Exception:
    data = {}

def attr(table, attr_id):
    for a in (table or []):
        if a.get("id") == attr_id:
            return a.get("raw", {}).get("value")
    return None

attrs  = (data.get("ata_smart_attributes") or {}).get("table") or []
passed = (data.get("smart_status") or {}).get("passed")

result = {
    "device":               disk,
    "label":                label,
    "health":               ("PASSED" if passed else "FAILED") if passed is not None else "UNKNOWN",
    "temperature_c":        (data.get("temperature") or {}).get("current"),
    "power_on_hours":       (data.get("power_on_time") or {}).get("hours"),
    "reallocated_sectors":  attr(attrs, 5),
    "uncorrectable_errors": attr(attrs, 187) if attr(attrs, 187) is not None else attr(attrs, 198),
    "collected_at":         timestamp,
}

if role == "ssd":
    lbas = attr(attrs, 241)
    result["wear_leveling_count"]  = attr(attrs, 177)
    result["total_host_writes_gb"] = round(lbas * 512 / 1e9, 1) if lbas is not None else None

with open(out_file, "w") as f:
    json.dump(result, f)
PYEOF

# ── Collect SMART data for one drive ─────────────────────────────────────────
collect_drive() {
    local label="$1"
    local role="$2"
    local raw_file="$TMPD/${role}_raw.json"
    local out_file="$TMPD/${role}_parsed.json"

    # Find the partition device by filesystem label, then derive the whole disk
    local partition disk
    partition=$(blkid -l -t LABEL="$label" -o device 2>/dev/null || true)
    if [[ -z "$partition" ]]; then
        echo "null" > "$out_file"
        return
    fi
    # Strip trailing partition digits: /dev/sda1 → /dev/sda, /dev/sdb → /dev/sdb
    disk=$(echo "$partition" | sed 's/[0-9]*$//')

    # smartctl can return non-zero even on success (e.g. exit 4 = some attributes failed).
    # Write whatever JSON we get; the parser handles empty/malformed input gracefully.
    smartctl -a -j -d sat "$disk" > "$raw_file" 2>/dev/null \
        || smartctl -a -j "$disk" > "$raw_file" 2>/dev/null \
        || echo "{}" > "$raw_file"

    python3 "$TMPD/parse.py" "$raw_file" "$disk" "$label" "$role" "$TIMESTAMP" "$out_file"
}

collect_drive "travelnet-ssd" "ssd"
collect_drive "Linux"         "hdd"

# ── Merge both results into final JSON ────────────────────────────────────────
python3 - "$TMPD/ssd_parsed.json" "$TMPD/hdd_parsed.json" "$TIMESTAMP" "$OUTPUT" << 'PYEOF'
import json, sys

ssd_file, hdd_file, timestamp, out_file = sys.argv[1:5]

def load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None

result = {
    "ssd":          load(ssd_file),
    "hdd":          load(hdd_file),
    "collected_at": timestamp,
}

with open(out_file, "w") as f:
    json.dump(result, f, indent=2)
PYEOF
