from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.schedules import CronSchedule
import subprocess
from zoneinfo import ZoneInfo
from datetime import datetime, timezone as dt_timezone

# Each entry: (local_hour, local_minute, day_of_month, grep_pattern, command)
_CRON_JOBS = [
    (
        4, 0, 1,
        "graceful_reboot.sh",
        "/bin/bash /home/dan/services/TravelNet/server/scripts/graceful_reboot.sh scheduled "
        ">> /home/dan/services/TravelNet/server/logs/reboot.log 2>&1"
    ),
    (
        0, 0, 1,
        "tailscale cert",
        "tailscale cert --cert-file /home/dan/services/Dashboard/certs/travelnet.tail186ff8.ts.net.crt "
        "--key-file /home/dan/services/Dashboard/certs/travelnet.tail186ff8.ts.net.key "
        "travelnet.tail186ff8.ts.net >> /home/dan/services/TravelNet/server/logs/tailscale_cert.log 2>&1 "
        "&& docker restart travelnet-nginx"
    ),
    (
        0, 5, 1,
        "watchdog.tail186ff8.ts.net",
        "scp -i /home/dan/.ssh/id_ed25519 "
        "/home/dan/services/Dashboard/certs/travelnet.tail186ff8.ts.net.crt "
        "dan@watchdog.tail186ff8.ts.net:/home/dan/watchdog/certs/travelnet.crt"
    ),
    (
        1, 0, 1,
        "certbot renew",
        "sudo certbot renew --quiet && "
        "cp /etc/letsencrypt/live/api.travelnet.dev/fullchain.pem /home/dan/services/Dashboard/certs/api.travelnet.dev.crt && "
        "chown dan:dan /home/dan/services/Dashboard/certs/api.travelnet.dev.crt && "
        "docker restart travelnet-nginx"
    ),
    (
        1, 5, 1,
        "api.travelnet.dev.crt",
        "scp -i /home/dan/.ssh/id_ed25519 "
        "/home/dan/services/Dashboard/certs/api.travelnet.dev.crt "
        "dan@watchdog.tail186ff8.ts.net:/home/dan/watchdog/certs/api.travelnet.dev.crt"
    ),
]


def update_reboot_cron(iana_tz: str):
    """Rewrite all timezone-sensitive host cron jobs to fire at the correct UTC time
    for the given local IANA timezone."""
    tz = ZoneInfo(iana_tz)
    now = datetime.now(tz)
    new_entries = []
    grep_patterns = []

    for local_hour, local_minute, dom, pattern, command in _CRON_JOBS:
        local_time = now.replace(hour=local_hour, minute=local_minute, second=0, microsecond=0)
        utc_time = local_time.astimezone(dt_timezone.utc)
        new_entries.append(f"{utc_time.minute} {utc_time.hour} {dom} * * {command}")
        grep_patterns.append(pattern)

    strip_cmd = "crontab -l 2>/dev/null || true"  # handle empty crontab safely
    for pattern in grep_patterns:
        strip_cmd += f" | grep -v '{pattern}'"

    new_block = "\n".join(new_entries)

    # Write to a temp file first, verify it's non-empty, then install
    full_cmd = (
        f'NEWCRON=$( ( {strip_cmd}; printf "{new_block}\n" ) ) && '
        f'[ -n "$NEWCRON" ] && '
        f'echo "$NEWCRON" | crontab -'
    )

    subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no",
         "dan@travelnet.tail186ff8.ts.net", full_cmd],
        check=True
    )

@flow(name="Update Deployment Timezones")
async def update_timezones_flow(timezone: str):
    async with get_client() as client:
        deployments = await client.read_deployments()
        for deployment in deployments:
            for sched in deployment.schedules:
                if hasattr(sched.schedule, "cron"):
                    await client.update_deployment_schedule(
                        deployment_id=deployment.id,
                        schedule_id=sched.id,
                        schedule=CronSchedule(cron=sched.schedule.cron, timezone=timezone),
                    )
    update_reboot_cron(timezone)
