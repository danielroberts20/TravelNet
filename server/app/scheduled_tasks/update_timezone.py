from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.schedules import CronSchedule
from prefect.exceptions import ObjectNotFound
from zoneinfo import ZoneInfo
from datetime import datetime, timezone as dt_timezone
from notifications import notify_on_completion, log_on_success


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
        "certbot renew --quiet",
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
    tz = ZoneInfo(iana_tz)
    now = datetime.now(tz)
    new_entries = []
    grep_patterns = []

    for local_hour, local_minute, dom, pattern, command in _CRON_JOBS:
        local_time = now.replace(hour=local_hour, minute=local_minute, second=0, microsecond=0)
        utc_time = local_time.astimezone(dt_timezone.utc)
        new_entries.append(f"{utc_time.minute} {utc_time.hour} {dom} * * {command}")
        grep_patterns.append(pattern)

    crontab_path = "/var/spool/cron/crontabs/dan"
    
    # Read existing crontab
    with open(crontab_path, "r") as f:
        existing = f.readlines()
    
    # Strip managed lines
    filtered = [
        line for line in existing
        if not any(pattern in line for pattern in grep_patterns)
    ]
    
    # Append new entries
    new_lines = filtered + [e + "\n" for e in new_entries]
    
    with open(crontab_path, "w") as f:
        f.writelines(new_lines)


@flow(name="Update Deployment Timezones", on_failure=[notify_on_completion], on_completion=[log_on_success])
async def update_timezones_flow(timezone: str):
    async with get_client() as client:
        deployments = await client.read_deployments()
        for deployment in deployments:
            for sched in deployment.schedules:
                if hasattr(sched.schedule, "cron"):
                    try:
                        await client.update_deployment_schedule(
                            deployment_id=deployment.id,
                            schedule_id=sched.id,
                            schedule=CronSchedule(cron=sched.schedule.cron, timezone=timezone),
                        )
                    except ObjectNotFound:
                        pass  # stale schedule ID, skip
    update_reboot_cron(timezone)
