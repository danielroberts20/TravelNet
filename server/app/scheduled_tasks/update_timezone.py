from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.schedules import CronSchedule
import subprocess
from zoneinfo import ZoneInfo
from datetime import datetime, timezone

def update_reboot_cron(timezone: str):
    tz = ZoneInfo(timezone)
    local_4am = datetime.now(tz).replace(hour=4, minute=0, second=0, microsecond=0)
    utc_4am = local_4am.astimezone(timezone.utc)
    
    new_cron = (
        f"{utc_4am.minute} {utc_4am.hour} 1 * * "
        f"/bin/bash /home/dan/services/TravelNet/server/scripts/graceful_reboot.sh scheduled "
        f">> /home/dan/services/TravelNet/server/logs/reboot.log 2>&1"
    )

    subprocess.run([
        "ssh", "-o", "StrictHostKeyChecking=no", "dan@pi-server.tail186ff8.ts.net",
        f'(crontab -l | grep -v graceful_reboot.sh; echo "{new_cron}") | crontab -'
    ], check=True)

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
