from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.schedules import CronSchedule


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
