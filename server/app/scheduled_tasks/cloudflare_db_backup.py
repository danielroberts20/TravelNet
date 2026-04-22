from prefect import flow
from prefect_shell import ShellOperation
from notifications import notify_on_completion, record_flow_result


@flow(
    name="Cloudflare DB Backup",
    on_completion=[notify_on_completion],
    on_failure=[notify_on_completion],
)
def cloudflare_backup_db_flow(prefix: str | None = None, suffix: str | None = None):
    env = {}
    if prefix is not None:
        env["PREFIX"] = prefix
    if suffix is not None:
        env["SUFFIX"] = suffix

    ShellOperation(
        commands=["bash /data/backup_db_to_cloudflare.sh"],
        env=env,
    ).run()

    result = {"status": "completed", "prefix": prefix, "suffix": suffix}
    record_flow_result(result)
    return result