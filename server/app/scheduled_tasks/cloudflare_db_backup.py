from prefect import flow
from prefect_shell import ShellOperation
from notifications import notify_on_completion


@flow(name="Cloudflare DB Backup", on_completion=[notify_on_completion], on_failure=[notify_on_completion])
def cloudflare_backup_db_flow():
    ShellOperation(
        commands=["bash /data/backup_db_to_cloudflare.sh"]
    ).run()