"""
scheduled_tasks/run_tests.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Run the full pytest suite and report results via CronJobMailer email.

NOTE: Unlike other scheduled tasks, this one runs on the HOST (not inside
Docker) because the test suite uses in-memory SQLite and mocks that require
the host Python environment. Do NOT run it via runcron.sh.

Scheduled: daily (e.g. 06:00)
  0 6 * * * cd /home/dan/services/TravelNet && python server/app/scheduled_tasks/run_tests.py

On success → ✅ email with passed/failed/error counts.
On failure → ❌ email with counts and full pytest output.
"""

import logging
import re
import subprocess
import sys
from pathlib import Path

# Add server/app to sys.path so TravelNet modules are importable on the host
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "server" / "app"))

from config.editable import load_overrides
load_overrides()

from config.settings import settings
from notifications import CronJobMailer

logger = logging.getLogger(__name__)


def run_tests() -> dict:
    """Run pytest against the full test suite and return a summary dict.

    :returns: dict with keys passed, failed, errors, exit_code, output.
    """
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "server/app/tests/", "-q", "--tb=short"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    output = result.stdout + result.stderr
    passed = failed = errors = 0

    for line in output.splitlines():
        for match in re.finditer(r"(\d+) (passed|failed|error)", line):
            count, label = int(match.group(1)), match.group(2)
            if label == "passed":
                passed = count
            elif label == "failed":
                failed = count
            elif label == "error":
                errors = count

    return {
        "passed": passed,
        "failed": failed,
        "errors": errors,
        "exit_code": result.returncode,
        "output": output,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    with CronJobMailer("run_tests", settings.smtp_config,
                       detail="Full pytest suite on host") as job:
        results = run_tests()
        job.add_metric("passed", results["passed"])
        job.add_metric("failed", results["failed"])
        job.add_metric("errors", results["errors"])

        if results["exit_code"] != 0:
            raise RuntimeError(
                f"Test suite failed (exit {results['exit_code']}):\n\n{results['output']}"
            )
