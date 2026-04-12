"""
scheduled_tasks/run_tests.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Run the full pytest suite and report results via email.

NOTE: Unlike other scheduled tasks, this one runs on the HOST (not inside
Docker) because the test suite uses in-memory SQLite and mocks that require
the host Python environment. It is NOT registered in deployments.py and must
not be run via runcron.sh. It is scheduled by a system cron on the host:

  0 6 1,15 * * /home/dan/services/TravelNet/.venv/bin/python \
      /home/dan/services/TravelNet/server/app/scheduled_tasks/run_tests.py

On success → ✅ email with passed/failed/error counts.
On failure → ❌ email with counts and full pytest output.
"""
from config.editable import load_overrides
load_overrides()

import re
import subprocess
import sys
from pathlib import Path

from prefect import task, flow
from prefect.logging import get_run_logger


@task
def run_pytest_suite() -> dict:
    logger = get_run_logger()

    # Resolve REPO_ROOT here so this module is safe to import inside Docker
    repo_root = Path(__file__).resolve().parents[3]
    if str(repo_root / "server" / "app") not in sys.path:
        sys.path.insert(0, str(repo_root / "server" / "app"))

    logger.info("Running pytest suite from %s...", repo_root)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "server/tests/", "-q", "--tb=short"],
        cwd=repo_root,
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

    logger.info(
        "pytest complete: %d passed, %d failed, %d errors (exit %d)",
        passed, failed, errors, result.returncode,
    )
    return {
        "passed": passed,
        "failed": failed,
        "errors": errors,
        "exit_code": result.returncode,
        "output": output,
    }


@flow(name="run-tests")
def run_tests_flow():
    logger = get_run_logger()
    results = run_pytest_suite()

    if results["exit_code"] != 0:
        raise RuntimeError(
            f"Test suite failed (exit {results['exit_code']}):\n\n{results['output']}"
        )

    logger.info("All tests passed.")
    return {
        "passed": results["passed"],
        "failed": results["failed"],
        "errors": results["errors"],
    }
