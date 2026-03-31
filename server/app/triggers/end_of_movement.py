import logging

from database.health.util import get_metric_entries, get_unique_metrics

logger = logging.getLogger(__name__)

def get_step_count():
    metrics = get_unique_metrics()
    logger.info(f"Available health metrics: {metrics}")
    #step_counts = get_metric_entries("Step Count")

def run():
    get_step_count()