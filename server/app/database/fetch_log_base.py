"""
database/fetch_log_base.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Abstract base class for structured external-API fetch attempt logs.

Each concrete subclass manages one log table for one external service.
"""
from abc import ABC, abstractmethod
from datetime import date, timedelta


class FetchLog(ABC):
    """Abstract base for structured external-API fetch attempt logs.

    Each concrete subclass manages one log table for one external service.
    Subclasses implement record() with service-specific fields, and prune()
    with a DELETE against their own table and timestamp column.

    prune_by_retention() is provided here and calls prune() with a computed
    cutoff — subclasses never need to override it.
    """

    @abstractmethod
    def record(self, **kwargs) -> None:
        """Write or upsert a fetch attempt record."""
        ...

    @abstractmethod
    def prune(self, cutoff: date) -> int:
        """Delete records older than cutoff. Returns number of rows deleted."""
        ...

    def prune_by_retention(self, retention_days: int) -> int:
        """Prune records outside the retention window.

        Args:
            retention_days: Records with end_date older than this many days
                            ago will be deleted.
        Returns:
            Number of rows deleted.
        """
        cutoff = date.today() - timedelta(days=retention_days)
        return self.prune(cutoff)
