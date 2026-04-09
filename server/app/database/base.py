"""
database/base.py
~~~~~~~~~~~~~~~~
Abstract base class for all SQLite table definitions.

Every table module defines a dataclass for its record type (T) and a
class that inherits BaseTable[T].  The ABC enforces two contracts:

  init()   — create the table and its indexes (idempotent, IF NOT EXISTS)
  insert() — insert a single record (idempotent, INSERT OR IGNORE)

Tables that must return the newly inserted row id (e.g. for use as a
foreign key in a subsequent insert) may declare insert() -> int instead
of the default None.  Python allows covariant return types in subclasses.

Multi-table transactions (where two tables must share a connection) are
handled outside the table classes — the calling code manages the
connection and calls table-specific helpers directly.
"""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar("T")


class BaseTable(ABC, Generic[T]):
    """Abstract base for all SQLite table definitions."""

    @abstractmethod
    def init(self) -> None:
        """Create this table and its indexes if they do not exist."""
        ...

    @abstractmethod
    def insert(self, record: T) -> None:
        """Insert a single record. Must be idempotent."""
        ...
