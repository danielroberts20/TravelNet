from config.general import LOCATION_NOISE_ACCURACY_THRESHOLD
from database.connection import get_conn
from database.location.noise.table import table as noise_table, LocationNoiseRecord


def flag_tier1() -> int:
    with get_conn() as conn:
        """Flag unflagged points where horizontal_accuracy > threshold."""
        rows = conn.execute("""
            SELECT o.id, o.horizontal_accuracy
            FROM location_overland o
            WHERE o.horizontal_accuracy > ?
            AND NOT EXISTS (
                SELECT 1 FROM location_noise n WHERE n.overland_id = o.id
            )
        """, (LOCATION_NOISE_ACCURACY_THRESHOLD,)).fetchall()

        for row in rows:
            noise_table.insert(LocationNoiseRecord(
                overland_id=row["id"],
                tier=1,
                reason="accuracy_threshold",
            ))

        return len(rows)

if __name__ == "__main__":
    flag_tier1()