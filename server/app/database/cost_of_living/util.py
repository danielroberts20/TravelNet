import sqlite3


def get_col_entry(conn, country_code: str, city: str | None = None) -> sqlite3.Row | None:
    if city:
        row = conn.execute(
            "SELECT * FROM cost_of_living WHERE country_code = ? AND city = ?",
            (country_code, city)
        ).fetchone()
        if row:
            return row
    # Fall back to country-level
    return conn.execute(
        "SELECT * FROM cost_of_living WHERE country_code = ? AND city = ''",
        (country_code,)
    ).fetchone()