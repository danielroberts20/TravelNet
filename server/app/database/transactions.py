from database.util import get_conn

def init():
    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY,
            timestamp INTEGER NOT NULL,
            device TEXT,                     -- optional: if you log manually from phone
            bank TEXT NOT NULL,              -- NatWest, Wise, Revolut, etc.
            merchant TEXT,
            category_raw TEXT,               -- original from bank CSV
            category_model TEXT,             -- assigned via ML / rules
            amount_original REAL NOT NULL,
            currency_original TEXT NOT NULL,
            amount_base REAL,                -- converted using fx_rates
            country_code TEXT,               -- inferred from transaction or FX
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(timestamp, bank, amount_original, currency_original)
        );
        """)

        # Indexes for performance
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tx_bank_timestamp
            ON transactions(bank, timestamp);
        """)
        
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tx_country
            ON transactions(country_code);
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tx_timestamp
            ON transactions(timestamp);
        """)