"""from database.util import get_conn
from sklearn.neighbors import KernelDensity #type: ignore
import numpy as np
import logging

logger = logging.getLogger(__name__)

with get_conn() as conn:
    rows = conn.execute('''SELECT timestamp FROM (
        SELECT timestamp FROM location_overland ORDER BY RANDOM() LIMIT 10000
    ) ORDER BY timestamp ASC''')
    data = rows.fetchall()
    data = np.array(data)
    timestamps = data[:, 0]  # flatten to 1D
    dt = np.array(timestamps, dtype='datetime64[s]').astype('int64')
    deltas = np.diff(dt)
    deltas = deltas[deltas <= 600]
    deltas = deltas[deltas >= 0]
    logger.info(deltas, deltas.shape)
    
    kde = KernelDensity(
        bandwidth="scott", 
        kernel="gaussian")
    kde.fit(deltas.reshape(-1, 1))
    logger.info("Fitted KDE")

    # Evaluate KDE over the plausible range
    x = np.linspace(deltas.min(), deltas.max(), 1000).reshape(-1, 1)
    log_density = kde.score_samples(x)
    k = float(x[np.argmax(log_density)])
    logger.info(f"k={k}")"""
        
