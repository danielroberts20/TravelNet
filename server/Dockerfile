FROM python:3.12-slim

WORKDIR /app

# System deps first — changes rarely, should almost never rebuild
RUN apt-get update \
    && apt-get install -y --no-install-recommends rclone age sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Python deps — only rebuilds when requirements.txt changes
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]