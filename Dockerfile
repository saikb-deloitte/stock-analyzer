# Centaur Prism — Fly.io / Docker build
# Used by Fly.io (and any other Docker-friendly host).
FROM python:3.11-slim

# System deps: gcc + libssl for curl_cffi compilation, tini for clean signals
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libssl-dev \
        tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache deps layer separately for fast rebuilds
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy app code last so code changes don't invalidate the deps cache
COPY . .

# Fly.io injects PORT at runtime (defaults to 8080)
ENV PORT=8080
EXPOSE 8080

# tini handles PID 1 + signal forwarding so SIGTERM cleanly shuts down gunicorn
ENTRYPOINT ["/usr/bin/tini", "--"]

# 1 worker, gevent for async I/O, 100 concurrent connections, 3-min timeout
CMD gunicorn app:app \
    --bind 0.0.0.0:$PORT \
    --timeout 180 \
    --workers 1 \
    --worker-class gevent \
    --worker-connections 100
