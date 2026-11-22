FROM python:3.13.7-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PORT=10000 DB_PATH=/data/familiprix.db AI_ALLOW_PAID=0 SEMANTIC_SEARCH_ENABLED=0 \
    AI_AUTO_ENRICH=0 AI_DEEP_LOOKUP=0 MALLOC_ARENA_MAX=2
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && groupadd --gid 10001 locator \
    && useradd --uid 10001 --gid locator --no-create-home locator \
    && mkdir /data && chown locator:locator /data
COPY --chown=locator:locator . .
USER 10001:10001
EXPOSE 10000
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:10000/livez',timeout=4)"
CMD ["python", "deploy/start.py"]
