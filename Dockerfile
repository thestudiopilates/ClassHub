FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md runtime.txt ./
COPY app ./app
COPY railway ./railway
COPY scripts ./scripts

RUN pip install --upgrade pip && pip install .

RUN chmod +x railway/run-web.sh railway/run-ops-sync.sh

EXPOSE 8000

CMD ["bash", "railway/run-web.sh"]
