FROM python:3.9-slim

# (optional but useful)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /main
COPY requirements.txt .

# Install curl for ECS healthcheck + deps, then clean apt cache
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && pip install --no-cache-dir -r requirements.txt \
 && rm -rf /var/lib/apt/lists/*

COPY . .
EXPOSE 80
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
