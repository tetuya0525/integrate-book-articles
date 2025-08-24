# ==============================================================================
# Dockerfile for Integrate Book Articles Service (v2.1)
# ==============================================================================
FROM python:3.12-slim

ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
WORKDIR $APP_HOME

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN adduser --system --group appuser
USER appuser

COPY . .

ENV PORT 8080
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "main:create_app()"]
