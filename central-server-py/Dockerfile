FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd --uid 10001 --create-home ids
COPY --chown=ids:ids api ./api
COPY --chown=ids:ids rules ./rules
RUN mkdir -p /app/models && chown ids:ids /app/models
USER ids
CMD ["python", "-m", "api.kafka_consumer"]
