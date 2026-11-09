"""The legacy print-only receiver must never acknowledge delivery."""
from fastapi import FastAPI, HTTPException

app = FastAPI()

@app.post('/ingest')
async def ingest_logs():
    raise HTTPException(status_code=503, detail='HTTP ingestion is disabled. Configure the agent Kafka transport.')
