import os
import json
import secrets
import logging
import sqlite3
from typing import Annotated
from contextlib import asynccontextmanager, closing

from fastapi import FastAPI, Depends, HTTPException, status, UploadFile, File
from fastapi.security import HTTPBasic, HTTPBasicCredentials, HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import nats
from nats.aio.client import Client as NATS

from app.config import settings
from app.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

security = HTTPBasic()
bearer = HTTPBearer()
nc: NATS = nats.NATS()

def get_current_username(credentials: Annotated[HTTPBasicCredentials, Depends(security)]):
    correct_username = secrets.compare_digest(credentials.username, settings.auth_username)
    correct_password = secrets.compare_digest(credentials.password, settings.auth_password)
    if not (correct_username and correct_password):
        logger.warning(f"Failed authentication attempt for username: {credentials.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

def verify_usage_token(credentials: Annotated[HTTPAuthorizationCredentials, Depends(bearer)]):
    if not secrets.compare_digest(credentials.credentials, settings.usage_token):
        logger.warning("Failed usage token authentication attempt")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Connecting to NATS at {settings.nats_url}")
    await nc.connect(settings.nats_url, connect_timeout=10)
    logger.info("Connected to NATS")
    
    js = nc.jetstream()
    try:
        await js.add_stream(name="audio_events", subjects=[settings.nats_subject])
        logger.info("JetStream stream 'audio_events' ensured")
    except Exception as e:
        logger.info(f"Stream may already exist or cannot be modified: {e}")

    # create storage dir if it doesn't exist
    os.makedirs(settings.storage_dir, exist_ok=True)
    logger.info(f"Ensured storage directory exists at {settings.storage_dir}")
    yield
    if nc.is_connected:
        logger.info("Closing NATS connection")
        await nc.close()

app = FastAPI(lifespan=lifespan)

@app.post("/upload")
async def upload_audio(
    file: UploadFile = File(...),
    username: str = Depends(get_current_username)
):
    logger.info(f"Upload request received from user '{username}' for file '{file.filename}'")
    if file.content_type != "audio/mp4" and not file.filename.endswith(".m4a"):
        logger.warning(f"Invalid file format rejected: {file.filename} with content type {file.content_type}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file format. Only MPEG-4 audio is supported.",
        )
    
    file_path = os.path.join(settings.storage_dir, file.filename)
    
    try:
        with open(file_path, "wb") as f:
            while contents := await file.read(1024 * 1024):
                f.write(contents)
        logger.debug(f"Successfully saved file to {file_path}")
    except Exception as e:
        logger.error(f"Failed to save file {file.filename}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save file: {e}")
        
    # Publish filename to NATS JetStream
    try:
        payload = json.dumps({"filename": file.filename}).encode("utf-8")
        js = nc.jetstream()
        await js.publish(settings.nats_subject, payload)
        logger.debug(f"Published NATS JetStream message for file '{file.filename}' to subject '{settings.nats_subject}'")
    except Exception as e:
        logger.error(f"Failed to publish NATS JetStream message for file '{file.filename}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to publish to message broker")
    
    return {"message": "File uploaded successfully", "filename": file.filename}

class UsageEvent(BaseModel):
    timestamp: int
    package: str
    event_type: str

class UsageEventBatch(BaseModel):
    device_id: str
    events: list[UsageEvent]

@app.post("/usage-events", dependencies=[Depends(verify_usage_token)])
def ingest_usage_events(batch: UsageEventBatch):
    rows = [(batch.device_id, e.timestamp, e.package, e.event_type) for e in batch.events]
    db_path = os.path.join(settings.storage_dir, "usage_events.db")
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS usage_events ("
            "device_id TEXT NOT NULL, timestamp INTEGER NOT NULL, package TEXT NOT NULL, event_type TEXT NOT NULL, "
            "PRIMARY KEY (device_id, timestamp, package, event_type))"
        )
        inserted = conn.executemany("INSERT OR IGNORE INTO usage_events (device_id, timestamp, package, event_type) VALUES (?, ?, ?, ?)", rows).rowcount
    logger.info(f"Usage events from device '{batch.device_id}': {len(rows)} received, {inserted} new")
    return {"received": len(rows), "inserted": inserted}
