import os
import json
import secrets
import logging
import sqlite3
from typing import Annotated
from contextlib import asynccontextmanager, closing

from fastapi import FastAPI, Depends, HTTPException, Request, status, UploadFile, File
from fastapi.exceptions import RequestValidationError
from fastapi.security import HTTPBasic, HTTPBasicCredentials, HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field, ValidationError
import nats
from nats.aio.client import Client as NATS

from app.config import settings
from app.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

security = HTTPBasic()
bearer = HTTPBearer()
nc: NATS = nats.NATS()

MAX_USAGE_BODY_BYTES = 10 * 1024 * 1024
MAX_USAGE_EVENTS = 100_000
MAX_USAGE_TEXT_LENGTH = 1024

def get_current_username(credentials: Annotated[HTTPBasicCredentials, Depends(security)]):
    correct_username = secrets.compare_digest(credentials.username.encode(), settings.auth_username.encode())
    correct_password = secrets.compare_digest(credentials.password.encode(), settings.auth_password.encode())
    if not (correct_username and correct_password):
        logger.warning(f"Failed authentication attempt for username: {credentials.username!r}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

def verify_usage_token(credentials: Annotated[HTTPAuthorizationCredentials, Depends(bearer)]):
    if not secrets.compare_digest(credentials.credentials.encode(), settings.usage_token.encode()):
        logger.warning("Failed usage token authentication attempt")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )

def usage_db_path() -> str:
    return os.path.join(settings.usage_dir, "usage_events.db")

def init_usage_db():
    os.makedirs(settings.usage_dir, exist_ok=True)
    with closing(sqlite3.connect(usage_db_path())) as conn, conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS usage_events ("
            "device_id TEXT NOT NULL, timestamp INTEGER NOT NULL, package TEXT NOT NULL, event_type TEXT NOT NULL, "
            "class_name TEXT NOT NULL, "
            "PRIMARY KEY (device_id, timestamp, package, event_type, class_name))"
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
    init_usage_db()
    logger.info(f"Ensured usage database exists in {settings.usage_dir}")
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
    logger.info(f"Upload request received from user '{username}' for file {file.filename!r}")
    if file.filename != os.path.basename(file.filename) or not file.filename.endswith(".m4a"):
        logger.warning(f"Invalid file name rejected: {file.filename!r} with content type {file.content_type!r}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file name. Only a bare .m4a filename is supported.",
        )
    
    file_path = os.path.join(settings.storage_dir, file.filename)
    
    try:
        with open(file_path, "wb") as f:
            while contents := await file.read(1024 * 1024):
                f.write(contents)
        logger.debug(f"Successfully saved file to {file_path!r}")
    except Exception as e:
        logger.error(f"Failed to save file {file.filename!r}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save file: {e}")
        
    # Publish filename to NATS JetStream
    try:
        payload = json.dumps({"filename": file.filename}).encode("utf-8")
        js = nc.jetstream()
        await js.publish(settings.nats_subject, payload)
        logger.debug(f"Published NATS JetStream message for file {file.filename!r} to subject '{settings.nats_subject}'")
    except Exception as e:
        logger.error(f"Failed to publish NATS JetStream message for file {file.filename!r}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to publish to message broker")
    
    return {"message": "File uploaded successfully", "filename": file.filename}

class UsageEvent(BaseModel):
    timestamp: int = Field(strict=True, ge=0, le=2**63 - 1)
    package: str = Field(min_length=1, max_length=MAX_USAGE_TEXT_LENGTH)
    event_type: str = Field(min_length=1, max_length=MAX_USAGE_TEXT_LENGTH)
    class_name: str = Field(default="", max_length=MAX_USAGE_TEXT_LENGTH)  # Android sends one only for activity events

class UsageEventBatch(BaseModel):
    device_id: str = Field(min_length=1, max_length=MAX_USAGE_TEXT_LENGTH)
    events: list[UsageEvent] = Field(max_length=MAX_USAGE_EVENTS)

async def read_usage_batch(request: Request) -> UsageEventBatch:
    # A dependency rather than a body parameter: FastAPI parses those before any dependency runs, auth included
    body = bytearray()
    async for chunk in request.stream():
        body += chunk
        if len(body) > MAX_USAGE_BODY_BYTES:
            raise HTTPException(status_code=status.HTTP_413_CONTENT_TOO_LARGE, detail="Request body too large")
    try:
        return UsageEventBatch.model_validate_json(body)
    except ValidationError as e:
        raise RequestValidationError(e.errors(include_input=False, include_context=False))

@app.post("/usage-events", dependencies=[Depends(verify_usage_token)])
def ingest_usage_events(batch: Annotated[UsageEventBatch, Depends(read_usage_batch)]):
    rows = [(batch.device_id, e.timestamp, e.package, e.event_type, e.class_name) for e in batch.events]
    with closing(sqlite3.connect(usage_db_path())) as conn, conn:
        inserted = conn.executemany("INSERT OR IGNORE INTO usage_events (device_id, timestamp, package, event_type, class_name) VALUES (?, ?, ?, ?, ?)", rows).rowcount
    logger.info(f"Usage events from device {batch.device_id!r}: {len(rows)} received, {inserted} new")
    return {"received": len(rows), "inserted": inserted}
