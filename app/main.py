import os
import json
import secrets
import logging
from typing import Annotated
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException, status, UploadFile, File
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import nats
from nats.aio.client import Client as NATS

from app.config import settings
from app.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

security = HTTPBasic()
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Connecting to NATS at {settings.nats_url}")
    await nc.connect(settings.nats_url, connect_timeout=10)
    logger.info("Connected to NATS")
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
        
    # Publish filename to NATS
    try:
        payload = json.dumps({"filename": file.filename}).encode("utf-8")
        await nc.publish(settings.nats_subject, payload)
        logger.debug(f"Published NATS message for file '{file.filename}' to subject '{settings.nats_subject}'")
    except Exception as e:
        logger.error(f"Failed to publish NATS message for file '{file.filename}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to publish to message broker")
    
    return {"message": "File uploaded successfully", "filename": file.filename}
