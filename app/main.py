import os
import json
import secrets
from typing import Annotated
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException, status, UploadFile, File
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import nats
from nats.aio.client import Client as NATS

from app.config import settings

security = HTTPBasic()
nc: NATS = nats.NATS()

def get_current_username(credentials: Annotated[HTTPBasicCredentials, Depends(security)]):
    correct_username = secrets.compare_digest(credentials.username, settings.auth_username)
    correct_password = secrets.compare_digest(credentials.password, settings.auth_password)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

@asynccontextmanager
async def lifespan(app: FastAPI):
    await nc.connect(settings.nats_url)
    # create storage dir if it doesn't exist
    os.makedirs(settings.storage_dir, exist_ok=True)
    yield
    if nc.is_connected:
        await nc.close()

app = FastAPI(lifespan=lifespan)

@app.post("/upload")
async def upload_audio(
    file: UploadFile = File(...),
    username: str = Depends(get_current_username)
):
    if file.content_type != "audio/mp4" and not file.filename.endswith(".m4a"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file format. Only MPEG-4 audio is supported.",
        )
    
    file_path = os.path.join(settings.storage_dir, file.filename)
    
    try:
        with open(file_path, "wb") as f:
            while contents := await file.read(1024 * 1024):
                f.write(contents)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {e}")
        
    # Publish filename to NATS
    payload = json.dumps({"filename": file.filename}).encode("utf-8")
    await nc.publish(settings.nats_subject, payload)
    
    return {"message": "File uploaded successfully", "filename": file.filename}
