from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    storage_dir: str = "./data/recordings"
    nats_url: str = "nats://localhost:4222"
    nats_subject: str = "audio.ingested"
    auth_username: str = "admin"
    auth_password: str = "secret"
    usage_token: str = ""  # empty rejects every request until USAGE_TOKEN is set
    log_level: str = "INFO"
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
