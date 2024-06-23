# settings.py
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_default_region: str
    dynamodb_endpoint: str = None  # Optional for local testing

    class Config:
        env_file = ".env"


settings = Settings()
