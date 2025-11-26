from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    UPSTOX_API_KEY: str
    UPSTOX_API_SECRET: str
    UPSTOX_REDIRECT_URI: str
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379

    class Config:
        env_file = ".env"

settings = Settings()
