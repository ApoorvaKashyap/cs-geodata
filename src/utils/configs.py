from pydantic import SecretStr
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables or a .env file.

    Precedence (highest → lowest):
        1. Real environment variables (e.g. ``export REDIS_HOST=...``)
        2. Values in ``.env`` in the project root
        3. The defaults declared below

    AWS secret fields use :class:`pydantic.SecretStr` so their values are
    never accidentally logged or included in ``model_dump()`` output.
    """

    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379

    # AWS
    aws_access_key_id: str = ""
    aws_secret_access_key: SecretStr = SecretStr("")
    aws_region: str = "ap-south-1"
    S3_BASE: str = ""

    # CoREStack API
    corestack_api_url: str = ""
    corestack_api_key: SecretStr = SecretStr("")
    base_geoserver: str = ""

    # Working Directory
    temp_path: str = "/tmp/"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
