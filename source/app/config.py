import dotenv, typing
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings

class ApiConfig(BaseSettings):
    api_host: str = Field('127.0.0.1')
    api_port: int = Field(5000)
    worker_num: int = Field(1)
    forwarded_allow_ips: str = Field('127.0.0.1')
    name_prefix: str = Field(...)
    auth_url: str = Field('')
    secret_key: str = Field('')
    mongo_uri: typing.Optional[str] = Field(None)
    mongo_db: typing.Optional[str] = Field(None)
    kafka_bootstrap_servers: typing.Optional[str] = Field(None)
    kafka_topic: typing.Optional[str] = Field(None)
    use_redis: bool = Field(False)
    redis_host: str = Field('127.0.0.1')
    redis_port: int = Field(6379)
    redis_db: int = Field(0)
    redis_auth: typing.Optional[str] = Field(None)
    redis_timeout: typing.Optional[float] = Field(None)
    authless_mode: bool = Field(False)

    @model_validator(mode='after')
    def validate_auth_requirements(self):
        if not self.authless_mode:
            if not self.auth_url:
                raise ValueError('AUTH_URL is required when AUTHLESS_MODE is False')
            if not self.secret_key:
                raise ValueError('SECRET_KEY is required when AUTHLESS_MODE is False')
        return self

    class Config:
        env_file = dotenv.find_dotenv(usecwd=True)
        env_file_encoding = 'utf-8'
        extra = 'ignore'

config = ApiConfig()
